/* chat - persistent peer-to-peer terminal chat over myio.
 *
 * usage: ./chat <port> [host [peer-port]]
 *
 * Always listens on <port>. When a host is given it also tries to connect
 * to host:peer-port (default: <port>), retrying every 10 seconds while
 * disconnected - so whichever of the two peers comes up first, they find
 * each other. When the peer goes away, the app returns to the waiting
 * state (listening + connect retries) instead of exiting.
 *
 * Single-threaded and asynchronous: the app keeps a stdin read pending at
 * all times, plus either a peer read (connected) or accept/connect/retry
 * tasks (disconnected), and multiplexes them all with myio_select().
 * Terminal IO goes through the interface too (myio_read/myio_write on fds
 * 0 and 1). The uv backend is used because an interactive multiplexer
 * needs ops that wait concurrently; a fully synchronous backend would
 * block on stdin at submit time and degenerate into a turn-based chat.
 *
 * If both sides dial each other at the same instant, two connections can
 * come up at once; each side keeps the one its select() reports first and
 * closes the other. The sides may disagree and drop both, but the retry
 * loop re-establishes contact.
 */
#include "myio.h"
#include "myio_uv.h"
#include "myio_xev.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BUFSZ 512

typedef struct {
    myio       *io;
    const char *host;      /* NULL: accept-only */
    int         peer_port;
    myio_sock  *listener;
    myio_sock  *peer;      /* NULL while disconnected */
    myio_task  *tin;       /* stdin read: always pending */
    myio_task  *tnet;      /* peer read: pending while connected */
    myio_task  *taccept;   /* pending while disconnected */
    myio_task  *tconn;     /* in-flight connect attempt */
    myio_task  *tsleep;    /* retry backoff timer */
    char        inbuf[BUFSZ];
    char        netbuf[BUFSZ];
    char        outbuf[BUFSZ + 8];
} chat;

static uint64_t retry_ms(void) {
    const char *e = getenv("CHAT_RETRY_MS"); /* test hook */
    return e ? (uint64_t)atoi(e) : 10000;
}

/* Retire a pending establishment task (accept/connect/sleep). If it lost a
 * race and already completed with a socket, that socket must be closed,
 * not leaked. */
static void drop_task(chat *c, myio_task **pt) {
    if (!*pt)
        return;
    myio_cancel(c->io, *pt);
    myio_result r = myio_join(c->io, *pt);
    *pt = NULL;
    if (myio_ok(r) && r.ptr)
        myio_join(c->io, myio_sock_close(c->io, r.ptr));
}

static void become_connected(chat *c, myio_sock *peer) {
    c->peer = peer;
    drop_task(c, &c->taccept);
    drop_task(c, &c->tconn);
    drop_task(c, &c->tsleep);
    c->tnet = myio_sock_read(c->io, c->peer, c->netbuf, sizeof c->netbuf);
    fprintf(stderr, "* peer connected\n");
}

static void go_disconnected(chat *c) {
    fprintf(stderr, "* waiting for a peer on port %d%s\n",
            myio_sock_port(c->io, c->listener),
            c->host ? " (and dialing out)" : "");
    c->taccept = myio_tcp_accept(c->io, c->listener);
    if (c->host)
        c->tconn = myio_tcp_connect(c->io, c->host, c->peer_port);
}

static void disconnect_peer(chat *c) {
    if (c->tnet) {
        myio_cancel(c->io, c->tnet);
        myio_task_free(c->io, c->tnet);
        c->tnet = NULL;
    }
    if (c->peer) {
        myio_join(c->io, myio_sock_close(c->io, c->peer));
        c->peer = NULL;
    }
}

/* Returns 0 on stdin EOF (quit), 1 otherwise. */
static int on_stdin(chat *c) {
    myio_result r = myio_join(c->io, c->tin);
    c->tin = NULL;
    if (!myio_ok(r) || r.value == 0) {
        fprintf(stderr, "* stdin closed, bye\n");
        return 0;
    }
    if (!c->peer) {
        fprintf(stderr, "* not connected, message dropped\n");
    } else {
        myio_result wr = myio_join(c->io, myio_sock_write(c->io, c->peer,
                                                          c->inbuf,
                                                          (size_t)r.value));
        if (!myio_ok(wr)) {
            fprintf(stderr, "* send failed: %s\n", strerror(wr.error));
            disconnect_peer(c);
            go_disconnected(c);
        }
    }
    c->tin = myio_read(c->io, 0, c->inbuf, sizeof c->inbuf, MYIO_NO_OFFSET);
    return 1;
}

static void on_net(chat *c) {
    myio_result r = myio_join(c->io, c->tnet);
    c->tnet = NULL;
    if (!myio_ok(r) || r.value == 0) {
        fprintf(stderr, "* peer disconnected\n");
        disconnect_peer(c);
        go_disconnected(c);
        return;
    }
    int n = snprintf(c->outbuf, sizeof c->outbuf, "peer> %.*s", (int)r.value,
                     c->netbuf);
    myio_join(c->io, myio_write(c->io, 1, c->outbuf, (size_t)n,
                                MYIO_NO_OFFSET));
    c->tnet = myio_sock_read(c->io, c->peer, c->netbuf, sizeof c->netbuf);
}

static void on_accept(chat *c) {
    myio_result r = myio_join(c->io, c->taccept);
    c->taccept = NULL;
    if (!myio_ok(r)) {
        fprintf(stderr, "* accept failed: %s\n", strerror(r.error));
        c->taccept = myio_tcp_accept(c->io, c->listener);
        return;
    }
    become_connected(c, r.ptr);
}

static void on_connect(chat *c) {
    myio_result r = myio_join(c->io, c->tconn);
    c->tconn = NULL;
    if (!myio_ok(r)) {
        fprintf(stderr,
                "* connect to %s:%d failed (%s), retrying in %llu ms\n",
                c->host, c->peer_port, strerror(r.error),
                (unsigned long long)retry_ms());
        c->tsleep = myio_sleep(c->io, retry_ms());
        return;
    }
    become_connected(c, r.ptr);
}

static void on_retry_timer(chat *c) {
    myio_join(c->io, c->tsleep);
    c->tsleep = NULL;
    c->tconn = myio_tcp_connect(c->io, c->host, c->peer_port);
}

int main(int argc, char **argv) {
    if (argc < 2 || argc > 4) {
        fprintf(stderr, "usage: %s <port> [host [peer-port]]\n", argv[0]);
        return 2;
    }
    chat c;
    memset(&c, 0, sizeof c);
    int port = atoi(argv[1]);
    c.host = argc > 2 ? argv[2] : NULL;
    c.peer_port = argc > 3 ? atoi(argv[3]) : port;

    /* Backend swap point: CHAT_BACKEND=xev runs the same program on libxev
     * instead of libuv. */
    const char *backend = getenv("CHAT_BACKEND");
    c.io = backend && strcmp(backend, "xev") == 0 ? myio_xev_new()
                                                  : myio_uv_new();
    if (!c.io) {
        fprintf(stderr, "* failed to create io backend\n");
        return 1;
    }
    int err = 0;
    c.listener = myio_tcp_listen(c.io, "0.0.0.0", port, 1, &err);
    if (!c.listener) {
        fprintf(stderr, "* cannot listen on port %d: %s\n", port,
                strerror(err));
        return 1;
    }
    fprintf(stderr, "* type messages; ctrl-d quits\n");
    c.tin = myio_read(c.io, 0, c.inbuf, sizeof c.inbuf, MYIO_NO_OFFSET);
    go_disconnected(&c);

    for (;;) {
        /* Fixed slots; whichever are NULL in the current state are simply
         * skipped by select. */
        enum { SLOT_STDIN, SLOT_NET, SLOT_ACCEPT, SLOT_CONN, SLOT_SLEEP,
               NSLOTS };
        myio_task *slots[NSLOTS] = { c.tin, c.tnet, c.taccept, c.tconn,
                                     c.tsleep };

        ptrdiff_t ready = myio_select(c.io, slots, NSLOTS);
        int quit = 0;
        switch (ready) {
        case SLOT_STDIN:  quit = !on_stdin(&c); break;
        case SLOT_NET:    on_net(&c);           break;
        case SLOT_ACCEPT: on_accept(&c);        break;
        case SLOT_CONN:   on_connect(&c);       break;
        case SLOT_SLEEP:  on_retry_timer(&c);   break;
        default:
            fprintf(stderr, "* select failed\n");
            quit = 1;
            break;
        }
        if (quit)
            break;
    }

    if (c.tin) {
        /* The pending stdin read is a blocking read() parked on the uv
         * thread pool; it cannot be canceled, and tearing down the loop
         * would wait for one more line of input. Let process exit reclaim
         * everything instead. */
        _exit(0);
    }
    drop_task(&c, &c.taccept);
    drop_task(&c, &c.tconn);
    drop_task(&c, &c.tsleep);
    disconnect_peer(&c);
    myio_join(c.io, myio_sock_close(c.io, c.listener));
    myio_destroy(c.io);
    return 0;
}
