/* chat_uv.c - the persistent peer-to-peer chat from chat.c written directly
 * against raw libuv, for comparison with the myio version.
 *
 * usage: ./chat_uv <port> [host [peer-port]]
 *
 * Same behavior as examples/chat.c: always listens on <port>, dials
 * host:peer-port when a host is given (retrying every 10 seconds, override
 * with CHAT_RETRY_MS), returns to waiting when the peer leaves, ctrl-d
 * quits.
 *
 * Where the myio version is a straight-line select loop, raw libuv inverts
 * the control flow into callbacks: every step (resolve, connect, accept,
 * read, write, retry timer) is a separate callback mutating shared state,
 * and every handle needs explicit close choreography before its memory can
 * be reclaimed.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#define BUFSZ 512

static struct {
    uv_loop_t       *loop;
    uv_tcp_t         listener;
    uv_tcp_t        *peer;     /* NULL while disconnected */
    uv_tcp_t        *dial;     /* in-flight outbound attempt */
    uv_getaddrinfo_t gai;
    uv_connect_t     conn;
    uv_timer_t       retry;
    union { uv_tty_t tty; uv_pipe_t pipe; } in;
    uv_stream_t     *stdin_stream;
    const char      *host;     /* NULL: accept-only */
    char             peer_port[16];
    int              listen_port;
    int              dialing;
    int              quitting;
    char             stdin_buf[BUFSZ];
    char             peer_buf[BUFSZ];
} G;

static void start_dial(void);

static uint64_t retry_ms(void) {
    const char *e = getenv("CHAT_RETRY_MS"); /* test hook */
    return e ? (uint64_t)atoi(e) : 10000;
}

static void waiting_msg(void) {
    fprintf(stderr, "* waiting for a peer on port %d%s\n", G.listen_port,
            G.host ? " (and dialing out)" : "");
}

static void free_handle_cb(uv_handle_t *h) {
    free(h);
}

/* ---- sending ---- */

typedef struct {
    uv_write_t req;
    char       data[];
} write_req;

static void write_done(uv_write_t *req, int status) {
    if (status < 0)
        fprintf(stderr, "* send failed (%s)\n", uv_strerror(status));
    free(req);
}

static void send_peer(const char *data, size_t len) {
    write_req *w = malloc(sizeof *w + len);
    if (!w)
        return;
    memcpy(w->data, data, len);
    uv_buf_t b = uv_buf_init(w->data, (unsigned)len);
    uv_write(&w->req, (uv_stream_t *)G.peer, &b, 1, write_done);
}

/* ---- peer connection ---- */

static void peer_alloc(uv_handle_t *h, size_t suggested, uv_buf_t *b) {
    (void)h;
    (void)suggested;
    *b = uv_buf_init(G.peer_buf, BUFSZ);
}

static void on_peer_read(uv_stream_t *s, ssize_t n, const uv_buf_t *b) {
    (void)s;
    if (n == 0)
        return;
    if (n < 0) {
        fprintf(stderr, "* peer disconnected\n");
        uv_close((uv_handle_t *)G.peer, free_handle_cb);
        G.peer = NULL;
        if (!G.quitting) {
            waiting_msg();
            start_dial();
        }
        return;
    }
    printf("peer> %.*s", (int)n, b->base);
    fflush(stdout);
}

static void adopt_peer(uv_tcp_t *t) {
    G.peer = t;
    uv_timer_stop(&G.retry);
    uv_read_start((uv_stream_t *)t, peer_alloc, on_peer_read);
    fprintf(stderr, "* peer connected\n");
}

/* ---- inbound: listener ---- */

static void on_connection(uv_stream_t *server, int status) {
    if (status < 0)
        return;
    uv_tcp_t *c = malloc(sizeof *c);
    if (!c)
        return;
    uv_tcp_init(G.loop, c);
    if (uv_accept(server, (uv_stream_t *)c) != 0) {
        uv_close((uv_handle_t *)c, free_handle_cb);
        return;
    }
    if (G.peer || G.quitting) { /* one peer at a time: turn extras away */
        uv_close((uv_handle_t *)c, free_handle_cb);
        return;
    }
    adopt_peer(c);
}

/* ---- outbound: resolve + connect + retry timer ---- */

static void retry_cb(uv_timer_t *t) {
    (void)t;
    start_dial();
}

static void schedule_retry(void) {
    if (!G.quitting && !G.peer)
        uv_timer_start(&G.retry, retry_cb, retry_ms(), 0);
}

static void dial_failed(int status) {
    fprintf(stderr, "* connect to %s:%s failed (%s), retrying in %llu ms\n",
            G.host, G.peer_port, uv_strerror(status),
            (unsigned long long)retry_ms());
    schedule_retry();
}

static void on_connect(uv_connect_t *req, int status) {
    (void)req;
    uv_tcp_t *t = G.dial;
    G.dial = NULL;
    G.dialing = 0;
    if (status < 0 || G.peer || G.quitting) {
        uv_close((uv_handle_t *)t, free_handle_cb);
        if (status < 0 && !G.peer && !G.quitting)
            dial_failed(status);
        return;
    }
    adopt_peer(t);
}

static void on_resolved(uv_getaddrinfo_t *req, int status,
                        struct addrinfo *res) {
    (void)req;
    if (status < 0 || G.peer || G.quitting) {
        G.dialing = 0;
        uv_freeaddrinfo(res);
        if (status < 0 && !G.peer && !G.quitting)
            dial_failed(status);
        return;
    }
    G.dial = malloc(sizeof *G.dial);
    uv_tcp_init(G.loop, G.dial);
    int rc = uv_tcp_connect(&G.conn, G.dial, res->ai_addr, on_connect);
    uv_freeaddrinfo(res);
    if (rc < 0) {
        uv_close((uv_handle_t *)G.dial, free_handle_cb);
        G.dial = NULL;
        G.dialing = 0;
        dial_failed(rc);
    }
}

static void start_dial(void) {
    if (!G.host || G.dialing || G.peer || G.quitting)
        return;
    G.dialing = 1;
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_socktype = SOCK_STREAM;
    int rc = uv_getaddrinfo(G.loop, &G.gai, on_resolved, G.host, G.peer_port,
                            &hints);
    if (rc < 0) {
        G.dialing = 0;
        dial_failed(rc);
    }
}

/* ---- stdin ---- */

static void stdin_alloc(uv_handle_t *h, size_t suggested, uv_buf_t *b) {
    (void)h;
    (void)suggested;
    *b = uv_buf_init(G.stdin_buf, BUFSZ);
}

static void on_stdin(uv_stream_t *s, ssize_t n, const uv_buf_t *b) {
    (void)s;
    if (n == 0)
        return;
    if (n < 0) {
        fprintf(stderr, "* stdin closed, bye\n");
        G.quitting = 1;
        uv_close((uv_handle_t *)G.stdin_stream, NULL);
        uv_close((uv_handle_t *)&G.listener, NULL);
        uv_close((uv_handle_t *)&G.retry, NULL);
        if (G.peer) {
            uv_close((uv_handle_t *)G.peer, free_handle_cb);
            G.peer = NULL;
        }
        /* a pending dial closes itself in on_connect/on_resolved */
        return;
    }
    if (!G.peer) {
        fprintf(stderr, "* not connected, message dropped\n");
        return;
    }
    send_peer(b->base, (size_t)n);
}

int main(int argc, char **argv) {
    if (argc < 2 || argc > 4) {
        fprintf(stderr, "usage: %s <port> [host [peer-port]]\n", argv[0]);
        return 2;
    }
    G.listen_port = atoi(argv[1]);
    G.host = argc > 2 ? argv[2] : NULL;
    snprintf(G.peer_port, sizeof G.peer_port, "%s",
             argc > 3 ? argv[3] : argv[1]);

    G.loop = uv_default_loop();
    uv_timer_init(G.loop, &G.retry);

    struct sockaddr_in addr;
    uv_ip4_addr("0.0.0.0", G.listen_port, &addr);
    uv_tcp_init(G.loop, &G.listener);
    int rc = uv_tcp_bind(&G.listener, (struct sockaddr *)&addr, 0);
    if (rc == 0)
        rc = uv_listen((uv_stream_t *)&G.listener, 1, on_connection);
    if (rc < 0) {
        fprintf(stderr, "* cannot listen on port %d: %s\n", G.listen_port,
                uv_strerror(rc));
        return 1;
    }

    switch (uv_guess_handle(0)) {
    case UV_TTY:
        uv_tty_init(G.loop, &G.in.tty, 0, 1);
        G.stdin_stream = (uv_stream_t *)&G.in.tty;
        break;
    case UV_NAMED_PIPE:
        uv_pipe_init(G.loop, &G.in.pipe, 0);
        uv_pipe_open(&G.in.pipe, 0);
        G.stdin_stream = (uv_stream_t *)&G.in.pipe;
        break;
    default:
        fprintf(stderr, "* unsupported stdin type\n");
        return 1;
    }
    uv_read_start(G.stdin_stream, stdin_alloc, on_stdin);

    fprintf(stderr, "* type messages; ctrl-d quits\n");
    waiting_msg();
    start_dial();

    uv_run(G.loop, UV_RUN_DEFAULT);
    uv_loop_close(G.loop);
    return 0;
}
