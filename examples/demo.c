/* Demo of the myio interface. The same program logic runs unchanged on the
 * libuv backend, the libxev backend, and the blocking synchronous backend:
 *
 *     ./demo uv
 *     ./demo xev
 *     ./demo sync
 */
#include "myio.h"
#include "myio_sync.h"
#include "myio_uv.h"
#include "myio_xev.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Join a task that must succeed and return its result.value. */
static int64_t must(myio *io, myio_task *t, const char *what) {
    myio_result r = myio_join(io, t);
    if (!myio_ok(r)) {
        fprintf(stderr, "%s failed: status=%s error=%s\n", what,
                myio_status_str(r.status), strerror(r.error));
        exit(1);
    }
    return r.value;
}

/* An arbitrary function to run through myio_spawn(). */
static int64_t count_vowels(void *arg) {
    int64_t n = 0;
    for (const char *s = arg; *s; s++)
        if (strchr("aeiou", *s))
            n++;
    return n;
}

int main(int argc, char **argv) {
    const char *backend = argc > 1 ? argv[1] : "uv";
    myio *io;
    if (strcmp(backend, "uv") == 0)
        io = myio_uv_new();
    else if (strcmp(backend, "xev") == 0)
        io = myio_xev_new();
    else if (strcmp(backend, "sync") == 0)
        io = myio_sync_new();
    else {
        fprintf(stderr, "usage: %s [uv|xev|sync]\n", argv[0]);
        return 2;
    }
    if (!io) {
        fprintf(stderr, "failed to create %s backend\n", backend);
        return 1;
    }
    printf("backend: %s\n", backend);

    /* 1. Write two files concurrently: submit both writes, then await the
     *    batch. On the uv backend they run in parallel on the thread pool;
     *    on the sync backend each completes during submission. */
    static const char msg1[] = "hello from myio, file one\n";
    static const char msg2[] = "hello from myio, file two\n";
    int64_t fd1 = must(io, myio_open(io, "demo1.tmp",
                                     O_CREAT | O_TRUNC | O_WRONLY, 0644),
                       "open demo1.tmp");
    int64_t fd2 = must(io, myio_open(io, "demo2.tmp",
                                     O_CREAT | O_TRUNC | O_WRONLY, 0644),
                       "open demo2.tmp");
    myio_task *writes[2] = {
        myio_write(io, fd1, msg1, sizeof msg1 - 1, 0),
        myio_write(io, fd2, msg2, sizeof msg2 - 1, 0),
    };
    if (myio_await_all(io, writes, 2) != 0) {
        fprintf(stderr, "a write failed\n");
        exit(1);
    }
    printf("concurrent writes: %lld + %lld bytes\n",
           (long long)must(io, writes[0], "write demo1.tmp"),
           (long long)must(io, writes[1], "write demo2.tmp"));
    must(io, myio_close(io, fd1), "close demo1.tmp");
    must(io, myio_close(io, fd2), "close demo2.tmp");

    /* 2. Read with a 500 ms timeout. The read should finish in time; if it
     *    did not, the result would come back MYIO_PENDING and the task
     *    would still be ours to cancel. */
    char buf[128];
    int64_t rfd = must(io, myio_open(io, "demo1.tmp", O_RDONLY, 0),
                       "open demo1.tmp for read");
    myio_task *rt = myio_read(io, rfd, buf, sizeof buf, 0);
    myio_result r = myio_await_timeout(io, rt, 500);
    if (r.status == MYIO_PENDING) {
        printf("read timed out\n");
        myio_cancel(io, rt);
    } else {
        printf("read in time: %lld bytes: %.*s", (long long)r.value,
               (int)r.value, buf);
    }
    myio_task_free(io, rt);
    must(io, myio_close(io, rfd), "close read fd");

    /* 3. Cancellation: start a long sleep and cancel it right away. Cancel
     *    is only a request - 0 means it was accepted, and the joined status
     *    is the authoritative outcome. The uv and xev backends stop the
     *    timer, so it joins as canceled; the sync backend has already slept
     *    by the time we get the handle back, so cancel reports -1 and the
     *    task completed normally. (This is also why the sync demo pauses
     *    ~300 ms here.) */
    myio_task *nap = myio_sleep(io, 300);
    int canceled = myio_cancel(io, nap);
    r = myio_join(io, nap);
    printf("cancel long sleep: cancel()=%d, final status=%s\n", canceled,
           myio_status_str(r.status));

    /* 4. Spawn an arbitrary function as a task: the escape hatch for work
     *    the built-in operations don't cover. */
    myio_task *bg = myio_spawn(io, count_vowels, (void *)(uintptr_t)msg1);
    printf("spawned count_vowels: %lld vowels\n",
           (long long)must(io, bg, "spawn count_vowels"));

    /* 5. TCP echo, client and server in the same thread. The connect is
     *    submitted before the accept on purpose: on the sync backend each
     *    op completes inside its submit call, so the accept must already
     *    have a pending connection (in the kernel backlog) to pick up. */
    int lerr = 0;
    myio_sock *ls = myio_tcp_listen(io, "127.0.0.1", 0, 16, &lerr);
    if (!ls) {
        fprintf(stderr, "listen failed: %s\n", strerror(lerr));
        exit(1);
    }
    int port = myio_sock_port(io, ls);
    printf("listening on 127.0.0.1:%d\n", port);

    myio_result cr = myio_join(io, myio_tcp_connect(io, "127.0.0.1", port));
    myio_result ar = myio_join(io, myio_tcp_accept(io, ls));
    if (!myio_ok(cr) || !myio_ok(ar)) {
        fprintf(stderr, "connect/accept failed: %s / %s\n",
                myio_status_str(cr.status), myio_status_str(ar.status));
        exit(1);
    }
    myio_sock *client = cr.ptr;
    myio_sock *server = ar.ptr;

    char sbuf[16], cbuf[16];
    must(io, myio_sock_write(io, client, "ping", 4), "client write");
    int64_t n = must(io, myio_sock_read(io, server, sbuf, sizeof sbuf),
                     "server read");
    printf("server received: %.*s\n", (int)n, sbuf);
    must(io, myio_sock_write(io, server, "pong", 4), "server write");
    n = must(io, myio_sock_read(io, client, cbuf, sizeof cbuf), "client read");
    printf("client received: %.*s\n", (int)n, cbuf);

    /* Closing the client makes the next server read report EOF (value 0). */
    must(io, myio_sock_close(io, client), "close client");
    n = must(io, myio_sock_read(io, server, sbuf, sizeof sbuf),
             "server read after peer close");
    printf("server read after peer close: %lld bytes (EOF)\n", (long long)n);
    must(io, myio_sock_close(io, server), "close server conn");
    must(io, myio_sock_close(io, ls), "close listener");

    remove("demo1.tmp");
    remove("demo2.tmp");
    myio_destroy(io);
    printf("done\n");
    return 0;
}
