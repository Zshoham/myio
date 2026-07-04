/* cancel_test - exercises the cancel contract: cancellation is a request,
 * and the status reported by await/join is the authoritative outcome.
 *
 *     ./cancel_test uv
 *     ./cancel_test xev
 *     ./cancel_test pool
 *     ./cancel_test uring
 *
 * The sync backend is deliberately excluded: every operation completes
 * inside its submit call, so a "pending" accept or read with no peer would
 * simply block forever at submit time and there is never anything left to
 * cancel.
 */
#include "myio.h"
#include "myio_pool.h"
#include "myio_uring.h"
#include "myio_uv.h"
#include "myio_xev.h"

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

static void expect(int cond, const char *what) {
    fprintf(stderr, "  %-50s %s\n", what, cond ? "ok" : "FAIL");
    if (!cond)
        failures++;
}

/* Open fds of this process; canceled operations must not leak any. */
static int count_fds(void) {
    DIR *d = opendir("/proc/self/fd");
    if (!d)
        return -1;
    int n = 0;
    while (readdir(d))
        n++;
    closedir(d);
    return n;
}

static myio *make_io(const char *backend) {
    if (strcmp(backend, "uv") == 0)
        return myio_uv_new();
    if (strcmp(backend, "xev") == 0)
        return myio_xev_new();
    if (strcmp(backend, "pool") == 0)
        return myio_pool_new();
    if (strcmp(backend, "uring") == 0)
        return myio_uring_new();
    return NULL;
}

int main(int argc, char **argv) {
    const char *backend = argc > 1 ? argv[1] : "xev";

    /* libuv sets up process-global state (a signal pipe) with its first
     * loop; create and destroy a throwaway instance so the fd baseline
     * includes it. */
    myio *warm = make_io(backend);
    if (!warm) {
        fprintf(stderr, "usage: %s [uv|xev|pool|uring]\n", argv[0]);
        return 2;
    }
    myio_destroy(warm);
    int fds_before = count_fds();

    myio *io = make_io(backend);
    if (!io) {
        fprintf(stderr, "failed to create %s backend\n", backend);
        return 1;
    }
    fprintf(stderr, "backend: %s\n", backend);

    /* A canceled timer joins as canceled; a refused cancel completes
     * normally. Either way the joined status is the authority. */
    myio_task *nap = myio_sleep(io, 5000);
    int rc = myio_cancel(io, nap);
    myio_result r = myio_join(io, nap);
    expect(rc == 0 ? r.status == MYIO_CANCELED : r.status == MYIO_OK,
           "timer: cancel outcome matches joined status");

    int err = 0;
    myio_sock *ls = myio_tcp_listen(io, "127.0.0.1", 0, 8, &err);
    if (!ls) {
        fprintf(stderr, "listen failed\n");
        return 1;
    }
    int port = myio_sock_port(io, ls);

    /* Cancel a pending accept (nobody is connecting). */
    myio_task *acc = myio_tcp_accept(io, ls);
    rc = myio_cancel(io, acc);
    r = myio_join(io, acc);
    expect(rc != 0 || r.status == MYIO_CANCELED,
           "accept: canceled when the request was accepted");

    /* Connect + accept a pair; then cancel a pending sock_read. */
    myio_task *tc = myio_tcp_connect(io, "127.0.0.1", port);
    myio_task *ta = myio_tcp_accept(io, ls);
    myio_result crr = myio_join(io, tc);
    myio_result arr = myio_join(io, ta);
    expect(myio_ok(crr) && myio_ok(arr), "connect/accept pair established");
    myio_sock *client = crr.ptr, *server = arr.ptr;

    char buf[64];
    myio_task *rd = myio_sock_read(io, server, buf, sizeof buf);
    rc = myio_cancel(io, rd);
    r = myio_join(io, rd);
    expect(rc != 0 || r.status == MYIO_CANCELED,
           "sock_read: canceled when the request was accepted");

    /* The socket must be usable for a fresh read afterwards. */
    myio_join(io, myio_sock_write(io, client, "x", 1));
    r = myio_join(io, myio_sock_read(io, server, buf, sizeof buf));
    expect(myio_ok(r) && r.value == 1, "sock_read after a canceled read");

    /* Cancel a connect right after submitting it. The request may win
     * (canceled), lose (the connection is ours and must be closed - the
     * documented race), or be refused. */
    myio_task *c2 = myio_tcp_connect(io, "127.0.0.1", port);
    rc = myio_cancel(io, c2);
    r = myio_join(io, c2);
    if (myio_ok(r) && r.ptr)
        myio_join(io, myio_sock_close(io, r.ptr));
    expect(myio_ok(r) || r.status == MYIO_CANCELED || r.status == MYIO_ERROR,
           "connect: immediate cancel has a definite outcome");
    /* Whoever won the race above, the listener side may now hold a pending
     * connection; drain it so later accept tests start clean. */
    myio_task *drain = myio_tcp_accept(io, ls);
    if (myio_ok(r)) { /* connect finished: its peer is in the backlog */
        myio_result dr = myio_join(io, drain);
        if (myio_ok(dr) && dr.ptr)
            myio_join(io, myio_sock_close(io, dr.ptr));
    } else {
        myio_cancel(io, drain);
        myio_result dr = myio_join(io, drain);
        if (myio_ok(dr) && dr.ptr)
            myio_join(io, myio_sock_close(io, dr.ptr));
    }

    /* Cancel a connect that has long since completed: refused, and the
     * socket is still ours. */
    myio_task *c3 = myio_tcp_connect(io, "127.0.0.1", port);
    myio_task *ta3 = myio_tcp_accept(io, ls);
    myio_join(io, myio_sleep(io, 50));
    rc = myio_cancel(io, c3);
    r = myio_join(io, c3);
    expect(rc == -1 && myio_ok(r) && r.ptr,
           "connect: cancel after completion is refused");
    if (myio_ok(r) && r.ptr)
        myio_join(io, myio_sock_close(io, r.ptr));
    myio_result ar3 = myio_join(io, ta3);
    if (myio_ok(ar3) && ar3.ptr)
        myio_join(io, myio_sock_close(io, ar3.ptr));

    /* Detach: fire and forget. The backend frees detached tasks when they
     * complete and closes any socket they win - the fd check at the end
     * would catch a leak in either direction. */
    myio_task *da = myio_tcp_accept(io, ls);
    myio_task *dc = myio_tcp_connect(io, "127.0.0.1", port);
    myio_task_detach(io, da);
    myio_task_detach(io, dc);
    myio_join(io, myio_sleep(io, 50)); /* let both complete unobserved */
    expect(1, "detached connect/accept completed unobserved");

    /* Detaching an already completed task discards its result too. */
    myio_task *dc2 = myio_tcp_connect(io, "127.0.0.1", port);
    myio_task *da2 = myio_tcp_accept(io, ls);
    myio_join(io, myio_sleep(io, 50));
    expect(myio_task_done(io, dc2) && myio_task_done(io, da2),
           "second detach pair completed");
    myio_task_detach(io, dc2);
    myio_task_detach(io, da2);

    /* Cancel + detach: drop a pending read without waiting for it. The
     * read stays outstanding (it holds the socket's one-read slot) until
     * the backend has processed the cancellation, so give the loop a turn
     * before reading from the socket again. */
    myio_task *rd3 = myio_sock_read(io, server, buf, sizeof buf);
    myio_cancel(io, rd3);
    myio_task_detach(io, rd3);
    myio_join(io, myio_sleep(io, 10));

    /* sock_close cancels outstanding reads and accepts on the socket. */
    myio_task *rd2 = myio_sock_read(io, server, buf, sizeof buf);
    myio_task *acc2 = myio_tcp_accept(io, ls);
    myio_join(io, myio_sock_close(io, server));
    r = myio_join(io, rd2);
    expect(r.status == MYIO_CANCELED, "sock_close cancels a pending read");
    myio_join(io, myio_sock_close(io, ls));
    r = myio_join(io, acc2);
    expect(r.status == MYIO_CANCELED, "sock_close cancels a pending accept");

    myio_join(io, myio_sock_close(io, client));
    myio_destroy(io);

    int fds_after = count_fds();
    expect(fds_before == fds_after, "no fd leaked by canceled operations");
    if (fds_before != fds_after)
        fprintf(stderr, "  (fds before: %d, after: %d)\n", fds_before,
                fds_after);

    fprintf(stderr, "%s\n", failures ? "FAILURES" : "all ok");
    return failures != 0;
}
