/* concurrency_test - exercises paths the demo's tidy submit/await ordering
 * never reaches: many operations genuinely in flight at once, and a socket
 * torn down while an operation is still blocked on it.
 *
 *     ./concurrency_test uv
 *     ./concurrency_test xev
 *     ./concurrency_test pool
 *
 * The sync backend is excluded for the same reason as cancel_test: every
 * operation completes inside its submit call, so a read with no data waiting
 * would simply block forever at submit time - there would be nothing in
 * flight to close out from under.
 */
#include "myio.h"
#include "myio_pool.h"
#include "myio_uv.h"
#include "myio_xev.h"

#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

static void expect(int cond, const char *what) {
    fprintf(stderr, "  %-55s %s\n", what, cond ? "ok" : "FAIL");
    if (!cond)
        failures++;
}

/* Open fds of this process; teardown must not leak any. */
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
    return NULL;
}

/* A spin of work, so backends with a worker pool actually run these in
 * parallel rather than instantly. Returns a value derived from the input. */
static int64_t spin(void *arg) {
    long iters = (long)(uintptr_t)arg;
    volatile long n = 0;
    for (long i = 0; i < iters; i++)
        n += i;
    return (int64_t)(n & 0x7fff);
}

#define NSPAWN 64

int main(int argc, char **argv) {
    const char *backend = argc > 1 ? argv[1] : "pool";

    /* libuv installs process-global state (a signal pipe) with its first
     * loop; warm it up so the fd baseline includes it. */
    myio *warm = make_io(backend);
    if (!warm) {
        fprintf(stderr, "usage: %s [uv|xev|pool]\n", argv[0]);
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

    /* Submit many tasks before awaiting any: real concurrency, and on a
     * thread pool this forces it to grow a worker per blocked slot. */
    myio_task *spawns[NSPAWN];
    for (int i = 0; i < NSPAWN; i++)
        spawns[i] = myio_spawn(io, spin, (void *)(uintptr_t)2000000);
    int all_ok = 1;
    for (int i = 0; i < NSPAWN; i++)
        if (!myio_ok(myio_join(io, spawns[i])))
            all_ok = 0;
    expect(all_ok, "64 concurrent spawns all completed");

    int err = 0;
    myio_sock *ls = myio_tcp_listen(io, "127.0.0.1", 0, 8, &err);
    if (!ls) {
        fprintf(stderr, "listen failed\n");
        return 1;
    }
    int port = myio_sock_port(io, ls);

    myio_result cr = myio_join(io, myio_tcp_connect(io, "127.0.0.1", port));
    myio_result ar = myio_join(io, myio_tcp_accept(io, ls));
    expect(myio_ok(cr) && myio_ok(ar), "connect/accept pair established");
    myio_sock *client = cr.ptr, *server = ar.ptr;

    /* Close a socket while a read on it is still blocked (no data is coming).
     * The close has to interrupt the read, which then joins as CANCELED. */
    char buf[64];
    myio_task *rd = myio_sock_read(io, server, buf, sizeof buf);
    myio_join(io, myio_sleep(io, 20)); /* let the read settle into a wait */
    myio_task *cl = myio_sock_close(io, server);
    myio_result rr = myio_join(io, rd);
    myio_join(io, cl);
    expect(rr.status == MYIO_CANCELED,
           "sock_close interrupts a blocked read (CANCELED)");

    /* Detach a still-blocked read and let destroy reclaim it: the backend
     * must stop the operation and free everything without hanging. */
    myio_task *orphan = myio_sock_read(io, client, buf, sizeof buf);
    myio_join(io, myio_sleep(io, 20));
    myio_cancel(io, orphan);      /* may be refused once it is running */
    myio_task_detach(io, orphan); /* fire and forget */

    myio_join(io, myio_sock_close(io, client));
    myio_join(io, myio_sock_close(io, ls));
    myio_destroy(io);
    expect(1, "destroy reclaimed a detached blocked read without hanging");

    int fds_after = count_fds();
    expect(fds_before == fds_after, "no fd leaked by socket teardown");
    if (fds_before != fds_after)
        fprintf(stderr, "  (fds before: %d, after: %d)\n", fds_before,
                fds_after);

    fprintf(stderr, "%s\n", failures ? "FAILURES" : "all ok");
    return failures != 0;
}
