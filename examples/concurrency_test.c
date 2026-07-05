/* concurrency_test - exercises paths the demo's tidy submit/await ordering
 * never reaches: many operations genuinely in flight at once, and a socket
 * torn down while an operation is still blocked on it.
 *
 *     ./concurrency_test uv
 *     ./concurrency_test xev
 *     ./concurrency_test pool
 *     ./concurrency_test uring
 *
 * The sync backend is excluded for the same reason as cancel_test: every
 * operation completes inside its submit call, so a read with no data waiting
 * would simply block forever at submit time - there would be nothing in
 * flight to close out from under.
 */
#include "myio.h"
#include "myio_pool.h"
#include "myio_uring.h"
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
    if (strcmp(backend, "uring") == 0)
        return myio_uring_new();
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

    /* Capabilities are a static property of the backend's execution model;
     * assert the bits each backend must expose. Only bits that are
     * unconditional for the backend are checked: xev's CANCEL_FILE depends on
     * which libxev backend libxev picks at runtime (io_uring vs epoll), so it
     * is deliberately left out here. */
    unsigned caps = myio_caps(io);
    if (strcmp(backend, "uv") == 0 || strcmp(backend, "pool") == 0 ||
        strcmp(backend, "xev") == 0) {
        expect((caps & MYIO_CAP_CONCURRENT_WAIT) &&
                   (caps & MYIO_CAP_NONBLOCKING_SUBMIT) &&
                   (caps & MYIO_CAP_ASYNC_SPAWN),
               "caps: concurrent-wait, nonblocking-submit, async-spawn set");
    } else if (strcmp(backend, "uring") == 0) {
        expect((caps & MYIO_CAP_CONCURRENT_WAIT) &&
                   (caps & MYIO_CAP_CANCEL_FILE) &&
                   !(caps & MYIO_CAP_NONBLOCKING_SUBMIT) &&
                   !(caps & MYIO_CAP_ASYNC_SPAWN),
               "caps: concurrent-wait + cancel-file only (inline DNS/spawn)");
    }

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

    /* Close a socket while a write on it is still blocked: write far more
     * than the kernel will buffer to a peer that never reads, close the
     * writer, and join the write - it must come back promptly as CANCELED
     * (or ERROR), never hang. Exercises the backend's duty to cancel
     * outstanding writes on close (an in-flight send otherwise keeps the
     * task pending forever, and the join deadlocks). */
    {
        myio_result c2 = myio_join(io, myio_tcp_connect(io, "127.0.0.1", port));
        myio_result a2 = myio_join(io, myio_tcp_accept(io, ls));
        expect(myio_ok(c2) && myio_ok(a2), "writer/peer pair established");
        myio_sock *wsock = c2.ptr, *peer = a2.ptr;
        size_t biglen = 8u << 20; /* 8 MB: far beyond socket buffering */
        char *big = malloc(biglen);
        memset(big, 'x', biglen);
        myio_task *w = myio_sock_write(io, wsock, big, biglen);
        myio_join(io, myio_sleep(io, 20)); /* let the send fill the buffers */
        myio_task *wcl = myio_sock_close(io, wsock);
        myio_result wres = myio_join(io, w); /* hangs if close skips writes */
        expect(wres.status == MYIO_CANCELED || wres.status == MYIO_ERROR,
               "sock_close cancels a blocked write (no hang)");
        myio_join(io, wcl);
        myio_join(io, myio_sock_close(io, peer));
        free(big);
    }

    /* Overlapping writes on one socket must land in submission order with
     * no interleaving: three patterned buffers, submitted back to back and
     * big enough that they are genuinely in flight together, must arrive as
     * their exact concatenation. Every backend serialises writes per socket
     * (event loops naturally, the thread pools through a per-socket write
     * FIFO), so this runs on all of them. */
    {
        myio_result c3 = myio_join(io, myio_tcp_connect(io, "127.0.0.1", port));
        myio_result a3 = myio_join(io, myio_tcp_accept(io, ls));
        expect(myio_ok(c3) && myio_ok(a3), "ordering pair established");
        myio_sock *tx = c3.ptr, *rx = a3.ptr;
        const size_t wsz[3] = { 4u << 20, 1u << 20, 2u << 20 };
        size_t total = wsz[0] + wsz[1] + wsz[2];
        char *wb[3];
        for (int i = 0; i < 3; i++) {
            wb[i] = malloc(wsz[i]);
            memset(wb[i], 'A' + i, wsz[i]);
        }
        char *rbuf = malloc(total);
        myio_task *w[3];
        for (int i = 0; i < 3; i++)
            w[i] = myio_sock_write(io, tx, wb[i], wsz[i]);
        size_t got = 0;
        while (got < total) {
            myio_result r =
                myio_join(io, myio_sock_read(io, rx, rbuf + got, total - got));
            if (!myio_ok(r) || r.value == 0)
                break;
            got += (size_t)r.value;
        }
        int writes_ok = 1;
        for (int i = 0; i < 3; i++) {
            myio_result r = myio_join(io, w[i]);
            if (!myio_ok(r) || r.value != (int64_t)wsz[i])
                writes_ok = 0;
        }
        expect(writes_ok, "3 overlapping writes each completed in full");
        int order_ok = got == total;
        size_t off = 0;
        for (int i = 0; i < 3 && order_ok; i++) {
            for (size_t j = 0; j < wsz[i]; j++)
                if (rbuf[off + j] != 'A' + i) {
                    order_ok = 0;
                    break;
                }
            off += wsz[i];
        }
        expect(order_ok, "overlapping writes arrive in order, uninterleaved");
        myio_join(io, myio_sock_close(io, tx));
        myio_join(io, myio_sock_close(io, rx));
        for (int i = 0; i < 3; i++)
            free(wb[i]);
        free(rbuf);
    }

    /* DNS resolution failures must surface as a negative result.error - the
     * platform's getaddrinfo EAI_* code, verbatim (see myio.h) - not some
     * backend-private range. "nonexistent.invalid" is reserved by RFC 6761
     * and never resolves, but which EAI_* code a resolver reports for it
     * varies (EAI_NONAME on some, EAI_AGAIN or EAI_NODATA on others), so
     * only the sign and the rendered message are checked here, not the
     * specific code. */
    {
        myio_result dr =
            myio_join(io, myio_tcp_connect(io, "nonexistent.invalid", 80));
        expect(dr.status == MYIO_ERROR, "DNS failure completes as MYIO_ERROR");
        expect(dr.error < 0, "DNS failure stores a negative EAI_* code");
        const char *msg = myio_strerror(io, dr.error);
        expect(msg && *msg, "myio_strerror renders the DNS failure message");
    }

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
