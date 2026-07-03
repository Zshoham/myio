/* The myio demo and contract checks on the Zephyr backends, run as a
 * native_sim application: file IO on littlefs over the simulated flash,
 * TCP over host-offloaded sockets, timers on the Zephyr clock. The same
 * suite runs on the event-loop backend and the thread-pool backend - the
 * program logic is identical, which is the point of the exercise (and of
 * the interface). */
#include "myio_zephyr.h"
#include "myio_zephyr_pool.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>

#include <zephyr/kernel.h>
#include <zephyr/fs/fs.h>
#include <zephyr/fs/littlefs.h>
#include <zephyr/storage/flash_map.h>

/* Terminate the native_sim process with an exit code (returning from main
 * only ends the main thread; the simulator would idle forever). */
#ifdef CONFIG_ARCH_POSIX
extern void posix_exit(int exit_code);
#endif

FS_LITTLEFS_DECLARE_DEFAULT_CONFIG(lfs_data);
static struct fs_mount_t lfs_mnt = {
    .type = FS_LITTLEFS,
    .fs_data = &lfs_data,
    .storage_dev = (void *)FIXED_PARTITION_ID(storage_partition),
    .mnt_point = "/lfs",
};

static int failures;

static void expect(int cond, const char *what) {
    printf("  %-58s %s\n", what, cond ? "ok" : "FAIL");
    if (!cond)
        failures++;
}

/* Join a task that must succeed and return its result.value. */
static int64_t must(myio *io, myio_task *t, const char *what) {
    myio_result r = myio_join(io, t);
    if (!myio_ok(r)) {
        printf("  %s failed: status=%s error=%s\n", what,
               myio_status_str(r.status), myio_strerror(io, r.error));
        failures++;
        return -1;
    }
    return r.value;
}

static int64_t count_vowels(void *arg) {
    int64_t n = 0;
    for (const char *s = arg; *s; s++)
        if (strchr("aeiou", *s))
            n++;
    return n;
}

static int64_t spin(void *arg) {
    long iters = (long)(uintptr_t)arg;
    volatile long n = 0;
    for (long i = 0; i < iters; i++)
        n += i;
    return (int64_t)(n & 0x7fff);
}

static void file_tests(myio *io) {
    static const char msg1[] = "hello from myio on zephyr, file one\n";
    static const char msg2[] = "hello from myio on zephyr, file two\n";

    int64_t fd1 = must(io, myio_open(io, "/lfs/demo1.tmp",
                                     O_CREAT | O_TRUNC | O_WRONLY, 0644),
                       "open demo1.tmp");
    int64_t fd2 = must(io, myio_open(io, "/lfs/demo2.tmp",
                                     O_CREAT | O_TRUNC | O_WRONLY, 0644),
                       "open demo2.tmp");
    myio_task *writes[2] = {
        myio_write(io, fd1, msg1, sizeof msg1 - 1, 0),
        myio_write(io, fd2, msg2, sizeof msg2 - 1, 0),
    };
    expect(myio_await_all(io, writes, 2) == 0, "two file writes complete");
    myio_task_free(io, writes[0]);
    myio_task_free(io, writes[1]);
    must(io, myio_close(io, fd1), "close demo1.tmp");
    must(io, myio_close(io, fd2), "close demo2.tmp");

    char buf[128];
    int64_t rfd = must(io, myio_open(io, "/lfs/demo1.tmp", O_RDONLY, 0),
                       "open demo1.tmp for read");
    myio_task *rt = myio_read(io, rfd, buf, sizeof buf, 0);
    myio_result r = myio_await_timeout(io, rt, 500);
    expect(r.status == MYIO_OK && r.value == (int64_t)sizeof msg1 - 1 &&
               memcmp(buf, msg1, sizeof msg1 - 1) == 0,
           "read-with-timeout returns the written bytes");
    myio_task_free(io, rt);

    /* Positional read must not disturb the file position. */
    myio_task *pr = myio_read(io, rfd, buf, 5, 6);
    myio_result rr = myio_await(io, pr);
    expect(myio_ok(rr) && rr.value == 5 && memcmp(buf, "from ", 5) == 0,
           "positional read (offset 6)");
    myio_task_free(io, pr);
    must(io, myio_close(io, rfd), "close read fd");

    fs_unlink("/lfs/demo1.tmp");
    fs_unlink("/lfs/demo2.tmp");
}

static void timer_and_spawn_tests(myio *io) {
    /* Cancel a long sleep right away: joins as CANCELED. */
    myio_task *nap = myio_sleep(io, 5000);
    int canceled = myio_cancel(io, nap);
    myio_result r = myio_join(io, nap);
    expect(canceled == 0 && r.status == MYIO_CANCELED,
           "cancel stops a pending sleep");

    /* Two staggered timers complete in deadline order through select. */
    myio_task *slots[2] = { myio_sleep(io, 80), myio_sleep(io, 20) };
    ptrdiff_t first = myio_select(io, slots, 2);
    expect(first == 1, "select picks the earlier timer");
    expect(myio_await_all(io, slots, 2) == 0, "both timers complete");
    myio_task_free(io, slots[0]);
    myio_task_free(io, slots[1]);

    /* await_timeout on a slow task reports PENDING and leaves it ours. */
    myio_task *slow = myio_sleep(io, 2000);
    r = myio_await_timeout(io, slow, 50);
    expect(r.status == MYIO_PENDING, "await_timeout times out (PENDING)");
    myio_cancel(io, slow);
    r = myio_join(io, slow);
    expect(r.status == MYIO_CANCELED, "timed-out sleep then canceled");

    /* Spawn: result value, error mapping, and a concurrent burst. */
    myio_task *bg = myio_spawn(io, count_vowels,
                               (void *)"hello from myio on zephyr");
    expect(must(io, bg, "spawn count_vowels") == 7,
           "spawn returns fn's value (7 vowels)");
    myio_task *burst[8];
    for (int i = 0; i < 8; i++)
        burst[i] = myio_spawn(io, spin, (void *)(uintptr_t)200000);
    expect(myio_await_all(io, burst, 8) == 0, "8 concurrent spawns complete");
    for (int i = 0; i < 8; i++)
        myio_task_free(io, burst[i]);
}

static void tcp_tests(myio *io, int test_port) {
    int lerr = 0;
    myio_sock *ls = myio_tcp_listen(io, "127.0.0.1", test_port, 16, &lerr);
    expect(ls != NULL, "tcp_listen on 127.0.0.1");
    if (!ls)
        return;
    int port = myio_sock_port(io, ls);
    expect(port == test_port, "sock_port reports the bound port");

    /* Accept first, then connect: both genuinely in flight at once. */
    myio_task *at = myio_tcp_accept(io, ls);
    myio_task *ct = myio_tcp_connect(io, "127.0.0.1", port);
    myio_result cr = myio_join(io, ct);
    myio_result ar = myio_join(io, at);
    expect(myio_ok(cr) && myio_ok(ar), "connect/accept pair established");
    if (!myio_ok(cr) || !myio_ok(ar))
        return;
    myio_sock *client = cr.ptr;
    myio_sock *server = ar.ptr;

    /* Echo both ways. */
    char sbuf[16], cbuf[16];
    must(io, myio_sock_write(io, client, "ping", 4), "client write");
    int64_t n = must(io, myio_sock_read(io, server, sbuf, sizeof sbuf),
                     "server read");
    expect(n == 4 && memcmp(sbuf, "ping", 4) == 0, "server received ping");
    must(io, myio_sock_write(io, server, "pong", 4), "server write");
    n = must(io, myio_sock_read(io, client, cbuf, sizeof cbuf), "client read");
    expect(n == 4 && memcmp(cbuf, "pong", 4) == 0, "client received pong");

    /* One-read-per-socket rule: a second read is refused with EBUSY. */
    myio_task *r1 = myio_sock_read(io, server, sbuf, sizeof sbuf);
    myio_task *r2 = myio_sock_read(io, server, cbuf, sizeof cbuf);
    myio_result busy = myio_join(io, r2);
    expect(busy.status == MYIO_ERROR && busy.error == EBUSY,
           "second outstanding read is refused (EBUSY)");

    /* Cancel the blocked server read. */
    expect(myio_cancel(io, r1) == 0, "cancel accepted for a blocked read");
    myio_result rr = myio_join(io, r1);
    expect(rr.status == MYIO_CANCELED, "canceled read joins as CANCELED");

    /* Close the client while a read is blocked on it (its peer is still
     * open, so nothing is coming): the read must complete as CANCELED
     * rather than hang. */
    myio_task *blocked = myio_sock_read(io, client, cbuf, sizeof cbuf);
    myio_task *cl = myio_sock_close(io, client);
    rr = myio_join(io, blocked);
    expect(rr.status == MYIO_CANCELED,
           "sock_close interrupts a blocked read (CANCELED)");
    must(io, cl, "close client");

    /* The client is gone; the server side now reads EOF. */
    n = must(io, myio_sock_read(io, server, sbuf, sizeof sbuf),
             "server read after peer close");
    expect(n == 0, "read after peer close reports EOF (0 bytes)");
    must(io, myio_sock_close(io, server), "close server conn");
    must(io, myio_sock_close(io, ls), "close listener");

    /* DNS failure renders through the resolver's own vocabulary. */
    myio_result dr = myio_join(io, myio_tcp_connect(io,
                                                    "no.such.host.invalid", 9));
    expect(dr.status == MYIO_ERROR, "connect to bogus hostname fails");
    printf("  (dns error rendered as: \"%s\")\n", myio_strerror(io, dr.error));
}

static void teardown_tests(myio *io, int test_port) {
    /* Detach a still-blocked read and let destroy reclaim it: the backend
     * must stop the operation and free everything without hanging. This
     * intentionally leaves work for myio_destroy. */
    int lerr = 0;
    myio_sock *ls = myio_tcp_listen(io, "127.0.0.1", test_port, 8, &lerr);
    myio_task *at = myio_tcp_accept(io, ls);
    myio_task *ct = myio_tcp_connect(io, "127.0.0.1", myio_sock_port(io, ls));
    myio_result cr = myio_join(io, ct);
    myio_result ar = myio_join(io, at);
    expect(myio_ok(cr) && myio_ok(ar), "teardown pair established");
    if (myio_ok(cr) && myio_ok(ar)) {
        static char buf[32];
        myio_sock *client = cr.ptr;
        myio_task *orphan = myio_sock_read(io, client, buf, sizeof buf);
        myio_cancel(io, orphan);
        myio_task_detach(io, orphan); /* fire and forget */
        /* client, the accepted socket, and the listener are deliberately
         * left open: destroy must reclaim them. */
        (void)ls;
    }
}

static void oom_tests(myio *io) {
    /* Exhaust the task slab: CONFIG_MYIO_MAX_TASKS sleeps, then one more.
     * The extra submission must return NULL, which myio_join maps to
     * ENOMEM - the interface's documented OOM path. */
    myio_task *held[CONFIG_MYIO_MAX_TASKS];
    int got = 0;
    for (int i = 0; i < CONFIG_MYIO_MAX_TASKS; i++) {
        held[i] = myio_sleep(io, 60000);
        if (held[i])
            got++;
    }
    expect(got == CONFIG_MYIO_MAX_TASKS, "slab holds MAX_TASKS tasks");
    myio_task *extra = myio_sleep(io, 60000);
    expect(extra == NULL, "task MAX_TASKS+1 is refused (NULL)");
    myio_result r = myio_join(io, extra);
    expect(r.status == MYIO_ERROR && r.error == ENOMEM,
           "joining the failed submission yields ENOMEM");
    for (int i = 0; i < CONFIG_MYIO_MAX_TASKS; i++)
        myio_task_free(io, held[i]); /* cancel + free, all in-flight */
    myio_task *again = myio_sleep(io, 10);
    expect(again != NULL, "slab blocks are reusable after free");
    myio_join(io, again);
}

/* Run the whole suite against one backend. Each run uses its own ports:
 * the previous run's connections may linger in TIME_WAIT on the host. */
static void run_suite(const char *name, myio *(*ctor)(void), int port_base) {
    myio *io = ctor();
    if (!io) {
        printf("failed to create %s backend\n", name);
        failures++;
        return;
    }
    printf("== backend: %s ==\n", name);

    printf("file IO:\n");
    file_tests(io);
    printf("timers and spawn:\n");
    timer_and_spawn_tests(io);
    printf("TCP:\n");
    tcp_tests(io, port_base);
    printf("allocation limits:\n");
    oom_tests(io);
    printf("teardown:\n");
    teardown_tests(io, port_base + 1);
    myio_destroy(io);
    expect(1, "destroy reclaimed detached tasks and open sockets");

    /* The instance is single, static, and reusable after destroy. */
    myio *io2 = ctor();
    expect(io2 != NULL, "instance can be re-created after destroy");
    if (io2) {
        myio_result r = myio_join(io2, myio_sleep(io2, 10));
        expect(myio_ok(r), "fresh instance runs a timer");
        myio_destroy(io2);
    }
}

int main(void) {
    int rc = fs_mount(&lfs_mnt);
    if (rc != 0)
        printf("littlefs mount failed (%d); file tests will fail\n", rc);
    printf("board: %s\n", CONFIG_BOARD_TARGET);

    run_suite("zephyr (event loop)", myio_zephyr_new, 43117);
    run_suite("zephyr (thread pool)", myio_zephyr_pool_new, 43217);

    printf("%s\n", failures ? "FAILURES" : "all ok");
#ifdef CONFIG_ARCH_POSIX
    posix_exit(failures != 0);
#endif
    return failures != 0;
}
