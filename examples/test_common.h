/* test_common.h - the scaffolding cancel_test and concurrency_test share:
 * failure counting and the expect() reporter, the /proc fd census behind the
 * leak checks, and the backend-name -> constructor map. The sync backend is
 * deliberately absent from the map: every operation completes inside its
 * submit call, so the pending-operation scenarios these tests exercise
 * cannot be expressed on it. */
#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include "myio.h"
#include "myio_pool.h"
#include "myio_uring.h"
#include "myio_uv.h"
#include "myio_xev.h"

#include <dirent.h>
#include <stdio.h>
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

#endif /* TEST_COMMON_H */
