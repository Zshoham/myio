/* Fully synchronous myio implementation: every operation runs to completion
 * with blocking POSIX calls inside the submit function, so await/select are
 * no-ops that just report the stored result. */
#define _POSIX_C_SOURCE 200809L
#include "myio_sync.h"

#include "myio_common.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    myio_result res;
} sync_task;

typedef struct {
    int fd;
} sync_sock;

static sync_task *task_of(myio_task *t) { return (sync_task *)t; }
static sync_sock *sock_of(myio_sock *s) { return (sync_sock *)s; }

static myio_task *task_err(int err) {
    sync_task *t = calloc(1, sizeof(*t));
    if (!t)
        return NULL;
    t->res.status = MYIO_ERROR;
    t->res.error = err;
    return (myio_task *)t;
}

static myio_task *task_ok(int64_t value, void *ptr) {
    sync_task *t = calloc(1, sizeof(*t));
    if (!t)
        return NULL;
    t->res.status = MYIO_OK;
    t->res.value = value;
    t->res.ptr = ptr;
    return (myio_task *)t;
}

static myio_task *task_from(int64_t rc) {
    return rc >= 0 ? task_ok(rc, NULL) : task_err(errno);
}

static myio_task *impl_open(myio *io, const char *path, int flags, int mode) {
    (void)io;
    return task_from(open(path, flags, mode));
}

static myio_task *impl_close(myio *io, int64_t fd) {
    (void)io;
    return task_from(close((int)fd));
}

static myio_task *impl_read(myio *io, int64_t fd, void *buf, size_t len,
                            int64_t offset) {
    (void)io;
    ssize_t n = offset == MYIO_NO_OFFSET ? read((int)fd, buf, len)
                                         : pread((int)fd, buf, len, offset);
    return task_from(n);
}

static myio_task *impl_write(myio *io, int64_t fd, const void *buf, size_t len,
                             int64_t offset) {
    (void)io;
    ssize_t n = offset == MYIO_NO_OFFSET ? write((int)fd, buf, len)
                                         : pwrite((int)fd, buf, len, offset);
    return task_from(n);
}

static myio_task *impl_sleep(myio *io, uint64_t ms) {
    (void)io;
    struct timespec ts = { .tv_sec = (time_t)(ms / 1000),
                           .tv_nsec = (long)(ms % 1000) * 1000000L };
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR)
        ;
    return task_from(0);
}

static myio_task *impl_spawn(myio *io, myio_fn fn, void *arg) {
    (void)io;
    int64_t rc = fn(arg);
    return rc >= 0 ? task_ok(rc, NULL) : task_err((int)-rc);
}

/* ---- TCP ---- */

static myio_task *wrap_fd(int fd) {
    sync_sock *s = malloc(sizeof(*s));
    if (!s) {
        close(fd);
        return NULL;
    }
    s->fd = fd;
    return task_ok(0, s);
}

static myio_task *impl_tcp_connect(myio *io, const char *host, int port) {
    (void)io;
    int err = 0;
    int fd = myio_posix_connect(host, port, &err);
    return fd >= 0 ? wrap_fd(fd) : task_err(err);
}

static myio_sock *impl_tcp_listen(myio *io, const char *host, int port,
                                  int backlog, int *err) {
    (void)io;
    int fd = myio_posix_listen(host, port, backlog, err);
    if (fd < 0)
        return NULL;
    sync_sock *s = malloc(sizeof(*s));
    if (!s) {
        if (err)
            *err = ENOMEM;
        close(fd);
        return NULL;
    }
    s->fd = fd;
    return (myio_sock *)s;
}

static myio_task *impl_tcp_accept(myio *io, myio_sock *listener) {
    (void)io;
    int fd = accept(sock_of(listener)->fd, NULL, NULL);
    return fd >= 0 ? wrap_fd(fd) : task_err(errno);
}

static myio_task *impl_sock_read(myio *io, myio_sock *sock, void *buf,
                                 size_t len) {
    (void)io;
    return task_from(recv(sock_of(sock)->fd, buf, len, 0));
}

static myio_task *impl_sock_write(myio *io, myio_sock *sock, const void *buf,
                                  size_t len) {
    (void)io;
    /* Match the interface contract (and libuv): the whole buffer is written. */
    size_t off = 0;
    while (off < len) {
        ssize_t n = send(sock_of(sock)->fd, (const char *)buf + off, len - off,
                         MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return task_err(errno);
        }
        off += (size_t)n;
    }
    return task_ok((int64_t)len, NULL);
}

static myio_task *impl_sock_close(myio *io, myio_sock *sock) {
    (void)io;
    int rc = close(sock_of(sock)->fd);
    free(sock);
    return task_from(rc);
}

static int impl_sock_port(myio *io, myio_sock *sock) {
    (void)io;
    return myio_local_port(sock_of(sock)->fd);
}

/* ---- synchronisation and lifetime ---- */

/* Every operation completes inside its submit call, so there is never a
 * pending task to make progress on: the generic await/select loops see
 * task_done() == 1 and never get here. */
static int impl_step(myio *io) {
    (void)io;
    return 0;
}

static int impl_cancel(myio *io, myio_task *task) {
    (void)io;
    (void)task;
    return -1; /* operations always finish before a cancel can be issued */
}

static myio_result impl_task_result(myio *io, const myio_task *task) {
    (void)io;
    return ((const sync_task *)task)->res;
}

static const char *impl_error_str(myio *io, int err) {
    (void)io;
    return myio_default_error_str(err);
}

static unsigned impl_caps(myio *io) {
    (void)io;
    /* Every operation completes inside submit: no concurrency, submit blocks,
     * nothing to cancel, spawn runs inline. */
    return 0;
}

static int impl_task_done(myio *io, const myio_task *task) {
    (void)io;
    (void)task;
    return 1;
}

static void impl_task_free(myio *io, myio_task *task) {
    (void)io;
    free(task);
}

static void impl_task_detach(myio *io, myio_task *task) {
    (void)io;
    sync_task *t = task_of(task);
    /* Every task is complete by now; discarding the result means a socket
     * it won must be closed here, or it would leak. */
    if (t->res.ptr) {
        sync_sock *s = t->res.ptr;
        close(s->fd);
        free(s);
    }
    free(t);
}

static void impl_destroy(myio *io) {
    free(io);
}

static const myio_ops sync_ops = {
    .open        = impl_open,
    .close       = impl_close,
    .read        = impl_read,
    .write       = impl_write,
    .sleep       = impl_sleep,
    .spawn       = impl_spawn,
    .tcp_connect = impl_tcp_connect,
    .tcp_listen  = impl_tcp_listen,
    .tcp_accept  = impl_tcp_accept,
    .sock_read   = impl_sock_read,
    .sock_write  = impl_sock_write,
    .sock_close  = impl_sock_close,
    .sock_port   = impl_sock_port,
    .step        = impl_step,
    .cancel      = impl_cancel,
    .task_result = impl_task_result,
    .error_str   = impl_error_str,
    .caps        = impl_caps,
    .task_done   = impl_task_done,
    .task_free   = impl_task_free,
    .task_detach = impl_task_detach,
    .destroy     = impl_destroy,
};

myio *myio_sync_new(void) {
    myio *io = calloc(1, sizeof(*io));
    if (!io)
        return NULL;
    io->ops = &sync_ops;
    return io;
}
