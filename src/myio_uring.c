/* io_uring-backed myio implementation, written directly against liburing.
 *
 * Every submit preps an SQE whose user_data is the task pointer and flushes
 * it to the kernel immediately (the header promises ops start executing when
 * submit returns). await/select/cancel drive one loop: submit_and_wait for a
 * CQE, then drain the completion queue, dispatching each CQE by task kind.
 * One task = one logical op = one CQE, with three exceptions: a short send
 * rearms a new SEND from the same task, cancel SQEs carry a tag instead
 * of a task (their CQE only says the *request* finished; the canceled op
 * still delivers its own CQE, which is what completes the task), and a
 * sock_write submitted behind another write on the same socket has no SQE
 * at all until it reaches the head of its socket's FIFO (see uring_sock).
 *
 * io_uring covers files, sockets and timers but not the resolver and not
 * arbitrary functions, so DNS and myio_spawn run inline in submit — legal
 * under the eager-submission model, same as the Zephyr backend. */
#define _GNU_SOURCE
#include "myio_uring.h"

#include "myio_common.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* user_data of cancel/timeout_remove SQEs: tasks are malloc-aligned, so a
 * set low bit cannot be a task pointer. The drain loop skips these — the
 * target op's own CQE is what matters. */
#define CANCEL_UDATA ((uint64_t)1)

enum task_kind {
    TASK_OPEN,
    TASK_CLOSE,
    TASK_READ,
    TASK_WRITE,
    TASK_SLEEP,
    TASK_SPAWN,
    TASK_CONNECT,
    TASK_ACCEPT,
    TASK_SOCK_READ,
    TASK_SOCK_WRITE,
    TASK_SOCK_CLOSE,
};

typedef struct uring_task uring_task;
typedef struct uring_sock uring_sock;

typedef struct {
    myio            base;
    struct io_uring ring;
    size_t          inflight; /* SQEs submitted, CQE not yet drained */
    uring_task     *tasks;    /* all live tasks, for destroy's cancel walk */
    uring_sock     *socks;    /* all live sockets, for destroy's fd sweep */
    char            errbuf[128];
} uring_io;

struct uring_task {
    uring_io       *io;
    enum task_kind  kind;
    int             done;
    int             detached; /* free on completion, result discarded */
    myio_result     res;
    uring_sock     *sock;    /* op's socket; connect: the not-yet-won sock */
    uring_sock     *newsock; /* accept: wrapper preallocated for the new fd */
    /* Op state the kernel reads asynchronously, so it must live in the task
     * until the CQE arrives: */
    char           *path;    /* open */
    struct __kernel_timespec ts; /* sleep */
    struct sockaddr_storage addr; /* connect / accept */
    socklen_t       addrlen;
    int             wfd;     /* sock_write: fd copy, survives sock teardown */
    const char     *wbuf;    /* sock_write rearm state */
    size_t          wlen, wsent;
    uring_task     *wnext;   /* sock_write: next write queued on the socket */
    int             wcanceled; /* sock_write: canceled; rearm must not resend */
    uring_task     *prev, *next;
};

struct uring_sock {
    uring_io   *io;
    int         fd;
    uring_task *read_task;   /* outstanding sock_read (at most one) */
    uring_task *accept_task; /* outstanding tcp_accept (at most one) */
    /* Outstanding sock_writes, FIFO. Only the head has a send SQE in
     * flight; the rest wait here (io_uring gives no ordering between two
     * concurrent sends on one fd, so serializing submissions ourselves is
     * what implements the header's writes-in-submission-order,
     * never-interleaved guarantee). Write tasks point back via t->sock. */
    uring_task *write_head;
    uring_task *write_tail;
    uring_sock *prev, *next;
};

static uring_task *task_of(myio_task *t) { return (uring_task *)t; }
static myio_task *handle_of(uring_task *t) { return (myio_task *)t; }
static uring_io *io_of(myio *io) { return (uring_io *)io; }
static uring_sock *sock_of(myio_sock *s) { return (uring_sock *)s; }

/* ---- intrusive lists ---- */

static void task_link(uring_io *u, uring_task *t) {
    t->next = u->tasks;
    if (u->tasks)
        u->tasks->prev = t;
    u->tasks = t;
}

static void task_unlink(uring_io *u, uring_task *t) {
    if (t->prev)
        t->prev->next = t->next;
    else if (u->tasks == t)
        u->tasks = t->next;
    if (t->next)
        t->next->prev = t->prev;
}

static void sock_unlink(uring_io *u, uring_sock *s) {
    if (s->prev)
        s->prev->next = s->next;
    else if (u->socks == s)
        u->socks = s->next;
    if (s->next)
        s->next->prev = s->prev;
}

/* ---- task and socket lifecycle ---- */

static uring_task *task_new(uring_io *u, enum task_kind kind) {
    uring_task *t = calloc(1, sizeof(*t));
    if (!t)
        return NULL;
    t->io = u;
    t->kind = kind;
    t->res.status = MYIO_PENDING;
    task_link(u, t);
    return t;
}

/* Complete a task that failed before its SQE could be submitted. */
static myio_task *task_fail(uring_task *t, int err) {
    t->res.status = MYIO_ERROR;
    t->res.error = err;
    t->done = 1;
    return handle_of(t);
}

static uring_sock *sock_new(uring_io *u, int fd) {
    uring_sock *s = calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->io = u;
    s->fd = fd;
    s->next = u->socks;
    if (u->socks)
        u->socks->prev = s;
    u->socks = s;
    return s;
}

/* Synchronous socket teardown: legal whenever no in-flight op references the
 * fd (a freshly won or never-used socket) — nothing here needs the libxev
 * backend's loop-sequenced close dance. */
static void sock_close_now(uring_io *u, uring_sock *s) {
    if (s->fd >= 0)
        close(s->fd);
    sock_unlink(u, s);
    free(s);
}

/* Free a detached task that has completed. Its result is discarded, so a
 * socket it won is closed here - no caller will ever claim it. */
static void task_reap(uring_io *u, uring_task *t) {
    if (t->res.ptr)
        sock_close_now(u, t->res.ptr);
    task_unlink(u, t);
    free(t->path);
    free(t);
}

/* Every completion ends here: marks the task done and, when it has been
 * detached, frees it on the spot (safe: the CQE is the kernel's final event
 * for the op, so nothing references the task afterwards). */
static void task_finish(uring_io *u, uring_task *t) {
    t->done = 1;
    if (t->detached)
        task_reap(u, t);
}

static void task_complete(uring_io *u, uring_task *t, int64_t res) {
    if (res >= 0) {
        t->res.status = MYIO_OK;
        t->res.value = res;
    } else if (res == -ECANCELED) {
        t->res.status = MYIO_CANCELED;
    } else {
        t->res.status = MYIO_ERROR;
        t->res.error = (int)-res;
    }
    task_finish(u, t);
}

/* ---- SQE submission ---- */

/* Get an SQE, flushing the queue once if it is full. */
static struct io_uring_sqe *sqe_get(uring_io *u) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&u->ring);
    if (!sqe) {
        io_uring_submit(&u->ring);
        sqe = io_uring_get_sqe(&u->ring);
    }
    return sqe;
}

/* Flush the just-prepped SQE to the kernel: eager submission. */
static void sqe_flush(uring_io *u) {
    io_uring_submit(&u->ring);
    u->inflight++;
}

/* ---- the per-socket write FIFO ---- */

/* Put `t`'s send on the ring (from wherever a previous short send stopped).
 * MSG_WAITALL: sock_write's all-or-error contract, expressed to the
 * kernel — it retries short sends in-kernel instead of bouncing each one
 * through a CQE and a fresh SQE (measured: a 4 MB write through a 4 KB
 * SO_SNDBUF took 128 userspace rearms without it, one CQE with). */
static int write_arm(uring_io *u, uring_task *t) {
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe)
        return -1;
    io_uring_prep_send(sqe, t->wfd, t->wbuf + t->wsent, t->wlen - t->wsent,
                       MSG_WAITALL);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    return 0;
}

/* The head write `t` finished (success, error, or cancellation): pop it off
 * its socket's FIFO, arm the next queued write's send, and complete it. A
 * write whose socket pointer was severed (sock_close/destroy dismantled the
 * FIFO already) just completes. Each write keeps its own result, and writes
 * queued behind a failed one still run, per the header's contract. */
static void write_finish(uring_io *u, uring_task *t, int64_t res) {
    uring_sock *s = t->sock;
    t->sock = NULL;
    if (s) { /* t is the in-flight head by construction */
        s->write_head = t->wnext;
        if (!s->write_head)
            s->write_tail = NULL;
        t->wnext = NULL;
        /* Promote successors until one's SQE lands or the queue empties. */
        while (s->write_head && write_arm(u, s->write_head) < 0) {
            uring_task *n = s->write_head;
            s->write_head = n->wnext;
            if (!s->write_head)
                s->write_tail = NULL;
            n->wnext = NULL;
            n->sock = NULL;
            task_complete(u, n, -EAGAIN);
        }
    }
    task_complete(u, t, res);
}

/* ---- the drive loop ---- */

static void dispatch(uring_io *u, uring_task *t, int32_t res);

static void drain_cqes(uring_io *u) {
    struct io_uring_cqe *cqe;
    unsigned head, count = 0;
    io_uring_for_each_cqe(&u->ring, head, cqe) {
        count++;
        u->inflight--;
        /* A cancel request's own CQE carries no task; the canceled op still
         * completes through its own CQE. */
        if (!(cqe->user_data & 1))
            dispatch(u, (uring_task *)(uintptr_t)cqe->user_data, cqe->res);
    }
    io_uring_cq_advance(&u->ring, count);
}

/* One blocking loop iteration; 0 when nothing is in flight (no CQE can ever
 * arrive, so waiting would deadlock). */
static int step(uring_io *u) {
    if (u->inflight == 0)
        return 0;
    io_uring_submit_and_wait(&u->ring, 1);
    drain_cqes(u);
    return 1;
}

static void dispatch(uring_io *u, uring_task *t, int32_t res) {
    switch (t->kind) {
    case TASK_SLEEP:
        /* Normal expiry of a count-0 timeout is reported as -ETIME. */
        task_complete(u, t, res == -ETIME ? 0 : res);
        break;
    case TASK_CONNECT:
        if (res == 0) {
            t->res.ptr = t->sock;
            t->sock = NULL;
            task_complete(u, t, 0);
        } else {
            sock_close_now(u, t->sock);
            t->sock = NULL;
            task_complete(u, t, res);
        }
        break;
    case TASK_ACCEPT:
        if (t->sock && t->sock->accept_task == t)
            t->sock->accept_task = NULL;
        if (res >= 0) {
            t->newsock->fd = res;
            t->res.ptr = t->newsock;
            t->newsock = NULL;
            task_complete(u, t, 0);
        } else {
            sock_close_now(u, t->newsock); /* fd still -1: just unlink+free */
            t->newsock = NULL;
            task_complete(u, t, res);
        }
        break;
    case TASK_SOCK_READ:
        if (t->sock && t->sock->read_task == t)
            t->sock->read_task = NULL;
        task_complete(u, t, res); /* res 0 = peer closed = EOF, per contract */
        break;
    case TASK_SOCK_WRITE:
        if (res < 0) {
            /* Error or cancellation (-ECANCELED from a close/cancel): either
             * way this head's slot frees up — pop the FIFO and promote the
             * next queued write. */
            write_finish(u, t, res);
            break;
        }
        t->wsent += (size_t)res;
        if (t->wsent < t->wlen) {
            /* Short send. MSG_WAITALL makes the kernel retry these itself
             * (one CQE per logical write), so this userspace rearm is only
             * the fallback: kernels before 5.19 ignore the flag for send,
             * and an error after partial progress surfaces a short CQE.
             * Continue from where the kernel stopped — unless the socket was
             * closed under us (sock severed) or the write was canceled, in
             * which case resubmitting would put a new send on a dead or
             * canceled stream; the partial bytes already sent are the
             * header-sanctioned truncation of a canceled write. */
            if (!t->sock || t->wcanceled) {
                write_finish(u, t, -ECANCELED);
                break;
            }
            if (write_arm(u, t) < 0)
                write_finish(u, t, -EAGAIN);
            break;
        }
        write_finish(u, t, (int64_t)t->wlen);
        break;
    case TASK_SOCK_CLOSE: {
        uring_sock *s = t->sock;
        t->sock = NULL;
        if (s) { /* the CLOSE op released the fd; free the wrapper */
            sock_unlink(u, s);
            free(s);
        }
        task_complete(u, t, res);
        break;
    }
    default: /* OPEN/CLOSE/READ/WRITE: res is the fd or byte count */
        task_complete(u, t, res);
        break;
    }
}

/* ---- filesystem operations ---- */

static myio_task *impl_open(myio *io, const char *path, int flags, int mode) {
    uring_io *u = io_of(io);
    uring_task *t = task_new(u, TASK_OPEN);
    if (!t)
        return NULL;
    /* The kernel reads the path from the SQE asynchronously; keep the copy
     * alive in the task until the CQE (this is also what gives the header's
     * "strings are copied by submit" guarantee). */
    t->path = strdup(path);
    if (!t->path)
        return task_fail(t, ENOMEM);
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe)
        return task_fail(t, EAGAIN);
    io_uring_prep_openat(sqe, AT_FDCWD, t->path, flags, mode);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    return handle_of(t);
}

static myio_task *impl_close(myio *io, int64_t fd) {
    uring_io *u = io_of(io);
    uring_task *t = task_new(u, TASK_CLOSE);
    if (!t)
        return NULL;
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe)
        return task_fail(t, EAGAIN);
    io_uring_prep_close(sqe, (int)fd);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    return handle_of(t);
}

/* MYIO_NO_OFFSET (-1) maps to io_uring's offset -1: "use and advance the
 * file position", which is exactly the same contract. */
static myio_task *impl_read(myio *io, int64_t fd, void *buf, size_t len,
                            int64_t offset) {
    uring_io *u = io_of(io);
    uring_task *t = task_new(u, TASK_READ);
    if (!t)
        return NULL;
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe)
        return task_fail(t, EAGAIN);
    io_uring_prep_read(sqe, (int)fd, buf, (unsigned)len, (uint64_t)offset);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    return handle_of(t);
}

static myio_task *impl_write(myio *io, int64_t fd, const void *buf, size_t len,
                             int64_t offset) {
    uring_io *u = io_of(io);
    uring_task *t = task_new(u, TASK_WRITE);
    if (!t)
        return NULL;
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe)
        return task_fail(t, EAGAIN);
    io_uring_prep_write(sqe, (int)fd, buf, (unsigned)len, (uint64_t)offset);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    return handle_of(t);
}

/* ---- timers ---- */

static myio_task *impl_sleep(myio *io, uint64_t ms) {
    uring_io *u = io_of(io);
    uring_task *t = task_new(u, TASK_SLEEP);
    if (!t)
        return NULL;
    t->ts.tv_sec = (int64_t)(ms / 1000);
    t->ts.tv_nsec = (long long)(ms % 1000) * 1000000;
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe)
        return task_fail(t, EAGAIN);
    io_uring_prep_timeout(sqe, &t->ts, 0, 0);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    return handle_of(t);
}

/* ---- spawned functions ---- */

/* io_uring's completion queue covers the OS, not the runtime: it cannot run
 * an arbitrary function. Run it inline in submit — sanctioned by the eager
 * execution model (the sync and Zephyr backends do the same). True async
 * spawn would need a thread completing through an eventfd read pending on
 * the ring: the uv_async/xev.Async self-wake device yet again. */
static myio_task *impl_spawn(myio *io, myio_fn fn, void *arg) {
    uring_task *t = task_new(io_of(io), TASK_SPAWN);
    if (!t)
        return NULL;
    int64_t ret = fn(arg);
    if (ret >= 0) {
        t->res.status = MYIO_OK;
        t->res.value = ret;
    } else {
        t->res.status = MYIO_ERROR;
        t->res.error = (int)-ret;
    }
    t->done = 1;
    return handle_of(t);
}

/* ---- TCP: connect ---- */

static myio_task *impl_tcp_connect(myio *io, const char *host, int port) {
    uring_io *u = io_of(io);
    uring_task *t = task_new(u, TASK_CONNECT);
    if (!t)
        return NULL;
    /* io_uring has no DNS op; resolve inline in submit (see impl_spawn). */
    char service[16];
    snprintf(service, sizeof service, "%d", port);
    struct addrinfo hints, *ai;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    int rc = getaddrinfo(host, service, &hints, &ai);
    if (rc != 0)
        /* getaddrinfo's EAI_* codes are negative on glibc; store rc as-is
         * so a negative result.error is always a resolver code, verbatim
         * and portable for callers to branch on (see myio.h). EAI_SYSTEM
         * carries no information of its own - errno has the real cause. */
        return task_fail(t, rc == EAI_SYSTEM ? errno : rc);
    /* Only the first resolved address is tried (the uv backend does the
     * same). The sockaddr must live in the task until the CQE. */
    memcpy(&t->addr, ai->ai_addr, ai->ai_addrlen);
    t->addrlen = ai->ai_addrlen;
    int fd = socket(ai->ai_family, SOCK_STREAM, 0);
    freeaddrinfo(ai);
    if (fd < 0)
        return task_fail(t, errno);
    t->sock = sock_new(u, fd);
    if (!t->sock) {
        close(fd);
        return task_fail(t, ENOMEM);
    }
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe) {
        sock_close_now(u, t->sock);
        t->sock = NULL;
        return task_fail(t, EAGAIN);
    }
    io_uring_prep_connect(sqe, fd, (struct sockaddr *)&t->addr, t->addrlen);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    return handle_of(t);
}

/* ---- TCP: listen / accept ---- */

static myio_sock *impl_tcp_listen(myio *io, const char *host, int port,
                                  int backlog, int *err) {
    uring_io *u = io_of(io);
    struct sockaddr_storage ss;
    socklen_t slen;
    memset(&ss, 0, sizeof ss);
    struct sockaddr_in *a4 = (struct sockaddr_in *)&ss;
    struct sockaddr_in6 *a6 = (struct sockaddr_in6 *)&ss;
    if (inet_pton(AF_INET, host, &a4->sin_addr) == 1) {
        a4->sin_family = AF_INET;
        a4->sin_port = htons((uint16_t)port);
        slen = sizeof *a4;
    } else if (inet_pton(AF_INET6, host, &a6->sin6_addr) == 1) {
        a6->sin6_family = AF_INET6;
        a6->sin6_port = htons((uint16_t)port);
        slen = sizeof *a6;
    } else {
        if (err)
            *err = EINVAL;
        return NULL;
    }
    int fd = socket(ss.ss_family, SOCK_STREAM, 0);
    if (fd < 0) {
        if (err)
            *err = errno;
        return NULL;
    }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (bind(fd, (struct sockaddr *)&ss, slen) < 0 ||
        listen(fd, backlog) < 0) {
        if (err)
            *err = errno;
        close(fd);
        return NULL;
    }
    uring_sock *s = sock_new(u, fd);
    if (!s) {
        if (err)
            *err = ENOMEM;
        close(fd);
        return NULL;
    }
    return (myio_sock *)s;
}

static myio_task *impl_tcp_accept(myio *io, myio_sock *listener) {
    uring_io *u = io_of(io);
    uring_sock *ls = sock_of(listener);
    uring_task *t = task_new(u, TASK_ACCEPT);
    if (!t)
        return NULL;
    if (ls->accept_task)
        return task_fail(t, EBUSY);
    /* Wrap the future fd now so the CQE path cannot fail on OOM. */
    t->newsock = sock_new(u, -1);
    if (!t->newsock)
        return task_fail(t, ENOMEM);
    t->addrlen = sizeof t->addr;
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe) {
        sock_close_now(u, t->newsock);
        t->newsock = NULL;
        return task_fail(t, EAGAIN);
    }
    io_uring_prep_accept(sqe, ls->fd, (struct sockaddr *)&t->addr,
                         &t->addrlen, 0);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    t->sock = ls;
    ls->accept_task = t;
    return handle_of(t);
}

/* ---- TCP: read / write / close ---- */

static myio_task *impl_sock_read(myio *io, myio_sock *sock, void *buf,
                                 size_t len) {
    uring_io *u = io_of(io);
    uring_sock *s = sock_of(sock);
    uring_task *t = task_new(u, TASK_SOCK_READ);
    if (!t)
        return NULL;
    if (s->read_task)
        return task_fail(t, EBUSY);
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe)
        return task_fail(t, EAGAIN);
    io_uring_prep_recv(sqe, s->fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    t->sock = s;
    s->read_task = t;
    return handle_of(t);
}

static myio_task *impl_sock_write(myio *io, myio_sock *sock, const void *buf,
                                  size_t len) {
    uring_io *u = io_of(io);
    uring_sock *s = sock_of(sock);
    uring_task *t = task_new(u, TASK_SOCK_WRITE);
    if (!t)
        return NULL;
    t->wfd = s->fd;
    t->wbuf = buf;
    t->wlen = len;
    t->sock = s;
    if (s->write_head) {
        /* A write is already outstanding: queue behind it with no SQE of its
         * own — write_finish arms it when it reaches the head (see the FIFO
         * comment on uring_sock). */
        s->write_tail->wnext = t;
        s->write_tail = t;
        return handle_of(t);
    }
    if (write_arm(u, t) < 0) {
        t->sock = NULL;
        return task_fail(t, EAGAIN);
    }
    s->write_head = s->write_tail = t;
    return handle_of(t);
}

/* Submit an ASYNC_CANCEL (or TIMEOUT_REMOVE) targeting `t`'s op. Its own CQE
 * is tagged and ignored: the canceled op's CQE settles the task. */
static int cancel_submit(uring_io *u, uring_task *t) {
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe)
        return -1;
    if (t->kind == TASK_SLEEP)
        io_uring_prep_timeout_remove(sqe, (uint64_t)(uintptr_t)t, 0);
    else
        io_uring_prep_cancel64(sqe, (uint64_t)(uintptr_t)t, 0);
    io_uring_sqe_set_data64(sqe, CANCEL_UDATA);
    sqe_flush(u);
    return 0;
}

static myio_task *impl_sock_close(myio *io, myio_sock *sock) {
    uring_io *u = io_of(io);
    uring_sock *s = sock_of(sock);
    uring_task *t = task_new(u, TASK_SOCK_CLOSE);
    if (!t)
        return NULL;
    /* Outstanding pull-style ops would otherwise block on the fd forever
     * (an in-flight recv holds a file reference, so the CLOSE alone would
     * not wake it). Cancel them first — SQEs are consumed in order — and
     * sever both pointer directions now: the sock may be freed by the CLOSE
     * CQE before the canceled ops' CQEs are drained. */
    if (s->read_task) {
        cancel_submit(u, s->read_task);
        s->read_task->sock = NULL;
        s->read_task = NULL;
    }
    if (s->accept_task) {
        cancel_submit(u, s->accept_task);
        s->accept_task->sock = NULL;
        s->accept_task = NULL;
    }
    /* Writes: same discipline, with one twist. Only the head write has a
     * send SQE in flight (an in-flight send also holds a file reference, so
     * the CLOSE alone would never wake it — the same trap as the recv
     * above); cancel it like the read. The queued rest have no SQE, so no
     * CQE will ever arrive for them: complete them CANCELED right here,
     * through task_complete so detach/reap semantics hold. */
    if (s->write_head) {
        uring_task *w = s->write_head;
        cancel_submit(u, w);
        w->sock = NULL;
        for (uring_task *q = w->wnext, *qn; q; q = qn) {
            qn = q->wnext;
            q->wnext = NULL;
            q->sock = NULL;
            task_complete(u, q, -ECANCELED);
        }
        w->wnext = NULL;
        s->write_head = s->write_tail = NULL;
    }
    struct io_uring_sqe *sqe = sqe_get(u);
    if (!sqe)
        return task_fail(t, EAGAIN);
    io_uring_prep_close(sqe, s->fd);
    io_uring_sqe_set_data(sqe, t);
    sqe_flush(u);
    t->sock = s;
    return handle_of(t);
}

static int impl_sock_port(myio *io, myio_sock *sock) {
    (void)io;
    return myio_local_port(sock_of(sock)->fd);
}

/* ---- synchronisation ---- */

static int impl_step(myio *io) {
    return step(io_of(io));
}

/* io_uring is the first backend where cancellation is itself an async op:
 * ASYNC_CANCEL gets its own CQE, and even that doesn't settle the target
 * (-EALREADY = still running, may yet complete). The vtable's synchronous
 * int wants an answer *now*, so after submitting the cancel we drive the
 * ring until the target's CQE lands — legal because the re-entrancy
 * guarantee means no user code can observe the loop turning inside
 * cancel(), and bounded because the kernel answers cancel requests
 * promptly. The result is a *truthful* cancel: 0 iff the op really was
 * canceled, which is stronger than the request-only contract demands. */
static int impl_cancel(myio *io, myio_task *task) {
    uring_io *u = io_of(io);
    uring_task *t = task_of(task);
    if (t->done)
        return -1;
    switch (t->kind) {
    case TASK_SLEEP:
    case TASK_OPEN:
    case TASK_CLOSE:
    case TASK_READ:
    case TASK_WRITE:
    case TASK_CONNECT:
    case TASK_ACCEPT:
    case TASK_SOCK_READ:
        if (cancel_submit(u, t) < 0)
            return -1;
        break;
    case TASK_SOCK_WRITE: {
        uring_sock *s = t->sock;
        if (s && s->write_head != t) {
            /* Queued behind an earlier write: it has no SQE, hence no kernel
             * state to unwind — unlink it from the FIFO and complete it
             * CANCELED on the spot. This keeps task_free on a queued write
             * non-blocking. */
            uring_task *prev = s->write_head;
            while (prev->wnext != t)
                prev = prev->wnext;
            prev->wnext = t->wnext;
            if (s->write_tail == t)
                s->write_tail = prev;
            t->wnext = NULL;
            t->sock = NULL;
            task_complete(u, t, -ECANCELED);
            return 0;
        }
        /* In-flight head (or one already severed by sock_close): ASYNC_CANCEL
         * the send like any other op. wcanceled closes the race that used to
         * force a refusal here: if the send completes short before the cancel
         * reaches it, the rearm path completes the task CANCELED instead of
         * resubmitting a send the cancel can no longer find. */
        if (cancel_submit(u, t) < 0)
            return -1;
        t->wcanceled = 1;
        break;
    }
    default:
        /* sock_close cannot be taken back; spawn ran inline and is never
         * pending here. */
        return -1;
    }
    while (!t->done)
        if (!step(u) && !t->done)
            return -1; /* cannot happen: the target op is in flight */
    return t->res.status == MYIO_CANCELED ? 0 : -1;
}

static myio_result impl_task_result(myio *io, const myio_task *task) {
    (void)io;
    return ((const uring_task *)task)->res;
}

static const char *impl_error_str(myio *io, int err) {
    uring_io *u = io_of(io);
    /* Copy into the instance's errbuf so the returned pointer stays valid;
     * myio_default_error_str picks EAI_* vs errno by sign. */
    snprintf(u->errbuf, sizeof u->errbuf, "%s", myio_default_error_str(err));
    return u->errbuf;
}

static unsigned impl_caps(myio *io) {
    (void)io;
    /* io_uring: ops run concurrently and in-flight file reads/writes can be
     * canceled via IORING_OP_ASYNC_CANCEL. No ASYNC_SPAWN - spawn runs the
     * user function inline in submit. No NONBLOCKING_SUBMIT either: tcp_connect
     * resolves DNS with a blocking getaddrinfo inline in submit, which would
     * violate that bit's "submit never blocks on IO" contract. */
    return MYIO_CAP_CONCURRENT_WAIT | MYIO_CAP_CANCEL_FILE;
}

/* ---- lifetime ---- */

static int impl_task_done(myio *io, const myio_task *task) {
    (void)io;
    return ((const uring_task *)task)->done;
}

static void impl_task_free(myio *io, myio_task *task) {
    uring_io *u = io_of(io);
    uring_task *t = task_of(task);
    if (!t->done) {
        /* The kernel holds references to the task's buffers (this is why
         * the header allows free to block): cancel if possible, then drive
         * until the CQE releases them. A drained ring with the task still
         * pending cannot happen (the op is in flight), but would mean no
         * CQE can ever touch it, so falling through is safe regardless. */
        impl_cancel(io, task);
        while (!t->done && step(u)) {
        }
    }
    task_unlink(u, t);
    free(t->path);
    free(t);
}

static void impl_task_detach(myio *io, myio_task *task) {
    uring_io *u = io_of(io);
    uring_task *t = task_of(task);
    if (t->done)
        task_reap(u, t);
    else
        t->detached = 1;
}

static void impl_destroy(myio *io) {
    uring_io *u = io_of(io);
    /* Dismantle the per-socket write FIFOs first: queued non-head writes
     * have no SQE (cancel_submit would target nothing and no CQE will ever
     * come for them), so complete them CANCELED directly — and severing the
     * head's sock pointer keeps its -ECANCELED CQE from promoting a queued
     * write onto the ring in the middle of the drain below. */
    for (uring_sock *s = u->socks; s; s = s->next) {
        if (!s->write_head)
            continue;
        uring_task *w = s->write_head;
        w->sock = NULL;
        for (uring_task *q = w->wnext, *qn; q; q = qn) {
            qn = q->wnext;
            q->wnext = NULL;
            q->sock = NULL;
            task_complete(u, q, -ECANCELED);
        }
        w->wnext = NULL;
        s->write_head = s->write_tail = NULL;
    }
    /* Request cancellation of everything still in flight, then drain until
     * the kernel has delivered every CQE. Closing the ring fd would cancel
     * implicitly, but draining explicitly is what guarantees no CQE ever
     * lands after task memory is freed, and that canceled connects/accepts
     * close the fds they were about to win. */
    for (uring_task *t = u->tasks; t; t = t->next)
        if (!t->done)
            cancel_submit(u, t);
    while (u->inflight > 0) {
        io_uring_submit_and_wait(&u->ring, 1);
        drain_cqes(u);
    }
    /* Detached tasks were reaped as their CQEs arrived; anything left was
     * abandoned by the caller. */
    for (uring_task *t = u->tasks, *next; t; t = next) {
        next = t->next;
        free(t->path);
        free(t);
    }
    for (uring_sock *s = u->socks, *next; s; s = next) {
        next = s->next;
        if (s->fd >= 0)
            close(s->fd);
        free(s);
    }
    io_uring_queue_exit(&u->ring);
    free(u);
}

static const myio_ops uring_ops = {
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

myio *myio_uring_new(void) {
    uring_io *u = calloc(1, sizeof(*u));
    if (!u)
        return NULL;
    if (io_uring_queue_init(256, &u->ring, 0) < 0) {
        free(u); /* old kernel, io_uring_disabled, or seccomp */
        return NULL;
    }
    u->base.ops = &uring_ops;
    return &u->base;
}
