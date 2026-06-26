/* Thread-pool myio backend: every operation is the same blocking POSIX call
 * the sync backend makes, but run on a worker thread so the submitting thread
 * keeps going and several operations make progress at once.
 *
 * Concurrency model
 * -----------------
 * One mutex guards all shared state (the work queue, the socket list, and
 * every task's status fields). Two condition variables sit under it:
 *   - work_cv  - idle workers sleep here until a task is queued;
 *   - done_cv  - the (single) user thread sleeps here in await/select, and
 *                a close worker sleeps here waiting for a socket to drain.
 * Every completion broadcasts done_cv, so all waiters just re-check their
 * own predicate. Per the header's single-thread rule, submit/await/cancel/
 * free are only ever called from the one user thread, so they never race
 * each other - only with the workers, which the mutex serialises.
 *
 * Pool growth
 * -----------
 * The pool starts empty. A submit hands the task to an idle worker if one is
 * waiting; otherwise it spawns a fresh thread. Because the operations block,
 * this is what stops a slow op from starving a newly submitted one: there is
 * always either a free worker or a brand-new one for incoming work. Threads
 * are kept (idle) for reuse and only joined at destroy.
 *
 * Cancellation
 * ------------
 * A task still queued is canceled by lifting it out of the queue. A task a
 * worker is already running is harder: it is blocked in a syscall. The sleep,
 * accept and socket-read operations wait inside ppoll() with a dedicated
 * signal (POOL_SIG) unblocked only for that call; cancel sets the task's
 * cancel_req flag and pthread_kill()s the worker, so the ppoll returns EINTR
 * and the worker reports MYIO_CANCELED. ppoll's atomic mask swap closes the
 * "signal arrives just before the blocking call" race a bare signal would
 * have. Operations with no such interruption point (a running connect, file
 * read/write, or socket write) refuse cancellation; as everywhere, await
 * reports the authoritative outcome. sock_close additionally shuts the socket
 * down to wake anything blocked on it, which then completes as canceled.
 */
#define _GNU_SOURCE /* ppoll */
#include "myio_pool.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* Real-time signal used only to kick a worker out of a cancelable ppoll().
 * Its handler does nothing; the wakeup (EINTR) is the whole point. */
#define POOL_SIG (SIGRTMIN)

typedef struct pool pool;

enum op_kind {
    OP_OPEN,
    OP_CLOSE,
    OP_READ,
    OP_WRITE,
    OP_SLEEP,
    OP_SPAWN,
    OP_CONNECT,
    OP_ACCEPT,
    OP_SOCK_READ,
    OP_SOCK_WRITE,
    OP_SOCK_CLOSE,
};

/* Does this op block on a pre-existing socket's fd? Such ops bump the
 * socket's inflight count so a concurrent close knows to wake and wait for
 * them. */
static int op_on_sock(enum op_kind k) {
    return k == OP_SOCK_READ || k == OP_SOCK_WRITE || k == OP_ACCEPT;
}

/* Does this op block inside a cancelable ppoll(), so a running instance can
 * still be interrupted by a signal? */
static int op_interruptible(enum op_kind k) {
    return k == OP_SLEEP || k == OP_ACCEPT || k == OP_SOCK_READ;
}

typedef struct pool_sock {
    int               fd;
    int               closing;  /* sock_close has begun; ops report CANCELED */
    int               inflight; /* read/write/accept ops touching this fd */
    struct pool_sock *next;
    struct pool_sock *prev;
} pool_sock;

typedef struct pool_task {
    pool            *io;
    enum op_kind     kind;
    /* operation arguments (only the relevant ones are set) */
    char            *path;    /* open path / connect host: malloc'd copy */
    int              flags;
    int              mode;
    int              port;
    int64_t          fd;
    void            *buf;
    size_t           len;
    int64_t          offset;
    uint64_t         ms;
    myio_fn          fn;
    void            *arg;
    pool_sock       *sock;    /* socket the op acts on, if any */
    /* status, all guarded by io->mu */
    int              started; /* a worker has dequeued it (cancel too late) */
    int              done;
    int              detached;
    int              cancel_req; /* cancel asked; an interruptible op honours it */
    pthread_t        worker;  /* thread running it, valid once started */
    myio_result      res;
    struct pool_task *qnext;  /* work-queue link */
} pool_task;

struct pool {
    myio            base;
    pthread_mutex_t mu;
    pthread_cond_t  work_cv;  /* workers wait for queued tasks */
    pthread_cond_t  done_cv;  /* await/select and close-drain wait */
    pool_task      *qhead;
    pool_task      *qtail;
    size_t          nqueued;  /* tasks in the queue, not yet dequeued */
    int             idle;     /* workers parked on work_cv */
    int             shutdown;
    pthread_t      *threads;
    size_t          nthreads;
    size_t          cap_threads;
    pool_sock      *socks;    /* every live socket, for teardown */
    sigset_t        wait_mask; /* thread mask to apply during ppoll: POOL_SIG on */
};

static pool *pool_of(myio *io) { return (pool *)io; }
static pool_task *task_of(myio_task *t) { return (pool_task *)t; }

/* ---- result constructors ---- */

static myio_result r_ok(int64_t value, void *ptr) {
    myio_result r = { MYIO_OK, value, 0, ptr };
    return r;
}
static myio_result r_err(int err) {
    myio_result r = { MYIO_ERROR, 0, err, NULL };
    return r;
}
static myio_result r_canceled(void) {
    myio_result r = { MYIO_CANCELED, 0, 0, NULL };
    return r;
}
/* For a raw syscall return: negative means failure, errno carries the code. */
static myio_result r_from(int64_t n) {
    return n >= 0 ? r_ok(n, NULL) : r_err(errno);
}

/* ---- socket list (all calls hold io->mu) ---- */

static pool_sock *sock_link(pool *p, int fd) {
    pool_sock *s = calloc(1, sizeof *s);
    if (!s)
        return NULL;
    s->fd = fd;
    s->next = p->socks;
    if (p->socks)
        p->socks->prev = s;
    p->socks = s;
    return s;
}

static void sock_unlink(pool *p, pool_sock *s) {
    if (s->prev)
        s->prev->next = s->next;
    else
        p->socks = s->next;
    if (s->next)
        s->next->prev = s->prev;
}

/* ---- task lifetime (reap/queue calls hold io->mu) ---- */

/* Discard a completed task: a socket it won but no one will claim is closed,
 * then the handle's memory is freed. */
static void reap(pool_task *t) {
    if (t->res.ptr) {
        pool_sock *s = t->res.ptr;
        sock_unlink(t->io, s);
        close(s->fd);
        free(s);
    }
    free(t->path);
    free(t);
}

static void queue_push(pool *p, pool_task *t) {
    t->qnext = NULL;
    if (p->qtail)
        p->qtail->qnext = t;
    else
        p->qhead = t;
    p->qtail = t;
    p->nqueued++;
}

static void queue_remove(pool *p, pool_task *t) {
    pool_task *prev = NULL;
    for (pool_task *c = p->qhead; c; prev = c, c = c->qnext) {
        if (c != t)
            continue;
        if (prev)
            prev->qnext = c->qnext;
        else
            p->qhead = c->qnext;
        if (p->qtail == c)
            p->qtail = prev;
        t->qnext = NULL;
        p->nqueued--;
        return;
    }
}

static int spawn_thread(pool *p); /* defined below, after the worker loop */

/* Queue a task and make sure a worker will reach it. A parked worker can only
 * be counted on if it is not already spoken for by an earlier still-queued
 * task, hence the compare against the whole queue length rather than a bare
 * "idle > 0" - otherwise two submits racing one idle worker would strand the
 * second task. Caller holds io->mu. Returns 0 if no worker could be secured. */
static int dispatch_locked(pool *p, pool_task *t) {
    queue_push(p, t);
    if ((size_t)p->idle >= p->nqueued) {
        pthread_cond_signal(&p->work_cv);
        return 1;
    }
    if (spawn_thread(p))
        return 1;
    queue_remove(p, t);
    return 0;
}

/* ---- worker thread ---- */

/* Resolve and connect, mirroring the sync backend. Returns a connected fd, or
 * -1 with *err set (errno, or a getaddrinfo EAI_* code kept verbatim). */
static int gai_errno(int rc) {
    return rc == EAI_SYSTEM ? errno : rc;
}

static int do_connect(const char *host, int port, int *err) {
    char service[16];
    snprintf(service, sizeof service, "%d", port);
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = NULL;
    int rc = getaddrinfo(host, service, &hints, &res);
    if (rc != 0) {
        *err = gai_errno(rc);
        return -1;
    }
    int fd = -1;
    *err = ECONNREFUSED;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) {
            *err = errno;
            continue;
        }
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
            break;
        *err = errno;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

/* Block until `fd` is readable (fd >= 0) or `timeout_ms` elapses (fd < 0, used
 * by sleep), whichever the op needs - but wake early and return -1 if the task
 * is canceled. POOL_SIG is delivered only inside ppoll (the rest of the time a
 * worker keeps it blocked), so a cancel that lands between the flag check and
 * the ppoll is still seen: the signal stays pending and fires the instant
 * ppoll unblocks it. Returns 0 to let the real syscall proceed, -1 if canceled.
 */
static int cancelable_wait(pool_task *t, int fd, int timeout_ms) {
    pool *p = t->io;
    struct timespec deadline;
    int timed = timeout_ms >= 0;
    if (timed) {
        clock_gettime(CLOCK_MONOTONIC, &deadline);
        deadline.tv_sec += timeout_ms / 1000;
        deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if (deadline.tv_nsec >= 1000000000L) {
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000L;
        }
    }
    struct pollfd pfd = { fd, POLLIN, 0 };
    for (;;) {
        pthread_mutex_lock(&p->mu);
        int canceled = t->cancel_req;
        pthread_mutex_unlock(&p->mu);
        if (canceled)
            return -1;
        struct timespec ts, *tsp = NULL;
        if (timed) {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            ts.tv_sec = deadline.tv_sec - now.tv_sec;
            ts.tv_nsec = deadline.tv_nsec - now.tv_nsec;
            if (ts.tv_nsec < 0) {
                ts.tv_sec--;
                ts.tv_nsec += 1000000000L;
            }
            if (ts.tv_sec < 0)
                return 0; /* deadline already passed: sleep is over */
            tsp = &ts;
        }
        int r = ppoll(fd >= 0 ? &pfd : NULL, fd >= 0 ? 1 : 0, tsp,
                      &p->wait_mask);
        if (r == 0 || r > 0)
            return 0; /* timed out, or fd ready: proceed to the syscall */
        if (errno == EINTR)
            continue; /* a cancel (or stray) signal: re-check at the top */
        return 0;     /* other ppoll error: let the syscall report it */
    }
}

/* sock_close: wait until every read/write/accept on the socket has finished
 * (they were woken by the shutdown done at submit time), then close the fd
 * and free the socket. Runs on a worker so it can block without stalling the
 * user thread. */
static void worker_close(pool_task *t) {
    pool *p = t->io;
    pool_sock *s = t->sock;
    pthread_mutex_lock(&p->mu);
    while (s->inflight > 0)
        pthread_cond_wait(&p->done_cv, &p->mu);
    sock_unlink(p, s);
    int fd = s->fd;
    pthread_mutex_unlock(&p->mu);

    close(fd);
    free(s);

    pthread_mutex_lock(&p->mu);
    t->res = r_ok(0, NULL);
    t->done = 1;
    if (t->detached)
        reap(t);
    pthread_cond_broadcast(&p->done_cv);
    pthread_mutex_unlock(&p->mu);
}

/* Run one task to completion. The blocking call happens with no lock held;
 * the result is committed under the lock. */
static void worker_run(pool_task *t) {
    pool *p = t->io;
    myio_result res = r_ok(0, NULL);
    int newfd = -1;   /* connect/accept: fd to wrap into a socket */
    int conn_err = 0;

    switch (t->kind) {
    case OP_OPEN:
        res = r_from(open(t->path, t->flags, t->mode));
        break;
    case OP_CLOSE:
        res = r_from(close((int)t->fd));
        break;
    case OP_READ: {
        ssize_t n = t->offset == MYIO_NO_OFFSET
                        ? read((int)t->fd, t->buf, t->len)
                        : pread((int)t->fd, t->buf, t->len, t->offset);
        res = r_from(n);
        break;
    }
    case OP_WRITE: {
        ssize_t n = t->offset == MYIO_NO_OFFSET
                        ? write((int)t->fd, t->buf, t->len)
                        : pwrite((int)t->fd, t->buf, t->len, t->offset);
        res = r_from(n);
        break;
    }
    case OP_SLEEP:
        if (cancelable_wait(t, -1, t->ms > INT_MAX ? INT_MAX : (int)t->ms) < 0)
            res = r_canceled();
        break;
    case OP_SPAWN: {
        int64_t rc = t->fn(t->arg);
        /* Keep the user's error code verbatim (it is not an errno). */
        res = rc >= 0 ? r_ok(rc, NULL) : r_err((int)-rc);
        break;
    }
    case OP_SOCK_READ:
        if (cancelable_wait(t, (int)t->fd, -1) < 0)
            res = r_canceled();
        else
            res = r_from(recv((int)t->fd, t->buf, t->len, 0));
        break;
    case OP_SOCK_WRITE: {
        /* All-or-error, like the interface contract and the sync backend. */
        size_t off = 0;
        res = r_ok((int64_t)t->len, NULL);
        while (off < t->len) {
            ssize_t n = send((int)t->fd, (const char *)t->buf + off,
                             t->len - off, MSG_NOSIGNAL);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                res = r_err(errno);
                break;
            }
            off += (size_t)n;
        }
        break;
    }
    case OP_CONNECT:
        newfd = do_connect(t->path, t->port, &conn_err);
        res = r_err(conn_err);
        break;
    case OP_ACCEPT:
        if (cancelable_wait(t, (int)t->fd, -1) < 0) {
            res = r_canceled();
        } else {
            newfd = accept((int)t->fd, NULL, NULL);
            res = newfd < 0 ? r_err(errno) : r_ok(0, NULL);
        }
        break;
    case OP_SOCK_CLOSE:
        return; /* dispatched to worker_close */
    }

    pthread_mutex_lock(&p->mu);
    if (t->kind == OP_CONNECT || t->kind == OP_ACCEPT) {
        if (newfd >= 0) {
            pool_sock *s = sock_link(p, newfd);
            if (!s) {
                close(newfd);
                res = r_err(ENOMEM);
            } else {
                res = r_ok(0, s);
            }
        }
    }
    if (op_on_sock(t->kind)) {
        t->sock->inflight--;
        if (t->sock->closing) {
            /* A connection accepted just as the listener closed is dropped. */
            if (res.ptr) {
                pool_sock *s = res.ptr;
                sock_unlink(p, s);
                close(s->fd);
                free(s);
            }
            res = r_canceled();
        }
    }
    t->res = res;
    t->done = 1;
    if (t->detached)
        reap(t);
    /* Always broadcast: an awaiter, or a close worker draining this socket,
     * may be waiting on done_cv. */
    pthread_cond_broadcast(&p->done_cv);
    pthread_mutex_unlock(&p->mu);
}

static void *worker_main(void *arg) {
    pool *p = arg;
    /* Keep POOL_SIG blocked except inside ppoll, so a stray delivery never
     * disturbs anything but a cancelable wait. */
    sigset_t block;
    sigemptyset(&block);
    sigaddset(&block, POOL_SIG);
    pthread_sigmask(SIG_BLOCK, &block, NULL);

    pthread_mutex_lock(&p->mu);
    for (;;) {
        while (!p->qhead && !p->shutdown) {
            p->idle++;
            pthread_cond_wait(&p->work_cv, &p->mu);
            p->idle--;
        }
        if (p->shutdown && !p->qhead)
            break;
        pool_task *t = p->qhead;
        p->qhead = t->qnext;
        if (!p->qhead)
            p->qtail = NULL;
        t->qnext = NULL;
        p->nqueued--;
        t->started = 1;
        t->worker = pthread_self(); /* cancel targets this thread now */
        pthread_mutex_unlock(&p->mu);

        if (t->kind == OP_SOCK_CLOSE)
            worker_close(t);
        else
            worker_run(t);

        pthread_mutex_lock(&p->mu);
    }
    pthread_mutex_unlock(&p->mu);
    return NULL;
}

/* ---- submission ---- */

/* Add one worker thread. Caller holds io->mu. Returns 0 on failure. */
static int spawn_thread(pool *p) {
    if (p->nthreads == p->cap_threads) {
        size_t nc = p->cap_threads ? p->cap_threads * 2 : 8;
        pthread_t *nt = realloc(p->threads, nc * sizeof *nt);
        if (!nt)
            return 0;
        p->threads = nt;
        p->cap_threads = nc;
    }
    if (pthread_create(&p->threads[p->nthreads], NULL, worker_main, p) != 0)
        return 0;
    p->nthreads++;
    return 1;
}

static pool_task *task_new(pool *p, enum op_kind kind) {
    pool_task *t = calloc(1, sizeof *t);
    if (!t)
        return NULL;
    t->io = p;
    t->kind = kind;
    t->res.status = MYIO_PENDING;
    return t;
}

/* Queue a fully-initialised task and make sure a worker will pick it up. */
static myio_task *submit(pool_task *t) {
    pool *p = t->io;
    pthread_mutex_lock(&p->mu);
    if (op_on_sock(t->kind))
        t->sock->inflight++;
    if (!dispatch_locked(p, t)) {
        /* No worker free and none could be created: fail the task now. */
        if (op_on_sock(t->kind))
            t->sock->inflight--;
        t->res = r_err(EAGAIN);
        t->done = 1;
    }
    pthread_mutex_unlock(&p->mu);
    return (myio_task *)t;
}

/* ---- file operations ---- */

static myio_task *impl_open(myio *io, const char *path, int flags, int mode) {
    pool_task *t = task_new(pool_of(io), OP_OPEN);
    if (!t)
        return NULL;
    t->path = strdup(path); /* copied: caller need not keep `path` alive */
    if (!t->path) {
        free(t);
        return NULL;
    }
    t->flags = flags;
    t->mode = mode;
    return submit(t);
}

static myio_task *impl_close(myio *io, int64_t fd) {
    pool_task *t = task_new(pool_of(io), OP_CLOSE);
    if (!t)
        return NULL;
    t->fd = fd;
    return submit(t);
}

static myio_task *impl_read(myio *io, int64_t fd, void *buf, size_t len,
                            int64_t offset) {
    pool_task *t = task_new(pool_of(io), OP_READ);
    if (!t)
        return NULL;
    t->fd = fd;
    t->buf = buf;
    t->len = len;
    t->offset = offset;
    return submit(t);
}

static myio_task *impl_write(myio *io, int64_t fd, const void *buf, size_t len,
                             int64_t offset) {
    pool_task *t = task_new(pool_of(io), OP_WRITE);
    if (!t)
        return NULL;
    t->fd = fd;
    t->buf = (void *)(uintptr_t)buf;
    t->len = len;
    t->offset = offset;
    return submit(t);
}

static myio_task *impl_sleep(myio *io, uint64_t ms) {
    pool_task *t = task_new(pool_of(io), OP_SLEEP);
    if (!t)
        return NULL;
    t->ms = ms;
    return submit(t);
}

static myio_task *impl_spawn(myio *io, myio_fn fn, void *arg) {
    pool_task *t = task_new(pool_of(io), OP_SPAWN);
    if (!t)
        return NULL;
    t->fn = fn;
    t->arg = arg;
    return submit(t);
}

/* ---- TCP ---- */

static myio_task *impl_tcp_connect(myio *io, const char *host, int port) {
    pool_task *t = task_new(pool_of(io), OP_CONNECT);
    if (!t)
        return NULL;
    t->path = strdup(host); /* copied, like `path` in open */
    if (!t->path) {
        free(t);
        return NULL;
    }
    t->port = port;
    return submit(t);
}

static myio_sock *impl_tcp_listen(myio *io, const char *host, int port,
                                  int backlog, int *err) {
    pool *p = pool_of(io);
    char service[16];
    snprintf(service, sizeof service, "%d", port);
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE | AI_NUMERICHOST;
    struct addrinfo *res = NULL;
    int rc = getaddrinfo(host, service, &hints, &res);
    if (rc != 0) {
        if (err)
            *err = gai_errno(rc);
        return NULL;
    }
    int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    int one = 1;
    if (fd >= 0)
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (fd < 0 || bind(fd, res->ai_addr, res->ai_addrlen) != 0 ||
        listen(fd, backlog) != 0) {
        if (err)
            *err = errno;
        if (fd >= 0)
            close(fd);
        freeaddrinfo(res);
        return NULL;
    }
    freeaddrinfo(res);
    pthread_mutex_lock(&p->mu);
    pool_sock *s = sock_link(p, fd);
    pthread_mutex_unlock(&p->mu);
    if (!s) {
        if (err)
            *err = ENOMEM;
        close(fd);
        return NULL;
    }
    return (myio_sock *)s;
}

static myio_task *impl_tcp_accept(myio *io, myio_sock *listener) {
    pool_sock *ls = (pool_sock *)listener;
    pool_task *t = task_new(pool_of(io), OP_ACCEPT);
    if (!t)
        return NULL;
    t->sock = ls;
    t->fd = ls->fd;
    return submit(t);
}

static myio_task *impl_sock_read(myio *io, myio_sock *sock, void *buf,
                                 size_t len) {
    pool_sock *s = (pool_sock *)sock;
    pool_task *t = task_new(pool_of(io), OP_SOCK_READ);
    if (!t)
        return NULL;
    t->sock = s;
    t->fd = s->fd;
    t->buf = buf;
    t->len = len;
    return submit(t);
}

static myio_task *impl_sock_write(myio *io, myio_sock *sock, const void *buf,
                                  size_t len) {
    pool_sock *s = (pool_sock *)sock;
    pool_task *t = task_new(pool_of(io), OP_SOCK_WRITE);
    if (!t)
        return NULL;
    t->sock = s;
    t->fd = s->fd;
    t->buf = (void *)(uintptr_t)buf;
    t->len = len;
    return submit(t);
}

static myio_task *impl_sock_close(myio *io, myio_sock *sock) {
    pool *p = pool_of(io);
    pool_sock *s = (pool_sock *)sock;
    pool_task *t = task_new(p, OP_SOCK_CLOSE);
    if (!t)
        return NULL;
    t->sock = s;
    pthread_mutex_lock(&p->mu);
    s->closing = 1;
    if (s->inflight == 0) {
        /* No op blocked on the socket: close right here, no worker needed. */
        sock_unlink(p, s);
        int fd = s->fd;
        free(s);
        close(fd);
        t->res = r_ok(0, NULL);
        t->done = 1;
        pthread_mutex_unlock(&p->mu);
        return (myio_task *)t;
    }
    /* Wake the blocked read/write/accept so they can finish; a close worker
     * then waits for them to drain. */
    shutdown(s->fd, SHUT_RDWR);
    if (!dispatch_locked(p, t)) {
        t->res = r_err(EAGAIN);
        t->done = 1;
    }
    pthread_mutex_unlock(&p->mu);
    return (myio_task *)t;
}

static int impl_sock_port(myio *io, myio_sock *sock) {
    (void)io;
    struct sockaddr_storage ss;
    socklen_t len = sizeof ss;
    if (getsockname(((pool_sock *)sock)->fd, (struct sockaddr *)&ss, &len) != 0)
        return -1;
    if (ss.ss_family == AF_INET)
        return ntohs(((struct sockaddr_in *)&ss)->sin_port);
    if (ss.ss_family == AF_INET6)
        return ntohs(((struct sockaddr_in6 *)&ss)->sin6_port);
    return -1;
}

/* ---- synchronisation and lifetime ---- */

static myio_result impl_await(myio *io, myio_task *task) {
    pool *p = pool_of(io);
    pool_task *t = task_of(task);
    pthread_mutex_lock(&p->mu);
    while (!t->done)
        pthread_cond_wait(&p->done_cv, &p->mu);
    myio_result r = t->res;
    pthread_mutex_unlock(&p->mu);
    return r;
}

static int impl_cancel(myio *io, myio_task *task) {
    pool *p = pool_of(io);
    pool_task *t = task_of(task);
    int rc = -1;
    pthread_mutex_lock(&p->mu);
    if (t->done) {
        rc = -1;
    } else if (!t->started) {
        /* Still queued: lift it straight out. */
        queue_remove(p, t);
        if (op_on_sock(t->kind))
            t->sock->inflight--;
        t->res = r_canceled();
        t->done = 1;
        if (t->detached)
            reap(t);
        pthread_cond_broadcast(&p->done_cv);
        rc = 0;
    } else if (op_interruptible(t->kind)) {
        /* Running inside a cancelable ppoll: flag it and kick the worker out.
         * The worker reports the outcome (canceled, unless it already
         * completed in the meantime). */
        t->cancel_req = 1;
        pthread_kill(t->worker, POOL_SIG);
        rc = 0;
    }
    pthread_mutex_unlock(&p->mu);
    return rc;
}

static ptrdiff_t impl_select(myio *io, myio_task **tasks, size_t ntasks) {
    pool *p = pool_of(io);
    pthread_mutex_lock(&p->mu);
    for (;;) {
        int any = 0;
        for (size_t i = 0; i < ntasks; i++) {
            if (!tasks[i])
                continue;
            any = 1;
            if (task_of(tasks[i])->done) {
                pthread_mutex_unlock(&p->mu);
                return (ptrdiff_t)i;
            }
        }
        if (!any) {
            pthread_mutex_unlock(&p->mu);
            return -1;
        }
        pthread_cond_wait(&p->done_cv, &p->mu);
    }
}

static const char *impl_error_str(myio *io, int err) {
    (void)io;
    /* Negatives are getaddrinfo's EAI_* range, kept verbatim by connect/
     * listen; everything else is errno-style. */
    return err < 0 ? gai_strerror(err) : strerror(err);
}

static int impl_task_done(myio *io, const myio_task *task) {
    pool *p = pool_of(io);
    pthread_mutex_lock(&p->mu);
    int d = ((const pool_task *)task)->done;
    pthread_mutex_unlock(&p->mu);
    return d;
}

static void impl_task_free(myio *io, myio_task *task) {
    pool *p = pool_of(io);
    pool_task *t = task_of(task);
    pthread_mutex_lock(&p->mu);
    if (!t->done) {
        if (!t->started) {
            /* Still queued: cancel it outright. */
            queue_remove(p, t);
            if (op_on_sock(t->kind))
                t->sock->inflight--;
            t->res = r_canceled();
            t->done = 1;
            pthread_cond_broadcast(&p->done_cv);
        } else {
            /* Running: the worker references this memory, so we must wait it
             * out - but first ask an interruptible op to stop, or the wait
             * could be unbounded (a read with no data ever coming). */
            if (op_interruptible(t->kind)) {
                t->cancel_req = 1;
                pthread_kill(t->worker, POOL_SIG);
            }
            while (!t->done)
                pthread_cond_wait(&p->done_cv, &p->mu);
        }
    }
    char *path = t->path;
    pthread_mutex_unlock(&p->mu);
    /* The result's socket (if any) belongs to the caller; only free the
     * handle, like the other backends. */
    free(path);
    free(t);
}

static void impl_task_detach(myio *io, myio_task *task) {
    pool *p = pool_of(io);
    pool_task *t = task_of(task);
    pthread_mutex_lock(&p->mu);
    if (t->done)
        reap(t);
    else
        t->detached = 1;
    pthread_mutex_unlock(&p->mu);
}

static void impl_destroy(myio *io) {
    pool *p = pool_of(io);
    pthread_mutex_lock(&p->mu);
    p->shutdown = 1;
    /* Drop tasks no worker has started; a queued op might block forever, so
     * it must never run during teardown. Detached ones are reaped (their
     * owners are gone); a well-behaved caller has already freed the rest. */
    for (pool_task *t = p->qhead; t;) {
        pool_task *next = t->qnext;
        if (op_on_sock(t->kind))
            t->sock->inflight--;
        t->res = r_canceled();
        t->done = 1;
        reap(t);
        t = next;
    }
    p->qhead = p->qtail = NULL;
    p->nqueued = 0;
    /* Wake anything blocked on a socket so its worker can exit. */
    for (pool_sock *s = p->socks; s; s = s->next)
        shutdown(s->fd, SHUT_RDWR);
    pthread_cond_broadcast(&p->work_cv);
    pthread_cond_broadcast(&p->done_cv);
    size_t n = p->nthreads;
    pthread_mutex_unlock(&p->mu);

    for (size_t i = 0; i < n; i++)
        pthread_join(p->threads[i], NULL);

    /* No workers left: free any sockets the caller never closed. */
    for (pool_sock *s = p->socks; s;) {
        pool_sock *next = s->next;
        close(s->fd);
        free(s);
        s = next;
    }
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->work_cv);
    pthread_cond_destroy(&p->done_cv);
    free(p->threads);
    free(p);
}

static const myio_ops pool_ops = {
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
    .await       = impl_await,
    .cancel      = impl_cancel,
    .select      = impl_select,
    .error_str   = impl_error_str,
    .task_done   = impl_task_done,
    .task_free   = impl_task_free,
    .task_detach = impl_task_detach,
    .destroy     = impl_destroy,
};

/* POOL_SIG's handler does nothing: its only job is to make ppoll return
 * EINTR. Installed process-wide once, the first time a pool is created. */
static void pool_sig_handler(int sig) { (void)sig; }

static pthread_once_t sig_once = PTHREAD_ONCE_INIT;
static void install_sig(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = pool_sig_handler; /* no SA_RESTART: we want EINTR */
    sigemptyset(&sa.sa_mask);
    sigaction(POOL_SIG, &sa, NULL);
}

myio *myio_pool_new(void) {
    pthread_once(&sig_once, install_sig);

    pool *p = calloc(1, sizeof *p);
    if (!p)
        return NULL;
    /* Mask used during a worker's ppoll: the current mask with POOL_SIG
     * unblocked, so the cancel signal can be delivered there and nowhere
     * else. */
    pthread_sigmask(SIG_BLOCK, NULL, &p->wait_mask);
    sigdelset(&p->wait_mask, POOL_SIG);
    if (pthread_mutex_init(&p->mu, NULL) != 0) {
        free(p);
        return NULL;
    }
    if (pthread_cond_init(&p->work_cv, NULL) != 0) {
        pthread_mutex_destroy(&p->mu);
        free(p);
        return NULL;
    }
    if (pthread_cond_init(&p->done_cv, NULL) != 0) {
        pthread_cond_destroy(&p->work_cv);
        pthread_mutex_destroy(&p->mu);
        free(p);
        return NULL;
    }
    p->base.ops = &pool_ops;
    return &p->base;
}
