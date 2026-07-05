/* Thread-pool myio backend on Zephyr: every operation is the same blocking
 * call the event-loop backend makes non-blocking, but run on a k_thread
 * worker so the submitting thread keeps going and several operations make
 * progress at once. The structure mirrors the desktop pool backend
 * (src/myio_pool.c) with Zephyr substitutions where POSIX primitives do
 * not exist:
 *
 * - pthread mutex/condvars -> one k_mutex and two k_condvars with the same
 *   roles: work_cv (idle workers wait for queued tasks) and done_cv
 *   (step() - the primitive under the header's await/select loops - and
 *   the close-drain wait; every completion broadcasts).
 * - POOL_SIG + ppoll -> a per-worker eventfd. A cancelable wait blocks in
 *   zsock_poll() over {the worker's eventfd, the operation's fd}; cancel
 *   sets the task's cancel_req flag and writes the eventfd. The eventfd
 *   counter is stateful, so a kick that lands before the poll starts is
 *   still seen - the atomic-mask-swap race ppoll exists to close cannot
 *   happen here at all.
 * - The growing pool -> a fixed set of CONFIG_MYIO_POOL_WORKERS workers on
 *   static stacks. A submit with every worker busy queues; cancellation
 *   can still lift a queued task out. The desktop pool grows a thread per
 *   blocked op precisely so queued work is never starved - a fixed pool
 *   cannot, so size the worker count for the maximum number of
 *   concurrently blocking operations, like any static RTOS resource.
 * - malloc/strdup -> k_mem_slabs for tasks and sockets, and a bounded
 *   in-task copy of open's path / connect's host (CONFIG_MYIO_STR_MAX;
 *   longer strings fail the submission with ENAMETOOLONG).
 *
 * Writes on one socket are serialised through a per-socket FIFO (write_head/
 * write_tail, linked by wnext, in submission order), exactly as the desktop
 * pool does: the worker running a write waits on done_cv until its task
 * reaches the head, then sends, so the bytes of overlapping writes never
 * interleave on the wire.
 *
 * Cancellation follows the desktop pool: a queued task is lifted out; a
 * running sleep, accept or socket read wakes through the eventfd and
 * reports MYIO_CANCELED; a socket write still waiting its turn behind an
 * earlier write cancels too (no bytes on the wire yet); a running connect,
 * file op, the socket write actually sending, or a spawned function refuses.
 * sock_close marks the socket closing, kicks every blocked op on it and wakes
 * writers waiting their turn (they report CANCELED), and a close worker waits
 * for them to drain before closing the fd. zsock_shutdown() is issued
 * best-effort to also wake a blocked send - the offloaded-sockets driver
 * does not implement it, so there the kick is what does the waking.
 */
#include "myio_zephyr_pool.h"

#include <string.h>
#include <stdio.h>
#include <fcntl.h>

#include <zephyr/kernel.h>
#include <zephyr/net/socket.h>
#include <zephyr/sys/fdtable.h>
#include <zephyr/zvfs/eventfd.h>
#ifdef CONFIG_FILE_SYSTEM
#include <zephyr/fs/fs.h>
#endif

typedef struct zp_io     zp_io;
typedef struct zp_task   zp_task;
typedef struct zp_sock   zp_sock;
typedef struct zp_worker zp_worker;

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
    OP_NONE, /* completed at submit time (EBUSY and friends) */
};

/* Does this op block on a pre-existing socket's fd? Such ops bump the
 * socket's inflight count so a concurrent close knows to wake and wait for
 * them. */
static int op_on_sock(enum op_kind k) {
    return k == OP_SOCK_READ || k == OP_SOCK_WRITE || k == OP_ACCEPT;
}

/* Does a running instance wait inside a cancelable poll, so it can be
 * kicked through its worker's eventfd? */
static int op_interruptible(enum op_kind k) {
    return k == OP_SLEEP || k == OP_ACCEPT || k == OP_SOCK_READ;
}

struct zp_sock {
    int      fd;
    int      port;        /* listener bind port: sock_port fallback */
    int      closing;     /* sock_close has begun; ops report CANCELED */
    int      inflight;    /* read/write/accept ops touching this fd */
    zp_task *read_task;   /* outstanding sock_read (at most one) */
    zp_task *accept_task; /* outstanding tcp_accept (at most one) */
    /* Outstanding sock_writes, FIFO in submission order. A write's worker
     * only sends once its task reaches write_head; the rest wait on done_cv,
     * so overlapping writes never interleave on the wire. */
    zp_task *write_head;
    zp_task *write_tail;
    zp_sock *next;
};

struct zp_task {
    zp_io          *io;
    enum op_kind    kind;
    /* operation arguments (only the relevant ones are set) */
    char            str[CONFIG_MYIO_STR_MAX]; /* open path / connect host */
    int             flags;
    int             port;
    int64_t         fd;
    void           *buf;
    size_t          len;
    int64_t         offset;
    uint64_t        ms;
    myio_fn         fn;
    void           *arg;
    zp_sock        *sock;       /* socket the op acts on, if any */
    /* status, all guarded by io->mu */
    int             started;    /* a worker has dequeued it */
    int             done;
    int             detached;
    int             cancel_req; /* an interruptible op honours it */
    zp_worker      *worker;     /* worker running it, valid once started */
    myio_result     res;
    zp_task        *qnext;      /* work-queue link */
    zp_task        *wnext;      /* sock_write: per-socket write FIFO link */
};

struct zp_worker {
    struct k_thread thread;
    int             evfd;    /* cancel kick: stateful, pollable */
    zp_task        *current; /* task being run, guarded by io->mu (destroy
                                flags it canceled without a task list) */
};

struct zp_io {
    myio             base;
    int              in_use;
    struct k_mutex   mu;
    struct k_condvar work_cv;
    struct k_condvar done_cv;
    zp_task         *qhead;
    zp_task         *qtail;
    int              nqueued;  /* tasks in the queue, not yet dequeued */
    int              nrunning; /* tasks a worker has dequeued, not yet done */
    /* Completion generation, for step(): completions counts every task a
     * worker finishes; comp_seen is how many the user thread has observed
     * through step(). A gap between them is progress step() can report
     * without waiting - this is what keeps a completion that lands between
     * the caller's task_done poll and step()'s condvar wait from being
     * missed (its broadcast would otherwise have gone unheard). */
    uint32_t         completions;
    uint32_t         comp_seen;
    int              shutdown;
    zp_sock         *socks;   /* every live socket, for teardown */
    zp_worker        workers[CONFIG_MYIO_POOL_WORKERS];
    struct k_mutex   fs_mu;   /* serialises compound seek+rw file ops */
    char             errbuf[64];
#ifdef CONFIG_FILE_SYSTEM
    struct fs_file_t files[CONFIG_MYIO_MAX_FILES];
    uint8_t          file_used[CONFIG_MYIO_MAX_FILES];
#endif
};

K_MEM_SLAB_DEFINE_STATIC(zp_task_slab, sizeof(zp_task), CONFIG_MYIO_MAX_TASKS,
                         __alignof__(zp_task));
K_MEM_SLAB_DEFINE_STATIC(zp_sock_slab, sizeof(zp_sock),
                         CONFIG_MYIO_MAX_SOCKETS, __alignof__(zp_sock));
K_THREAD_STACK_ARRAY_DEFINE(zp_stacks, CONFIG_MYIO_POOL_WORKERS,
                            CONFIG_MYIO_POOL_STACK_SIZE);

static zp_io *io_of(myio *io) { return (zp_io *)io; }
static zp_task *task_of(myio_task *t) { return (zp_task *)t; }

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

/* ---- socket list (all calls hold io->mu) ---- */

static zp_sock *sock_link(zp_io *p, int fd) {
    zp_sock *s;
    if (k_mem_slab_alloc(&zp_sock_slab, (void **)&s, K_NO_WAIT) != 0)
        return NULL;
    memset(s, 0, sizeof(*s));
    s->fd = fd;
    s->next = p->socks;
    p->socks = s;
    return s;
}

static void sock_unlink(zp_io *p, zp_sock *s) {
    for (zp_sock **c = &p->socks; *c; c = &(*c)->next) {
        if (*c == s) {
            *c = s->next;
            break;
        }
    }
}

/* ---- task lifetime (all calls hold io->mu) ---- */

/* Discard a completed task: a socket it won but no one will claim is
 * closed, then the slab block is released. */
static void reap(zp_task *t) {
    if (t->res.ptr) {
        zp_sock *s = t->res.ptr;
        sock_unlink(t->io, s);
        zsock_close(s->fd);
        k_mem_slab_free(&zp_sock_slab, s);
    }
    k_mem_slab_free(&zp_task_slab, t);
}

static void queue_push(zp_io *p, zp_task *t) {
    t->qnext = NULL;
    if (p->qtail)
        p->qtail->qnext = t;
    else
        p->qhead = t;
    p->qtail = t;
    p->nqueued++;
}

static void queue_remove(zp_io *p, zp_task *t) {
    zp_task *prev = NULL;
    for (zp_task *c = p->qhead; c; prev = c, c = c->qnext) {
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

/* Clear the per-socket one-outstanding-op slots this task occupies. */
static void sock_slots_clear(zp_task *t) {
    if (t->kind == OP_SOCK_READ && t->sock->read_task == t)
        t->sock->read_task = NULL;
    if (t->kind == OP_ACCEPT && t->sock->accept_task == t)
        t->sock->accept_task = NULL;
}

/* ---- per-socket write FIFO (all calls hold io->mu) ---- */

static void write_enqueue(zp_sock *s, zp_task *t) {
    t->wnext = NULL;
    if (s->write_tail)
        s->write_tail->wnext = t;
    else
        s->write_head = t;
    s->write_tail = t;
}

static void write_dequeue(zp_sock *s, zp_task *t) {
    zp_task *prev = NULL;
    for (zp_task *c = s->write_head; c; prev = c, c = c->wnext) {
        if (c != t)
            continue;
        if (prev)
            prev->wnext = c->wnext;
        else
            s->write_head = c->wnext;
        if (s->write_tail == c)
            s->write_tail = prev;
        t->wnext = NULL;
        return;
    }
}

/* Complete a task that never ran (canceled while queued, or dropped at
 * destroy). Caller holds io->mu. */
static void complete_unrun(zp_io *p, zp_task *t, myio_result res) {
    queue_remove(p, t);
    if (op_on_sock(t->kind)) {
        t->sock->inflight--;
        sock_slots_clear(t);
        if (t->kind == OP_SOCK_WRITE)
            write_dequeue(t->sock, t);
    }
    t->res = res;
    t->done = 1;
    if (t->detached)
        reap(t);
    k_condvar_broadcast(&p->done_cv);
}

/* ---- cancelable waiting (worker side) ---- */

/* Kick a worker out of a cancelable wait. Caller holds io->mu and has set
 * t->cancel_req; the write makes any current or future poll return. */
static void worker_kick(zp_task *t) {
    if (t->worker)
        zvfs_eventfd_write(t->worker->evfd, 1);
}

/* Block until `fd` is readable (fd >= 0) or `timeout_ms` elapses (fd < 0,
 * used by sleep) - waking early and returning -1 if the task is canceled.
 * A stale kick left over from a cancel that lost its race is drained and
 * ignored: the cancel_req flag, not the eventfd, is authoritative. Returns
 * 0 to let the real call proceed. */
static int cancelable_wait(zp_task *t, int fd, int64_t timeout_ms) {
    zp_io *p = t->io;
    int evfd = t->worker->evfd;
    int64_t deadline = timeout_ms >= 0 ? k_uptime_get() + timeout_ms : 0;
    for (;;) {
        k_mutex_lock(&p->mu, K_FOREVER);
        int canceled = t->cancel_req;
        k_mutex_unlock(&p->mu);
        if (canceled)
            return -1;
        struct zsock_pollfd fds[2] = {
            { .fd = evfd, .events = ZSOCK_POLLIN },
            { .fd = fd, .events = ZSOCK_POLLIN },
        };
        int timeout = -1;
        if (timeout_ms >= 0) {
            int64_t d = deadline - k_uptime_get();
            if (d <= 0)
                return 0; /* deadline passed: the sleep is over */
            timeout = d > INT32_MAX ? INT32_MAX : (int)d;
        }
        int rc = zsock_poll(fds, fd >= 0 ? 2 : 1, timeout);
        if (rc == 0)
            return 0; /* timed out: the sleep is over */
        if (rc < 0)
            return 0; /* let the real call report the problem */
        if (fds[0].revents) {
            zvfs_eventfd_t v;
            zvfs_eventfd_read(evfd, &v); /* drain; the flag decides above */
            continue;
        }
        return 0; /* fd is ready */
    }
}

/* ---- the workers ---- */

#ifdef CONFIG_FILE_SYSTEM
/* Compound seek+transfer+restore under fs_mu: two workers touching the
 * same file must not interleave the position juggling. */
static myio_result file_rw(zp_task *t) {
    zp_io *p = t->io;
    struct fs_file_t *f = &p->files[t->fd];
    k_mutex_lock(&p->fs_mu, K_FOREVER);
    off_t saved = 0;
    int rc = 0;
    if (t->offset != MYIO_NO_OFFSET) {
        saved = fs_tell(f);
        rc = saved < 0 ? (int)saved : fs_seek(f, (off_t)t->offset, FS_SEEK_SET);
    }
    ssize_t n = 0;
    if (rc == 0) {
        n = t->kind == OP_WRITE ? fs_write(f, t->buf, t->len)
                                : fs_read(f, t->buf, t->len);
        if (n < 0)
            rc = (int)n;
        if (t->offset != MYIO_NO_OFFSET)
            fs_seek(f, saved, FS_SEEK_SET);
    }
    k_mutex_unlock(&p->fs_mu);
    return rc < 0 ? r_err(-rc) : r_ok(n, NULL);
}
#endif

/* Resolve and connect, blocking, mirroring the desktop pool: every
 * resolved address is tried in turn. Returns a connected fd or -1 with
 * *err set (errno-style, or a negative EAI_* code kept verbatim). */
static int do_connect(const char *host, int port, int *err) {
    char service[8];
    snprintf(service, sizeof service, "%d", port);
    struct zsock_addrinfo hints = { .ai_socktype = SOCK_STREAM };
    struct zsock_addrinfo *res = NULL;
    int rc = zsock_getaddrinfo(host, service, &hints, &res);
    if (rc != 0) {
        /* DNS_EAI_SYSTEM defers to errno, like glibc's EAI_SYSTEM. */
        *err = rc == DNS_EAI_SYSTEM ? errno : (rc < 0 ? rc : -rc);
        return -1;
    }
    int fd = -1;
    *err = ECONNREFUSED;
    for (struct zsock_addrinfo *ai = res; ai; ai = ai->ai_next) {
        fd = zsock_socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) {
            *err = errno;
            continue;
        }
        if (zsock_connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
            break;
        *err = errno;
        zsock_close(fd);
        fd = -1;
    }
    zsock_freeaddrinfo(res);
    return fd;
}

/* sock_close: wait until every read/write/accept on the socket has
 * finished (they were kicked at submit time), then close the fd and free
 * the socket. Runs on a worker so it can block without stalling the user
 * thread. */
static void worker_close(zp_task *t) {
    zp_io *p = t->io;
    zp_sock *s = t->sock;
    k_mutex_lock(&p->mu, K_FOREVER);
    while (s->inflight > 0)
        k_condvar_wait(&p->done_cv, &p->mu, K_FOREVER);
    sock_unlink(p, s);
    int fd = s->fd;
    k_mem_slab_free(&zp_sock_slab, s);
    k_mutex_unlock(&p->mu);

    zsock_close(fd);

    k_mutex_lock(&p->mu, K_FOREVER);
    t->res = r_ok(0, NULL);
    t->done = 1;
    p->nrunning--;
    p->completions++;
    if (t->detached)
        reap(t);
    k_condvar_broadcast(&p->done_cv);
    k_mutex_unlock(&p->mu);
}

/* Run one task to completion. The blocking call happens with no lock held;
 * the result is committed under the lock. */
static void worker_run(zp_task *t) {
    zp_io *p = t->io;
    myio_result res = r_ok(0, NULL);
    int newfd = -1; /* connect/accept: fd to wrap into a socket */

    /* An op queued behind a sock_close must not run against the dying fd
     * (it would block with no close kick coming - close only kicks what
     * was in flight when it was submitted). */
    if (op_on_sock(t->kind)) {
        k_mutex_lock(&p->mu, K_FOREVER);
        int closing = t->sock->closing;
        k_mutex_unlock(&p->mu);
        if (closing) {
            res = r_canceled();
            goto commit;
        }
    }

    switch (t->kind) {
#ifdef CONFIG_FILE_SYSTEM
    case OP_OPEN: {
        fs_mode_t zflags = 0;
        switch (t->flags & O_ACCMODE) {
        case O_RDONLY: zflags = FS_O_READ; break;
        case O_WRONLY: zflags = FS_O_WRITE; break;
        default:       zflags = FS_O_RDWR; break;
        }
        if (t->flags & O_CREAT)
            zflags |= FS_O_CREATE;
        if (t->flags & O_APPEND)
            zflags |= FS_O_APPEND;
        if (t->flags & O_TRUNC)
            zflags |= FS_O_TRUNC;
        k_mutex_lock(&p->mu, K_FOREVER);
        int slot = -1;
        for (int i = 0; i < CONFIG_MYIO_MAX_FILES; i++) {
            if (!p->file_used[i]) {
                slot = i;
                p->file_used[i] = 1; /* reserved; released below on failure */
                break;
            }
        }
        k_mutex_unlock(&p->mu);
        if (slot < 0) {
            res = r_err(EMFILE);
            break;
        }
        fs_file_t_init(&p->files[slot]);
        int rc = fs_open(&p->files[slot], t->str, zflags);
        if (rc < 0) {
            k_mutex_lock(&p->mu, K_FOREVER);
            p->file_used[slot] = 0;
            k_mutex_unlock(&p->mu);
            res = r_err(-rc);
        } else {
            res = r_ok(slot, NULL);
        }
        break;
    }
    case OP_CLOSE: {
        int rc = fs_close(&p->files[t->fd]);
        if (rc == 0) {
            k_mutex_lock(&p->mu, K_FOREVER);
            p->file_used[t->fd] = 0;
            k_mutex_unlock(&p->mu);
        }
        res = rc < 0 ? r_err(-rc) : r_ok(0, NULL);
        break;
    }
    case OP_READ:
    case OP_WRITE:
        res = file_rw(t);
        break;
#else
    case OP_OPEN:
    case OP_CLOSE:
    case OP_READ:
    case OP_WRITE:
        res = r_err(ENOSYS);
        break;
#endif
    case OP_SLEEP:
        if (cancelable_wait(t, -1, (int64_t)t->ms) < 0)
            res = r_canceled();
        break;
    case OP_SPAWN: {
        int64_t rc = t->fn(t->arg);
        /* Keep the user's error code verbatim (it is not an errno). */
        res = rc >= 0 ? r_ok(rc, NULL) : r_err((int)-rc);
        break;
    }
    case OP_SOCK_READ:
        if (cancelable_wait(t, (int)t->fd, -1) < 0) {
            res = r_canceled();
        } else {
            ssize_t n = zsock_recv((int)t->fd, t->buf, t->len, 0);
            res = n < 0 ? r_err(errno) : r_ok(n, NULL);
        }
        break;
    case OP_SOCK_WRITE: {
        /* Wait for this write's turn: only the FIFO head sends, so the bytes
         * of overlapping writes never interleave on the wire. Bail out
         * without sending if the socket is closing/tearing down or the wait
         * was canceled - the commit below reports CANCELED. */
        k_mutex_lock(&p->mu, K_FOREVER);
        while (t->sock->write_head != t && !t->sock->closing &&
               !p->shutdown && !t->cancel_req)
            k_condvar_wait(&p->done_cv, &p->mu, K_FOREVER);
        int stop = t->sock->closing || p->shutdown || t->cancel_req;
        k_mutex_unlock(&p->mu);
        if (stop) {
            res = r_canceled();
            break;
        }
        /* All-or-error, per the interface contract. Blocking sends. */
        size_t off = 0;
        res = r_ok((int64_t)t->len, NULL);
        while (off < t->len) {
            ssize_t n = zsock_send((int)t->fd, (const char *)t->buf + off,
                                   t->len - off, 0);
            if (n < 0) {
                res = r_err(errno);
                break;
            }
            off += (size_t)n;
        }
        break;
    }
    case OP_CONNECT: {
        int err = 0;
        newfd = do_connect(t->str, t->port, &err);
        res = r_err(err);
        break;
    }
    case OP_ACCEPT:
        if (cancelable_wait(t, (int)t->fd, -1) < 0) {
            res = r_canceled();
        } else {
            newfd = zsock_accept((int)t->fd, NULL, NULL);
            res = newfd < 0 ? r_err(errno) : r_ok(0, NULL);
        }
        break;
    default:
        break; /* OP_SOCK_CLOSE dispatched to worker_close, OP_NONE never runs */
    }

commit:
    k_mutex_lock(&p->mu, K_FOREVER);
    if (newfd >= 0) {
        zp_sock *s = sock_link(p, newfd);
        if (!s) {
            zsock_close(newfd);
            res = r_err(ENOMEM);
        } else {
            res = r_ok(0, s);
        }
    }
    if (op_on_sock(t->kind)) {
        t->sock->inflight--;
        sock_slots_clear(t);
        if (t->kind == OP_SOCK_WRITE)
            write_dequeue(t->sock, t); /* let the next queued write take over */
        if (t->sock->closing) {
            /* A connection accepted just as the listener closed is dropped. */
            if (res.ptr) {
                zp_sock *s = res.ptr;
                sock_unlink(p, s);
                zsock_close(s->fd);
                k_mem_slab_free(&zp_sock_slab, s);
            }
            res = r_canceled();
        }
    }
    t->res = res;
    t->done = 1;
    p->nrunning--;
    p->completions++;
    if (t->detached)
        reap(t);
    /* Always broadcast: an awaiter, or a close worker draining this
     * socket, may be waiting on done_cv. */
    k_condvar_broadcast(&p->done_cv);
    k_mutex_unlock(&p->mu);
}

static void worker_main(void *p1, void *p2, void *p3) {
    zp_io *p = p1;
    zp_worker *w = p2;
    (void)p3;
    k_mutex_lock(&p->mu, K_FOREVER);
    for (;;) {
        while (!p->qhead && !p->shutdown)
            k_condvar_wait(&p->work_cv, &p->mu, K_FOREVER);
        if (p->shutdown && !p->qhead)
            break;
        zp_task *t = p->qhead;
        p->qhead = t->qnext;
        if (!p->qhead)
            p->qtail = NULL;
        t->qnext = NULL;
        p->nqueued--;
        p->nrunning++;
        t->started = 1;
        t->worker = w; /* cancel kicks this worker's eventfd now */
        w->current = t;
        k_mutex_unlock(&p->mu);

        if (t->kind == OP_SOCK_CLOSE)
            worker_close(t);
        else
            worker_run(t);

        k_mutex_lock(&p->mu, K_FOREVER);
        w->current = NULL;
    }
    k_mutex_unlock(&p->mu);
}

/* ---- submission ---- */

static zp_task *task_new(zp_io *p, enum op_kind kind) {
    zp_task *t;
    if (k_mem_slab_alloc(&zp_task_slab, (void **)&t, K_NO_WAIT) != 0)
        return NULL; /* slab exhausted: the documented NULL-on-OOM path */
    memset(t, 0, sizeof(*t));
    t->io = p;
    t->kind = kind;
    t->res.status = MYIO_PENDING;
    return t;
}

/* A submission that failed before anything was queued. */
static myio_task *task_new_failed(zp_io *p, int err) {
    zp_task *t = task_new(p, OP_NONE);
    if (!t)
        return NULL;
    t->res = r_err(err);
    t->done = 1;
    return (myio_task *)t;
}

/* Queue a fully-initialised task and wake a worker for it. */
static myio_task *submit(zp_task *t) {
    zp_io *p = t->io;
    k_mutex_lock(&p->mu, K_FOREVER);
    if (op_on_sock(t->kind))
        t->sock->inflight++;
    if (t->kind == OP_SOCK_WRITE)
        write_enqueue(t->sock, t); /* submission-order turn, under the lock */
    queue_push(p, t);
    k_condvar_signal(&p->work_cv);
    k_mutex_unlock(&p->mu);
    return (myio_task *)t;
}

/* Copy `s` into the task's bounded string field; the header promises path
 * and host are copied by submit, and a slab task cannot hold arbitrary
 * lengths. */
static int str_copy(zp_task *t, const char *s) {
    size_t n = strlen(s);
    if (n >= sizeof t->str)
        return -1;
    memcpy(t->str, s, n + 1);
    return 0;
}

/* ---- file operations ---- */

static myio_task *impl_open(myio *io, const char *path, int flags, int mode) {
    zp_io *p = io_of(io);
    (void)mode; /* Zephyr filesystems carry no permission bits */
    zp_task *t = task_new(p, OP_OPEN);
    if (!t)
        return NULL;
    if (str_copy(t, path) < 0) {
        t->res = r_err(ENAMETOOLONG);
        t->done = 1;
        return (myio_task *)t;
    }
    t->flags = flags;
    return submit(t);
}

static myio_task *impl_close(myio *io, int64_t fd) {
    zp_io *p = io_of(io);
    if (fd < 0 || fd >= CONFIG_MYIO_MAX_FILES || !p->file_used[fd])
        return task_new_failed(p, EBADF);
    zp_task *t = task_new(p, OP_CLOSE);
    if (!t)
        return NULL;
    t->fd = fd;
    return submit(t);
}

static myio_task *impl_file_rw(zp_io *p, enum op_kind kind, int64_t fd,
                               void *buf, size_t len, int64_t offset) {
    if (fd < 0 || fd >= CONFIG_MYIO_MAX_FILES || !p->file_used[fd])
        return task_new_failed(p, EBADF);
    zp_task *t = task_new(p, kind);
    if (!t)
        return NULL;
    t->fd = fd;
    t->buf = buf;
    t->len = len;
    t->offset = offset;
    return submit(t);
}

static myio_task *impl_read(myio *io, int64_t fd, void *buf, size_t len,
                            int64_t offset) {
    return impl_file_rw(io_of(io), OP_READ, fd, buf, len, offset);
}

static myio_task *impl_write(myio *io, int64_t fd, const void *buf, size_t len,
                             int64_t offset) {
    return impl_file_rw(io_of(io), OP_WRITE, fd, (void *)(uintptr_t)buf, len,
                        offset);
}

static myio_task *impl_sleep(myio *io, uint64_t ms) {
    zp_task *t = task_new(io_of(io), OP_SLEEP);
    if (!t)
        return NULL;
    t->ms = ms;
    return submit(t);
}

static myio_task *impl_spawn(myio *io, myio_fn fn, void *arg) {
    zp_task *t = task_new(io_of(io), OP_SPAWN);
    if (!t)
        return NULL;
    t->fn = fn;
    t->arg = arg;
    return submit(t);
}

/* ---- TCP ---- */

static myio_task *impl_tcp_connect(myio *io, const char *host, int port) {
    zp_task *t = task_new(io_of(io), OP_CONNECT);
    if (!t)
        return NULL;
    if (str_copy(t, host) < 0) {
        t->res = r_err(ENAMETOOLONG);
        t->done = 1;
        return (myio_task *)t;
    }
    t->port = port;
    return submit(t);
}

static myio_sock *impl_tcp_listen(myio *io, const char *host, int port,
                                  int backlog, int *err) {
    zp_io *p = io_of(io);
    struct sockaddr_storage addr;
    socklen_t alen;
    memset(&addr, 0, sizeof addr);
    struct sockaddr_in *a4 = (struct sockaddr_in *)&addr;
    struct sockaddr_in6 *a6 = (struct sockaddr_in6 *)&addr;
    if (zsock_inet_pton(AF_INET, host, &a4->sin_addr) == 1) {
        a4->sin_family = AF_INET;
        a4->sin_port = htons(port);
        alen = sizeof *a4;
    } else if (zsock_inet_pton(AF_INET6, host, &a6->sin6_addr) == 1) {
        a6->sin6_family = AF_INET6;
        a6->sin6_port = htons(port);
        alen = sizeof *a6;
    } else {
        if (err)
            *err = EINVAL;
        return NULL;
    }
    int fd = zsock_socket(addr.ss_family, SOCK_STREAM, IPPROTO_TCP);
    if (fd < 0) {
        if (err)
            *err = errno;
        return NULL;
    }
    int one = 1;
    zsock_setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    if (zsock_bind(fd, (struct sockaddr *)&addr, alen) < 0 ||
        zsock_listen(fd, backlog) < 0) {
        if (err)
            *err = errno;
        zsock_close(fd);
        return NULL;
    }
    k_mutex_lock(&p->mu, K_FOREVER);
    zp_sock *s = sock_link(p, fd);
    k_mutex_unlock(&p->mu);
    if (!s) {
        if (err)
            *err = ENOMEM;
        zsock_close(fd);
        return NULL;
    }
    s->port = port;
    return (myio_sock *)s;
}

static myio_task *impl_tcp_accept(myio *io, myio_sock *listener) {
    zp_io *p = io_of(io);
    zp_sock *ls = (zp_sock *)listener;
    if (ls->accept_task)
        return task_new_failed(p, EBUSY);
    zp_task *t = task_new(p, OP_ACCEPT);
    if (!t)
        return NULL;
    t->sock = ls;
    t->fd = ls->fd;
    ls->accept_task = t;
    return submit(t);
}

static myio_task *impl_sock_read(myio *io, myio_sock *sock, void *buf,
                                 size_t len) {
    zp_io *p = io_of(io);
    zp_sock *s = (zp_sock *)sock;
    if (s->read_task)
        return task_new_failed(p, EBUSY);
    zp_task *t = task_new(p, OP_SOCK_READ);
    if (!t)
        return NULL;
    t->sock = s;
    t->fd = s->fd;
    t->buf = buf;
    t->len = len;
    s->read_task = t;
    return submit(t);
}

static myio_task *impl_sock_write(myio *io, myio_sock *sock, const void *buf,
                                  size_t len) {
    zp_sock *s = (zp_sock *)sock;
    zp_task *t = task_new(io_of(io), OP_SOCK_WRITE);
    if (!t)
        return NULL;
    t->sock = s;
    t->fd = s->fd;
    t->buf = (void *)(uintptr_t)buf;
    t->len = len;
    return submit(t);
}

static myio_task *impl_sock_close(myio *io, myio_sock *sock) {
    zp_io *p = io_of(io);
    zp_sock *s = (zp_sock *)sock;
    zp_task *t = task_new(p, OP_SOCK_CLOSE);
    if (!t)
        return NULL;
    t->sock = s;
    k_mutex_lock(&p->mu, K_FOREVER);
    s->closing = 1;
    /* Stop the socket's pull-style tasks: a queued one is lifted out, a
     * running one is flagged and kicked so its cancelable wait returns. */
    zp_task *slots[2] = { s->read_task, s->accept_task };
    for (int i = 0; i < 2; i++) {
        zp_task *c = slots[i];
        if (!c)
            continue;
        if (!c->started) {
            complete_unrun(p, c, r_canceled());
        } else {
            c->cancel_req = 1;
            worker_kick(c);
        }
    }
    if (s->inflight == 0) {
        /* Nothing blocked on the socket: close right here, no worker
         * needed. */
        sock_unlink(p, s);
        int fd = s->fd;
        k_mem_slab_free(&zp_sock_slab, s);
        t->res = r_ok(0, NULL);
        t->done = 1;
        k_mutex_unlock(&p->mu);
        zsock_close(fd);
        return (myio_task *)t;
    }
    /* The shutdown is best-effort: it wakes a blocked send where the stack
     * supports it (the offloaded-sockets driver does not implement it). The
     * broadcast wakes writers still waiting their turn on done_cv, which see
     * `closing` and complete CANCELED without sending. */
    zsock_shutdown(s->fd, ZSOCK_SHUT_RDWR);
    k_condvar_broadcast(&p->done_cv);
    queue_push(p, t);
    k_condvar_signal(&p->work_cv);
    k_mutex_unlock(&p->mu);
    return (myio_task *)t;
}

static int impl_sock_port(myio *io, myio_sock *sock) {
    (void)io;
    zp_sock *s = (zp_sock *)sock;
    struct sockaddr_storage ss;
    socklen_t len = sizeof ss;
    if (zsock_getsockname(s->fd, (struct sockaddr *)&ss, &len))
        /* Not every stack can say (offloaded sockets often lack
         * getsockname); fall back to the port the caller bound. */
        return s->port > 0 ? s->port : -1;
    if (ss.ss_family == AF_INET)
        return ntohs(((struct sockaddr_in *)&ss)->sin_port);
    if (ss.ss_family == AF_INET6)
        return ntohs(((struct sockaddr_in6 *)&ss)->sin6_port);
    return -1;
}

/* ---- synchronisation and lifetime ---- */

/* Report progress: a worker completion the user thread has not yet seen,
 * or failing that, wait for the next one. Every pending task is either
 * queued or running (nqueued + nrunning > 0), so 0 - no progress possible -
 * is only ever returned when nothing is outstanding at all. The generation
 * check makes step() immune to the check-then-wait race: a completion that
 * landed after the caller's last task_done poll is reported immediately
 * instead of sleeping through its (already spent) broadcast. A spurious 1
 * merely sends the generic loop around once more. */
static int impl_step(myio *io) {
    zp_io *p = io_of(io);
    k_mutex_lock(&p->mu, K_FOREVER);
    while (p->completions == p->comp_seen) {
        if (p->nqueued == 0 && p->nrunning == 0) {
            k_mutex_unlock(&p->mu);
            return 0;
        }
        k_condvar_wait(&p->done_cv, &p->mu, K_FOREVER);
    }
    p->comp_seen = p->completions;
    k_mutex_unlock(&p->mu);
    return 1;
}

static int impl_cancel(myio *io, myio_task *task) {
    zp_io *p = io_of(io);
    zp_task *t = task_of(task);
    int rc = -1;
    k_mutex_lock(&p->mu, K_FOREVER);
    if (t->done) {
        rc = -1;
    } else if (!t->started) {
        /* Still queued: lift it straight out. */
        complete_unrun(p, t, r_canceled());
        rc = 0;
    } else if (op_interruptible(t->kind)) {
        /* Running inside a cancelable wait: flag it and kick the worker.
         * The worker reports the outcome (canceled, unless it already
         * completed in the meantime). */
        t->cancel_req = 1;
        worker_kick(t);
        rc = 0;
    } else if (t->kind == OP_SOCK_WRITE && t->sock->write_head != t) {
        /* A write still waiting its turn behind an earlier write: no bytes on
         * the wire yet, so it can still be canceled. Flag it and wake it; the
         * worker completes it CANCELED and pops the FIFO. (The head write is
         * already sending and refuses, like any running blocking call.) */
        t->cancel_req = 1;
        k_condvar_broadcast(&p->done_cv);
        rc = 0;
    }
    k_mutex_unlock(&p->mu);
    return rc;
}

static myio_result impl_task_result(myio *io, const myio_task *task) {
    zp_io *p = io_of(io);
    k_mutex_lock(&p->mu, K_FOREVER);
    myio_result r = ((const zp_task *)task)->res;
    k_mutex_unlock(&p->mu);
    return r;
}

static const char *impl_error_str(myio *io, int err) {
    zp_io *p = io_of(io);
    if (err < 0)
        return zsock_gai_strerror(err);
    const char *s = strerror(err);
    if (s && *s)
        return s;
    snprintf(p->errbuf, sizeof p->errbuf, "error %d", err);
    return p->errbuf;
}

static unsigned impl_caps(myio *io) {
    (void)io;
    /* Worker-thread pool: ops run concurrently, submit only enqueues, and
     * spawn runs off-thread. Blocking file ops on a worker cannot be
     * interrupted, so no CANCEL_FILE. */
    return MYIO_CAP_CONCURRENT_WAIT | MYIO_CAP_NONBLOCKING_SUBMIT |
           MYIO_CAP_ASYNC_SPAWN;
}

static int impl_task_done(myio *io, const myio_task *task) {
    zp_io *p = io_of(io);
    k_mutex_lock(&p->mu, K_FOREVER);
    int d = ((const zp_task *)task)->done;
    k_mutex_unlock(&p->mu);
    return d;
}

static void impl_task_free(myio *io, myio_task *task) {
    zp_io *p = io_of(io);
    zp_task *t = task_of(task);
    k_mutex_lock(&p->mu, K_FOREVER);
    if (!t->done) {
        if (!t->started) {
            complete_unrun(p, t, r_canceled());
        } else {
            /* Running: the worker references this memory, so wait it out -
             * but first ask an interruptible op to stop, or the wait could
             * be unbounded (a read with no data ever coming, or a write still
             * waiting its turn behind a stalled peer). */
            if (op_interruptible(t->kind)) {
                t->cancel_req = 1;
                worker_kick(t);
            } else if (t->kind == OP_SOCK_WRITE && t->sock->write_head != t) {
                t->cancel_req = 1;
                k_condvar_broadcast(&p->done_cv);
            }
            while (!t->done)
                k_condvar_wait(&p->done_cv, &p->mu, K_FOREVER);
        }
    }
    k_mutex_unlock(&p->mu);
    /* The result's socket (if any) belongs to the caller; only release the
     * handle, like the other backends. */
    k_mem_slab_free(&zp_task_slab, t);
}

static void impl_task_detach(myio *io, myio_task *task) {
    zp_io *p = io_of(io);
    zp_task *t = task_of(task);
    k_mutex_lock(&p->mu, K_FOREVER);
    if (t->done)
        reap(t);
    else
        t->detached = 1;
    k_mutex_unlock(&p->mu);
}

static void impl_destroy(myio *io) {
    zp_io *p = io_of(io);
    k_mutex_lock(&p->mu, K_FOREVER);
    p->shutdown = 1;
    /* Drop tasks no worker has started; a queued op might block forever,
     * so it must never run during teardown. Detached ones are reaped;
     * owned ones are completed as canceled (a well-behaved caller has
     * already freed them). */
    while (p->qhead) {
        zp_task *t = p->qhead;
        int owned = !t->detached; /* complete_unrun reaps detached tasks */
        complete_unrun(p, t, r_canceled());
        if (owned)
            reap(t); /* destroy owns what the caller abandoned */
    }
    /* Cancel every interruptible op a worker is still blocked in; what
     * cannot be canceled (a spawn, a blocked send) is waited out. The
     * shutdowns wake blocked sends where the stack implements them. */
    for (zp_sock *s = p->socks; s; s = s->next)
        zsock_shutdown(s->fd, ZSOCK_SHUT_RDWR);
    for (int i = 0; i < CONFIG_MYIO_POOL_WORKERS; i++) {
        zp_task *c = p->workers[i].current;
        if (c && op_interruptible(c->kind)) {
            c->cancel_req = 1;
            worker_kick(c);
        }
    }
    k_condvar_broadcast(&p->work_cv);
    k_condvar_broadcast(&p->done_cv);
    k_mutex_unlock(&p->mu);

    for (int i = 0; i < CONFIG_MYIO_POOL_WORKERS; i++)
        k_thread_join(&p->workers[i].thread, K_FOREVER);

    /* No workers left: free any sockets the caller never closed. */
    while (p->socks) {
        zp_sock *s = p->socks;
        p->socks = s->next;
        zsock_close(s->fd);
        k_mem_slab_free(&zp_sock_slab, s);
    }
#ifdef CONFIG_FILE_SYSTEM
    for (int i = 0; i < CONFIG_MYIO_MAX_FILES; i++) {
        if (p->file_used[i]) {
            fs_close(&p->files[i]);
            p->file_used[i] = 0;
        }
    }
#endif
    for (int i = 0; i < CONFIG_MYIO_POOL_WORKERS; i++)
        zsock_close(p->workers[i].evfd);
    p->in_use = 0;
}

static const myio_ops zp_ops = {
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

/* The instance is static: the worker stacks and slabs are compile-time
 * singletons, so the instance may as well be one too. */
static zp_io the_pool;

myio *myio_zephyr_pool_new(void) {
    zp_io *p = &the_pool;
    if (p->in_use)
        return NULL;
    memset(p, 0, sizeof(*p));
    k_mutex_init(&p->mu);
    k_mutex_init(&p->fs_mu);
    k_condvar_init(&p->work_cv);
    k_condvar_init(&p->done_cv);
    for (int i = 0; i < CONFIG_MYIO_POOL_WORKERS; i++) {
        p->workers[i].evfd = zvfs_eventfd(0, ZVFS_EFD_NONBLOCK);
        if (p->workers[i].evfd < 0) {
            for (int j = 0; j < i; j++)
                zsock_close(p->workers[j].evfd);
            return NULL;
        }
    }
    for (int i = 0; i < CONFIG_MYIO_POOL_WORKERS; i++) {
        k_thread_create(&p->workers[i].thread, zp_stacks[i],
                        K_THREAD_STACK_SIZEOF(zp_stacks[i]), worker_main, p,
                        &p->workers[i], NULL,
                        K_PRIO_PREEMPT(CONFIG_MYIO_POOL_PRIORITY), 0,
                        K_NO_WAIT);
    }
    p->base.ops = &zp_ops;
    p->in_use = 1;
    return &p->base;
}
