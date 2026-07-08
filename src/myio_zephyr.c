/* Zephyr-backed myio implementation.
 *
 * Zephyr offers no event loop and no completion API - only readiness
 * primitives - so unlike the libuv/libxev backends this one runs its own
 * mini event loop: the step() primitive under the header's generic
 * await/select is one turn of run_loop_once(), which is ours:
 * zsock_poll() over the in-flight socket operations plus one eventfd,
 * with the timeout derived from the earliest pending timer.
 *
 * The backend is a hybrid the execution model explicitly sanctions: file
 * operations complete inside submit (Zephyr has no async file IO; fs_* calls
 * against flash are quick blocking calls), while sockets, timers and
 * spawned functions are genuinely asynchronous. DNS resolution is also
 * performed inline in submit via zsock_getaddrinfo(); the connect handshake
 * that follows is asynchronous.
 *
 * Cross-thread completions (myio_spawn runs on a dedicated work queue) are
 * marshalled through a spinlock-protected done list and a zvfs eventfd kept
 * in every poll set - the same wakeup device libuv (uv_async) and libxev
 * (xev.Async) need internally.
 *
 * Embedded-minded allocation: tasks and sockets come from fixed k_mem_slabs
 * (CONFIG_MYIO_MAX_TASKS / CONFIG_MYIO_MAX_SOCKETS), so submissions really
 * do return NULL on exhaustion, exercising the interface's documented OOM
 * path. Nothing here calls malloc.
 */
#include "myio_zephyr.h"

#include "myio_common.h"
#include "myio_wq.h"

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

enum task_kind {
    TASK_INLINE, /* completed inside submit (file ops, sock_close, EBUSY) */
    TASK_TIMER,
    TASK_SPAWN,
    TASK_CONNECT,
    TASK_ACCEPT,
    TASK_SOCK_READ,
    TASK_SOCK_WRITE,
};

typedef struct z_io   z_io;
typedef struct z_task z_task;
typedef struct z_sock z_sock;

struct z_task {
    z_io           *io;
    enum task_kind  kind;
    int             done;
    int             detached;   /* free on completion, result discarded */
    myio_result     res;
    z_task         *next;       /* io->pending list linkage */
    z_sock         *sock;       /* connect: sock being built; accept: the
                                   listener; read/write: the target */
    void           *buf;        /* sock_read destination */
    const void     *cbuf;       /* sock_write source */
    size_t          len;
    size_t          off;        /* sock_write: bytes already queued */
    myio_wq_node    wnode;      /* sock_write: link in the socket's write FIFO */
    int64_t         deadline;   /* timer: absolute k_uptime_get() ms */
    myio_fn         fn;         /* spawn: function, argument, return */
    void           *arg;
    int64_t         fn_ret;
    struct k_work   work;       /* spawn: work queue item */
    z_task         *done_next;  /* spawn: io->spawn_done list linkage */
    int             pushed;     /* spawn: fn finished, sits on spawn_done */
};

struct z_sock {
    int      fd;
    int      port;         /* listener: the requested bind port; offloaded
                              stacks may not implement getsockname, so this
                              is sock_port's fallback (0 = unknown) */
    z_task  *read_task;    /* outstanding sock_read (at most one) */
    z_task  *accept_task;  /* outstanding tcp_accept (at most one) */
    /* Outstanding sock_writes, FIFO in submission order. Only the head is on
     * the poll set (advanced by write_try); the rest wait here until it
     * completes, so overlapping writes hit the wire one at a time and never
     * interleave. Write tasks chain through their wnode links. */
    myio_wq  wq;
    z_sock  *next;         /* io->socks registry, for destroy's sweep */
};

/* The write task whose wnode is `n` (n must be non-NULL). */
#define wq_task(n) myio_wq_item(n, z_task, wnode)

struct z_io {
    myio     base;
    int      in_use;
    int      evfd;         /* wakes zsock_poll for off-thread completions */
    z_task  *pending;      /* every in-flight task: pollables, timers, spawns */
    z_sock  *socks;        /* every live socket, so destroy can reclaim them */
    z_task  *spawn_done;   /* spawns finished off-thread, not yet harvested */
    struct k_spinlock done_lock; /* guards spawn_done and task->pushed */
    char     errbuf[64];   /* last myio_strerror rendering */
    /* run_loop_once's poll set. Instance state rather than locals: at the
     * default MYIO_MAX_TASKS=64 the two arrays weigh ~1 KB, too much to
     * carve out of the driving thread's stack on every loop turn (and the
     * cost scales silently with the Kconfig). Safe as shared state because
     * an instance is driven from a single thread (see myio.h). */
    struct zsock_pollfd fds[CONFIG_MYIO_MAX_TASKS + 1];
    z_task  *owner[CONFIG_MYIO_MAX_TASKS + 1];
#ifdef CONFIG_FILE_SYSTEM
    struct fs_file_t files[CONFIG_MYIO_MAX_FILES];
    uint8_t          file_used[CONFIG_MYIO_MAX_FILES];
#endif
};

K_MEM_SLAB_DEFINE_STATIC(task_slab, sizeof(z_task), CONFIG_MYIO_MAX_TASKS,
                         __alignof__(z_task));
K_MEM_SLAB_DEFINE_STATIC(sock_slab, sizeof(z_sock), CONFIG_MYIO_MAX_SOCKETS,
                         __alignof__(z_sock));

#ifndef CONFIG_MYIO_SPAWN_INLINE
/* One dedicated work queue for myio_spawn, started on first use and kept
 * for the lifetime of the program (work queues cannot be torn down cheaply,
 * and a device typically creates one myio instance anyway). */
K_THREAD_STACK_DEFINE(spawn_stack, CONFIG_MYIO_SPAWN_STACK_SIZE);
static struct k_work_q spawn_q;
static int spawn_q_started;
#endif

static z_task *task_of(myio_task *t) { return (z_task *)t; }
static myio_task *handle_of(z_task *t) { return (myio_task *)t; }
static z_io *io_of(myio *io) { return (z_io *)io; }
static z_sock *zsock_of(myio_sock *s) { return (z_sock *)s; }

/* ---- task allocation and completion ---- */

static z_task *task_new(z_io *io, enum task_kind kind) {
    z_task *t;
    if (k_mem_slab_alloc(&task_slab, (void **)&t, K_NO_WAIT) != 0)
        return NULL; /* slab exhausted: the documented NULL-on-OOM path */
    memset(t, 0, sizeof(*t));
    t->io = io;
    t->kind = kind;
    t->res.status = MYIO_PENDING;
    return t;
}

static void sock_release(z_io *io, z_sock *s);

static void task_free_mem(z_task *t) {
#ifndef CONFIG_MYIO_SPAWN_INLINE
    if (t->kind == TASK_SPAWN && k_work_busy_get(&t->work) != 0) {
        /* The work queue thread still touches the k_work item briefly after
         * the handler returns; wait until it is truly idle before reusing
         * the memory. Bounded: the handler's last act is pushing to
         * spawn_done, so this only waits out a few instructions. */
        struct k_work_sync sync;
        k_work_flush(&t->work, &sync);
    }
#endif
    k_mem_slab_free(&task_slab, t);
}

/* Free a detached task that has completed. Its result is discarded, so a
 * socket it won is closed here - no caller will ever claim it. */
static void task_reap(z_task *t) {
    if (t->res.ptr)
        sock_release(t->io, t->res.ptr);
    task_free_mem(t);
}

static void pending_remove(z_io *io, z_task *t) {
    for (z_task **p = &io->pending; *p; p = &(*p)->next) {
        if (*p == t) {
            *p = t->next;
            t->next = NULL;
            return;
        }
    }
}

/* Every completion ends here: unlinks the task from the in-flight list,
 * stores the result, and reaps it if detached. Safe to call from the user
 * thread only (the loop and all submits run there; spawn completions are
 * marshalled to the loop before reaching this). */
static void task_complete(z_task *t, myio_status status, int64_t value,
                          int error, void *ptr) {
    pending_remove(t->io, t);
    t->res.status = status;
    t->res.value = value;
    t->res.error = error;
    t->res.ptr = ptr;
    t->done = 1;
    if (t->detached)
        task_reap(t);
}

static void task_ok(z_task *t, int64_t value, void *ptr) {
    task_complete(t, MYIO_OK, value, 0, ptr);
}

static void task_err(z_task *t, int error) {
    task_complete(t, MYIO_ERROR, 0, error, NULL);
}

static void task_canceled(z_task *t) {
    task_complete(t, MYIO_CANCELED, 0, 0, NULL);
}

/* A submission that failed before anything went in flight. */
static myio_task *task_new_failed(z_io *io, int err) {
    z_task *t = task_new(io, TASK_INLINE);
    if (!t)
        return NULL;
    task_err(t, err);
    return handle_of(t);
}

static void pending_push(z_io *io, z_task *t) {
    t->next = io->pending;
    io->pending = t;
}

/* ---- sockets: creation and teardown ---- */

static z_sock *sock_new(z_io *io, int fd) {
    z_sock *s;
    if (k_mem_slab_alloc(&sock_slab, (void **)&s, K_NO_WAIT) != 0)
        return NULL;
    memset(s, 0, sizeof(*s));
    s->fd = fd;
    s->next = io->socks;
    io->socks = s;
    return s;
}

static void sock_release(z_io *io, z_sock *s) {
    for (z_sock **p = &io->socks; *p; p = &(*p)->next) {
        if (*p == s) {
            *p = s->next;
            break;
        }
    }
    zsock_close(s->fd);
    k_mem_slab_free(&sock_slab, s);
}

/* The socket layer speaks Zephyr's ZVFS flag values, which deliberately do
 * not match any libc's O_NONBLOCK (this file may be compiled against the
 * host libc on native_sim). */
static int sock_set_nonblock(int fd) {
    int fl = zsock_fcntl(fd, ZVFS_F_GETFL, 0);
    if (fl < 0)
        return -1;
    return zsock_fcntl(fd, ZVFS_F_SETFL, fl | ZVFS_O_NONBLOCK);
}

/* ---- filesystem operations: complete inside submit ---- */

#ifdef CONFIG_FILE_SYSTEM

static myio_task *fs_result(z_io *io, int rc, int64_t value) {
    z_task *t = task_new(io, TASK_INLINE);
    if (!t)
        return NULL;
    if (rc < 0)
        task_err(t, -rc);
    else
        task_ok(t, value, NULL);
    return handle_of(t);
}

static myio_task *impl_open(myio *io, const char *path, int flags, int mode) {
    z_io *z = io_of(io);
    (void)mode; /* Zephyr filesystems carry no permission bits */
    int slot = -1;
    for (int i = 0; i < CONFIG_MYIO_MAX_FILES; i++) {
        if (!z->file_used[i]) {
            slot = i;
            break;
        }
    }
    if (slot < 0)
        return task_new_failed(z, EMFILE);
    fs_mode_t zflags = 0;
    switch (flags & O_ACCMODE) {
    case O_RDONLY: zflags = FS_O_READ; break;
    case O_WRONLY: zflags = FS_O_WRITE; break;
    default:       zflags = FS_O_RDWR; break;
    }
    if (flags & O_CREAT)
        zflags |= FS_O_CREATE;
    if (flags & O_APPEND)
        zflags |= FS_O_APPEND;
    if (flags & O_TRUNC)
        zflags |= FS_O_TRUNC;
    fs_file_t_init(&z->files[slot]);
    int rc = fs_open(&z->files[slot], path, zflags);
    if (rc == 0)
        z->file_used[slot] = 1;
    return fs_result(z, rc, slot);
}

static myio_task *impl_close(myio *io, int64_t fd) {
    z_io *z = io_of(io);
    if (fd < 0 || fd >= CONFIG_MYIO_MAX_FILES || !z->file_used[fd])
        return task_new_failed(z, EBADF);
    int rc = fs_close(&z->files[fd]);
    if (rc == 0)
        z->file_used[fd] = 0;
    return fs_result(z, rc, 0);
}

/* Positional read/write: seek, transfer, restore - pread/pwrite must not
 * move the file position. Race-free because instances are single-threaded. */
static myio_task *file_rw(z_io *z, int64_t fd, void *buf, const void *cbuf,
                          size_t len, int64_t offset) {
    if (fd < 0 || fd >= CONFIG_MYIO_MAX_FILES || !z->file_used[fd])
        return task_new_failed(z, EBADF);
    struct fs_file_t *f = &z->files[fd];
    off_t saved = 0;
    if (offset != MYIO_NO_OFFSET) {
        saved = fs_tell(f);
        if (saved < 0)
            return fs_result(z, (int)saved, 0);
        int rc = fs_seek(f, (off_t)offset, FS_SEEK_SET);
        if (rc < 0)
            return fs_result(z, rc, 0);
    }
    ssize_t n = cbuf ? fs_write(f, cbuf, len) : fs_read(f, buf, len);
    if (offset != MYIO_NO_OFFSET)
        fs_seek(f, saved, FS_SEEK_SET);
    return fs_result(z, n < 0 ? (int)n : 0, n);
}

static myio_task *impl_read(myio *io, int64_t fd, void *buf, size_t len,
                            int64_t offset) {
    return file_rw(io_of(io), fd, buf, NULL, len, offset);
}

static myio_task *impl_write(myio *io, int64_t fd, const void *buf, size_t len,
                             int64_t offset) {
    return file_rw(io_of(io), fd, NULL, buf, len, offset);
}

#else /* !CONFIG_FILE_SYSTEM */

static myio_task *impl_open(myio *io, const char *path, int flags, int mode) {
    (void)path; (void)flags; (void)mode;
    return task_new_failed(io_of(io), ENOSYS);
}
static myio_task *impl_close(myio *io, int64_t fd) {
    (void)fd;
    return task_new_failed(io_of(io), ENOSYS);
}
static myio_task *impl_read(myio *io, int64_t fd, void *buf, size_t len,
                            int64_t offset) {
    (void)fd; (void)buf; (void)len; (void)offset;
    return task_new_failed(io_of(io), ENOSYS);
}
static myio_task *impl_write(myio *io, int64_t fd, const void *buf, size_t len,
                             int64_t offset) {
    (void)fd; (void)buf; (void)len; (void)offset;
    return task_new_failed(io_of(io), ENOSYS);
}

#endif /* CONFIG_FILE_SYSTEM */

/* ---- timers ---- */

static myio_task *impl_sleep(myio *io, uint64_t ms) {
    z_task *t = task_new(io_of(io), TASK_TIMER);
    if (!t)
        return NULL;
    t->deadline = k_uptime_get() + (int64_t)ms;
    pending_push(t->io, t);
    return handle_of(t);
}

/* ---- spawned functions (dedicated work queue) ---- */

#ifdef CONFIG_MYIO_SPAWN_INLINE

/* Zero-extra-threads build: spawn degrades to a blocking call inside
 * submit, which the execution model sanctions (RAM-constrained targets
 * genuinely choose this). */
static myio_task *impl_spawn(myio *io, myio_fn fn, void *arg) {
    z_task *t = task_new(io_of(io), TASK_INLINE);
    if (!t)
        return NULL;
    int64_t rc = fn(arg);
    if (rc >= 0)
        task_ok(t, rc, NULL);
    else
        task_err(t, (int)-rc);
    return handle_of(t);
}

#else /* !CONFIG_MYIO_SPAWN_INLINE */

/* Runs on the spawn work queue thread. Push-to-done-list must be the last
 * touch of the task: once the eventfd is written, the loop thread may
 * complete and even free it (task_free_mem's k_work_flush covers the work
 * item bookkeeping that follows this return). */
static void spawn_work_handler(struct k_work *work) {
    z_task *t = CONTAINER_OF(work, z_task, work);
    z_io *io = t->io;
    t->fn_ret = t->fn(t->arg);
    k_spinlock_key_t key = k_spin_lock(&io->done_lock);
    t->pushed = 1;
    t->done_next = io->spawn_done;
    io->spawn_done = t;
    k_spin_unlock(&io->done_lock, key);
    zvfs_eventfd_write(io->evfd, 1);
}

static myio_task *impl_spawn(myio *io, myio_fn fn, void *arg) {
    z_task *t = task_new(io_of(io), TASK_SPAWN);
    if (!t)
        return NULL;
    t->fn = fn;
    t->arg = arg;
    k_work_init(&t->work, spawn_work_handler);
    pending_push(t->io, t);
    k_work_submit_to_queue(&spawn_q, &t->work);
    return handle_of(t);
}

/* Harvest spawns whose fn finished on the work queue thread. Runs on the
 * user thread, so completing (and possibly reaping) here is safe. */
static void drain_spawn_done(z_io *io) {
    for (;;) {
        k_spinlock_key_t key = k_spin_lock(&io->done_lock);
        z_task *t = io->spawn_done;
        if (t)
            io->spawn_done = t->done_next;
        k_spin_unlock(&io->done_lock, key);
        if (!t)
            return;
        if (t->fn_ret >= 0)
            task_ok(t, t->fn_ret, NULL);
        else
            task_err(t, (int)-t->fn_ret);
    }
}

#endif /* CONFIG_MYIO_SPAWN_INLINE */

/* ---- TCP: connect ---- */

static myio_task *impl_tcp_connect(myio *io, const char *host, int port) {
    z_io *z = io_of(io);
    z_task *t = task_new(z, TASK_CONNECT);
    if (!t)
        return NULL;
    /* DNS resolution is a blocking call inside submit: Zephyr's async
     * resolver needs the native IP stack (offloaded-socket builds have
     * none), and `host` need not outlive this call either way. */
    char service[8];
    snprintf(service, sizeof service, "%d", port);
    struct zsock_addrinfo hints = { .ai_socktype = SOCK_STREAM };
    struct zsock_addrinfo *res = NULL;
    int rc = zsock_getaddrinfo(host, service, &hints, &res);
    if (rc != 0) {
        /* EAI_* codes are negative on Zephyr; keep them verbatim so
         * error_str renders the real resolver error. DNS_EAI_SYSTEM defers
         * to errno, like glibc's EAI_SYSTEM. */
        task_err(t, rc == DNS_EAI_SYSTEM ? errno : (rc < 0 ? rc : -rc));
        return handle_of(t);
    }
    int fd = zsock_socket(res->ai_family, SOCK_STREAM, IPPROTO_TCP);
    if (fd < 0 || sock_set_nonblock(fd) < 0) {
        int err = errno;
        if (fd >= 0)
            zsock_close(fd);
        zsock_freeaddrinfo(res);
        task_err(t, err);
        return handle_of(t);
    }
    z_sock *s = sock_new(z, fd);
    if (!s) {
        zsock_close(fd);
        zsock_freeaddrinfo(res);
        task_err(t, ENOMEM);
        return handle_of(t);
    }
    t->sock = s;
    /* Only the first resolved address is tried (like the libuv backend). */
    rc = zsock_connect(fd, res->ai_addr, res->ai_addrlen);
    zsock_freeaddrinfo(res);
    if (rc == 0) {
        t->sock = NULL;
        task_ok(t, 0, s);
    } else if (errno == EINPROGRESS) {
        pending_push(z, t); /* completes on POLLOUT */
    } else {
        t->sock = NULL;
        sock_release(z, s);
        task_err(t, errno);
    }
    return handle_of(t);
}

/* POLLOUT on a connecting socket: the handshake finished, one way or the
 * other; SO_ERROR says which. */
static void connect_ready(z_task *t) {
    z_sock *s = t->sock;
    int soerr = 0;
    socklen_t slen = sizeof soerr;
    if (zsock_getsockopt(s->fd, SOL_SOCKET, SO_ERROR, &soerr, &slen) < 0)
        soerr = errno;
    t->sock = NULL;
    if (soerr == 0) {
        task_ok(t, 0, s);
    } else {
        sock_release(t->io, s);
        task_err(t, soerr);
    }
}

/* ---- TCP: listen / accept ---- */

static myio_sock *impl_tcp_listen(myio *io, const char *host, int port,
                                  int backlog, int *err) {
    z_io *z = io_of(io);
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
    if (sock_set_nonblock(fd) < 0 ||
        zsock_bind(fd, (struct sockaddr *)&addr, alen) < 0 ||
        zsock_listen(fd, backlog) < 0) {
        if (err)
            *err = errno;
        zsock_close(fd);
        return NULL;
    }
    z_sock *s = sock_new(z, fd);
    if (!s) {
        if (err)
            *err = ENOMEM;
        zsock_close(fd);
        return NULL;
    }
    s->port = port;
    return (myio_sock *)s;
}

/* Try to accept one connection; complete the task if something happened.
 * Returns 0 when the task completed, -1 when it should (re)arm on POLLIN. */
static int accept_try(z_task *t) {
    z_sock *ls = t->sock;
    int fd = zsock_accept(ls->fd, NULL, NULL);
    if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return -1;
        ls->accept_task = NULL;
        task_err(t, errno);
        return 0;
    }
    ls->accept_task = NULL;
    if (sock_set_nonblock(fd) < 0) {
        int e = errno;
        zsock_close(fd);
        task_err(t, e);
        return 0;
    }
    z_sock *c = sock_new(t->io, fd);
    if (!c) {
        zsock_close(fd);
        task_err(t, ENOMEM);
        return 0;
    }
    task_ok(t, 0, c);
    return 0;
}

static myio_task *impl_tcp_accept(myio *io, myio_sock *listener) {
    z_io *z = io_of(io);
    z_sock *ls = zsock_of(listener);
    if (ls->accept_task)
        return task_new_failed(z, EBUSY);
    z_task *t = task_new(z, TASK_ACCEPT);
    if (!t)
        return NULL;
    t->sock = ls;
    ls->accept_task = t;
    if (accept_try(t) < 0)
        pending_push(z, t);
    return handle_of(t);
}

/* ---- TCP: read / write / close ---- */

static int read_try(z_task *t) {
    z_sock *s = t->sock;
    ssize_t n = zsock_recv(s->fd, t->buf, t->len, 0);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return -1;
        s->read_task = NULL;
        task_err(t, errno);
        return 0;
    }
    s->read_task = NULL;
    task_ok(t, n, NULL); /* 0 = peer closed */
    return 0;
}

static myio_task *impl_sock_read(myio *io, myio_sock *sock, void *buf,
                                 size_t len) {
    z_io *z = io_of(io);
    z_sock *s = zsock_of(sock);
    if (s->read_task)
        return task_new_failed(z, EBUSY);
    z_task *t = task_new(z, TASK_SOCK_READ);
    if (!t)
        return NULL;
    t->sock = s;
    t->buf = buf;
    t->len = len;
    s->read_task = t;
    if (read_try(t) < 0)
        pending_push(z, t);
    return handle_of(t);
}

/* Push as much of the buffer as the socket will take. The all-or-error
 * contract is "one rearm inside a backend": a partial send is resumed on the
 * next POLLOUT. Does not complete the task; returns 0 when the whole buffer
 * drained, -1 when it would block (poll for POLLOUT and call again), or a
 * positive errno on a fatal send error. */
static int write_try(z_task *t) {
    while (t->off < t->len) {
        ssize_t n = zsock_send(t->sock->fd, (const char *)t->cbuf + t->off,
                               t->len - t->off, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return -1;
            return errno;
        }
        t->off += (size_t)n;
    }
    return 0;
}

/* Drive the socket's head write (which is `t` and not yet on the poll set):
 * send what we can, and if it would block, put it on the poll set for
 * POLLOUT. Each head that finishes is popped, completed, and its successor
 * armed - looped rather than recursed to keep the stack flat on the tiny
 * worker stacks these targets run. */
static void write_arm(z_io *z, z_task *t) {
    for (;;) {
        int r = write_try(t);
        if (r < 0) {
            pending_push(z, t); /* wait for POLLOUT */
            return;
        }
        /* Pop the head before completing: task_ok/task_err may reap a
         * detached task, so advance the FIFO first. */
        myio_wq_node *next = myio_wq_pop(&t->sock->wq);
        if (r == 0)
            task_ok(t, (int64_t)t->len, NULL);
        else
            task_err(t, r);
        if (!next)
            return;
        t = wq_task(next); /* arm the next queued write */
    }
}

/* POLLOUT on the head write `t` (already on the poll set): resume the send,
 * and on completion pop it and arm the next queued write. */
static void write_ready(z_io *z, z_task *t) {
    int r = write_try(t);
    if (r < 0)
        return; /* still blocking; stays on the poll set for the next POLLOUT */
    myio_wq_node *next = myio_wq_pop(&t->sock->wq);
    if (r == 0)
        task_ok(t, (int64_t)t->len, NULL);
    else
        task_err(t, r);
    if (next)
        write_arm(z, wq_task(next)); /* the successor is not yet on the poll set */
}

static myio_task *impl_sock_write(myio *io, myio_sock *sock, const void *buf,
                                  size_t len) {
    z_io *z = io_of(io);
    z_sock *s = zsock_of(sock);
    z_task *t = task_new(z, TASK_SOCK_WRITE);
    if (!t)
        return NULL;
    t->sock = s;
    t->cbuf = buf;
    t->len = len;
    /* A write that lands behind an outstanding one is only queued - no poll
     * entry or send attempt until it reaches the head. */
    if (myio_wq_push(&s->wq, &t->wnode))
        write_arm(z, t);
    return handle_of(t);
}

static myio_task *impl_sock_close(myio *io, myio_sock *sock) {
    z_io *z = io_of(io);
    z_sock *s = zsock_of(sock);
    z_task *t = task_new(z, TASK_INLINE);
    if (!t)
        return NULL;
    /* Outstanding pull-style tasks would otherwise never complete. */
    if (s->read_task) {
        z_task *rt = s->read_task;
        s->read_task = NULL;
        task_canceled(rt);
    }
    if (s->accept_task) {
        z_task *at = s->accept_task;
        s->accept_task = NULL;
        task_canceled(at);
    }
    /* Outstanding writes reference the fd about to close; cancel the whole
     * per-socket FIFO (only the head sat on the poll set - task_canceled
     * removes it - but the queued rest must be completed too). */
    while (s->wq.head) {
        z_task *w = wq_task(s->wq.head);
        myio_wq_pop(&s->wq);
        task_canceled(w);
    }
    sock_release(z, s);
    task_ok(t, 0, NULL);
    return handle_of(t);
}

static int impl_sock_port(myio *io, myio_sock *sock) {
    (void)io;
    z_sock *s = zsock_of(sock);
    struct sockaddr_storage ss;
    socklen_t len = sizeof ss;
    if (zsock_getsockname(s->fd, (struct sockaddr *)&ss, &len))
        /* Not every stack can say (offloaded sockets often lack
         * getsockname); fall back to the port the caller bound. */
        return s->port > 0 ? s->port : -1;
    return myio_sockaddr_port(&ss);
}

/* ---- the loop ---- */

/* One iteration of the hand-rolled event loop: poll every in-flight socket
 * operation plus the eventfd, with the timeout set by the nearest timer;
 * then fire expired timers, advance ready socket operations, and harvest
 * off-thread spawn completions. */
static void run_loop_once(z_io *io) {
    struct zsock_pollfd *fds = io->fds;
    z_task **owner = io->owner;
    int n = 0;
    int64_t next_deadline = INT64_MAX;

    for (z_task *t = io->pending; t; t = t->next) {
        short ev = 0;
        switch (t->kind) {
        case TASK_TIMER:
            if (t->deadline < next_deadline)
                next_deadline = t->deadline;
            continue;
        case TASK_CONNECT:    ev = ZSOCK_POLLOUT; break;
        case TASK_SOCK_WRITE: ev = ZSOCK_POLLOUT; break;
        case TASK_ACCEPT:     ev = ZSOCK_POLLIN;  break;
        case TASK_SOCK_READ:  ev = ZSOCK_POLLIN;  break;
        default:
            continue; /* spawns complete through the eventfd */
        }
        fds[n].fd = t->sock->fd;
        fds[n].events = ev;
        fds[n].revents = 0;
        owner[n] = t;
        n++;
    }
    fds[n].fd = io->evfd;
    fds[n].events = ZSOCK_POLLIN;
    fds[n].revents = 0;
    n++;

    int timeout = -1;
    if (next_deadline != INT64_MAX) {
        int64_t delta = next_deadline - k_uptime_get();
        timeout = delta > 0 ? (delta > INT32_MAX ? INT32_MAX : (int)delta) : 0;
    }

    int rc = zsock_poll(fds, n, timeout);

    int64_t now = k_uptime_get();
    for (z_task *t = io->pending, *next; t; t = next) {
        next = t->next;
        if (t->kind == TASK_TIMER && t->deadline <= now)
            task_ok(t, 0, NULL);
    }

    if (rc <= 0)
        return;
    /* A ready fd may belong to a task that a timer completion above (via a
     * detached reap) has already freed - owner[] entries are only touched
     * when their task is still on the pending list. */
    for (int i = 0; i < n - 1; i++) {
        if (!fds[i].revents)
            continue;
        z_task *t = owner[i];
        int still_pending = 0;
        for (z_task *p = io->pending; p; p = p->next)
            if (p == t) {
                still_pending = 1;
                break;
            }
        if (!still_pending)
            continue;
        switch (t->kind) {
        case TASK_CONNECT:    connect_ready(t); break;
        case TASK_ACCEPT:     (void)accept_try(t); break;
        case TASK_SOCK_READ:  (void)read_try(t); break;
        case TASK_SOCK_WRITE: write_ready(io, t); break;
        default: break;
        }
    }
    if (fds[n - 1].revents) {
        zvfs_eventfd_t v;
        zvfs_eventfd_read(io->evfd, &v);
#ifndef CONFIG_MYIO_SPAWN_INLINE
        drain_spawn_done(io);
#endif
    }
}

/* Anything in flight can eventually complete: pollables and timers through
 * the poll loop, spawns through the eventfd. An empty pending list while
 * the awaited task is not done means it never will be. */
static int progress_possible(z_io *io) {
    return io->pending != NULL;
}

/* ---- synchronisation ---- */

static int impl_step(myio *io) {
    z_io *z = io_of(io);
    if (!progress_possible(z))
        return 0;
    run_loop_once(z);
    return 1;
}

static myio_result impl_task_result(myio *io, const myio_task *task) {
    (void)io;
    return ((const z_task *)task)->res;
}

/* Cancellation is the cleanest of any backend: a readiness-based operation
 * that has not fired holds no in-kernel state, so cancel is deregister +
 * complete MYIO_CANCELED on the spot. The exceptions: a partially sent
 * sock_write (bytes are already with the peer), and a spawn whose fn is
 * already running. */
static int impl_cancel(myio *io, myio_task *task) {
    z_io *z = io_of(io);
    z_task *t = task_of(task);
    if (t->done)
        return -1;
    switch (t->kind) {
    case TASK_TIMER:
        task_canceled(t);
        return 0;
    case TASK_CONNECT: {
        z_sock *s = t->sock;
        t->sock = NULL;
        task_canceled(t);
        sock_release(z, s);
        return 0;
    }
    case TASK_ACCEPT:
        t->sock->accept_task = NULL;
        task_canceled(t);
        return 0;
    case TASK_SOCK_READ:
        t->sock->read_task = NULL;
        task_canceled(t);
        return 0;
    case TASK_SOCK_WRITE: {
        z_sock *s = t->sock;
        if (t->off > 0)
            return -1; /* part of the buffer is already on the wire */
        /* No bytes sent yet (always so for a queued non-head write): unlink
         * it from the socket's write FIFO and cancel it. If it was the head,
         * promote the next queued write onto the poll set. */
        int was_head = s->wq.head == &t->wnode;
        myio_wq_remove(&s->wq, &t->wnode);
        task_canceled(t); /* also removes the head's poll entry */
        if (was_head && s->wq.head)
            write_arm(z, wq_task(s->wq.head));
        return 0;
    }
#ifndef CONFIG_MYIO_SPAWN_INLINE
    case TASK_SPAWN: {
        int stopped = 0;
        k_spinlock_key_t key = k_spin_lock(&z->done_lock);
        /* Not yet pushed to spawn_done and removable from the queue before
         * running: fn never ran and never will. Holding done_lock closes
         * the race with the handler's push. */
        if (!t->pushed && k_work_cancel(&t->work) == 0)
            stopped = 1;
        k_spin_unlock(&z->done_lock, key);
        if (!stopped)
            return -1; /* running or already finished; await reports it */
        task_canceled(t);
        return 0;
    }
#endif
    default:
        return -1;
    }
}

static const char *impl_error_str(myio *io, int err) {
    z_io *z = io_of(io);
    /* Negative codes are the resolver's EAI_* range, kept verbatim by
     * tcp_connect; everything else is errno-style. */
    if (err < 0)
        return zsock_gai_strerror(err);
    const char *s = strerror(err);
    if (s && *s)
        return s;
    snprintf(z->errbuf, sizeof z->errbuf, "error %d", err);
    return z->errbuf;
}

static unsigned impl_caps(myio *io) {
    (void)io;
    /* Event loop: sockets and timers make progress concurrently. No
     * NONBLOCKING_SUBMIT - DNS resolution and file operations run inline in
     * submit. No CANCEL_FILE (inline blocking file ops). ASYNC_SPAWN unless
     * built to run spawned functions inline in submit. */
    return MYIO_CAP_CONCURRENT_WAIT
#ifndef CONFIG_MYIO_SPAWN_INLINE
           | MYIO_CAP_ASYNC_SPAWN
#endif
        ;
}

/* ---- lifetime ---- */

static int impl_task_done(myio *io, const myio_task *task) {
    (void)io;
    return ((const z_task *)task)->done;
}

static void impl_task_free(myio *io, myio_task *task) {
    z_task *t = task_of(task);
    if (!t->done) {
        /* The in-flight operation references this memory: cancel if
         * possible, then step until completion before releasing it. (A
         * pending task is on the pending list, so progress stays possible
         * until it completes.) */
        if (impl_cancel(io, task) != 0)
            while (!t->done && impl_step(io)) {
            }
    }
    task_free_mem(t);
}

static void impl_task_detach(myio *io, myio_task *task) {
    (void)io;
    z_task *t = task_of(task);
    if (t->done)
        task_reap(t);
    else
        t->detached = 1;
}

static void impl_destroy(myio *io) {
    z_io *z = io_of(io);
    /* Finish or abandon everything still in flight. Cancellation stops
     * almost anything here; what it cannot stop (a running spawn, a
     * partially sent write) is driven to completion. Detached tasks are
     * reaped by their completion; canceled owned tasks leak their slab
     * block, but the header requires callers to free owned tasks first. */
    while (z->pending) {
        if (impl_cancel(io, handle_of(z->pending)) != 0)
            run_loop_once(z);
    }
    /* Sockets never closed by the caller (and listeners) are reclaimed. */
    while (z->socks)
        sock_release(z, z->socks);
#ifdef CONFIG_FILE_SYSTEM
    for (int i = 0; i < CONFIG_MYIO_MAX_FILES; i++) {
        if (z->file_used[i]) {
            fs_close(&z->files[i]);
            z->file_used[i] = 0;
        }
    }
#endif
    zsock_close(z->evfd);
    z->in_use = 0;
}

static const myio_ops zephyr_ops = {
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

/* The instance is static: embedded targets want no heap dependency, and the
 * slabs backing tasks and sockets are compile-time singletons anyway. */
static z_io the_io;

myio *myio_zephyr_new(void) {
    z_io *z = &the_io;
    if (z->in_use)
        return NULL;
    memset(z, 0, sizeof(*z));
    z->evfd = zvfs_eventfd(0, 0);
    if (z->evfd < 0)
        return NULL;
#ifndef CONFIG_MYIO_SPAWN_INLINE
    if (!spawn_q_started) {
        k_work_queue_init(&spawn_q);
        k_work_queue_start(&spawn_q, spawn_stack,
                           K_THREAD_STACK_SIZEOF(spawn_stack),
                           K_PRIO_PREEMPT(CONFIG_MYIO_SPAWN_PRIORITY), NULL);
        spawn_q_started = 1;
    }
#endif
    z->base.ops = &zephyr_ops;
    z->in_use = 1;
    return &z->base;
}
