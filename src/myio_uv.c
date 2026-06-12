/* libuv-backed myio implementation. */
#define _GNU_SOURCE /* uv.h needs POSIX/GNU types hidden by strict -std=c11 */
#include "myio_uv.h"

#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

typedef struct {
    myio      base;
    uv_loop_t loop;
    char      errbuf[128]; /* last myio_strerror rendering */
} uv_io;

enum task_kind {
    TASK_FS,
    TASK_TIMER,
    TASK_SPAWN,
    TASK_CONNECT,
    TASK_ACCEPT,
    TASK_SOCK_READ,
    TASK_SOCK_WRITE,
    TASK_SOCK_CLOSE,
};

typedef struct uv_sock uv_sock;

typedef struct uv_task {
    uv_io          *io;
    enum task_kind  kind;
    int             done;
    int             detached;   /* free on completion, result discarded */
    myio_result     res;
    uv_buf_t        buf;        /* keeps the iov alive for read/write */
    uv_sock        *sock;       /* socket the task operates on, if any */
    myio_fn         fn;         /* spawn task: function and its argument */
    void           *arg;
    int64_t         fn_ret;
    int             connecting; /* connect task: DNS done, handshake in flight */
    uv_connect_t    conn;       /* outside the union: starts inside gai_cb
                                   while libuv may still hold the gai req */
    /* Timer handles are freed from their uv_close callback; these flags
     * coordinate that with task_free. */
    int             timer_closed;
    int             free_on_close;
    union {
        uv_fs_t          fs;
        uv_timer_t       timer;
        uv_getaddrinfo_t gai;
        uv_write_t       write;
        uv_work_t        work;
    } u;
} uv_task;

struct uv_sock {
    uv_tcp_t  tcp;
    uv_io    *io;
    uv_task  *read_task;    /* outstanding sock_read (at most one) */
    uv_task  *accept_task;  /* outstanding tcp_accept (at most one) */
    uv_task  *close_task;
    uv_sock  *pending_head; /* accepted connections not yet claimed */
    uv_sock  *pending_tail;
    uv_sock  *next_pending;
};

static uv_task *task_of(myio_task *t) { return (uv_task *)t; }
static myio_task *handle_of(uv_task *t) { return (myio_task *)t; }
static uv_io *io_of(myio *io) { return (uv_io *)io; }
static uv_sock *sock_of(myio_sock *s) { return (uv_sock *)s; }

static uv_task *task_new(uv_io *io, enum task_kind kind) {
    uv_task *t = calloc(1, sizeof(*t));
    if (!t)
        return NULL;
    t->io = io;
    t->kind = kind;
    t->res.status = MYIO_PENDING;
    return t;
}

static void sock_destroy(uv_sock *s);
static void timer_close_cb(uv_handle_t *h);

/* Free a detached task that has completed. Its result is discarded, so a
 * socket it won is closed here - no caller will ever claim it. */
static void task_reap(uv_task *t) {
    if (t->res.ptr)
        sock_destroy(t->res.ptr);
    if (t->kind == TASK_TIMER && !t->timer_closed) {
        /* The timer handle lives inside the task; it must finish closing
         * before the memory can go. */
        t->free_on_close = 1;
        if (!uv_is_closing((uv_handle_t *)&t->u.timer))
            uv_close((uv_handle_t *)&t->u.timer, timer_close_cb);
        return;
    }
    free(t);
}

/* Every completion ends here: marks the task done and, when it has been
 * detached, frees it on the spot (safe: a libuv request's callback is its
 * final event, so nothing references the task afterwards). */
static void task_finish(uv_task *t) {
    t->done = 1;
    if (t->detached)
        task_reap(t);
}

static void task_complete(uv_task *t, int64_t uv_result) {
    if (uv_result >= 0) {
        t->res.status = MYIO_OK;
        t->res.value = uv_result;
    } else if (uv_result == UV_ECANCELED) {
        t->res.status = MYIO_CANCELED;
    } else {
        t->res.status = MYIO_ERROR;
        /* Stored negated, so OS failures read as plain errnos while DNS
         * failures keep libuv's own UV_EAI_* range; error_str undoes the
         * negation and lets uv_strerror name either kind. */
        t->res.error = (int)-uv_result;
    }
    task_finish(t);
}

/* A task that failed before it could even be submitted. */
static myio_task *task_new_failed(uv_io *io, enum task_kind kind, int err) {
    uv_task *t = task_new(io, kind);
    if (!t)
        return NULL;
    t->res.status = MYIO_ERROR;
    t->res.error = err;
    t->done = 1;
    return handle_of(t);
}

/* ---- sockets: creation and teardown ---- */

static uv_sock *sock_new(uv_io *io) {
    uv_sock *s = calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->io = io;
    uv_tcp_init(&io->loop, &s->tcp);
    s->tcp.data = s;
    return s;
}

/* Final step of every socket's life: completes the close task if one is
 * attached, then releases the memory. Outstanding pull tasks are normally
 * canceled by impl_sock_close before this runs; completing them here too
 * covers sockets torn down by destroy's walk. */
static void sock_close_cb(uv_handle_t *h) {
    uv_sock *s = h->data;
    if (s->read_task) {
        task_complete(s->read_task, UV_ECANCELED);
        s->read_task = NULL;
    }
    if (s->accept_task) {
        task_complete(s->accept_task, UV_ECANCELED);
        s->accept_task = NULL;
    }
    if (s->close_task)
        task_complete(s->close_task, 0);
    free(s);
}

static void sock_destroy(uv_sock *s) {
    if (!uv_is_closing((uv_handle_t *)&s->tcp))
        uv_close((uv_handle_t *)&s->tcp, sock_close_cb);
}

/* ---- filesystem operations ---- */

static void fs_cb(uv_fs_t *req) {
    uv_task *t = req->data;
    int64_t result = req->result;
    uv_fs_req_cleanup(req);
    task_complete(t, result);
}

/* If submission failed synchronously, complete the task immediately. */
static myio_task *fs_submitted(uv_task *t, int rc) {
    if (rc < 0)
        task_complete(t, rc);
    return handle_of(t);
}

static myio_task *impl_open(myio *io, const char *path, int flags, int mode) {
    uv_task *t = task_new(io_of(io), TASK_FS);
    if (!t)
        return NULL;
    t->u.fs.data = t;
    return fs_submitted(t, uv_fs_open(&t->io->loop, &t->u.fs, path, flags,
                                      mode, fs_cb));
}

static myio_task *impl_close(myio *io, int64_t fd) {
    uv_task *t = task_new(io_of(io), TASK_FS);
    if (!t)
        return NULL;
    t->u.fs.data = t;
    return fs_submitted(t, uv_fs_close(&t->io->loop, &t->u.fs, (uv_file)fd,
                                       fs_cb));
}

static myio_task *impl_read(myio *io, int64_t fd, void *buf, size_t len,
                            int64_t offset) {
    uv_task *t = task_new(io_of(io), TASK_FS);
    if (!t)
        return NULL;
    t->u.fs.data = t;
    t->buf = uv_buf_init(buf, (unsigned)len);
    return fs_submitted(t, uv_fs_read(&t->io->loop, &t->u.fs, (uv_file)fd,
                                      &t->buf, 1, offset, fs_cb));
}

static myio_task *impl_write(myio *io, int64_t fd, const void *buf, size_t len,
                             int64_t offset) {
    uv_task *t = task_new(io_of(io), TASK_FS);
    if (!t)
        return NULL;
    t->u.fs.data = t;
    t->buf = uv_buf_init((char *)(uintptr_t)buf, (unsigned)len);
    return fs_submitted(t, uv_fs_write(&t->io->loop, &t->u.fs, (uv_file)fd,
                                       &t->buf, 1, offset, fs_cb));
}

/* ---- timers ---- */

static void timer_close_cb(uv_handle_t *h) {
    uv_task *t = h->data;
    t->timer_closed = 1;
    if (t->free_on_close)
        free(t);
}

static void timer_cb(uv_timer_t *timer) {
    uv_task *t = timer->data;
    /* Close first: task_complete may free a detached task, and reaping a
     * timer relies on seeing the close already in progress. */
    uv_close((uv_handle_t *)timer, timer_close_cb);
    task_complete(t, 0);
}

static myio_task *impl_sleep(myio *io, uint64_t ms) {
    uv_task *t = task_new(io_of(io), TASK_TIMER);
    if (!t)
        return NULL;
    uv_timer_init(&t->io->loop, &t->u.timer);
    t->u.timer.data = t;
    int rc = uv_timer_start(&t->u.timer, timer_cb, ms, 0);
    if (rc < 0) {
        task_complete(t, rc);
        uv_close((uv_handle_t *)&t->u.timer, timer_close_cb);
    }
    return handle_of(t);
}

/* ---- spawned functions (libuv thread pool) ---- */

static void work_cb(uv_work_t *req) {
    uv_task *t = req->data;
    t->fn_ret = t->fn(t->arg);
}

static void after_work_cb(uv_work_t *req, int status) {
    uv_task *t = req->data;
    if (status < 0) {
        task_complete(t, status); /* UV_ECANCELED: fn never ran */
    } else if (t->fn_ret >= 0) {
        task_complete(t, t->fn_ret);
    } else {
        /* Keep user error codes verbatim instead of routing them through
         * task_complete's libuv-code interpretation. */
        t->res.status = MYIO_ERROR;
        t->res.error = (int)-t->fn_ret;
        task_finish(t);
    }
}

static myio_task *impl_spawn(myio *io, myio_fn fn, void *arg) {
    uv_task *t = task_new(io_of(io), TASK_SPAWN);
    if (!t)
        return NULL;
    t->fn = fn;
    t->arg = arg;
    t->u.work.data = t;
    int rc = uv_queue_work(&t->io->loop, &t->u.work, work_cb, after_work_cb);
    if (rc < 0)
        task_complete(t, rc);
    return handle_of(t);
}

/* ---- TCP: connect ---- */

static void connect_cb(uv_connect_t *req, int status) {
    uv_task *t = req->data;
    if (status == 0) {
        t->res.ptr = t->sock;
        task_complete(t, 0);
        return;
    }
    sock_destroy(t->sock); /* no-op if a cancel already started the close */
    t->sock = NULL;
    task_complete(t, status);
}

static void gai_cb(uv_getaddrinfo_t *req, int status, struct addrinfo *res) {
    uv_task *t = req->data;
    if (status < 0) {
        sock_destroy(t->sock);
        t->sock = NULL;
        uv_freeaddrinfo(res);
        task_complete(t, status);
        return;
    }
    t->connecting = 1;
    t->conn.data = t;
    /* Only the first resolved address is tried. */
    int rc = uv_tcp_connect(&t->conn, &t->sock->tcp, res->ai_addr, connect_cb);
    uv_freeaddrinfo(res);
    if (rc < 0) {
        sock_destroy(t->sock);
        t->sock = NULL;
        task_complete(t, rc);
    }
}

static myio_task *impl_tcp_connect(myio *io, const char *host, int port) {
    uv_task *t = task_new(io_of(io), TASK_CONNECT);
    if (!t)
        return NULL;
    t->sock = sock_new(t->io);
    if (!t->sock) {
        free(t);
        return NULL;
    }
    char service[16];
    snprintf(service, sizeof service, "%d", port);
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    t->u.gai.data = t;
    int rc = uv_getaddrinfo(&t->io->loop, &t->u.gai, gai_cb, host, service,
                            &hints);
    if (rc < 0) {
        task_complete(t, rc);
        sock_destroy(t->sock);
        t->sock = NULL;
    }
    return handle_of(t);
}

/* ---- TCP: listen / accept ---- */

static void connection_cb(uv_stream_t *server, int status) {
    uv_sock *ls = server->data;
    if (status < 0) {
        uv_task *t = ls->accept_task;
        if (t) {
            ls->accept_task = NULL;
            task_complete(t, status);
        }
        return;
    }
    /* libuv requires accepting in this callback; park the connection if no
     * accept task is waiting for it. */
    uv_sock *c = sock_new(ls->io);
    if (!c)
        return; /* connection stays in the backlog */
    if (uv_accept(server, (uv_stream_t *)&c->tcp) != 0) {
        sock_destroy(c);
        return;
    }
    uv_task *t = ls->accept_task;
    if (t) {
        ls->accept_task = NULL;
        t->res.ptr = c;
        task_complete(t, 0);
    } else {
        if (ls->pending_tail)
            ls->pending_tail->next_pending = c;
        else
            ls->pending_head = c;
        ls->pending_tail = c;
    }
}

static myio_sock *impl_tcp_listen(myio *io, const char *host, int port,
                                  int backlog, int *err) {
    uv_sock *s = sock_new(io_of(io));
    if (!s) {
        if (err)
            *err = ENOMEM;
        return NULL;
    }
    struct sockaddr_storage addr;
    int rc = uv_ip4_addr(host, port, (struct sockaddr_in *)&addr);
    if (rc != 0)
        rc = uv_ip6_addr(host, port, (struct sockaddr_in6 *)&addr);
    if (rc == 0)
        rc = uv_tcp_bind(&s->tcp, (struct sockaddr *)&addr, 0);
    if (rc == 0)
        rc = uv_listen((uv_stream_t *)&s->tcp, backlog, connection_cb);
    if (rc != 0) {
        if (err)
            *err = -rc;
        sock_destroy(s);
        return NULL;
    }
    return (myio_sock *)s;
}

static myio_task *impl_tcp_accept(myio *io, myio_sock *listener) {
    uv_io *u = io_of(io);
    uv_sock *ls = sock_of(listener);
    if (ls->accept_task)
        return task_new_failed(u, TASK_ACCEPT, EBUSY);
    uv_task *t = task_new(u, TASK_ACCEPT);
    if (!t)
        return NULL;
    t->sock = ls;
    uv_sock *c = ls->pending_head;
    if (c) {
        ls->pending_head = c->next_pending;
        if (!ls->pending_head)
            ls->pending_tail = NULL;
        c->next_pending = NULL;
        t->res.ptr = c;
        task_complete(t, 0);
    } else {
        ls->accept_task = t;
    }
    return handle_of(t);
}

/* ---- TCP: read / write / close ---- */

static void read_alloc_cb(uv_handle_t *h, size_t suggested, uv_buf_t *buf) {
    (void)suggested;
    uv_sock *s = h->data;
    *buf = s->read_task ? s->read_task->buf : uv_buf_init(NULL, 0);
}

static void read_cb(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    (void)buf;
    uv_sock *s = stream->data;
    if (nread == 0)
        return; /* spurious wakeup (EAGAIN); keep waiting */
    uv_read_stop(stream);
    uv_task *t = s->read_task;
    if (!t)
        return;
    s->read_task = NULL;
    task_complete(t, nread == UV_EOF ? 0 : nread);
}

static myio_task *impl_sock_read(myio *io, myio_sock *sock, void *buf,
                                 size_t len) {
    uv_io *u = io_of(io);
    uv_sock *s = sock_of(sock);
    if (s->read_task)
        return task_new_failed(u, TASK_SOCK_READ, EBUSY);
    uv_task *t = task_new(u, TASK_SOCK_READ);
    if (!t)
        return NULL;
    t->sock = s;
    t->buf = uv_buf_init(buf, (unsigned)len);
    s->read_task = t;
    int rc = uv_read_start((uv_stream_t *)&s->tcp, read_alloc_cb, read_cb);
    if (rc < 0) {
        s->read_task = NULL;
        task_complete(t, rc);
    }
    return handle_of(t);
}

static void write_cb(uv_write_t *req, int status) {
    uv_task *t = req->data;
    task_complete(t, status < 0 ? status : (int64_t)t->buf.len);
}

static myio_task *impl_sock_write(myio *io, myio_sock *sock, const void *buf,
                                  size_t len) {
    uv_task *t = task_new(io_of(io), TASK_SOCK_WRITE);
    if (!t)
        return NULL;
    t->sock = sock_of(sock);
    t->buf = uv_buf_init((char *)(uintptr_t)buf, (unsigned)len);
    t->u.write.data = t;
    int rc = uv_write(&t->u.write, (uv_stream_t *)&t->sock->tcp, &t->buf, 1,
                      write_cb);
    if (rc < 0)
        task_complete(t, rc);
    return handle_of(t);
}

static myio_task *impl_sock_close(myio *io, myio_sock *sock) {
    uv_sock *s = sock_of(sock);
    uv_task *t = task_new(io_of(io), TASK_SOCK_CLOSE);
    if (!t)
        return NULL;
    /* Outstanding pull-style tasks would otherwise never complete. */
    if (s->read_task) {
        uv_read_stop((uv_stream_t *)&s->tcp);
        task_complete(s->read_task, UV_ECANCELED);
        s->read_task = NULL;
    }
    if (s->accept_task) {
        task_complete(s->accept_task, UV_ECANCELED);
        s->accept_task = NULL;
    }
    /* Drop parked connections nobody accepted. */
    for (uv_sock *c = s->pending_head; c;) {
        uv_sock *next = c->next_pending;
        sock_destroy(c);
        c = next;
    }
    s->pending_head = s->pending_tail = NULL;
    s->close_task = t;
    sock_destroy(s);
    return handle_of(t);
}

static int impl_sock_port(myio *io, myio_sock *sock) {
    (void)io;
    struct sockaddr_storage ss;
    int len = sizeof ss;
    if (uv_tcp_getsockname(&sock_of(sock)->tcp, (struct sockaddr *)&ss, &len))
        return -1;
    if (ss.ss_family == AF_INET)
        return ntohs(((struct sockaddr_in *)&ss)->sin_port);
    if (ss.ss_family == AF_INET6)
        return ntohs(((struct sockaddr_in6 *)&ss)->sin6_port);
    return -1;
}

/* ---- synchronisation ---- */

static myio_result impl_await(myio *io, myio_task *task) {
    uv_task *t = task_of(task);
    while (!t->done) {
        if (uv_run(&io_of(io)->loop, UV_RUN_ONCE) == 0 && !t->done) {
            /* Loop has nothing left to do; the task can never complete. */
            t->res.status = MYIO_ERROR;
            t->res.error = EDEADLK;
            t->done = 1;
        }
    }
    return t->res;
}

/* Cancellation is a request; the status reported by await is authoritative.
 * libuv can stop timers, socket reads and pending accepts synchronously
 * (no callback fires afterwards), so those genuinely complete as
 * MYIO_CANCELED right here. Everything else is forwarded to libuv and the
 * completion callback reports the outcome - canceled or not. */
static int impl_cancel(myio *io, myio_task *task) {
    (void)io;
    uv_task *t = task_of(task);
    if (t->done)
        return -1;
    switch (t->kind) {
    case TASK_TIMER:
        uv_timer_stop(&t->u.timer);
        uv_close((uv_handle_t *)&t->u.timer, timer_close_cb);
        task_complete(t, UV_ECANCELED);
        return 0;
    case TASK_FS:
        /* Succeeds only while the request is still queued on the thread
         * pool; fs_cb will then deliver UV_ECANCELED. */
        return uv_cancel((uv_req_t *)&t->u.fs) == 0 ? 0 : -1;
    case TASK_SPAWN:
        return uv_cancel((uv_req_t *)&t->u.work) == 0 ? 0 : -1;
    case TASK_CONNECT:
        if (!t->connecting)
            return uv_cancel((uv_req_t *)&t->u.gai) == 0 ? 0 : -1;
        /* Closing the handle makes connect_cb fire with UV_ECANCELED. */
        sock_destroy(t->sock);
        return 0;
    case TASK_SOCK_READ:
        uv_read_stop((uv_stream_t *)&t->sock->tcp);
        t->sock->read_task = NULL;
        task_complete(t, UV_ECANCELED);
        return 0;
    case TASK_ACCEPT:
        t->sock->accept_task = NULL;
        task_complete(t, UV_ECANCELED);
        return 0;
    default:
        return -1; /* writes and closes cannot be taken back */
    }
}

static ptrdiff_t impl_select(myio *io, myio_task **tasks, size_t ntasks) {
    for (;;) {
        int any = 0;
        for (size_t i = 0; i < ntasks; i++) {
            if (!tasks[i])
                continue;
            any = 1;
            if (task_of(tasks[i])->done)
                return (ptrdiff_t)i;
        }
        if (!any)
            return -1;
        if (uv_run(&io_of(io)->loop, UV_RUN_ONCE) == 0) {
            for (size_t i = 0; i < ntasks; i++)
                if (tasks[i] && task_of(tasks[i])->done)
                    return (ptrdiff_t)i;
            return -1; /* loop drained without completing any of them */
        }
    }
}

static const char *impl_error_str(myio *io, int err) {
    uv_io *u = io_of(io);
    /* Codes were stored negated; -err is a libuv error code, which on POSIX
     * also covers plain errnos (UV_ENOENT == -ENOENT and so on). */
    return uv_strerror_r(-err, u->errbuf, sizeof u->errbuf);
}

/* ---- lifetime ---- */

static int impl_task_done(myio *io, const myio_task *task) {
    (void)io;
    return ((const uv_task *)task)->done;
}

static void impl_task_free(myio *io, myio_task *task) {
    uv_task *t = task_of(task);
    if (!t->done) {
        /* The in-flight request references this memory: cancel if possible,
         * then drain until completion before releasing it. */
        impl_cancel(io, task);
        impl_await(io, task);
    }
    if (t->kind == TASK_TIMER && !t->timer_closed) {
        t->free_on_close = 1;
        if (!uv_is_closing((uv_handle_t *)&t->u.timer))
            uv_close((uv_handle_t *)&t->u.timer, timer_close_cb);
        return; /* freed by timer_close_cb once the loop runs */
    }
    free(t);
}

static void impl_task_detach(myio *io, myio_task *task) {
    (void)io;
    uv_task *t = task_of(task);
    if (t->done)
        task_reap(t);
    else
        t->detached = 1;
}

static void close_walk_cb(uv_handle_t *h, void *arg) {
    (void)arg;
    if (uv_is_closing(h))
        return;
    switch (h->type) {
    case UV_TIMER: {
        /* A detached sleep whose timer never fired is reclaimed through
         * the close callback; an owned one stays for task_free. */
        uv_task *t = h->data;
        if (t->detached)
            t->free_on_close = 1;
        uv_close(h, timer_close_cb);
        break;
    }
    case UV_TCP:
        uv_close(h, sock_close_cb);
        break;
    default:
        uv_close(h, NULL);
        break;
    }
}

static void impl_destroy(myio *io) {
    uv_io *u = io_of(io);
    uv_walk(&u->loop, close_walk_cb, NULL);
    uv_run(&u->loop, UV_RUN_DEFAULT); /* flush pending close callbacks */
    uv_loop_close(&u->loop);
    free(u);
}

static const myio_ops uv_ops = {
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

myio *myio_uv_new(void) {
    uv_io *u = calloc(1, sizeof(*u));
    if (!u)
        return NULL;
    if (uv_loop_init(&u->loop) != 0) {
        free(u);
        return NULL;
    }
    u->base.ops = &uv_ops;
    return &u->base;
}
