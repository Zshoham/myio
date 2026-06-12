/*
 * myio - a backend-agnostic asynchronous IO interface.
 *
 * Programs depend only on this header. The actual IO mechanism (libuv event
 * loop, thread pool, stackful/stackless coroutines, or plain blocking calls)
 * is chosen by constructing a concrete `myio *` instance and can be swapped
 * without changing program code.
 *
 * Execution model
 * ---------------
 * Every operation is submitted eagerly: it begins executing as soon as the
 * submit function returns, and yields a `myio_task *` handle. The handle is
 * later resolved with `myio_await()`, which blocks the caller until the task
 * completes and returns its result. In a fully synchronous backend the
 * operation completes inside the submit call and `await` is a no-op that
 * just fetches the stored result; the program's structure stays identical.
 *
 * Submitting several tasks before awaiting any of them is how concurrency is
 * expressed:
 *
 *     myio_task *a = myio_read(io, fd1, buf1, n1, MYIO_NO_OFFSET);
 *     myio_task *b = myio_read(io, fd2, buf2, n2, MYIO_NO_OFFSET);
 *     myio_result ra = myio_await(io, a);   // both reads run concurrently
 *     myio_result rb = myio_await(io, b);
 *
 * `myio_select()` blocks until at least one task of a set has completed and
 * returns its index; NULL entries are skipped, so a fixed array of "slots"
 * can be selected on directly. Combining an IO task with a `myio_sleep()`
 * task gives timeouts (or use `myio_await_timeout()`). `myio_cancel()` is
 * only a request: the backend may refuse it, and even an accepted request
 * can lose the race with completion. The task's final status, reported by
 * `myio_await()`, is the sole authority on whether it was canceled.
 *
 * `myio_spawn()` runs an arbitrary (possibly blocking) function as a task,
 * making anything the built-in operations don't cover - name lookups,
 * database clients, computations - schedulable through the same interface.
 *
 * Task lifetime
 * -------------
 * Task handles are owned by the caller and must be released with
 * `myio_task_free()`. Awaiting does not free the task, so a result may be
 * re-read and tasks may appear in several `select` calls. Freeing a task
 * that is still in flight is allowed; the backend cancels it or waits for
 * it as needed. `myio_join()` awaits and frees in one step - use it
 * whenever the result is needed exactly once.
 *
 * Threading: a `myio` instance and its tasks must be driven from a single
 * thread (the backend itself may use threads internally).
 */
#ifndef MYIO_H
#define MYIO_H

#include <errno.h>
#include <stddef.h>
#include <stdint.h>

typedef struct myio      myio;
typedef struct myio_task myio_task;
typedef struct myio_sock myio_sock;  /* backend-defined TCP socket handle */

/* User function for myio_spawn(): runs to completion exactly once. Returns
 * the task's result value (>= 0), or a negated errno code on failure. */
typedef int64_t (*myio_fn)(void *arg);

typedef enum {
    MYIO_PENDING  = 0,  /* still in flight */
    MYIO_OK       = 1,  /* completed successfully, see result.value */
    MYIO_ERROR    = 2,  /* failed, see result.error */
    MYIO_CANCELED = 3,  /* canceled before completion */
} myio_status;

typedef struct {
    myio_status status;
    int64_t     value;  /* op-dependent: fd for open, byte count for read/write */
    int         error;  /* errno-style code, valid when status == MYIO_ERROR */
    void       *ptr;    /* op-dependent handle: the myio_sock* delivered by
                           tcp_connect and tcp_accept, NULL otherwise */
} myio_result;

/* Pass as `offset` to read/write at the file's current position. */
#define MYIO_NO_OFFSET (-1)

/* Did the operation complete successfully? */
static inline int myio_ok(myio_result r) {
    return r.status == MYIO_OK;
}

/* Human-readable name of a status, for logging and diagnostics. */
static inline const char *myio_status_str(myio_status s) {
    switch (s) {
    case MYIO_PENDING:  return "pending";
    case MYIO_OK:       return "ok";
    case MYIO_ERROR:    return "error";
    case MYIO_CANCELED: return "canceled";
    }
    return "invalid";
}

/* Backend vtable. Programs normally use the inline wrappers below instead. */
typedef struct myio_ops {
    /* IO operations: start immediately, return a handle (NULL on OOM). */
    myio_task *(*open)(myio *io, const char *path, int flags, int mode);
    myio_task *(*close)(myio *io, int64_t fd);
    myio_task *(*read)(myio *io, int64_t fd, void *buf, size_t len,
                       int64_t offset);
    myio_task *(*write)(myio *io, int64_t fd, const void *buf, size_t len,
                        int64_t offset);
    myio_task *(*sleep)(myio *io, uint64_t ms);
    myio_task *(*spawn)(myio *io, myio_fn fn, void *arg);

    /* TCP networking. Listening is synchronous (bind/listen never block);
     * everything else follows the usual task model. */
    myio_task *(*tcp_connect)(myio *io, const char *host, int port);
    myio_sock *(*tcp_listen)(myio *io, const char *host, int port,
                             int backlog, int *err);
    myio_task *(*tcp_accept)(myio *io, myio_sock *listener);
    myio_task *(*sock_read)(myio *io, myio_sock *sock, void *buf, size_t len);
    myio_task *(*sock_write)(myio *io, myio_sock *sock, const void *buf,
                             size_t len);
    myio_task *(*sock_close)(myio *io, myio_sock *sock);
    int        (*sock_port)(myio *io, myio_sock *sock);

    /* Synchronisation. */
    myio_result (*await)(myio *io, myio_task *task);
    int         (*cancel)(myio *io, myio_task *task); /* 0 requested, -1 refused */
    ptrdiff_t   (*select)(myio *io, myio_task **tasks, size_t ntasks);
                                                  /* NULL entries are skipped */

    /* Task and instance lifetime. */
    int  (*task_done)(myio *io, const myio_task *task);   /* non-blocking poll */
    void (*task_free)(myio *io, myio_task *task);
    void (*destroy)(myio *io);
} myio_ops;

struct myio {
    const myio_ops *ops;
};

/* Open a file; on success result.value is the file descriptor.
 * `flags`/`mode` use the platform's open(2) conventions (O_RDONLY etc.). */
static inline myio_task *myio_open(myio *io, const char *path, int flags,
                                   int mode) {
    return io->ops->open(io, path, flags, mode);
}

static inline myio_task *myio_close(myio *io, int64_t fd) {
    return io->ops->close(io, fd);
}

/* Read up to `len` bytes; result.value is the byte count (0 at EOF).
 * `buf` must stay valid until the task completes. */
static inline myio_task *myio_read(myio *io, int64_t fd, void *buf, size_t len,
                                   int64_t offset) {
    return io->ops->read(io, fd, buf, len, offset);
}

static inline myio_task *myio_write(myio *io, int64_t fd, const void *buf,
                                    size_t len, int64_t offset) {
    return io->ops->write(io, fd, buf, len, offset);
}

/* Complete after `ms` milliseconds; useful as a timer/timeout task. */
static inline myio_task *myio_sleep(myio *io, uint64_t ms) {
    return io->ops->sleep(io, ms);
}

/* Run `fn(arg)` as a task (asynchronously where the backend can). On
 * completion, result.value is fn's return value, or result.error its negated
 * negative return. `arg` must stay valid until the task completes. */
static inline myio_task *myio_spawn(myio *io, myio_fn fn, void *arg) {
    return io->ops->spawn(io, fn, arg);
}

/*
 * TCP networking
 * --------------
 * Sockets are opaque `myio_sock *` handles owned by the backend; they are
 * released by awaiting a `myio_sock_close()` task (or by `myio_destroy`).
 * At most one read and one accept may be outstanding per socket at a time.
 *
 * Note for fully synchronous backends: since every operation completes
 * inside its submit call, submit operations in an order where each can
 * complete on its own — e.g. for an in-process test, submit the connect
 * before the accept so the accept finds a pending connection.
 */

/* Connect to `host` (IP literal or hostname, resolved by the backend) on
 * `port`. On MYIO_OK, result.ptr is the connected `myio_sock *`. */
static inline myio_task *myio_tcp_connect(myio *io, const char *host,
                                          int port) {
    return io->ops->tcp_connect(io, host, port);
}

/* Bind to `host` (IP literal) and `port` and start listening. Synchronous:
 * returns the listener, or NULL with an errno-style code in *err. Pass
 * port 0 to let the OS pick one; query it with myio_sock_port(). */
static inline myio_sock *myio_tcp_listen(myio *io, const char *host, int port,
                                         int backlog, int *err) {
    return io->ops->tcp_listen(io, host, port, backlog, err);
}

/* Accept one connection from a listener. On MYIO_OK, result.ptr is the
 * accepted `myio_sock *`. */
static inline myio_task *myio_tcp_accept(myio *io, myio_sock *listener) {
    return io->ops->tcp_accept(io, listener);
}

/* Read up to `len` bytes; result.value is the byte count, 0 when the peer
 * closed the connection. `buf` must stay valid until completion. */
static inline myio_task *myio_sock_read(myio *io, myio_sock *sock, void *buf,
                                        size_t len) {
    return io->ops->sock_read(io, sock, buf, len);
}

/* Write the whole buffer; completes once all `len` bytes are queued to the
 * kernel (result.value == len). */
static inline myio_task *myio_sock_write(myio *io, myio_sock *sock,
                                         const void *buf, size_t len) {
    return io->ops->sock_write(io, sock, buf, len);
}

/* Close the socket and release its handle; `sock` is invalid once the task
 * completes. Outstanding read/accept tasks on it complete as MYIO_CANCELED. */
static inline myio_task *myio_sock_close(myio *io, myio_sock *sock) {
    return io->ops->sock_close(io, sock);
}

/* Local port the socket is bound to, or -1 on error. */
static inline int myio_sock_port(myio *io, myio_sock *sock) {
    return io->ops->sock_port(io, sock);
}

/* Block until `task` completes and return its result. Idempotent once the
 * task is done. */
static inline myio_result myio_await(myio *io, myio_task *task) {
    return io->ops->await(io, task);
}

/* Request cancellation of an in-flight task. Returns 0 when the request was
 * accepted, -1 when there is nothing to attempt (the task already completed,
 * or the backend cannot stop this kind of operation at all). Acceptance is
 * not a promise: cancellation is inherently racy, and the operation may
 * still complete normally (or fail) before the request takes effect. The
 * authoritative outcome is whatever myio_await() reports - MYIO_CANCELED
 * only if the operation was actually stopped. In particular, a canceled
 * tcp_connect or tcp_accept may still win and deliver a socket in
 * result.ptr, which the caller then owns and must close as usual. A task
 * with a cancellation pending stays outstanding until await reports it
 * (e.g. a canceled sock_read still counts toward the one-read-per-socket
 * limit until then). */
static inline int myio_cancel(myio *io, myio_task *task) {
    return io->ops->cancel(io, task);
}

/* Block until at least one of `tasks` has completed; return the index of a
 * completed task (the lowest if several are done). NULL entries are skipped,
 * so a state machine can select on a fixed array of slots and switch on the
 * returned index. Returns -1 if no non-NULL task was given or the backend
 * cannot make progress. Does not consume results: await the winner
 * afterwards (it will not block). */
static inline ptrdiff_t myio_select(myio *io, myio_task **tasks,
                                    size_t ntasks) {
    return io->ops->select(io, tasks, ntasks);
}

/* Non-blocking: has the task completed (in any status)? */
static inline int myio_task_done(myio *io, const myio_task *task) {
    return io->ops->task_done(io, task);
}

/* No-op on NULL, so results of failed submissions can be freed blindly. */
static inline void myio_task_free(myio *io, myio_task *task) {
    if (task)
        io->ops->task_free(io, task);
}

/* Release the instance. Outstanding tasks should be freed first. */
static inline void myio_destroy(myio *io) {
    io->ops->destroy(io);
}

/*
 * Convenience helpers
 * -------------------
 * Built entirely from the operations above, so they work identically on
 * every backend.
 */

/* Await `task`, free it, and return its result: for results needed exactly
 * once. NULL-tolerant: a failed submission joins to an ENOMEM error, so
 * `myio_join(io, myio_write(io, ...))` is always safe. */
static inline myio_result myio_join(myio *io, myio_task *task) {
    if (!task) {
        myio_result r = { MYIO_ERROR, 0, ENOMEM, NULL };
        return r;
    }
    myio_result r = io->ops->await(io, task);
    io->ops->task_free(io, task);
    return r;
}

/* Block until every task has completed (NULL entries are skipped). Returns
 * 0 if all completed with MYIO_OK, -1 otherwise. Does not free the tasks;
 * await or join them individually for their results. */
static inline int myio_await_all(myio *io, myio_task **tasks, size_t ntasks) {
    int all_ok = 0;
    for (size_t i = 0; i < ntasks; i++) {
        if (!tasks[i])
            all_ok = -1;
        else if (io->ops->await(io, tasks[i]).status != MYIO_OK)
            all_ok = -1;
    }
    return all_ok;
}

/* Await `task` for at most `ms` milliseconds. If it completes in time, its
 * result is returned; otherwise a result with status MYIO_PENDING is
 * returned and the task stays in flight (cancel or keep awaiting it - the
 * caller still owns it either way). */
static inline myio_result myio_await_timeout(myio *io, myio_task *task,
                                             uint64_t ms) {
    if (!task) {
        myio_result r = { MYIO_ERROR, 0, ENOMEM, NULL };
        return r;
    }
    if (!io->ops->task_done(io, task)) {
        myio_task *timer = io->ops->sleep(io, ms);
        if (!timer)
            return io->ops->await(io, task); /* OOM: degrade to plain await */
        myio_task *pair[2] = { task, timer };
        io->ops->select(io, pair, 2);
        if (!io->ops->task_done(io, task)) {
            io->ops->task_free(io, timer);
            myio_result r = { MYIO_PENDING, 0, 0, NULL };
            return r;
        }
        io->ops->cancel(io, timer);
        io->ops->task_free(io, timer);
    }
    return io->ops->await(io, task);
}

#endif /* MYIO_H */
