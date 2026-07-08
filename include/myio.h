/*
 * myio - a backend-agnostic asynchronous IO interface.
 *
 * Programs depend only on this header. The actual IO mechanism (a libuv or
 * libxev event loop, native io_uring, a thread pool, plain blocking calls -
 * on desktop or on Zephyr RTOS) is chosen by constructing a concrete
 * `myio *` instance and can be swapped without changing program code.
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
 * Data buffers handed to an operation must stay valid until the task
 * completes. Strings are the exception: submit functions copy `path` and
 * `host`, so the caller may free or reuse them as soon as the submit call
 * returns (open and DNS resolution outlive the call by design, so every
 * backend wants the copy anyway).
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
 * that is still in flight is allowed but may block: the in-flight operation
 * references the task's memory (and the caller's buffers), so the backend
 * requests cancellation where it can and then waits until the operation
 * completes or can be stopped. `myio_task_detach()` is the non-blocking
 * alternative for "I don't care anymore": ownership passes to the backend,
 * which frees the task when it completes and discards its result (closing
 * a socket the task may win). `myio_join()` awaits and frees in one step -
 * use it whenever the result is needed exactly once.
 *
 * There is a trap in operations the backend cannot cancel. An in-flight
 * operation that cannot be stopped - a blocking read of a quiet stdin parked
 * on a thread pool, say - keeps referencing the task and the caller's buffers
 * until it finishes on its own, so `myio_task_free()` blocks on it, and,
 * because a detached task the backend must still finish also holds the
 * instance open, so does `myio_destroy()` even after `myio_task_detach()`:
 * both wait for input that may never come. For process-exit paths the
 * sanctioned escape hatch is to skip `myio_destroy()` entirely and let process
 * teardown reclaim the memory and file descriptors (examples/chat.c does this
 * for its pending stdin read).
 *
 * Threading and re-entrancy: a `myio` instance and its tasks must be driven
 * from a single thread (the backend itself may use threads internally). The
 * interface has no user-visible callbacks, so user code - and with it every
 * vtable call - runs only between backend loop iterations, never from
 * inside one. This is a guarantee backends may lean on: a submit, cancel,
 * or free issued by the caller can never race with the backend's own
 * completion processing, which permits cancellation and teardown orderings
 * that would be unsound under re-entrancy.
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
    int         error;  /* error code, valid when status == MYIO_ERROR. Sign
                           is the convention: negative is the platform's
                           getaddrinfo EAI_* code, verbatim - the same value
                           on every backend, so callers can branch on e.g.
                           EAI_NONAME portably - for a DNS resolution
                           failure; positive is errno-style for everything
                           else. myio_spawn is the one wrinkle: its user
                           function returns a negated code on failure (see
                           myio_fn above), which lands here still positive
                           but passed through verbatim rather than
                           reinterpreted as a system errno - the caller
                           defined what that number means. Render with
                           myio_strerror(), not strerror(), which
                           understands both ranges */
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

/*
 * Backend capabilities
 * --------------------
 * Bits returned by myio_caps(), describing what a backend's execution model
 * guarantees. They are static properties of the backend, not transient state:
 * a bit is either always set for a given instance or never. A program that
 * needs a capability should query it once at startup and fail loudly (rather
 * than silently misbehave later) when it is absent - see examples/chat.c,
 * which asserts MYIO_CAP_CONCURRENT_WAIT because an interactive multiplexer
 * cannot run on a backend that blocks on stdin at submit time.
 */
enum {
    /* Submitted operations make progress while others are still pending, so
     * myio_select() can genuinely multiplex several tasks. False only for the
     * fully synchronous backend, where every operation completes inside its
     * own submit call and nothing is ever concurrently "in flight". */
    MYIO_CAP_CONCURRENT_WAIT = 1u << 0,

    /* Submit calls never block the caller on IO: they hand back a pending
     * task and return promptly. False for the sync backend (everything blocks
     * inside submit) and for the Zephyr event-loop backend (DNS resolution
     * and file operations run inline in submit) and the native io_uring
     * backend (its tcp_connect resolves DNS inline in submit). */
    MYIO_CAP_NONBLOCKING_SUBMIT = 1u << 1,

    /* An in-flight file read or write can be canceled - myio_cancel() may
     * return 0 for one and myio_await() may then report MYIO_CANCELED. True
     * for the native io_uring backend and for the libxev backend when its
     * comptime backend is io_uring; false everywhere else, where file IO runs
     * as an uninterruptible blocking call (on a thread pool or inline). */
    MYIO_CAP_CANCEL_FILE = 1u << 2,

    /* myio_spawn() runs its function off the submitting thread, so a blocking
     * spawn does not stall the caller. False for the sync backend, for the
     * native io_uring backend (spawn runs inline in submit), and for the
     * Zephyr backend built with CONFIG_MYIO_SPAWN_INLINE. */
    MYIO_CAP_ASYNC_SPAWN = 1u << 3,
};

/* Backend vtable. Programs normally use the inline wrappers below instead.
 * Backends may assume none of these functions is ever invoked from inside a
 * completion callback (the re-entrancy guarantee above). */
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

    /* Synchronisation. The blocking await/select loops are not vtable
     * entries: they are implemented once in this header (myio_await and
     * myio_select below) on top of the two primitives here plus task_done. */

    /* Block until the backend has processed at least one completion (or
     * otherwise made observable progress), then return 1; a single call may
     * process any number of completions. Return 0 only when no progress is
     * possible: nothing in flight, loop drained - and never from a call
     * that completed a task, even when that task was the last one and
     * completing it drained the loop. Callers may therefore read a 0 as
     * "no task changed state during this call, and none ever will", which
     * is what lets the generic loops below report a deadlock without
     * re-polling their tasks. (Backends whose loop reports only "still
     * alive" - which is already false on the turn that processes the final
     * completion - keep a completion counter and report a turn that
     * advanced it as progress; see the libuv backend's step.) The generic
     * loops rely on a completed task's done flag being visible (through
     * task_done) as soon as the step() call that processed it returns; in
     * exchange, step() is only ever called between completions, never from
     * inside one (the re-entrancy guarantee above). */
    int         (*step)(myio *io);

    int         (*cancel)(myio *io, myio_task *task); /* 0 requested, -1 refused */

    /* Non-blocking: the task's stored result. Meaningful once task_done
     * reports completion; before that, a result with status MYIO_PENDING. */
    myio_result (*task_result)(myio *io, const myio_task *task);

    /* Human-readable message for a result.error code, following the sign
     * convention on myio_result.error above: negative renders via
     * gai_strerror (a getaddrinfo EAI_* code), positive via strerror. Plain
     * (positive) errno values must always be understood: the helpers in
     * this header produce them (e.g. ENOMEM in myio_join). The returned
     * string stays valid at least until the next error_str call on this
     * instance. */
    const char *(*error_str)(myio *io, int error);

    /* Bitwise-OR of MYIO_CAP_* describing this backend's execution model.
     * Static for the instance's lifetime (see the capabilities notes above). */
    unsigned (*caps)(myio *io);

    /* Task and instance lifetime. task_done is a non-blocking poll and
     * must agree with task_result: done exactly when the stored result's
     * status is no longer MYIO_PENDING. */
    int  (*task_done)(myio *io, const myio_task *task);
    void (*task_free)(myio *io, myio_task *task);
    void (*task_detach)(myio *io, myio_task *task);       /* free on completion */
    void (*destroy)(myio *io);
} myio_ops;

struct myio {
    const myio_ops *ops;
};

/* Open a file; on success result.value is the file descriptor.
 * `flags`/`mode` use the platform's open(2) conventions (O_RDONLY etc.).
 * `path` is copied; it need not outlive this call.
 *
 * The int64_t descriptors are a per-instance namespace: a value is only
 * portable to other operations on the same `myio` instance that produced it
 * via myio_open. POSIX-backed backends additionally accept raw OS descriptors
 * (0/1/2 and any other real fd) as an extension - the examples rely on this
 * to read stdin and write stdout - but backends with a private file table do
 * not: the Zephyr pair maps each fd to an internal fs_file_t table, where an
 * unopened value like 0 has no meaning. */
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

/* Write up to `len` bytes; result.value is the byte count actually
 * written, which - as with write(2) - may be less than `len`. This is
 * deliberately asymmetric with myio_sock_write(): a file write is
 * positional, so resubmitting from offset + result.value is trivial for
 * the caller. `buf` must stay valid until the task completes. */
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
 * Writes are not limited: several may be outstanding on one socket, and the
 * backend performs them in submission order, never interleaving the bytes
 * of different writes on the wire - as if each write completed before the
 * next began. Zero-length writes take their turn like any other write: one
 * submitted behind an in-flight write completes after it, on every backend.
 * (Without this guarantee, overlapping writes were silently
 * backend-dependent: an event loop serializes them naturally, but two
 * thread-pool workers could interleave chunks of two buffers.)
 *
 * Note for fully synchronous backends: since every operation completes
 * inside its submit call, submit operations in an order where each can
 * complete on its own — e.g. for an in-process test, submit the connect
 * before the accept so the accept finds a pending connection.
 */

/* Connect to `host` (IP literal or hostname, resolved by the backend) on
 * `port`. On MYIO_OK, result.ptr is the connected `myio_sock *`.
 * `host` is copied; it need not outlive this call. */
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
 * kernel (result.value == len). Deliberately all-or-error, unlike the
 * write(2) semantics of file myio_write(): continuing a partial send is
 * one rearm inside a backend, but a caller-side await-and-resubmit helper
 * could not participate in select(), so partial socket writes would be an
 * unsolvable composition problem for callers. Concurrent writes on one
 * socket run in submission order without interleaving (see the section
 * comment above); each keeps its individual all-or-error result, and after
 * a failed or canceled write the stream may end mid-message - writes queued
 * behind it still run, but the connection is typically torn down at that
 * point anyway. */
static inline myio_task *myio_sock_write(myio *io, myio_sock *sock,
                                         const void *buf, size_t len) {
    return io->ops->sock_write(io, sock, buf, len);
}

/* Close the socket and release its handle; `sock` is invalid once the task
 * completes. Outstanding read, accept, and write tasks on it complete as
 * MYIO_CANCELED. A canceled write may already have placed some of its bytes
 * on the wire - unavoidable when a connection is being torn down - so the
 * peer can observe a truncated message. */
static inline myio_task *myio_sock_close(myio *io, myio_sock *sock) {
    return io->ops->sock_close(io, sock);
}

/* Local port the socket is bound to, or -1 on error. */
static inline int myio_sock_port(myio *io, myio_sock *sock) {
    return io->ops->sock_port(io, sock);
}

/* Block until `task` completes and return its result. Idempotent once the
 * task is done. If the backend runs out of work while the task is still
 * pending (a drained loop: the task can never complete, typically a
 * dependency deadlock), a result with status MYIO_ERROR and error EDEADLK
 * is returned - and the task itself STAYS pending, because the backend
 * never completed it. Freeing such a task remains safe: myio_task_free has
 * its own drain path and does not rely on await having settled it. */
static inline myio_result myio_await(myio *io, myio_task *task) {
    while (!io->ops->task_done(io, task)) {
        if (!io->ops->step(io)) {
            /* step() never returns 0 from a call that completed a task,
             * so this task can never complete: dependency deadlock. */
            myio_result r = { MYIO_ERROR, 0, EDEADLK, NULL };
            return r;
        }
    }
    return io->ops->task_result(io, task);
}

/* Request cancellation of an in-flight task. Returns 0 when the request was
 * accepted, -1 when there is nothing to attempt (the task already completed,
 * or the backend cannot stop this kind of operation at all). A 0 may be
 * optimistic: it means only that the request was accepted or delivered, not
 * that cancellation will happen or has happened. Some backends answer
 * truthfully (0 only when they can genuinely act on the request); others
 * return an optimistic 0 without knowing whether the operation can still be
 * stopped - both are conforming. Acceptance is therefore not a promise:
 * cancellation is inherently racy, and the operation may still complete
 * normally (or fail) before the request takes effect. The authoritative
 * outcome, under either interpretation, is whatever myio_await() reports -
 * MYIO_CANCELED only if the operation was actually stopped. In particular, a canceled
 * tcp_connect or tcp_accept may still win and deliver a socket in
 * result.ptr, which the caller then owns and must close as usual. A task
 * with a cancellation pending stays outstanding until await reports it
 * (e.g. a canceled sock_read still counts toward the one-read-per-socket
 * limit until then). */
static inline int myio_cancel(myio *io, myio_task *task) {
    return io->ops->cancel(io, task);
}

/* Human-readable message for a result.error code - e.g. a DNS resolution
 * failure (negative error, an EAI_* code) renders as the real resolver
 * error instead of a squashed errno. Use instead of strerror(), which only
 * understands the positive half of the range. */
static inline const char *myio_strerror(myio *io, int error) {
    return io->ops->error_str(io, error);
}

/* Bitwise-OR of MYIO_CAP_* bits describing this backend's execution model.
 * Constant for the instance's lifetime, so a program that requires a
 * capability should check it once at startup (e.g.
 * `if (!(myio_caps(io) & MYIO_CAP_CONCURRENT_WAIT)) fail(...)`) and refuse to
 * run without it, rather than misbehave once the missing behaviour bites. */
static inline unsigned myio_caps(myio *io) {
    return io->ops->caps(io);
}

/* Block until at least one of `tasks` has completed; return the index of a
 * completed task (the lowest if several are done). NULL entries are skipped,
 * so a state machine can select on a fixed array of slots and switch on the
 * returned index. Returns -1 if no non-NULL task was given, or if the
 * backend ran out of work with none of them completed - the select analogue
 * of await's EDEADLK: none of these tasks can ever complete. Does not
 * consume results: await the winner afterwards (it will not block). Because
 * the lowest completed index wins, a task that completes and is resubmitted
 * on every iteration can starve later slots; place always-hot tasks in
 * later slots when that matters. */
static inline ptrdiff_t myio_select(myio *io, myio_task **tasks,
                                    size_t ntasks) {
    for (;;) {
        int any = 0;
        for (size_t i = 0; i < ntasks; i++) {
            if (!tasks[i])
                continue;
            any = 1;
            if (io->ops->task_done(io, tasks[i]))
                return (ptrdiff_t)i;
        }
        if (!any)
            return -1;
        if (!io->ops->step(io))
            return -1; /* loop drained without completing any of them */
    }
}

/* Non-blocking: has the task completed (in any status)? */
static inline int myio_task_done(myio *io, const myio_task *task) {
    return io->ops->task_done(io, task);
}

/* Release a task handle. If the task is still in flight this may block:
 * cancellation is requested where possible, and the call then waits until
 * the operation completes or stops referencing the task and the caller's
 * buffers. Prefer myio_task_detach() when the result no longer matters.
 * No-op on NULL, so results of failed submissions can be freed blindly. */
static inline void myio_task_free(myio *io, myio_task *task) {
    if (task)
        io->ops->task_free(io, task);
}

/* Relinquish ownership of a task without waiting for it (fire and forget).
 * The backend frees the task when it completes - immediately if it already
 * has - and the handle is invalid as soon as this returns. The result is
 * discarded: if the task wins a socket (tcp_connect/tcp_accept), the
 * backend closes it, even when the task had already completed and the
 * result was read. Detaching does not cancel; call myio_cancel() first to
 * also request that the operation stop. Buffers passed to a detached
 * operation must stay valid until the instance is destroyed, since the
 * caller can no longer learn when the operation finishes. No-op on NULL. */
static inline void myio_task_detach(myio *io, myio_task *task) {
    if (task)
        io->ops->task_detach(io, task);
}

/* Release the instance. Caller-owned tasks should be freed first; detached
 * tasks are finished or abandoned by the backend (which may block until
 * their operations can be stopped or complete). */
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
    myio_result r = myio_await(io, task);
    io->ops->task_free(io, task);
    return r;
}

/* Block until every task has completed (NULL entries are skipped). Returns
 * 0 if all completed with MYIO_OK, -1 otherwise. Does not free the tasks;
 * await or join them individually for their results. */
static inline int myio_await_all(myio *io, myio_task **tasks, size_t ntasks) {
    int rc = 0;
    for (size_t i = 0; i < ntasks; i++) {
        if (!tasks[i] || myio_await(io, tasks[i]).status != MYIO_OK)
            rc = -1;
    }
    return rc;
}

/* Retire a task whose result no longer matters - typically a select()
 * loser or a superseded attempt: request cancellation and detach in one
 * step. Cancellation is only best-effort, and ownership passes to the
 * backend regardless; if the operation wins anyway its result is
 * discarded (a socket it delivers is closed by the backend). The detach
 * rules still apply: buffers handed to the operation must stay valid
 * until it completes, or until the instance is destroyed. No-op on
 * NULL. */
static inline void myio_task_drop(myio *io, myio_task *task) {
    if (!task)
        return;
    io->ops->cancel(io, task);
    io->ops->task_detach(io, task);
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
    if (!myio_task_done(io, task)) {
        myio_task *timer = myio_sleep(io, ms);
        if (!timer)
            return myio_await(io, task); /* OOM: degrade to plain await */
        myio_task *pair[2] = { task, timer };
        myio_select(io, pair, 2);
        if (!myio_task_done(io, task)) {
            myio_task_free(io, timer);
            myio_result r = { MYIO_PENDING, 0, 0, NULL };
            return r;
        }
        /* The task won: the still-ticking timer is of no further interest. */
        myio_task_drop(io, timer);
    }
    return myio_await(io, task);
}

#endif /* MYIO_H */
