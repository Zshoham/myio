# myio

A backend-agnostic asynchronous IO interface for C. Programs code against
`include/myio.h` only; the IO mechanism (event loop, thread pool, coroutines,
or plain blocking calls) is picked by constructing a concrete backend and can
be swapped without touching program logic. Four backends exist: libuv,
libxev, a thread pool, and a blocking synchronous one.

## Model

Operations start executing the moment they are submitted and return a
`myio_task *` handle. `myio_await()` blocks until a task finishes and returns
its `myio_result`; `myio_select()` waits for the first of a set to complete
(race an op against `myio_sleep()` for timeouts); `myio_cancel()` is only a
request — the status `myio_await()` reports is the authoritative outcome. In a
fully synchronous backend the operation completes inside the submit call and
`await` is a no-op — the calling code stays identical.

Task handles are caller-owned: release them with `myio_task_free()` after use.

Besides file IO and timers, the interface covers TCP: `myio_tcp_connect()`
and `myio_tcp_accept()` deliver an opaque `myio_sock *` in `result.ptr`,
`myio_tcp_listen()` creates a listener synchronously (port 0 = OS-assigned,
query with `myio_sock_port()`), and `myio_sock_read/write/close()` operate on
connections. At most one read and one accept may be outstanding per socket.
`myio_spawn()` runs an arbitrary (possibly blocking) function as a task, for
anything the built-in operations don't cover.

Convenience helpers, built only from the vtable so they work on every
backend: `myio_join()` (await + free, NULL-tolerant), `myio_await_all()`
(asyncio's gather), `myio_await_timeout()` (asyncio's wait_for; returns a
MYIO_PENDING result on timeout and leaves the task owned by the caller), and
`myio_ok()`. `myio_select()` skips NULL entries, so state machines can select
on a fixed array of slots and switch on the returned index.

## Design notes

The shape of the interface borrows deliberately:

- **Zig `std.Io`**: the io instance is an explicit parameter to every
  operation, and swapping the implementation (event loop, thread pool,
  coroutines, blocking) is a construction-time choice, not a compile-time one.
- **Rust async**: `myio_join()` is the consuming await; plain `myio_await()`
  exists separately because `select` losers still need their results read.
- **Go**: `myio_spawn()` is the "go" for blocking work, and NULL-skipping
  `myio_select()` mirrors selecting over nil channels to disable cases.
- **Python asyncio**: `myio_await_all()` and `myio_await_timeout()` are
  gather/wait_for, implemented generically on top of the vtable.

Tasks are intentionally *not* freed by `await` (unlike Rust futures): results
can be re-read, and tasks can appear in several selects. `myio_join()` is the
one-shot form.

## The libxev backend

[libxev](https://github.com/mitchellh/libxev) (vendored at
`vendor/libxev`, commit 9ce8e8e) is a completion-based event loop in Zig.
Its official C bindings cover only the loop, timers and the thread pool -
TCP and file IO exist only in the Zig API - so this backend is written in
Zig (`src/myio_xev.zig`) directly against that API and exports
`myio_xev_new()` with the C ABI; programs still include only C headers.

Notes from the port, compared with the libuv backend:

- **The myio task model fit without changes.** libxev is pull-style where
  libuv is push-style, which actually matches myio better: `accept` and
  socket reads are one-shot submissions, so the libuv backend's parked
  connection queue and `read_start`/`read_stop` choreography disappear.
- The implementation is generic over the libxev backend type and is
  instantiated for both io_uring and epoll; `myio_xev_new()` picks io_uring
  at runtime when the kernel (and any seccomp policy) allows it, else epoll.
- libxev has no async `open(2)` and no async DNS, so those run as blocking
  calls on libxev's thread pool, completing the task through an `xev.Async`
  wakeup - the same trick libuv uses internally for both.
- Cancellation differs per libxev backend: io_uring delivers
  `error.Canceled` to the canceled completion's callback, epoll kills the
  completion *silently* (so the cancel operation's own callback completes
  the task and releases what the dead completion held), and epoll cannot
  cancel thread-pool-routed work at all (so file reads/writes refuse
  cancellation there, like a blocking backend). myio's request-only
  `cancel` contract absorbs all three.
- One libxev invariant shapes the code: a completion callback must not
  synchronously `close()` an fd that is still registered with the loop
  (epoll deregisters it *after* the callback returns). Failed connects are
  therefore reclaimed with a loop-sequenced close, not `close(2)`.

## The thread-pool backend

`src/myio_pool.c` (`myio_pool_new()`) runs the exact blocking POSIX calls the
synchronous backend makes, but on a pool of worker threads, so the submitting
thread keeps going and several operations make progress at once. It depends
only on libc and pthreads — no event loop.

- **One mutex, two condition variables.** The mutex guards the work queue, the
  socket list, and every task's status fields. Workers sleep on a `work` cv
  until a task is queued; the (single) user thread sleeps on a `done` cv in
  `await`/`select`. Every completion broadcasts `done`, and each waiter
  re-checks its own predicate. The header's single-thread rule means
  submit/await/cancel/free never race each other, only the workers — which the
  mutex serialises.
- **The pool grows on demand.** A submit hands the task to an idle worker if
  one is waiting, otherwise it spawns a fresh thread. Because the operations
  block, this is what keeps a slow op from starving a newly submitted one:
  there is always a free or brand-new worker for incoming work. Threads are
  kept for reuse and joined only at `destroy`.
- **Cancellation reaches into running operations.** A still-queued task is
  lifted out of the queue. A running sleep, accept or socket read is blocked
  inside `ppoll()` with a dedicated signal unblocked only there; `cancel` sets
  a flag and `pthread_kill`s the worker, so the `ppoll` returns `EINTR` and the
  op reports `MYIO_CANCELED`. `ppoll`'s atomic mask swap closes the race a bare
  signal would have (one arriving just before the blocking call). Operations
  with no such interruption point — a running connect, file read/write, or
  socket write — refuse cancellation; as always, `await` is authoritative.
  `sock_close` additionally `shutdown(2)`s the socket to wake anything blocked
  on it, then a close worker waits for those to drain before releasing the fd.

## Chat comparison

The persistent peer-to-peer chat is implemented four times with identical
behavior and wire format (they all interoperate). Build the extras with
`make chat_uv` and `make chat-rs`; the Python one runs directly.

| Implementation | Lines | Shape |
|---|---|---|
| `chat.c` (myio) | ~235 | straight-line loop: `myio_select` over task slots |
| `chat_uv.c` (raw libuv) | ~285 | inverted into callbacks mutating shared state |
| `chat_asyncio.py` | ~130 | cooperating coroutine tasks sharing a peer slot |
| `chat-rs` (tokio) | ~130 | `tokio::select!` state machine, like the myio one |

Observations:

- **Raw libuv** is the control-flow ceiling: every step (resolve, connect,
  accept, read, retry timer) is a separate callback, and handle teardown
  needs explicit close choreography. The myio task model exists to flatten
  exactly this back into sequential code.
- **tokio** is the closest in shape to the myio version — `select!` over the
  current state's pending operations vs `myio_select` over task slots. Its
  win is implicit cancellation: dropping the losing future closes its
  connection, where C must cancel and close explicitly.
- **asyncio** decomposes into tasks (dialer, per-peer reader, stdin driver)
  coordinating through shared state instead of one select loop - less code,
  but the control flow lives in four places instead of one.
- The dynamic-language and native-async versions are half the size of the C
  ones; within C, myio buys back the linear control flow at a cost of ~50
  lines over raw libuv going the other way - and the same program also runs
  on the blocking backend.

## Layout

| Path | Purpose |
|---|---|
| `include/myio.h` | the interface (vtable + inline wrappers) |
| `include/myio_uv.h`, `src/myio_uv.c` | libuv backend (`myio_uv_new`) |
| `include/myio_xev.h`, `src/myio_xev.zig` | libxev backend (`myio_xev_new`), in Zig |
| `include/myio_pool.h`, `src/myio_pool.c` | thread-pool backend (`myio_pool_new`), libc + pthreads only |
| `include/myio_sync.h`, `src/myio_sync.c` | blocking single-threaded backend (`myio_sync_new`) |
| `build.zig`, `vendor/libxev/` | build + vendored dependency of the libxev backend |
| `examples/demo.c` | example exercising concurrency, select, cancel |
| `examples/cancel_test.c` | cancel/detach contract checks (uv, xev, pool) |
| `examples/concurrency_test.c` | concurrency + socket teardown checks (uv, xev, pool) |
| `examples/chat.c` | single-threaded async peer-to-peer terminal chat |
| `examples/chat_uv.c` | the same chat in raw libuv (comparison) |
| `examples/chat_asyncio.py` | the same chat in Python asyncio (comparison) |
| `examples/chat-rs/` | the same chat in Rust + tokio (comparison) |

## Build and run

```sh
make            # builds ./demo and ./chat (requires libuv and zig >= 0.16)
make test       # runs the demo on all four backends, plus the contract tests
./demo uv
./demo xev
./demo pool
./demo sync
```

The libxev backend is compiled into `zig-out/lib/libmyio_xev.a` by `zig
build` (the Makefile does this automatically) and linked into the demo and
the chat. `CHAT_BACKEND=xev ./chat ...` runs the chat on libxev instead of
libuv; the two interoperate.

The chat (`./chat <port> [host [peer-port]]`) always listens on its port and,
when a host is given, also dials `host:peer-port`, retrying every 10 seconds
while disconnected — so whichever peer comes up first, they find each other.
When the peer leaves it goes back to waiting instead of exiting:

```sh
./chat 7777 other-host   # terminal 1
./chat 7777 other-host   # terminal 2 (whichever dials second connects)
```

It multiplexes everything through `myio_select()`: a stdin read is pending at
all times, alongside either the peer read (connected) or the accept/connect/
retry-timer tasks (disconnected). Terminal IO goes through the interface too.
Set `CHAT_RETRY_MS` to shorten the retry interval for testing.

## Adding a backend

Implement the `myio_ops` vtable, embed `myio` as the first member of your
context struct (or return a bare `myio` if you need no state), and provide a
constructor like `myio *myio_mybackend_new(void)`. See `src/myio_sync.c` for
the minimal shape.
