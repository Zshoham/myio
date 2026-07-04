# myio

A backend-agnostic asynchronous IO interface for C. Programs code against
`include/myio.h` only; the IO mechanism (event loop, thread pool, coroutines,
or plain blocking calls) is picked by constructing a concrete backend and can
be swapped without touching program logic. Seven backends exist: libuv,
libxev, native io_uring, a thread pool, a blocking synchronous one, and two
Zephyr RTOS backends (an event loop and a thread pool) — the Zephyr pair makes myio a
portability layer across OS classes: develop protocol logic on Linux
against libuv/libxev, ship the same code in firmware.

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

## The io_uring backend

`src/myio_uring.c` (`myio_uring_new()`) implements the vtable directly on
Linux io_uring via liburing — no event-loop library in between. It exists
as a stress test of the interface: io_uring is the purest completion-queue
model there is, so it shows precisely what the task model absorbs and where
it pinches. The constructor returns NULL where io_uring is unavailable
(pre-5.6 kernel, `kernel.io_uring_disabled`, Docker's default seccomp
profile), so callers can fall back to another backend.

What the interface absorbed without contortions:

- **The myio task IS the io_uring completion object.** `user_data` carries
  the task pointer and one SQE yields exactly one CQE, so there is no
  impedance layer at all: no request objects (libuv), no completion structs
  (libxev). The dispatch that libuv spreads over 13 callbacks is one
  `switch (t->kind)` in the CQE drain, and every submit is prep + flush.
  Honest measurement, though: at 830 lines vs libuv's 729 it is *not*
  shorter — what the callbacks cost libuv, the explicit live-task and
  live-socket lists cost here (libuv's loop tracks its handles for you;
  a raw ring tracks nothing, and `destroy` has to find everything).
- **The buffer-lifetime rules were already the kernel's rules.** "Buffers
  stay valid until the task completes", "free may block", "detached buffers
  stay valid until destroy" were written with thread pools in mind, but they
  are exactly the conditions under which the kernel may read task memory
  asynchronously (sockaddrs, timespecs, path strings live in the task until
  the CQE). Nothing needed to change.
- **One-CQE-per-SQE makes detach airtight.** A canceled op still delivers
  its `-ECANCELED` CQE, so a detached task is always reaped on a real
  kernel event — the guarantee epoll-libxev's silently-killed completions
  had to reconstruct by hand.
- **`sock_write`'s all-or-error contract has a kernel spelling:
  `MSG_WAITALL`.** The header's "one rearm inside a backend" prediction
  came true and then improved: a first version rearmed short sends from
  userspace, and pushing 4 MB through a 4 KB `SO_SNDBUF` took 128
  CQE→SQE round trips; with `MSG_WAITALL` the kernel retries internally
  and delivers one CQE for the whole buffer. (Wall time was identical
  either way — and identical to a plain blocking `send()` control — the
  tiny-buffer test is bound by TCP window/delayed-ACK stalls, not by the
  backend; the 1,154 `io_uring_enter` calls cost 21 ms of CPU across a
  78 s run. The userspace rearm survives as the fallback for kernels
  before 5.19 and for errors after partial progress.)

What io_uring forced or exposed:

- **Cancellation is itself an async operation — the one place the vtable
  pinches.** `ASYNC_CANCEL` is an SQE with its own CQE, and even its result
  does not settle the target (`-EALREADY` means "running, may still
  complete"), but `cancel()` returns a synchronous int. The backend
  resolves this by submitting the cancel and then driving the ring until
  the *target's* CQE lands — legal because the re-entrancy guarantee means
  no user code can observe the loop turning inside `cancel()`, bounded
  because the kernel answers cancel requests promptly. The result is a
  cancel that is *truthful* rather than optimistic: 0 iff the op really
  was canceled — stronger than the request-only contract demands.
- **The completion queue covers the OS, not the runtime.** io_uring has no
  DNS op and cannot run arbitrary functions, so `tcp_connect` resolves
  inline in submit and `myio_spawn` runs inline in submit — both sanctioned
  by the eager-execution model (the Zephyr backend set the precedent). A
  truly async spawn would need a thread completing through an eventfd whose
  read is a pending ring op: the uv_async/xev.Async self-wake device yet
  again. Every full backend so far has needed one; this backend dodges it
  only by going inline.
- **Eager submission costs one `io_uring_enter` per operation.** Submit
  batching is io_uring's headline feature, and the header's "begins
  executing as soon as the submit function returns" promise forbids it
  (deferring SQEs to the next await would, e.g., skew when timers start).
  Measured at 13–18 µs per call it is immaterial here, but it is the same
  syscall count as blocking IO — and multishot accept/recv, provided
  buffer rings, and registered fds are equally unreachable through a
  pull-style one-op-at-a-time vtable with caller-owned buffers. The
  interface caps how good the io_uring code can get at roughly the libuv
  level; what it buys instead is the flattest possible implementation.
- `sock_close` must cancel outstanding ops *before* closing the fd: an
  in-flight recv holds a file reference, so `IORING_OP_CLOSE` alone would
  never wake it. Cancel-then-close in SQ order suffices in practice; the
  close CQE may arrive before the canceled ops' CQEs, so socket and tasks
  are unlinked from each other at submit time.

## The Zephyr backend

`src/myio_zephyr.c` (`myio_zephyr_new()`) runs the interface on Zephyr RTOS.
libuv and libxev only ever competed on how to do async IO on a big POSIX OS;
this backend is the test of whether `myio.h` works across OS classes. The
repo doubles as a Zephyr module (`zephyr/module.yml` + Kconfig), enabled
with `CONFIG_MYIO=y`.

- **The first backend with a hand-rolled event loop.** Zephyr has no event
  loop and no completion API, only readiness primitives — so where libuv
  pushes callbacks and libxev pulls completions, here `await`/`select` drive
  our own `run_loop_once()`: one `zsock_poll()` over every in-flight socket
  operation plus an eventfd, timeout set by the nearest pending timer, then
  expired timers fire, ready socket ops advance (or rearm — `sock_write`'s
  all-or-error contract really is "one rearm inside a backend"), and
  off-thread completions are harvested.
- **A hybrid, as the execution model sanctions.** Zephyr has no async file
  IO, and filesystem calls against flash are quick blocking calls: file
  operations (over the `fs_*` API, with a small fd table) complete inside
  submit, exactly like the sync backend, while sockets, timers, and spawns
  are genuinely asynchronous. Callers cannot tell the difference — that is
  the eager-submission design paying off. DNS also resolves inline in
  submit (Zephyr's async resolver needs the native IP stack, which
  offloaded-socket builds don't have).
- **`myio_spawn` runs on a dedicated work queue** whose completions marshal
  through a spinlock-protected list plus a zvfs eventfd kept in every poll
  set — the third backend in a row to need a uv_async/xev.Async-style
  self-wake device internally. `CONFIG_MYIO_SPAWN_INLINE=y` gives a
  zero-extra-threads build where spawn degrades to a blocking call in
  submit, again legal per the model.
- **Fixed-size allocation.** Tasks and sockets come from `k_mem_slab`s
  (`CONFIG_MYIO_MAX_TASKS`/`_SOCKETS`); nothing calls malloc. The interface
  already absorbed this: a submission with no free block returns NULL and
  `myio_join` maps it to ENOMEM — a path desktop backends never hit but a
  64-task slab makes routine (the Zephyr demo tests it).
- **Cancellation is the cleanest of any backend**: a readiness-based
  operation that hasn't fired holds no in-kernel state, so cancel is
  deregister + complete `MYIO_CANCELED` on the spot for nearly everything.
  The refusals: a partially sent `sock_write` and a spawn already running.
- Two flag-vocabulary traps, worth recording: Zephyr's socket layer speaks
  `ZVFS_O_NONBLOCK` (0x4000), deliberately not any libc's `O_NONBLOCK` (on
  native_sim the backend compiles against the *host* libc, where the values
  differ and the mismatch reads as `O_EXCL`); and `myio_open`'s O_* flags
  are mapped to `FS_O_*` explicitly for the same reason.
- On the host-offloaded socket driver (`native_sim` testing) `getsockname`
  doesn't exist, so `myio_sock_port()` falls back to the port the caller
  bound — bind-to-port-0 discovery is the one thing that can't work there.

### The Zephyr thread-pool backend

`src/myio_zephyr_pool.c` (`myio_zephyr_pool_new()`, `CONFIG_MYIO_POOL=y`)
is the desktop thread-pool backend ported to Zephyr: the same blocking
calls, run on `k_thread` workers, with the same one-mutex/two-condvars
concurrency model. The desktop pool and this one differ exactly where
POSIX and an RTOS differ:

- **The pool cannot grow.** Desktop workers are spawned on demand so a
  blocked op never starves new work; here the workers
  (`CONFIG_MYIO_POOL_WORKERS`) and their stacks are static, and a submit
  with every worker busy queues. Size the worker count for the maximum
  number of concurrently *blocking* operations — the usual static-RTOS
  sizing exercise. Cancellation still lifts a queued task out.
- **The cancel kick is an eventfd, not a signal.** The desktop pool
  interrupts a blocked `ppoll()` with `pthread_kill` and needs ppoll's
  atomic mask swap to close the signal-before-the-call race. Zephyr has no
  signals; instead each worker owns an eventfd and a cancelable wait blocks
  in `zsock_poll({eventfd, fd})`. The eventfd counter is stateful, so a
  kick that lands before the poll starts is still seen — the race ppoll
  exists to close cannot happen at all. `sock_close` and `destroy` use the
  same kick (`zsock_shutdown` is issued best-effort, but the offloaded
  driver doesn't implement it, so the kick does the waking).
- **`strdup` became a bounded field**: submit copies `path`/`host` into a
  `CONFIG_MYIO_STR_MAX` array inside the slab task; longer strings fail
  the submission with ENAMETOOLONG. Tasks and sockets come from the same
  fixed-slab discipline as the event-loop backend.

Where the desktop pool gets async DNS and file IO for free (blocking calls
on a growable pool), this one does too — which makes it the mirror image
of the event-loop backend: the pool blocks cheaply but holds a worker per
in-flight op; the loop holds no threads but resolves DNS and opens files
inline in submit. Both pass the same contract suite; the demo runs it on
each in turn.

### Comparing the two Zephyr backends

The two are answers to "where does the blocking go?": the event-loop
backend makes every operation non-blocking and parks in one
`zsock_poll()`; the pool backend leaves operations blocking and parks a
worker thread in each one. Everything else follows from that choice:

| | `myio_zephyr_new` (event loop) | `myio_zephyr_pool_new` (thread pool) |
|---|---|---|
| Threads | 0 (1 with the spawn work queue) | `MYIO_POOL_WORKERS` static workers |
| RAM | ~1 eventfd + slabs | + one stack (default 4 KB) and eventfd per worker |
| In-flight ops | one poll-set entry each (`ZVFS_POLL_MAX`) | one *worker* each; excess queues |
| File IO / DNS | inline in submit — the caller blocks briefly | on a worker — submit returns immediately |
| `myio_spawn` | serialized on the one work-queue thread | up to `MYIO_POOL_WORKERS` in parallel |
| Cancellation | deregister + complete on the spot, almost always | queued: lifted out; running sleep/read/accept: eventfd kick; connect/file/send/spawn: refused |
| Overload behavior | submit returns NULL (poll set/slab full) | tasks queue behind blocked workers — starvation if undersized |

The substantive differences:

- **Submit-time blocking is the loop's hidden cost.** `myio_tcp_connect`
  resolves DNS inline and `myio_open` walks the filesystem inline, so a
  slow resolver stalls the *submitting* thread. The pool never blocks a
  submit — its DNS and file IO are genuinely asynchronous, which is the
  one thing it does that the loop cannot.
- **Scaling shape is opposite.** The loop handles many idle sockets for
  free (one pollfd each) but processes every completion serially on the
  user thread. The pool's ceiling is hard — in-flight *blocking* ops
  beyond the worker count queue, and a queued task behind
  permanently-blocked workers is the starvation the desktop pool avoided
  by growing — but what does run, runs concurrently.
- **Cancellation tells you which model the OS likes.** Readiness ops hold
  no kernel state, so the loop cancels almost anything synchronously. The
  pool has to reach into a blocked thread — the per-worker eventfd kick —
  and refuses whatever has no interruption point, exactly like its
  desktop twin.

Rule of thumb: default to the event loop — it is the firmware-shaped one
(no stacks to size, degrades by refusing work rather than by starving
it); take the pool when submits must never block or `myio_spawn`s must
run in parallel. Both pass the same contract suite, and the demo runs it
on each in turn, so the choice stays swappable after the fact — the
interface's whole thesis.

### Building and running on native_sim

The demo (`examples/zephyr_demo`) runs the desktop demo's program logic
plus the cancel/teardown/OOM contract checks on `native_sim`, with littlefs
on the simulated flash for file IO and sockets offloaded to the host — real
networking with no TAP or root setup:

The Zephyr tree (pinned to v4.2.0) and the littlefs module are vendored as
submodules, and the python venv with west's deps is created on first build
— the only manual step is fetching the submodules:

```sh
git submodule update --init vendor/zephyr vendor/littlefs

make zephyr_demo    # creates examples/zephyr_demo/venv, configures, builds
make test-zephyr    # build and run; exits 0 iff every check passed
```

The Makefile defaults `ZEPHYR_BASE`/`ZEPHYR_LFS` to `vendor/zephyr` and
`vendor/littlefs`, and creates a venv with west's python deps at
`examples/zephyr_demo/venv` (via `python3 -m venv` + pip installing
`vendor/zephyr/scripts/requirements-base.txt`); override with
`make ZEPHYR_BASE=... ZEPHYR_LFS=... ZEPHYR_VENV=... test-zephyr`. No Zephyr
SDK is needed: `native_sim` builds with the host gcc
(`ZEPHYR_TOOLCHAIN_VARIANT=host`). In a west workspace the equivalent is
`west build -b native_sim/native/64` with this repo listed in
`ZEPHYR_EXTRA_MODULES`.

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
| `include/myio_uring.h`, `src/myio_uring.c` | native io_uring backend (`myio_uring_new`), Linux + liburing |
| `include/myio_sync.h`, `src/myio_sync.c` | blocking single-threaded backend (`myio_sync_new`) |
| `include/myio_zephyr.h`, `src/myio_zephyr.c` | Zephyr event-loop backend (`myio_zephyr_new`) |
| `include/myio_zephyr_pool.h`, `src/myio_zephyr_pool.c` | Zephyr thread-pool backend (`myio_zephyr_pool_new`) |
| `zephyr/` | Zephyr module glue (module.yml, CMakeLists, Kconfig) |
| `build.zig`, `vendor/libxev/` | build + vendored dependency of the libxev backend |
| `vendor/zephyr/`, `vendor/littlefs/` | vendored Zephyr tree (v4.2.0) and littlefs module for the Zephyr backends |
| `examples/demo.c` | example exercising concurrency, select, cancel |
| `examples/cancel_test.c` | cancel/detach contract checks (uv, xev, pool, uring) |
| `examples/concurrency_test.c` | concurrency + socket teardown checks (uv, xev, pool, uring) |
| `examples/zephyr_demo/` | the demo + contract checks as a native_sim Zephyr app |
| `examples/chat.c` | single-threaded async peer-to-peer terminal chat |
| `examples/chat_uv.c` | the same chat in raw libuv (comparison) |
| `examples/chat_asyncio.py` | the same chat in Python asyncio (comparison) |
| `examples/chat-rs/` | the same chat in Rust + tokio (comparison) |

## Build and run

```sh
make            # builds ./demo and ./chat (requires libuv, liburing, zig >= 0.16)
make test       # runs the demo on all five desktop backends, plus the contract tests
./demo uv
./demo xev
./demo pool
./demo uring
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
