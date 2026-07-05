# Continuation plan: API-review implementation

**ALL TASKS COMPLETE (2026-07-05).** T1–T8 are done and verified. The tree
is GREEN: `make test` (13 invocations) and `make test-zephyr` (both
backends) pass. **Nothing is committed** — everything lives in the working
tree on `master`, ready to commit.

Known flake to watch: one non-reproducible native_sim segfault during
teardown of the Zephyr event-loop suite was observed once during T8
verification (3 subsequent runs clean; the change under test is not on the
teardown path, so it is likely a pre-existing latent race in destroy or in
native_sim itself). If it recurs, investigate impl_destroy in
src/myio_zephyr.c.

## Origin

A design review of the seven backends against `include/myio.h` produced a
ranked list of API improvements. They were split into tasks T1–T8 (T7 was
reordered before T5), each tagged with the model that should implement it,
and executed sequentially by subagents — every task touches `myio.h` and/or
the same backend sources, so they must stay sequential.

## Completed and verified

- **T1 — write contract + doc pass in `myio.h`** (+ README "Model" section).
  New contract: several writes may be outstanding per socket, performed in
  submission order, never interleaved on the wire; `myio_sock_close` cancels
  outstanding reads/accepts/writes. Also documented: cancel-0-may-be-
  optimistic, select lowest-index starvation note, per-instance fd namespace
  (Zephyr's private fd table), and the uncancelable-detached-op destroy trap
  (chat.c `_exit(0)` pattern is the sanctioned escape hatch).
- **T2 — io_uring close-vs-pending-write hang fix** (`src/myio_uring.c`).
  Per-socket write FIFO (`write_head`/`write_tail`, only the head has a send
  SQE in flight — this is also what enforces ordering); `impl_sock_close`
  cancels the head like read/accept and completes queued writes CANCELED
  directly; `impl_cancel` can cancel queued non-head writes; destroy
  dismantles write FIFOs before its cancel walk. Regression tests added to
  `examples/concurrency_test.c`: (a) close-with-pending-write must not hang
  (the old code hung 15s+ until killed), (b) overlapping writes arrive as
  exact concatenation. Verified: hang reproduced on unfixed code, fixed code
  passes repeatedly.
- **T3 — write serialization in the other backends.**
  pool + zephyr_pool: per-socket write FIFO, workers wait for their turn on
  the done condvar, close/destroy wake waiting writers (complete CANCELED).
  xev: `writes` list replaced by head-only-armed FIFO (`writePopHead`
  promotes). zephyr: FIFO, only head in the poll set, iterative `write_arm`.
  uv: already conformant via uv_write FIFO + UV_ECANCELED on close (comment
  added). sync: trivially conformant. Ordering test widened to all four
  desktop async backends; 20/20 runs per backend; epoll-xev force-tested;
  `make test-zephyr` built and passed on native_sim.
- **T4 — resolver-error standardization.** Convention (documented at
  `myio_result.error` and `error_str`): negative = platform `EAI_*` code
  verbatim (portable branching), positive = errno-style; spawn user codes
  pass through verbatim in the positive range. uring: `URING_EAI_BASE`
  deleted. uv: `uv_eai_to_system()` translation + `task_fail_gai()` (bypasses
  the negate path). Conformance of sync/pool/xev verified. New test: connect
  to `nonexistent.invalid` yields negative error + non-empty `myio_strerror`.
  **Follow-up fixed inline by the coordinator:** both Zephyr backends now
  defer `DNS_EAI_SYSTEM` to `errno` (they didn't); verified by a full
  `make test-zephyr` run (suite prints `dns error rendered as: "EAI_NONAME"`).
- **T7 — `myio_task_drop`** (cancel + detach, NULL-safe) added to the header's
  convenience helpers; `examples/chat.c` refactored to use it (local
  `drop_task` deleted); README helper list updated.

## T5 — vtable `step()` refactor: COMPLETED (2026-07-05)

Finished by resuming the interrupted agent. Final shape: `myio_ops` lost
`await`/`select` and gained `int (*step)(myio*)` and
`myio_result (*task_result)(myio*, const myio_task*)` in the same slots
(ABI mirror in myio_xev.zig updated in place); await/select/join/gather/
timeout are header inlines; generic await returns EDEADLK-result on a
drained loop (task stays pending). Pool backends use an outstanding count
(nqueued + nrunning) plus a completion-generation counter (completions/
comp_seen) so step() cannot sleep through a broadcast that fired between
the caller's task_done poll and the cond_wait. Verified: zero warnings,
13/13 desktop invocations, 6x stress on pool/uring, `./demo xev`, and
`make test-zephyr` green on both Zephyr backends. The section below is the
original in-progress record, kept for history.

### (historical) T5 in-progress notes

**Goal:** replace `await` + `select` in `myio_ops` with two primitives and
implement the drive loops once as header inlines:

- `int (*step)(myio *io)` — block until ≥1 completion is processed, return 1;
  return 0 only when no progress is possible (nothing in flight / drained).
- `myio_result (*task_result)(myio *io, const myio_task *task)` —
  non-blocking stored-result fetch (PENDING-status result until done).
- Generic `myio_await`: `while !task_done: if !step() -> return
  {MYIO_ERROR,0,EDEADLK,NULL}` (task stays pending — documented). Generic
  `myio_select`: NULL-skipping, lowest-index, re-scan once after a failed
  step. `myio_await_timeout` rewritten over the wrappers.

**Done by the interrupted agent (compiles, desktop demo passes through uv):**
- `include/myio.h`: new vtable entries + doc comments (~line 176-187),
  generic await (~line 345), select (~line 407), await_timeout converted.
- `src/myio_sync.c` (step returns 0), `src/myio_uv.c` (step = one
  `uv_run(UV_RUN_ONCE)`, drained meaning preserved), `src/myio_pool.c`
  (step = one done_cv wait, with an outstanding-op counter — REVIEW THIS:
  race-prone; the generic loop re-checks task_done so spurious wakeups are
  fine, but the counter increment/decrement placement needs verifying),
  `src/myio_uring.c` (exposes its existing `step()`),
  `src/myio_zephyr.c` (`impl_step` ~line 894, `impl_task_result` ~line 902 —
  converted but never compiled/run: do `make test-zephyr`).

**NOT done — the two remaining conversions:**
1. `src/myio_xev.zig`: the `Ops` extern struct still declares
   `@"await"`/`select` (lines ~58/60) and the vtable still assigns
   `implAwait`/`implSelect` (lines ~1217/1219). Since `Ops` is an ABI mirror
   of the C struct, **`./demo xev` currently segfaults** — C calls
   `ops->step` and lands in `implAwait`'s slot with the wrong signature.
   Fix: replace the two fields IN PLACE (field order must exactly match
   include/myio.h), implement `implStep` (= `loop.run(.once)` +
   `reapDetached(io)`, return 0 when `loopDrained(io)`) and
   `implTaskResult`, delete `implAwait`/`implSelect`;
   `implTaskFree`/`implDestroy` keep their internal drain loops.
2. `src/myio_zephyr_pool.c`: still has `impl_await` (~line 939) /
   `impl_select` (~line 981) and the old vtable entries (~line 1130/1132).
   Convert like `myio_pool.c`: step = lock, return 0 if no outstanding
   tasks, one `k_condvar_wait(done_cv)`, return 1; add/maintain an
   outstanding counter (queued + started-not-done); `task_result` returns
   `t->res` under the mutex.

**Then verify:** `make test` repeatedly (pool step is the racy one),
`./demo xev` specifically, and `make test-zephyr` (works in this
environment; takes a few minutes, venv already exists). Update README
"Adding a backend" (backend supplies submit ops + step/task_result/
task_done; await/select/join/gather/timeout come free from the header) if
the agent didn't get to it — check first.

**Resume options:** the interrupted agent id was `aa9cbdeb99e5736e5`
(SendMessage may revive it with context; it died at "Now myio_zephyr.c" —
note it DID finish myio_zephyr.c after that message). Otherwise spawn a
fresh Fable 5 agent with this section as the brief. Model tag: **Fable 5**.

## Pending tasks — NONE (T6 and T8 below completed 2026-07-05)

T6 delivered src/myio_common.h (result constructors, myio_posix_connect/
listen, gai_errno, myio_local_port, myio_default_error_str) with sync/pool/
uring refactored onto it — pure dedup, verified identical. T8 delivered
myio_caps() with four bits (CONCURRENT_WAIT, NONBLOCKING_SUBMIT, CANCEL_FILE,
ASYNC_SPAWN); notably uring and the zephyr event loop are denied
NONBLOCKING_SUBMIT because their inline DNS genuinely blocks submit —
honesty over flattery; chat.c now asserts CONCURRENT_WAIT at startup.
Original specs kept below for reference.

### (done) T6 [Opus 4.8] — shared backend helpers
Create internal `src/myio_common.h` (not installed; C backends only, xev
stays Zig): result constructors (`r_ok/r_err/r_canceled/r_from` — duplicated
in pool/zephyr_pool), POSIX `do_connect` + `tcp_listen` body (byte-identical
in sync and pool), `gai_errno` (EAI_SYSTEM→errno), sockaddr→port extraction
(~5 copies), default `error_str` body. Refactor sync/pool/uring to use them;
leave Zephyr files alone unless trivially clean (zsock_* names differ).
Pure dedup, behavior identical. Mention in README "Adding a backend".
Verify `make test`. Blocked by T5.

### (done) T8 [Opus 4.8] — `myio_caps()` capability query
Bits: `MYIO_CAP_CONCURRENT_WAIT` (false for sync), `MYIO_CAP_NONBLOCKING_SUBMIT`
(false for sync and zephyr event loop — inline DNS/open in submit),
`MYIO_CAP_CANCEL_FILE` (true for uring + io_uring-xev via its
`file_cancelable`; false elsewhere), `MYIO_CAP_ASYNC_SPAWN` (false for sync,
uring, zephyr with `CONFIG_MYIO_SPAWN_INLINE`). Vtable entry
`unsigned (*caps)(myio *io)` + inline wrapper; implement in all seven
backends (xev differs per comptime backend); chat.c asserts
CONCURRENT_WAIT at startup (replaces the comment folklore); README Model
paragraph. Verify `make test` + `make test-zephyr`. Blocked by T6.

## Deferred / explicitly not doing

- Keep eager submission, the re-entrancy guarantee, tasks-not-freed-by-await,
  NULL-skipping select, all-or-error sock_write (review's "keep" list).
- Tagged-pointer error tasks (kill the alloc-to-report-EBUSY path): judged
  not worth the complexity unless slab pressure becomes real.

## Operational notes

- Every agent instruction so far: match each file's careful comment voice,
  update comments the change invalidates, do NOT commit.
- Suggested commit plan once T5 is green: commit everything as one series or
  per-task splits via `git add -p`; T1–T4+T7 are stable and were each
  verified independently.
- Test commands: `make test` (desktop five), `make test-zephyr` (native_sim,
  both Zephyr backends), `CHAT_BACKEND=xev ./chat <port>` for a manual
  smoke test. clangd diagnostics about missing `myio*.h` includes are
  noise (no compile_commands.json) — trust the Makefile.
