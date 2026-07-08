# Review pass over the API-hardening round (commits ccab740...HEAD)

**FIX ROUND COMPLETE (2026-07-05, uncommitted in the working tree).**
Everything below was implemented except the two items marked optional:
finding 9 (zephyr_pool nqueued, kept as an explicit invariant) and
follow-up D (dropping task_done from the vtable). What landed:

- **A + findings 12/10/11:** step() re-specified ("a call that completes a
  task must not return 0"); uv/xev grew an `ncompleted` counter in their
  completion funnels; the generic await/select re-poll branches are gone;
  the uv/xev/zephyr drain loops now call impl_step; the stale zephyr top
  comment is fixed. README "Adding a backend" updated.
- **B + finding 13:** new OS-include-free `src/myio_wq.h` (push/pop/remove
  intrusive FIFO); adopted by uring, zephyr, pool, zephyr_pool; the Zig
  backend mirrors the shape with writePush/writePopHead/writeListRemove.
- **C + finding 15:** xev routes len==0 writes through the FIFO; the
  ordering paragraph in myio.h says zero-length writes take their turn;
  regression test added (fails on the old xev shortcut).
- **Findings 1/2:** pool + zephyr_pool rewired to dispatch only the FIFO
  head to a worker; non-head writes are parked (no worker, no queue slot)
  and promoted by write_advance on the head's completion. The done_cv
  write-turn wait is gone (the write-turn share of finding 2; step() and
  close-drain remain the only waiters). Verified by a thread-count probe:
  32 congested writes on one socket = 4 threads (was ~34).
- **Findings 5/6/7/8:** memcmp verify; uring uses gai_errno(); shared
  examples/test_common.h; myio_await_timeout calls myio_task_drop.

Verified green: `make test` (13 desktop invocations), 5x stress on all
four async desktop backends (concurrency + cancel), and `make test-zephyr`
(both Zephyr backends on native_sim).

Second-pass review (high effort, 8 finder angles + verification) over the
three commits: write-serialization FIFOs + io_uring close-vs-write fix,
EAI_* error convention, step()/task_result() vtable refactor, myio_caps(),
myio_task_drop(), myio_common.h, tests.

**Status: Phase 2 verification complete.** All 15 candidates verified
against the code (CONFIRMED / PLAUSIBLE / REFUTED marks inline below).
Finder angles complete: reuse (2), efficiency (5), simplification (6),
altitude (3), conventions (0 — no CLAUDE.md exists), and the three
correctness angles — line-by-line diff scan, removed-behavior audit,
cross-file tracer (incl. the Zig ABI-mirror field-order check, which
positively verified).

## Candidates so far

### Efficiency

1. **[CONFIRMED] src/myio_pool.c:418 — queued socket write occupies a
   worker while waiting its turn.** A write behind the FIFO head is
   dispatched to a worker (dequeued at worker_main:519, nrunning++) that
   just blocks on done_cv (myio_pool.c:419-421) until its turn. Submitting
   more concurrent writes to one socket than the pool has threads ties up
   every worker; unrelated ops on other sockets starve until the
   congested socket drains. The pool grows workers on demand so it is
   growth-bounded; **src/myio_zephyr_pool.c:554 mirrors the design with a
   FIXED worker count (CONFIG_MYIO_POOL_WORKERS) — starvation there is
   permanent, not growth-bounded.** Fix shape: leave non-head writes in
   the pool's task queue (undelivered) until the FIFO head advances, only
   then hand them to a worker. The most substantive finding.

2. **[CONFIRMED] src/myio_pool.c:495 — one shared done_cv broadcast wakes
   every waiter.** Completions broadcast a single condvar shared by
   step(), write-turn waiters, and close-drainers: one completion wakes
   O(waiters) threads to re-check false predicates. Fix shape: per-socket
   condvar for write turns, or targeted signal.

3. **[CONFIRMED — accepted cost] include/myio.h:446 — generic select
   scans with one mutex round-trip per task_done() on lock-based
   backends.** Old pool select scanned all tasks under one lock; the
   generic loop does n lock/unlock pairs per scan per wakeup. Cost is
   uncontended-mutex churn; shape of fix if it ever matters: an optional
   batched vtable fast path. Accepted cost of the refactor — the lock
   backends (pool/zephyr_pool) still own a faster path if profiling ever
   shows it.

4. **[CONFIRMED — accepted cost] include/myio.h:386 — generic await
   costs ~3 vtable calls (task_done + step + task_result) where old await
   cost one.** Same trade-off as (3); the step() generation-counter
   design (pool/zephyr_pool) keeps most wakeups to a single step() call.

5. **[PLAUSIBLE — partial] examples/concurrency_test.c:157,182 — 8 MB +
   7 MB test buffers per backend per run.** The byte-by-byte verification
   at concurrency_test.c:210-217 is a clear win for memcmp (the buffers
   are filled with single repeating chars 'A'+i, so memcmp on a filled
   reference would do). The buffer *size* is intentional — they must
   exceed SO_SNDBUF so the writes are genuinely in flight together (the
   whole point of the ordering test), so shrinking to "a few hundred KB"
   risks weakening the test on hosts with large socket buffers. Keep the
   sizes, swap the verify loop for memcmp.

### Reuse

6. **[CONFIRMED] src/myio_uring.c:511 — impl_tcp_connect re-inlines the
   EAI_SYSTEM→errno ternary instead of calling gai_errno() from
   myio_common.h**, which the file already includes (myio_uring.c:20) and
   uses myio_local_port / myio_default_error_str from. uring cannot share
   myio_posix_connect (it needs the sockaddr for an async connect SQE),
   but the one-line ternary is exactly gai_errno(rc). Two copies of the
   mapping can drift.

7. **[CONFIRMED] examples/concurrency_test.c:27 — failures/expect()/
   count_fds()/make_io() duplicated verbatim from
   examples/cancel_test.c:25.** Both files carry byte-identical copies
   (failures, expect, count_fds, make_io). A shared examples/test_common.h
   would keep the two binaries agreeing on backend lists and fd counting.

### Simplification

8. **[CONFIRMED] include/myio.h:561 — myio_await_timeout hand-rolls
   cancel+detach for the losing timer** instead of calling myio_task_drop
   (), defined 14 lines below in the same header (myio_await_timeout does
   `myio_cancel + myio_task_detach` at :561-562; myio_task_drop at :575
   does exactly that, plus a NULL guard). Move task_drop above and call
   it — also gets the NULL guard for free.

9. **[PLAUSIBLE — low value] src/myio_zephyr_pool.c:148 — the new
   `nqueued` counter is redundant in this backend.** nqueued is
   decremented exactly when qhead advances (worker pickup at :649,
   queue_remove at :252) and incremented only at queue_push (:237), so
   `nqueued == 0` ⟺ `qhead == NULL`. step() at :970 checks
   `nqueued == 0 && nrunning == 0`, which `qhead == NULL && nrunning == 0`
   already answers. Unlike the desktop pool, zephyr_pool has FIXED workers
   and no idle>=nqueued spawn check that would actually consume the count.
   Three update sites of extra invariant; a missed decrement makes
   impl_step spin/hang. Redundant but harmless, and arguably clearer as
   an explicit invariant — fix is optional.

10. **[PARTIAL] step() body re-inlined in task_free/destroy drain loops**
    in three backends instead of calling the step function itself:
    src/myio_uv.c:688, src/myio_zephyr.c:1013 (+ impl_destroy ~1037),
    src/myio_xev.zig:1148. **uv:688 is a clean fit** — the drain loop
    `while (!t->done && uv_run(...,UV_RUN_ONCE) != 0)` is literally
    impl_step's body, and short-circuit on `!t->done` makes the
    0-return-on-final-completion safe, so `while (!t->done && impl_step
    (io))` is equivalent and simpler. **xev:1148 and zephyr:1013 are not
    clean swaps**: xev's drain uses `catch break` / `loopDrained` guards
    that differ from implStep's `catch return 0`, and zephyr:1013 drives
    run_loop_once with a progress_possible predicate rather than the
    condvar-waiting impl_step. If step() ever grows bookkeeping the uv
    copy silently diverges; the other two would need a separate
    drain-primitive to share.

11. **[CONFIRMED] src/myio_zephyr.c:5 — stale top-of-file comment** still
    describes the pre-refactor design ("await and select are the same
    while(!done) run_loop_once() shape"); the backend now only implements
    step()/task_result(). The sibling files' comments were updated; this
    one was missed.

### Correctness (cross-file tracer)

15. **[CONFIRMED] src/myio_xev.zig:887 — zero-length sock_write bypasses
    the write FIFO and completes immediately**, the only backend that
    does this (implSockWrite at :887 short-circuits `len == 0` via
    completeOk; the other five — pool:709, uring:635, uv:532, zephyr:729,
    zephyr_pool:880 — route len==0 through the normal FIFO / uv_write
    path). A zero-length write submitted behind an in-flight write
    completes first on xev but waits its turn on the other five,
    observably violating the header's "as if each write completed before
    the next began" (include/myio.h:308) completion-ordering contract for
    callers watching completion order — even though no wire bytes
    reorder. Fix: route len==0 through the FIFO like the other five
    backends (or explicitly carve zero-length writes out of the ordering
    contract in the header).

The tracer also POSITIVELY VERIFIED (no finding): the Zig Ops extern
struct matches include/myio.h field-for-field; myio_common.h helpers are
byte-identical to the code they replaced (checked against ccab740);
demo.c/cancel_test.c per-backend assumptions still hold; the pool
completions/comp_seen generation counters check out.

### Altitude

12. **[CONFIRMED] src/myio_uv.c:600 + src/myio_xev.zig:1057 — step()
    violates its own documented contract on the last completion.** Both
    forward the loop's "still alive" return as the progress signal
    (uv_run UV_RUN_ONCE at :601; `!loopDrained` at :1062), which is 0 on
    the very call that processes the final completion. The generic header
    loops compensate with the permanent "step returned 0, re-poll tasks
    once more" branch (await at include/myio.h:389-392, select at :460-
    463) that pool/uring/zephyr_pool never need. Both step() comments
    acknowledge the dependence. Any future direct consumer of the vtable
    step() that follows the documented contract literally will
    spuriously report deadlock on uv/xev only. Deep fix: make uv/xev
    step() track whether the run(.once) call completed any task (a
    completion counter, as the pools do) and return 1 for it; then the
    generic re-poll branch can go.

13. **[CONFIRMED] Write-FIFO pointer bookkeeping hand-copied ~4 times**
    (myio_pool.c:243, myio_zephyr_pool.c:267, myio_zephyr.c:663,
    myio_uring.c:255 + 766, plus the Zig list at myio_xev.zig:909) with
    different task-struct names (pool_task / zp_task / z_task / uring_task
    / Task). A tail-fixup bug fixed in one copy leaves the others.
    Candidate for a macro-based or field-offset helper in myio_common.h
    (C backends); the Zig copy is necessarily separate. Overlaps finding
    10's theme: shared discipline, per-file copies.

14. **[REFUTED as stated] include/myio.h:163 — MYIO_CAP_NONBLOCKING_SUBMIT
    bundles two axes.** The header at :158-163 ALREADY documents the bit
    as connect-inclusive ("its tcp_connect resolves DNS inline in
    submit"), and the uring caps comment at src/myio_uring.c:820-822
    explicitly explains why uring does not set it. The "document the bit
    as connect-inclusive" suggestion is already done. Residual
    (non-fixable, by design): callers that never resolve hostnames cannot
    distinguish uring from a fully-async backend via this one bit — but
    that is an inherent property of a coarse capability enum, and
    concurrency_test.c falling back to strcmp on backend names is a test-
    harness choice, not a contract gap. A narrower MYIO_CAP_ASYNC_DNS bit
    could be added if a real caller needs it; no current caller does.

### Correctness angles (completed)

**Line-by-line diff scan.** Walked the full ccab740..HEAD diff for the
substantive backends (pool, uring, zephyr_pool, sync, header). The two
advertised fixes check out:
- **uring close-vs-write** (src/myio_uring.c:695-714): sock_close now
  ASYNC_CANCELs the in-flight head write and completes the queued rest
  CANCELED on the spot (no SQE → no CQE would ever come for them). The
  new `wcanceled` flag (:83, set at :786) closes the rearm-after-short-
  send race: a head write whose send completes short before the cancel
  reaches it takes the `!t->sock || t->wcanceled` branch (:351) and
  finishes CANCELED instead of resubmitting a send the cancel can no
  longer find.
- **pool close-vs-write** (src/myio_pool.c:740-744): sock_close now
  `shutdown(SHUT_RDWR)` (interrupts the head send) **and** broadcasts
  done_cv (wakes turn-waiting writers, which see `closing` and complete
  CANCELED without sending). The old code only did shutdown, so
  turn-waiters slept forever.

**Removed-behavior audit.** The sync backend lost the most surface
(impl_await/impl_select → impl_step/impl_task_result). Verified
equivalent: old impl_select returned the first non-NULL index (all
tasks done); the generic select (include/myio.h:448-456) returns the
first non-NULL *done* index, which for sync is the first non-NULL index
— identical. old impl_await returned t->res; generic await sees
task_done==1 immediately and returns task_result — identical, and sync
never returns the new EDEADLK path (the loop body never runs). The EAI
mapping change in uring (URING_EAI_BASE offset → verbatim negative rc)
is an intentional contract change, documented in the header (:103-110)
and asserted by concurrency_test.c:237 (`dr.error < 0`).
myio_common.h helpers are byte-identical to the code they replaced
(tracer-verified against ccab740). chat.c's drop_task → myio_task_drop
refactor is behavior-equivalent (manual NULL after, same cancel+detach).
No removed-behavior regressions found.

**Cross-file tracer.** (Mostly completed in the earlier pass.) Positively
verified: the Zig `Ops` extern struct matches include/myio.h `myio_ops`
field-for-field and field-order; the MYIO_CAP_* constants in
myio_xev.zig:72-75 mirror the header enum; the result constructors and
gai_errno in myio_common.h are byte-identical to the per-backend copies
they replaced; demo.c / cancel_test.c per-backend assumptions still hold;
pool/zephyr_pool completions/comp_seen generation counters are
consistent (decremented in worker_run and worker_close commits; queued-
cancel via complete_unrun/impl_cancel correctly skips nrunning but
broadcasts, and the generic loops see the now-done task via task_done
before ever calling step, so no missed wakeup).

### Conventions

None — no CLAUDE.md files exist in the repo or user scope.

## Architectural follow-ups (interface-level)

Third pass: re-read include/myio.h (vtable + generic loops) and
myio_common.h against the findings above, asking which interface decisions
would remove whole *classes* of these problems instead of patching
instances. Verified the enabling preconditions in the code (completion
funnels, per-backend step() shapes) before recommending.

### A. Re-specify step(): a call that completes a task must not return 0
(root-fixes 12, unlocks 10)

The current contract lets uv/xev return 0 on the very turn that processes
the final completion, so the generic await/select carry permanent
compensating re-poll branches (include/myio.h:389-392, :460-463) and any
future direct vtable consumer is a landmine. Five of seven backends
already conform: pool/zephyr_pool via their completions/comp_seen
generation counter, zephyr because it checks progress_possible *before*
running the turn (myio_zephyr.c:894-899), uring because its step drains
CQEs, sync because its step is never reached. Only uv and xev forward the
"loop still alive" flag.

The fix is small because both violators funnel every completion through
one function (task_finish at src/myio_uv.c:107, finish at
src/myio_xev.zig:263): add an `ncompleted` counter to the io struct,
bump it in the funnel, and step() becomes "capture counter; run once;
return alive || counter advanced". Then:

- the two re-poll branches in the header's generic loops are deleted and
  the EDEADLK path becomes directly trustworthy;
- finding 10's drain loops can legally collapse onto the primitive:
  `while (!t->done && impl_step(io))` becomes correct in all three
  backends that currently re-inline divergent step bodies (uv:688,
  zephyr:1013, xev:1148) — the "not clean swaps" caveat in finding 10
  exists only because the current contract is too weak.

This is the one change that makes the *documented* interface the thing
backends actually implement.

### B. Shared intrusive write-FIFO helper (fixes 13, prevents 15-class
drift)

The ordering contract's trickiest mechanics — tail push, head pop +
successor promote, arbitrary remove with tail fixup — are hand-copied in
four C backends plus once in Zig, each over its own task type; finding 15
(xev's len==0 bypass) is exactly the drift this invites. Extract a
type-neutral intrusive queue: a `myio_wq_node { next }` embedded in each
task struct, `myio_wq { head, tail }` in each sock, container_of to
recover the task. Three operations:

- `wq_push(q, n) -> int became_head` — the "arm the send now" signal.
  Making push the only entrance turns "every write takes its turn"
  into the path of least resistance; a len==0 shortcut becomes a
  deliberate deviation instead of an easy accident.
- `wq_pop_head(q) -> new_head` — completion-side promote (the
  write_finish loop shape at myio_uring.c:251).
- `wq_remove(q, n) -> int found` — cancel/teardown unlink, with the
  tail fixup written exactly once.

Placement: NOT myio_common.h as it stands — that header is POSIX-only by
charter and drags in netdb/arpa, which is why the Zephyr backends
deliberately cannot include it. Either a new OS-include-free
`src/myio_wq.h` usable by all five C backends, or split myio_common.h
into an OS-neutral core plus a POSIX section. The Zig copy stays separate
(different type system) but should mirror the same three-op shape so the
disciplines stay recognizably identical across languages.

### C. Decide zero-length-write ordering in the header, not per backend
(settles 15)

Recommend routing len==0 through the FIFO (i.e. change xev to match the
other five) and adding one sentence to the ordering paragraph
(include/myio.h:308) — "zero-length writes take their turn like any
other" — rather than carving an exception into the contract. Uniformity
across seven backends is worth more than one backend's micro-shortcut.

### D. Optional: drop task_done from the vtable

task_result is already required to return status MYIO_PENDING before
completion (include/myio.h:225), so `task_done(t)` is derivable as
`task_result(t).status != MYIO_PENDING`. Removing it is one less function
per backend (x7) and one less field to keep in ABI sync in the Zig Ops
mirror; the public myio_task_done() wrapper stays, implemented on
task_result. Cost: the generic loops' hot poll returns a small struct
instead of an int — noise. Only worth folding in if the vtable is being
touched anyway (it is, if A lands).

### E. Deliberate non-changes

- **Findings 1 and 2 are pool implementation strategy, not interface.**
  The interface already permits the fix (leave non-head writes
  undelivered in the task queue until promoted). Resist the tempting
  interface "fix" of capping outstanding writes per socket like the
  one-read rule: the header's own rationale (include/myio.h:349-359,
  partial-write composition) is why the ordering guarantee exists;
  capping just exports the FIFO to every caller.
- **Findings 3 and 4 stay accepted costs.** If profiling ever disagrees,
  the escape hatch is optional vtable fast-path entries (NULL = fall
  back to the generic loop). Note the pattern; do not add it now.
- **A generic write-serialization layer above the vtable does not fit**
  this interface: sockets are opaque backend types, the header has no
  completion hook (by the re-entrancy design), and the wrappers are
  submit-time-only. Per-backend FIFOs sharing the list mechanics (B) is
  the right altitude.
- Finding 8 (myio_await_timeout should call myio_task_drop) is a
  header-local cleanup to fold into the same edit as A/C.

Suggested order: A (contract), then C + 15's xev change, then B
(mechanical adoption in four backends), with D and the finding-8 cleanup
riding along on the header edit. Findings 1/2/5/6/7/9/11 proceed
independently as the per-backend fixes already described above.

## Performance / code-quality pass (2026-07-05, post-fix-round)

Focused on allocations, struct footprints, stack usage, and algorithmic
complexity. Struct sizes measured (x86-64, sizeof probes; xev via
@compileLog), syscall counts via strace, scheduling via perf stat.

### Measured footprints

| struct       | size | dominated by |
|--------------|------|--------------|
| pool_task    | 176 B | flat op args (fine) |
| uring_task   | 288 B | sockaddr_storage 128 B (connect/accept only) |
| uv_task      | 656 B | union sized by uv_fs_t 440 B (timer needs 152, write 192, work 128) |
| xev Task     | 760 B (io_uring) / 592 B (epoll) | three embedded xev.Completion (184/128 B each) |
| uv_sock      | 304 B | uv_tcp_t (inherent) |

### Findings, ranked

**Implementation status (same day):** findings 1-3 are implemented and
verified (details inline below); 4-6 stay as documented non-changes.
- (1) uv: the request union became a flexible-array tail (`union uv_req
  u[]`), task_new allocates per kind - a sleep task went 656->368 B, a
  socket read/accept/close 656->216 B; the FAM keeps -Warray-bounds
  honest where a plain short calloc would not. uring: op state (sleep ts,
  connect/accept sockaddr, the sock_write group incl. its wq node) now
  shares a `union op` - 288->232 B. xev: measured but deliberately NOT
  slimmed - the pool-stage fields (c_async, notifier, pool_*) appear at
  34 sites including taskIdle, the memory-reclaim gate; a nullable side
  allocation there buys ~230 B/task at the cost of destabilizing the
  trickiest lifetime code in the tree. Deferred with this shape on file.
- (2) zephyr: run_loop_once's fds[]/owner[] arrays moved into z_io (a
  static singleton; single-driver rule makes that race-free) - the
  driving thread's per-turn stack cost is gone.
- (3) pool: workers are now detached, park on work_cv with a
  CLOCK_MONOTONIC pthread_cond_timedwait, and retire after POOL_IDLE_SEC
  (10 s) without work; destroy waits for a live-count to hit zero instead
  of joining, and the threads[] array is gone. Verified live: 4 threads
  during a 32-write burst, 2 after 11.5 s idle. The retire-vs-dispatch
  race is closed by re-checking qhead under the mutex after a timeout.

1. **Task structs pay the worst-case op's footprint on every allocation.**
   Every uv sleep task carries the 440-byte uv_fs_t; every uring task the
   128-byte sockaddr_storage only connect/accept use. Fix shapes: uv's
   union is the LAST member, so task_new(kind) can malloc the true size
   per kind (timer 656→368); uring can fold the mutually exclusive
   op-state fields (addr/addrlen, ts, path, the w* write group) into a
   union (~150 B saved). xev is harder (one comptime struct, fixed
   Completions - c_async is only needed by thread-pool stage ops); accept
   or split per kind later. Modest, low-risk memory/cache win.

2. **myio_zephyr.c run_loop_once carves ~1 KB off the driving thread's
   stack every turn** (`fds[CONFIG_MYIO_MAX_TASKS+1]` + matching `owner[]`
   pointer array = ~1040 B at the default 64; scales linearly with the
   Kconfig). On RTOS-sized stacks this is the largest stack consumer in
   the backend, and it grows silently when MAX_TASKS is raised. Fix:
   move both arrays into z_io - the header's single-driving-thread rule
   makes an instance buffer race-free, and z_io is a static singleton, so
   it trades stack risk for predictable static RAM.

3. **The desktop pool never reaps idle workers** (documented design:
   spawn on demand, join only at destroy). A burst of N concurrently
   blocking ops leaves N parked threads (default 8 MB stack VMA each)
   for the instance's lifetime. Fix shape: pthread_cond_timedwait in
   worker_main's idle park; exit after an idle timeout when idle >
   some floor. Worth doing for long-lived processes; not urgent for the
   examples.

4. **One-to-two heap allocations (calloc) per submitted task, no reuse**
   (uv/uring/pool/sync; xev via c_allocator). A hot request loop is
   malloc-bound before it is IO-bound only in theory - not visible in
   perf on the test workloads - so defer, consistent with the earlier
   deferral of tagged-pointer error tasks. Fix shape if ever needed: a
   small capped per-instance LIFO freelist in task_new/task free paths.
   (Related existing cost, kept: failed submissions allocate a task just
   to carry EBUSY/EAGAIN/ENAMETOOLONG.)

5. **uv one-shot reads flip read_start/read_stop per sock_read** (epoll
   watcher churn inside libuv). Inherent to adapting uv's push API to
   the pull model with at-most-one-outstanding-read; accepted.

6. **uring pays one io_uring_enter per submit** - measured: 98 enter
   calls for the entire concurrency_test run, ~102 us/call. Required by
   the eager-submission contract (the op must be executing when submit
   returns); SQPOLL is the escape hatch if a real workload ever cares.

### Verified clean (no findings)

- myio_wq: push/pop O(1); remove O(n) only on cancel/close/teardown
  paths. Pool queue_remove likewise O(n) on rare paths only.
- uring's task registry is doubly linked (O(1) unlink per completion);
  socket registry likewise.
- No oversized on-stack objects in the POSIX backends (largest:
  sockaddr_storage 128 B, instance errbufs are in the io struct).
- myio_result by value (32 B) across the vtable: negligible.
- Post-rewrite done_cv fan-out is small (user thread + close-drain
  workers only); the write-turn share of the old broadcast storm is gone.
- Generic await/select vtable-call overhead: already documented as
  accepted costs (findings 3/4 above), unchanged by this pass.
- perf stat on concurrency_test pool: dominated by user-space buffer
  work (memset/memcmp of the 15 MB test buffers), not scheduling or
  locking - the mutex-per-task_done cost of the generic loops is real
  but invisible at these op rates.

## Code-quality / extraction pass (2026-07-06)

Goal: interface polish, backend cleanups, and extracting the patterns a
future backend would otherwise re-invent. All implemented and verified
(make test clean, 3x stress on the four async desktop backends,
make test-zephyr green).

### Extractions (easing backend #8)

- **myio_common.h restructured into an OS-neutral core + POSIX section.**
  The core (result constructors r_ok/r_err/r_canceled/r_from and a new
  `myio_sockaddr_port`) now serves every C backend including the Zephyr
  pair (`#ifdef __ZEPHYR__` include fencing). Consequences: zephyr_pool's
  local copies of the result constructors are gone (the T6 leftovers),
  and the sockaddr->port family switch that existed in four places
  (myio_local_port, uv, zephyr, zephyr_pool) exists once; uv now includes
  the common core too.
- **myio_wq generalized beyond writes.** libuv's accepted-but-unclaimed
  connection queue (pending_head/pending_tail/next_pending - a third
  hand-rolled FIFO shape) now rides on myio_wq; header doc updated to
  frame wq as "the intrusive FIFO, defining job the write queue".
- **pool gained complete_unrun()** (the helper zephyr_pool already had):
  the queued-or-parked "complete CANCELED on the spot" logic previously
  hand-inlined at four sites (impl_cancel, impl_task_free, sock_close's
  parked sweep, destroy's queue drop + parked sweep) is one function; the
  two pool backends read structurally identical again.
- **sync's three task constructors collapsed** onto one task_res(result)
  wrapper reusing the common r_* constructors.
- **README "Adding a backend"** now lists both internal helper headers and
  what each provides.

### Interface polish (myio.h)

- Intro no longer advertises a "stackful/stackless coroutines" backend
  that does not exist; it names the real mechanism family.
- task_done's vtable doc states the agreement requirement with
  task_result (done <=> stored status != MYIO_PENDING).
- myio_select's -1 is documented as the select analogue of await's
  EDEADLK (sharpened for the strengthened step() contract).
- myio_await_all: misleading `all_ok` local (0 meant ok, -1 not) renamed
  and the loop tightened.

### Considered and rejected

- **A lock-shim to merge pool and zephyr_pool** (macros over pthread vs
  k_mutex/k_condvar): would merge ~2x700 lines into one file of
  preprocessor indirection; the pair's readability comes precisely from
  each being idiomatic for its platform. complete_unrun/write_advance
  parity is the intended amount of sharing.
- **A macro generating the task_of/handle_of/io_of/sock_of casts** per
  backend: four one-line casts do not justify macro-generated functions.
- **Sharing the connect/listen walks with Zephyr** via symbol-prefix
  macros (zsock_socket vs socket): the walks differ in error detail and
  the macro layer would cost more than the ~40 duplicated lines.

## Carried over / out of scope

- The one-time non-reproducible native_sim teardown segfault in the
  Zephyr event-loop suite (see CONTINUATION.md) — not part of this
  diff's scope, still unresolved.
