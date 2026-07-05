# Review pass over the API-hardening round (commits ccab740...HEAD)

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

## Carried over / out of scope

- The one-time non-reproducible native_sim teardown segfault in the
  Zephyr event-loop suite (see CONTINUATION.md) — not part of this
  diff's scope, still unresolved.
