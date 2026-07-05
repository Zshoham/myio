# Review pass over the API-hardening round (commits ccab740...HEAD)

Second-pass review (high effort, 8 finder angles + verification) over the
three commits: write-serialization FIFOs + io_uring close-vs-write fix,
EAI_* error convention, step()/task_result() vtable refactor, myio_caps(),
myio_task_drop(), myio_common.h, tests.

**Status: IN PROGRESS — candidates below are UNVERIFIED finder output**
unless marked otherwise. Angles completed: reuse (2 candidates),
efficiency (5), simplification (6), altitude (3), conventions (0 — no
CLAUDE.md exists). Still running (the three correctness angles):
line-by-line scan, removed-behavior audit, cross-file tracer.
Verification (CONFIRMED/PLAUSIBLE/REFUTED) not yet run — do NOT act on
these without verifying against the code first.

## Candidates so far

### Efficiency

1. **src/myio_pool.c:413 — queued socket write occupies a worker while
   waiting its turn.** A write behind the FIFO head is dispatched to a
   worker (dequeued, nrunning++) that just blocks on done_cv until its
   turn. Submitting more concurrent writes to one socket than the pool has
   threads ties up every worker; unrelated ops on other sockets starve
   until the congested socket drains. Fix shape: leave non-head writes in
   the pool's task queue (undelivered) until the FIFO head advances, only
   then hand them to a worker. Likely the most substantive finding so far;
   also check src/myio_zephyr_pool.c which mirrors the design with a FIXED
   worker count (starvation there is permanent, not growth-bounded).

2. **src/myio_pool.c:495 — one shared done_cv broadcast wakes every
   waiter.** Completions broadcast a single condvar shared by step(),
   write-turn waiters, and close-drainers: one completion wakes O(waiters)
   threads to re-check false predicates. Fix shape: per-socket condvar for
   write turns, or targeted signal.

3. **include/myio.h:446 — generic select scans with one mutex round-trip
   per task_done() on lock-based backends.** Old pool select scanned all
   tasks under one lock; the generic loop does n lock/unlock pairs per
   scan per wakeup. Cost is uncontended-mutex churn; shape of fix if it
   ever matters: an optional batched vtable fast path. (Accepted cost of
   the refactor? — verify before caring.)

4. **include/myio.h:386 — generic await costs ~3 lock round-trips
   (task_done + step + task_result) where old await cost one.** Same
   trade-off as (3).

5. **examples/concurrency_test.c:157 — 8 MB + 7 MB test buffers per
   backend per run, with byte-by-byte verification.** Multiplied across
   the backend matrix in `make test`. Fix: few-hundred-KB buffers (still
   exceed SO_SNDBUF) and memcmp.

### Reuse

6. **src/myio_uring.c:511 — impl_tcp_connect re-inlines the
   EAI_SYSTEM→errno ternary instead of calling gai_errno() from
   myio_common.h**, which the file already includes and uses elsewhere.
   Two copies of the mapping can drift.

7. **examples/concurrency_test.c:27 — failures/expect()/count_fds()/
   make_io() duplicated verbatim from examples/cancel_test.c.** A shared
   examples/test_common.h would keep the two binaries agreeing on backend
   lists and fd counting.

### Simplification

8. **include/myio.h:561 — myio_await_timeout hand-rolls cancel+detach for
   the losing timer** instead of calling myio_task_drop(), defined 14
   lines below in the same header. Move task_drop above and call it.

9. **src/myio_zephyr_pool.c:148 — the new `nqueued` counter is redundant
   in this backend.** Unlike the desktop pool (where the count feeds the
   idle>=nqueued spawn check), every read here only asks "is it zero",
   which `qhead == NULL` already answers. Three update sites of extra
   invariant; a missed decrement makes impl_step spin/hang.

10. **step() body re-inlined in task_free/destroy drain loops** in three
    backends instead of calling the step function itself:
    src/myio_uv.c:688, src/myio_zephyr.c:1013 (+ impl_destroy ~1037),
    src/myio_xev.zig:1148. If step() ever grows bookkeeping, the private
    copies silently diverge. Replace with `while (!done && impl_step(io))`.

11. **src/myio_zephyr.c:5 — stale top-of-file comment** still describes
    the pre-refactor design ("await and select are the same while(!done)
    run_loop_once() shape"); the backend now only implements
    step()/task_result(). The sibling files' comments were updated; this
    one was missed.

### Correctness (cross-file tracer)

15. **src/myio_xev.zig:887 — zero-length sock_write bypasses the write
    FIFO and completes immediately**, the only backend that does this. A
    zero-length write submitted behind an in-flight write completes first,
    observably violating the header's "performed in submission order, as
    if each write completed before the next began" for callers watching
    completion order — even though no wire bytes reorder. Fix: route
    len==0 through the FIFO like the other five backends (or explicitly
    carve zero-length writes out of the ordering contract in the header).

The tracer also POSITIVELY VERIFIED (no finding): the Zig Ops extern
struct matches include/myio.h field-for-field; myio_common.h helpers are
byte-identical to the code they replaced (checked against ccab740);
demo.c/cancel_test.c per-backend assumptions still hold; the pool
completions/comp_seen generation counters check out.

### Altitude

12. **src/myio_uv.c:600 + src/myio_xev.zig:1057 — step() violates its own
    documented contract on the last completion.** Both forward the
    loop's "still alive" return as the progress signal, which is 0 on the
    very call that processes the final completion. The generic header
    loops compensate with a permanent "step returned 0, re-poll tasks once
    more" branch that pool/uring/zephyr_pool never need. Any future direct
    consumer of the vtable step() that follows the documented contract
    literally will spuriously report deadlock on uv/xev only. Deep fix:
    make uv/xev step() track whether the run(.once) call completed any
    task (a completion counter, as the pools do) and return 1 for it; then
    the generic re-poll branch can go.

13. **Write-FIFO pointer bookkeeping hand-copied ~4 times**
    (myio_pool.c:243, myio_zephyr_pool.c:267, myio_zephyr.c:663,
    myio_uring.c:255 + 766) with different task-struct names. A tail-fixup
    bug fixed in one copy leaves the others. Candidate for a macro-based
    or field-offset helper in myio_common.h (C backends). Overlaps
    finding 10's theme: shared discipline, per-file copies.

14. **include/myio.h:163 — MYIO_CAP_NONBLOCKING_SUBMIT bundles two axes.**
    uring is fully non-blocking except DNS in tcp_connect, yet reports
    the same "false" as the sync backend where everything blocks; callers
    that never resolve hostnames can't tell the difference, and
    concurrency_test.c already falls back to strcmp on backend names
    instead of composing caps. Consider a narrower bit (e.g.
    MYIO_CAP_ASYNC_DNS) or documenting the bit as connect-inclusive.

### Conventions

None — no CLAUDE.md files exist in the repo or user scope.

## Pending

- Finder angles still running: line-by-line diff scan, removed-behavior
  audit, cross-file tracer (includes the Zig ABI-mirror field-order
  check), simplification, altitude.
- Phase 2 verification of all candidates.
- Carried over from earlier sessions (not part of this diff's scope but
  unresolved): the one-time non-reproducible native_sim teardown segfault
  in the Zephyr event-loop suite (see CONTINUATION.md).
