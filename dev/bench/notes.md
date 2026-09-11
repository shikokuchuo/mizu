# Recorded benchmark notes

Dated performance records for rei. The live reports that print against
these baselines are `tests/testthat/test-benchmark.R` (eyeball them in CI
logs); `dev/bench/rei-bench.R` runs the rei-only rows and
`dev/bench/rei-mirai-bench.R` the matched-scenario mirai comparison. Nothing here is asserted against — runner timing is too
variable for thresholds. Append new dated outcomes at the bottom.

All records are M4 Pro, R 4.6.1 unless marked; A/B ratios are the valid
comparisons, absolutes drift with host load. proc.time ticks at ~1 ms on
this host, so timed intervals are kept >> 1 ms by looping.

## Incumbent baselines (2026-07)

- nanonext `ipc://` pair: 31.7 us per round trip (~31.5k RT/s)
- mirai local dispatch: 63-124 us per task
- target regime: >100k small messages/s sustained
- `rei_map` overhead regime: serial lapply ~0.2 us/element; per-element
  `rei_submit`/`rei_collect` ~4 us; `mirai_map` ~63-124 us/element

## Durable calibration facts

- Small-op floor is the R-interpreter boundary: >85% of a pool round
  trip, the closure call alone ~40% of it. The batch verbs and
  `rei_map` are the answers for throughput-bound callers.
- Park/wake pair: ~8 us macOS, ~16 us virtualized Linux (isolated by
  pacing an echo peer, stock vs spin=TRUE).
- Zero-copy floors: SHM_VEC loses to the copy tiers below ~8-16 KiB
  (ARENA ~1.9-2.2 us at 256 B-4 KiB vs SHM_VEC ~2.3-2.9 us).
  `REI_ZC_FLOOR` (pool) sits at the 16-64 KiB band;
  `REI_ZC_FLOOR_RAW` = 256 KiB (channel arena; lifts under the
  Linux-only churn signal). The pool's raw spill is a region too:
  region-raw never beat the view at 64 KiB+ (26.4 vs 21.5 us).
- Large-payload suite cases are sized for a 1 GB /dev/shm (docker
  default): `gc()` between reps, ~32 MiB per rep; suite peak ~440 MiB
  (was ~1.8 GB). Channel/map numbers from 2026-08-17 on are not
  comparable with the 64 MiB records.

## 2026-08-11/12: zero-copy phases

- Before (memcpy-bound): channel round trip 0.10 ms at 1 MiB (ARENA),
  0.72 at 8 MiB, 19.0 at 64 MiB; pool result 0.18 / 1.22 / 16.0 ms;
  `sora_map` 64 MiB template 0.69 s (0.08 us/element).
- After (SHM_VEC/REF): receive is an ALTREP wrap (~0.45 us); echo
  re-send rides REF. Channel 64 MiB 19.0 -> 3.00 ms (6.3x); pool 64 MiB
  16.0 -> 10.25 ms (residual: worker allocVector + fill + one send-side
  memcpy).
- Guards: ALTREP `1:2^27` round trip ~3 us (133 B stream); 100 MiB
  matrix round trip 36 ms, then 3-of-100 cols 0.74 ms/read; held 8 x 16
  MiB results read 8 spills / 0 reused / 8 fresh (held views pin regions
  by design; pre-views it was 8/7/1); 32 MiB named vector 725 ms vs
  15 ms bare — the attrs blob is ~97% of cost.
- Profile split: for flat atomics the serialize pass IS the send memcpy
  and unserialize is allocVector + memcpy; object-graph parse is noise.
  `.collect = "view"` skips the gather memcpy (0.688 s eager vs 0.666 s
  view + reduce on the 64 MiB template; matters for reduce-shaped maps).

## 2026-08-17: immediate kinds (NIL, STR1), RAWSPILL, gates

- NIL/STR1 immediates + C-side submit assembly: pool round trip 2.4 ->
  1.5 us; channel NULL 3.1 -> 1.3 us; string 2.6 -> 1.4 us; one-way
  streaming 30 -> 34-40M msg/s; pool pipelined 0.57 -> 0.69M tasks/s.
- RAWSPILL (bare-bytes raw vectors past inline): channel round trip
  800 B 3.7 -> 1.3 us, 8 KB 5.1 -> 3.0 us.
- Keeperless collect wake gate: pool pipelined 0.67M -> 0.84M tasks/s
  (1 worker), 0.41M -> 0.83M (4 workers).
- Spin clock stride 8 -> 64 (clock reads were ~1/3 of host CPU in a
  channel round trip); `sora_now()` reads `mach_absolute_time` directly.
- Task handles pack (idx, seq) into the extptr address: below noise
  (0.84M -> 0.86M tasks/s), kept for strictly less hot-path code.
  Freelist assessed and rejected: task structs ~1% of controller CPU;
  the ~25% alloc/GC slice is R-heap churn, inherent to the task model.
- Rejected: skipping the drain-empty head publish for keeperless drains
  (it is also the producer's space-reclaim signal; gating it shrunk
  effective ring capacity by up to K-1 phantom slots).

## 2026-08-17: compact codec ahead of R_Serialize

- The finding that mattered: R_Serialize allocates a VECSXP(1099) ref
  table on EVERY call (R_Unserialize a VECSXP(128)) — several hundred ns
  plus ~10 KB of garbage per payload per side. The codec frames
  NULL/symbols/atomics (attrs included)/strings/list trees/calls without
  it; rejects ALTREP (so codec streams pin no keeper); closures,
  environments, S4, attributed pairlist nodes, over-deep graphs fall
  back. gp/LEVELS is not carried.
- Measured (base -> codec): channel round trip (0L) 1.00 -> 0.80 us;
  list(1, "a") 2.60 -> 1.20 us; pool NULL task 1.50 -> 0.75 us;
  sum(runif(10)) 1.75 -> 1.25 us; pool pipelined 1.0M -> 2.0M tasks/s.

## 2026-08-17: sora_collect_all

- One R boundary crossing per burst instead of per task: pool pipelined
  1.0M -> 2.0M tasks/s vs the per-collect loop (~200 ns of the ~500
  ns/task floor was the collect closure plus `ts[[i]]`).

## 2026-08-18: zero-allocation task decode

- INLINE codec task frames stream-decode in place on the worker
  (`sora_codec_read_task`); a constant task allocates nothing there.
  Wire bytes unchanged. Pool round trip `1L` 1040-1120 -> 980-1020 ns;
  4-arg and pipelined a wash (the eval dominates).

## 2026-08-18: gap-adaptive wait budgets (sora_spin_learn)

- Was: static-ceilinged budgets — the channel recv reset to 16 us on any
  success; the pool collect grew only by near-miss doubling up to 32 us
  and could never climb past a gap above 2x the tried budget. A
  turnaround past the ceiling paid the park/wake pair on every wait.
- Now: every wait exit learns the measured turnaround — within
  `SORA_SPIN_CAP_NS` (64 us, uniform) the next budget grows to 1.5x the
  gap + 8 us headroom (never below the site constant); past the cap it
  halves toward the floor (a spin long enough to catch there would burn
  more than the pair it saves). The never-waited hot path is untouched
  (no clock read, no budget update).
- Measured (interleaved A/B, medians): channel round trip at 20 us peer
  gap 29.9 -> 22.9 us, 40 us 51.3 -> 43.4; pool 50 us task 60.9 ->
  52.7. Past the cap it parks with less CPU than stock (1.3% vs 4.3% at
  a 500 us gap — the old channel rule re-spun 16 us per wait). A 256 us
  cap was measured and rejected: the 64-256 us band bought -4% latency
  for a full core.
- Linux container (r-base 4.6.1, aarch64 linuxkit VM, 14 vcpus): the
  pair costs ~16 us there, so the win band extends to ~128 us (50 us
  task 90.3 -> 54.7, 100 us task 132.4 -> 105.0 at cap 128k). Kept one
  uniform 64 us for simplicity; bare-metal Linux likely has a cheaper
  pair and a lower crossing.
- Suites green on macOS and the Linux container, stock and adaptive
  alike. Pre-existing aside (verified identical on stock): serial
  submit-collect loops run ~3x slower under devtools::test on this
  package's own suite than in a plain R session (submit ~240 vs ~40 us,
  worker claim ~190 vs ~37 us, worker exec identical) — the test-run
  context inflates the wake/claim path. One test-parker flake seen once
  in a container full-suite run (stray SIGCHLD mid-park); passes in
  isolation on both builds.

## 2026-08-19: sora_submit_batch (pool TSO analog)

- One .Call, one payload assembly loop, per-element tail publish, and a
  wake cadence (every min(64, inj_cap) publishes) plus a protocol wake
  pass per worker after the last publish. The single submit's
  publish->wake pairing is load-bearing, not ceremony: a worker that
  loses the park race mid-burst (pre-park re-check read a stale tail)
  sleeps for the full 1 h run bound, and only a wake paired with the
  final publish contains that race. An earlier version waking only at
  element 0 deadlocked into collect timeouts that leaked handles into
  sora_error_slots_exhausted a burst later.
- Measured (scenario 2, evaluate 1L, 1 worker, submit_batch +
  collect_all per burst): 5.0M tasks/s vs 0.67M looped single submits
  (7.5x) — the R boundary was the remaining per-task cost. mirai
  dispatcher 9.4k, direct 20.1k tasks/s on the same row.
- sora_map deliberately does not use it: map submits O(workers) runner
  tasks per call, so batching its submit loop is noise; the raw pool
  API loop (fire n, collect n) is where the crossing count is
  unbounded.

## 2026-08-19: codec CLOSXP framing (closures by reference environment)

- The compact codec now writes closures: formals / body ride the existing
  pairlist / call framings, the environment crosses by reference — global
  / base / empty by kind byte, a package namespace by name
  (R_FindNamespace on read, R_Unserialize's discipline). Local
  environments, bytecode bodies, and non-srcref-attributed language nodes
  decline to R_Serialize as before. Attributes trail formals/body in the
  stream so the reader constructs with R_mkClosure before they apply (the
  closure slot setters left the public C API in R 4.5.0).
- keep.source parsing hangs srcrefs on the closure and its body language
  nodes, and on the chain heads of a top-level task expression (`{`
  blocks); the srcfile they name cannot cross processes, so the write
  crosses a stripped copy — map.c's sora_strip_srcref for closures
  (probed by the closure-level attribute), sora_strip_lang for language
  trees (probed on the chain head; nested blocks strip on CAR recursion).
  identical()'s ignore.srcref discipline. One deep duplicate per
  srcref-bearing subtree write; non-srcref attributes still decline.
- Measured (arm64 macOS, R 4.6.1, dev/bench/closure-codec.R): the
  list(f = <closure>, x = 1L) payload round-trips in ~900 ns via the
  codec vs 2500-2900 ns via R_Serialize (~3x); keep.source closure
  payload ~2300 ns (strip included) vs ~12500 ns, keep.source `{` block
  ~2000 ns vs 12300-18200 ns (~6x — the fallback drags the srcfile
  environment along). Serial submit-collect with the closure as a task
  argument: 3.20 -> 1.80-2.20 us plain (keep.source 2.60-3.00 us), plain
  task 1.40-1.60 us.

## 2026-08-21: retain-table seam carve (Phase 1 step 1, commit 82d8c67)

- The keeper VECSXPs became the explicit per-slot retain table (spill.c):
  regions owned by the handle (free list / lent ledger / mapping cache hold
  mori_shm pointers, closed + unlinked at eviction and teardown), the
  serialize-tier pin rides a parallel VECSXP in the prot chain. The
  ledger-overflow drop keeps the name (a REF in flight resolves by name)
  where the wrap GC finalizer unlinked it — the deferred unlink without
  the GC. Behavior-preserving; the suite is green (1993 pass).
- Measured (arm64 macOS, R 4.6.1, test-benchmark.R, report-only): channel
  round trip 1.50 us; one-way 25.0M msg/s; pool round trip 1.0 us/task;
  pool pipelined 1.0M tasks/s, collect_all 2.0M; channel 1/8/32 MiB round
  trip 0.09/0.50/2.00 ms (~21-32 GiB/s); pool 1/8/64 MiB result
  0.25/1.50/13.00 ms/task; sora_map 32 MiB template 0.356s. Consistent
  with the 2026-08-20 records — no hot-path regression from the carve.

## 2026-08-21: check/park binding hooks (Phase 1 step 1, commit 2 of 4)

- The 12 in-loop R_CheckUserInterrupt() sites in channel.c/pool.c became
  indirect sora_check_interrupt() calls through the handle's new
  sora_binding (check = sora_r_check, registered at create/attach/join),
  and every bounded sora_park in those loops gained the around-park
  sora_park_bracket() (park = NULL for R — one load + predicted branch
  per sleep). map.c's site is untouched. Behavior-preserving: the R hook
  longjmps at the same points; the abandon-return contract is for the
  core verbs of step 2. Suite green (1993 pass, 2 macOS skips).
- Measured (arm64 macOS, R 4.6.1, test-benchmark.R, report-only): channel
  round trip 1.50 us; one-way 28.6M msg/s; pool round trip 1.0 us/task;
  pool pipelined 1.0M tasks/s, collect_all 2.0M; channel 1/8/32 MiB round
  trip 0.06/0.25/1.50 ms; pool 1/8/64 MiB result 0.22/1.25/11.00 ms/task;
  sora_map 32 MiB template 0.316s (view collect + reduce 0.312s).
  Consistent with the retain-table record earlier today — no hot-path
  cost from the indirection.

## 2026-08-21: channel stage_r.c split (Phase 1 step 1, commit 3 of 4)

- chan_send1's tier dispatch and chan_materialize moved to new
  src/stage_r.c as the R binding's stage_fn/read_fn
  (sora_r_stage_channel / sora_r_read_channel), registered in the
  handle's sora_binding at create/attach alongside check — the binding
  struct gained the stage/read fields (typedefs sora_stage_fn /
  sora_read_fn), and the channel handle structs moved from channel.c to
  sora.h so the stager reaches the free list and the open/zc caches. The
  transport keeps the ring, arena, wakes, and the retain-table commit,
  and supplies the staging services: sora_stage_arena_alloc (returns the
  chunk pointer — the arena base stays transport-private) and the
  exported sora_chan_reap (the pre-spill reap). On receive the transport
  resolves arena-referencing frames (ARENA, channel RAWSPILL) to their
  byte range before invoking read_fn — the callback never learns arena
  mechanics. The retain entry is stager-initialized (the
  sora_payload_stage discipline); a mid-stage raise still abandons
  cleanly. Behavior-preserving; the suite is green (1993 pass, 2 macOS
  skips).
- Measured (arm64 macOS, R 4.6.1, test-benchmark.R, report-only):
  channel round trip 1.50 us; one-way 25.0M msg/s; pool round trip 1.0
  us/task; pool pipelined 1.0M tasks/s, collect_all 2.0M; channel
  1/8/32 MiB round trip 0.06/0.50/1.50 ms; pool 1/8/64 MiB result
  0.22/1.25/12.00 ms/task; sora_map 32 MiB template 0.336s (view
  collect + reduce 0.331s). Consistent with the check/park record — no
  hot-path cost from the stage/read indirection.

## 2026-08-21: pool exec_fn split (Phase 1 step 1, commit 4 of 4)

- The pool's SEXP half moved behind the binding seam: the task frame
  decode, pool_eval_expr, and the sora_condition_flatten ERR framing
  moved to stage_r.c as the R binding's exec_fn (sora_r_exec_pool),
  registered at sora_pool_set_eval alongside the binding ctx (the
  handle's prot chain, registered at create/join/attach); the pool
  handle struct moved from pool.c to sora.h (as the channel's did at
  commit 3). The result publish split into the core's mechanics and the
  binding's staging through the sink (sora_result_sink): the core's
  sora_result_publish stages OK results via the handle's binding.stage
  (sora_r_stage_pool, the payload.c tier dispatch — submit staging now
  rides the same hook), sora_result_publish_err publishes the binding's
  INLINE-framed flattened envelope (tiered fallback below the codec's
  inline guarantee), sora_result_publish_died is the status-only
  terminal for a vanished task payload, and the shared tail keeps the
  status CAS, keeper swap, waiter wake, cancel race, and the terminal
  trace event. The trace hook is a per-handle core registration
  (sora_pool_set_trace sets fn ptr + ctx; the R closure rides prot[2]
  through stage_r.c's thunk) with the emit sites in core paths. One
  trace-semantics change: "start" now fires at claim (before the
  binding's decode), so a task whose payload vanished with a dead
  enqueuer traces start+drop where it traced drop alone — untested
  death path, no live-pool sequence changes (the suite's lifecycle
  expectations pin those). st_tasks accounting moved into the publish
  tail (one increment per publish, as before). Behavior-preserving; the
  suite is green (1993 pass, 2 macOS skips).
- Measured (arm64 macOS, R 4.6.1, test-benchmark.R, report-only):
  channel round trip 1.50 us; one-way 18.2M msg/s; pool round trip 1.0
  us/task; pool pipelined 1.0M tasks/s, collect_all 2.0M; channel
  1/8/32 MiB round trip 0.06/0.50/1.50 ms; pool 1/8/64 MiB result
  0.22/1.50/11.00 ms/task; sora_map 32 MiB template 0.338s (view
  collect + reduce 0.334s). Consistent with the channel-split record —
  no hot-path cost from the exec/sink indirection.

## 2026-08-23: the extraction no-regression run

Measured on the renamed package (the rename commit with the rchk
protect fix folded in, installed) against the 2026-08-21 records —
the pre-extraction baseline. test-benchmark.R: channel round trip 1.50 us;
one-way 20.0M msg/s; pool round trip 1.0 us/task; pool pipelined 1.0M,
collect_all 2.0M; channel 1/8/32 MiB round trip 0.06/0.50/1.50 ms; pool
1/8/64 MiB result 0.22/1.50/11.00 ms/task; rei_map 32 MiB template
0.338s (view collect + reduce 0.339s); rei_map trivial f 0.09
us/element; held 8 x 16 MiB reads 8 spills / 0 reused / 8 fresh.
Small-payload cases (the seam's fixed-overhead probe, n=20k): channel
NIL round trip 0.95 us, STR1 1.00 us (2026-08-17 baselines 1.3/1.4 us);
pool submit/collect 0.90 us/task (baseline 1.0). No hot-path regression
from the extraction — the callback seam costs nothing measurable.

- The attributed guard's bare sub-case reads 121-124 ms on rei AND on
  the installed pre-extraction sora (same script A/B) — a pre-existing
  single-iteration artifact of that guard under the post-2026-08-17
  sizing regime, not an extraction regression. The attributed total is
  636 ms (725 ms in the 2026-08-12 record).
- ALTREP 1:2^27 round trip reads 4.00 us on two runs (~3 us on
  2026-08-12): 1000 iterations against the ~1 ms proc.time tick — the
  difference is one tick, inside the quantization band.

## 2026-08-23: throughput-regression fix 1 (cold error recorders)

The uniform throughput regression vs sora traced to librei's
record-and-return-status idiom: `rei_err_record` / `rei_err_record_tls`
are returning, variadic functions called on every hot verb's failure
branches with no cold annotation, so the compiler treated those branches
as live (register pressure, worse layout, blown inline budgets — helpers
sora's build inlined to nothing were real calls in rei.so). Upstream
librei 1b66319 marks them (and `rei_err_describe`) `REI_COLD`.
dev/bench/rei-mirai.R after re-vendor + install (M4 Pro, R 4.6.1):
sequential rt channel 0.9 us, pool 1.0 us/task; pipelined channel
1.18M rt/s, channel batch 20.0M rt/s, pool 833k tasks/s; streaming
45.5M msg/s; payload / fan-out / map rows at parity. Matches or beats
the sora reference column on every row.

## 2026-08-23: throughput-regression fixes 2-5 + the batch-row repair

Follow-up to the fix-1 entry above (same day, same machine). Landed:
the supply-callback submit_batch (upstream 81f7cca; the binding stages
through one reusable wire pair — no per-task VECSXP allocation), the
per-handle read-ctx template (upstream bb0b8a6), the 8-byte packed
rei_task with the handle carried in the extptr address (upstream
524165a; no malloc/free per task), and the Linux THP MADV_COLLAPSE at
spill free-list insert (upstream 78a3a35; compiles out on macOS).
dev/bench/rei-mirai.R: sequential rt channel 0.9 us, pool 0.5 us/task;
pipelined channel 1.19M rt/s, channel batch 20.0M rt/s, pool 833k
tasks/s, pool batch 2.86M tasks/s; streaming 41.7M msg/s; payload /
fan-out / map rows unchanged. Interleaved same-script A/B on the
sustained pool burst (50 x 10k): rei 2.23-2.29M tasks/s vs sora
2.14-2.20M. test-benchmark.R: channel round trip 1.50 us; one-way
25.0M msg/s; pool round trip 1.0 us/task; pool pipelined 1.0M,
collect_all 2.0M; channel 1/8/32 MiB round trip 0.09/0.25/1.50 ms;
pool 1/8/64 MiB result 0.19/1.50/11.00 ms/task; rei_map 32 MiB
template 0.344s (view collect + reduce 0.339s); rei_map trivial f
0.10 us/element; held 8 x 16 MiB reads 8 spills / 0 reused / 8 fresh —
all at the 2026-08-23 records. Fix 6 (collect_any/collect_all double
marshaling) not done: collect_all already beats sora post-fix-1, and
nothing since put it back in profile range.

Bench hygiene: the pool-batch row in dev/bench/rei-mirai.R (and the
mirrored sora-mirai.R) now runs k=10 bursts per rep — one 10k burst
lands inside mclock's 1 ms tick and quantized to 10M/5M/3.3M tasks/s,
which is how the regression stayed hidden from that row.

## 2026-08-23: the Linux pool-rt gap — getpid on the fork guard

The residual Linux gap vs sora (plan-linux-parity.md: pool sequential rt
1.5 vs 1.0 us, pipelined pool ~10-20% behind, pre-existing) traced to the
fork guards, not the wait path. `strace -c -f` on a 20k sequential-rt loop
(container, rocker/r-ver:4.6.1 on linuxkit): identical futex (41186) and
sched_yield (~16k) counts both packages, but rei made 116105 getpid calls
vs sora's 54600 — ~6/round trip against sora's ~3. glibc has not cached
getpid since 2.25, so each is a real syscall. rei pays the check twice per
verb (the .Call veneer's handle unwrap, then the core's pool_get) plus the
task finalizer's guard; sora's monolith checks once per verb.

Fix (upstream librei fe46aa8, vendored): rei_self_pid() caches the pid in
a static atomic, zeroed by a pthread_atfork child handler so a forked
child re-reads; pthread_once registers the handler on the first miss.
Fork-through-libc is the only in-scope fork (R's own), as for every
atfork guard. All fork guards (veneer, core verbs, cancel/release/state,
the finalizer) keep their checks — they are now relaxed atomic loads.

Measured after (dev/bench/rei-sora-linux.R, interleaved A/B, 5 rounds,
best-of): pool rt rei 1.0 = sora 1.0 us; pipelined pool rei 1.0M vs sora
909k tasks/s; channel batch rei 14.3M = sora 14.3M rt/s best-case (rei's
per-round median one mclock tick below sora's — the k-burst read is that
the plan's 16.7-vs-20.0M row was tick quantization, not a real deficit).
strace re-check: getpid absent from the profile (was 116105 calls).
Suites: Linux container rei 2009 / sora 2004 pass, 0 fail, 0 skip; macOS
1998 pass, 0 fail, 2 expected zc skips.

## 2026-08-24: batch-recv heap corruption fixed — sink-callback verbs

The streaming scenario's child death (~750 rounds x 20k messages, xzone
malloc SIGTRAP in the receive path) was binding-side: recv_batch and
collect_all materialized n fresh SEXPs into an R_alloc'd void ** across
the remaining reads' allocations — unprotected and invisible to rchk —
so a mid-batch GC freed them and the later SET_VECTOR_ELT wrote dangling
pointers. Fix landed upstream (librei ff8987c, vendored): sink-callback
forms rei_channel_recv_batch_fn / rei_pool_collect_all_fn deliver each
product as it is read; the rei veneer anchors it in a pre-PROTECTed
VECSXP (rei_vec_sink). collect_any got plain PROTECTs.

Acceptance: /tmp/repro-stream5.R completes all 3000 rounds (60M
messages, 4x the old failure point) with no crash; the full bench below
runs end-to-end, streaming row included (was the crash site). Suites:
1998 pass, 0 fail, 2 expected zc skips.

  sequential rt        rei channel                   1.0 us/rt
  sequential rt        rei pool                      0.7 us/task
  pipelined            rei channel           1,213,314.3 rt/s
  pipelined            rei channel batch    20,956,672.1 rt/s
  pipelined            rei pool                983,973.5 tasks/s
  pipelined            rei pool batch        4,478,999.1 tasks/s
  payload 8,000 B      rei pool                      3.2 us/task
  payload 800,000 B    rei pool                     76.9 us/task
  payload 8,000,000 B  rei pool                    405.1 us/task
  fan-out              in-process               23,248.0 tasks/s
  fan-out              rei pool                 81,505.8 tasks/s
  streaming            rei channel          44,069,324.7 msg/s
  map fan-out          rei_map                  82,441.2 elts/s
  map skewed f         rei_map                      11.8 ms wall

Follow-up the same day: rei-bench.R is back on bench::mark (median of
auto-calibrated iterations, filter_gc = FALSE, check/memory off) — the
revert to best-of-3 was only dodging this bug. The streaming row's
~0.5 s of 2M-message iterations (~22M messages) now doubles as the
receive-path stress run and passes. Medians read lower than best-of-3
on the jitter-prone rows (pool pipelined 419k vs 984k tasks/s, pool
batch 3.0M vs 4.5M, sequential pool rt 1.1 vs 0.7 us); the steady rows
are unchanged (channel batch 20.8M rt/s, streaming 44.6M msg/s,
payloads 2.5 / 75.1 / 399.0 us).

A/B regression check (same day): /tmp/ab-bench.R, best-of-5, the fixed
build vs HEAD (pre-fix) installed alternately, two runs each. Deltas are
within run-to-run variance on every row — channel rt 0.88/0.90 us,
channel batch 22.5M/21.7-22.2M rt/s, pool rt 0.78/0.74-0.78 us, pool
batch 5.1-5.3M/4.8-5.2M tasks/s, streaming 45.2-46.2M/46.4-47.0M msg/s
(post/pre). The sink indirection plus the pre-allocated result vector
costs nothing measurable; the batch rows if anything trend slightly
faster (one allocation, no second pass over the array).

## 2026-08-25: map ~10 us compute regime

dev/bench/rei-mirai-bench.R scenario 6's compute regime is now
sum(runif(2e3)) x 2,000 as one map call, ms wall (was scenario 4's
sum(runif(1e4)) fan-out, elts/s) — the README table's last row, so all
four rows source from the one file; dev/bench/rei-bench.R's scenario 6
compute regime mirrors it (rei rows only). Task size calibrated at 11.5 us on
this host (~10 us nominal, the size of the vignette's winsum
benchmark). Measured (M4 Pro, R 4.6.1): serial lapply 18.0 ms, rei_map
5.0 ms, mirai_map 218 ms dispatcher / 89 ms direct. README row updated
to 5.0 ms / 218 ms / 44x (was 5.09 ms / 210 ms / 41x from the
vignette's bench::mark precompile). Scenario 4 (parallel fan-out) in
both scripts now uses the same task, restoring the 4/6 same-work link:
in-process 118k tasks/s, rei pool 333k, mirai 9.2k dispatcher /
23.0k direct.

## 2026-09-10: PROT-anchored pin lifetimes (drop-side O(1))

Replaced R_PreserveObject/R_ReleaseObject on the hot per-payload pin paths
with a per-handle cons-cell chain rooted in the extptr's PROT field
(rei_r_pin/rei_r_drop; O(1) tombstone release replacing the precious list's
first-match linear scan; upstream librei 0d5ef84 adds the
rei_handle_binding_ctx accessor). The win is all on the drop side and grows
with the session's precious-list size, so flat in a fresh-session benchmark
is the expected outcome. test-benchmark.R, this host, standalone run:
channel rt 2.00 us, one-way 20M msg/s, pool rt 1.0 us/task, pool pipelined
500k / collect_all 1.0M tasks/s, ALTREP 1:2^27 round trip 5.00 us (the
serialize-fallback pinned row — flat), channel 1/8/32 MiB 0.09/0.50/2.00 ms,
pool 1/8/64 MiB 0.25/1.50/12.00 ms, rei_map 32 MiB template 0.347s / view
0.346s, 32 MiB attributed 687 ms. Full suite 2232 pass, 0 fail (5 expected
skips). RSS smoke: 1e5 env-payload roundtrips per leg x 3 — 152.5 MB high-water
on leg 1, +0.1/+0.0 MB on legs 2-3 (pins do not accumulate; the splice keeps
the chain proportional to live pins).

A/B regression check (same day, vs e2f1818, alternating installs, two rounds
each): the switch build drew a bad LTO code-layout ticket — channel batch
14.0-15.5M vs 19.5-21.7M rt/s and streaming 33-34M vs 42-44M msg/s, on rows
that execute no pin code at all, and a cross-process env-echo probe went
12-14 to 18.6-18.8 us/rt. Isolation: the vendored-accessor-only build is
flat; an old-mechanics variant (the new call sites, R_PreserveObject /
R_ReleaseObject bodies) runs at pre speed everywhere; the in-process pin
delta (env rt minus codec rt) is 0.30-0.35 us pre vs 0.40-0.50 post; and an
in-process echoed env round trip (2 pins + 2 drops + 2 serializes) is
identical at 3.8 us across pre / post / cold builds. So the mechanics cost
nothing measurable and the hit was layout. REI_COLD on rei_r_pin /
pins_splice (the 2026-08-23 cold-recorder discipline — the pin runs only on
the serialize-paying tiers) restores the rows: channel batch 21.0-21.7M,
streaming 41.8-43.3M msg/s, pipelined and sequential rt flat, full suite
2232 pass. A ~2.5 us residual on the synthetic cross-process env-echo probe
is child-side layout draw (the identical in-process round trip is flat).

## 2026-09-11: int64 native wire type (REI_TYPE_INT64)

Class-only integer64 (bit64's layout) stages as bare int64 bytes on the raw
tiers: the single-probe rei_raw_type gate (rei_raw_eligible deleted), the
class consumed by the wire tag at stage and re-applied at receive
(rei_wire_alloc / the rei_view_vec_wrap case), REIH roots stamped 32 with no
attrs section. rei_map admits class-only integer64 to the raw x section
(past the as.list coercion, which keeps every other object); the batch loop
re-classes per-element scalars. librei a96807f (pre-release: the ABI
version stays 1).
test-benchmark.R, this host, standalone run: int64 channel round trips raw
vs legacy tier (codec below the zc floor, SHM_VEC+attrs above) — 4 KiB
0.00/0.01 ms, 128 KiB 0.01/0.02 ms, 1 MiB 0.06/0.09 ms, 8 MiB 0.25/0.50 ms;
rei_map 100k-element int64 x on 2 workers 0.145 s raw section vs 3.418 s
descriptor (the list coercion + per-runner unserialize it replaced, ~24x).
Full suite 2269 pass, 0 fail (5 expected skips).
