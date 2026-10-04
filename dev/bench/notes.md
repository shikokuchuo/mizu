# Recorded benchmark notes

Dated performance records for mizu. The live reports that print against
these baselines are `tests/testthat/test-benchmark.R` (eyeball them in CI
logs); `dev/bench/mizu-bench.R` runs the mizu-only rows,
`dev/bench/mizu-mirai-bench.R` the matched-scenario mirai comparison, and
`dev/bench/crosslang-map-bench.R` the cross-language map comparison.
Nothing here is asserted against — runner timing is too variable for
thresholds.

Two parts: the best-known table below is the headline — swap a row's
value in only when a run beats it, with the date of that measurement
(lower is better on the latency/wall rows, higher on the rate rows) —
and the dated run log at the bottom is the source of truth, carrying the
bands (several rows swing wide run-to-run; a regression read is "outside
the band", not "slower than the best") and each change's context. Row
definitions evolve with the suite; a row's date says which definition
the value is from, and the log's entry carries it. Reference-framework
rows (mirai, the serial anchors) live in the log only. Append new dated
outcomes at the bottom of the log; update the table only on a new best.

Log entries follow one format (shared with the libmizu and pymizu bench
notes):

```
## YYYY-MM-DD — <what changed or what was run> [(<commit>[, ...])]

<1-4 sentences: the change or the run's purpose, plus any caveat a
reader needs to interpret the numbers — host drift, a re-measure, a
definition change.>

Results: a short list, or a small table when the entry is an A/B.
Status: <suite/lint state when recorded> — omit when nothing was run.
```

All records are M4 Pro, R 4.6.1 unless marked; A/B ratios are the valid
comparisons, absolutes drift with host load. proc.time ticks at ~1 ms on
this host, so timed intervals are kept >> 1 ms by looping.

## Best-known results

The canonical suite (`mizu-bench.R`, plus the cross-language map's
`crosslang-map-bench.R` rows), best recorded value per row. One-off
probes and A/B isolations stay in the log.

| row | best | measured |
|---|---|---|
| sequential rt, channel | 1.0 us/rt | 2026-10-01 |
| sequential rt, pool | 1.4 us/task | 2026-10-03 |
| pipelined, channel | 981k rt/s | 2026-09-26 |
| pipelined, channel batch | 20.4M rt/s | 2026-10-03 |
| pipelined, pool | 680k tasks/s | 2026-10-03 |
| pipelined, pool batch | 4.07M tasks/s | 2026-10-01 |
| payload 8,000 B | 2.8 us/task | 2026-09-30 |
| payload 800,000 B | 75.5 us/task | 2026-09-26 |
| payload 8,000,000 B | 415.6 us/task | 2026-09-26 |
| fan-out x 2000, 4 workers | 378k tasks/s | 2026-09-26 |
| streaming, channel batch | 42.0M msg/s | 2026-10-01 |
| map trivial f | 0.1 us/elt | 2026-09-26 |
| map trivial f, template | 0.1 us/elt | 2026-09-26 |
| map trivial f, .seed | 0.1 us/elt | 2026-09-26 |
| map trivial f, prepared | 0.1 us/elt | 2026-10-01 |
| map ~10us tasks x 2000 | 5.3 ms wall | 2026-09-28 |
| map skewed f x 4000 | 11.1 ms wall | 2026-10-01 |
| serialize-tier result | 3.3 us/task | 2026-09-30 |
| codec-tier result | 2.1 us/task | 2026-10-01 |
| crosslang map trivial f, spec | 0.23 us/elt | 2026-10-01 |
| crosslang map trivial f, spec template | 0.06 us/elt | 2026-10-01 |
| crosslang map trivial f, spec .seed | 1.8 us/elt | 2026-10-01 |
| crosslang map trivial f, spec prepared | 0.22 us/elt | 2026-10-01 |
| crosslang map ~10us tasks, spec | 3.2 ms wall | 2026-10-01 |
| crosslang frame relay, unmodified REF | 0.004 ms/rt | 2026-10-01 |
| crosslang frame relay, one computed col | 0.47 ms/rt | 2026-10-01 |
| crosslang channel rt, small (<= 8 KB) | 0.9-2.5 us/rt | 2026-10-04 |
| crosslang channel rt, 8 MB (REF relay) | 390.7 us/rt | 2026-10-04 |
| crosslang channel rt, 100k difftime | 159.8 us/rt | 2026-10-04 |

## Incumbent baselines (2026-07)

- nanonext `ipc://` pair: 31.7 us per round trip (~31.5k RT/s)
- mirai local dispatch: 63-124 us per task
- target regime: >100k small messages/s sustained
- `mizu_map` overhead regime: serial lapply ~0.2 us/element; per-element
  `mizu_submit`/`mizu_collect` ~4 us; `mirai_map` ~63-124 us/element

## Durable calibration facts

- Small-op floor is the R-interpreter boundary: >85% of a pool round
  trip, the closure call alone ~40% of it. The batch verbs and
  `mizu_map` are the answers for throughput-bound callers.
- Park/wake pair: ~8 us macOS, ~16 us virtualized Linux (isolated by
  pacing an echo peer, stock vs spin=TRUE).
- Zero-copy floors: SHM_VEC loses to the copy tiers below ~8-16 KiB
  (ARENA ~1.9-2.2 us at 256 B-4 KiB vs SHM_VEC ~2.3-2.9 us).
  `MIZU_ZC_FLOOR` (pool) sits at the 16-64 KiB band;
  `MIZU_ZC_FLOOR_RAW` = 256 KiB (channel arena; lifts under the
  Linux-only churn signal). The pool's raw spill is a region too:
  region-raw never beat the view at 64 KiB+ (26.4 vs 21.5 us).
- Large-payload suite cases are sized for a 1 GB /dev/shm (docker
  default): `gc()` between reps, ~32 MiB per rep; suite peak ~440 MiB
  (was ~1.8 GB). Channel/map numbers from 2026-08-17 on are not
  comparable with the 64 MiB records.

## Run log

Dated outcome records, oldest first — the source of truth for the table
above. Append new dated outcomes at the bottom.

## 2026-08-11/12 — zero-copy phases

Before (memcpy-bound) -> after (SHM_VEC/REF; receive is an ALTREP wrap
~0.45 us, echo re-send rides REF): channel 64 MiB 19.0 -> 3.00 ms
(6.3x); pool 64 MiB 16.0 -> 10.25 ms (residual: worker allocVector +
fill + one send-side memcpy). Guards: ALTREP `1:2^27` round trip ~3 us
(133 B stream); 100 MiB matrix round trip 36 ms, then 3-of-100 cols
0.74 ms/read; held 8 x 16 MiB results read 8 spills / 0 reused / 8
fresh (held views pin regions by design; pre-views it was 8/7/1); 32
MiB named vector 725 ms vs 15 ms bare — the attrs blob is ~97% of cost.
Profile split: for flat atomics the serialize pass IS the send memcpy
and unserialize is allocVector + memcpy; object-graph parse is noise.
`.collect = "view"` skips the gather memcpy (0.688 s eager vs 0.666 s
view + reduce on the 64 MiB template; matters for reduce-shaped maps).

## 2026-08-17 — immediate kinds (NIL, STR1), RAWSPILL, gates

NIL/STR1 immediates + C-side submit assembly: pool rt 2.4 -> 1.5 us;
channel NULL 3.1 -> 1.3 us, string 2.6 -> 1.4 us; one-way streaming 30
-> 34-40M msg/s; pool pipelined 0.57 -> 0.69M tasks/s. RAWSPILL
(bare-bytes raw vectors past inline): channel rt 800 B 3.7 -> 1.3 us,
8 KB 5.1 -> 3.0 us. Keeperless collect wake gate: pool pipelined 0.67M
-> 0.84M tasks/s (1 worker), 0.41M -> 0.83M (4 workers). Spin clock
stride 8 -> 64 (clock reads were ~1/3 of host CPU in a channel round
trip). Task handles pack (idx, seq) into the extptr address: below
noise, kept for strictly less hot-path code. Freelist assessed and
rejected: task structs ~1% of controller CPU; the ~25% alloc/GC slice
is R-heap churn, inherent to the task model. Rejected: skipping the
drain-empty head publish for keeperless drains (it is also the
producer's space-reclaim signal; gating it shrunk effective ring
capacity by up to K-1 phantom slots).

## 2026-08-17 — compact codec ahead of R_Serialize

The finding that mattered: R_Serialize allocates a VECSXP(1099) ref
table on EVERY call (R_Unserialize a VECSXP(128)) — several hundred ns
plus ~10 KB of garbage per payload per side. The codec frames
NULL/symbols/atomics (attrs included)/strings/list trees/calls without
it; rejects ALTREP (so codec streams pin no keeper); closures,
environments, S4, attributed pairlist nodes, over-deep graphs fall
back. gp/LEVELS is not carried.

Results (base -> codec): channel rt (0L) 1.00 -> 0.80 us; list(1, "a")
2.60 -> 1.20 us; pool NULL task 1.50 -> 0.75 us; sum(runif(10)) 1.75
-> 1.25 us; pool pipelined 1.0M -> 2.0M tasks/s.

## 2026-08-17 — collect_all

One R boundary crossing per burst instead of per task: pool pipelined
1.0M -> 2.0M tasks/s vs the per-collect loop (~200 ns of the ~500
ns/task floor was the collect closure plus `ts[[i]]`).

## 2026-08-18 — zero-allocation task decode

INLINE codec task frames stream-decode in place on the worker
(`codec_read_task`); a constant task allocates nothing there. Wire
bytes unchanged. Pool round trip `1L` 1040-1120 -> 980-1020 ns; 4-arg
and pipelined a wash (the eval dominates).

## 2026-08-18 — gap-adaptive wait budgets (spin_learn)

Was: static-ceilinged budgets — the channel recv reset to 16 us on any
success; the pool collect grew only by near-miss doubling and could
never climb past a gap above 2x the tried budget. Now: every wait exit
learns the measured turnaround — within the 64 us cap (uniform) the
next budget grows to 1.5x the gap + 8 us headroom (never below the site
constant); past the cap it halves toward the floor. The never-waited
hot path is untouched (no clock read, no budget update).

Results (interleaved A/B, medians): channel rt at 20 us peer gap 29.9
-> 22.9 us, 40 us 51.3 -> 43.4; pool 50 us task 60.9 -> 52.7. Past the
cap it parks with less CPU than stock (1.3% vs 4.3% at a 500 us gap).
A 256 us cap was measured and rejected: the 64-256 us band bought -4%
latency for a full core. Linux container (r-base 4.6.1, aarch64
linuxkit VM, 14 vcpus): the pair costs ~16 us there, so the win band
extends to ~128 us (50 us task 90.3 -> 54.7, 100 us task 132.4 ->
105.0 at cap 128k); kept one uniform 64 us for simplicity — bare-metal
Linux likely has a cheaper pair and a lower crossing.

Status: suites green on macOS and the Linux container, stock and
adaptive alike. Aside (verified identical on stock): serial
submit-collect loops run ~3x slower under devtools::test on this
package's own suite than in a plain R session (submit ~240 vs ~40 us,
worker claim ~190 vs ~37 us, worker exec identical) — the test-run
context inflates the wake/claim path. One test-parker flake seen once
in a container full-suite run (stray SIGCHLD mid-park); passes in
isolation on both builds.

## 2026-08-19 — submit_batch (pool TSO analog)

One .Call, one payload assembly loop, per-element tail publish, and a
wake cadence (every min(64, inj_cap) publishes) plus a protocol wake
pass per worker after the last publish. The single submit's
publish->wake pairing is load-bearing, not ceremony: a worker that
loses the park race mid-burst (pre-park re-check read a stale tail)
sleeps for the full run bound, and only a wake paired with the final
publish contains that race (an earlier version waking only at element 0
deadlocked into collect timeouts that leaked handles into
slots_exhausted a burst later).

Results (evaluate 1L, 1 worker, submit_batch + collect_all per burst):
5.0M tasks/s vs 0.67M looped single submits (7.5x) — the R boundary was
the remaining per-task cost; mirai dispatcher 9.4k, direct 20.1k
tasks/s on the same row. map deliberately does not use it: map submits
O(workers) runner tasks per call, so batching its submit loop is noise;
the raw pool API loop (fire n, collect n) is where the crossing count
is unbounded.

## 2026-08-19 — codec CLOSXP framing (closures by reference environment)

The compact codec now writes closures: formals / body ride the existing
pairlist / call framings, the environment crosses by reference — global
/ base / empty by kind byte, a package namespace by name
(R_Unserialize's discipline). Local environments, bytecode bodies, and
non-srcref-attributed language nodes decline to R_Serialize as before.
keep.source srcrefs cross via a stripped copy (the srcfile they name
cannot cross processes; identical()'s ignore.srcref discipline), one
deep duplicate per srcref-bearing subtree write.

Measured (dev/bench/closure-codec.R): the list(f = <closure>, x = 1L)
payload round-trips in ~900 ns via the codec vs 2500-2900 ns via
R_Serialize (~3x); keep.source closure payload ~2300 ns (strip
included) vs ~12500 ns, keep.source `{` block ~2000 ns vs 12300-18200
ns (~6x — the fallback drags the srcfile environment along). Serial
submit-collect with the closure as a task argument: 3.20 -> 1.80-2.20
us plain (keep.source 2.60-3.00 us), plain task 1.40-1.60 us.

## 2026-08-21 — retain-table seam carve (extraction 1/4) (82d8c67)

The keeper VECSXPs became the explicit per-slot retain table (spill.c):
regions owned by the handle (free list / lent ledger / mapping cache
hold shm pointers, closed + unlinked at eviction and teardown), the
serialize-tier pin rides a parallel VECSXP in the prot chain. The
ledger-overflow drop keeps the name (a REF in flight resolves by name)
where the wrap GC finalizer unlinked it — the deferred unlink without
the GC. Behavior-preserving.

Results (test-benchmark.R, report-only): channel rt 1.50 us; one-way
25.0M msg/s; pool rt 1.0 us/task, pipelined 1.0M / collect_all 2.0M
tasks/s; channel 1/8/32 MiB rt 0.09/0.50/2.00 ms (~21-32 GiB/s); pool
1/8/64 MiB result 0.25/1.50/13.00 ms/task; map 32 MiB template 0.356 s.

Status: suite 1993 pass.

## 2026-08-21 — check/park binding hooks (extraction 2/4)

The 12 in-loop R_CheckUserInterrupt() sites in channel.c/pool.c became
indirect binding check calls (check = r_check, registered at
create/attach/join), and every bounded park in those loops gained the
around-park bracket (park = NULL for R — one load + predicted branch
per sleep). map.c's site is untouched. Behavior-preserving: the R hook
longjmps at the same points.

Results: channel rt 1.50 us; one-way 28.6M msg/s; pool rt 1.0 us/task,
pipelined 1.0M / collect_all 2.0M; channel 1/8/32 MiB 0.06/0.25/1.50
ms; pool 1/8/64 MiB 0.22/1.25/11.00 ms; map 32 MiB template 0.316 s
(view collect + reduce 0.312 s).

Status: suite 1993 pass (2 macOS skips).

## 2026-08-21 — channel stage_r.c split (extraction 3/4)

Channel tier dispatch and materialize moved to new src/stage_r.c as the
binding's stage_fn/read_fn, registered at create/attach alongside check
— the binding struct gained the stage/read fields, and the channel
handle structs moved from channel.c so the stager reaches the free list
and the open/zc caches. The transport keeps the ring, arena, wakes, and
the retain-table commit, and supplies the staging services (arena alloc
returns the chunk pointer — the arena base stays transport-private; the
exported pre-spill reap). On receive the transport resolves
arena-referencing frames (ARENA, channel RAWSPILL) to their byte range
before invoking read_fn — the callback never learns arena mechanics.
The retain entry is stager-initialized; a mid-stage raise still
abandons cleanly. Behavior-preserving.

Results: channel rt 1.50 us; one-way 25.0M msg/s; pool rt 1.0 us/task,
pipelined 1.0M / collect_all 2.0M; channel 1/8/32 MiB 0.06/0.50/1.50
ms; pool 1/8/64 MiB 0.22/1.25/12.00 ms; map 32 MiB template 0.336 s
(view 0.331 s).

Status: suite 1993 pass (2 macOS skips).

## 2026-08-21 — pool exec_fn split (extraction 4/4)

The pool's SEXP half moved behind the binding seam: the task frame
decode, pool_eval_expr, and the condition_flatten ERR framing moved to
stage_r.c as the binding's exec_fn; the result publish split into the
core's mechanics and the binding's staging through the result sink
(stage OK results via the handle's binding.stage, publish_err the
INLINE-framed flattened envelope, publish_died the status-only
terminal); the trace hook became a per-handle core registration with
the emit sites in core paths. One trace-semantics change: "start" now
fires at claim (before the binding's decode), so a task whose payload
vanished with a dead enqueuer traces start+drop where it traced drop
alone — untested death path, no live-pool sequence changes (the suite's
lifecycle expectations pin those). Behavior-preserving.

Results: channel rt 1.50 us; one-way 18.2M msg/s; pool rt 1.0 us/task,
pipelined 1.0M / collect_all 2.0M; channel 1/8/32 MiB 0.06/0.50/1.50
ms; pool 1/8/64 MiB 0.22/1.50/11.00 ms; map 32 MiB template 0.338 s
(view 0.334 s).

Status: suite 1993 pass (2 macOS skips).

## 2026-08-23 — the extraction no-regression run

Measured on the renamed package (the rename commit with the rchk
protect fix folded in, installed) against the 2026-08-21 records — the
pre-extraction baseline.

Results (test-benchmark.R): channel rt 1.50 us; one-way 20.0M msg/s;
pool rt 1.0 us/task, pipelined 1.0M / collect_all 2.0M; channel 1/8/32
MiB 0.06/0.50/1.50 ms; pool 1/8/64 MiB 0.22/1.50/11.00 ms; map 32 MiB
template 0.338 s (view 0.339 s); map trivial f 0.09 us/element; held 8
x 16 MiB reads 8 spills / 0 reused / 8 fresh. Small-payload cases (the
seam's fixed-overhead probe, n=20k): channel NIL rt 0.95 us, STR1 1.00
us (2026-08-17 baselines 1.3/1.4 us); pool submit/collect 0.90 us/task
(baseline 1.0). No hot-path regression from the extraction — the
callback seam costs nothing measurable.

Caveats: the attributed guard's bare sub-case reads 121-124 ms on rei
AND on the installed pre-extraction sora (same-script A/B) — a
pre-existing single-iteration artifact of that guard under the
post-2026-08-17 sizing regime, not an extraction regression (attributed
total 636 ms; 725 in the 2026-08-12 record). ALTREP `1:2^27` rt reads
4.00 us on two runs (~3 us on 2026-08-12): 1000 iterations against the
~1 ms proc.time tick — one tick, inside the quantization band.

## 2026-08-23 — throughput-regression fix 1: cold error recorders (librei 1b66319)

The uniform throughput regression vs sora traced to libmizu's
record-and-return-status idiom: the error recorders are returning,
variadic functions called on every hot verb's failure branches with no
cold annotation, so the compiler treated those branches as live
(register pressure, worse layout, blown inline budgets — helpers sora's
build inlined to nothing were real calls in rei.so). Upstream marks
them COLD.

Results (mizu-mirai bench after re-vendor + install): sequential rt
channel 0.9 us, pool 1.0 us/task; pipelined channel 1.18M rt/s,
channel batch 20.0M rt/s, pool 833k tasks/s; streaming 45.5M msg/s;
payload / fan-out / map rows at parity — matches or beats the sora
reference column on every row.

## 2026-08-23 — throughput-regression fixes 2-5 + the batch-row repair (81f7cca, bb0b8a6, 524165a, 78a3a35)

Landed: the supply-callback submit_batch (the binding stages through
one reusable wire pair — no per-task VECSXP allocation), the per-handle
read-ctx template, the 8-byte packed task with the handle carried in
the extptr address (no malloc/free per task), and the Linux THP
MADV_COLLAPSE at spill free-list insert (compiles out on macOS).

Results (mizu-mirai bench): sequential rt channel 0.9 us, pool 0.5
us/task; pipelined channel 1.19M rt/s, channel batch 20.0M rt/s, pool
833k tasks/s, pool batch 2.86M tasks/s; streaming 41.7M msg/s; payload
/ fan-out / map rows unchanged. Interleaved same-script A/B on the
sustained pool burst (50 x 10k): rei 2.23-2.29M tasks/s vs sora
2.14-2.20M. test-benchmark.R anchors: channel rt 1.50 us; one-way
25.0M msg/s; pool rt 1.0, pipelined 1.0M / collect_all 2.0M; channel
1/8/32 MiB 0.09/0.25/1.50 ms; pool 1/8/64 MiB 0.19/1.50/11.00 ms; map
32 MiB template 0.344 s (view 0.339 s); map trivial f 0.10 us/element;
held 8 x 16 MiB reads 8 spills / 0 reused / 8 fresh. Fix 6
(collect_any/collect_all double marshaling) not done: collect_all
already beats sora post-fix-1, and nothing since put it back in profile
range.

Bench hygiene: the pool-batch row now runs k=10 bursts per rep — one
10k burst lands inside mclock's 1 ms tick and quantized to 10M/5M/3.3M
tasks/s, which is how the regression stayed hidden from that row.

## 2026-08-23 — the Linux pool-rt gap: getpid on the fork guard (librei fe46aa8)

The residual Linux gap vs sora (pool sequential rt 1.5 vs 1.0 us,
pipelined pool ~10-20% behind, pre-existing) traced to the fork guards,
not the wait path. `strace -c -f` on a 20k sequential-rt loop
(container, rocker/r-ver:4.6.1 on linuxkit): identical futex (41,186)
and sched_yield (~16k) counts both packages, but rei made 116,105
getpid calls vs sora's 54,600 — ~6/round trip against ~3. glibc has not
cached getpid since 2.25, so each is a real syscall. rei paid the check
twice per verb (the .Call veneer's handle unwrap, then the core's
pool_get) plus the task finalizer's guard; sora's monolith checked once
per verb. Fix: self_pid() caches the pid in a static atomic, zeroed by
a pthread_atfork child handler so a forked child re-reads. Fork
guards keep their checks — they are now relaxed atomic loads.

Results (interleaved A/B, 5 rounds, best-of): pool rt rei 1.0 = sora
1.0 us; pipelined pool rei 1.0M vs sora 909k tasks/s; channel batch rei
14.3M = sora 14.3M rt/s best-case (rei's per-round median one mclock
tick below sora's — the k-burst read is that the plan's 16.7-vs-20.0M
row was tick quantization, not a real deficit). strace re-check: getpid
absent from the profile (was 116,105 calls).

Status: Linux container rei 2009 / sora 2004 pass; macOS 1998 pass (2
expected zc skips).

## 2026-08-24 — batch-recv heap corruption fixed: sink-callback verbs (librei ff8987c)

The streaming scenario's child death (~750 rounds x 20k messages, xzone
malloc SIGTRAP in the receive path) was binding-side: recv_batch and
collect_all materialized n fresh SEXPs into an R_alloc'd void **
across the remaining reads' allocations — unprotected and invisible to
rchk — so a mid-batch GC freed them and the later SET_VECTOR_ELT wrote
dangling pointers. Fix: sink-callback forms deliver each product as it
is read; the veneer anchors it in a pre-PROTECTed VECSXP (vec_sink).
collect_any got plain PROTECTs. The full bench (below) runs end-to-end,
streaming row included (was the crash site).

Results (mizu-bench summary):

| scenario | framework | value | unit |
|---|---|---|---|
| sequential rt | rei channel | 1.0 | us/rt |
| sequential rt | rei pool | 0.7 | us/task |
| pipelined | rei channel | 1,213,314.3 | rt/s |
| pipelined | rei channel batch | 20,956,672.1 | rt/s |
| pipelined | rei pool | 983,973.5 | tasks/s |
| pipelined | rei pool batch | 4,478,999.1 | tasks/s |
| payload 8,000 B | rei pool | 3.2 | us/task |
| payload 800,000 B | rei pool | 76.9 | us/task |
| payload 8,000,000 B | rei pool | 405.1 | us/task |
| fan-out | in-process | 23,248.0 | tasks/s |
| fan-out | rei pool | 81,505.8 | tasks/s |
| streaming | rei channel | 44,069,324.7 | msg/s |
| map fan-out | rei_map | 82,441.2 | elts/s |
| map skewed f | rei_map | 11.8 | ms wall |

Status: /tmp/repro-stream5.R completes all 3000 rounds (60M messages,
4x the old failure point) with no crash; suite 1998 pass, 0 fail (2
expected zc skips).

Follow-up the same day: mizu-bench.R is back on bench::mark (median of
auto-calibrated iterations, filter_gc = FALSE, check/memory off) — the
revert to best-of-3 was only dodging this bug. The streaming row's
~0.5 s of 2M-message iterations (~22M messages) now doubles as the
receive-path stress run and passes. Medians read lower than best-of-3
on the jitter-prone rows (pool pipelined 419k vs 984k tasks/s, pool
batch 3.0M vs 4.5M, sequential pool rt 1.1 vs 0.7 us); the steady rows
are unchanged (channel batch 20.8M rt/s, streaming 44.6M msg/s,
payloads 2.5 / 75.1 / 399.0 us). A/B regression check (best-of-5, the
fixed build vs HEAD installed alternately, two runs each): deltas
within run-to-run variance on every row — the sink indirection plus the
pre-allocated result vector costs nothing measurable; the batch rows if
anything trend slightly faster (one allocation, no second pass over the
array).

## 2026-08-25 — map ~10 us compute regime

Scenario 6's compute regime is now sum(runif(2e3)) x 2,000 as one map
call, ms wall (was scenario 4's sum(runif(1e4)) fan-out, elts/s) — the
README table's last row, so all four rows source from the one file;
scenario 4 (parallel fan-out) in both scripts now uses the same task,
restoring the 4/6 same-work link. Task size calibrated at 11.5 us on
this host (~10 us nominal, the size of the vignette's winsum
benchmark).

Results: serial lapply 18.0 ms, map 5.0 ms, mirai_map 218 ms
dispatcher / 89 ms direct — README row updated to 5.0 ms / 218 ms /
44x. Scenario 4: in-process 118k tasks/s, rei pool 333k, mirai 9.2k
dispatcher / 23.0k direct.

## 2026-09-10 — PROT-anchored pin lifetimes (drop-side O(1)) (librei 0d5ef84)

R_PreserveObject/R_ReleaseObject on the hot per-payload pin paths
replaced with a per-handle cons-cell chain rooted in the extptr's PROT
field (O(1) tombstone release replacing the precious list's first-match
linear scan). The win is all on the drop side and grows with the
session's precious-list size, so flat in a fresh-session benchmark is
the expected outcome.

Results (test-benchmark.R, standalone run): channel rt 2.00 us, one-way
20M msg/s, pool rt 1.0 us/task, pipelined 500k / collect_all 1.0M
tasks/s, ALTREP `1:2^27` rt 5.00 us (the serialize-fallback pinned row
— flat), channel 1/8/32 MiB 0.09/0.50/2.00 ms, pool 1/8/64 MiB
0.25/1.50/12.00 ms, map 32 MiB template 0.347 s / view 0.346 s, 32 MiB
attributed 687 ms. RSS smoke: 1e5 env-payload roundtrips per leg x 3 —
152.5 MB high-water on leg 1, +0.1/+0.0 MB on legs 2-3 (pins do not
accumulate; the splice keeps the chain proportional to live pins).

Status: full suite 2232 pass, 0 fail (5 expected skips).

A/B regression check (same day, vs e2f1818, alternating installs, two
rounds each): the switch build drew a bad LTO code-layout ticket —
channel batch 14.0-15.5M vs 19.5-21.7M rt/s and streaming 33-34M vs
42-44M msg/s, on rows that execute no pin code at all, and a
cross-process env-echo probe went 12-14 to 18.6-18.8 us/rt. Isolation:
the vendored-accessor-only build is flat; an old-mechanics variant (the
new call sites, Preserve/Release bodies) runs at pre speed everywhere;
the in-process pin delta (env rt minus codec rt) is 0.30-0.35 us pre vs
0.40-0.50 post; and an in-process echoed env round trip (2 pins + 2
drops + 2 serializes) is identical at 3.8 us across pre / post / cold
builds. So the mechanics cost nothing measurable and the hit was
layout. COLD on the pin/splice functions (the 2026-08-23 cold-recorder
discipline — the pin runs only on the serialize-paying tiers) restores
the rows: channel batch 21.0-21.7M, streaming 41.8-43.3M msg/s,
pipelined and sequential rt flat. A ~2.5 us residual on the synthetic
cross-process env-echo probe is child-side layout draw (the identical
in-process round trip is flat).

## 2026-09-11 — int64 native wire type (REI_TYPE_INT64) (librei a96807f)

Class-only integer64 (bit64's layout) stages as bare int64 bytes on the
raw tiers: the single-probe raw gate, the class consumed by the wire
tag at stage and re-applied at receive (wire_alloc / the view vec-wrap
case), REIH roots stamped 32 with no attrs section. map admits
class-only integer64 to the raw x section (past the as.list coercion,
which keeps every other object); the batch loop re-classes per-element
scalars. Pre-release: the ABI version stays 1.

Results (int64 channel round trips, raw vs legacy tier — codec below
the zc floor, SHM_VEC+attrs above): 4 KiB 0.00/0.01 ms, 128 KiB
0.01/0.02 ms, 1 MiB 0.06/0.09 ms, 8 MiB 0.25/0.50 ms; map int64 x on 2
workers: 0.155 s raw section (100k elements) vs 0.035 s descriptor (20k
elements).

Status: full suite 2269 pass, 0 fail (5 expected skips).

## 2026-09-11 — integer64 map templates (regression check) (d6dd821)

int64 output areas stamped for an integer64 .template (exact-type
writes, classed value/view collects) — a no-op for the existing tiers.

Results (test-benchmark.R, standalone run): channel rt 1.50 us, one-way
22.2M msg/s, pool rt 1.0 us/task, pipelined 1.0M / collect_all 2.0M,
ALTREP rt 5.00 us, channel 1/8/32 MiB 0.06/0.50/1.50 ms, pool 1/8/64
MiB 0.22/1.25/10.00 ms, map 32 MiB template 0.320 s / view 0.325 s,
attributed 569 ms — the template rows exercise the edited map
write/gather directly and hold. int64 rows: 4 KiB 0.00/0.00, 128 KiB
0.01/0.02, 1 MiB 0.09/0.09, 8 MiB 0.50/0.50 ms (raw 1/8 MiB soft
against the morning record but matching their legacy counterparts; the
channel path is untouched — layout draw), map int64 x 0.155 s raw
(100k) / 0.035 s descriptor (20k).

Status: 47 pass, 0 fail.

## 2026-09-11 — wire-resolve mapping dedup (consumer-mapping cache + open hook)

The vendored resolve paths dedupe consumer mappings through a
process-global name-keyed LRU (VIEW_CACHE_MAX = 16) whose misses open
via the embedder's open hook (mizu registers its zc open: page-0 RW,
tail RO); the binding's wire-resolve record sheds its per-resolve RW
split mapping, and ref_mark's RO-branch open+close pair goes with it.
Fixes the vm.max_map_count wedge of the morning's CI postmortem (100k
nested references x ~3 VMAs).

Results (d5cfa25 before -> working tree after): channel rt 1.50/1.50
us, one-way 28.6M/25.0M msg/s, pool rt 1.0/1.0 us/task, pipelined
2.0M/1.0M + collect_all 2.0M/2.0M tasks/s, map trivial 0.09/0.09
us/element, ALTREP rt 5.00/5.00 us, channel 1/8/32 MiB
0.06/0.25-0.50/2.00 ms both, pool 1/8/64 MiB 0.19/1.25-1.50/11.00 ms
both, map 32 MiB template 0.332/0.334 s, view collect 0.328/0.325 s,
attributed 579/582 ms, int64 rows identical, map int64 x 0.138/0.138 s
raw. The touched path is off every hot tier; the spread rows repeat
their usual layout draw. New row: nested-view fan-in (2000 refs to one
800 KiB region, in-process) 1.00 ms round trip — 0.50 us per reference
against the ~7 us resolve pre-change.

Status: full suite 2300 pass, 0 fail (6 expected skips: 3 macOS, 3
pyrei env).

## 2026-09-11 — README refresh; the sequential-rt tick mirage

The pool round-trip 0.7/0.5 us readings were whole-ms mclock
quantization: the row looped n=2000 (~2 ms real) and best-of-3 caught a
1 ms tick. Sustained is ~1.1 us (n=200k), matching the 2026-08-18
fine-timer record. The row now loops np=20000 in both bench scripts
(the channel row's remedy) and reads 0.8-0.9 us vs mirai dispatcher
100-106 us across runs — README row set to 0.8 / 100.5 / 126x. pyrei
A/B (perf_counter_ns, no quantization): 0.75-0.80 us/rt — same core,
the ~0.3 us delta the R-side .Call boundary. Pipelined 909k was the
same tick luck (a ~12 ms rep snapping to 10/11/12/13 ms =
1.0M/909k/833k/769k); that row stays at the recorded 833k / 9.7k /
86x. Payload 8 MB and map ~10us rows held: this run's 433 us / 18.5 ms
and 6.0 / 216 ms read below the recorded 46x / 44x.

## 2026-09-13 — binding unification A/B + aux decode pair (regression check) (librei d0e31ff/c35b9c4/f1296a7 -> 401a4fd/63f49d9; 1cedd42/5d1b54c -> e23cbbe)

The widened seam, stage_raw, and the morsel module adopted, then the
handle-binding-ctx drop + aux decode pair.

Results (test-benchmark.R, two runs): channel rt 1.50 us, one-way
25.0M msg/s, pool rt 1.0 us, pipelined 1.0M + collect_all 2.0M tasks/s,
ALTREP rt 4.00 us, channel 1/8/32 MiB 0.06/0.50/1.50 ms, pool 1/8/64
MiB 0.19/1.25/11.00-12.00 ms, map 32 MiB template 0.333 s, view
collect 0.331 s, attributed 576 ms, nested-view fan-in 1.00 ms, int64
rows identical — all inside the recorded bands. Both spread draws
reproduced: map trivial 0.10 then 0.09 us/element; pool 64 MiB 12.00
then 11.00 ms.

Status: full suite 2314 pass (3 macOS skips).

## 2026-09-26 — serialize-tier results scenario (keeperless-flag before-measurement) (9df0195)

Pre-change baseline for the keeperless wire flag (a stager-authored
INLINE aux claim replacing the payload-magic probe; plan:
.posit/assistant/plans/2026-09-26-keeperless-wire-flag.md). New
scenario 7 A/Bs sequential submit+collect of a closure result the codec
declines (a local-env closure -> R_Serialize: pinned at stage,
keeper-ful at collect, the worker's sweep wake fires per result)
against one it carries (a global-env closure -> the codec's kind byte:
unpinned, keeperless). The flag work recovers the machinery share; the
byte cost stays.

Results (full mizu-bench.R): serialize result 3.9 us/task, codec result
2.8 us/task — a 1.1 us/task gap over an equally small closure result:
the keeper machinery (pin, retain entry, keeper-drop record, the
cross-process sweep wake) plus the serialize/codec byte-cost
difference. Anchors, same run: rt channel 1.1 us / pool 1.5 us,
pipelined channel 981k rt/s (batch 14.8M) / pool 599k tasks/s (batch
2.7M), payloads 3.4/75.5/415.6 us, fan-out 363k tasks/s (in-process
110k), streaming 35.1M msg/s, map trivial f 0.1 us/elt across the
variants, ~10us 5.4 ms wall (serial 21.3), skewed 11.3 ms.

## 2026-09-26 — keeperless wire flag landed (after-measurement) (over 0974a74)

The stager claims "no retain-table entry" on bit 0 of the INLINE aux
word; mizu_keeperless reads the header claim instead of probing payload
codec magics; the R binding's serialize-tier pin is now conditional on
the serialize wire hook firing (zc.c's REF-used flag).

Results (full mizu-bench.R): serialize result 3.6 us/task
(before-record 3.9), codec result 2.2 (2.8) — both inside this host's
~1 us run-to-run band (six further scenario-7 draws: serialize 3.5-5.9,
codec 2.2-3.7). Anchors held across the board: rt channel 1.2 us /
pool 1.7 us, pipelined channel 946k rt/s (batch 18.4M) / pool 593k
tasks/s (batch 3.3M), payloads 3.1/85.5/459.0 us, fan-out 378k tasks/s
(in-process 95k), streaming 41.1M msg/s, map rows unchanged (trivial f
0.1 us/elt, ~10us 5.5 ms, skewed 11.4 ms). Collect phase isolated (4k
results staged, the worker drained and parked, then collects timed
alone): serialize 0.75-2.0 us/collect against codec 1.5-2.5 — parity,
consistent with the keeper-sweep wake being skipped. The deterministic
half (no pin at stage; the worker's task env finalizing with no sweep)
is asserted by the new test-pool.R case, and the before-record's
byte-cost share (serialize vs codec on a local-env closure) is what the
remaining scenario-7 gap measures.

Status: full suite 2310 pass, 0 fail (6 expected skips: 3 macOS, 3
pymizu env).

## 2026-09-28 — decline and batch-receive fixes (after-measurement) (0015abd..b03c5b5; libmizu 151fcae)

recv_batch keeps its consumed prefix, collect_all claims only the
reported handle, leave fails an announced in-flight claim; mizu-side:
the exec eval mark rides the whole exec, the codec's missing-namespace
read substitutes .GlobalEnv, the foreign-payload decline is a stashed
classed condition. All changes touch failure branches only; nothing
lands on a hot path.

Results (full mizu-bench.R): rt channel 1.1 us / pool 1.6 us, pipelined
channel 924k rt/s (batch 13.8M) / pool 420k tasks/s (batch 2.6M),
payloads 3.1/80.0/494.5 us, fan-out 370k tasks/s (in-process 109k),
streaming 32.5M msg/s, map trivial f 0.1 us/elt, ~10us 5.3 ms wall
(serial 19.3), skewed 11.3 ms, serialize result 4.3 us/task, codec
result 2.7. The pipelined-pool dip against the 2026-09-26 record
(593k) is host noise: a targeted re-measure of the same loop (4x500
fire-and-collect) draws 666k-1M tasks/s across five reps. Everything
else inside the historical bands.

Status: full suite 2342 pass, 0 fail (3 expected macOS skips).

## 2026-09-30 — the interchange codec (1.1, before/after acceptance)

The 'I' interchange codec landed (foreign-handle interop, the identity
exchange, capability-gated zero-copy); same-language staging is
unchanged by construction (one predicted branch per decision point).

Results (mizu-bench.R, before (312d091) -> after): rt 1.0 -> 1.1
channel, 1.5 -> 1.7 pool us (inside the recorded 1.0-1.7 bands);
pipelined channel 921.7k -> 917.2k rt/s, pool 375.6k -> 592.5k tasks/s
(the pipelined-pool band swings wide across records); payloads
2.6/80.7/457.5 -> 2.8/81.3/448.9 us; streaming 42.7M -> 41.7M msg/s;
map rows flat; serialize/codec result 3.4/2.2 -> 3.3/2.7 us.

Status: full suite 2648 pass, 0 fail (expected skips).

## 2026-09-30 — the 'I' attribute blob and validity sections (3.5a) (libmizu ad3700a)

Attribute blobs in the view layouts are now complete 'I' streams when
encodable (R_Serialize otherwise), applied apply-as-is at receive — an
R->R frame/factor/named-vector receive no longer pays an R_Unserialize
of its attributes. Foreign sends build the MIZH/MIZL validity-bitmap
section fused into the layout write ({off, count} for a column with
NAs, {0, -1} for a clean one, {0, 0} on same-language writes and the
core's flat reserve); mizu declares MIZU_CAP_ATTRS. libmizu ad3700a
fixes the NA test's REAL/CPLX payload discrimination to R's ISNA (any
NaN with low word 1954) ahead of the fused builds.

Results (R->R round trips, before (f6cd80d) -> after): 16 MB frame (1e6
rows) 0.80 -> 1.20 ms, 4 MB factor 0.20 -> 0.20 ms, 32 MiB named
vector (4M names) 480.5 -> 418.0 ms. The frame residual is the
automatic row.names' exact 1:n verification per send: R >= 4.6 stores
it as an ALTREP sequence and the 'I' writer's compact canonicalization
must verify by value — one scan per send, with the size hook's verdict
memo removing the second, and send-only timings actually improved (1.15
-> 1.05 ms).

Status: full suite 2804 pass, 0 fail (3 expected macOS skips); rchk
zero findings in package code.

## 2026-10-01 — the cross-language map (Phase 5, acceptance record) (a85ce19/b655992; pymizu 370df3a/17722cf; libmizu c6f8cd2)

A `mizu_call()` spec as `mizu_map()`'s f on any pool: the 'I'
descriptor (`list[task, x | nil]`), kind-2 runner tasks (region name,
ordinal << 32 | generation, the neutral (seed, offset) pair), and the
foreign runner-shape normalization at collect (DESIGN.md's kind-2
runner row). The dedicated comparison is `crosslang-map-bench.R`,
matched regime-for-regime with mizu-mirai-bench.R's scenario 6.
Cross-language maps land within 2-3x of the native map's per-element
cost and two orders of magnitude under the task-per-element baseline;
template closes most of the remaining gap.

Results (trivial f over 10k doubles, 4 workers): serial lapply 0.18
us/elt (the anchor), mirai_map 40.3 (the task-per-element model's
cost, what the map amortizes), mizu_map native 0.10, spec 0.23 (the
interop descriptor + kind-2 runners + per-element interop results),
spec template 0.06 (results in the output area, no per-element
framing), spec .seed 1.8 (SHA-256 per element on the Python side), spec
prepared 0.22. Compute regime (n = 2000, ~10 us elements): serial 19.2,
native 5.3, spec 3.2 ms wall — the spec row is faster because numpy's
random+sum outruns R's runif+sum per element, a language effect, not a
transport one.

Status: full suite 2961 pass, 0 fail (3 expected macOS skips); rchk
zero findings; R CMD check 0/0/0.

## 2026-10-01 — Phase 5 full-bench no-regression run

Full mizu-bench.R at the cross-language map's landing
(a85ce19/b655992) against the 2026-09-30 record: the
kind-2 runner and 'I' descriptor live off the native hot paths (a spec
f branches once at stage; the exec hook's kind byte sits behind the
task-tag dispatch), so the expectation was flat, and it is.

Results: rt 1.0 channel / 1.5 pool us (the 1.0-1.7 band), pipelined
channel 943.2k rt/s (batch 19.4M) / pool 374.6k tasks/s (the wide
band's low end, batch 4.07M), payloads 3.1/80.8/456.0 us, fan-out 375k
tasks/s (in-process 109k), streaming 42.0M msg/s, map trivial f 0.1
us/elt across plain/template/.seed/prepared, ~10us 5.3 ms wall, skewed
11.1 ms, serialize result 3.3 us/task, codec result 2.1.

Status: full suite 2961 pass, 0 fail (3 expected macOS skips); rchk
zero findings; R CMD check 0/0/0.

## 2026-10-01 — the O(1) ALTREP row.names check (F6)

R >= 4.6's automatic row.names ride an ALTREP compact integer sequence,
whose info is REALSXP c(length, first, incr): the 'I' writer's exact
1:n verdict is now three reads off the info (shape-gated, the scan the
fallback) instead of an O(n) GET_REGION pass per send — the 3.5a
record's one same-language regression closed.

Results (the 3.5a row, 16 MB frame (1e6 rows) R->R, before -> after):
round trip 1.20 -> 0.80 ms (the pre-3.5a baseline), send-only 1.05 ->
0.77 ms.

Status: full suite 2852 pass, 0 fail (expected skips).

## 2026-10-01 — task arguments by reference (F1)

The 'I' task stream gains the ref leaf (0x13): a received view re-sent
as an argument crosses as its identifier (REFHELD at emit, the spec
pinned to the claim-side release), and one fresh layout-eligible
argument stages a single SHM_VEC checkout — D3's size-pass-first picks
the first candidate whose remainder fits inline. The worker force-fires
its argument loans after the outcome write (D5: keyed on what the write
emitted, never reachability), and emission gates on the new
MIZU_CAP_TASKREF (bit 3). The foreign layout write's validity build
gained a pre-scan gate along the way: NA-free vectors now skip the
bitmap pass entirely (they are the common case), only a vector carrying
an NA pays it.

Results (8 MB float64 argument to a foreign-pool task, best-of per the
suite row): R submitter -> Python workers — REF (a received view
re-sent) 0.12 ms/task (the pre-F1 wire copied both ways, ~0.38 ms);
SHM_VEC (a fresh array) 0.50 ms — the layout write plus the Arrow-ready
validity scan against 0.38 ms for the copy (the scan's residual cost,
halved from ~1.0 ms by the gate); same-language spec flat at 1.50 ms
(private frames untouched). Python submitter -> R workers — SHM_VEC
1.14, REF 1.03 against the pre-F1 copy's 1.59/1.51 ms; Python pool flat
at 0.23 ms.

Status: full suite 3029 pass, 0 fail (3 expected macOS skips); pymizu
358 pass, 5 skip.

## 2026-10-01 — the per-column frame REF (F2)

MIZL directory tag 33 (the remote leaf) is normative: a frame whose
columns match a registered export same-index crosses with one remote
leaf per matched column — the identifier span, the referenced leaf's
attrs size and validity claim as resolved — where pre-F2 every
partially-mutated relay wrote every column. mizu reads remote leaves
(the view layer's one resolve path plus the claim validation) and
advertises MIZU_CAP_MIZL_REF (bit 4); pymizu writes them (the
per-column provenance match, the conditional conjunction) and reads
them (the frame path's per-column hold). Emission is per frame: a peer
short of the bit gets full layout leaves for every column.

Results (10-column frame, 1e6 rows of float64, R -> polars -> R,
best-of per the suite row): the unmodified relay (the whole-frame REF,
the regression guard) 0.004 ms/rt; one computed column via polars (1
layout leaf + 9 remote leaves) 0.47 ms/rt against the pre-F2
ten-column layout write (~4 ms/rt, the same-language flat row at
4.00 ms). proc.time ticks at ~6 ms on this host at writing: the REF
row's interval is kept >> tick by 500-rep loops. New rows on the
best-known table.

Status: full suite 3045 pass, 0 fail (3 expected macOS skips); pymizu
363 pass, 5 skip; libmizu make test + test-fuzz + tidy clean.

## 2026-10-03 — the veneer helper extraction (no-regression A/B)

The channel/pool veneer's duplicated statics — the thread-local
create/attach raise, pow2, the suffix and identity-pair validations, and
the handle peek/get unwrap — moved into one shared TU (src/verbs.c), with
the R layer's own dedup alongside (the launch/startup paths, the state
decodes, the map helpers). A cross-TU move on the verb boundary, so the
read was the sequential-rt rows: with -flto the helpers inline back, and
they read flat. Full mizu-bench.R back-to-back on one host, pre-change
(before) against the working tree (after).

Results (before -> after): sequential rt 1.0 -> 1.0 us channel / 1.5 ->
1.5 pool; pipelined channel 912.7k -> 926.6k rt/s (batch 20.31M ->
19.88M), pool 376.3k -> 377.4k tasks/s (batch 3.16M -> 3.43M); payloads
2.8 / 77.4 / 416.2 -> 3.0 / 77.8 / 419.2 us; fan-out 366.1k -> 369.8k;
streaming 43.75M -> 44.32M msg/s; map walls 5.3 / 11.2 -> 5.3 / 11.2 ms;
serialize 3.5 -> 3.2 us, codec 2.7 -> 1.6 us. Every delta inside the
run-to-run band; the after run's streaming, channel-batch and codec rows
read above the best-known table, but as an A/B isolation they stay here.

Status: full suite 3011 pass, 0 fail (expected macOS/pymizu skips).

## 2026-10-03 — the core dedup + channel fork guard (no-regression A/B)

The libmizu core's duplicated blocks moved into internal.h static
inlines (mizu_binding_check, mizu_wait_ms, mizu_shm_set_name; the
submit_many double clock read hoisted to one) and the channel gained
the fork guard — one cached-pid compare per verb entry
(chan_get/chan_forked; alive/peer_ident/drop answer empty, destroy
unguarded) — vendored at libmizu 909563b. Static-inline moves with the
fork compare the only added hot-path work, so the expectation was
flat. A/B on one host, sequential runs, identical flags: baseline HEAD
(core 1efb883) against the working tree (core 909563b plus the R-side
recode_states/valid_token dedup, neither hot), one run each.

Results (A -> B): sequential rt 1.0 -> 1.0 us channel / 1.5 -> 1.5
pool; pipelined channel 894.0k -> 923.9k rt/s (batch 19.68M ->
19.42M), pool 358.7k -> 352.5k tasks/s (batch 3.71M -> 4.34M);
payloads 3.1 / 81.0 / 429.4 -> 3.0 / 77.8 / 429.7 us; fan-out 371.1k
-> 371.3k; streaming 41.73M -> 41.95M msg/s; map walls 5.3 / 11.1 ->
5.4 / 11.3 ms; serialize 3.2 -> 3.2 us, codec 1.7 -> 1.7 us. Every
delta inside the run-to-run band; as an A/B isolation it stays here.

Status: full suite 3171 pass, 0 fail (3 expected macOS skips).

## 2026-10-03 — dedicated re-baseline attempt: codec tier, pool batch

A best-of-3 re-baseline run (full mizu-bench.R x3 back-to-back, nothing
else in flight, loadavg ~2.6-2.9; the working-tree build at libmizu
909563b) after the day's A/B isolations read above best on two rows.
Neither target reproduces at recording time: codec tier 2.6/2.5/2.4 us
(best-of-3 2.4 — the record stands at 2.1 from 2026-10-01; the 1.6/1.7
A/B reads needed a quieter machine) and pool batch 2.77M/2.99M/3.64M
(best-of-3 3.64M — the record stands at 4.07M from 2026-10-01; the A/B
day's 4.34M was the wide row's lucky draw). Three incidental rows did
set bests, all in R2's quiet window, and swap onto the table: pipelined
pool 372.7k/679.8k/376.7k -> 680k (a bimodal row — its band here runs
~370k-680k), pipelined channel batch 20.0M/20.4M/19.8M -> 20.4M, and
sequential rt pool 1.4 us at display precision (R1/R3 1.5). Map walls
tie or below (best-of-3 5.3 / 11.3 against records 5.3 / 11.1).

Status: full suite 3171 pass, 0 fail (3 expected macOS skips).

## 2026-10-04 — cross-language channel bench harness + difftime interchange

First run of dev/bench/crosslang-channel-bench.R (new: R host against a
Python echo peer, plain-echo and copy-echo peers per payload — one rt is
R stage, Python read, Python stage, R read) and the first measurement of
the difftime <-> timedelta64[us] interchange that landed the same day
(the R writer emits attr(realv, {class: "difftime", units}), pymizu homes
timedelta64[us]; a temporal numpy scalar no longer crosses as its uint8
byte view). Three channel rows enter the table (small, 8 MB REF relay,
100k difftime); the rest stay log-only as the suite's first record.

Results: NULL 0.9, scalar double 0.9, scalar string 0.8, 8 KB double
2.3 (copy echo 2.9), 800 KB double 51.0 (68.7), 8 MB double 390.7
(625.7) us/rt; 10k strings 188.4, 100k logical+NA 32.5, 10k Date 72.0,
100k difftime 159.8, frame 1k x 4 77.7, frame 100k x 3 189.8 us/rt;
pipelined 8 KB doubles 99.0k rt/s. The crosslang map suite re-run after
the interop change holds every band (spec 0.23 us/elt, template 0.06,
seed 1.8, compute 3.1 ms).

Status: mizu full suite 3176 pass, 0 fail (3 expected macOS skips);
pymizu 396 pass, 0 fail (5 skips); ruff + pyrefly clean.

## 2026-10-04 — difftime frame columns (the frame half of the interchange)

difftime columns joined the frame whitelist (R qualification + the MIZL
leaf blob; pymizu's FCOL_TD — duration[us] on the Arrow export,
timedelta64[us] from to_dict(), the Arrow stream's duration[unit] read).
Same-day re-run of the channel bench for the one new row; the
fcol_ensure_export lazy-scan double-count this surfaced (a pre-existing
latent bug — a lazy-scanned column with NAs reported 2x null_count on
the Arrow export) is fixed and the run validates it.

Results: frame 100k x 2 with a difftime column 237.6 us/rt (the
all-double frame 100k x 3 at 206.3 in the same run); 100k difftime
standalone 148.0 us/rt.

Status: full suite 3189 pass, 0 fail (3 expected macOS skips).

## 2026-10-04 — MIZS write-asymmetry instrument rows + baseline (ebd2521)

New rows ahead of the string-write fixes (the mizs-write-asymmetry
plan): a send-only row against an acking Python sink on both
cross-language drivers (the ack is the NULL row's cost; the remainder
is the send-side stage the echo round trip only implies), a
UTF-8-marked 10k-string variant, and same-language 10k-string / small
named char rows in mizu-bench.R. The UTF-8 variant carries non-ASCII
content (valué-…): R clears encoding marks on pure-ASCII CHARSXPs, so
an ASCII "marked" vector is unmarked — the first cut of the row
measured the same unmarked payload twice. The small named char row
lives same-language only: a named atomic has no portable foreign home,
so the cross-language send of it raises not_portable.

Baseline (the committed pre-fix build): R-hosted 10k strings 187.4
us/rt echo, 165.3 us/send sink; UTF-8-marked 142.8 us/rt echo, 120.9
us/send sink — the mark skipping the gate's byte validation prices
that pass at ~44 us on this payload. Same-language: 10k strings
124.1 us/rt channel echo, small named char 1.4 us/rt (the declined
path — F0's no-regression row). Python-hosted mirror: 10k strings
22.7 us/rt echo, 21.7 us/send sink. The acceptance target for the
fixes: R send-only within ~1.5x of the Python sink number.

Status: rows report-only; no suite run (no code change).

## 2026-10-04 — MIZS string-write passes: gate flag read, fused probe, bulk sections

Landed the mizs-write-asymmetry fixes in risk order, each A/B'd against
the day's instrument baseline (the entry above): F1 (the foreign UTF-8
gate reads R >= 4.5's Rf_charIsASCII bit instead of validating), F0
(the probe's byte sum is the exact layout size for an attribute-free
STRSXP — the size walk is skipped — and the foreign gate walk is fused
with the probe behind the ALTREP fence), F2+F4 (the foreign write
bulk-fills encoding CE_UTF8 with NA bytes zeroed in the loop, validity
a bulk 0xFF with NA bits cleared, offsets running-pointer stores, the
upfront section memset gone), and one fix the plan's F2 premise forced:
frame string columns were the one ungated foreign path into the MIZS
writer (a latin1 column reached the Python reader marked and failed
there — verified empirically), so the frame branch of
mizu_zc_eligible_foreign now gates string columns like the top level
(latin1 takes the 'I' copy translated, bytes declines at send; REF-able
view columns are never walked). A further pass the sample profile
justified (F5): the three string walks take one STRING_PTR_RO call per
walk (R >= 4.5, ALTREP keeps STRING_ELT) and the gate tests the ASCII
bit ahead of the mark switch — mkCharLenCE clears every mark on
pure-ASCII CHARSXPs (compiled probe), so the bit means unmarked native.

Results (A/B against the committed baseline, same host): R-hosted 10k
strings 187.4 -> 86.6 us/rt echo, 165.3 -> 64.9 us/send sink; the
UTF-8-marked variant 142.8 -> 95.8 echo, 120.9 -> 73.8 sink (the mark
no longer wins — unmarked ASCII now takes the one-call fast path).
Same-language: 10k strings 124.1 -> 88.3 us/rt channel echo; small
named char 1.4 -> 1.5 us/rt (the declined path, no regression). F1
alone was worth ~39 us/send (H1: the byte validation), F0 ~32 (H2: one
pointer walk), F2+F4 ~7 (H3: minor as predicted), F5 ~22. Python-hosted
mirror flat (21.7 -> 20.9 us/send — the Python stage is unchanged
code). The residual 3.1x against the Python sink (was 7.6x) is libR's
call-based CHARSXP accessors: sampling attributes ~30 us of the 65 to
LENGTH/R_CHAR/charIsASCII call overhead (10k elements x 4 calls) with
memcpy (~19 us) and transport at parity — the public C API has no
inline CHARSXP length/bytes accessors, and closing further would mean
sxpinfo bit-poking, which the rchk-clean public-API discipline rules
out. The ~1.5x acceptance target is not reachable within it; the
payload matrix validates the edge shapes (latin1 control: 'I' copy, no
view; 1k-long strings: 262.8 us/send, the memcpy-dominated ratio
narrowed to ~2x; 50% NAs: 30.3; a mori ALTREP string vector: fenced to
the copy tiers, no view on receipt).

Status: full suite 3196 pass, 0 fail (3 expected macOS skips); pymizu
398 pass / 5 skip plus 96 crosslang pass against this build; mori
re-vendored from this tree, 433 pass, 0 fail (23 expected macOS skips);
rchk bcheck identical to the pristine tree (zero package findings);
libmizu make test green.

## 2026-10-04 — the nested MIZL string-leaf duplicate, sized (no code change)

The follow-up measurement the MIZS plan's cross-references called for:
whether the remaining duplicate walks (foreign gate + tree probe +
nested size per string column, vs the top level's fused single walk)
show at scale. The committed frame rows can't show it — their string
columns are <= 5 KB — so a 100k-string column was measured directly
(foreign sink and same-language echo, quiet host, 635df0c). Findings
filed as a new investigation note (.posit plans,
mizl-string-leaf-duplicate.md): candidates N1 (budget-capped tree
probe) and N2 (verdict fused into the tree walk) are zc.c-local; N3
(recorded per-leaf sums retiring the nested size walk's string
component) crosses the vendored seam and waits on N1/N2's evidence.

Results: 100k strings top-level 574.9 us/send; frame 100k x 3 numeric
108.7; frame with the 100k-string column 893.8 (the column's marginal
785 vs 575 top-level — ~210 us, two duplicate walks at ~105 each).
Same-language: top-level 676.3 us/rt, frame with the column 867.4 (one
duplicate walk plus frame overhead). pymizu pays two walks per nested
string column — the parity target.

Status: measurement only; suite state as the previous entry.

## 2026-10-04 — MIZL nested string leaves: capped probe, fused walks, presized sums

Landed the mizl-string-leaf-duplicate plan's three candidates (the
plan's rows measured ad-hoc: foreign sink and same-language echo, the
frame columns materialized — a compact ALTREP column takes the frame
off the MIZL tier same-language). N1: the tree probe caps at the
gate — exact within budget, partial past it; string-leaf walks shorten,
but every node stays visited so a non-REF-able view past the clearing
point still rejects. N2: the foreign caps walk and probe fused into one
mizu_zc_tree_walk_fx, verdict before sum per leaf. N3: string-leaf body
sums recorded in pre-order feed a new mizu_view_layout_size_sums,
retiring the foreign size pass's string walks (a count mismatch falls
back to the plain pass; same-language keeps the capped probe — already
at its two-walk floor).

Results (per-step A/B): the foreign frame with a 100k-string column
843.7 -> 739.6 -> 742.4 -> 605.5 us/send (N2 ~0 — N1 had already cut
the foreign probe to O(gate)); same-language 827.9 -> 716.6 -> 696.1
us/rt. The column's marginal cost 0.93x the top-level send (target
1.2x) — pymizu's two-walk parity; same-language 1.09x (target 1.1x).
Guards unmoved.

Status: full suite 3212 pass, 0 fail (3 expected macOS skips); pymizu
398 pass / 5 skip plus 96 crosslang pass; mori re-vendored from this
tree, 433 pass, 0 fail (23 expected macOS skips); rchk zero package
findings; libmizu untouched.
