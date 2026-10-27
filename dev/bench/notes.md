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
| payload 8,000 B | 2.7 us/task | 2026-10-22 |
| payload 800,000 B | 47.3 us/task | 2026-10-22 |
| payload 8,000,000 B | 262.8 us/task | 2026-10-22 |
| fan-out x 2000, 4 workers | 383k tasks/s | 2026-10-22 |
| streaming, channel batch | 42.0M msg/s | 2026-10-01 |
| map trivial f | 0.1 us/elt | 2026-09-26 |
| map trivial f, template | 0.1 us/elt | 2026-09-26 |
| map trivial f, .seed | 0.1 us/elt | 2026-09-26 |
| map trivial f, prepared | 0.1 us/elt | 2026-10-01 |
| map ~10us tasks x 2000 | 5.2 ms wall | 2026-10-22 |
| map skewed f x 4000 | 11.1 ms wall | 2026-10-01 |
| serialize-tier result | 3.3 us/task | 2026-09-30 |
| codec-tier result | 2.0 us/task | 2026-10-22 |
| crosslang map trivial f, spec | 0.23 us/elt | 2026-10-01 |
| crosslang map trivial f, spec template | 0.06 us/elt | 2026-10-01 |
| crosslang map trivial f, spec .seed | 1.8 us/elt | 2026-10-01 |
| crosslang map trivial f, spec prepared | 0.22 us/elt | 2026-10-01 |
| crosslang map ~10us tasks, spec | 3.2 ms wall | 2026-10-01 |
| crosslang frame relay, unmodified REF | 0.004 ms/rt | 2026-10-01 |
| crosslang frame relay, one computed col | 0.47 ms/rt | 2026-10-01 |
| crosslang channel rt, small (<= 8 KB) | 0.9-2.5 us/rt | 2026-10-04 |
| crosslang channel rt, 8 MB (REF relay) | 390.7 us/rt | 2026-10-04 |
| submit plan k = 0 (W2 row) | 37.2 us/task | 2026-10-22 |
| submit plan k = 1 (W2 row) | 39.5 us/task | 2026-10-22 |
| submit plan k = 4 (W2 row) | 43.7 us/task | 2026-10-22 |
| submit plan k = 16 (W2 row) | 61.0 us/task | 2026-10-22 |
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

## 2026-10-04 — W2: the zc-selection fold (the task plan walks once)

The mizu half of pymizu's 2026-10-04 W2 entry: the 'I' task submit's
per-candidate size-pass loop folded into the one record walk —
ixp_probe and mizu_interop_task_plan are gone, the walk records the
first 16 candidates with their by-value subtree sizes (has_ref read
off the emitted ref leaves — exact, where the probe's was an
approximation: it missed views nested inside a recorded candidate and
counted views inside attr shapes the writer never emits by
reference), and selection is arithmetic over the walk's own total.
One walk for a candidate-free inline spec, two for a by-reference
one — was k+2. Selection A/B-verified identical to the pre-fold
implementation over 60 randomized spec trees plus the 11-row
candidate matrix (worker-side view probes, the adjusted ==
inline_max boundary inclusive both sides).

Results (plan-cost A/B, pool_pair in-process — a 1000-string
by-value filler plus k 40 KB vectors, submit+step+collect):

| k | before | after |
|----|----|----|
| 0 | 113.1 | 98.6 |
| 1 | 83.9 | 60.9 |
| 4 | 145.3 | 66.2 |
| 16 | 400.1 | 104.8 |

Flat in k (was linear — the per-candidate passes re-walked the whole
argument tree); the residual growth is the spill bytes themselves.
The full bench's rows unmoved (sequential rt 1.5 us/task, payload
8 KB 2.7, 800 KB 79.9, 8 MB 437.3 us/task); the crosslang spec-map
row 0.24 us/elt vs the 0.23 record (host drift). pymizu's
counterpart landed the same day (its k = 16 row 2,225.3 -> 431.1
us/task).

Status: full suite 3291 pass, 0 fail (3 expected macOS skips; new
candidate-matrix, randomized-spec, and full-caps rows in
test-call.R).

## 2026-10-06 — streaming mizu_map (.stream = TRUE) [(be838b3)]

Validation rows for the streaming map (a sliding submit/collect window
of fixed x-slices; SHM residency bounded by window x slice, never the
whole of x). The footprint model confirms at W/C: descriptor-only
region + W serialized slices = 6.4% of the region path's 15.3 MB for a
16 MB x (the model's 6.2%). The first read caught a loop pathology:
the generic-path splice dispatched per element from R (~43 ms/chunk at
15625-element chunks), which the C splice (mizu_map_vsplice) removes —
post-fix the trivial-f rows run at/under the region path and big-x
carries the documented serialized-slice trade-off, not a starvation
(the template row is the starvation control: no result publishes).

Results (big-x = 2e6 doubles, 16 MB; trivial f = 1e6, 4 workers):
big-x region 0.581s | stream 0.764s (1.31x); SHM 15.3 MB region |
0.97 MB stream (6.4%, model 6.2%). trivial f region 0.103s | stream
0.052s (0.50x); trivial f template region 0.044s | stream 0.046s
(1.05x). stream chunk sweep auto(128)/32/128/512: 0.70/0.60/0.70/0.18s.
Suite rows in band (rt 1.00 us, map trivial f 0.10 us/elt, 32 MiB
template 0.08 us/elt).

Status: full suite 3461 pass, 0 fail (3 expected macOS skips); R CMD
check 0 errors / 0 warnings / 0 notes.

## 2026-10-12 — the Arrow frame paths vs the conversion baselines

A/B for the Arrow send gate (the core writer off the Arrow buffers) and
mizu_as_arrow's zero-copy receive export, against the conversions they
replace (dev/bench/arrow-bench.R): a 2e5-row x 5-col table (double,
int32, string, dictionary, timestamp[us] — ~6.1 MB as a frame) on one
channel, bench::mark medians, >= 10 iterations.

Results (send): mizu_send(arrow_table) 1.8 ms, 1.1 MB alloc |
convert_array_stream + mizu_send(df) 20.0 ms, 8.6 MB alloc (~11x, the
per-element CHARSXP creation and the second fixed-width copy). Results
(receive): mizu_as_arrow(frame) 2.2 ms, 2.5 MB alloc |
as_nanoarrow_array_stream(frame) 10.1 ms, 5.4 MB alloc (~4.6x, the
borrow vs the full materialize).

Status: full suite 3584 pass, 0 fail (3 expected macOS skips); rchk
clean (only R's own "too many states" in RunGenCollect / bcEval_loop /
strptime_internal).

## 2026-10-12 — the remote-column borrow vs the copy baseline

A/B for mizu_as_arrow's remote-leaf borrow (every region-backed column
now borrows through the one uniform locate — the frame's own leaves,
remote leaves, and root views — one counted loan per distinct region),
dev/bench/arrow-bench.R's relay scenario: a 2e5-row frame whose a / a2
columns cross back as remote leaves over the host's relayed region (b a
local leaf), bench::mark medians, >= 10 iterations.

Results (receive): mizu_as_arrow(relay) 0.75 ms, 0.4 KB alloc |
as_nanoarrow_array_stream(relay) 0.69 ms, 3.8 MB alloc — parity on time
(a memcpy of fixed-width columns is cheap), the win the ~10^4x fewer
allocated bytes (no materialization; the pages serve in place).

Status: full suite 3635 pass, 0 fail (3 expected macOS skips).

## 2026-10-14 — the small-table codec path + the ns/dict/null-count trims

The cheap half of the Arrow review's small-table item: a data.frame's
automatic row.names now write as the compact c(NA, -n) in the codec
(R >= 4.6's expanded ALTREP 1:n declined every ordinary data.frame to
R_Serialize), and the conversion bail materializes nanoarrow's lazy
ALTREP strings so the codec takes a below-gate Arrow frame. Also in this
build: MIZU_CAP_MIZL_TSN dropped from R's caps (an ns column lands as a
view, not a copy), the MIZS export null count is a popcount, the Arrow
send's namespace lookups are cached, and the top-level send's extptr
finalizer is gone.

Results (channel_pair, 50-row df double/int32/string, send + recv
round trips with a drain per iteration, bench::mark medians over 3
rounds): the Arrow path ~200 us round trip (190-294 across rounds) vs
the review's 303 us pre-fix probe — an honest ~1.5x, the components
summing consistently: the nanoarrow pull ~51 us, the convert-back ~141
us, the codec round trip ~2.5 us; serialize(df) 3.36 us / 24.3 KB the
pre-fix reference. The ~141 us convert-back (a double nanoarrow round
trip) is the residual the thorough small-frame form would remove (~3x,
to ~65 us). (An earlier send-only read of this mark ran with no drain —
the ring and arena filled mid-mark and later iterations returned the
full sentinel unstaged — and its 30.7 us / 738 ns medians read low.)
The 2e5-row suite (dev/bench/arrow-bench.R): send_table 1.54 ms (was
1.8 — the dictionary levels no longer copy at open), receive 2.22 ms
(was 2.2), relay 763 us (was 750; drift band). An ns timestamp column's
receive now borrows: the column is a view (test-arrow.R's rc_of pins).

Status: full suite 3782 pass, 0 fail (3 expected macOS skips).

## 2026-10-14 — the thorough small-table form: one 'I' write off the Arrow buffers

The Arrow review's small-table item, thorough form: a below-gate frame
now crosses as one 'I' stream written straight off the Arrow buffers
(libmizu's new mizu_arrow_write_ix, 0c6f5c9, re-vendored into both
bindings) over the INLINE / arena / SHM_RAW tiers, aux keeperless — the
convert-back to a data.frame (the ~141 us residual) is gone. pymizu's
inline emitters are deleted (~200 lines) for the same core writer; the
stream bytes are pinned identical by the libmizu unit tests and the
unmodified cross-language suites. The same-language read paths needed
no change: mizu_stream_read already dispatches the 0x49 first byte
ahead of the codec and the R native streams.

Results (the 2026-10-14 methodology: channel_pair, 50-row df
double/int32/string, send + recv round trips drained per iteration,
bench::mark medians over 3 rounds, A/B back-to-back on this build):
238/255/289 us -> 80.5/83.5/85.2 us round trip (~3.1x; the ~65 us
projection inside the host-drift band — the ~51 us nanoarrow pull is
the irreducible floor), steady-state alloc 36.8 KB -> 11.4 KB. df_rt
unchanged at ~2.8 us.

Status: full suite 3789 pass, 0 fail (3 expected macOS skips), rchk
clean.

## 2026-10-14 — W2: O(n) dict-key uniqueness, one qualification per node per stage

The interop review's cliff item: mizu_interop_names_ok now settles
uniqueness in one pass over an open-addressed set keyed on each
element's translated UTF-8 form (was O(n^2), re-validating every earlier
element per pair), and the attr qualification verdicts ride a
generation-stamped per-stage pointer cache armed/disarmed by the stage
hooks (the single-slot ix_blob_memo retired) — a frame's factor levels
qualified four times per R->R round trip, five to a foreign peer, now
once. The stage guard (R_ExecWithCleanup around both stage hooks) keeps
an armed generation from outliving the staged tree's anchoring.

Results (A/B back-to-back on this build): factor frame 100k x 5k-level
col 424 -> 0.33 ms/rt (character col 0.67 — the acceptance gate); the
'I' writer hook at 5k levels 212 ms -> 0.4 ms per size+write pair.
Hot rows flat: channel seq rt 1.11 -> 0.82 us, pool seq rt 0.94 -> 0.94
us, pool pipelined 767k -> 758k tasks/s (the guard costs nothing
measurable). New canonical row "100k frame, 5k-level col" (factor vs
character column) joins mizu-bench.R and test-benchmark.R.

Status: full suite 3800 pass, 0 fail (3 expected macOS skips); the
crosslang files green with pymizu's venv on PATH; pymizu crosslang
suites 119 pass; rchk clean (bcheck, 1152 functions — only R's own "too many states" notes).

## 2026-10-14 — W4: ASCII fast path, one encode per pass in the 'I' string writer

The interop review's string-writer item: ix_char_utf8 tests
Rf_charIsASCII first on R >= 4.5 (pure-ASCII CHARSXPs are unmarked, so
every ASCII string paid a full UTF-8 validation — the MIZS gate's
2026-10-04 pattern), and ixw_str_size now hands the UTF-8 pointer to
ixw_str_put instead of re-deriving it — the two passes validated and
encoded each string four times, now once per pass.
mizu_interop_names_ok rides the same fast path per name.

Results (A/B back-to-back on this build, the review's writer-hook
repro, 20-byte strings, bench::mark medians, ns/string):

| case | before | after |
|---|---|---|
| ASCII, 10k strings | 42.6 | 11.5 |
| ASCII, 100k | 43.9 | 11.2 |
| ASCII, 1M | 47.4 (47.4 ms) | 13.9 (13.9 ms) |
| UTF-8-marked, 10k | 15.9 | 14.0 |
| UTF-8-marked, 100k | 17.6 | 13.0 |

The acceptance gate was ASCII within ~1.2x of the marked figure; it now
runs slightly ahead of it.

Status: full suite 3800 pass, 0 fail (3 expected macOS skips); the
crosslang files green with pymizu's venv on PATH; rchk clean.

## 2026-10-14 — W3: registry-first task name resolution on R workers

The interop review's per-task loadNamespace item: ixt_resolve_name
calls R_FindNamespace (the registry read, loadNamespace only on a
miss) instead of eval'ing loadNamespace per kind-0 task — the plan's
literal Rf_findVarInFrame(R_NamespaceRegistry) is behind
ENABLE_LEGACY_NONAPI on R 4.6's headers, and R_FindNamespace is the
public entry implementing the same discipline. pool_task_runner's
per-runner R_FindNamespace + findFun is cached on first use (the
arrow_ns pattern). The optional CHARSXP->function cache is skipped:
resolution is ~1.5 us of a 2.7 us round trip.

Results (A/B back-to-back on this build, 1-worker pool, bench::mark
medians): spec round trip (base::identity over 100 doubles) 12.8 ->
2.7 us; plain mizu_submit round trip flat at 1.19 us.
loadNamespace("stats") on a loaded namespace measures 11.9 us — the
removed share.

Status: full suite 3803 pass, 0 fail (3 expected macOS skips); the
crosslang files green with pymizu's venv on PATH; rchk clean (bcheck,
1153 functions — only R's own "too many states" notes).

## 2026-10-16 — latent-bug fixes (R-01..R-06, C-03): A/B against a854d76

The corruption-first audit PR (077de8b): the MIZL wrap-time remote-leaf
walk + loans (R-02), the spec-task release gate on REFCNT (R-01), the
eval mark across the OK publish (R-03), R_CheckStack in the five zc
walkers (R-04), map batch quote-splicing (R-05), the streaming int64
gate (R-06). A/B is dev/bench/mizu-bench.R back-to-back, two runs per
side interleaved (A = a854d76, B = 077de8b), same flags both sides,
min-of-two per row. The mizu-free anchor (serial lapply ~10us) drifted
-6.1% across the session, so ±5% reads as noise.

Rows touching the changes: MIZL receives (100k frame) +1.3% / +1.6%
(inside the drift envelope); spec plans k = 0..16 (the REFCNT gate)
-4.7% .. +0.9%; payloads (SHM_VEC churn) -3.3% .. +1.5% with large
within-A variance on the 8 MB row (589 vs 468); streaming -4.9% and
pipelined pool batch +16.9% both unattributable (untouched paths,
within-run drift comparable); sequential rt / fan-out / serialize /
codec / map rows all flat (+/-1%). Verdict: no measurable regression
from the wrap-time walk or the REFCNT gate.

Status: full suite 3582 pass, 0 fail (51 skips, the expected set); the
crosslang files green with pymizu's venv on PATH (238 + 86); rchk
clean (bcheck, 1174 functions — only R's own "too many states" notes).
## 2026-10-16 — latent-bug fixes (R-07..R-21): A/B against 75beb21

The second mizu audit PR: Elt / Get_region on the five vec classes
and u64 (R-07), the read-hook containment (R-21 — an R_ToplevelExec
catch scoped to the serialize / zc tiers, the trivial tiers
unguarded), and the non-performance items (R-08..R-20). A/B is
dev/bench/mizu-bench.R back-to-back (A = 75beb21, B = the PR tree).

Results: every touched row within band — sequential rt flat (1.0 /
1.4-1.5 us), serialize / codec results flat (3.2-3.4 / 1.8-2.2 us),
payloads and frames within run-to-run variance, streaming flat; the
containment measures at baseline on direct probes (REF / codec /
STR1 roundtrips unchanged). No measurable regression.
Status: full suite 3738 pass, 0 fail (51 skips, the expected set);
crosslang green with pymizu's venv on PATH (4 runs); rchk clean
(bcheck, 1199 functions — only R's own "too many states" notes).

## 2026-10-19 — C1 sign-masked NA pre-filter: A/B against 92e5717

The C1 fix (src/view.c mizu_view_na_present REALSXP/CPLXSXP: the
exponent pre-filter gains the sign mask matching mizu_ext_na_real_bits).
The loop runs only on foreign SHM_VEC stages of attributed vectors and
frame columns, so the instrument is crosslang-channel-bench.R (the
frame rows exercise it full-length: NA-free columns scan every
element), not the same-language mizu-bench.R. A/B back-to-back
(A = 92e5717, B = the fix), full script once per build plus a
frames-only reduced script (min_time 1.0) alternated A/B/A/B.

Results: frame 100k x 3 — A 725.5 / 1226.2, B 1202.5 / 1198.4 /
1225.2 us/rt: the 725.5 was host-load drift between batches (the
reduced A reproduces B's ~1200 cluster on the same script); frame
100k x 2 +td 1052/1436 vs 1013/1396/1423; frame 1k x 4 97/99 vs
96/99/99; every other row within the same drift band. Verdict: no
measurable regression — the extra AND per element is below noise.
Status: full suite green (summary reporter, 63 skips — the no-venv
crosslang set plus the usual CRAN/Linux set); test-interop.R 335 pass
0 fail; test-crosslang.R 136 pass 0 fail with the venv on PATH (both
C1 tests fail pre-fix with the expected diffs); rchk not run (no
docker here — the change is a branch-local expression edit, no SEXP
traffic).

## 2026-10-19 — Linux create-path port: statvfs fail-fast + one-pass populate (libmizu 85cd4fb)

The two Linux create-path fixes from the 2026-10-19 Xeon session landed
upstream and re-vendored: oversize creates fail fast on a statvfs
availability probe ahead of the reservation (the posix_fallocate grind
through all of tmpfs is gone), and the reservation is one
madvise(MADV_POPULATE_WRITE) pass, fallocate + MAP_POPULATE on < 5.14.
Verified on the reference host itself (kernel 6.8 Xeon W-2235 bare
metal): full mizu-bench.R with GLIBC_TUNABLES (mmap_threshold 32 MiB,
trim_threshold 128 MiB), plus the R-level create spot check
(mizu_region_create, n = 200 fresh 8 MB regions held in a list so no
finalizer runs in the timed loop).

Results: fresh 8 MB region create 2.01 ms — the one-pass populate holds
against the session's 2.9 ms fallocate + MAP_POPULATE; a 2^52 create
fails mizu_error_shm sub-millisecond with /dev/shm use unchanged.
mizu-bench.R: sequential rt channel 2.7 us, pool 3.0 us; pipelined
channel 428.7k rt/s, channel batch 6.34M rt/s, pool 207.2k tasks/s,
pool batch 698.6k tasks/s; payload 8 KB 6.7 us, 800 KB 343.7 us, 8 MB
3,750.9 us — the payload rows hold against the session's 345 / 3,733;
10k strings 996.3 us/rt, small named char 3.9 us/rt; 100k frame factor
column 1,214.3 us/rt, character column 10,462.6 us/rt; fan-out 92.7k
tasks/s (in-process anchor 25.2k); streaming 12.1M msg/s; map trivial f
0.2 us/elt (template 0.1, .seed 0.3, prepared 0.2), ~10us map 22.4 ms
wall (serial 81.4), skewed 50.9 ms; serialize result 8.9 us, codec
result 5.3 us; submit-plan k 0/1/4/16 73.7 / 77.1 / 97.6 / 181.5 us.
No best-known rows touched (Linux host; the table rows are M4 Pro).
Status: full suite green on macOS (4790 pass, 0 fail, 4 skips).

## 2026-10-20 — 4G sweep + read-path A/Bs (92e5717 / 94e545e, b05920a)

Two back-to-back brackets closing the bug-fix plan's bench
verification: the 4G core delta in isolation (94e545e -> b05920a), and
everything since C1's parent (92e5717 -> b05920a: M4, M11-M16, the
M-series re-vendor, 4A-4H, 4G — the M4/4H slot-header snapshots had
landed after C1's own A/B, above). Full mizu-bench.R once per arm; the
MIZL read path the 4G validators tighten got its own probe (a 200-leaf
list view's REF round-trip + element walk, 5000 iters x3 per arm), and
the four read-path rows were alternated A/B/A/B on a reduced script
(min_time 1.0) after the suite's us-scale rows scattered +-10-30%
between runs on near-identical builds.

Results: every suite row within its band on every arm; the pipelined
pool dips (568k -> 366k and 365k -> 583k across suites) re-probed as
host jitter — the isolated row runs 764k/768k/763k vs 774k/754k/743k
tasks/s. MIZL probe medians 6.06 -> 6.07 ms/iter. Read-path rows (A ->
B medians): payload 8K 3.45 -> 3.43 us; 10k strings 88.5 -> 88.3 us;
serialize result 5.01 -> 4.95 us; codec result 2.74 -> 2.63 us — B/A
0.96-1.00 throughout. A/B isolations — no best-known rows touched.
Status: full suite green on b05920a (4858 pass, 0 fail, 5 skips).

## 2026-10-21 — lever 1: extent-aligned region mappings (libmizu e58b085 -> a66a25b)

The THP-alignment lever from the zc-churn-budget plan, A/B on the second
Linux host (i7-1165G7, kernel 7.0.0-38-generic, R 4.6.1, GLIBC_TUNABLES
mmap 32 MiB / trim 128 MiB both arms, no taskset; perf counters are
kernel-blocked here, so attribution is proc.time + strace). Arms
installed side-by-side from the two vendor pins; full mizu-bench.R once
per arm back-to-back, then a reduced row set alternated A/B/A/B x3 and
targeted isolations. Mechanism acceptance first: the 8 MB row's
MADV_COLLAPSE calls flip from 116/116 EINVAL (A) to 116/116 success (B)
— 492 per 252-task run, exactly 2 per task (the two recycled regions'
inserts); the channel rows fire 2 per run (control-region create +
attach), EINVAL under A, success under B.

Results (full suite, A -> B): sequential rt channel 2.7 -> 2.7 us, pool
2.8 -> 2.8; pipelined channel 416.3k -> 438.6k rt/s, channel batch
7.14M -> 7.24M rt/s, pool 254.9k -> 269.4k tasks/s, pool batch
1.116M -> 1.134M; payload 8 KB 6.2 -> 6.2 us, 800 KB 247.5 -> 250.8 us;
10k strings 1,135.8 -> 1,062.9 us/rt (the one consistent gain — see
below), small named 4.4 -> 4.3; frame factor col 1,195.6 -> 1,173.0,
char col 11,085.3 -> 11,428.4; fan-out 137.9k -> 142.5k (in-process
anchor 51.1k -> 51.7k); streaming 14.86M -> 14.05M msg/s (-5.5%,
re-probed as jitter: isolated runs B 15.12M/13.19M span A
14.86M/14.86M); map/serialize/codec/plan rows flat (serialize 9.2 ->
9.1, codec 5.8 -> 5.6). The one regression: payload 8 MB 2,811 ->
3,177 us/task; the alternation narrows it to A 2,614/2,637/2,684 vs B
2,806/2,840/2,908 (medians +8%), and the 252-task harness to +2-3% —
horizon-dependent, and fully attributed: worker.sys +40-75 us/task and
submitter.sys +5-10 with user CPU and faults (111.8/task) flat on both
sides, i.e. the two now-real insert collapses per task (~10-20 us
steady) replacing their instant EINVALs. A C probe pins the shape: the
first collapse of a region with a live consumer mapping costs 8.4 ms
(3x the 2.6 ms solo cost), decaying over 2-3 write cycles (1,062 -> 195
-> 2.9 us means) — the plan's "~ms one-time per region" watch item
confirmed with a 3x second-mapping factor and a short decay, so
short-horizon loops show +8% and long loops +2-3%. The 10k-string gain
is the same mechanism in the other direction (the collapsed
control-region ring); no other row shows an effect past noise, matching
the plan's "second-order on this row" — the view-read fault elimination
needs a view-materializing workload the bench rows don't have, and the
create-cost win is lever 2's. A/B isolations — no best-known rows
touched (Linux host; the table rows are M4 Pro).
Status: full suite green on a66a25b (4564 pass, 0 fail, 63 skips — the
cross-language probes are absent on this box).

## 2026-10-21 — lever 3: the byte-gated boundary minor gc (R binding)

The zc-churn-budget plan's lever 3, A/B on the second Linux host
(i7-1165G7, kernel 7.0.0-38-generic, R 4.6.1, `LTO = -flto` in the
user Makevars; no taskset, perf counters kernel-blocked). Arm A the
pre-change HEAD (a2a6e69), arm B the boundary change; arms installed
side-by-side, full mizu-bench.R once per arm back-to-back, then a
reduced row set alternated A/B/A/B x3, and isolated single-row
probes. Mechanism acceptance first (real 1-worker pool, 30-rep 8 MB
identity loop, results dropped): the armed-bytes counter fires the
boundary per collect (16 MiB armed per wrap against the 8 MiB gate),
ledger_entries <= 2 on the new pool-dump field, fl_hits 28/30 —
free-list recycling replaces the stock churn fallback's SHM_RAW +
fresh creates — /dev/shm +40 MB over the run (flat per rep), median
2.0 ms/rep.

8 MB payload row (reduced harness, 30 reps/mark): A 5,787-5,924
us/task across three runs, B 2,488-2,501 across three — -57% (one B
mark at 3,107, still -47%) — with the mechanism above as the
attribution. The full-bench A1 -> B1 read 5,949 -> 2,206; the
harnesses differ on the A side (the full bench's row runs after the
other payload rows in-process), so the reduced alternation is the
controlled comparison. Every other row within band: seq rt pool
3.6/3.6/3.6 -> 3.6/4.0/3.7 us, pool batch 1.084-1.089M ->
1.068-1.126M tasks/s, 10k strings 1,073-1,244 -> 1,104-1,253 us/rt,
plan k = 0 69.8-70.0 -> 67.4-68.3 us/task, payload 800 KB 322-355 ->
322-333 us.

A/B isolations — no best-known rows touched (Linux host; the table
rows are M4 Pro).

Status: full suite green (4574 pass, 0 fail, 63 skips); the
cross-language suite green against pymizu (186 pass, 12 skips —
pyarrow absent); rchk zero findings in package code (the pre-existing
pool_submit unprotected-arguments warning fixed in this round). The
pymizu venv needed a plain reinstall first (stale non-editable copy).

## 2026-10-21 — lever 2: huge-folio region creates (libmizu a66a25b -> 438c555)

The zc-churn-budget plan's lever 2, A/B on the second Linux host
(i7-1165G7, kernel 7.0.0-38-generic, R 4.6.1, GLIBC_TUNABLES mmap
32 MiB / trim 128 MiB both arms, LTO builds; no taskset, perf counters
kernel-blocked). Arm A mizu b41d491 (vendored a66a25b), arm B the
lever-2 re-vendor (438c555); arms installed side-by-side. Mechanism
acceptance first (C probe, n = 200 held 8 MiB creates, interleaved
A/B/A/B x3): p50 2.08-2.11 -> 0.96-0.98 ms per create, minflt 2,048 ->
4.0 per create (one per 2 MiB extent), p99 2.5-2.6 -> 1.5-2.1 ms, and
the ShmemHugePages delta reads exactly +1,638,400 kB for the 200 held
regions — every region born huge. The plan's defrag risk, first probe
run on a fragmented free list (~3 h uptime): p50 3.78 / p99 10.2 ms —
bounded, comparable to stock populate's own recorded bad tail
(3.1-9.5 ms), with the steady state re-measuring sub-ms, so no
per-process latch. The long-uptime-box tail check stays open per the
risk note.

R-side (dedicated harness, A/B/A/B x2): cold fresh-pool 8 MB identity
task 3.00 -> 2.00 ms median, minflt 2,049 -> 5 — the plan's ~2 ms
rep-1 prediction; 8 MB row rep-1 3,300-3,433 -> 2,767 us/task with
steady reps unchanged (lever-3 recycling: no creates left to save);
held-views growth identical on both arms (+17 files / +54.5 MB held,
+16 / +46.5 after drop+gc) — the churn-fallback safety regime
untouched. Full mizu-bench.R once per arm back-to-back: payload 8 MB
2,472.6 -> 2,341.5 us/task (-5%), 10k strings 1,148.9 -> 1,007.6
us/rt; the two out-of-band rows (pipelined pool -13%, codec result
+26%) re-probed as jitter by a 3x alternation (A 303,981-308,531 vs B
302,764-312,213 tasks/s; codec 6.8/6.8/6.8 vs 6.8/6.9/7.5 us); every
other row within band.

A/B isolations — no best-known rows touched (Linux host; the table
rows are M4 Pro).

Status: libmizu make test + make test-integration green (new
test_region blocks: mincore all-resident incl. the non-extent tail,
and a collapse-availability-gated fault-count assert);
tools/check-exports.sh symbol-identical to exports.txt (its diff is
this box's locale sort order, present on the stock build too); mizu
suite green on 438c555 (4574 pass,
0 fail, 63 skips — the cross-language probes are absent on this box).
clang-tidy unavailable on this box (the report-only tidy leg skipped).

## 2026-10-21 — the churn budget + submitter sweep (libmizu 438c555 -> 40a5ac5)

The zc-churn-budget plan's step 4 (the budget itself), A/B on the
second Linux host (i7-1165G7, kernel 7.0.0-38-generic, R 4.6.1,
GLIBC_TUNABLES mmap 32 MiB / trim 128 MiB both arms, LTO builds; no
taskset). Arm A mizu 0d3a0c1 (vendored 438c555: levers 1-3 in, the
any-lent trigger), arm B the budget re-vendor (40a5ac5); arms
installed side-by-side. Protocol: a dedicated harness A/B/A/B x2 (the
8 MB rows, 60 held results, a pipelined held run, the submitter-churn
scenario), full mizu-bench.R once per arm, and the standout rows
re-probed A/B/A/B x3.

The Findings' regression regime is not just closed but banked: the
8 MB identity row runs 3,200/2,867 -> 1,700/1,667 us (harness marks;
the standalone probe 2,149 -> 1,311 us/task, full-bench 2,381 ->
1,339) — with lever 3 alone the any-lent trigger still flipped the
handle to the copy tiers on every task-lagged reclaim; the 256 MB
budget absorbs the transient lent. x+0 3,200/3,767 -> 2,967/2,833.
Full bench: 10k-string echo 1,134 -> 211 us/rt (probe 1,152-1,312 ->
209-213, n_itr 370-423 -> 1,342-1,356 — the whole mark now fits under
the budget), frame factor col 1,061 -> 776, char col 11,039 -> 8,036;
every other row within band.

Past-budget (60 held 8 MB x+0 results): B holds the zc window at
4.0 ms for 32 tasks (A is already degraded at 6-7); the fallback phase
runs 5-6 ms on both arms — the Findings' asymmetric-steady-state
regression is gone (degraded B == steady A, and B banked the 32-task
fast window); /dev/shm pins bounded at 311 MB against the budget
(the no-churn disaster pinned +1,600 MB at 200 held; the budget is the
design's cap for exactly this), and the drop+gc recovery returns views
on both arms (B 3.4-4.8 ms/task, shm back to 55 MB). Pipelined 40
held: 3.63-3.68 vs 3.52-3.55 ms/task — parity, so the pipelined
view-holding regime does not regress and B (the shared churn word)
stays parked.

Submitter sweep (worker-stashed arg views): A churns at task ~2
(task32 FALSE) and never lifts after release+gc (FALSE x5 — the
pre-existing starvation gap: its SHM_RAW pops hit the free list, so no
miss-sweep ever runs); B holds zc to task ~34 (task32 TRUE), crosses
(task40 FALSE), and lifts on the first reclaim (TRUE x5). shm peak
287 MB, bounded.

Free-list caps re-derivation (owed to this round): no change forced —
the lever-3 8 MB gate drips 1-2 regions per firing against the
2-per-class cap, and regions > 16 MB drip 1 by construction. Left
as-is.

A/B isolations — no best-known rows touched (Linux host; the table
rows are M4 Pro).

Status: libmizu make test + make test-integration green (test_spill
gains the budget blocks — below-budget no-churn, past-budget set and
clear, a reclaims-nothing sweep keeps churn, the overflow drop keeps
its bytes counted; test_pool gains submit_sweep_churn). mizu suite
green (4589 pass, 0 fail, 61 skips): the two pool churn tests merged
and reworked to cross the budget (40 held 8 MB x+0 results), the
channel churn test likewise (40 held 8 MB sends), and the two
skip_on_os("linux") zc tests unskipped — their lent volumes stay far
under the budget now.

## 2026-10-22 — spill free-list caps for GC-batched recycling, the channel gc boundary, and the collapse latch (libmizu 5404135)

The 800 KB pool payload row was the suite's smallest relative gain vs
mirai (~2x where 8 KB is ~15x and 8 MB ~7x). The first read — releases
GC-gated, so regions never recycle mid-loop — was wrong on the release
side: releases already ride the zc gc-boundary's minor GCs
(mizu_zc_gc_boundary, charged per wrapped view byte). The starvation
point was downstream: the free list's per-size-class cap of 2 destroyed
every reclaimed region beyond the second at insert (~78% of reclaims
munmap+unlinked immediately), so uniform-size traffic recycled through
exactly two slots (~22% reuse) and every other iteration paid a fresh
region create (POPULATE ~230 us + destroy ~70 us on this host). Fix:
FL_MAX 16 -> 32, FL_CLASS 2 -> 16, FL_BYTES 32 -> 64 MB, LEDGER_MAX
64 -> 256 (the knee is low because releases arrive steadily). Also two
follow-ons: the gc boundary now fires on the channel recv verbs too
(verbs_channel.c — the pool collects already had it; the channel had
the same starvation plus no release cadence), and the free-list-insert
MADV_COLLAPSE is skipped once a mapping is known huge (a collapsed
latch on mizu_shm_s — the re-scan was ~67 us per reuse cycle at
>= 2 MiB).

Results (Linux x86_64 host, fresh-process best-of; the best-known table
is M4 Pro, so no rows touched): pool payload 800,000 B default slots
(SHM_VEC) ~400 -> 117 us/task (spill reuse 22% -> 99%); the bench's
sized-slot row 445.8 -> 244.3; channel echo 800 KB default slots 365
-> 130 us/rt; pool payload 8,000,000 B band 1000-1667 -> 900-967 us
(the collapse latch; zero ftruncate in steady state); 10k strings
channel 206.4 -> 112.1 us/rt; 100k frame 5k-level character column
7224.4 -> 1727.2 us/rt. Hot rows flat (sequential rt 2.8/2.8 us,
payload 8 KB 6.1 -> 6.0, streaming, map rows). The four rows initially
reading worse (pipelined pool -17%, channel batch -7%, 8 MB +13%,
factor column +13%) were host-band noise on repeat runs. mizu's ratio
vs mirai is now ~15x / ~7.8x / ~7.2x across 8 KB / 800 KB / 8 MB — the
800 KB dip is gone.

Status: libmizu make test green (test_spill rescaled to the new caps +
the collapsed-latch block). mizu suite green (4651 pass, 0 fail, 70
skips): test-spill-reuse.R rescaled likewise. pymizu vendored at the
same pin: 919 pass; two crosslang tests surfaced a pre-existing
producer-exit race (fire-and-exit R sources unlinking a payload's
region before the consumer resolves it — 20% on the pre-change build,
5% post), fixed by rendezvous waits on the 10 region-backed sources,
not by the transport.

## 2026-10-22 — M4 Pro re-record on the day's builds; the stage-guard regression found and fixed [(53382e7, 5cb1aca, 91fb808, de2875f)]

First M4 Pro record carrying the day's work: libmizu 5404135 (spill
free-list caps for GC-batched recycling + the collapse latch — the
Linux read is the entry above) and 42062e6 (the in-flight drain wait
at channel destroy), the channel recv-side zc gc boundary (53382e7),
and the MIZU_CAP_LABELED interop rewrite (91fb808). The bench edit
rides along: the 800 KB pool payload drops its sized slots (was
slot_size = 1 MiB, the in-slot RAWVEC copy floor) and spills on
default slots like the 8 MB row, so the row's date flips definition
per the table convention.

The re-record surfaced depressed channel batch / streaming rows
(14.7-15.2M rt/s / 26-29M msg/s against the 20.4M / 42.0M bests) that
did not recover as the host quieted — the pool fire-rate rows' lows
meanwhile matched their recorded bimodal low modes (host noise, the
10-03 record's precedent). A bisect over the window since 2943a7d
(the last in-band M4 Pro reading, 10-03 — re-verified in-band on the
day) landed on b9836cf (W2, 10-14): its stage guard ran one
R_ExecWithCleanup per staged element, ~12 ns/msg at the streaming
row's ~24 ns/msg budget — -28% streaming, -17% batch (A/B/A/B on the
boundary: 40.9-42.0M -> 29.7-30.2M msg/s, 18.1-18.5M -> 15.0-15.3M
rt/s). Invisible on the pool's us-scale tasks, which is all W2's
"guard costs nothing" check measured. Fixed the same day: the guard
keeps per-invocation generations with plain arm/disarm, and every
consult-root arms afresh — the five public 'I' writer entries self-arm
when no generation is live, covering the off-hook consultors (the map
descriptor veneer, the foreign ERR publish) — so a raise-orphaned
generation is never consulted and no unwind boundary runs anywhere.

Results (post-fix build): streaming 40.2M probe / 37.4M full-bench
msg/s, channel batch 17.5M / 18.1M rt/s — in band against the
pre-regression 40.9-43.7M / 18.1-18.5M; the 42.0M / 20.4M bests
stand, reachable again. Pool pipelined 641.8k (the high mode — the
guard taxed pool stages too). The re-record itself (the guard never
touched these rows): sequential rt channel 1.0 / pool 1.5 us; payload
8 KB 2.8 -> 2.7 us; payload 800 KB 75.5 -> 47.3 us/task — the new
default-slot SHM_VEC definition beating the old sized-slot copy
floor's record (the Linux entry above is the same shape: ~400 -> 117
there); payload 8 MB 415.6 -> 262.8 us/task; 10k strings 72.7-74.7
us/rt (band ~86-88 — the recv-side gc boundary keeps the echo peer's
views releasing on cadence); small named char 1.4-1.5; 100k frame
5k-level factor 220.6-290.6 / character 700.2-734.2 us/rt; fan-out
378k -> 383k tasks/s (mirai-bench's best-of-3 read 400.0k); map
trivial f 0.1 us/elt across the variants; map ~10us 5.3 -> 5.2 ms
wall; map skewed 11.1 ms (ties the best); serialize 3.3 us (ties);
codec 2.1 -> 2.0 us; submit plan k = 0/1/4/16 37.2 / 39.5 / 43.7 /
61.0 us/task — all four swap (was 62.1 / 64.6 / 68.1 / 84.2, the
10-04 records; the day's 'I'-stream rewrite the candidate mechanism,
no A/B taken).

vs mirai (best-of-3, tick-quantized): 800 KB 35.0 vs 1,690-1,745
us/task (~48x — the dip the sized-slot row hid is gone on this host
too); 8 KB 5.0 vs 68-106 (~14-21x); 8 MB 400.0 vs 18,000-18,100
(~45x); fan-out 400.0k vs 9.6k/23.8k tasks/s; map trivial 0.1 vs
43.3/104.0 us/elt; ~10us map 5.0 vs 82-208 ms wall. Reference rows in
their incumbent bands (nanonext 32.0 us/rt vs the 31.7 baseline; mirai
sequential 66-105 us vs 63-124).

Best-known swaps (11 rows, all 2026-10-22): payload 8,000 B 2.7,
payload 800,000 B 47.3, payload 8,000,000 B 262.8, fan-out 383k,
map ~10us 5.2 ms, codec-tier 2.0, plan k = 0/1/4/16 37.2 / 39.5 /
43.7 / 61.0. Cross-language rows not re-run — their benches are
unchanged and the MIZU_CAP_LABELED commit carries its own corpus
tests; the 2026-10-04 rows stand.

Status: full suite green on the fixed build (4978 pass, 0 fail, 5
skips — the pymizu venv on PATH, so the cross-language legs ran).

## 2026-10-23 — closing the residual: the batch-mode stage scope

Yesterday's fix left ~5% on the channel batch rows (38.2M msg/s vs the
pre-guard parent's 40.7M anchor — the anchor is stable across both
days). Decomposition: the guard's remaining per-element work was the
whole residual — a stub build with the mark-list reset kept but the
arm/disarm removed matched the parent exactly. Two changes close it:
the reset gates on nmarks > 0 (an empty list is one static load), and
the two batch veneers (channel send_batch, pool submit_batch) now hold
one armed generation across the element loop — the elements stay
anchored by the veneer's argument throughout — behind one
R_ExecWithCleanup per verb (~12 ns per 4096 elements, amortized to
nothing). The guard skips its per-element arm/disarm in batch mode;
any stage outside an opted-in verb self-arms per invocation, so a
forgotten verb degrades, never goes unsafe. The Arrow decline
re-entries push/pop a temporary scope (mizu_stage_temp_begin/end), so
a converted value's verdict keys die with it instead of riding the
batch's generation.

The follow-up audit also caught a hole in yesterday's conditional
self-arm: a writer entry reached while a raise-orphaned generation was
live would have consulted it (ix_qc_enter no-opped on any armed
generation, unable to tell live from orphaned). The writer entries now
split: in-hook callers (payload.c, the channel stage's condition send,
the pool spec branch) use _scoped forms riding the caller's
generation; off-hook callers (the veneers, the foreign ERR publish,
pool_result_gate) arm unconditionally, so an orphan is always
replaced, never consulted.

Results (quiet-window A/B, 6 alternations + a same-day preguard
control): committed fix 38.6M -> batch-mode 39.5M msg/s streaming
(anchor 40.7M both days); channel batch 17.4M -> 17.9M rt/s (anchor
18.5-18.6M). The remaining ~3% vs the anchor is two static loads per
element (the batch-mode check + the reset gate) — the per-element
safety floor; the anchor itself spread 40.3-41.4M across the two
days. Full bench in band: channel batch 18.56M, streaming 37.8M,
every other row at yesterday's records. The 42.0M / 20.4M bests
stand; the build is within ~3% of the pre-regression code path.

Status: full suite green (4978 pass, 0 fail, 5 skips — the pymizu
venv on PATH, so the cross-language legs ran).

## 2026-10-25 — notify endpoints land: the event-driven surface [(libmizu 65ed8c2 + R binding, working tree)]

Phase 1+2 of the event-driven promises plan: the vendored core gains the
edge-triggered notify endpoints (the bit rides the word the publish
already loads), and the R binding grows the later/promises surface
(mizu_recv_async, mizu_recv_handler, as.promise methods,
mizu_collect_all/any_async, the handle-first mizu_map). The bench gate:
the unarmed publish path must be flat, the armed-promise row within
band, and the batch-handler / collect_all_async rows against their sync
anchors. The handler row sizes the drain quantum (2048 messages or 1 ms
per callback; 256 was 2.2x off the sync batch row, 2048 lands it).

Results (A/B against the previous build, HEAD worktree in a temp lib):
sequential rt 1.0 us/rt channel, 1.8 us/task pool — both flat;
pipelined channel 943k -> 955k-981k rt/s (3 runs) — flat; channel batch
18.4M -> 18.3M — flat; pipelined pool 398k -> 320k and pool batch 2.25M
-> 1.80M bench medians, but the one-shot spread (HEAD 411k-924k and
1.7M-4.4M over 3 reps; new 571k-632k and 1.3M-1.8M) overlaps fully —
load noise, not a regression. New rows: armed channel (a recv promise
pending the whole run) 1.16M rt/s — at/above the unarmed band, one
endpoint signal total as designed; channel batch handler 16.0M msg/s =
0.87x the sync batch row (18.3M) — inside the ~1.2x target;
mizu_collect_all_async 2.54M tasks/s vs the sync collect_all row at
1.80M — within band. All other rows in band (payloads, fan-out,
streaming, map, serialize, plan).

Status: full suite green (5200 pass, 0 fail, 6 skips).

## 2026-10-27 — async rows join the report suite (tests/testthat/test-benchmark.R)

The section 2b event-driven rows move into the report-only test suite
ahead of the async audit's efficiency work, joined by three regime
rows: deep FIFO (4,000 pending recv promises, one burst — the
pending-queue and per-message promise cost at depth), many waiters (64
concurrent collections — the per-wake scan on the submitter's one
coalescing word), and per-task promises (one enrollment per task — the
enroll/settle cost the batch forms amortize). First recordings on a
non-quiet machine; the rows exist for A/B, so deltas from here matter
more than the absolutes.

Results: channel armed 599k rt/s; channel batch handler 11.1M msg/s;
collect_all_async 2.44M tasks/s; deep FIFO 761 us/msg; many waiters
99.2k tasks/s; per-task promises 3.9k tasks/s. The three regime rows
sit far below the batched forms as the audit predicts (the O(n^2) FIFO
churn, the per-wake all-waiter scans, and per-enrollment arms).

Status: the two new tests pass (8/8); suite not re-run (test-only
change).

## 2026-10-27 — async efficiency: linked-list registries, C verdicts, exact-size bursts [(with libmizu notify efficiency)]

The async audit's R-side findings land: the channel FIFO and the pool
waiter registry are env linked lists (a list paid a vector copy per op
either end — the deep-FIFO burst was O(n^2)); the drain's per-waiter
probe is one C verdict call (mizu_pool_waiter_terminal, early-exit
any/all) where the old form allocated a logical per task per wake and
re-scanned it in R; recv_batch(timeout = 0) sizes its out vector from
the new core mizu_channel_avail accessor (no cap-size allocation +
shrink per burst); the drain hoists mode/batch out of the per-item
loop, calls the .Call recv directly, and reads the clock every 64
items; the requireNamespace probes memoize. Core-side (vendored): the
pool publish's coalescing-word RMW is load-gated, re-arms of a
continuously armed word skip the endpoint drain, and the tx opens
before the edge is claimed (a lost-wake hole on open failure).

Results (A/B vs the morning's first recordings, best of the day's
runs): deep FIFO 761 -> 324-394 us/msg (~2x); per-task promises 3.9k ->
7.3-7.9k tasks/s (~2x); batch handler 11.1M -> 14.3M msg/s (+29%);
many waiters 99.2k -> 110-142k tasks/s; collect_all_async 2.44M ->
2.3-3.3M tasks/s (band, flat); channel armed 599k -> 585-633k rt/s
(flat, as designed — one signal per run).

Status: full suite green (5340 pass, 0 fail, 10 skips — fork/system-
library/macOS skips; the pymizu venv on PATH ran the cross-language
legs).

## 2026-10-27 — pool waiter registry moved to C; blocking collects' refusal check moved into the veneers

The phase-1 item of the weak-rows plan, re-cut after measuring the
first (R-side) cut. Profiling the per-task promises row with the
promises machinery factored out put mizu's own async cost at 22-31
us/task against 1.5 us for sync submit+collect — and the cost was not
the announce/arm (removing both entirely saved nothing; Rprof had
attributed 72% to pool_announce_arm, an artifact of ITIMER_PROF
counting the listener thread's CPU during the arm's concurrent
publish/signal) but the R-level drain per settle: the per-pass snapshot
vectors, the tryCatch around the collect, mono_time, the unlink
bookkeeping, the env index (hex key per task), the disarm/arm pair.
The registry became a single arm/disarm cycle per task because the
worker publishes a 1L task before as.promise() runs, so the enrollment
drain settled it and emptied the registry 1995 times in 2000 — the
"arm on transition" never engaged.

The re-cut: the waiter registry lives in C (notify_r.c, hung off the
handle's mizu_r_notify record): entries cache their task words, the
waiter envs ride a parallel preserved VECSXP, and four verbs replace the
R linked list + index + snapshot builder + armed flag. Enrollment is one
.Call fusing the conflict check (linear; a throwaway open-addressing
table past the T*W crossover), the announces, the arm, and the terminal
probe — probing terminal FIRST, so an already-published task settles
from the call site with no announce-arm cycle at all. The per-wake scan
is one .Call returning up to 64 settled waiters (unlinked, announces
withdrawn, re-armed ahead of the probe), the drain loop ending on the
empty scan. The task kind settles through mizu_pool_collect_settle
(every failure boxed: no tryCatch per task). And the blocking collect
verbs' "one consumer per result" refusal moved from an R-level .Call
per task (notify_check_collectable: mizu_task_notify_state + is.null,
~0.6 us of the pipelined row's 1.5 us/task; a 10k-task loop ahead of
every collect_all / collect_any) into the C veneers (mizu_r_notify_refuse
after the unpack: one pointer test when nothing is enrolled).
mizu_pool_tasks_terminal / mizu_pool_waiter_terminal / mizu_task_key /
mizu_pool_task_notify / mizu_pool_notify_arm|disarm / mizu_task_notify_state
/ mizu_map_runners_terminal are gone from the .Call table.

Results (A/B vs the committed baseline eb2628e, standalone harness,
medians of 5-7 with best in parentheses; tight run_now(0.001) pump):
pool rt (submit+collect, k=10) 694k -> 840k tasks/s (+21%; best 787k ->
885k); pool collect_all (10k x 10) 1.79M -> 3.45M (+93%; best 2.27M ->
4.55M) — the per-task refusal loop was half the row; pool collect_any
(10k x 10) 1.18M -> 3.03M; collect_all_async 2.33M -> 2.86M (+23%; best
2.78M -> 3.33M); many waiters (64 x 31) 248k -> 248k (best 248k ->
283k); per-task promises (2000) 5.8k -> 6.3k tasks/s (+8%; best 7.6k ->
9.1k) — the row's floor stays the promises package (promise() + resolve
measures 52 us, R6 construction; later() itself is a flat 5 us at any
queue depth); deep FIFO 160 -> 144 us/msg (noise; the channel path is
untouched). Host is a laptop under load — ratios decide.

Suite-row reads on the final build (taken while the rchk container ran
alongside under amd64 emulation, so not comparable with the table):
pipelined 1.0M, collect_all 2.0M, collect_all_async 2.78M, many waiters
198k, per-task 3.8k, deep FIFO 354 us/msg.

Status: full suite green (5382 pass, 0 fail, 7 skips; the pymizu venv on
PATH ran the cross-language legs); rchk clean on a tree copy (the one
report is the pre-existing [UP] note on mizu_channel_recv_call).

## 2026-10-27 — mizu_task_handler: the promise-free per-task consumer

The per-task promises row's floor was the promises package itself
(promise() + resolve ~52 us of R6 construction per task). The pool-side
mirror of the channel's mizu_recv_handler(): mizu_task_handler(task, fn)
enrolls a `taskh` waiter on the phase-1 C registry — the task verdict
with its own mode only so refusal messages name it — and settles through
the existing boxed collect: fn receives the value, or the classed
condition the sync verb would have raised. No promise, no then, one fn
call per task. C delta: one registry mode (a label, a case fallthrough,
the mode bound).

Measured (2000 tasks, 1 worker, medians of 7, standalone): per-task
handlers 200k tasks/s vs per-task promises 12.9k on the same build —
15.5x; the suite's weakest ratio closes from ~1/80th of the pipelined
row to ~1/5th of it. Suite row reads (shared pool history): handlers
90.9k vs promises 10.3k (8.8x).
