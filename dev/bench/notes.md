# Recorded benchmark notes

Dated performance records for sora. The live reports that print against
these baselines are `tests/testthat/test-benchmark.R` (eyeball them in CI
logs); `dev/bench/sora-mirai.R` runs the matched-scenario mirai
comparison. Nothing here is asserted against — runner timing is too
variable for thresholds. Append new dated outcomes at the bottom.

All records are M4 Pro, R 4.6.1 unless marked; A/B ratios are the valid
comparisons, absolutes drift with host load. proc.time ticks at ~1 ms on
this host, so timed intervals are kept >> 1 ms by looping.

## Incumbent baselines (2026-07)

- nanonext `ipc://` pair: 31.7 us per round trip (~31.5k RT/s)
- mirai local dispatch: 63-124 us per task
- target regime: >100k small messages/s sustained
- `sora_map` overhead regime: serial lapply ~0.2 us/element; per-element
  `sora_submit`/`sora_collect` ~4 us; `mirai_map` ~63-124 us/element

## Durable calibration facts

- Small-op floor is the R-interpreter boundary: >85% of a pool round
  trip, the closure call alone ~40% of it. The batch verbs and
  `sora_map` are the answers for throughput-bound callers.
- Park/wake pair: ~8 us macOS, ~16 us virtualized Linux (isolated by
  pacing an echo peer, stock vs spin=TRUE).
- Zero-copy floors: SHM_VEC loses to the copy tiers below ~8-16 KiB
  (ARENA ~1.9-2.2 us at 256 B-4 KiB vs SHM_VEC ~2.3-2.9 us).
  `SORA_ZC_FLOOR` (pool) sits at the 16-64 KiB band;
  `SORA_ZC_FLOOR_RAW` = 256 KiB (channel arena; lifts under the
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
