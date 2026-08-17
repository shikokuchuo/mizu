# Recorded benchmark notes

Dated performance records for sora. The live reports that print against
these baselines are `tests/testthat/test-benchmark.R` (eyeball them in CI
logs); `dev/bench/sora-mirai.R` runs the matched-scenario mirai
comparison. Nothing here is asserted against — runner timing is too
variable for thresholds. Append new dated outcomes at the bottom.

## Incumbent baselines (M4 Pro, 2026-07)

- nanonext `ipc://` pair: 31.7 us per round trip (~31.5k RT/s)
- mirai local dispatch: 63-124 us per task
- target regime: >100k small messages/s sustained
- `sora_map` overhead regime: serial lapply ~0.2 us/element; per-element
  `sora_submit`/`sora_collect` ~4 us; `mirai_map` ~63-124 us/element (one
  mirai task per element)

## Zero-copy phases (M4 Pro, 2026-08-11/12, R 4.6.1)

Phase 0 measured the memcpy-bound regime as the "before" column for
SHM_VEC / REF (see `.claude/zero-copy-plan.md`; proc.time ticks at ~1 ms
on this host, so every timed interval was kept >> 1 ms by looping). Before
Phase 1, a large atomic vector fell from RAWVEC (one memcpy each way) to
ARENA (serialize + unserialize) or SHM_RAW (count pass + serialize + open
+ full unserialize); the receive-side copy/parse is what Phase 1 removed.

- channel round trip: 0.10 ms at 1 MiB (ARENA), 0.72 at 8 MiB, 19.0 at
  64 MiB
- pool result: 0.18 ms at 1 MiB, 1.22 at 8 MiB, 16.0 at 64 MiB
- `sora_map` 64 MiB template: 0.69 s (0.08 us/element)
- ALTREP `1:2^27` round trip ~3 us (133 B stream — never materializes)
- 100 MiB matrix round trip 36 ms; the 3-of-100-column read then 0.74 ms
- held 8 x 16 MiB results: 8 spills, 7 reused, 1 fresh region (held R
  objects pinned nothing pre-views)
- 32 MiB named vector round trip 725 ms vs 15 ms bare: the attrs blob is
  ~97% of cost; Phase 1's eager attrs parse keeps a share of it

Profile split (macOS sample, 64 MiB payloads): for flat atomic vectors the
serialize pass IS the send memcpy (`mori_write_fixed` -> memmove) and
unserialize is allocVector + memcpy (`mori_read_bytes` -> memmove) —
object-graph parse is noise, and region create vanishes after warm-up
(free list). `sora_map`'s stage + gather memcpys are ~1.5% of trivial-f
wall time (per-element eval dominates); the gather view matters for
reduce-shaped maps.

Phase 1 outcome (2026-08-12, same host): the receive side of a large
vector is an ALTREP wrap (~0.45 us), and the echo peer's re-send rides REF
(zero bytes move). Channel 64 MiB round trip 19.0 -> 3.00 ms (6.3x); pool
64 MiB result 16.0 -> 10.25 ms (the residual is the worker's own
allocVector + fill + the one send-side memcpy). The full-sweep reads
(`identical()` on the view) fault pages on demand under the lazy mapping —
no populate hint added: the delta is decisively above noise without one.
The held-results case now reads 8 spills / 0 reused / 8 fresh: held views
pin their regions in the lent-region ledger by design.

Phase 3 (2026-08-12): the template case gained a `.collect = "view"` arm —
the result wraps the output area as an ALTREP view (no gather memcpy) and
the check reduces it (sum), so the read faults pages on demand. A view x
(a received channel/pool view) maps by reference: the descriptor carries
its identifier and workers read elements off the shared pages. Measured:
0.688 s eager vs 0.666 s view + reduce — the gather memcpy is ~3% of
trivial-f wall time, as Phase 0 measured; the win grows with the gather's
share (reduce-shaped maps).

Floor data (in-process one-way, steady state): ARENA ~1.9-2.2 us at
256 B-4 KiB; the SHM_RAW send half alone is ~1.8-2.4 us there, so SHM_VEC
(send + ~0.45 us wrap) lands ~2.3-2.9 us — it LOSES to ARENA below
~8-16 KiB even before region churn (fresh create ~6-7 us vs arena flat).
The crossover sat in the 16-64 KiB band, warranting the internal floor
constant (`SORA_ZC_FLOOR`).

## 2026-08-17: immediate payload kinds (NIL, STR1)

Same host, Sys.time-timed A/B against the previous build.

`SORA_KIND_NIL` stages NULL as an immediate (no serialize pass, no receive
allocation) and the self-contained kinds (NIL, RAWVEC) skip the keeper
pin; `sora_submit` assembles its payload in C. Pool round trip 2.4 -> 1.5
us/task, channel NULL round trip 3.1 -> 1.3 us, one-way streaming
30 -> 34-40M msg/s, pool pipelined 0.57 -> 0.69M tasks/s.

`SORA_KIND_STR1` frames a length-1 string as bytes + cetype
(encoding-faithful, NA as an aux sentinel) — channel string round trip
2.6 -> 1.4 us, at the RAWVEC scalar floor.

## 2026-08-17: RAWSPILL tier, keeperless-wake gate, spin clock strides

Same host, back-to-back builds, fresh peers per rep.

`SORA_KIND_RAWSPILL` moves attribute-free atomic vectors past the inline
budget as bare bytes — a channel arena chunk or a pool spill region —
skipping the serialize pass and the receive-side parse: channel round trip
800 B 3.7 -> 1.3 us, 8 KB 5.1 -> 3.0 us (the 240 B - 32 KiB band between
RAWVEC and the zc floor; above the floor SHM_VEC still claimed them at
this point).

Collect no longer wakes the producing worker for keeperless result kinds
(NIL/RAWVEC/STR1 create no keeper record to reap): the per-collect fence +
parked-mask load + ulock_wake (12% of controller CPU under
fire-then-collect, one syscall per collect with the worker parked) is
gone — pool pipelined 0.67M -> 0.84M tasks/s (1 worker), 0.41M -> 0.83M
(4 workers).

The pre-park spins read the clock every 64 cheap-predicate iterations
instead of 8 (the reads were ~1/3 of host CPU in a channel round trip;
they overlap the wait, so latency moves little) and `sora_now()` calls
`mach_absolute_time` directly on macOS. Scalar round trips and one-way
streaming are unchanged (already at the R-boundary floor).

Rejected: skipping the drain-empty head publish for keeperless drains —
the publish is also the producer's space-reclaim signal, and gating it
shrunk effective ring capacity by up to K-1 phantom slots
(test-ring.R's batch-verbs test is the authority).

## 2026-08-17: raw-vector zc floor recalibration (SORA_ZC_FLOOR_RAW)

With the copy tier memcpy-cheap, the channel's raw-vector crossover to
SHM_VEC moved from the 16-64 KiB band to the 256-512 KiB band (echo round
trips, zc's friendliest pattern — the REF return is free):

| payload | SHM_VEC | RAWSPILL (arena) |
|---|---|---|
| 64 KiB  | 16.7 us | 7.8 us  |
| 128 KiB | 22.0 us | 19.4 us |
| 256 KiB | 37.5 us | 26.3 us |
| 512 KiB | 50.2 us | 67.1 us |
| 1 MiB   | 71.0 us | 115.7 us |

`SORA_ZC_FLOOR_RAW` = 256 KiB gates the channel's arena claim of raw
vectors (and lifts entirely under the Linux-only churn signal — the arena
is churn-immune). The pool keeps `SORA_ZC_FLOOR`: its raw spill is a
region too — region-raw never beat the view at 64 KiB+ (26.4 vs 21.5 us),
ties to 1 MiB.

## 2026-08-17: task handles pack into the extptr address

(idx, seq) pack into the extptr address itself (24 + 40 bits; the address
is opaque to R, never dereferenced), removing the per-task malloc/free.
Assessment of the freelist idea: the C struct pair measured only ~1% of
controller CPU — the xzm_free profile share is R's own heap machinery, not
task structs — and a freelist would have been GC-cadence-bounded on top.
The remaining ~25% alloc/GC slice is R-heap object churn per task (payload
list, extptr cell, class cons, finalizer cell; the worker's unserialized
task list), the task model's inherent object count — no handle-side change
touches it. Measured effect of the pack: below the run-to-run noise floor
(pipelined 0.84M -> 0.86M tasks/s); kept because it is strictly less code
on the hot path (no allocation-failure path, nothing to free), not for the
number.

## 2026-08-17: large-payload reports budgeted to a 1 GB /dev/shm

The large-payload cases in `test-benchmark.R` are now sized to fit
docker's default 1 GB /dev/shm, so `R CMD check --use-valgrind` runs in a
`--shm-size=1g` container. Received/collected views release their spill
regions only via finalizers — GC-gated in every holder process, and an
echo peer releases on *its* GC, not ours — so every iteration of a timing
loop used to stage a fresh region for the whole phase: the suite peaked at
~1.8 GB. Now: `gc()` between reps (outside the timed sections), per-rep
iterations capped at ~32 MiB of payload, the channel case tops at 32 MiB
(was 64), the template map stages 32 MiB (was 64), and the attributed case
runs single iterations. Channel/map numbers printed from this date on are
not directly comparable with the 64 MiB records above. Suite peak
/dev/shm use after the change: ~440 MiB (was ~1.8 GB).

## 2026-08-17: the compact codec (src/codec.c) ahead of R_Serialize

Same host, Sys.time-timed A/B against the previous build (min of 5 x
4-5k iterations).

The finding that mattered: R_Serialize allocates a VECSXP(1099)
ref-tracking hash table on EVERY call (R_Unserialize a VECSXP(128) read
table) — several hundred ns plus ~10 KB of immediate garbage per payload
per side, the largest remaining cost of a small pool task or channel
message once the immediate kinds landed. R's writer never references
vectors or pairlist nodes through that table (only symbols, environments,
pointers), so a writer that never emits references loses nothing for the
hot-path subset.

The codec frames NULL, symbols, atomic vectors (attributes included),
strings, list/vector trees, and calls as a self-describing stream (first
byte SORA_CODEC_MAGIC where an R binary stream carries 'B' — readers
dispatch on it, the slot header is untouched). It rejects ALTREP anywhere
in the graph, so no mori identifier can ride along: codec streams pin NO
keeper (the NIL/RAWVEC/STR1 discipline extended to the serialize tiers).
Closures, environments, S4, ALTREP, attributed pairlist nodes
(R_getAttributes synthesizes a spurious "names" from tail tags there),
and over-deep graphs fall back to R_Serialize exactly as before. The
gp/LEVELS word is not carried (identical() never compares it).

Measured (base -> codec):

| case | base | codec |
|---|---|---|
| channel round trip (0L, RAWVEC) | 1.00 us | 0.80 us |
| channel round trip (list(1, "a")) | 2.60 us | 1.20 us |
| pool round trip (NULL task) | 1.50 us | 0.75 us |
| pool round trip (sum(x), x = runif(10)) | 1.75 us | 1.25 us |
| pool pipelined | 1.0M tasks/s | 2.0M tasks/s |

Two companion changes, same theme of not paying for what the payload
doesn't need: the channel's per-verb keeper reap is now gated on
outstanding keepers / arena bytes / lent regions (keeperless traffic
skips the cross-core head load entirely), and the worker skips the
per-task fresh environment when the task expression is a value type
(eval is the identity there — no env, no arg binding). The pool collect
keeperless-wake gate now keys on sora_keeperless() (immediates + inline
codec streams), not the three immediate kinds alone.

After the codec, profiles of the pool round trip and pipelined loops are
>85% R-interpreter boundary (the closures, substitute, list(...) of the
API itself); the sora C share is single-digit percent. The remaining
floor is the call boundary — the batch verbs and sora_map are the
answers for throughput-bound callers.
