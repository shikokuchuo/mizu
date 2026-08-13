# Prepared-map rows: prepare + k runs against k plain kio_map calls, in
# back-to-back and idle-gap variants. Report-only, kioto-only — run
# manually on a quiet machine like map-gate.R; the numbers land in the PR
# description. The two deltas gate the contingent follow-ups, not any
# merge:
#
# - back-to-back delta: prices the per-run k = 1 sizing ramp (each run's
#   first-call CLAIM CAS resets the learned batch size) — gates the
#   learned-k carry-over follow-up. Prior: ~log2(cap) extra transitions
#   per runner, single-digit us against a per-run floor of tens of us of
#   submit + collect — expect skip.
# - idle-gap delta: prices the per-worker re-attach + descriptor
#   unserialize after the pre-park idle sweep drops the worker's map ctx
#   cache — gates the prepared-ctx sweep-pinning follow-up. Prior: ~6 us
#   per worker for a trivial f descriptor, scaling with the descriptor's
#   serialized size — matters only for fat closures.
#
# Measured 2026-08-04 (M4 Pro, W = 4, n = 1e4 trivial f): back-to-back
# deltas are inside run noise (~3.2-3.4 ms/run both ways — the ~50 us of
# staging saved is invisible against the map itself), and idle-gap runs
# are dominated by multi-ms wake-from-park latency with fully overlapping
# distributions. Both follow-ups gate to SKIP at this shape, as their
# priors expected; re-run with a fat closure before revisiting the
# sweep-pinning entry.
#
#   Rscript dev/bench/map-prepared.R

library(kioto)

REPS <- 5L
K <- 10L
now <- function() as.numeric(Sys.time())
best <- function(f) min(vapply(seq_len(REPS), function(i) f(), 0))

p <- kio_pool(n_workers = 4L)
n <- 10000L
x <- seq_len(n) + 0                 # materialized: the region path
f <- function(i) i + 1

invisible(kio_map(p, x, f))         # warm pool and worker caches

cat(sprintf("%d x kio_map, back-to-back:      %8.1f us/run\n", K,
            1e6 / K * best(function() {
              t0 <- now()
              for (j in seq_len(K)) kio_map(p, x, f)
              now() - t0
            })))

pm <- kio_map_prepare(p, x, f)
invisible(kio_map_run(pm))
cat(sprintf("%d x kio_map_run, back-to-back:  %8.1f us/run\n", K,
            1e6 / K * best(function() {
              t0 <- now()
              for (j in seq_len(K)) kio_map_run(pm)
              now() - t0
            })))

# idle-gap variant: every worker parks between runs and its pre-park
# sweep drops the map-context cache, so each prepared run pays one
# re-attach + descriptor unserialize per worker (no restage) — plain
# kio_map pays a fresh region + full ctx miss regardless
idle_gap <- function(run) {
  best(function() {
    while (kio_pool_status(p)[["parked"]] < 4L) Sys.sleep(0.01)
    Sys.sleep(0.05)                 # past the pre-park sweep
    t0 <- now()
    run()
    now() - t0
  })
}
cat(sprintf("kio_map, idle-gap:               %8.1f us/run\n",
            1e6 * idle_gap(function() kio_map(p, x, f))))
cat(sprintf("kio_map_run, idle-gap:           %8.1f us/run\n",
            1e6 * idle_gap(function() kio_map_run(pm))))

# phase B swap row: run over a replacement same-shape 8 MB x — an
# in-place memcpy over the warm region — against restaging it fresh per
# run. Measured 2026-08-04 (M4 Pro, quiet): the swap itself costs
# ~0.2 ms and the full stage ~0.4 ms, both noise against the ~250 ms the
# run's 1e6 elements cost regardless — the deltas at this shape are
# inside run variance, exactly the plan's prior. Phase B is kept for its
# semantics (iterate over new same-shape data on one handle, no restage
# bookkeeping), not for a measured win. An earlier ~105 ms/run "restage
# cost" was contaminated by a concurrent build on the same machine;
# distrust any run of this file that wasn't alone on the box.
xb <- runif(1e6)                    # 8 MB
g <- function(v) v > 0.5
invisible(kio_map(p, xb, g, .template = logical(1)))
pmb <- kio_map_prepare(p, xb, g, .template = logical(1))
invisible(kio_map_run(pmb))
cat(sprintf("8MB x, restage + run:            %8.1f us/run\n",
            1e6 * best(function() {
              t0 <- now()
              kio_map(p, runif(1e6), g, .template = logical(1))
              now() - t0
            })))
cat(sprintf("8MB x, swap + run:               %8.1f us/run\n",
            1e6 * best(function() {
              t0 <- now()
              kio_map_run(pmb, x = runif(1e6))
              now() - t0
            })))

kio_pool_stop(p)
