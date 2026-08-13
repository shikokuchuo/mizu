# Report-only loaded-machine probe for the yield phase (KIO_SPIN_YIELDS):
# 8 KB ping-pong medians, meant to be run under CPU contention. Pair with
# a KIO_SPIN_YIELDS 0 rebuild for the A/B (see linux-payload-perf-plan.md
# step 7).
#
#   docker run --rm --shm-size=2g --cpus=2 -e OPENBLAS_NUM_THREADS=1 \
#     -v "$PWD/dev/bench:/b" kioto-bench sh -c \
#     'Rscript -e "while (TRUE) NULL" & Rscript /b/spin-pingpong.R'

library(kioto)

p <- kio_pool(1L)
on.exit(kio_pool_stop(p))
x <- runif(1e3)   # 8 KB
invisible(kio_collect(kio_submit(p, x, x = x), timeout = 30))

n <- 500L
ts <- vapply(seq_len(n), function(i) {
  t0 <- kioto:::mono_time()
  kio_collect(kio_submit(p, x, x = x), timeout = 30)
  kioto:::mono_time() - t0
}, 0)
cat(sprintf("ping-pong 8 KB x%d: median %.1f us | p90 %.1f us | collect parks +%d\n",
            n, median(ts) * 1e6, quantile(ts, 0.9) * 1e6,
            kio_pool_dump(p)[["local"]][["collect_parks"]]))
