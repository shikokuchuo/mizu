# Report-only validation of the time-boxed adaptive spins (src/kioto.h
# KIO_SPIN_*): reads the collect-side park count (kio_pool_dump()$local)
# and worker parks (kio_pool_stats()) across controlled traffic phases.
# Asserts nothing; eyeball in CI logs.
#
# Expect: ping-pong pins both budgets at max (parks ~0); an 8 MB burst
# decays them to the floor (every wait parks); the following ping-pong
# regrows from the floor in ~5-10 episodes (parks << reps), proving the
# sticky-floor fix; an idle pool burns ~0 CPU (parks dominate).
#
# Run in the bench container:
#   docker run --rm --shm-size=2g -v "$PWD/dev/bench:/b" kioto-bench Rscript /b/spin-validate.R

library(kioto)

p <- kio_pool(1L)
on.exit(kio_pool_stop(p))

collect_parks <- function() kio_pool_dump(p)[["local"]][["collect_parks"]]
worker_parks <- function() sum(kio_pool_stats(p)[["workers"]][["parks"]])

phase <- function(label, x, reps) {
  cp0 <- collect_parks()
  wp0 <- worker_parks()
  ts <- vapply(
    seq_len(reps),
    function(i) {
      t0 <- kioto:::mono_time()
      kio_collect(kio_submit(p, x, x = x), timeout = 30)
      kioto:::mono_time() - t0
    },
    0
  )
  cat(sprintf(
    "%-28s median %7.1f us | collect parks +%d | worker parks +%d\n",
    label,
    median(ts) * 1e6,
    collect_parks() - cp0,
    worker_parks() - wp0
  ))
}

x8k <- runif(1e3) # 8 KB
x8m <- runif(1e6) # 8 MB

invisible(kio_collect(kio_submit(p, x, x = x8k), timeout = 30)) # warm

phase("8 KB ping-pong (x200)", x8k, 200L)
phase("8 MB burst (x10)", x8m, 10L)
phase("8 KB recovery (x200)", x8k, 200L)

# idle-pool CPU: with decay at the floor the worker parks, so its
# userspace+kernel clock ticks over 2 idle seconds should be ~0
wpid <- kio_pool_stats(p)[["workers"]][["pid"]][1L]
ticks <- function(pid) {
  f <- sprintf("/proc/%d/stat", pid)
  if (!file.exists(f)) {
    return(NA_real_)
  }
  v <- scan(f, what = character(), quiet = TRUE)
  sum(as.numeric(v[14:15])) / 100 # utime + stime, 100 Hz ticks
}
cpu0 <- ticks(wpid)
Sys.sleep(2)
cpu1 <- ticks(wpid)
cat(sprintf(
  "%-28s worker CPU over 2 idle s: %s\n",
  "idle pool",
  if (is.na(cpu1)) "n/a (not Linux)" else sprintf("%.2f s", cpu1 - cpu0)
))
