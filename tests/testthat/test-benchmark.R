# Benchmarks-as-reports: each run prints its timings for eyeballing in the
# CI log. The incumbent baselines, phase outcomes, and every dated
# calibration record live in dev/bench/notes.md (the best-known table on
# top, the dated log at the bottom — append new runs there).
# Nothing asserts on the numbers — runner timing is too variable for
# thresholds to hold reliably — so only the transport's correctness is
# tested here. The pool reports double as the regression tripwire for
# anything added to the worker's per-task path (the stat counters
# deliberately publish only at park/tick cadence — a slowdown here is the
# first place a violation of that rule shows up). Every test skips on CRAN
# (timing reports are a CI-log tool only); CI sets NOT_CRAN.

test_that("round-trip latency reports against the socket baseline", {
  skip_on_cran()
  skip_if_no_child_mizu()
  ch <- mizu_channel(echo_expr, capacity = 1024L)

  rt <- function(n) {
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) {
      mizu_send(ch, 0L)
      mizu_recv(ch, 30)
    }
    (proc.time()[[3]] - t0) / n * 1e6
  }
  rt(500L) # warm-up
  us <- min(rt(2000L), rt(2000L), rt(2000L))
  cat(sprintf("\nround-trip: %.2f us (baseline 31.73 us)\n", us))
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("one-way throughput reports against the >100k msg/s regime", {
  skip_on_cran()
  skip_if_no_child_mizu()
  n <- 200000L
  ch <- mizu_channel(
    quote({
      total <- 0L
      repeat {
        xs <- mizu_recv_batch(ch, n = 4096L, timeout = 30)
        if (inherits(xs, "mizu_timeout")) {
          next # an idle producer is not terminal (cf. echo_expr)
        }
        if (inherits(xs, "mizu_sentinel")) {
          break
        }
        total <- total + length(xs)
        if (total >= 200000L) {
          mizu_send(ch, total)
          break
        }
      }
    }),
    capacity = 16384L
  )

  batch <- as.list(rep(0L, 4096L))
  t0 <- proc.time()[[3]]
  sent <- 0L
  while (sent < n) {
    want <- min(4096L, n - sent)
    sent <- sent + mizu_send_batch(ch, batch[seq_len(want)])
  }
  expect_identical(mizu_recv(ch, 60), n) # peer's receipt count
  rate <- n / (proc.time()[[3]] - t0)
  cat(sprintf("\none-way: %.0f msg/s (target > 100000)\n", rate))
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("pool task dispatch reports against the mirai baseline", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(1L, max_submitters = 2L) # 2048 result slots for us

  rt <- function(n) {
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) {
      mizu_collect(mizu_submit(p, NULL), timeout = 30)
    }
    (proc.time()[[3]] - t0) / n * 1e6
  }
  rt(200L) # warm-up
  us <- min(rt(1000L), rt(1000L))
  cat(sprintf(
    "\npool round-trip: %.1f us/task (baseline: mirai 63-124 us)\n",
    us
  ))

  tp <- function(n) {
    t0 <- proc.time()[[3]]
    ts <- vector("list", n)
    for (i in seq_len(n)) {
      ts[[i]] <- mizu_submit(p, NULL)
    }
    for (i in seq_len(n)) {
      mizu_collect(ts[[i]], timeout = 30)
    }
    n / (proc.time()[[3]] - t0)
  }
  tp(200L)
  rate <- max(tp(2000L), tp(2000L))
  cat(sprintf("pool pipelined: %.0f tasks/s\n", rate))

  # fire-then-collect with one batch collect: the R call boundary paid
  # once per burst instead of once per task
  tpa <- function(n) {
    t0 <- proc.time()[[3]]
    ts <- vector("list", n)
    for (i in seq_len(n)) {
      ts[[i]] <- mizu_submit(p, NULL)
    }
    mizu_collect_all(ts, timeout = 30)
    n / (proc.time()[[3]] - t0)
  }
  tpa(200L)
  ratea <- max(tpa(2000L), tpa(2000L))
  cat(sprintf("pool pipelined, collect_all: %.0f tasks/s\n", ratea))

  # once the worker parks, its stat mirror is exact
  total <- 200 + 2 * 1000 + 2 * (200 + 2 * 2000)
  expect_true(wait_until(mizu_pool_stats(p)[["workers"]][["tasks"]] == total))
  expect_true(mizu_pool_stop(p))
})

test_that("mizu_map reports against serial lapply and per-task dispatch", {
  skip_on_cran() # host + 2 workers exceeds 2 cores
  skip_if_no_child_mizu()
  p <- mizu_pool(2L)

  # overhead regime: trivial f, where per-element cost is everything
  # (baselines: dev/bench/notes.md)
  n <- 100000L
  x <- seq_len(n) + 0L # materialized: RAWVEC path
  f <- function(i) i + 1L
  mp <- function() {
    t0 <- proc.time()[[3]]
    r <- mizu_map(p, x, f, .template = numeric(1), .timeout = 60)
    us <- (proc.time()[[3]] - t0) / n * 1e6
    expect_identical(r, x + 1)
    us
  }
  mp() # warm-up
  us <- min(mp(), mp())
  t0 <- proc.time()[[3]]
  base <- vapply(x, f, numeric(1))
  lap <- (proc.time()[[3]] - t0) / n * 1e6
  cat(sprintf(
    "\nmizu_map trivial f: %.2f us/element (serial vapply %.2f, per-task dispatch ~4, mirai_map 63-124)\n",
    us,
    lap
  ))

  # compute-bound regime: the win is wall-clock division of real work
  slow <- function(i) {
    t0 <- proc.time()[[3]]
    while (proc.time()[[3]] - t0 < 0.005) {
      NULL
    }
    i
  }
  t0 <- proc.time()[[3]]
  r <- mizu_map(p, 1:64, slow, .timeout = 60)
  el <- proc.time()[[3]] - t0
  expect_identical(r, as.list(1:64))
  cat(sprintf("mizu_map 64 x 5ms on 2 workers: %.2fs (serial 0.32s)\n", el))
  expect_true(mizu_pool_stop(p))
})

# The memcpy-bound regime the next cases measure: the before/after
# baselines and every dated outcome are recorded in dev/bench/notes.md.

test_that("large-vector channel round trip reports the memcpy-bound regime", {
  skip_on_cran()
  skip_if_no_child_mizu()
  ch <- mizu_channel(echo_expr, capacity = 64L)

  rt <- function(x, n) {
    mizu_send(ch, x)
    expect_identical(mizu_recv(ch, 60), x) # correctness on the warm-up
    best <- Inf
    for (r in 1:3) {
      gc() # views free their spill regions only via finalizers; keep lent
      # regions bounded so the suite fits a 1 GB /dev/shm (docker default)
      t0 <- proc.time()[[3]]
      for (i in seq_len(n)) {
        mizu_send(ch, x)
        mizu_recv(ch, 60)
      }
      best <- min(best, proc.time()[[3]] - t0)
    }
    best / n
  }
  # all three sizes are past the zc floor: SHM_VEC (the copy tiers are the
  # churn fallback). The echo peer's received views release only on its own
  # GC, so regions lent to the peer pile up for the whole phase: total staged
  # volume is budgeted to fit a 1 GB /dev/shm (docker default)
  for (mb in c(1, 8, 32)) {
    x <- seq_len(mb * 131072) + 0
    s <- rt(x, max(2L, 32L %/% mb))
    cat(sprintf(
      "\nchannel %d MiB round trip: %.2f ms (%.0f MiB/s payload)\n",
      mb,
      s * 1e3,
      2 * mb / s
    ))
  }
  gc()
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("pool task returning a large vector reports the memcpy-bound regime", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(1L, max_submitters = 2L)

  for (mb in c(1, 8, 64)) {
    n <- mb * 131072
    t <- mizu_submit(p, seq_len(n) + 0, n = n)
    expect_identical(mizu_collect(t, 60), seq_len(n) + 0) # warm-up
    best <- Inf
    for (r in 1:3) {
      gc() # collected views free their spill regions only via finalizers
      t0 <- proc.time()[[3]]
      for (i in seq_len(max(1L, 32L %/% mb))) {
        mizu_collect(mizu_submit(p, seq_len(n) + 0, n = n), 60)
      }
      best <- min(best, proc.time()[[3]] - t0)
    }
    s <- best / max(1L, 32L %/% mb)
    cat(sprintf(
      "\npool %d MiB result: %.2f ms/task (%.0f MiB/s payload)\n",
      mb,
      s * 1e3,
      mb / s
    ))
  }
  gc()
  expect_true(mizu_pool_stop(p))
})

test_that("template mizu_map at large n reports the staging/gather memcpy regime", {
  skip_on_cran() # host + 2 workers exceeds 2 cores
  skip_if_no_child_mizu()
  p <- mizu_pool(2L)

  # 32 MiB x section staged once (map.c), 32 MiB template output gathered back
  n <- 4 * 1024 * 1024
  x <- seq_len(n) + 0
  f <- function(i) i + 1
  mp <- function() {
    t0 <- proc.time()[[3]]
    r <- mizu_map(p, x, f, .template = numeric(1), .timeout = 60)
    el <- proc.time()[[3]] - t0
    expect_identical(r, x + 1)
    el
  }
  mp() # warm-up
  el <- min(mp(), mp())
  cat(sprintf(
    "\nmizu_map 32 MiB template: %.3fs (%.2f us/element)\n",
    el,
    el / n * 1e6
  ))

  # .collect = "view" skips the gather memcpy (the result is an ALTREP
  # view over the output area); the full-sweep read faults pages
  mpv <- function() {
    t0 <- proc.time()[[3]]
    r <- mizu_map(
      p,
      x,
      f,
      .template = numeric(1),
      .collect = "view",
      .timeout = 60
    )
    el <- proc.time()[[3]] - t0
    expect_identical(sum(r), sum(x + 1))
    el
  }
  mpv() # warm-up
  gc() # a collected view's map region is released only by its finalizer
  elv <- min(mpv(), mpv())
  cat(sprintf(
    "mizu_map 32 MiB template, view collect + reduce: %.3fs (%.2f us/element)\n",
    elv,
    elv / n * 1e6
  ))
  expect_true(mizu_pool_stop(p))
})

test_that("guard: ALTREP input stays a compact stream", {
  skip_on_cran()
  skip_if_no_child_mizu()
  ch <- mizu_channel(echo_expr, capacity = 64L)

  x <- 1:(128 * 1024 * 1024) # ALTREP seq: 512 MiB materialized
  xb <- serialize(x, NULL)
  len <- length(xb)
  expect_lt(len, 1024L) # the compact-stream premise
  mizu_send(ch, x)
  # compare the serialized streams, not the objects: identical() would
  # materialize both sequences (two 512 MiB allocations — valgrind's
  # large-range mmap warnings); equal compact streams are equal values
  expect_identical(serialize(mizu_recv(ch, 60), NULL), xb)
  n <- 1000L # looped: proc.time ticks at ~1 ms on this platform
  t0 <- proc.time()[[3]]
  for (i in seq_len(n)) {
    mizu_send(ch, x)
    mizu_recv(ch, 60)
  }
  el <- proc.time()[[3]] - t0
  cat(sprintf(
    "\nALTREP 1:2^27 round trip: %.2f us (%d B serialized)\n",
    el / n * 1e6,
    len
  ))
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("guard: partial read of a wide matrix pays full unserialize today", {
  skip_on_cran()
  skip_if_no_child_mizu()
  ch <- mizu_channel(echo_expr, capacity = 64L)

  m <- matrix(seq_len(131072 * 100) + 0, nrow = 131072) # 100 MiB, 100 cols
  mizu_send(ch, m)
  t0 <- proc.time()[[3]]
  y <- mizu_recv(ch, 60)
  t1 <- proc.time()[[3]]
  expect_identical(y, m)
  for (i in 1:100) {
    s <- y[, 1:3]
  } # looped: one read is below the timer tick
  t2 <- proc.time()[[3]]
  expect_identical(s, m[, 1:3])
  cat(sprintf(
    "\n100 MiB matrix round trip: %.1f ms; then 3-of-100 cols: %.2f ms/read\n",
    (t1 - t0) * 1e3,
    (t2 - t1) * 10
  ))
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("guard: held results do not pin payload regions today", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(1L, max_submitters = 2L)

  n <- 2 * 1024 * 1024 # 16 MiB results
  held <- vector("list", 8L)
  for (i in seq_len(8L)) {
    held[[i]] <- mizu_collect(mizu_submit(p, seq_len(n) + 0, n = n), 60)
  }
  st <- mizu_pool_stats(p)[["submitters"]]
  st <- st[st[["status"]] == "live", ] # one row per slot; the idle slot reads zero
  cat(sprintf(
    "\nheld 8 x 16 MiB: spills %d, spill_reuse %d, fresh regions %d\n",
    st[["spills"]],
    st[["spill_reuse"]],
    st[["spills"]] - st[["spill_reuse"]]
  ))
  expect_identical(held[[8L]], seq_len(n) + 0)
  expect_true(mizu_pool_stop(p))
})

test_that("guard: attributed large vector reports the attrs-parse share", {
  skip_on_cran()
  skip_if_no_child_mizu()
  ch <- mizu_channel(echo_expr, capacity = 64L)

  x <- seq_len(4 * 1024 * 1024) + 0 # 32 MiB
  names(x) <- paste0("n", seq_along(x)) # non-trivial attrs blob
  rt <- function(v, n) {
    mizu_send(ch, v)
    expect_identical(mizu_recv(ch, 60), v)
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) {
      mizu_send(ch, v)
      mizu_recv(ch, 60)
    }
    (proc.time()[[3]] - t0) / n
  }
  # each round trip stages a fresh ~64 MiB serialized stream, released only
  # via finalizers: single iterations keep resident regions within a 1 GB
  # /dev/shm (docker default)
  s <- rt(x, 1L)
  gc()
  sx <- rt(unname(x), 1L)
  cat(sprintf(
    "\n32 MiB attributed round trip: %.2f ms (bare %.2f, attrs %.2f)\n",
    s * 1e3,
    sx * 1e3,
    (s - sx) * 1e3
  ))
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("nested-view fan-in reports the wire-resolve dedup regime", {
  skip_on_cran()
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)

  # 2000 references to one region in a single payload: the receive side pays
  # one cached mapping, not two fresh mappings per reference (the Linux
  # vm.max_map_count wedge of the 2026-09-11 CI postmortem)
  k <- 2000L
  payload <- rep(list(v), k)
  mizu_send(p[["peer"]], payload)
  w <- mizu_recv(p[["host"]], 60) # warm-up and correctness
  expect_true(.Call(mizu:::mizu_zc_view_check, w[[1L]]))
  expect_identical(as.numeric(w[[k]]), x)
  rm(w)
  gc() # resolved views free their counts only via finalizers
  best <- Inf
  for (r in 1:3) {
    t0 <- proc.time()[[3]]
    mizu_send(p[["peer"]], payload)
    w <- mizu_recv(p[["host"]], 60)
    best <- min(best, proc.time()[[3]] - t0)
    rm(w)
    gc()
  }
  cat(sprintf(
    "nested-view fan-in (%d refs to one 800 KiB region): %.2f ms round trip (%.2f us per ref)\n",
    k,
    best * 1e3,
    best * 1e6 / k
  ))
  rm(payload)
  gc()
  channel_end(p)
})

test_that("int64 reports the raw tiers against the legacy paths", {
  skip_on_cran()
  skip_if_no_child_mizu()
  skip_if_not_installed("bit64")
  ch <- mizu_channel(echo_expr, capacity = 64L)

  rt <- function(x, n) {
    mizu_send(ch, x)
    expect_identical(mizu_recv(ch, 60), x) # correctness on the warm-up
    best <- Inf
    for (r in 1:3) {
      gc() # views free their spill regions only via finalizers
      t0 <- proc.time()[[3]]
      for (i in seq_len(n)) {
        mizu_send(ch, x)
        mizu_recv(ch, 60)
      }
      best <- min(best, proc.time()[[3]] - t0)
    }
    best / n
  }
  # class-only integer64 takes the raw memcpy tiers; one extra attribute
  # reproduces the pre-change tier per size (the "legacy" column): the codec
  # stream below the zc floor (4 KiB), SHM_VEC with an attrs section above
  # it. The raw tiers: arena RAWSPILL up to 256 KiB, SHM_VEC past it. Lent
  # views pile up per phase as in the large-vector rows above
  for (kb in c(4, 128, 1024, 8192)) {
    n <- kb * 128L
    x <- bit64::as.integer64(seq_len(n))
    xa <- structure(x, note = "attr") # attributed: the pre-change tier
    reps <- max(4L, 32768L %/% kb) # enough iterations to smooth clock quantum
    s <- rt(x, reps)
    sa <- rt(xa, reps)
    cat(sprintf(
      "\nint64 channel %d KiB round trip: %.2f ms raw, %.2f ms legacy\n",
      kb,
      s * 1e3,
      sa * 1e3
    ))
  }
  gc()
  expect_true(mizu_close(ch, timeout = 10))

  # mizu_map: the raw x section against the descriptor path (names force the
  # list coercion and the per-runner descriptor unserialize it replaced)
  p <- mizu_pool(2L)
  n <- 100000L
  x <- bit64::as.integer64(seq_len(n))
  f <- function(xi) xi
  # no closure over the test frame: after the raw map, r1 holds 100k
  # view-backed scalars (the runners' big value lists arrive SHM_VEC), and a
  # serialized f would drag them into map two's descriptor as per-element
  # references — one fresh mapping per resolve exhausts vm.max_map_count on
  # the Linux workers
  environment(f) <- baseenv()
  t0 <- proc.time()[[3]]
  r1 <- mizu_map(p, x, f, .timeout = 60)
  raw_s <- proc.time()[[3]] - t0
  # the descriptor leg at a fifth of the elements: bit64's
  # as.list.integer64 protects per element, overflowing R < 4.6's fixed
  # protection stack at full length
  nd <- 20000L
  xn <- setNames(x[seq_len(nd)], paste0("e", seq_len(nd)))
  t0 <- proc.time()[[3]]
  r2 <- mizu_map(p, xn, f, .timeout = 60)
  desc_s <- proc.time()[[3]] - t0
  expect_identical(r1[seq_len(nd)], unname(r2))
  cat(sprintf(
    "mizu_map int64 x (%d elements, 2 workers): %.3f s raw section, %.3f s descriptor (%d elements)\n",
    n,
    raw_s,
    desc_s,
    nd
  ))
  expect_true(mizu_pool_stop(p))
})
