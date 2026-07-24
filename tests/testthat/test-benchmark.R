# Benchmarks-as-reports: each run prints its timings next to the Phase 0
# incumbent baseline (M4 Pro, 2026-07 — nanonext ipc:// pair: 31.7 us per
# round-trip, ~31.5k RT/s; mirai local dispatch: 63-124 us per task; target
# regime: >100k small messages/s sustained) for eyeballing in the CI log. Nothing asserts on the numbers — runner timing is too
# variable for thresholds to hold reliably — so only the transport's
# correctness is tested here. The pool reports double as the regression
# tripwire for anything added to the worker's per-task path (Phase 5's
# stat counters deliberately publish only at park/tick cadence — a
# slowdown here is the first place a violation of that rule shows up).

test_that("round-trip latency reports against the socket baseline", {
  skip_if_no_child_kioto()
  ch <- kio_channel(echo_expr, capacity = 1024L)

  rt <- function(n) {
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) {
      kio_send(ch, 0L)
      kio_flush(ch)
      kio_recv(ch, 30)
    }
    (proc.time()[[3]] - t0) / n * 1e6
  }
  rt(500L)                                  # warm-up
  us <- min(rt(2000L), rt(2000L), rt(2000L))
  cat(sprintf("\nround-trip: %.2f us (baseline 31.73 us)\n", us))
  expect_true(kio_close(ch, timeout = 10))
})

test_that("one-way throughput reports against the >100k msg/s regime", {
  skip_if_no_child_kioto()
  n <- 200000L
  ch <- kio_channel(quote({
    total <- 0L
    repeat {
      xs <- kio_recv_batch(ch, n = 4096L, timeout = 30)
      if (inherits(xs, "kio_condition")) break
      total <- total + length(xs)
      if (total >= 200000L) {
        kio_send(ch, total)
        kio_flush(ch)
        break
      }
    }
  }), capacity = 16384L)

  batch <- as.list(rep(0L, 4096L))
  t0 <- proc.time()[[3]]
  sent <- 0L
  while (sent < n) {
    want <- min(4096L, n - sent)
    sent <- sent + kio_send_batch(ch, batch[seq_len(want)])
  }
  kio_flush(ch)
  expect_identical(kio_recv(ch, 60), n)     # peer's receipt count
  rate <- n / (proc.time()[[3]] - t0)
  cat(sprintf("\none-way: %.0f msg/s (target > 100000)\n", rate))
  expect_true(kio_close(ch, timeout = 10))
})

test_that("pool task dispatch reports against the mirai baseline", {
  skip_if_no_child_kioto()
  p <- kio_pool(1L, max_submitters = 2L)    # 2048 result slots for us

  rt <- function(n) {
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) kio_collect(kio_submit(p, NULL), timeout = 30)
    (proc.time()[[3]] - t0) / n * 1e6
  }
  rt(200L)                                  # warm-up
  us <- min(rt(1000L), rt(1000L))
  cat(sprintf("\npool round-trip: %.1f us/task (baseline: mirai 63-124 us)\n",
              us))

  tp <- function(n) {
    t0 <- proc.time()[[3]]
    ts <- vector("list", n)
    for (i in seq_len(n)) ts[[i]] <- kio_submit(p, NULL)
    for (i in seq_len(n)) kio_collect(ts[[i]], timeout = 30)
    n / (proc.time()[[3]] - t0)
  }
  tp(200L)
  rate <- max(tp(2000L), tp(2000L))
  cat(sprintf("pool pipelined: %.0f tasks/s\n", rate))

  # once the worker parks, its stat mirror is exact
  total <- 200 + 2 * 1000 + 200 + 2 * 2000
  expect_true(wait_until(kio_pool_stats(p)$workers$tasks == total))
  expect_true(kio_pool_stop(p))
})

test_that("kio_map reports against serial lapply and per-task dispatch", {
  skip_if_no_child_kioto()
  p <- kio_pool(2L)

  # overhead regime: trivial f, where per-element cost is everything.
  # Baselines (M4 Pro, 2026-07): serial lapply ~0.2 us/element; per-element
  # kio_submit/kio_collect ~4 us (the pool round-trip above); mirai_map
  # ~63-124 us/element (one mirai task per element).
  n <- 100000L
  x <- seq_len(n) + 0L                        # materialized: RAWVEC path
  f <- function(i) i + 1L
  mp <- function() {
    t0 <- proc.time()[[3]]
    r <- kio_map(p, x, f, .template = numeric(1))
    us <- (proc.time()[[3]] - t0) / n * 1e6
    expect_identical(r, x + 1)
    us
  }
  mp()                                        # warm-up
  us <- min(mp(), mp())
  t0 <- proc.time()[[3]]
  base <- vapply(x, f, numeric(1))
  lap <- (proc.time()[[3]] - t0) / n * 1e6
  cat(sprintf("\nkio_map trivial f: %.2f us/element (serial vapply %.2f, per-task dispatch ~4, mirai_map 63-124)\n",
              us, lap))

  # compute-bound regime: the win is wall-clock division of real work
  slow <- function(i) {
    t0 <- proc.time()[[3]]
    while (proc.time()[[3]] - t0 < 0.005) NULL
    i
  }
  t0 <- proc.time()[[3]]
  r <- kio_map(p, 1:64, slow)
  el <- proc.time()[[3]] - t0
  expect_identical(r, as.list(1:64))
  cat(sprintf("kio_map 64 x 5ms on 2 workers: %.2fs (serial 0.32s)\n", el))
  expect_true(kio_pool_stop(p))
})
