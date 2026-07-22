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
  skip_if_no_child_mov()
  ch <- mov_channel(echo_expr, capacity = 1024L)

  rt <- function(n) {
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) {
      mov_send(ch, 0L)
      mov_flush(ch)
      mov_recv(ch, 30)
    }
    (proc.time()[[3]] - t0) / n * 1e6
  }
  rt(500L)                                  # warm-up
  us <- min(rt(2000L), rt(2000L), rt(2000L))
  cat(sprintf("\nround-trip: %.2f us (baseline 31.73 us)\n", us))
  expect_true(mov_close(ch, timeout = 10))
})

test_that("one-way throughput reports against the >100k msg/s regime", {
  skip_if_no_child_mov()
  n <- 200000L
  ch <- mov_channel(quote({
    total <- 0L
    repeat {
      xs <- mov_recv_batch(ch, n = 4096L, timeout = 30)
      if (inherits(xs, "mov_condition")) break
      total <- total + length(xs)
      if (total >= 200000L) {
        mov_send(ch, total)
        mov_flush(ch)
        break
      }
    }
  }), capacity = 16384L)

  batch <- as.list(rep(0L, 4096L))
  t0 <- proc.time()[[3]]
  sent <- 0L
  while (sent < n) {
    want <- min(4096L, n - sent)
    sent <- sent + mov_send_batch(ch, batch[seq_len(want)])
  }
  mov_flush(ch)
  expect_identical(mov_recv(ch, 60), n)     # peer's receipt count
  rate <- n / (proc.time()[[3]] - t0)
  cat(sprintf("\none-way: %.0f msg/s (target > 100000)\n", rate))
  expect_true(mov_close(ch, timeout = 10))
})

test_that("pool task dispatch reports against the mirai baseline", {
  skip_if_no_child_mov()
  p <- mov_pool(1L, max_submitters = 2L)    # 2048 result slots for us

  rt <- function(n) {
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) mov_collect(mov_submit(p, NULL), timeout = 30)
    (proc.time()[[3]] - t0) / n * 1e6
  }
  rt(200L)                                  # warm-up
  us <- min(rt(1000L), rt(1000L))
  cat(sprintf("\npool round-trip: %.1f us/task (baseline: mirai 63-124 us)\n",
              us))

  tp <- function(n) {
    t0 <- proc.time()[[3]]
    ts <- vector("list", n)
    for (i in seq_len(n)) ts[[i]] <- mov_submit(p, NULL)
    for (i in seq_len(n)) mov_collect(ts[[i]], timeout = 30)
    n / (proc.time()[[3]] - t0)
  }
  tp(200L)
  rate <- max(tp(2000L), tp(2000L))
  cat(sprintf("pool pipelined: %.0f tasks/s\n", rate))

  # once the worker parks, its stat mirror is exact
  total <- 200 + 2 * 1000 + 200 + 2 * 2000
  expect_true(wait_until(mov_pool_stats(p)$workers$tasks == total))
  expect_true(mov_pool_stop(p))
})
