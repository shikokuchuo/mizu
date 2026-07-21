# Benchmarks-as-tests: the ring must beat the committed Phase 0 baseline in
# its target regime (tools/baseline/BASELINE.md — nanonext ipc:// pair:
# 31.7 us per round-trip, ~31.5k RT/s; target regime: >100k small messages/s
# sustained). The thresholds are the baseline numbers themselves, measured on
# the reference hardware, so slower CI runners retain real headroom: the ring
# runs 5-10x inside them there.

test_that("round-trip latency beats the socket baseline", {
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
  expect_lt(us, 31.73)
  expect_true(mov_close(ch, timeout = 10))
})

test_that("one-way throughput sustains the >100k msg/s regime", {
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
  expect_gt(rate, 100000)
  expect_true(mov_close(ch, timeout = 10))
})
