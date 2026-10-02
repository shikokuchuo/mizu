# One-line print methods: identity and state on live handles, "closed"
# (never an error) on released ones, class alone for tasks and sentinels.

test_that("channel handles print one line and survive release", {
  p <- channel_pair()
  expect_output(print(p[["host"]]), "<mizu_channel .*mizu_.*: host, open>")
  expect_output(print(p[["peer"]]), "<mizu_channel .*mizu_.*: peer, open>")
  # either side's close bit flips the state before release
  .Call(mizu:::mizu_channel_close_signal, p[["peer"]])
  expect_output(print(p[["host"]]), "<mizu_channel .*: host, closed>")
  expect_true(mizu_close(p[["host"]], timeout = 5))
  expect_true(mizu_close(p[["peer"]], timeout = 5))
  expect_output(print(p[["host"]]), "<mizu_channel: closed>", fixed = TRUE)
})

test_that("pool and task handles print one line and survive destroy", {
  p <- pool_pair(workers = 2L)
  expect_output(
    print(p[["ctrl"]]),
    "<mizu_pool .*mizu_.*: controller, 2/2 workers live, 0 pending>"
  )
  expect_output(print(p[["wk"]]), "<mizu_pool .*: worker, 2/2 workers live")
  t <- mizu_submit(p[["ctrl"]], 1 + 1)
  expect_output(print(t), "<mizu_task: pending>", fixed = TRUE)
  expect_output(print(p[["ctrl"]]), "1 pending")
  pool_step(p)
  expect_output(print(t), "<mizu_task: ok>", fixed = TRUE)
  expect_identical(mizu_collect(t), 2)
  expect_output(print(t), "<mizu_task: collected>", fixed = TRUE)
  # the probe consumes nothing: an err result still collects as the error
  t2 <- mizu_submit(p[["ctrl"]], stop("boom"))
  pool_step(p)
  expect_output(print(t2), "<mizu_task: err>", fixed = TRUE)
  expect_error(mizu_collect(t2), "boom")
  # cancel state, and every handle reads dropped once the pool is gone
  t3 <- mizu_submit(p[["ctrl"]], 1)
  mizu_cancel(t3)
  expect_output(print(t3), "<mizu_task: cancel>", fixed = TRUE)
  pool_end(p)
  expect_output(print(t3), "<mizu_task: dropped>", fixed = TRUE)
  expect_output(print(p[["ctrl"]]), "<mizu_pool: closed>", fixed = TRUE)
})

test_that("sentinels print as their class", {
  p <- channel_pair()
  s <- mizu_recv(p[["host"]], timeout = 0)
  expect_s3_class(s, "mizu_timeout")
  expect_output(print(s), "<mizu_timeout>", fixed = TRUE)
})

test_that("a finalized in-process peer handle reads closed at print", {
  p <- channel_pair()
  host <- p[["host"]]
  rm(p)
  invisible(gc()) # the peer handle's finalizer signals close
  expect_output(print(host), ": host, closed>", fixed = TRUE)
  expect_true(mizu_close(host, timeout = 5))
})

test_that("a channel whose peer is gone prints the verdict", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(quote(Sys.sleep(30)))
  kill_hard(.Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]])
  expect_true(wait_until(!mizu_alive(ch)))
  expect_output(print(ch), "peer gone", fixed = TRUE)
  expect_true(mizu_close(ch, timeout = 5))
})

test_that("a prepared map prints its staging", {
  p <- pool_pair()
  f <- function(i) i + 1L
  environment(f) <- globalenv()
  pm <- mizu_map_prepare(p[["ctrl"]], 1:4, f)
  expect_output(print(pm), "inline blob", fixed = TRUE)
  pool_end(p)
})

test_that("a region-staged prepared map prints its region name", {
  p <- pool_pair()
  pm <- mizu_map_prepare(p[["ctrl"]], runif(100000), identity)
  expect_output(print(pm), "100000 elements", fixed = TRUE)
  expect_output(print(pm), pm[["st"]][["name"]], fixed = TRUE)
  pool_end(p)
})

test_that("a remote error prints one line leading with type: message", {
  e <- .Call(
    mizu:::mizu_interop_read_call,
    as.raw(c(
      0x49,
      0x01,
      0x11,
      0x00,
      0x00,
      0x0a,
      0x00,
      0x00,
      0x00,
      as.integer(charToRaw("ValueError")),
      0x04,
      0x00,
      0x00,
      0x00,
      as.integer(charToRaw("boom")),
      0x00,
      0x00,
      0x00,
      0x00
    ))
  )
  expect_output(
    print(e),
    "<mizu_error_remote: ValueError: boom>",
    fixed = TRUE
  )
})
