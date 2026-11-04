# One-line print methods: identity and state on live handles, "closed"
# (never an error) on released ones, class alone for tasks and
# sentinels, and the head plus argument counts for call specs.

test_that("channel handles print one line and survive release", {
  p <- channel_pair()
  expect_snapshot(print(p[["host"]]), transform = scrub_mizu_names)
  expect_snapshot(print(p[["peer"]]), transform = scrub_mizu_names)
  # either side's close bit flips the state before release
  .Call(mizu:::mizu_channel_close_signal, p[["peer"]])
  expect_snapshot(print(p[["host"]]), transform = scrub_mizu_names)
  expect_true(mizu_close(p[["host"]], timeout = 5))
  expect_true(mizu_close(p[["peer"]], timeout = 5))
  expect_snapshot(print(p[["host"]]))
})

test_that("pool and task handles print one line and survive destroy", {
  p <- pool_pair(workers = 2L)
  expect_snapshot(print(p[["ctrl"]]), transform = scrub_mizu_names)
  expect_snapshot(print(p[["wk"]]), transform = scrub_mizu_names)
  t <- mizu_submit(p[["ctrl"]], 1 + 1)
  expect_snapshot(print(t))
  expect_snapshot(print(p[["ctrl"]]), transform = scrub_mizu_names)
  pool_step(p)
  expect_snapshot(print(t))
  expect_identical(mizu_collect(t), 2)
  expect_snapshot(print(t))
  # the probe consumes nothing: an err result still collects as the error
  t2 <- mizu_submit(p[["ctrl"]], stop("boom"))
  pool_step(p)
  expect_snapshot(print(t2))
  expect_error(mizu_collect(t2), "boom")
  # cancel state, and every handle reads dropped once the pool is gone
  t3 <- mizu_submit(p[["ctrl"]], 1)
  mizu_cancel(t3)
  expect_snapshot(print(t3))
  pool_end(p)
  expect_snapshot(print(t3))
  expect_snapshot(print(p[["ctrl"]]))
})

test_that("call specs print the head and argument counts", {
  expect_snapshot(print(mizu_call("stats::quantile", 1:10, probs = 0.5)))
  expect_snapshot(print(mizu_call(.source = "x + 1", x = 41L)))
  expect_snapshot(print(mizu_call(.source = "x + 1\nx + 2")))
  expect_snapshot(print(mizu_call(.source = strrep("x", 100L))))
  expect_snapshot(print(mizu_call("base::sqrt")))
  expect_snapshot(print(mizu_call("stats::quantile", 1:10)))
})

test_that("sentinels print as their class", {
  p <- channel_pair(capacity = 4L)
  tm <- mizu_recv(p[["host"]], timeout = 0)
  expect_s3_class(tm, "mizu_timeout")
  expect_snapshot(print(tm))

  for (i in 1:4) {
    expect_true(mizu_send(p[["host"]], i))
  }
  fl <- mizu_send(p[["host"]], 5L)
  expect_s3_class(fl, "mizu_full")
  expect_snapshot(print(fl))

  for (i in 1:4) {
    expect_identical(mizu_recv(p[["peer"]], 5), i)
  }
  .Call(mizu:::mizu_channel_close_signal, p[["peer"]])
  cl <- mizu_recv(p[["host"]], 5)
  expect_s3_class(cl, "mizu_closed")
  expect_snapshot(print(cl))
  expect_true(mizu_close(p[["host"]], timeout = 5))
  expect_true(mizu_close(p[["peer"]], timeout = 5))
})

test_that("a finalized in-process peer handle reads closed at print", {
  skip_on_cran() # the close signal rides the peer handle's finalizer at gc()
  p <- channel_pair()
  host <- p[["host"]]
  rm(p)
  invisible(gc()) # the peer handle's finalizer signals close
  expect_snapshot(print(host), transform = scrub_mizu_names)
  expect_true(mizu_close(host, timeout = 5))
})

test_that("a channel whose peer is gone prints the verdict", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(quote(Sys.sleep(30)))
  kill_hard(.Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]])
  expect_true(wait_until(!mizu_alive(ch)))
  expect_snapshot(print(ch), transform = scrub_mizu_names)
  expect_true(mizu_close(ch, timeout = 5))
})

test_that("a prepared map prints its staging", {
  p <- pool_pair()
  f <- function(i) i + 1L
  environment(f) <- globalenv()
  pm <- mizu_map_prepare(p[["ctrl"]], 1:4, f)
  expect_snapshot(print(pm))
  pool_end(p)
})

test_that("a region-staged prepared map prints its region name", {
  p <- pool_pair()
  pm <- mizu_map_prepare(p[["ctrl"]], runif(100000), identity)
  expect_snapshot(print(pm), transform = scrub_mizu_names)
  expect_match(capture_output(print(pm)), pm[["st"]][["name"]], fixed = TRUE)
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
  expect_snapshot(print(e))
})

test_that("map handles print their submission state", {
  p <- pool_pair()
  m <- mizu_map(p[["ctrl"]], 1:4, identity)
  expect_snapshot(print(m))
  ms <- mizu_map(p[["ctrl"]], 1:4, function(i) i, .stream = TRUE)
  expect_snapshot(print(ms))
  big <- mizu_map(p[["ctrl"]], runif(100000), identity)
  expect_snapshot(print(big), transform = scrub_mizu_names)
  expect_match(capture_output(print(big)), big[["st"]][["name"]], fixed = TRUE)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(m), as.list(1:4))
  expect_snapshot(print(m))
  mizu_cancel(ms)
  mizu_cancel(big)
  pool_end(p)
})
