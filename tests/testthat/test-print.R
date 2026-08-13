# One-line print methods: identity and state on live handles, "closed"
# (never an error) on released ones, class alone for tasks and sentinels.

test_that("channel handles print one line and survive release", {
  p <- channel_pair()
  expect_output(print(p[["host"]]), "<kio_channel .*kio_.*: host, open>")
  expect_output(print(p[["peer"]]), "<kio_channel .*kio_.*: peer, open>")
  # either side's close bit flips the state before release
  .Call(kioto:::kio_channel_close_signal, p[["peer"]])
  expect_output(print(p[["host"]]), "<kio_channel .*: host, closed>")
  expect_true(kio_close(p[["host"]], timeout = 5))
  expect_true(kio_close(p[["peer"]], timeout = 5))
  expect_output(print(p[["host"]]), "<kio_channel: closed>", fixed = TRUE)
})

test_that("pool and task handles print one line and survive destroy", {
  p <- pool_pair(workers = 2L)
  expect_output(print(p[["ctrl"]]),
                "<kio_pool .*kio_.*: controller, 2/2 workers live, 0 pending>")
  expect_output(print(p[["wk"]]), "<kio_pool .*: worker, 2/2 workers live")
  t <- kio_submit(p[["ctrl"]], 1 + 1)
  expect_output(print(t), "<kio_task: pending>", fixed = TRUE)
  expect_output(print(p[["ctrl"]]), "1 pending")
  pool_step(p)
  expect_output(print(t), "<kio_task: ok>", fixed = TRUE)
  expect_identical(kio_collect(t), 2)
  expect_output(print(t), "<kio_task: collected>", fixed = TRUE)
  # the probe consumes nothing: an err result still collects as the error
  t2 <- kio_submit(p[["ctrl"]], stop("boom"))
  pool_step(p)
  expect_output(print(t2), "<kio_task: err>", fixed = TRUE)
  expect_error(kio_collect(t2), "boom")
  # cancel state, and every handle reads dropped once the pool is gone
  t3 <- kio_submit(p[["ctrl"]], 1)
  kio_cancel(t3)
  expect_output(print(t3), "<kio_task: cancel>", fixed = TRUE)
  pool_end(p)
  expect_output(print(t3), "<kio_task: dropped>", fixed = TRUE)
  expect_output(print(p[["ctrl"]]), "<kio_pool: closed>", fixed = TRUE)
})

test_that("sentinels print as their class", {
  p <- channel_pair()
  s <- kio_recv(p[["host"]], timeout = 0)
  expect_s3_class(s, "kio_timeout")
  expect_output(print(s), "<kio_timeout>", fixed = TRUE)
})

test_that("a channel whose peer is gone prints the verdict", {
  p <- channel_pair()
  host <- p[["host"]]
  rm(p)
  invisible(gc()) # the peer handle's finalizer releases its liveness lock
  expect_output(print(host), "peer gone", fixed = TRUE)
  expect_true(kio_close(host, timeout = 5))
})

test_that("a prepared map prints its staging", {
  p <- pool_pair(slot_size = 512L)
  f <- function(i) i + 1L
  environment(f) <- globalenv()
  pm <- kio_map_prepare(p[["ctrl"]], 1:4, f)
  expect_output(print(pm), "inline blob", fixed = TRUE)
  pool_end(p)
})
