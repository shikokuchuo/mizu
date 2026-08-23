# One-line print methods: identity and state on live handles, "closed"
# (never an error) on released ones, class alone for tasks and sentinels.

test_that("channel handles print one line and survive release", {
  p <- channel_pair()
  expect_output(print(p[["host"]]), "<rei_channel .*rei_.*: host, open>")
  expect_output(print(p[["peer"]]), "<rei_channel .*rei_.*: peer, open>")
  # either side's close bit flips the state before release
  .Call(rei:::rei_channel_close_signal, p[["peer"]])
  expect_output(print(p[["host"]]), "<rei_channel .*: host, closed>")
  expect_true(rei_close(p[["host"]], timeout = 5))
  expect_true(rei_close(p[["peer"]], timeout = 5))
  expect_output(print(p[["host"]]), "<rei_channel: closed>", fixed = TRUE)
})

test_that("pool and task handles print one line and survive destroy", {
  p <- pool_pair(workers = 2L)
  expect_output(
    print(p[["ctrl"]]),
    "<rei_pool .*rei_.*: controller, 2/2 workers live, 0 pending>"
  )
  expect_output(print(p[["wk"]]), "<rei_pool .*: worker, 2/2 workers live")
  t <- rei_submit(p[["ctrl"]], 1 + 1)
  expect_output(print(t), "<rei_task: pending>", fixed = TRUE)
  expect_output(print(p[["ctrl"]]), "1 pending")
  pool_step(p)
  expect_output(print(t), "<rei_task: ok>", fixed = TRUE)
  expect_identical(rei_collect(t), 2)
  expect_output(print(t), "<rei_task: collected>", fixed = TRUE)
  # the probe consumes nothing: an err result still collects as the error
  t2 <- rei_submit(p[["ctrl"]], stop("boom"))
  pool_step(p)
  expect_output(print(t2), "<rei_task: err>", fixed = TRUE)
  expect_error(rei_collect(t2), "boom")
  # cancel state, and every handle reads dropped once the pool is gone
  t3 <- rei_submit(p[["ctrl"]], 1)
  rei_cancel(t3)
  expect_output(print(t3), "<rei_task: cancel>", fixed = TRUE)
  pool_end(p)
  expect_output(print(t3), "<rei_task: dropped>", fixed = TRUE)
  expect_output(print(p[["ctrl"]]), "<rei_pool: closed>", fixed = TRUE)
})

test_that("sentinels print as their class", {
  p <- channel_pair()
  s <- rei_recv(p[["host"]], timeout = 0)
  expect_s3_class(s, "rei_timeout")
  expect_output(print(s), "<rei_timeout>", fixed = TRUE)
})

test_that("a finalized in-process peer handle reads closed at print", {
  p <- channel_pair()
  host <- p[["host"]]
  rm(p)
  invisible(gc()) # the peer handle's finalizer signals close
  expect_output(print(host), ": host, closed>", fixed = TRUE)
  expect_true(rei_close(host, timeout = 5))
})

test_that("a channel whose peer is gone prints the verdict", {
  skip_if_no_child_rei()
  ch <- rei_channel(quote(Sys.sleep(30)))
  kill_hard(.Call(rei:::rei_channel_stat, ch)[["peer_pid"]])
  expect_true(wait_until(!rei_alive(ch)))
  expect_output(print(ch), "peer gone", fixed = TRUE)
  expect_true(rei_close(ch, timeout = 5))
})

test_that("a prepared map prints its staging", {
  p <- pool_pair()
  f <- function(i) i + 1L
  environment(f) <- globalenv()
  pm <- rei_map_prepare(p[["ctrl"]], 1:4, f)
  expect_output(print(pm), "inline blob", fixed = TRUE)
  pool_end(p)
})
