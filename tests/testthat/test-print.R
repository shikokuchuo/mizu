# One-line print methods: identity and state on live handles, "closed"
# (never an error) on released ones, class alone for tasks and sentinels.

test_that("channel handles print one line and survive release", {
  p <- channel_pair()
  expect_output(print(p[["host"]]), "<sora_channel .*rei_.*: host, open>")
  expect_output(print(p[["peer"]]), "<sora_channel .*rei_.*: peer, open>")
  # either side's close bit flips the state before release
  .Call(sora:::sora_channel_close_signal, p[["peer"]])
  expect_output(print(p[["host"]]), "<sora_channel .*: host, closed>")
  expect_true(sora_close(p[["host"]], timeout = 5))
  expect_true(sora_close(p[["peer"]], timeout = 5))
  expect_output(print(p[["host"]]), "<sora_channel: closed>", fixed = TRUE)
})

test_that("pool and task handles print one line and survive destroy", {
  p <- pool_pair(workers = 2L)
  expect_output(
    print(p[["ctrl"]]),
    "<sora_pool .*rei_.*: controller, 2/2 workers live, 0 pending>"
  )
  expect_output(print(p[["wk"]]), "<sora_pool .*: worker, 2/2 workers live")
  t <- sora_submit(p[["ctrl"]], 1 + 1)
  expect_output(print(t), "<sora_task: pending>", fixed = TRUE)
  expect_output(print(p[["ctrl"]]), "1 pending")
  pool_step(p)
  expect_output(print(t), "<sora_task: ok>", fixed = TRUE)
  expect_identical(sora_collect(t), 2)
  expect_output(print(t), "<sora_task: collected>", fixed = TRUE)
  # the probe consumes nothing: an err result still collects as the error
  t2 <- sora_submit(p[["ctrl"]], stop("boom"))
  pool_step(p)
  expect_output(print(t2), "<sora_task: err>", fixed = TRUE)
  expect_error(sora_collect(t2), "boom")
  # cancel state, and every handle reads dropped once the pool is gone
  t3 <- sora_submit(p[["ctrl"]], 1)
  sora_cancel(t3)
  expect_output(print(t3), "<sora_task: cancel>", fixed = TRUE)
  pool_end(p)
  expect_output(print(t3), "<sora_task: dropped>", fixed = TRUE)
  expect_output(print(p[["ctrl"]]), "<sora_pool: closed>", fixed = TRUE)
})

test_that("sentinels print as their class", {
  p <- channel_pair()
  s <- sora_recv(p[["host"]], timeout = 0)
  expect_s3_class(s, "sora_timeout")
  expect_output(print(s), "<sora_timeout>", fixed = TRUE)
})

test_that("a finalized in-process peer handle reads closed at print", {
  p <- channel_pair()
  host <- p[["host"]]
  rm(p)
  invisible(gc()) # the peer handle's finalizer signals close
  expect_output(print(host), ": host, closed>", fixed = TRUE)
  expect_true(sora_close(host, timeout = 5))
})

test_that("a channel whose peer is gone prints the verdict", {
  skip_if_no_child_sora()
  ch <- sora_channel(quote(Sys.sleep(30)))
  kill_hard(.Call(sora:::sora_channel_stat, ch)[["peer_pid"]])
  expect_true(wait_until(!sora_alive(ch)))
  expect_output(print(ch), "peer gone", fixed = TRUE)
  expect_true(sora_close(ch, timeout = 5))
})

test_that("a prepared map prints its staging", {
  p <- pool_pair()
  f <- function(i) i + 1L
  environment(f) <- globalenv()
  pm <- sora_map_prepare(p[["ctrl"]], 1:4, f)
  expect_output(print(pm), "inline blob", fixed = TRUE)
  pool_end(p)
})
