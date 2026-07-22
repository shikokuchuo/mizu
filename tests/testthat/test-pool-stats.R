# Phase 5 observability: cumulative per-worker stats (mirrored into the
# region only at park / fairness-tick / leave cadence, never per task),
# submitter counters derived from the injection rings' own monotonic
# positions, and per-handle trace hooks over the task lifecycle. All
# in-process via pool_pair: the single-step harness makes both the counts
# and the publish cadence deterministic.

test_that("worker stats count tasks and publish at park cadence, not per task", {
  p <- pool_pair()
  zero <- mov_pool_stats(p$ctrl)$workers
  expect_identical(zero$tasks, 0)
  expect_identical(zero$injections, 0)
  expect_identical(zero$parks, 0)

  ts <- lapply(1:3, function(i) mov_submit(p$ctrl, 1 + 1))
  for (i in 1:3) expect_identical(pool_step(p), 1L)
  # executed but not yet mirrored: publication waits for a park or tick
  expect_identical(mov_pool_stats(p$ctrl)$workers$tasks, 0)

  expect_identical(pool_step(p), 0L)        # no work: publishes on return
  st <- mov_pool_stats(p$ctrl)$workers
  expect_identical(st$tasks, 3)
  expect_identical(st$injections, 3)
  expect_identical(st$steals, 0)
  expect_identical(st$parks, 0)             # timeout = 0 never parks

  # a timed idle step parks and republishes on the way out; Windows tick
  # quantization can split one idle step into several short parks
  expect_identical(pool_step(p, timeout = 0.05), 0L)
  expect_gte(mov_pool_stats(p$ctrl)$workers$parks, 1)

  for (t in ts) expect_identical(mov_collect(t, 5), 2)
  pool_end(p)
})

test_that("steals and helps are counted against the claiming worker", {
  p <- pool_pair(workers = 2L)
  ts <- lapply(1:2, function(i) mov_submit(p$ctrl, x, x = i))
  expect_identical(pool_pull(p, 2L), 2L)    # onto worker 0's own deque
  expect_identical(pool_step(p, wk = p$wks[[2]]), 1L)
  expect_identical(pool_step(p, wk = p$wks[[2]]), 1L)
  expect_identical(pool_step(p, wk = p$wks[[2]]), 0L)
  expect_identical(pool_step(p), 0L)        # worker 0 publishes too
  st <- mov_pool_stats(p$ctrl)$workers
  expect_identical(st$tasks, c(0, 2))
  expect_identical(st$steals, c(0, 2))
  expect_identical(st$injections, c(2, 0))  # the pull claimed the ring
  for (t in ts) mov_collect(t, 5)

  # helps: a worker blocked in a nested collect executes its own subtask
  t <- mov_submit(p$ctrl, mov_collect(mov_submit(pool, 2 + 2), timeout = 5))
  expect_identical(pool_step(p), 1L)
  expect_identical(mov_collect(t, 5), 4)
  expect_identical(pool_step(p), 0L)
  st <- mov_pool_stats(p$ctrl)$workers
  expect_identical(st$helps[1], 1)
  expect_identical(st$tasks[1], 2)          # outer + helped subtask
  pool_end(p)
})

test_that("submitter counters are the ring positions: exact, no cadence", {
  p <- pool_pair()
  ts <- lapply(1:2, function(i) mov_submit(p$ctrl, x, x = i))
  st <- mov_pool_stats(p$ctrl)$submitters
  expect_identical(st$injected[1], 2)
  expect_identical(st$claimed[1], 0)
  expect_identical(st$queued[1], 2)
  expect_identical(pool_step(p), 1L)
  expect_identical(pool_step(p), 1L)
  st <- mov_pool_stats(p$ctrl)$submitters
  expect_identical(st$claimed[1], 2)
  expect_identical(st$queued[1], 0)
  for (t in ts) mov_collect(t, 5)
  pool_end(p)
})

test_that("a departed worker's stats are exact; a rejoining one resets them", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, 1 + 1)
  expect_identical(pool_step(p), 1L)
  mov_collect(t, 5)
  .Call(mov:::mov_pool_leave, p$wk)         # publishes the final mirror
  expect_identical(mov_pool_stats(p$ctrl)$workers$tasks, 1)

  suffix <- .Call(mov:::mov_pool_suffix, p$ctrl)
  wk2 <- .Call(mov:::mov_pool_worker_join, suffix, 0L)
  expect_identical(mov_pool_stats(p$ctrl)$workers$tasks, 0)
  .Call(mov:::mov_pool_leave, wk2)
  .Call(mov:::mov_pool_destroy, p$ctrl)
})

test_that("trace hooks see the task lifecycle in order, with stable ids", {
  p <- pool_pair()
  log <- new.env()
  log$ev <- character()
  fn <- function(event, id) log$ev <- c(log$ev, paste(event, id))
  mov_pool_trace(p$ctrl, fn)
  mov_pool_trace(p$wk, fn)

  t <- mov_submit(p$ctrl, 1 + 1)
  expect_identical(pool_step(p), 1L)
  expect_identical(mov_collect(t, 5), 2)
  expect_identical(log$ev, c("submit 0:1", "start 0:1", "done 0:1"))

  # an erroring task publishes ERR: the end event is "error"
  log$ev <- character()
  t <- mov_submit(p$ctrl, stop("boom"))
  expect_identical(pool_step(p), 1L)
  expect_error(mov_collect(t, 5), "boom")
  expect_identical(log$ev, c("submit 0:2", "start 0:2", "error 0:2"))

  # a cancelled entry is discarded at its claim: no start, one drop
  log$ev <- character()
  t <- mov_submit(p$ctrl, 1 + 1)
  mov_cancel(t)
  expect_identical(pool_step(p), 1L)
  expect_identical(log$ev, c("submit 0:3", "drop 0:3"))

  # removing the hook silences both sides
  mov_pool_trace(p$ctrl, NULL)
  mov_pool_trace(p$wk, NULL)
  log$ev <- character()
  t <- mov_submit(p$ctrl, 1 + 1)
  expect_identical(pool_step(p), 1L)
  expect_identical(mov_collect(t, 5), 2)
  expect_identical(log$ev, character())
  pool_end(p)
})

test_that("nested submits trace under the worker's own submitter identity", {
  p <- pool_pair()
  log <- new.env()
  log$ev <- character()
  mov_pool_trace(p$wk, function(event, id)
    log$ev <- c(log$ev, paste(event, id)))
  t <- mov_submit(p$ctrl, mov_collect(mov_submit(pool, 2 + 2), timeout = 5))
  expect_identical(pool_step(p), 1L)
  expect_identical(mov_collect(t, 5), 4)
  # outer task from submitter 0; the nested one from the worker's lazily
  # claimed slot (1), executed by help mode inside the outer's collect
  expect_identical(log$ev, c("start 0:1", "submit 1:1", "start 1:1",
                             "done 1:1", "done 0:1"))
  pool_end(p)
})

test_that("an error from a worker-side hook is infrastructure failure", {
  p <- pool_pair()
  mov_pool_trace(p$wk, function(event, id) stop("hook boom"))
  t <- mov_submit(p$ctrl, "never published")
  # the hook errors at the "start" site, outside any task eval: the step
  # raises — the worker-fatal path — and nothing publishes to the slot
  expect_error(pool_step(p), "hook boom")
  expect_s3_class(mov_collect(t, timeout = 0), "mov_timeout")
  mov_pool_trace(p$wk, NULL)
  pool_end(p)
})

test_that("mov_pool_trace validates its hook argument", {
  p <- pool_pair()
  expect_error(mov_pool_trace(p$ctrl, "not a function"),
               "expected a function or NULL")
  pool_end(p)
})
