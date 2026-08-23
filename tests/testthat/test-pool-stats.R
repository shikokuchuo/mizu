# Phase 5 observability: cumulative per-worker stats (mirrored into the
# region only at park / fairness-tick / leave cadence, never per task),
# submitter counters derived from the injection rings' own monotonic
# positions, and per-handle trace hooks over the task lifecycle. All
# in-process via pool_pair: the single-step harness makes both the counts
# and the publish cadence deterministic.

test_that("worker stats count tasks and publish at park cadence, not per task", {
  p <- pool_pair()
  zero <- rei_pool_stats(p[["ctrl"]])[["workers"]]
  expect_identical(zero[["tasks"]], 0)
  expect_identical(zero[["injections"]], 0)
  expect_identical(zero[["parks"]], 0)

  ts <- lapply(1:3, function(i) rei_submit(p[["ctrl"]], 1 + 1))
  for (i in 1:3) {
    expect_identical(pool_step(p), 1L)
  }
  # executed but not yet mirrored: publication waits for a park or tick
  expect_identical(rei_pool_stats(p[["ctrl"]])[["workers"]][["tasks"]], 0)

  expect_identical(pool_step(p), 0L) # no work: publishes on return
  st <- rei_pool_stats(p[["ctrl"]])[["workers"]]
  expect_identical(st[["tasks"]], 3)
  expect_identical(st[["injections"]], 3)
  expect_identical(st[["steals"]], 0)
  expect_identical(st[["parks"]], 0) # timeout = 0 never parks

  # a timed idle step parks and republishes on the way out; Windows tick
  # quantization can split one idle step into several short parks
  expect_identical(pool_step(p, timeout = 0.05), 0L)
  expect_gte(rei_pool_stats(p[["ctrl"]])[["workers"]][["parks"]], 1)

  for (t in ts) {
    expect_identical(rei_collect(t, 5), 2)
  }
  pool_end(p)
})

test_that("steals and helps are counted against the claiming worker", {
  p <- pool_pair(workers = 2L)
  ts <- lapply(1:2, function(i) rei_submit(p[["ctrl"]], x, x = i))
  expect_identical(pool_pull(p, 2L), 2L) # onto worker 0's own deque
  expect_identical(pool_step(p, wk = p[["wks"]][[2]]), 1L)
  expect_identical(pool_step(p, wk = p[["wks"]][[2]]), 1L)
  expect_identical(pool_step(p, wk = p[["wks"]][[2]]), 0L)
  expect_identical(pool_step(p), 0L) # worker 0 publishes too
  st <- rei_pool_stats(p[["ctrl"]])[["workers"]]
  expect_identical(st[["tasks"]], c(0, 2))
  expect_identical(st[["steals"]], c(0, 2))
  expect_identical(st[["injections"]], c(2, 0)) # the pull claimed the ring
  for (t in ts) {
    rei_collect(t, 5)
  }

  # helps: a worker blocked in a nested collect executes its own subtask
  t <- rei_submit(
    p[["ctrl"]],
    rei_collect(rei_submit(pool, 2 + 2), timeout = 5)
  )
  expect_identical(pool_step(p), 1L)
  expect_identical(rei_collect(t, 5), 4)
  expect_identical(pool_step(p), 0L)
  st <- rei_pool_stats(p[["ctrl"]])[["workers"]]
  expect_identical(st[["helps"]][1], 1)
  expect_identical(st[["tasks"]][1], 2) # outer + helped subtask
  pool_end(p)
})

test_that("submitter counters are the ring positions: exact, no cadence", {
  p <- pool_pair()
  ts <- lapply(1:2, function(i) rei_submit(p[["ctrl"]], x, x = i))
  st <- rei_pool_stats(p[["ctrl"]])[["submitters"]]
  expect_identical(st[["injected"]][1], 2)
  expect_identical(st[["claimed"]][1], 0)
  expect_identical(st[["queued"]][1], 2)
  expect_identical(pool_step(p), 1L)
  expect_identical(pool_step(p), 1L)
  st <- rei_pool_stats(p[["ctrl"]])[["submitters"]]
  expect_identical(st[["claimed"]][1], 2)
  expect_identical(st[["queued"]][1], 0)
  for (t in ts) {
    rei_collect(t, 5)
  }
  pool_end(p)
})

test_that("spills count SHM_RAW payloads against the task's submitter", {
  p <- pool_pair() # 512 B slots
  expect_identical(
    rei_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1],
    0
  )

  # inline traffic leaves the counter untouched
  t0 <- rei_submit(p[["ctrl"]], x + 1L, x = 1L)
  pool_step(p)
  rei_collect(t0, 5)
  expect_identical(
    rei_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1],
    0
  )

  # an oversized task payload spills at submit, exact and cadence-free
  v <- runif(100000)
  t1 <- rei_submit(p[["ctrl"]], sum(v), v = v)
  expect_identical(
    rei_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1],
    1
  )
  pool_step(p)
  rei_collect(t1, 5) # scalar result: no second spill
  expect_identical(
    rei_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1],
    1
  )

  # an oversized result spills at publish, attributed to the submitter
  t2 <- rei_submit(p[["ctrl"]], seq_len(n) + 0, n = 100000L)
  pool_step(p)
  rei_collect(t2, 5)
  st <- rei_pool_stats(p[["ctrl"]])[["submitters"]]
  expect_identical(st[["spills"]][1], 2)
  # one submit-side and one publish-side spill, in different directions and
  # from cold free lists on both handles: each cost a fresh region
  expect_identical(st[["spill_reuse"]][1], 0)
  pool_end(p)
})

test_that("a departed worker's stats are exact; a rejoining one resets them", {
  p <- pool_pair()
  t <- rei_submit(p[["ctrl"]], 1 + 1)
  expect_identical(pool_step(p), 1L)
  rei_collect(t, 5)
  .Call(rei:::rei_pool_leave, p[["wk"]]) # publishes the final mirror
  expect_identical(rei_pool_stats(p[["ctrl"]])[["workers"]][["tasks"]], 1)

  suffix <- .Call(rei:::rei_pool_suffix, p[["ctrl"]])
  wk2 <- .Call(rei:::rei_pool_worker_join, suffix, 0L)
  expect_identical(rei_pool_stats(p[["ctrl"]])[["workers"]][["tasks"]], 0)
  .Call(rei:::rei_pool_leave, wk2)
  .Call(rei:::rei_pool_destroy, p[["ctrl"]])
})

test_that("trace hooks see the task lifecycle in order, with stable ids", {
  p <- pool_pair()
  log <- new.env()
  log[["ev"]] <- character()
  fn <- function(event, id) log[["ev"]] <- c(log[["ev"]], paste(event, id))
  rei_pool_trace(p[["ctrl"]], fn)
  rei_pool_trace(p[["wk"]], fn)

  t <- rei_submit(p[["ctrl"]], 1 + 1)
  expect_identical(pool_step(p), 1L)
  expect_identical(rei_collect(t, 5), 2)
  expect_identical(log[["ev"]], c("submit 0:1", "start 0:1", "done 0:1"))

  # an erroring task publishes ERR: the end event is "error"
  log[["ev"]] <- character()
  t <- rei_submit(p[["ctrl"]], stop("boom"))
  expect_identical(pool_step(p), 1L)
  expect_error(rei_collect(t, 5), "boom")
  expect_identical(log[["ev"]], c("submit 0:2", "start 0:2", "error 0:2"))

  # a cancelled entry is discarded at its claim: no start, one drop
  log[["ev"]] <- character()
  t <- rei_submit(p[["ctrl"]], 1 + 1)
  rei_cancel(t)
  expect_identical(pool_step(p), 1L)
  expect_identical(log[["ev"]], c("submit 0:3", "drop 0:3"))

  # removing the hook silences both sides
  rei_pool_trace(p[["ctrl"]], NULL)
  rei_pool_trace(p[["wk"]], NULL)
  log[["ev"]] <- character()
  t <- rei_submit(p[["ctrl"]], 1 + 1)
  expect_identical(pool_step(p), 1L)
  expect_identical(rei_collect(t, 5), 2)
  expect_identical(log[["ev"]], character())
  pool_end(p)
})

test_that("nested submits trace under the worker's own submitter identity", {
  p <- pool_pair()
  log <- new.env()
  log[["ev"]] <- character()
  rei_pool_trace(p[["wk"]], function(event, id) {
    log[["ev"]] <- c(log[["ev"]], paste(event, id))
  })
  t <- rei_submit(
    p[["ctrl"]],
    rei_collect(rei_submit(pool, 2 + 2), timeout = 5)
  )
  expect_identical(pool_step(p), 1L)
  expect_identical(rei_collect(t, 5), 4)
  # outer task from submitter 0; the nested one from the worker's lazily
  # claimed slot (1), executed by help mode inside the outer's collect
  expect_identical(
    log[["ev"]],
    c("start 0:1", "submit 1:1", "start 1:1", "done 1:1", "done 0:1")
  )
  pool_end(p)
})

test_that("an error from a worker-side hook is infrastructure failure", {
  p <- pool_pair()
  rei_pool_trace(p[["wk"]], function(event, id) stop("hook boom"))
  t <- rei_submit(p[["ctrl"]], "never published")
  # the hook errors at the "start" site, outside any task eval: the step
  # raises — the worker-fatal path — and nothing publishes to the slot
  expect_error(pool_step(p), "hook boom")
  expect_s3_class(rei_collect(t, timeout = 0), "rei_timeout")
  rei_pool_trace(p[["wk"]], NULL)
  pool_end(p)
})

test_that("rei_pool_trace validates its hook argument", {
  p <- pool_pair()
  expect_error(
    rei_pool_trace(p[["ctrl"]], "not a function"),
    "expected a function or NULL"
  )
  pool_end(p)
})
