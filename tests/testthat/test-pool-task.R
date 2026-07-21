# Task flow over the in-process harness: submit into the injection ring,
# single-step worker execution, result publication and collect, the cancel
# state machine, and the payload-lifetime keeper discipline. Every wait here
# is deterministic — the worker runs only when the test steps it.

test_that("a task round-trips every result payload kind", {
  p <- pool_pair()
  tasks <- list(
    raw = mov_submit(p$ctrl, x * 2L, x = 21L),                 # RAWVEC
    inline = mov_submit(p$ctrl, list(a = x, b = "y"), x = 1),  # INLINE
    shm = mov_submit(p$ctrl, seq_len(n) + 0, n = 100000L)      # SHM_RAW
  )
  expect_identical(mov_pool_status(p$ctrl)$injection, 3)
  while (pool_step(p) == 1L) NULL
  expect_identical(mov_collect(tasks$raw, timeout = 5), 42L)
  expect_identical(mov_collect(tasks$inline, timeout = 5),
                   list(a = 1, b = "y"))
  expect_identical(mov_collect(tasks$shm, timeout = 5),
                   as.double(seq_len(100000L)))
  pool_end(p)
})

test_that("task arguments arrive as the only bindings", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, sort(ls(environment())), a = 1, b = 2)
  pool_step(p)
  expect_identical(mov_collect(t, timeout = 5), c("a", "b"))
  expect_error(mov_submit(p$ctrl, x + 1, 5), "must be named")
  pool_end(p)
})

test_that("large task payloads travel by region and round-trip", {
  p <- pool_pair()   # default 256 B slots: this spills to SHM_RAW
  big <- runif(100000)
  t <- mov_submit(p$ctrl, sum(v), v = big)
  pool_step(p)
  expect_identical(mov_collect(t, timeout = 5), sum(big))
  pool_end(p)
})

test_that("a task error is published and re-signalled at collect", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, stop("boom ", x), x = "today")
  pool_step(p)
  err <- tryCatch(mov_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "simpleError")
  expect_identical(conditionMessage(err), "boom today")
  # the slot released with the collect: reusable immediately
  expect_identical(unname(mov_pool_status(p$ctrl)$tasks), rep(0L, 5L))
  pool_end(p)
})

test_that("collect times out with the sentinel and later succeeds", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, "done")
  expect_s3_class(mov_collect(t, timeout = 0), c("mov_timeout",
                                                 "mov_condition"))
  expect_s3_class(mov_collect(t, timeout = 0.1), "mov_timeout")
  pool_step(p)
  expect_identical(mov_collect(t, timeout = 5), "done")
  pool_end(p)
})

test_that("a handle collects exactly once", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, 1L)
  pool_step(p)
  expect_identical(mov_collect(t, timeout = 5), 1L)
  expect_error(mov_collect(t, timeout = 5), "already collected")
  pool_end(p)
})

test_that("cancel discards a still-queued task; the worker frees the slot", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, stop("never runs"))
  expect_true(mov_cancel(t))
  expect_false(mov_cancel(t))                    # already cancelled
  expect_error(mov_collect(t, timeout = 5), "cancelled")
  expect_identical(mov_pool_status(p$ctrl)$tasks[["cancel"]], 1L)
  # the queued entry is consumed later; only then does CANCEL become FREE
  pool_step(p)
  expect_identical(unname(mov_pool_status(p$ctrl)$tasks), rep(0L, 5L))
  pool_end(p)
})

test_that("cancel is too late once the task has completed", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, "ran")
  pool_step(p)
  expect_false(mov_cancel(t))
  expect_identical(mov_collect(t, timeout = 5), "ran")
  pool_end(p)
})

test_that("a dropped handle cancels its pending task at finalization", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, "orphaned")
  rm(t)
  gc()
  expect_identical(mov_pool_status(p$ctrl)$tasks[["cancel"]], 1L)
  pool_step(p)                                   # consume + free
  expect_identical(unname(mov_pool_status(p$ctrl)$tasks), rep(0L, 5L))
  pool_end(p)
})

test_that("a dropped handle frees an uncollected published result", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, "never collected")
  pool_step(p)
  expect_identical(mov_pool_status(p$ctrl)$tasks[["ok"]], 1L)
  rm(t)
  gc()                                           # OK -> FREE without mapping
  expect_identical(unname(mov_pool_status(p$ctrl)$tasks), rep(0L, 5L))
  pool_end(p)
})

test_that("result slots are bounded per submitter and reused after collect", {
  p <- pool_pair(max_submitters = 8L, result_slots = 16L)   # 2 per submitter
  t1 <- mov_submit(p$ctrl, 1L)
  t2 <- mov_submit(p$ctrl, 2L)
  expect_error(mov_submit(p$ctrl, 3L), "result slots exhausted")
  while (pool_step(p) == 1L) NULL
  expect_identical(mov_collect(t1, timeout = 5), 1L)
  # collect released a slot: the allocator reuses it
  t3 <- mov_submit(p$ctrl, 3L)
  pool_step(p)
  expect_identical(mov_collect(t2, timeout = 5), 2L)
  expect_identical(mov_collect(t3, timeout = 5), 3L)
  pool_end(p)
})

test_that("injection back-pressure is per-submitter and error-bounded", {
  p <- pool_pair(injection_cap = 2L)
  t1 <- mov_submit(p$ctrl, 1L)
  t2 <- mov_submit(p$ctrl, 2L)
  expect_error(mov_submit(p$ctrl, 3L, .timeout = 0.2),
               "submission timed out")
  # a worker pop frees exactly this ring's space
  pool_step(p)
  t3 <- mov_submit(p$ctrl, 3L, .timeout = 0)
  while (pool_step(p) == 1L) NULL
  for (t in list(t1, t2, t3)) expect_no_error(mov_collect(t, timeout = 5))
  pool_end(p)
})

test_that("queued task payloads stay pinned across the sender's GC", {
  p <- pool_pair()
  v <- runif(100000)                     # SHM_RAW task payload
  s <- sum(v)
  t <- mov_submit(p$ctrl, sum(v), v = v)
  rm(v)
  gc()   # without the task keeper, the payload region would unlink here
  pool_step(p)                           # the worker's open must succeed
  expect_identical(mov_collect(t, timeout = 5), s)
  pool_end(p)
})

test_that("published results stay pinned across the worker's GC", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, seq_len(n) + 0, n = 100000L)   # SHM_RAW result
  pool_step(p)
  gc()   # without the result keeper, the result region would unlink here
  expect_identical(mov_collect(t, timeout = 5), as.double(seq_len(100000L)))
  pool_end(p)
})

test_that("a destroyed pool invalidates outstanding task handles", {
  p <- pool_pair()
  t <- mov_submit(p$ctrl, "never run")   # no step: stays PENDING
  pool_end(p)                            # broadcast CANCELs it
  expect_error(mov_collect(t, timeout = 5), "pool handle is closed")
})
