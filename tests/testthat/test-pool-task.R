# Task flow over the in-process harness: submit into the injection ring,
# single-step worker execution, result publication and collect, the cancel
# state machine, and the payload-lifetime keeper discipline. Every wait here
# is deterministic — the worker runs only when the test steps it.

test_that("a task round-trips every result payload kind", {
  p <- pool_pair()
  tasks <- list(
    raw = sora_submit(p[["ctrl"]], x * 2L, x = 21L), # RAWVEC
    inline = sora_submit(p[["ctrl"]], list(a = x, b = "y"), x = 1), # INLINE
    shm = sora_submit(p[["ctrl"]], seq_len(n) + 0, n = 100000L) # SHM_RAW
  )
  expect_identical(sora_pool_status(p[["ctrl"]])[["injection"]], 3)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(sora_collect(tasks[["raw"]], timeout = 5), 42L)
  expect_identical(sora_collect(tasks[["inline"]], timeout = 5), list(a = 1, b = "y"))
  expect_identical(
    sora_collect(tasks[["shm"]], timeout = 5),
    as.double(seq_len(100000L))
  )
  pool_end(p)
})

test_that("task arguments arrive as the only bindings", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], sort(ls(environment())), a = 1, b = 2)
  pool_step(p)
  expect_identical(sora_collect(t, timeout = 5), c("a", "b"))
  expect_error(sora_submit(p[["ctrl"]], x + 1, 5), "must be named")
  pool_end(p)
})

test_that("large task payloads travel by region and round-trip", {
  p <- pool_pair() # default 256 B slots: this spills to SHM_RAW
  big <- runif(100000)
  t <- sora_submit(p[["ctrl"]], sum(v), v = big)
  pool_step(p)
  expect_identical(sora_collect(t, timeout = 5), sum(big))
  pool_end(p)
})

test_that("large inline entries survive ring and deque claims", {
  p <- pool_pair(
    max_submitters = 1L,
    injection_cap = 2L,
    per_worker_cap = 2L,
    result_slots = 2L,
    slot_size = 1048576L
  )
  big <- runif(100000)
  t <- sora_submit(p[["ctrl"]], v, v = big)
  expect_identical(pool_pull(p, 1L), 1L)
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(t, timeout = 5), big)
  pool_end(p)
})

test_that("a task error is published and re-signalled at collect", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], stop("boom ", x), x = "today")
  pool_step(p)
  err <- tryCatch(sora_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "simpleError")
  expect_identical(conditionMessage(err), "boom today")
  # the slot released with the collect: reusable immediately
  expect_identical(unname(sora_pool_status(p[["ctrl"]])[["tasks"]]), rep(0L, 5L))
  pool_end(p)
})

test_that("a classed condition re-signals with class and fields intact", {
  p <- pool_pair()
  cond <- structure(
    list(message = "typed", call = NULL, data = 42L),
    class = c("sora_test_error", "error", "condition")
  )
  t <- sora_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(sora_collect(t, timeout = 5), error = identity)
  expect_identical(class(err), c("sora_test_error", "error", "condition"))
  expect_identical(conditionMessage(err), "typed")
  expect_identical(err[["data"]], 42L)
  pool_end(p)
})

test_that("only error conditions fail a task: a warning passes through", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], {
    warning("advisory only")
    "completed"
  })
  expect_warning(pool_step(p), "advisory only")
  expect_identical(sora_collect(t, timeout = 5), "completed")
  pool_end(p)
})

test_that("collect times out with the sentinel and later succeeds", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], "done")
  expect_s3_class(sora_collect(t, timeout = 0), c("sora_timeout", "sora_sentinel"))
  expect_s3_class(sora_collect(t, timeout = 0.1), "sora_timeout")
  pool_step(p)
  expect_identical(sora_collect(t, timeout = 5), "done")
  pool_end(p)
})

test_that("a handle collects exactly once", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], 1L)
  pool_step(p)
  expect_identical(sora_collect(t, timeout = 5), 1L)
  expect_error(sora_collect(t, timeout = 5), "already collected")
  pool_end(p)
})

test_that("cancel discards a still-queued task; the worker frees the slot", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], stop("never runs"))
  expect_true(sora_cancel(t))
  expect_false(sora_cancel(t)) # already cancelled
  expect_error(sora_collect(t, timeout = 5), "cancelled")
  expect_identical(sora_pool_status(p[["ctrl"]])[["tasks"]][["cancel"]], 1L)
  # the queued entry is consumed later; only then does CANCEL become FREE
  pool_step(p)
  expect_identical(unname(sora_pool_status(p[["ctrl"]])[["tasks"]]), rep(0L, 5L))
  pool_end(p)
})

test_that("cancel is too late once the task has completed", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], "ran")
  pool_step(p)
  expect_false(sora_cancel(t))
  expect_identical(sora_collect(t, timeout = 5), "ran")
  pool_end(p)
})

test_that("a dropped handle cancels its pending task at finalization", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], "orphaned")
  rm(t)
  gc()
  expect_identical(sora_pool_status(p[["ctrl"]])[["tasks"]][["cancel"]], 1L)
  pool_step(p) # consume + free
  expect_identical(unname(sora_pool_status(p[["ctrl"]])[["tasks"]]), rep(0L, 5L))
  pool_end(p)
})

test_that("a dropped handle frees an uncollected published result", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], "never collected")
  pool_step(p)
  expect_identical(sora_pool_status(p[["ctrl"]])[["tasks"]][["ok"]], 1L)
  rm(t)
  gc() # OK -> FREE without mapping
  expect_identical(unname(sora_pool_status(p[["ctrl"]])[["tasks"]]), rep(0L, 5L))
  pool_end(p)
})

test_that("result slots are bounded per submitter and reused after collect", {
  p <- pool_pair(max_submitters = 8L, result_slots = 16L) # 2 per submitter
  t1 <- sora_submit(p[["ctrl"]], 1L)
  t2 <- sora_submit(p[["ctrl"]], 2L)
  expect_error(sora_submit(p[["ctrl"]], 3L), "result slots exhausted")
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(sora_collect(t1, timeout = 5), 1L)
  # collect released a slot: the allocator reuses it
  t3 <- sora_submit(p[["ctrl"]], 3L)
  pool_step(p)
  expect_identical(sora_collect(t2, timeout = 5), 2L)
  expect_identical(sora_collect(t3, timeout = 5), 3L)
  pool_end(p)
})

test_that("injection back-pressure is per-submitter and error-bounded", {
  p <- pool_pair(injection_cap = 2L)
  t1 <- sora_submit(p[["ctrl"]], 1L)
  t2 <- sora_submit(p[["ctrl"]], 2L)
  expect_error(sora_submit(p[["ctrl"]], 3L, .timeout = 0.2), "submission timed out")
  # a worker pop frees exactly this ring's space
  pool_step(p)
  t3 <- sora_submit(p[["ctrl"]], 3L, .timeout = 0)
  while (pool_step(p) == 1L) {
    NULL
  }
  for (t in list(t1, t2, t3)) {
    expect_no_error(sora_collect(t, timeout = 5))
  }
  pool_end(p)
})

test_that("queued task payloads stay pinned across the sender's GC", {
  p <- pool_pair()
  v <- runif(100000) # SHM_RAW task payload
  s <- sum(v)
  t <- sora_submit(p[["ctrl"]], sum(v), v = v)
  rm(v)
  gc() # without the task keeper, the payload region would unlink here
  pool_step(p) # the worker's open must succeed
  expect_identical(sora_collect(t, timeout = 5), s)
  pool_end(p)
})

test_that("published results stay pinned across the worker's GC", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], seq_len(n) + 0, n = 100000L) # SHM_RAW result
  pool_step(p)
  gc() # without the result keeper, the result region would unlink here
  expect_identical(sora_collect(t, timeout = 5), as.double(seq_len(100000L)))
  pool_end(p)
})

test_that("a destroyed pool invalidates outstanding task handles", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], "never run") # no step: stays PENDING
  pool_end(p) # broadcast CANCELs it
  expect_error(sora_collect(t, timeout = 5), "pool handle is closed")
})

test_that("collect_try boxes a cancellation instead of raising", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], 1L)
  sora_cancel(t)
  v <- .Call(sora:::sora_pool_collect_try, t, 0)
  expect_s3_class(v, "sora_caught")
  expect_s3_class(v[[1L]], "sora_error_cancelled")
  pool_step(p)
  pool_end(p)
})

test_that("a corrupt task payload is infrastructure failure, not the task's", {
  p <- pool_pair()
  t1 <- .Call(sora:::sora_pool_submit, p[["ctrl"]], list(quote(1L), "args"), Inf, 0L)
  t2 <- .Call(
    sora:::sora_pool_submit,
    p[["ctrl"]],
    list(quote(1L), list(2L)),
    Inf,
    0L
  ) # unnamed argument list
  # the shape check fires before the in_eval gate arms: the error is not
  # attributable to the task and stays fatal to the step
  expect_error(pool_step(p), "corrupt task payload")
  expect_error(pool_step(p), "corrupt task payload")
  # the claims were consumed but never published: cancel the stranded slots
  sora_cancel(t1)
  sora_cancel(t2)
  # the next step heals the dangling announce; the worker keeps serving
  t3 <- sora_submit(p[["ctrl"]], "alive")
  pool_step(p)
  expect_identical(sora_collect(t3, timeout = 5), "alive")
  pool_end(p)
})

test_that("a trace-hook error outside any task eval is not attributed", {
  p <- pool_pair()
  sora_pool_trace(p[["wk"]], function(event, id) {
    if (event == "done") stop("hook boom")
  })
  t <- sora_submit(p[["ctrl"]], 42L)
  # the hook fires after the publish: run_outcome refuses the error and
  # the step's caller must treat it as infrastructure failure
  expect_error(pool_step(p), "hook boom")
  expect_identical(sora_collect(t, timeout = 5), 42L)
  sora_pool_trace(p[["wk"]], NULL)
  pool_end(p)
})

test_that("a stale task handle reads collected and errors on collect", {
  p <- pool_pair(result_slots = 8L) # one result slot per submitter
  t1 <- sora_submit(p[["ctrl"]], 1 + 1)
  pool_step(p)
  expect_identical(sora_collect(t1, 5), 2)
  t2 <- sora_submit(p[["ctrl"]], 2 + 2) # reuses the one slot, bumping its sequence
  expect_identical(.Call(sora:::sora_pool_task_state, t1), "collected")
  expect_false(sora_cancel(t1))
  expect_error(sora_collect(t1, 5), "already collected or invalidated")
  pool_step(p)
  expect_identical(sora_collect(t2, 5), 4)
  pool_end(p)
})

test_that("cancel is FALSE once collected and once the pool is gone", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], 1 + 1)
  pool_step(p)
  expect_identical(sora_collect(t, 5), 2)
  expect_false(sora_cancel(t))
  pool_end(p)
  expect_false(sora_cancel(t))
})

test_that("task_state rejects non-task handles", {
  expect_error(.Call(sora:::sora_pool_task_state, NULL), "not a task handle")
})
