# Classed error conditions (?mizu_error): the contract is class dispatch —
# inherits() / tryCatch(mizu_error... = ) — plus structured fields, never
# message text. map.R is the first in-tree consumer (submit-timeout and
# worker-died dispatch), so these classes are regression-locked here.

test_that("submit timeout raises mizu_error_submit_timeout", {
  p <- pool_pair(injection_cap = 2L)
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  t2 <- mizu_submit(p[["ctrl"]], 2L)
  err <- tryCatch(mizu_submit(p[["ctrl"]], 3L, .timeout = 0), error = identity)
  expect_s3_class(err, "mizu_error_submit_timeout")
  expect_s3_class(err, "mizu_error")
  expect_match(conditionMessage(err), "submission timed out")
  pool_end(p)
})

test_that("result-slot exhaustion raises mizu_error_slots_exhausted", {
  p <- pool_pair(max_submitters = 8L, result_slots = 16L) # 2 per submitter
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  t2 <- mizu_submit(p[["ctrl"]], 2L)
  expect_error(
    mizu_submit(p[["ctrl"]], 3L),
    class = "mizu_error_slots_exhausted"
  )
  pool_end(p)
})

test_that("submitting to a stopped pool raises mizu_error_stopped", {
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  sub <- mizu_pool_attach(mizu_pool_status(ctrl)[["name"]])
  mizu_pool_stop(ctrl, timeout = 0)
  expect_error(mizu_submit(sub, 1L), class = "mizu_error_stopped")
})

test_that("cancelled collect raises mizu_error_cancelled; mizu_error catches", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], 1L)
  mizu_cancel(t)
  err <- tryCatch(mizu_collect(t, timeout = 5), mizu_error = identity)
  expect_s3_class(err, "mizu_error_cancelled")
  pool_step(p)
  pool_end(p)
})

test_that("startup timeout raises mizu_error_startup", {
  expect_error(
    mizu_pool(
      n_workers = 1L,
      launcher = function(token, slot) NULL,
      startup_timeout = 0.5
    ),
    class = "mizu_error_startup"
  )
  expect_error(
    mizu_channel(
      quote({}),
      launcher = function(token) NULL,
      startup_timeout = 0.5
    ),
    class = "mizu_error_startup"
  )
})

test_that("region open failure raises mizu_error_shm with NA bytes", {
  err <- tryCatch(
    .Call(mizu:::mizu_region_open, "/mizu_nonexistent_0", FALSE),
    error = identity
  )
  expect_s3_class(err, "mizu_error_shm")
  expect_s3_class(err, "mizu_error")
  expect_true(is.na(err[["bytes"]]))
})

test_that("region create failure raises mizu_error_shm carrying bytes", {
  # 2^52 bytes passes argument validation but no platform can map it
  err <- tryCatch(.Call(mizu:::mizu_region_create, 2^52), error = identity)
  expect_s3_class(err, "mizu_error_shm")
  expect_identical(err[["bytes"]], 2^52)
})

test_that("channel create failure raises mizu_error_shm carrying bytes", {
  # the ~2.2 TB region a 1.1e12 arena implies: Linux's tmpfs refuses the
  # ftruncate out of hand; macOS and Windows map lazily, so the create
  # there may not fail at all
  skip_on_os(c("mac", "windows"))
  err <- tryCatch(
    .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 1.1e12, FALSE),
    error = function(e) e
  )
  expect_s3_class(err, "mizu_error_shm")
  expect_gte(err[["bytes"]], 2 * 1.1e12)
})

test_that("a task value classed mizu_caught collects verbatim", {
  p <- pool_pair()
  on.exit(pool_end(p))
  value <- structure(list(simpleError("boom")), class = "mizu_caught")
  t <- mizu_submit(
    p$ctrl,
    structure(list(simpleError("boom")), class = "mizu_caught")
  )
  pool_step(p)
  expect_identical(mizu_collect(t, 5), value)
  t <- mizu_submit(
    p$ctrl,
    structure(list(simpleError("boom")), class = "mizu_caught")
  )
  pool_step(p)
  expect_identical(
    mizu_collect_any(list(t), 5),
    list(index = 1L, value = value)
  )
  t <- mizu_submit(
    p$ctrl,
    structure(list(simpleError("boom")), class = "mizu_caught")
  )
  pool_step(p)
  expect_identical(mizu_collect_all(list(t), 5), list(value))
})

test_that("mizu_error_remote: class test, message and re-raise dispatch", {
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
  expect_true(mizu_is_remote_error(e))
  expect_false(mizu_is_remote_error(errorCondition("boom")))
  expect_false(mizu_is_remote_error(42))
  expect_identical(conditionMessage(e), "ValueError: boom")
  raised <- tryCatch(mizu_raise(e), mizu_error_remote = identity)
  expect_identical(raised, e)
  also <- tryCatch(mizu_raise(e), mizu_error = identity)
  expect_identical(also, e)
  expect_error(mizu_raise(errorCondition("boom")), "mizu_error_remote")
})

test_that("an oversized task error flattens to the budget, dropping fields", {
  p <- pool_pair(slot_size = 256L)
  t <- mizu_submit(p$ctrl, {
    big <- paste(rep("f", 400), collapse = "")
    stop(errorCondition("boom", f1 = big, f2 = big, f3 = big))
  })
  pool_step(p)
  err <- tryCatch(mizu_collect(t, 5), error = function(e) e)
  expect_s3_class(err, "error")
  expect_identical(conditionMessage(err), "boom")
  expect_identical(err[["dropped_fields"]], c("f1", "f2", "f3"))
  # a class vector so large even the reduced form overflows: fallback text
  t2 <- mizu_submit(p$ctrl, {
    klass <- paste0("k", seq_len(8L), strrep("_", 38L))
    stop(errorCondition("boom2", f1 = "x", class = klass))
  })
  pool_step(p)
  err2 <- tryCatch(mizu_collect(t2, 5), error = function(e) e)
  expect_identical(
    conditionMessage(err2),
    "mizu: task error (untransportable condition)"
  )
  expect_null(err2[["f1"]])
  pool_end(p)
})

test_that("collect_all re-stamps a condition that already carries an index", {
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(
    p$ctrl,
    stop(errorCondition("boom", index = 7L, mizu_map_index = 7L))
  )
  pool_step(p)
  err <- tryCatch(mizu_collect_all(list(t)), error = function(e) e)
  expect_identical(err[["index"]], 1L)
  expect_identical(err[["mizu_map_index"]], 7L)
})

test_that("a task error with a non-string message flattens to the fallback", {
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(
    p$ctrl,
    stop(errorCondition(c("boom", "boom"), extra = 1L))
  )
  pool_step(p)
  err <- tryCatch(mizu_collect(t, 5), error = function(e) e)
  expect_identical(
    conditionMessage(err),
    "mizu: task error (untransportable condition)"
  )
  expect_identical(err[["extra"]], 1L)
})

test_that("a condition the assembly cannot carry keeps its dropped names", {
  p <- pool_pair(slot_size = 8192L)
  on.exit(pool_end(p))
  # the field probes within the depth cap alone, but the assembly's own
  # level pushes it past: the verification pass takes the terminal fallback
  deep <- 1L
  for (i in seq_len(500L)) {
    deep <- list(deep)
  }
  cond <- errorCondition("boom", deep = deep, big = strrep("x", 9000))
  t <- mizu_submit(p$ctrl, stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, 5), error = function(e) e)
  expect_identical(
    conditionMessage(err),
    "mizu: task error (untransportable condition)"
  )
  expect_identical(err[["dropped_fields"]], "big")
})

test_that("a task error with no message field flattens to the fallback", {
  p <- pool_pair()
  t <- mizu_submit(p$ctrl, stop(errorCondition(NULL)))
  pool_step(p)
  err <- tryCatch(mizu_collect(t, 5), error = function(e) e)
  expect_s3_class(err, "error")
  expect_identical(
    conditionMessage(err),
    "mizu: task error (untransportable condition)"
  )
  pool_end(p)
})
