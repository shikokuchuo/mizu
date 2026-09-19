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
  p <- pool_pair(max_submitters = 8L, result_slots = 16L)   # 2 per submitter
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  t2 <- mizu_submit(p[["ctrl"]], 2L)
  expect_error(mizu_submit(p[["ctrl"]], 3L), class = "mizu_error_slots_exhausted")
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
    mizu_pool(n_workers = 1L, launcher = function(token, slot) NULL,
             startup_timeout = 0.5),
    class = "mizu_error_startup")
  expect_error(
    mizu_channel(quote({}), launcher = function(token) NULL,
                startup_timeout = 0.5),
    class = "mizu_error_startup")
})

test_that("region open failure raises mizu_error_shm with NA bytes", {
  err <- tryCatch(.Call(mizu:::mizu_region_open, "/mizu_nonexistent_0", FALSE),
                  error = identity)
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
