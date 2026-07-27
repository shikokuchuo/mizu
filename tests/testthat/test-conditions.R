# Classed error conditions (?kio_error): the contract is class dispatch —
# inherits() / tryCatch(kio_error... = ) — plus structured fields, never
# message text. map.R is the first in-tree consumer (submit-timeout and
# worker-died dispatch), so these classes are regression-locked here.

test_that("submit timeout raises kio_error_submit_timeout", {
  p <- pool_pair(injection_cap = 2L)
  t1 <- kio_submit(p$ctrl, 1L)
  t2 <- kio_submit(p$ctrl, 2L)
  err <- tryCatch(kio_submit(p$ctrl, 3L, .timeout = 0), error = identity)
  expect_s3_class(err, "kio_error_submit_timeout")
  expect_s3_class(err, "kio_error")
  expect_match(conditionMessage(err), "submission timed out")
  pool_end(p)
})

test_that("result-slot exhaustion raises kio_error_slots_exhausted", {
  p <- pool_pair(max_submitters = 8L, result_slots = 16L)   # 2 per submitter
  t1 <- kio_submit(p$ctrl, 1L)
  t2 <- kio_submit(p$ctrl, 2L)
  expect_error(kio_submit(p$ctrl, 3L), class = "kio_error_slots_exhausted")
  pool_end(p)
})

test_that("submitting to a stopped pool raises kio_error_stopped", {
  ctrl <- .Call(kioto:::kio_pool_create, 1L, 8L, 64L, 64L, 64L, 256L,
                tempdir())
  sub <- kio_pool_attach(kio_pool_status(ctrl)$name)
  kio_pool_stop(ctrl, timeout = 0)
  expect_error(kio_submit(sub, 1L), class = "kio_error_stopped")
})

test_that("cancelled collect raises kio_error_cancelled; kio_error catches", {
  p <- pool_pair()
  t <- kio_submit(p$ctrl, 1L)
  kio_cancel(t)
  err <- tryCatch(kio_collect(t, timeout = 5), kio_error = identity)
  expect_s3_class(err, "kio_error_cancelled")
  pool_step(p)
  pool_end(p)
})

test_that("startup timeout raises kio_error_startup", {
  expect_error(
    kio_pool(n_workers = 1L, launcher = function(suffix, slot) NULL,
             startup_timeout = 0.5),
    class = "kio_error_startup")
  expect_error(
    kio_channel(quote({}), launcher = function(suffix) NULL,
                startup_timeout = 0.5),
    class = "kio_error_startup")
})

test_that("region open failure raises kio_error_shm with NA bytes", {
  err <- tryCatch(.Call(kioto:::kio_region_open, "/kio_nonexistent_0", FALSE),
                  error = identity)
  expect_s3_class(err, "kio_error_shm")
  expect_s3_class(err, "kio_error")
  expect_true(is.na(err$bytes))
})

test_that("region create failure raises kio_error_shm carrying bytes", {
  # 2^52 bytes passes argument validation but no platform can map it
  err <- tryCatch(.Call(kioto:::kio_region_create, 2^52), error = identity)
  expect_s3_class(err, "kio_error_shm")
  expect_identical(err$bytes, 2^52)
})
