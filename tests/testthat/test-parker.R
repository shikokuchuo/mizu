# Per-entity parker: epoch word + shared futex / __ulock(SHARED) / named
# auto-reset events. Directed wakes only; spurious wakes are absorbed by the
# caller's re-check (park itself may return WOKEN early at worst).

test_that("an undisturbed park times out on schedule", {
  xp <- .Call(kioto:::kio_region_create, 4096)
  t0 <- proc.time()[[3]]
  rc <- .Call(kioto:::kio_park_call, xp, 0L, 200L, TRUE)
  elapsed <- proc.time()[[3]] - t0
  expect_identical(rc, 1L)
  expect_gte(elapsed, 0.15)
})

test_that("a zero timeout polls without sleeping", {
  xp <- .Call(kioto:::kio_region_create, 4096)
  t0 <- proc.time()[[3]]
  rc <- .Call(kioto:::kio_park_call, xp, 0L, 0L, TRUE)
  expect_identical(rc, 1L)
  expect_lt(proc.time()[[3]] - t0, 0.1)
})

test_that("unpark advances the epoch monotonically", {
  xp <- .Call(kioto:::kio_region_create, 4096)
  expect_identical(.Call(kioto:::kio_epoch_call, xp, 0L), 0)
  .Call(kioto:::kio_unpark_call, xp, 0L, TRUE)
  .Call(kioto:::kio_unpark_call, xp, 0L, TRUE)
  expect_identical(.Call(kioto:::kio_epoch_call, xp, 0L), 2)
  expect_identical(.Call(kioto:::kio_epoch_call, xp, 1L), 0)
})

test_that("entities outside the layout are rejected", {
  xp <- .Call(kioto:::kio_region_create, 4096)
  expect_error(.Call(kioto:::kio_park_call, xp, 2L, 0L, TRUE), "invalid entity")
  small <- .Call(kioto:::kio_region_create, 128)
  expect_error(.Call(kioto:::kio_park_call, small, 0L, 0L, TRUE),
               "too small for an entity block")
})

test_that("a directed unpark from another process wakes a parked waiter", {
  skip_if_no_child_kioto()
  xp <- .Call(kioto:::kio_region_create, 4096)
  nm <- .Call(kioto:::kio_region_name, xp)

  # materialize the (Windows) event before the child can try to open it
  .Call(kioto:::kio_park_call, xp, 0L, 0L, TRUE)

  kioto:::kio_spawn(sprintf('
    library(kioto)
    rw <- .Call(kioto:::kio_region_open, %s, TRUE)
    Sys.sleep(0.3)
    .Call(kioto:::kio_unpark_call, rw, 0L, FALSE)
  ', deparse(nm)))

  t0 <- proc.time()[[3]]
  rc <- .Call(kioto:::kio_park_call, xp, 0L, 30000L, TRUE)
  elapsed <- proc.time()[[3]] - t0
  expect_identical(rc, 0L)
  expect_lt(elapsed, 25)
  expect_identical(.Call(kioto:::kio_epoch_call, xp, 0L), 1)
})
