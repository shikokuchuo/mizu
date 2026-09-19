# Per-entity parker: epoch word + shared futex / __ulock(SHARED) / named
# auto-reset events. Directed wakes only; spurious wakes are absorbed by the
# caller's re-check (park itself may return WOKEN early at worst).

test_that("an undisturbed park times out", {
  xp <- .Call(mizu:::mizu_region_create, 4096)
  rc <- .Call(mizu:::mizu_park_call, xp, 0L, 200L, TRUE)
  expect_identical(rc, 1L)
})

test_that("a zero timeout polls", {
  xp <- .Call(mizu:::mizu_region_create, 4096)
  rc <- .Call(mizu:::mizu_park_call, xp, 0L, 0L, TRUE)
  expect_identical(rc, 1L)
})

test_that("unpark advances the epoch monotonically", {
  xp <- .Call(mizu:::mizu_region_create, 4096)
  expect_identical(.Call(mizu:::mizu_epoch_call, xp, 0L), 0)
  .Call(mizu:::mizu_unpark_call, xp, 0L, TRUE)
  .Call(mizu:::mizu_unpark_call, xp, 0L, TRUE)
  expect_identical(.Call(mizu:::mizu_epoch_call, xp, 0L), 2)
  expect_identical(.Call(mizu:::mizu_epoch_call, xp, 1L), 0)
})

test_that("entities outside the layout are rejected", {
  xp <- .Call(mizu:::mizu_region_create, 4096)
  expect_error(.Call(mizu:::mizu_park_call, xp, 2L, 0L, TRUE), "invalid entity")
  small <- .Call(mizu:::mizu_region_create, 128)
  expect_error(
    .Call(mizu:::mizu_park_call, small, 0L, 0L, TRUE),
    "too small for an entity block"
  )
})

test_that("a directed unpark from another process wakes a parked waiter", {
  skip_if_no_child_mizu()
  xp <- .Call(mizu:::mizu_region_create, 4096)
  nm <- .Call(mizu:::mizu_region_name, xp)

  # materialize the (Windows) event before the child can try to open it
  .Call(mizu:::mizu_park_call, xp, 0L, 0L, TRUE)

  mizu:::mizu_spawn(sprintf(
    '
    library(mizu)
    rw <- .Call(mizu:::mizu_region_open, %s, TRUE)
    Sys.sleep(0.3)
    .Call(mizu:::mizu_unpark_call, rw, 0L, FALSE)
  ',
    deparse(nm)
  ))

  rc <- .Call(mizu:::mizu_park_call, xp, 0L, 30000L, TRUE)
  expect_identical(rc, 0L)
  expect_identical(.Call(mizu:::mizu_epoch_call, xp, 0L), 1)
})
