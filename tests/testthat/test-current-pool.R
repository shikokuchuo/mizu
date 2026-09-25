# mizu_current_pool(): the runtime-owned accessor for the evaluating
# worker's own pool handle. `NULL` outside a task is a state, not an
# error — pinned by the save/restore in the C exec hook, exercised in
# process by the pool_pair()/pool_step() harness.

test_that("mizu_current_pool() is NULL outside a task", {
  # a prior file's erroring stepped task may have stranded the borrowed
  # global on a since-unreachable worker: its finalizer clears it
  gc()
  expect_null(mizu_current_pool())
})

test_that("mizu_current_pool() does not stick after a stepped task", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], 1 + 1)
  expect_identical(pool_step(p), 1L)
  expect_identical(mizu_collect(t, timeout = 5), 2)
  expect_null(mizu_current_pool())
  pool_end(p)
})

test_that("a task sees a pool handle that submits onto its worker's deque", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], {
    pool <- mizu_current_pool()
    s <- mizu_submit(pool, "nested")
    list(
      is_pool = inherits(pool, "mizu_pool"),
      deque = mizu_pool_status(pool)[["deque"]],
      value = mizu_collect(s, timeout = 5)
    )
  })
  expect_identical(pool_step(p), 1L)
  expect_identical(
    mizu_collect(t, timeout = 5),
    list(is_pool = TRUE, deque = 1, value = "nested")
  )
  pool_end(p)
})
