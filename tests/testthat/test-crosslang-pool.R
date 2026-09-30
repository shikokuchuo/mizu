test_that("an R submitter drives a Python worker pool (both kinds)", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  # name kind, with result values per the interchange table
  t1 <- mizu_submit_call(p, mizu_call("numpy.mean", c(1, 2, 3, 4)))
  expect_equal(mizu_collect(t1), 2.5)
  t2 <- mizu_submit_call(
    p,
    mizu_call("numpy.quantile", c(1, 2, 3), q = c(0.25, 0.5, 0.75))
  )
  expect_equal(mizu_collect(t2), c(1.5, 2, 2.5))
  # source kind: statement prefix + trailing expression -> the value
  src <- paste(
    "import numpy as np",
    "x = np.asarray(x, dtype=np.float64) * 2",
    "x.sum().item()",
    sep = "\n"
  )
  t3 <- mizu_submit_call(p, mizu_call(source = src, x = 1:5))
  expect_equal(mizu_collect(t3), 30)
  # statements only -> NULL
  t4 <- mizu_submit_call(p, mizu_call(source = "y = 1"))
  expect_null(mizu_collect(t4))
  # a Python list result crosses as an R list of scalars
  t5 <- mizu_submit_call(p, mizu_call(source = "[1.5, 'two', None]"))
  expect_identical(mizu_collect(t5), list(1.5, "two", NULL))
})

test_that("errors cross with remote_type, and result portability is gated", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(1L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  t1 <- mizu_submit_call(p, mizu_call("builtins.len", 1.5))
  cnd <- tryCatch(mizu_collect(t1), mizu_error_remote = function(c) c)
  expect_s3_class(cnd, "mizu_error_remote")
  expect_equal(cnd$remote_type, "TypeError")
  # a non-portable *argument* is a submit-time error; a bare name too
  expect_error(
    mizu_submit_call(p, mizu_call("builtins.repr", lm(mpg ~ wt, mtcars))),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_submit_call(p, mizu_call("len", 1:3)),
    "qualified name"
  )
  # a non-portable *result* fails the task with an error stream
  t3 <- mizu_submit_call(p, mizu_call(source = "{1, 2, 3}"))
  c3 <- tryCatch(mizu_collect(t3), mizu_error_remote = function(c) c)
  expect_s3_class(c3, "mizu_error_remote")
  expect_equal(c3$remote_type, "mizu_error_not_portable")

  # nested submit inside a foreign-run task
  src <- paste0(
    "import pymizu\n",
    "pool = pymizu.current_pool()\n",
    "t = pool.submit(abs, -42)\n",
    "t.collect()"
  )
  t4 <- mizu_submit_call(p, mizu_call(source = src))
  expect_identical(mizu_collect(t4), 42L)

  # collect_any / collect_all across languages
  t5 <- mizu_submit_call(p, mizu_call(source = "1 + 1"))
  t6 <- mizu_submit_call(p, mizu_call(source = "raise ValueError('boom')"))
  r <- mizu_collect_any(list(t5, t6))
  expect_identical(r[["index"]], 1L)
  expect_identical(r[["value"]], 2)
  t7 <- mizu_submit_call(p, mizu_call(source = "1 + 1"))
  t8 <- mizu_submit_call(p, mizu_call(source = "raise ValueError('boom')"))
  cnd2 <- tryCatch(
    mizu_collect_all(list(t7, t8)),
    mizu_error_remote = function(c) c
  )
  expect_s3_class(cnd2, "mizu_error_remote")
  expect_equal(cnd2[["index"]], 2L)
  expect_equal(cnd2$remote_type, "ValueError")
  expect_identical(mizu_collect(t7), 2)

  d <- mizu_pool_dump(p)
  expect_identical(d[["language"]], "Python")
})

test_that("a dead Python worker surfaces as the died condition", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(1L, launcher = launcher)
  on.exit(mizu_pool_stop(p))
  pid <- mizu_pool_status(p)[["workers"]][["pid"]][1L]
  t <- mizu_submit_call(p, mizu_call(source = "import time\ntime.sleep(30)"))
  expect_true(wait_until(any(
    mizu_pool_dump(p)[["workers"]][["in_flight"]] != -1L
  )))
  kill_hard(pid)
  cnd <- tryCatch(mizu_collect(t), mizu_error_died = function(c) c)
  expect_s3_class(cnd, "mizu_error_died")
})
