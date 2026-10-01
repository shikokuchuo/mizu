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
  expect_equal(c3$remote_type, "DeclinedError")

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
  expect_identical(r[["value"]], 2L)
  t7 <- mizu_submit_call(p, mizu_call(source = "1 + 1"))
  t8 <- mizu_submit_call(p, mizu_call(source = "raise ValueError('boom')"))
  cnd2 <- tryCatch(
    mizu_collect_all(list(t7, t8)),
    mizu_error_remote = function(c) c
  )
  expect_s3_class(cnd2, "mizu_error_remote")
  expect_equal(cnd2[["index"]], 2L)
  expect_equal(cnd2$remote_type, "ValueError")
  expect_identical(mizu_collect(t7), 2L)

  d <- mizu_pool_dump(p)
  expect_identical(d[["language"]], "Python")
})

test_that("a dead Python worker surfaces as the died condition", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(1L, launcher = launcher)
  on.exit(mizu_pool_stop(p))
  pid <- mizu_pool_dump(p)[["workers"]][["pid"]][1L]
  t <- mizu_submit_call(p, mizu_call(source = "import time\ntime.sleep(30)"))
  expect_true(wait_until(any(
    mizu_pool_dump(p)[["workers"]][["in_flight"]] != -1L
  )))
  kill_hard(pid)
  cnd <- tryCatch(mizu_collect(t), mizu_error_worker_died = function(c) c)
  expect_s3_class(cnd, "mizu_error_worker_died")
})

test_that("a large array argument crosses to Python workers by reference", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  big <- runif(200000L) # 1.6 MB — one SHM_VEC layout write, zero stream bytes
  t1 <- mizu_submit_call(p, mizu_call("numpy.mean", big))
  expect_equal(mizu_collect(t1), mean(big))
  # the worker proves the zero-copy arrival: the array's .base is the view
  t2 <- mizu_submit_call(
    p,
    mizu_call(NULL, big, source = "type(_1.base).__name__")
  )
  expect_identical(mizu_collect(t2), "_ShmView")

  # a received view re-sent as an argument: REF, REFHELD, the loan balanced
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  mizu_send(ch$host, big)
  xv <- mizu_recv(ch$peer, Inf)
  rc0 <- .Call(mizu:::mizu_zc_refcount, xv)
  t3 <- mizu_submit_call(p, mizu_call("numpy.mean", xv))
  expect_equal(mizu_collect(t3), mean(big))
  # the worker's resolve release lands after its result publish
  expect_true(wait_until(.Call(mizu:::mizu_zc_refcount, xv)[1L] == rc0[1L]))
  rc1 <- .Call(mizu:::mizu_zc_refcount, xv)
  expect_identical(rc1[2L], 1L) # REFHELD

  # result-is-the-arg: arrives intact, a view, elevated by our own add
  t4 <- mizu_submit_call(p, mizu_call(NULL, xv, source = "_1"))
  res <- mizu_collect(t4)
  expect_true(.Call(mizu:::mizu_zc_view_check, res))
  expect_true(wait_until(
    .Call(mizu:::mizu_zc_refcount, res)[1L] == rc1[1L] + 1L
  ))
  expect_identical(res, big)
  channel_end(ch)
})

test_that("multiple ref args and a nested view cross to Python workers", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  a <- matrix(runif(40000L), nrow = 200L)
  b <- matrix(runif(40000L), nrow = 200L)
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  mizu_send(ch$host, a)
  va <- mizu_recv(ch$peer, Inf)
  mizu_send(ch$host, b)
  vb <- mizu_recv(ch$peer, Inf)
  # two positional REFs and a fresh SHM_VEC, one nested in a list
  big <- runif(100000L)
  t1 <- mizu_submit_call(p, mizu_call("numpy.dot", va, vb))
  expect_equal(mizu_collect(t1), a %*% b)
  # the fresh array arrives as a _ShmView; the attributed matrix views
  # arrive as arrays over their regions (.base set — zero-copy)
  t2 <- mizu_submit_call(
    p,
    mizu_call(
      NULL,
      list(va),
      big,
      z = vb,
      source = paste(
        "type(_2.base).__name__ + '/' + ",
        "str(_1[0].base is not None) + '/' + str(z.base is not None)"
      )
    )
  )
  expect_identical(mizu_collect(t2), "_ShmView/True/True")
  channel_end(ch)
})

test_that("a list-tree view arg resolves to the element view on Python workers", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(1L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  big <- replicate(2000L, runif(100L), simplify = FALSE)
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  mizu_send(ch$host, big)
  lv <- mizu_recv(ch$peer, Inf)
  ev <- lv[[2L]]
  expect_true(.Call(mizu:::mizu_zc_view_check, ev))
  expect_true(grepl("[", .Call(mizu:::mizu_zc_view_name, ev), fixed = TRUE))
  t <- mizu_submit_call(p, mizu_call("numpy.mean", ev))
  expect_equal(mizu_collect(t), mean(big[[2L]]))
  channel_end(ch)
})

test_that("a worker killed mid-task leaks one bounded count, REFHELD set", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  big <- runif(200000L)
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  mizu_send(ch$host, big)
  xv <- mizu_recv(ch$peer, Inf)
  rc0 <- .Call(mizu:::mizu_zc_refcount, xv)
  t1 <- mizu_submit_call(
    p,
    mizu_call(NULL, xv, source = "import time\ntime.sleep(30)")
  )
  expect_true(wait_until(any(
    mizu_pool_dump(p)[["workers"]][["in_flight"]] != -1L
  )))
  d <- mizu_pool_dump(p)[["workers"]]
  pid <- d[["pid"]][which(d[["in_flight"]] != -1L)[1L]]
  kill_hard(pid)
  cnd <- tryCatch(mizu_collect(t1), mizu_error_worker_died = function(c) c)
  expect_s3_class(cnd, "mizu_error_worker_died")
  rc1 <- .Call(mizu:::mizu_zc_refcount, xv)
  expect_identical(rc1[1L], rc0[1L] + 1L) # the dead worker's add leaks one
  expect_identical(rc1[2L], 1L) # REFHELD
  # no free-list corruption: the surviving worker runs the next task
  t2 <- mizu_submit_call(p, mizu_call("numpy.mean", xv))
  expect_equal(mizu_collect(t2), mean(big))
  channel_end(ch)
})

test_that("a view x crosses to foreign map workers as a ref (D6)", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  big <- runif(100000L)
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  mizu_send(ch$host, big)
  xv <- mizu_recv(ch$peer, Inf)
  rc0 <- .Call(mizu:::mizu_zc_refcount, xv)
  res <- mizu_map(p, xv, mizu_call("numpy.square"))
  expect_equal(unlist(res), big^2)
  rc1 <- .Call(mizu:::mizu_zc_refcount, xv)
  expect_identical(rc1[2L], 1L) # REFHELD
  # the workers' map contexts hold their resolved x views (pymizu's ctx
  # cache releases at map-context eviction or worker teardown — the
  # deterministic balance row is the in-process R-worker row in
  # test-call.R)
  expect_true(.Call(mizu:::mizu_zc_refcount, xv)[1L] > rc0[1L])
  channel_end(ch)
})
