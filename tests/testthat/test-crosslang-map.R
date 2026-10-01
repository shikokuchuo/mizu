# Cross-language maps (Phase 5): an R submitter mapping mizu_call() specs
# over a Python worker pool. Same-language spec maps are test-map.R's.

test_that("name and source kinds return elements in order", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  x <- c(1, 4, 9, 16)
  expect_equal(unlist(mizu_map(p, x, mizu_call("numpy.sqrt"))), sqrt(x))
  expect_identical(
    unlist(mizu_map(p, 1:5, mizu_call(source = "x * 2"))),
    1:5 * 2L
  )
  # the element binds as x; named constants as names; positional as _1
  expect_identical(
    unlist(mizu_map(p, as.list(1:5), mizu_call(source = "x + k", k = 10L))),
    1:5 + 10L
  )
  expect_identical(
    unlist(mizu_map(p, 1:3, mizu_call(name = NULL, 10L, source = "x * _1"))),
    1:3 * 10L
  )
  # a statement prefix runs per element; the trailing expression's value
  src <- paste("import math", "math.floor(x)", sep = "\n")
  expect_equal(
    unlist(mizu_map(p, c(1.7, 2.3), mizu_call(source = src))),
    c(1, 2)
  )
})

test_that("template and view collect work across languages", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  x <- runif(8, 1, 10)
  expect_equal(
    mizu_map(p, x, mizu_call("numpy.log"), .template = numeric(1)),
    log(x)
  )
  v <- mizu_map(
    p,
    x,
    mizu_call("numpy.log"),
    .template = numeric(1),
    .collect = "view"
  )
  expect_equal(as.numeric(v), log(x))
  # an integer template across languages
  expect_identical(
    mizu_map(p, 1:4, mizu_call("builtins.int"), .template = integer(1)),
    1:4
  )
})

test_that("a per-element error crosses with its element index", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  src <- paste("if x == 5:", "    raise ValueError('boom')", "x", sep = "\n")
  cnd <- tryCatch(
    mizu_map(p, 1:10, mizu_call(source = src)),
    mizu_error_remote = function(c) c
  )
  expect_s3_class(cnd, "mizu_error_remote")
  expect_identical(cnd[["index"]], 5L) # 1-based here, 0-based on the Python side
  expect_match(conditionMessage(cnd), "boom")
})

test_that("seed invariance holds within a worker language", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  f <- mizu_call(source = "import random\nrandom.random()")
  a <- mizu_map(p, 1:30, f, .seed = 42)
  b <- mizu_map(p, 1:30, f, .seed = 42, .chunks = 7)
  expect_identical(a, b)
  # the split-map contract across the neutral (seed, offset) pair
  whole <- mizu_map(p, 1:40, f, .seed = 7)
  split <- c(
    mizu_map(p, 1:20, f, .seed = 7),
    mizu_map(p, 21:40, f, .seed = c(7, 20))
  )
  expect_identical(unlist(split), unlist(whole))
})

test_that("a prepared spec map re-runs under fresh seeds without restaging", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(2L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  pm <- mizu_map_prepare(
    p,
    1:12,
    mizu_call(source = "import random\nrandom.random()")
  )
  name <- pm[["st"]][["name"]]
  r1 <- mizu_map_run(pm, .seed = 1)
  expect_identical(pm[["st"]][["name"]], name)
  r2 <- mizu_map_run(pm, .seed = 2)
  expect_identical(pm[["st"]][["name"]], name)
  expect_false(identical(unlist(r1), unlist(r2)))
  expect_identical(mizu_map_run(pm, .seed = 1), r1)
})

test_that("a killed Python worker's lost-set scan reports the elements", {
  py <- skip_if_no_pymizu()
  launcher <- mizu_py_pool_launcher(py, stdout = FALSE, stderr = FALSE)
  p <- mizu_pool(1L, launcher = launcher)
  on.exit(mizu_pool_stop(p))

  pid <- mizu_pool_dump(p)[["workers"]][["pid"]][1L]
  src <- "import time\ntime.sleep(0.2)\nx"
  st <- mizu:::map_stage(p, 1:40, mizu_call(source = src), list(), chunks = 8)
  mizu:::map_submit(p, st)
  # the kill must land after the runner's first issue: the lost set is
  # issued-minus-published, and an unissued runner loses nothing
  expect_true(
    wait_until(.Call(mizu:::mizu_map_info, st[["wrap"]])[["cursor"]] > 0)
  )
  kill_hard(pid)
  cnd <- tryCatch(
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    mizu_error_worker_died = function(c) c
  )
  expect_s3_class(cnd, "mizu_error_worker_died")
  elts <- cnd[["elements"]]
  expect_gte(nrow(elts), 1)
  expect_gte(min(elts[, "lo"]), 1)
  expect_lte(max(elts[, "hi"]), 40)
})
