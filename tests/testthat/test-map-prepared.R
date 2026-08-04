# Prepared maps: stage once, run many. The in-process tests drive the
# composable internals (rearm / submit / step / collect) against pool_pair
# for deterministic single-stepping; the blocking kio_map_run() surface is
# exercised end-to-end over spawned workers.

# collect under a file-wide 30s guard, as in test-map.R
collect30 <- function(pool, st)
  kioto:::map_collect(pool, st, deadline = kioto:::mono_time() + 30)

test_that("a prepared map re-runs on one region under a bumped generation", {
  p <- pool_pair()
  pm <- kio_map_prepare(p$ctrl, 1:6 + 0, function(i) i * 2)
  st <- pm$st
  name1 <- st$name
  expect_identical(.Call(kioto:::kio_map_info, st$wrap)$generation, 0)
  # run 1, driven by hand — exactly one step: the single runner drains
  # the cursor, and an extra empty step would sweep the worker ctx cache
  kioto:::map_rearm(p$ctrl, st, NULL)
  expect_identical(.Call(kioto:::kio_map_info, st$wrap)$generation, 1)
  kioto:::map_submit(p$ctrl, st)
  expect_identical(pool_step(p), 1L)
  expect_identical(collect30(p$ctrl, st), as.list(1:6 * 2))
  cache <- .Call(kioto:::kio_pool_map_cache, p$wk)
  ctx1 <- get(name1, envir = cache)
  # run 2: same region and name, next generation, and the worker reuses
  # its cached context — no re-attach, no descriptor unserialize
  kioto:::map_rearm(p$ctrl, st, NULL)
  i <- .Call(kioto:::kio_map_info, st$wrap)
  expect_identical(i$generation, 2)
  expect_identical(i$cursor, 0)
  expect_identical(st$name, name1)
  kioto:::map_submit(p$ctrl, st)
  expect_identical(pool_step(p), 1L)
  expect_identical(collect30(p$ctrl, st), as.list(1:6 * 2))
  expect_true(identical(ctx1, get(name1, envir = cache)))
  pool_end(p)
})

test_that("a blob-sized prepared map resubmits its staged blob", {
  p <- pool_pair(slot_size = 512L)   # the default 480-byte entry budget
  f <- function(i) i + 1L
  environment(f) <- globalenv()
  pm <- kio_map_prepare(p$ctrl, 1:4, f)
  expect_type(pm$st$blob, "raw")
  for (run in 1:2) {
    kioto:::map_rearm(p$ctrl, pm$st, NULL)
    kioto:::map_submit(p$ctrl, pm$st)
    while (pool_step(p) == 1L) NULL
    expect_identical(collect30(p$ctrl, pm$st), as.list(2:5))
  }
  pool_end(p)
})

test_that("an empty prepared map runs without a pool round-trip", {
  p <- pool_pair()
  pm <- kio_map_prepare(p$ctrl, list(), sqrt)
  expect_null(pm$st)
  expect_identical(kio_map_run(pm), list())
  expect_identical(kio_map_run(pm), list())
  pool_end(p)
})

test_that("prepared handles print their region and staleness", {
  p <- pool_pair()
  pm <- kio_map_prepare(p$ctrl, 1:4 + 0, identity)
  expect_output(print(pm), "4 elements")
  expect_output(print(pm), pm$st$name, fixed = TRUE)
  pm$st <- NULL
  expect_output(print(pm), "stale")
  expect_error(kio_map_run(list()), "not a prepared-map handle")
  pool_end(p)
})

test_that("prepared runs reuse the region and match kio_map exactly", {
  skip_if_no_child_kioto()
  p <- kio_pool(n_workers = 2L)
  f <- function(i) rnorm(2L)
  x <- 1:8 + 0
  pm <- kio_map_prepare(p, x, f)
  name1 <- pm$st$name
  r1 <- kio_map_run(pm, .seed = 7L)
  r2 <- kio_map_run(pm, .seed = 7L)
  expect_identical(r1, r2)                 # invariance across re-runs
  expect_identical(pm$st$name, name1)      # one region throughout
  expect_identical(r1, kio_map(p, x, f, .seed = 7L))
  expect_false(identical(kio_map_run(pm, .seed = 8L), r1))
  # the template path re-runs over the same output area, un-zeroed: a
  # clean prior run overwrote every element
  pm2 <- kio_map_prepare(p, x, function(i) i + 1, .template = numeric(1))
  expect_identical(kio_map_run(pm2), x + 1)
  expect_identical(kio_map_run(pm2), x + 1)
  expect_true(kio_pool_stop(p))
})

test_that("an unclean run marks the handle stale; the next run restages", {
  skip_if_no_child_kioto()
  p <- kio_pool(n_workers = 1L)
  pm <- kio_map_prepare(p, 1:4 + 0, function(i) {
    Sys.sleep(i / 2)
    i
  })
  name1 <- pm$st$name
  r <- kio_map_run(pm, .timeout = 0.3)
  expect_s3_class(r, "kio_timeout")
  expect_null(pm$st)                       # stale: the next run restages
  r <- kio_map_run(pm, .timeout = 60)
  expect_identical(r, as.list(1:4 + 0))
  expect_false(identical(pm$st$name, name1))
  # an error in f is unclean too — and restaging reproduces it cleanly
  pm3 <- kio_map_prepare(p, 1:3 + 0,
                         function(i) if (i == 2) stop("bad") else i)
  expect_error(kio_map_run(pm3), "bad")
  expect_null(pm3$st)
  expect_error(kio_map_run(pm3), "bad")
  expect_true(kio_pool_stop(p))
})
