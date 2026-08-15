# Prepared maps: stage once, run many. The in-process tests drive the
# composable internals (rearm / submit / step / collect) against pool_pair
# for deterministic single-stepping; the blocking sora_map_run() surface is
# exercised end-to-end over spawned workers.

# collect under a file-wide 30s guard, as in test-map.R
collect30 <- function(pool, st)
  sora:::map_collect(st, deadline = sora:::mono_time() + 30)

test_that("a prepared map re-runs on one region under a bumped generation", {
  p <- pool_pair()
  pm <- sora_map_prepare(p[["ctrl"]], 1:6 + 0, function(i) i * 2)
  st <- pm[["st"]]
  name1 <- st[["name"]]
  expect_identical(.Call(sora:::sora_map_info, st[["wrap"]])[["generation"]], 0)
  # run 1, driven by hand — exactly one step: the single runner drains
  # the cursor, and an extra empty step would sweep the worker ctx cache
  sora:::map_rearm(p[["ctrl"]], st, NULL)
  expect_identical(.Call(sora:::sora_map_info, st[["wrap"]])[["generation"]], 1)
  sora:::map_submit(p[["ctrl"]], st)
  expect_identical(pool_step(p), 1L)
  expect_identical(collect30(p[["ctrl"]], st), as.list(1:6 * 2))
  cache <- .Call(sora:::sora_pool_map_cache, p[["wk"]])
  ctx1 <- get(name1, envir = cache)
  # run 2: same region and name, next generation, and the worker reuses
  # its cached context — no re-attach, no descriptor unserialize
  sora:::map_rearm(p[["ctrl"]], st, NULL)
  i <- .Call(sora:::sora_map_info, st[["wrap"]])
  expect_identical(i[["generation"]], 2)
  expect_identical(i[["cursor"]], 0)
  expect_identical(st[["name"]], name1)
  sora:::map_submit(p[["ctrl"]], st)
  expect_identical(pool_step(p), 1L)
  expect_identical(collect30(p[["ctrl"]], st), as.list(1:6 * 2))
  expect_true(identical(ctx1, get(name1, envir = cache)))
  pool_end(p)
})

test_that("a blob-sized prepared map resubmits its staged blob", {
  p <- pool_pair(slot_size = 512L)   # the default 480-byte entry budget
  f <- function(i) i + 1L
  environment(f) <- globalenv()
  pm <- sora_map_prepare(p[["ctrl"]], 1:4, f)
  expect_type(pm[["st"]][["blob"]], "raw")
  for (run in 1:2) {
    sora:::map_rearm(p[["ctrl"]], pm[["st"]], NULL)
    sora:::map_submit(p[["ctrl"]], pm[["st"]])
    while (pool_step(p) == 1L) NULL
    expect_identical(collect30(p[["ctrl"]], pm[["st"]]), as.list(2:5))
  }
  pool_end(p)
})

test_that("an empty prepared map runs without a pool round-trip", {
  p <- pool_pair()
  pm <- sora_map_prepare(p[["ctrl"]], list(), sqrt)
  expect_null(pm[["st"]])
  expect_identical(sora_map_run(pm), list())
  expect_identical(sora_map_run(pm), list())
  pool_end(p)
})

test_that("prepared handles print their region and staleness", {
  p <- pool_pair()
  pm <- sora_map_prepare(p[["ctrl"]], 1:4 + 0, identity)
  expect_output(print(pm), "4 elements")
  expect_output(print(pm), pm[["st"]][["name"]], fixed = TRUE)
  pm[["st"]] <- NULL
  expect_output(print(pm), "stale")
  expect_error(sora_map_run(list()), "not a prepared-map handle")
  pool_end(p)
})

test_that("prepared runs reuse the region and match sora_map exactly", {
  skip_if_no_child_sora()
  p <- sora_pool(n_workers = 2L)
  f <- function(i) rnorm(2L)
  x <- 1:8 + 0
  pm <- sora_map_prepare(p, x, f)
  name1 <- pm[["st"]][["name"]]
  r1 <- sora_map_run(pm, .seed = 7L)
  r2 <- sora_map_run(pm, .seed = 7L)
  expect_identical(r1, r2)                 # invariance across re-runs
  expect_identical(pm[["st"]][["name"]], name1)      # one region throughout
  expect_identical(r1, sora_map(p, x, f, .seed = 7L))
  expect_false(identical(sora_map_run(pm, .seed = 8L), r1))
  # the template path re-runs over the same output area, un-zeroed: a
  # clean prior run overwrote every element
  pm2 <- sora_map_prepare(p, x, function(i) i + 1, .template = numeric(1))
  expect_identical(sora_map_run(pm2), x + 1)
  expect_identical(sora_map_run(pm2), x + 1)
  expect_true(sora_pool_stop(p))
})

test_that("an unclean run marks the handle stale; the next run restages", {
  skip_if_no_child_sora()
  p <- sora_pool(n_workers = 1L)
  pm <- sora_map_prepare(p, 1:4 + 0, function(i) {
    Sys.sleep(i / 2)
    i
  })
  name1 <- pm[["st"]][["name"]]
  r <- sora_map_run(pm, .timeout = 0.3)
  expect_s3_class(r, "sora_timeout")
  expect_null(pm[["st"]])                       # stale: the next run restages
  r <- sora_map_run(pm, .timeout = 60)
  expect_identical(r, as.list(1:4 + 0))
  expect_false(identical(pm[["st"]][["name"]], name1))
  # an error in f is unclean too — and restaging reproduces it cleanly
  pm3 <- sora_map_prepare(p, 1:3 + 0,
                         function(i) if (i == 2) stop("bad") else i)
  expect_error(sora_map_run(pm3), "bad")
  expect_null(pm3[["st"]])
  expect_error(sora_map_run(pm3), "bad")
  expect_true(sora_pool_stop(p))
})

test_that("phase B: a same-shape x swaps in place; changes restage", {
  p <- pool_pair()
  x1 <- 1:6 + 0
  pm <- sora_map_prepare(p[["ctrl"]], x1, function(v) v * 10)
  name1 <- pm[["st"]][["name"]]
  # identical type and length: the new bytes memcpy over the region's x
  # section — same region, same name, no restage
  x2 <- rev(x1)
  sora:::map_swap_x(pm, x2)
  expect_identical(pm[["st"]][["name"]], name1)
  sora:::map_rearm(p[["ctrl"]], pm[["st"]], NULL)
  sora:::map_submit(p[["ctrl"]], pm[["st"]])
  while (pool_step(p) == 1L) NULL
  expect_identical(collect30(p[["ctrl"]], pm[["st"]]), as.list(x2 * 10))
  # names never cross the wire: they ride the handle for assembly
  x3 <- setNames(x1, letters[1:6])
  sora:::map_swap_x(pm, x3)
  expect_identical(pm[["st"]][["name"]], name1)
  sora:::map_rearm(p[["ctrl"]], pm[["st"]], NULL)
  sora:::map_submit(p[["ctrl"]], pm[["st"]])
  while (pool_step(p) == 1L) NULL
  expect_identical(collect30(p[["ctrl"]], pm[["st"]]), as.list(x3 * 10))
  # the C primitive rejects any shape or type mismatch outright
  expect_error(.Call(sora:::sora_map_swap_x, pm[["st"]][["wrap"]], 1:6),
               "must match the staged type and length")
  expect_error(.Call(sora:::sora_map_swap_x, pm[["st"]][["wrap"]], c(x1, 7)),
               "must match the staged type and length")
  # and the R surface turns those into a transparent restage
  sora:::map_swap_x(pm, 1:7)
  expect_null(pm[["st"]])
  pool_end(p)
})

test_that("sora_map_run validates .seed and surfaces rearm slot exhaustion", {
  p <- pool_pair()
  pm <- sora_map_prepare(p[["ctrl"]], 1:4, identity)
  expect_error(sora_map_run(pm, .seed = "x"), ".seed must be")
  held <- lapply(1:8, function(i) sora_submit(p[["ctrl"]], v, v = i))
  expect_error(sora_map_run(pm), class = "sora_error_slots_exhausted")
  # both raised before the reset touched anything: the staged state is
  # intact for a retry
  expect_false(is.null(pm[["st"]]))
  while (pool_step(p) == 1L) NULL
  for (i in 1:8) expect_identical(sora_collect(held[[i]], timeout = 5), i)
  sora:::map_rearm(p[["ctrl"]], pm[["st"]], NULL)
  sora:::map_submit(p[["ctrl"]], pm[["st"]])
  while (pool_step(p) == 1L) NULL
  expect_identical(collect30(p[["ctrl"]], pm[["st"]]), as.list(1:4))
  pool_end(p)
})

test_that("phase B: sora_map_run(pm, x =) round-trips end to end", {
  skip_if_no_child_sora()
  p <- sora_pool(n_workers = 2L)
  x1 <- runif(64)
  pm <- sora_map_prepare(p, x1, function(v) v + 1)
  name1 <- pm[["st"]][["name"]]
  expect_identical(sora_map_run(pm), as.list(x1 + 1))
  x2 <- runif(64)
  expect_identical(sora_map_run(pm, x = x2), as.list(x2 + 1))
  expect_identical(pm[["st"]][["name"]], name1)          # swapped, not restaged
  # a shape change restages transparently and still answers
  x3 <- runif(32)
  expect_identical(sora_map_run(pm, x = x3), as.list(x3 + 1))
  expect_false(identical(pm[["st"]][["name"]], name1))
  expect_true(sora_pool_stop(p))
})
