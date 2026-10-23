# The load-time platform gate (64-bit pointer width, then the pidfd probe
# on Linux) lives in mizu_onload_probe; the unload path (R_unload_mizu) is
# exercised only by an actual DLL unload.

test_that(".onLoad's platform probe passes on this platform", {
  expect_null(mizu:::.onLoad(NULL, "mizu"))
})

test_that("a forced DLL unload runs the teardown and leaves a clean process", {
  skip_if_no_child_mizu()
  f <- tfile()
  mizu:::mizu_spawn(sprintf(
    '
    library(mizu)
    p <- mizu_pool(n_workers = 1L)
    t <- mizu_submit(p, 40 + 2)
    stopifnot(identical(mizu_collect(t, 10), 42))
    mizu_pool_stop(p)
    ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch, 5))))
    mizu_send(ch, 1:10)
    stopifnot(identical(mizu_recv(ch, 10), 1:10))
    mizu_close(ch, 10)
    gc()
    dyn.unload(getLoadedDLLs()[["mizu"]][["path"]])
    stopifnot(is.null(getLoadedDLLs()[["mizu"]]))
    writeLines("ok", %s)
  ',
    deparse(f)
  ))
  expect_true(wait_for_file(f, 30))
  expect_identical(readLines(f), "ok")
})
