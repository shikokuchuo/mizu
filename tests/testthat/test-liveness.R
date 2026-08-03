# flock / LockFileEx liveness primitive: fd-scoped, held for a process's
# lifetime, released by the kernel on any exit path. ACQUIRED on a probe
# means the previous holder is dead; HELD means alive.

test_that("the lock excludes other open file descriptions, even in-process", {
  lf <- tempfile()
  h1 <- .Call(kioto:::kio_live_open_call, lf)
  expect_identical(.Call(kioto:::kio_live_try_call, h1), 0L)   # ACQUIRED

  h2 <- .Call(kioto:::kio_live_open_call, lf)
  expect_identical(.Call(kioto:::kio_live_try_call, h2), 1L)   # HELD

  .Call(kioto:::kio_live_close_call, h1)
  expect_identical(.Call(kioto:::kio_live_try_call, h2), 0L)   # holder gone
  expect_error(.Call(kioto:::kio_live_try_call, h1), "closed")
})

test_that("lock files land in the resolved per-platform directory", {
  dir <- .Call(kioto:::kio_live_dir_call)
  expect_true(dir.exists(dir))
  p <- channel_pair()
  suffix <- .Call(kioto:::kio_channel_suffix, p$host)
  expect_true(file.exists(file.path(dir, sprintf("kio_%s.live.host", suffix))))
  expect_true(file.exists(file.path(dir, sprintf("kio_%s.live.peer", suffix))))
})

test_that("KIOTO_LIVENESS_DIR overrides the default, read-through", {
  d <- tempfile("livedir")
  dir.create(d)
  Sys.setenv(KIOTO_LIVENESS_DIR = d)
  on.exit(Sys.unsetenv("KIOTO_LIVENESS_DIR"))
  expect_identical(.Call(kioto:::kio_live_dir_call), d)
  p <- channel_pair()
  suffix <- .Call(kioto:::kio_channel_suffix, p$host)
  expect_true(file.exists(file.path(d, sprintf("kio_%s.live.host", suffix))))
  expect_true(file.exists(file.path(d, sprintf("kio_%s.live.peer", suffix))))
})

test_that("a holder's death releases the lock to a probing survivor", {
  skip_if_no_child_kioto()
  lf <- tfile()
  pidfile <- tfile()

  kioto:::kio_spawn(sprintf('
    library(kioto)
    h <- .Call(kioto:::kio_live_open_call, %s)
    stopifnot(.Call(kioto:::kio_live_try_call, h) == 0L)
    tmp <- paste0(%s, ".tmp")
    writeLines(as.character(Sys.getpid()), tmp)
    file.rename(tmp, %s)
    Sys.sleep(30)
  ', deparse(lf), deparse(pidfile), deparse(pidfile)))

  expect_true(wait_for_file(pidfile))
  pid <- as.integer(readLines(pidfile))

  h <- .Call(kioto:::kio_live_open_call, lf)
  expect_identical(.Call(kioto:::kio_live_try_call, h), 1L)    # child alive

  tools::pskill(pid)
  expect_true(wait_until(.Call(kioto:::kio_live_try_call, h) == 0L))
})
