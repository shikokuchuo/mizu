# flock / LockFileEx liveness primitive: fd-scoped, held for a process's
# lifetime, released by the kernel on any exit path. ACQUIRED on a probe
# means the previous holder is dead; HELD means alive.

test_that("the lock excludes other open file descriptions, even in-process", {
  lf <- tempfile()
  h1 <- .Call(kioto:::kio_live_open_call, lf)
  expect_identical(.Call(kioto:::kio_live_try_call, h1), 0L) # ACQUIRED

  h2 <- .Call(kioto:::kio_live_open_call, lf)
  expect_identical(.Call(kioto:::kio_live_try_call, h2), 1L) # HELD

  .Call(kioto:::kio_live_close_call, h1)
  expect_identical(.Call(kioto:::kio_live_try_call, h2), 0L) # holder gone
  expect_error(.Call(kioto:::kio_live_try_call, h1), "closed")
})

test_that("lock files land in the resolved per-platform directory", {
  dir <- .Call(kioto:::kio_live_dir_call)
  expect_true(dir.exists(dir))
  p <- channel_pair()
  suffix <- .Call(kioto:::kio_channel_suffix, p[["host"]])
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
  suffix <- .Call(kioto:::kio_channel_suffix, p[["host"]])
  expect_true(file.exists(file.path(d, sprintf("kio_%s.live.host", suffix))))
  expect_true(file.exists(file.path(d, sprintf("kio_%s.live.peer", suffix))))
})

test_that("a holder's death releases the lock to a probing survivor", {
  skip_if_no_child_kioto()
  lf <- tfile()
  pidfile <- tfile()

  kioto:::kio_spawn(sprintf(
    '
    library(kioto)
    h <- .Call(kioto:::kio_live_open_call, %s)
    if (.Call(kioto:::kio_live_try_call, h) != 0L) stop("lock busy")
    tmp <- paste0(%s, ".tmp")
    writeLines(as.character(Sys.getpid()), tmp)
    file.rename(tmp, %s)
    Sys.sleep(30)
  ',
    deparse(lf),
    deparse(pidfile),
    deparse(pidfile)
  ))

  expect_true(wait_for_file(pidfile))
  pid <- as.integer(readLines(pidfile))

  h <- .Call(kioto:::kio_live_open_call, lf)
  expect_identical(.Call(kioto:::kio_live_try_call, h), 1L) # child alive

  tools::pskill(pid)
  expect_true(wait_until(.Call(kioto:::kio_live_try_call, h) == 0L))
})

test_that("liveness handles validate their arguments", {
  expect_error(.Call(kioto:::kio_live_open_call, 42L), "expected a file path")
  expect_error(
    .Call(kioto:::kio_live_open_call, file.path(tempfile("kio-absent"), "x")),
    "cannot open liveness file"
  )
})

test_that("an unusable liveness directory fails both constructors cleanly", {
  on.exit({
    Sys.unsetenv("KIOTO_LIVENESS_DIR")
    gc() # release the regions the walked-back creates left behind
  })
  Sys.setenv(KIOTO_LIVENESS_DIR = file.path(tempfile("kio-absent"), "locks"))
  expect_error(
    .Call(kioto:::kio_channel_create, quote(NULL), 64L, 256L, 0, FALSE),
    "cannot lock host liveness file"
  )
  expect_error(
    .Call(kioto:::kio_pool_create, 1L, 8L, 64L, 64L, 64L, 256L),
    "cannot create liveness file"
  )
  # past the 900-byte wire budget both refuse before creating anything
  Sys.setenv(KIOTO_LIVENESS_DIR = paste0("/", strrep("d", 920)))
  expect_error(
    .Call(kioto:::kio_channel_create, quote(NULL), 64L, 256L, 0, FALSE),
    "liveness directory path too long"
  )
  expect_error(
    .Call(kioto:::kio_pool_create, 1L, 8L, 64L, 64L, 64L, 256L),
    "liveness directory path too long"
  )
})
