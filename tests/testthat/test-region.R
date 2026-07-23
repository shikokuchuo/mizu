# Vendored SHM core under the /kio_ namespace, the kioto-owned writable attach,
# and the GC extptr wrappers (producer: munmap + unlink; consumer: munmap).

test_that("regions are created under the kioto namespace with correct size", {
  xp <- .Call(kioto:::kio_region_create, 4096)
  nm <- .Call(kioto:::kio_region_name, xp)
  prefix <- if (.Platform$OS.type == "windows") "Local\\kio_" else "/kio_"
  expect_true(startsWith(nm, prefix))
  expect_identical(.Call(kioto:::kio_region_size, xp), 4096)
})

test_that("kioto regions are invisible to mori", {
  skip_if_not_installed("mori")
  xp <- .Call(kioto:::kio_region_create, 4096)
  expect_null(mori::map_shared(.Call(kioto:::kio_region_name, xp)))
})

test_that("read-only and writable attaches map the same pages", {
  xp <- .Call(kioto:::kio_region_create, 4096)
  nm <- .Call(kioto:::kio_region_name, xp)
  ro <- .Call(kioto:::kio_region_open, nm, FALSE)
  rw <- .Call(kioto:::kio_region_open, nm, TRUE)
  # consumers discover size by fstat/VirtualQuery: at least the created size,
  # rounded up to page granularity (16K pages on Apple Silicon)
  expect_gte(.Call(kioto:::kio_region_size, ro), 4096)

  .Call(kioto:::kio_poke, xp, 1000, as.raw(1:8))
  expect_identical(.Call(kioto:::kio_peek, ro, 1000, 8), as.raw(1:8))
  .Call(kioto:::kio_poke, rw, 2000, as.raw(0xff))
  expect_identical(.Call(kioto:::kio_peek, xp, 2000, 1), as.raw(0xff))
})

test_that("peek and poke are bounds-checked", {
  xp <- .Call(kioto:::kio_region_create, 4096)
  expect_error(.Call(kioto:::kio_peek, xp, 4090, 8), "out of bounds")
  expect_error(.Call(kioto:::kio_poke, xp, 4096, as.raw(1)), "out of bounds")
})

test_that("invalid creates and opens error cleanly", {
  expect_error(.Call(kioto:::kio_region_create, 0), "invalid region size")
  expect_error(.Call(kioto:::kio_region_open, "/kio_nonexistent_0", FALSE),
               "cannot open")
  expect_error(.Call(kioto:::kio_region_open, "/kio_nonexistent_0", TRUE),
               "cannot open")
  expect_error(.Call(kioto:::kio_region_name, new.env()),
               "not a kioto region handle")
})

test_that("producer GC releases the name; live consumers keep reading", {
  xp <- .Call(kioto:::kio_region_create, 4096)
  nm <- .Call(kioto:::kio_region_name, xp)
  .Call(kioto:::kio_poke, xp, 0, as.raw(42))
  ro <- .Call(kioto:::kio_region_open, nm, FALSE)

  rm(xp)
  gc()
  # the consumer's mapping survives the producer's GC on every platform
  expect_identical(.Call(kioto:::kio_peek, ro, 0, 1), as.raw(42))

  if (.Platform$OS.type != "windows") {
    # POSIX: the producer's finalizer unlinked the name immediately
    expect_error(.Call(kioto:::kio_region_open, nm, FALSE), "cannot open")
  }
  # Windows kernel objects have no unlink step — the name lives until the
  # last handle (here the consumer's) closes; POSIX is already unlinked
  rm(ro)
  gc()
  expect_error(.Call(kioto:::kio_region_open, nm, FALSE), "cannot open")
})

test_that("clean child exit runs the session-exit finalizers", {
  skip_if_no_child_kioto()
  f <- tfile()
  kioto:::kio_spawn(sprintf('
    library(kioto)
    xp <- .Call(kioto:::kio_region_create, 4096)
    tmp <- paste0(%s, ".tmp")
    writeLines(.Call(kioto:::kio_region_name, xp), tmp)
    file.rename(tmp, %s)
  ', deparse(f), deparse(f)))
  expect_true(wait_for_file(f))
  nm <- readLines(f)
  expect_true(wait_until(
    inherits(tryCatch(.Call(kioto:::kio_region_open, nm, FALSE),
                      error = identity), "error")))
})
