# flock / LockFileEx liveness primitive: fd-scoped, held for a process's
# lifetime, released by the kernel on any exit path. ACQUIRED on a probe
# means the previous holder is dead; HELD means alive.

test_that("the lock excludes other open file descriptions, even in-process", {
  lf <- tempfile()
  h1 <- .Call(mov:::mov_live_open_call, lf)
  expect_identical(.Call(mov:::mov_live_try_call, h1), 0L)   # ACQUIRED

  h2 <- .Call(mov:::mov_live_open_call, lf)
  expect_identical(.Call(mov:::mov_live_try_call, h2), 1L)   # HELD

  .Call(mov:::mov_live_close_call, h1)
  expect_identical(.Call(mov:::mov_live_try_call, h2), 0L)   # holder gone
  expect_error(.Call(mov:::mov_live_try_call, h1), "closed")
})

test_that("a holder's death releases the lock to a probing survivor", {
  skip_if_no_child_mov()
  lf <- tfile()
  pidfile <- tfile()

  mov:::mov_spawn(sprintf('
    library(mov)
    h <- .Call(mov:::mov_live_open_call, %s)
    stopifnot(.Call(mov:::mov_live_try_call, h) == 0L)
    tmp <- paste0(%s, ".tmp")
    writeLines(as.character(Sys.getpid()), tmp)
    file.rename(tmp, %s)
    Sys.sleep(30)
  ', deparse(lf), deparse(pidfile), deparse(pidfile)))

  expect_true(wait_for_file(pidfile))
  pid <- as.integer(readLines(pidfile))

  h <- .Call(mov:::mov_live_open_call, lf)
  expect_identical(.Call(mov:::mov_live_try_call, h), 1L)    # child alive

  tools::pskill(pid)
  expect_true(wait_until(.Call(mov:::mov_live_try_call, h) == 0L))
})
