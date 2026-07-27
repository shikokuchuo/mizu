# Cross-process channels: host-spawn setup, the ready rendezvous, peer_main's
# expression contract and close epilogue, peer-death detection (listener flag
# -> flock verdict -> survivor cleanup), and the startup deadline walk-back.

test_that("a spawned echo peer round-trips every payload kind", {
  skip_if_no_child_kioto()
  ch <- kio_channel(echo_expr, capacity = 256L, arena_size = 65536)
  expect_true(kio_alive(ch))

  for (x in list(42L,                                  # RAW fast path
                 c(a = 1, b = 2),                      # INLINE
                 as.list(1:200),                       # ARENA
                 runif(100000))) {                     # SHM_RAW
    kio_send(ch, x)
    expect_identical(kio_recv(ch, 30), x)
  }
  expect_true(kio_close(ch, timeout = 10))
})

test_that("the peer evaluates its expression with only ch bound", {
  skip_if_no_child_kioto()
  ch <- kio_channel(quote(
    kio_send(ch, list(bindings = ls(), has_kioto = "package:kioto" %in% search()))
  ))
  info <- kio_recv(ch, 30)
  expect_identical(info$bindings, "ch")
  expect_true(info$has_kioto)
  kio_close(ch, timeout = 10)
})

test_that("a returning peer expression signals an orderly close", {
  skip_if_no_child_kioto()
  ch <- kio_channel(quote(kio_send(ch, "done")))
  expect_identical(kio_recv(ch, 30), "done")
  expect_s3_class(kio_recv(ch, 30), "kio_closed")
  expect_true(kio_close(ch, timeout = 10))
})

test_that("a peer error is an orderly close, not a death", {
  skip_if_no_child_kioto()
  ch <- kio_channel(quote(stop("boom")), stderr = FALSE)
  expect_s3_class(kio_recv(ch, 30), "kio_closed")
  expect_true(kio_close(ch, timeout = 10))
})

test_that("peer death surfaces as a sticky kio_peer_gone after draining", {
  skip_if_no_child_kioto()
  ch <- kio_channel(quote({
    kio_send(ch, "before death")
    Sys.sleep(300)
  }))
  nm <- .Call(kioto:::kio_channel_stat, ch)$name
  expect_identical(kio_recv(ch, 30), "before death")
  tools::pskill(.Call(kioto:::kio_channel_stat, ch)$peer_pid)

  # empty ring + confirmed death: the listener wakes the park, the flock
  # probe delivers the verdict
  r <- kio_recv(ch, timeout = 30)
  expect_s3_class(r, "kio_peer_gone")
  expect_false(kio_alive(ch))
  expect_s3_class(kio_send(ch, 1), "kio_peer_gone")     # sticky
  expect_s3_class(kio_recv(ch, 0), "kio_peer_gone")

  # the survivor unlinked the region name
  if (.Platform$OS.type != "windows")
    expect_error(.Call(kioto:::kio_region_open, nm, FALSE), "cannot open")
  expect_true(kio_close(ch, timeout = 10))
})

test_that("a peer that dies mid-stream loses nothing already published", {
  skip_if_no_child_kioto()
  ch <- kio_channel(quote({
    for (i in 1:5) kio_send(ch, i)
    quit(save = "no")                       # dies without close
  }))
  got <- list()
  repeat {
    r <- kio_recv(ch, timeout = 30)
    if (inherits(r, "kio_condition")) break
    got <- c(got, list(r))
  }
  expect_identical(got, as.list(1:5))       # drained before the verdict
  expect_s3_class(r, "kio_peer_gone")
  kio_close(ch, timeout = 10)
})

test_that("the startup deadline walks the channel back", {
  t0 <- proc.time()[[3]]
  err <- tryCatch(
    kio_channel(quote(NULL), launcher = function(suffix) NULL,
                startup_timeout = 0.5),
    error = identity)
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "failed to attach")
  expect_lt(proc.time()[[3]] - t0, 10)
})

test_that("a custom launcher receives the suffix and drives the spawn", {
  skip_if_no_child_kioto()
  seen <- NULL
  ch <- kio_channel(echo_expr, launcher = function(suffix) {
    seen <<- suffix
    kioto:::spawn_peer(suffix)
  })
  expect_match(seen, "^[0-9a-f]+_[0-9a-f]+$")
  kio_send(ch, "via launcher")
  expect_identical(kio_recv(ch, 30), "via launcher")
  expect_true(kio_close(ch, timeout = 10))
})

test_that("mori-shared objects map zero-copy in the peer process", {
  skip_if_no_child_kioto()
  skip_if_not_installed("mori")
  ch <- kio_channel(quote({
    x <- kio_recv(ch, timeout = 30)
    kio_send(ch, list(shared = mori::is_shared(x),
                      name = mori::shared_name(x),
                      total = sum(x)))
  }))
  x <- mori::share(runif(1000))
  kio_send(ch, x)
  info <- kio_recv(ch, 30)
  expect_true(info$shared)
  expect_identical(info$name, mori::shared_name(x))
  expect_identical(info$total, sum(x))
  kio_close(ch, timeout = 10)
})
