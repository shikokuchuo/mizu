# Cross-process channels: host-spawn setup, the ready rendezvous, peer_main's
# expression contract and close epilogue, peer-death detection (listener flag
# -> flock verdict -> survivor cleanup), and the startup deadline walk-back.

test_that("a spawned echo peer round-trips every payload kind", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(echo_expr, capacity = 256L, arena_size = 65536)
  expect_true(mizu_alive(ch))

  for (x in list(
    42L, # RAW fast path
    c(a = 1, b = 2), # INLINE
    as.list(1:200), # ARENA
    runif(100000)
  )) {
    # SHM_RAW
    mizu_send(ch, x)
    expect_identical(mizu_recv(ch, 30), x)
  }
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a source-string drop is parsed and evaluated by the peer", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(
    'mizu_send(ch, paste0("source:", 1 + 2))',
    launcher = mizu_launcher()
  )
  expect_identical(mizu_recv(ch, 30), "source:3")
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a malformed source drop errors in the peer, closing orderly", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(
    "this is not R code !!!",
    launcher = mizu_launcher(stderr = FALSE)
  )
  expect_s3_class(mizu_recv(ch, 30), "mizu_closed")
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("the peer evaluates its expression with only ch bound", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(quote(
    mizu_send(
      ch,
      list(bindings = ls(), has_mizu = "package:mizu" %in% search())
    )
  ))
  info <- mizu_recv(ch, 30)
  expect_identical(info[["bindings"]], "ch")
  expect_true(info[["has_mizu"]])
  mizu_close(ch, timeout = 10)
})

test_that("a returning peer expression signals an orderly close", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(quote(mizu_send(ch, "done")))
  expect_identical(mizu_recv(ch, 30), "done")
  expect_s3_class(mizu_recv(ch, 30), "mizu_closed")
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a peer error is an orderly close, not a death", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(
    quote(stop("boom")),
    launcher = mizu_launcher(stderr = FALSE)
  )
  expect_s3_class(mizu_recv(ch, 30), "mizu_closed")
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("peer death surfaces as a sticky mizu_peer_gone after draining", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(quote({
    mizu_send(ch, "before death")
    Sys.sleep(300)
  }))
  nm <- .Call(mizu:::mizu_channel_stat, ch)[["name"]]
  expect_identical(mizu_recv(ch, 30), "before death")
  tools::pskill(.Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]])

  # empty ring + confirmed death: the listener wakes the park, the flock
  # probe delivers the verdict
  r <- mizu_recv(ch, timeout = 30)
  expect_s3_class(r, "mizu_peer_gone")
  expect_false(mizu_alive(ch))
  expect_s3_class(mizu_send(ch, 1), "mizu_peer_gone") # sticky
  expect_s3_class(mizu_recv(ch, 0), "mizu_peer_gone")

  # the survivor unlinked the region name
  if (.Platform[["OS.type"]] != "windows") {
    expect_error(.Call(mizu:::mizu_region_open, nm, FALSE), "cannot open")
  }
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a peer that exits mid-stream loses nothing already published", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(quote({
    for (i in 1:5) {
      mizu_send(ch, i)
    }
    quit(save = "no") # a clean exit: the exit finalizer signals close
  }))
  got <- list()
  repeat {
    r <- mizu_recv(ch, timeout = 30)
    if (inherits(r, "mizu_sentinel")) {
      break
    }
    got <- c(got, list(r))
  }
  expect_identical(got, as.list(1:5)) # drained before the verdict
  expect_s3_class(r, "mizu_closed")
  mizu_close(ch, timeout = 10)
})

test_that("a peer hard-killed mid-stream drains, then reports gone", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(quote({
    for (i in 1:5) {
      mizu_send(ch, i)
    }
    Sys.sleep(30)
  }))
  got <- list()
  for (i in 1:5) {
    got <- c(got, list(mizu_recv(ch, timeout = 30)))
  }
  kill_hard(.Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]])
  r <- mizu_recv(ch, timeout = 30)
  expect_identical(got, as.list(1:5)) # drained before the verdict
  expect_s3_class(r, "mizu_peer_gone")
  mizu_close(ch, timeout = 10)
})

test_that("the startup deadline walks the channel back", {
  err <- tryCatch(
    mizu_channel(
      quote({}),
      launcher = function(token) NULL,
      startup_timeout = 0.5
    ),
    error = identity
  )
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "failed to attach")
})

test_that("a custom launcher receives the token and drives the spawn", {
  skip_if_no_child_mizu()
  seen <- NULL
  ch <- mizu_channel(echo_expr, launcher = function(token) {
    seen <<- token
    mizu:::spawn_peer(token)
  })
  expect_match(seen, "^[0-9a-f]+_[0-9a-f]+$")
  mizu_send(ch, "via launcher")
  expect_identical(mizu_recv(ch, 30), "via launcher")
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("mori-shared objects map zero-copy in the peer process", {
  skip_if_no_child_mizu()
  skip_if_not_installed("mori")
  ch <- mizu_channel(quote({
    x <- mizu_recv(ch, timeout = 30)
    mizu_send(
      ch,
      list(
        shared = mori::is_shared(x),
        name = mori::shared_name(x),
        total = sum(x)
      )
    )
  }))
  x <- mori::share(runif(1000))
  mizu_send(ch, x)
  info <- mizu_recv(ch, 30)
  expect_true(info[["shared"]])
  expect_identical(info[["name"]], mori::shared_name(x))
  expect_equal(info[["total"]], sum(x))
  mizu_close(ch, timeout = 10)
})
