# Cross-process channels: host-spawn setup, the ready rendezvous, peer_main's
# expression contract and close epilogue, peer-death detection (listener flag
# -> flock verdict -> survivor cleanup), and the startup deadline walk-back.

test_that("a spawned echo peer round-trips every payload kind", {
  skip_if_no_child_sora()
  ch <- sora_channel(echo_expr, capacity = 256L, arena_size = 65536)
  expect_true(sora_alive(ch))

  for (x in list(
    42L, # RAW fast path
    c(a = 1, b = 2), # INLINE
    as.list(1:200), # ARENA
    runif(100000)
  )) {
    # SHM_RAW
    sora_send(ch, x)
    expect_identical(sora_recv(ch, 30), x)
  }
  expect_true(sora_close(ch, timeout = 10))
})

test_that("the peer evaluates its expression with only ch bound", {
  skip_if_no_child_sora()
  ch <- sora_channel(quote(
    sora_send(
      ch,
      list(bindings = ls(), has_sora = "package:sora" %in% search())
    )
  ))
  info <- sora_recv(ch, 30)
  expect_identical(info[["bindings"]], "ch")
  expect_true(info[["has_sora"]])
  sora_close(ch, timeout = 10)
})

test_that("a returning peer expression signals an orderly close", {
  skip_if_no_child_sora()
  ch <- sora_channel(quote(sora_send(ch, "done")))
  expect_identical(sora_recv(ch, 30), "done")
  expect_s3_class(sora_recv(ch, 30), "sora_closed")
  expect_true(sora_close(ch, timeout = 10))
})

test_that("a peer error is an orderly close, not a death", {
  skip_if_no_child_sora()
  ch <- sora_channel(
    quote(stop("boom")),
    launcher = sora_launcher(stderr = FALSE)
  )
  expect_s3_class(sora_recv(ch, 30), "sora_closed")
  expect_true(sora_close(ch, timeout = 10))
})

test_that("peer death surfaces as a sticky sora_peer_gone after draining", {
  skip_if_no_child_sora()
  ch <- sora_channel(quote({
    sora_send(ch, "before death")
    Sys.sleep(300)
  }))
  nm <- .Call(sora:::sora_channel_stat, ch)[["name"]]
  expect_identical(sora_recv(ch, 30), "before death")
  tools::pskill(.Call(sora:::sora_channel_stat, ch)[["peer_pid"]])

  # empty ring + confirmed death: the listener wakes the park, the flock
  # probe delivers the verdict
  r <- sora_recv(ch, timeout = 30)
  expect_s3_class(r, "sora_peer_gone")
  expect_false(sora_alive(ch))
  expect_s3_class(sora_send(ch, 1), "sora_peer_gone") # sticky
  expect_s3_class(sora_recv(ch, 0), "sora_peer_gone")

  # the survivor unlinked the region name
  if (.Platform[["OS.type"]] != "windows") {
    expect_error(.Call(sora:::sora_region_open, nm, FALSE), "cannot open")
  }
  expect_true(sora_close(ch, timeout = 10))
})

test_that("a peer that exits mid-stream loses nothing already published", {
  skip_if_no_child_sora()
  ch <- sora_channel(quote({
    for (i in 1:5) {
      sora_send(ch, i)
    }
    quit(save = "no") # a clean exit: the exit finalizer signals close
  }))
  got <- list()
  repeat {
    r <- sora_recv(ch, timeout = 30)
    if (inherits(r, "sora_sentinel")) {
      break
    }
    got <- c(got, list(r))
  }
  expect_identical(got, as.list(1:5)) # drained before the verdict
  expect_s3_class(r, "sora_closed")
  sora_close(ch, timeout = 10)
})

test_that("a peer hard-killed mid-stream drains, then reports gone", {
  skip_if_no_child_sora()
  ch <- sora_channel(quote({
    for (i in 1:5) {
      sora_send(ch, i)
    }
    Sys.sleep(30)
  }))
  got <- list()
  for (i in 1:5) {
    got <- c(got, list(sora_recv(ch, timeout = 30)))
  }
  kill_hard(.Call(sora:::sora_channel_stat, ch)[["peer_pid"]])
  r <- sora_recv(ch, timeout = 30)
  expect_identical(got, as.list(1:5)) # drained before the verdict
  expect_s3_class(r, "sora_peer_gone")
  sora_close(ch, timeout = 10)
})

test_that("the startup deadline walks the channel back", {
  err <- tryCatch(
    sora_channel(
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
  skip_if_no_child_sora()
  seen <- NULL
  ch <- sora_channel(echo_expr, launcher = function(token) {
    seen <<- token
    sora:::spawn_peer(token)
  })
  expect_match(seen, "^[0-9a-f]+_[0-9a-f]+$")
  sora_send(ch, "via launcher")
  expect_identical(sora_recv(ch, 30), "via launcher")
  expect_true(sora_close(ch, timeout = 10))
})

test_that("mori-shared objects map zero-copy in the peer process", {
  skip_if_no_child_sora()
  skip_if_not_installed("mori")
  ch <- sora_channel(quote({
    x <- sora_recv(ch, timeout = 30)
    sora_send(
      ch,
      list(
        shared = mori::is_shared(x),
        name = mori::shared_name(x),
        total = sum(x)
      )
    )
  }))
  x <- mori::share(runif(1000))
  sora_send(ch, x)
  info <- sora_recv(ch, 30)
  expect_true(info[["shared"]])
  expect_identical(info[["name"]], mori::shared_name(x))
  expect_identical(info[["total"]], sum(x))
  sora_close(ch, timeout = 10)
})
