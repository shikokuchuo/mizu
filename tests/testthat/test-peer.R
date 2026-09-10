# Cross-process channels: host-spawn setup, the ready rendezvous, peer_main's
# expression contract and close epilogue, peer-death detection (listener flag
# -> flock verdict -> survivor cleanup), and the startup deadline walk-back.

test_that("a spawned echo peer round-trips every payload kind", {
  skip_if_no_child_rei()
  ch <- rei_channel(echo_expr, capacity = 256L, arena_size = 65536)
  expect_true(rei_alive(ch))

  for (x in list(
    42L, # RAW fast path
    c(a = 1, b = 2), # INLINE
    as.list(1:200), # ARENA
    runif(100000)
  )) {
    # SHM_RAW
    rei_send(ch, x)
    expect_identical(rei_recv(ch, 30), x)
  }
  expect_true(rei_close(ch, timeout = 10))
})

test_that("a source-string drop is parsed and evaluated by the peer", {
  skip_if_no_child_rei()
  ch <- rei_channel(
    'rei_send(ch, paste0("source:", 1 + 2))',
    launcher = rei_launcher()
  )
  expect_identical(rei_recv(ch, 30), "source:3")
  expect_true(rei_close(ch, timeout = 10))
})

test_that("a malformed source drop errors in the peer, closing orderly", {
  skip_if_no_child_rei()
  ch <- rei_channel(
    "this is not R code !!!",
    launcher = rei_launcher(stderr = FALSE)
  )
  expect_s3_class(rei_recv(ch, 30), "rei_closed")
  expect_true(rei_close(ch, timeout = 10))
})

test_that("the peer evaluates its expression with only ch bound", {
  skip_if_no_child_rei()
  ch <- rei_channel(quote(
    rei_send(
      ch,
      list(bindings = ls(), has_rei = "package:rei" %in% search())
    )
  ))
  info <- rei_recv(ch, 30)
  expect_identical(info[["bindings"]], "ch")
  expect_true(info[["has_rei"]])
  rei_close(ch, timeout = 10)
})

test_that("a returning peer expression signals an orderly close", {
  skip_if_no_child_rei()
  ch <- rei_channel(quote(rei_send(ch, "done")))
  expect_identical(rei_recv(ch, 30), "done")
  expect_s3_class(rei_recv(ch, 30), "rei_closed")
  expect_true(rei_close(ch, timeout = 10))
})

test_that("a peer error is an orderly close, not a death", {
  skip_if_no_child_rei()
  ch <- rei_channel(
    quote(stop("boom")),
    launcher = rei_launcher(stderr = FALSE)
  )
  expect_s3_class(rei_recv(ch, 30), "rei_closed")
  expect_true(rei_close(ch, timeout = 10))
})

test_that("peer death surfaces as a sticky rei_peer_gone after draining", {
  skip_if_no_child_rei()
  ch <- rei_channel(quote({
    rei_send(ch, "before death")
    Sys.sleep(300)
  }))
  nm <- .Call(rei:::rei_channel_stat, ch)[["name"]]
  expect_identical(rei_recv(ch, 30), "before death")
  tools::pskill(.Call(rei:::rei_channel_stat, ch)[["peer_pid"]])

  # empty ring + confirmed death: the listener wakes the park, the flock
  # probe delivers the verdict
  r <- rei_recv(ch, timeout = 30)
  expect_s3_class(r, "rei_peer_gone")
  expect_false(rei_alive(ch))
  expect_s3_class(rei_send(ch, 1), "rei_peer_gone") # sticky
  expect_s3_class(rei_recv(ch, 0), "rei_peer_gone")

  # the survivor unlinked the region name
  if (.Platform[["OS.type"]] != "windows") {
    expect_error(.Call(rei:::rei_region_open, nm, FALSE), "cannot open")
  }
  expect_true(rei_close(ch, timeout = 10))
})

test_that("a peer that exits mid-stream loses nothing already published", {
  skip_if_no_child_rei()
  ch <- rei_channel(quote({
    for (i in 1:5) {
      rei_send(ch, i)
    }
    quit(save = "no") # a clean exit: the exit finalizer signals close
  }))
  got <- list()
  repeat {
    r <- rei_recv(ch, timeout = 30)
    if (inherits(r, "rei_sentinel")) {
      break
    }
    got <- c(got, list(r))
  }
  expect_identical(got, as.list(1:5)) # drained before the verdict
  expect_s3_class(r, "rei_closed")
  rei_close(ch, timeout = 10)
})

test_that("a peer hard-killed mid-stream drains, then reports gone", {
  skip_if_no_child_rei()
  ch <- rei_channel(quote({
    for (i in 1:5) {
      rei_send(ch, i)
    }
    Sys.sleep(30)
  }))
  got <- list()
  for (i in 1:5) {
    got <- c(got, list(rei_recv(ch, timeout = 30)))
  }
  kill_hard(.Call(rei:::rei_channel_stat, ch)[["peer_pid"]])
  r <- rei_recv(ch, timeout = 30)
  expect_identical(got, as.list(1:5)) # drained before the verdict
  expect_s3_class(r, "rei_peer_gone")
  rei_close(ch, timeout = 10)
})

test_that("the startup deadline walks the channel back", {
  err <- tryCatch(
    rei_channel(
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
  skip_if_no_child_rei()
  seen <- NULL
  ch <- rei_channel(echo_expr, launcher = function(token) {
    seen <<- token
    rei:::spawn_peer(token)
  })
  expect_match(seen, "^[0-9a-f]+_[0-9a-f]+$")
  rei_send(ch, "via launcher")
  expect_identical(rei_recv(ch, 30), "via launcher")
  expect_true(rei_close(ch, timeout = 10))
})

test_that("mori-shared objects map zero-copy in the peer process", {
  skip_if_no_child_rei()
  skip_if_not_installed("mori")
  ch <- rei_channel(quote({
    x <- rei_recv(ch, timeout = 30)
    rei_send(
      ch,
      list(
        shared = mori::is_shared(x),
        name = mori::shared_name(x),
        total = sum(x)
      )
    )
  }))
  x <- mori::share(runif(1000))
  rei_send(ch, x)
  info <- rei_recv(ch, 30)
  expect_true(info[["shared"]])
  expect_identical(info[["name"]], mori::shared_name(x))
  expect_equal(info[["total"]], sum(x))
  rei_close(ch, timeout = 10)
})
