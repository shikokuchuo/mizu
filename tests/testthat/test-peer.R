# Cross-process channels: host-spawn setup, the ready rendezvous, peer_main's
# expression contract and close epilogue, peer-death detection (listener flag
# -> flock verdict -> survivor cleanup), and the startup deadline walk-back.

test_that("a spawned echo peer round-trips every payload kind", {
  skip_if_no_child_mov()
  ch <- mov_channel(echo_expr, capacity = 256L, arena_size = 65536)
  expect_true(mov_alive(ch))

  for (x in list(42L,                                  # RAW fast path
                 c(a = 1, b = 2),                      # INLINE
                 as.list(1:200),                       # ARENA
                 runif(100000))) {                     # SHM_RAW
    mov_send(ch, x)
    mov_flush(ch)
    expect_identical(mov_recv(ch, 30), x)
  }
  expect_true(mov_close(ch, timeout = 10))
})

test_that("the peer evaluates its expression with only ch bound", {
  skip_if_no_child_mov()
  ch <- mov_channel(quote({
    mov_send(ch, list(bindings = ls(), has_mov = "package:mov" %in% search()))
    mov_flush(ch)
  }))
  info <- mov_recv(ch, 30)
  expect_identical(info$bindings, "ch")
  expect_true(info$has_mov)
  mov_close(ch, timeout = 10)
})

test_that("a returning peer expression signals an orderly close", {
  skip_if_no_child_mov()
  ch <- mov_channel(quote({
    mov_send(ch, "done")
    mov_flush(ch)
  }))
  expect_identical(mov_recv(ch, 30), "done")
  expect_s3_class(mov_recv(ch, 30), "mov_closed")
  expect_true(mov_close(ch, timeout = 10))
})

test_that("a peer error is an orderly close, not a death", {
  skip_if_no_child_mov()
  ch <- mov_channel(quote(stop("boom")), stderr = FALSE)
  expect_s3_class(mov_recv(ch, 30), "mov_closed")
  expect_true(mov_close(ch, timeout = 10))
})

test_that("peer death surfaces as a sticky mov_peer_gone after draining", {
  skip_if_no_child_mov()
  ch <- mov_channel(quote({
    mov_send(ch, "before death")
    mov_flush(ch)
    Sys.sleep(300)
  }))
  nm <- .Call(mov:::mov_channel_stat, ch)$name
  expect_identical(mov_recv(ch, 30), "before death")
  tools::pskill(.Call(mov:::mov_channel_stat, ch)$peer_pid)

  # empty ring + confirmed death: the listener wakes the park, the flock
  # probe delivers the verdict
  r <- mov_recv(ch, timeout = 30)
  expect_s3_class(r, "mov_peer_gone")
  expect_false(mov_alive(ch))
  expect_s3_class(mov_send(ch, 1), "mov_peer_gone")     # sticky
  expect_s3_class(mov_recv(ch, 0), "mov_peer_gone")

  # the survivor unlinked the region name
  if (.Platform$OS.type != "windows")
    expect_error(.Call(mov:::mov_region_open, nm, FALSE), "cannot open")
  expect_true(mov_close(ch, timeout = 10))
})

test_that("a peer that dies mid-stream loses nothing already published", {
  skip_if_no_child_mov()
  ch <- mov_channel(quote({
    for (i in 1:5) mov_send(ch, i)
    mov_flush(ch)
    mov_send(ch, "unflushed")               # staged, never published
    quit(save = "no")                       # dies without close
  }))
  got <- list()
  repeat {
    r <- mov_recv(ch, timeout = 30)
    if (inherits(r, "mov_condition")) break
    got <- c(got, list(r))
  }
  expect_identical(got, as.list(1:5))       # drained before the verdict
  expect_s3_class(r, "mov_peer_gone")
  mov_close(ch, timeout = 10)
})

test_that("the startup deadline walks the channel back", {
  t0 <- proc.time()[[3]]
  err <- tryCatch(
    mov_channel(quote(NULL), launcher = function(suffix) NULL,
                startup_timeout = 0.5),
    error = identity)
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "failed to attach")
  expect_lt(proc.time()[[3]] - t0, 10)
})

test_that("a custom launcher receives the suffix and drives the spawn", {
  skip_if_no_child_mov()
  seen <- NULL
  ch <- mov_channel(echo_expr, launcher = function(suffix) {
    seen <<- suffix
    mov:::spawn_peer(suffix)
  })
  expect_match(seen, "^[0-9a-f]+_[0-9a-f]+$")
  mov_send(ch, "via launcher")
  mov_flush(ch)
  expect_identical(mov_recv(ch, 30), "via launcher")
  expect_true(mov_close(ch, timeout = 10))
})

test_that("mori-shared objects map zero-copy in the peer process", {
  skip_if_no_child_mov()
  skip_if_not_installed("mori")
  ch <- mov_channel(quote({
    x <- mov_recv(ch, timeout = 30)
    mov_send(ch, list(shared = mori::is_shared(x),
                      name = mori::shared_name(x),
                      total = sum(x)))
    mov_flush(ch)
  }))
  x <- mori::share(runif(1000))
  mov_send(ch, x)
  mov_flush(ch)
  info <- mov_recv(ch, 30)
  expect_true(info$shared)
  expect_identical(info$name, mori::shared_name(x))
  expect_identical(info$total, sum(x))
  mov_close(ch, timeout = 10)
})
