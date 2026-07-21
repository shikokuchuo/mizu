# Channel setup and lifecycle: exact-sized create, preamble + drop slot +
# liveness-dir staging, ready rendezvous, the close protocol's four steps,
# and handle discipline. Ring mechanics are test-ring.R; spawned peers are
# test-peer.R.

test_that("mov_channel_create validates its parameters", {
  expect_error(.Call(mov:::mov_channel_create, quote(NULL), 3L, 256L, 0,
                     tempdir(), FALSE),
               "capacity must be a power of two")
  expect_error(.Call(mov:::mov_channel_create, quote(NULL), 64L, 32L, 0,
                     tempdir(), FALSE),
               "slot_size must be a power of two")
  expect_error(.Call(mov:::mov_channel_create, quote(NULL), 64L, 256L, 100,
                     tempdir(), FALSE),
               "arena_size must be a non-negative multiple of 64")
})

test_that("a fresh channel reports its layout and state", {
  p <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  st <- .Call(mov:::mov_channel_stat, p$host)
  prefix <- if (.Platform$OS.type == "windows") "Local\\mov_" else "/mov_"
  expect_true(startsWith(st$name, prefix))
  expect_identical(st$side, "host")
  expect_identical(st$capacity, 128)
  expect_identical(st$slot_size, 512)
  expect_identical(st$arena_size, 8192)
  expect_identical(st$inline_max, 496)
  expect_false(st$spin)
  expect_true(st$ready)
  expect_identical(st$closed, 0L)
  expect_identical(st$peer_pid, as.double(Sys.getpid()))
  expect_identical(.Call(mov:::mov_channel_stat, p$peer)$side, "peer")
  expect_s3_class(p$host, "mov_channel")
})

test_that("attach validates the region: absent, malformed, or not a channel", {
  expect_error(.Call(mov:::mov_channel_attach, "0_0"), "cannot open")
  expect_error(.Call(mov:::mov_channel_attach, "evil'; echo pwned"),
               "malformed region-name suffix")
  expect_error(.Call(mov:::mov_channel_attach, strrep("a", 40)),
               "malformed region-name suffix")

  # a raw mov region is not a channel: zeroed bytes fail the magic check
  xp <- .Call(mov:::mov_region_create, 4096)
  nm <- .Call(mov:::mov_region_name, xp)
  prefix <- if (.Platform$OS.type == "windows") "Local\\mov_" else "/mov_"
  suffix <- substr(nm, nchar(prefix) + 1L, nchar(nm))
  expect_error(.Call(mov:::mov_channel_attach, suffix),
               "invalid channel region")
})

test_that("the close protocol rendezvouses and releases both ends", {
  p <- channel_pair()
  # the peer signals close (the epilogue primitive: flush + bit + wake)
  mov_send(p$peer, "parting")
  .Call(mov:::mov_channel_close_signal, p$peer)
  # drain-before-verdict: published messages come out before mov_closed
  expect_identical(mov_recv(p$host, 5), "parting")
  expect_s3_class(mov_recv(p$host, 5), "mov_closed")
  expect_s3_class(mov_send(p$host, 1), "mov_closed")
  # host's close finds the peer bit already set, and vice versa
  expect_true(mov_close(p$host, timeout = 5))
  expect_true(mov_close(p$peer, timeout = 5))
  # released handles: verbs error, close is idempotent, alive is FALSE
  expect_error(mov_recv(p$host, 0), "channel handle is closed")
  expect_error(mov_send(p$host, 1), "channel handle is closed")
  expect_true(mov_close(p$host))
  expect_false(mov_alive(p$host))
})

test_that("close times out with a warning when the peer never answers", {
  p <- channel_pair()
  expect_warning(ok <- mov_close(p$host, timeout = 0.2), "timed out")
  expect_false(ok)
  # the handle survives a timed-out close; the peer sees the closed word
  expect_s3_class(mov_recv(p$peer, 0.2), "mov_closed")
  expect_true(mov_close(p$peer, timeout = 5))    # host bit already set
  expect_true(mov_close(p$host, timeout = 5))    # now rendezvous succeeds
})

test_that("close releases the region name on rendezvous", {
  skip_on_os("windows")   # kernel objects have no unlink step to observe
  p <- channel_pair()
  nm <- .Call(mov:::mov_channel_stat, p$host)$name
  expect_no_error(.Call(mov:::mov_region_open, nm, FALSE))
  .Call(mov:::mov_channel_close_signal, p$peer)
  expect_true(mov_close(p$host, timeout = 5))
  expect_error(.Call(mov:::mov_region_open, nm, FALSE), "cannot open")
})

test_that("non-channel handles are rejected across the verb surface", {
  expect_error(mov_send(new.env(), 1), "not a channel handle")
  expect_error(mov_recv("x"), "not a channel handle")
  expect_error(mov_close(42), "not a channel handle")
  expect_error(mov_alive(NULL), "not a channel handle")
})

test_that("sentinels are class-tagged package constants", {
  p <- channel_pair()
  t1 <- mov_recv(p$host, 0)
  t2 <- mov_recv(p$host, 0)
  expect_s3_class(t1, c("mov_timeout", "mov_condition"))
  expect_identical(t1, t2)
})
