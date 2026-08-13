# Channel setup and lifecycle: exact-sized create, preamble + drop slot +
# liveness-dir staging, ready rendezvous, the close protocol's four steps,
# and handle discipline. Ring mechanics are test-ring.R; spawned peers are
# test-peer.R.

test_that("kio_channel validates expr is a language object", {
  expect_error(kio_channel(42), "must be a quoted expression")
  expect_error(kio_channel("kio_recv(ch)"), "must be a quoted expression")
  expect_error(kio_channel(NULL), "must be a quoted expression")
})

test_that("kio_channel_create validates its parameters", {
  expect_error(.Call(kioto:::kio_channel_create, quote(NULL), 3L, 256L, 0,
                     FALSE),
               "capacity must be a power of two")
  expect_error(.Call(kioto:::kio_channel_create, quote(NULL), 64L, 32L, 0,
                     FALSE),
               "slot_size must be a power of two")
  expect_error(.Call(kioto:::kio_channel_create, quote(NULL), 64L, 256L, 100,
                     FALSE),
               "arena_size must be a non-negative multiple of 64")
})

test_that("a fresh channel reports its layout and state", {
  p <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  st <- .Call(kioto:::kio_channel_stat, p[["host"]])
  prefix <- if (.Platform[["OS.type"]] == "windows") "Local\\kio_" else "/kio_"
  expect_true(startsWith(st[["name"]], prefix))
  expect_identical(st[["side"]], "host")
  expect_identical(st[["capacity"]], 128)
  expect_identical(st[["slot_size"]], 512)
  expect_identical(st[["arena_size"]], 8192)
  expect_identical(st[["inline_max"]], 496)
  expect_false(st[["spin"]])
  expect_true(st[["ready"]])
  expect_identical(st[["closed"]], 0L)
  expect_identical(st[["peer_pid"]], as.double(Sys.getpid()))
  expect_identical(.Call(kioto:::kio_channel_stat, p[["peer"]])[["side"]], "peer")
  expect_s3_class(p[["host"]], "kio_channel")
})

test_that("attach validates the region: absent, malformed, or not a channel", {
  expect_error(.Call(kioto:::kio_channel_attach, "0_0"), "cannot open")
  expect_error(.Call(kioto:::kio_channel_attach, "evil'; echo pwned"),
               "malformed region-name suffix")
  expect_error(.Call(kioto:::kio_channel_attach, strrep("a", 40)),
               "malformed region-name suffix")

  # a raw kioto region is not a channel: zeroed bytes fail the magic check
  xp <- .Call(kioto:::kio_region_create, 4096)
  nm <- .Call(kioto:::kio_region_name, xp)
  prefix <- if (.Platform[["OS.type"]] == "windows") "Local\\kio_" else "/kio_"
  suffix <- substr(nm, nchar(prefix) + 1L, nchar(nm))
  expect_error(.Call(kioto:::kio_channel_attach, suffix),
               "invalid channel region")
})

test_that("the close protocol rendezvouses and releases both ends", {
  p <- channel_pair()
  # the peer signals close (the epilogue primitive: flush + bit + wake)
  kio_send(p[["peer"]], "parting")
  .Call(kioto:::kio_channel_close_signal, p[["peer"]])
  # drain-before-verdict: published messages come out before kio_closed
  expect_identical(kio_recv(p[["host"]], 5), "parting")
  expect_s3_class(kio_recv(p[["host"]], 5), "kio_closed")
  expect_s3_class(kio_send(p[["host"]], 1), "kio_closed")
  # host's close finds the peer bit already set, and vice versa
  expect_true(kio_close(p[["host"]], timeout = 5))
  expect_true(kio_close(p[["peer"]], timeout = 5))
  # released handles: verbs error, close is idempotent, alive is FALSE
  expect_error(kio_recv(p[["host"]], 0), "channel handle is closed")
  expect_error(kio_send(p[["host"]], 1), "channel handle is closed")
  expect_true(kio_close(p[["host"]]))
  expect_false(kio_alive(p[["host"]]))
})

test_that("close times out with a warning when the peer never answers", {
  p <- channel_pair()
  expect_warning(ok <- kio_close(p[["host"]], timeout = 0.2), "timed out")
  expect_false(ok)
  # the handle survives a timed-out close; the peer sees the closed word
  expect_s3_class(kio_recv(p[["peer"]], 0.2), "kio_closed")
  expect_true(kio_close(p[["peer"]], timeout = 5))    # host bit already set
  expect_true(kio_close(p[["host"]], timeout = 5))    # now rendezvous succeeds
})

test_that("close releases the region name on rendezvous", {
  skip_on_os("windows")   # kernel objects have no unlink step to observe
  p <- channel_pair()
  nm <- .Call(kioto:::kio_channel_stat, p[["host"]])[["name"]]
  expect_no_error(.Call(kioto:::kio_region_open, nm, FALSE))
  .Call(kioto:::kio_channel_close_signal, p[["peer"]])
  expect_true(kio_close(p[["host"]], timeout = 5))
  expect_error(.Call(kioto:::kio_region_open, nm, FALSE), "cannot open")
})

test_that("non-channel handles are rejected across the verb surface", {
  expect_error(kio_send(new.env(), 1), "not a channel handle")
  expect_error(kio_recv("x"), "not a channel handle")
  expect_error(kio_close(42), "not a channel handle")
  expect_error(kio_alive(NULL), "not a channel handle")
})

test_that("sentinels are class-tagged package constants", {
  p <- channel_pair()
  t1 <- kio_recv(p[["host"]], 0)
  t2 <- kio_recv(p[["host"]], 0)
  expect_s3_class(t1, c("kio_timeout", "kio_sentinel"))
  expect_identical(t1, t2)
})

test_that("kio_is_sentinel tests provenance, not class", {
  p <- channel_pair()
  t <- kio_recv(p[["host"]], 0)
  expect_true(kio_is_sentinel(t))
  # class alone is spoofable in-band; identity is not
  spoof <- structure("timeout", class = c("kio_timeout", "kio_sentinel"))
  expect_true(inherits(spoof, "kio_sentinel"))
  expect_false(kio_is_sentinel(spoof))
  # a forwarded sentinel arrives as a copy: a payload, not a verdict
  kio_send(p[["host"]], t)
  fwd <- kio_recv(p[["peer"]], 5)
  expect_s3_class(fwd, "kio_timeout")
  expect_false(kio_is_sentinel(fwd))
  expect_false(kio_is_sentinel(NULL))
  expect_false(kio_is_sentinel("timeout"))
})

test_that("a finite-timeout empty recv expires both parked and mid-spin", {
  p <- channel_pair()
  # parked: the wait outlives the 16 us spin budget, so the bounded sleep
  # runs out — the post-park deadline exit
  expect_s3_class(kio_recv(p[["host"]], timeout = 0.05), "kio_timeout")
  # mid-spin: a deadline inside the spin budget expires before any park
  expect_s3_class(kio_recv(p[["host"]], timeout = 1e-6), "kio_timeout")
  channel_end(p)
})

test_that("an overlong liveness directory is rejected before create", {
  old <- Sys.getenv("KIOTO_LIVENESS_DIR", unset = NA)
  on.exit(
    if (is.na(old)) {
      Sys.unsetenv("KIOTO_LIVENESS_DIR")
    } else {
      Sys.setenv(KIOTO_LIVENESS_DIR = old)
    },
    add = TRUE
  )
  Sys.setenv(KIOTO_LIVENESS_DIR = strrep("x", 1000))
  expect_error(
    .Call(kioto:::kio_channel_create, quote(NULL), 64L, 256L, 4096, FALSE),
    "liveness directory path too long"
  )
})
