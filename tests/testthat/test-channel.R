# Channel setup and lifecycle: exact-sized create, preamble + drop slot +
# liveness-dir staging, ready rendezvous, the close protocol's four steps,
# and handle discipline. Ring mechanics are test-ring.R; spawned peers are
# test-peer.R.

test_that("mizu_channel validates expr is a language object", {
  expect_error(mizu_channel(42), "must be a quoted expression")
  # a character scalar is now a source-string drop, not an error
  expect_error(mizu_channel(c("a", "b")), "must be a quoted expression")
  expect_error(mizu_channel(NA_character_), "must be a quoted expression")
  expect_error(mizu_channel(NULL), "must be a quoted expression")
})

test_that("mizu_channel_create validates its parameters", {
  expect_error(
    .Call(mizu:::mizu_channel_create, quote(NULL), 3L, 256L, 0, FALSE),
    "capacity must be a power of two"
  )
  expect_error(
    .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 32L, 0, FALSE),
    "slot_size must be a power of two"
  )
  expect_error(
    .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 100, FALSE),
    "arena_size must be a non-negative multiple of 64"
  )
  expect_error(
    .Call(mizu:::mizu_channel_create, c("a", "b"), 64L, 256L, 0, FALSE),
    "a source drop must be a single non-NA string"
  )
})

test_that("a fresh channel reports its layout and state", {
  p <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  st <- .Call(mizu:::mizu_channel_stat, p[["host"]])
  prefix <- if (.Platform[["OS.type"]] == "windows") {
    "Local\\mizu_"
  } else {
    "/mizu_"
  }
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
  expect_identical(
    .Call(mizu:::mizu_channel_stat, p[["peer"]])[["side"]],
    "peer"
  )
  expect_s3_class(p[["host"]], "mizu_channel")
})

test_that("a raising launcher leaves no channel region behind", {
  suffix <- NULL
  expect_error(
    mizu_channel(
      quote(1 + 1),
      launcher = function(token) {
        suffix <<- token
        stop("boom")
      }
    ),
    "boom"
  )
  expect_error(.Call(mizu:::mizu_channel_attach, suffix, NULL), "cannot open")
})

test_that("attach validates the region: absent, malformed, or not a channel", {
  expect_error(
    .Call(mizu:::mizu_channel_attach, 42, NULL),
    "expected a region-name suffix"
  )
  expect_error(.Call(mizu:::mizu_channel_attach, "0_0", NULL), "cannot open")
  expect_error(
    .Call(mizu:::mizu_channel_attach, "evil'; echo pwned", NULL),
    "malformed region-name suffix"
  )
  expect_error(
    .Call(mizu:::mizu_channel_attach, strrep("a", 40), NULL),
    "malformed region-name suffix"
  )

  # a raw mizu region is not a channel: zeroed bytes fail the magic check
  xp <- .Call(mizu:::mizu_region_create, 4096)
  nm <- .Call(mizu:::mizu_region_name, xp)
  prefix <- if (.Platform[["OS.type"]] == "windows") {
    "Local\\mizu_"
  } else {
    "/mizu_"
  }
  suffix <- substr(nm, nchar(prefix) + 1L, nchar(nm))
  expect_error(
    .Call(mizu:::mizu_channel_attach, suffix, NULL),
    "invalid channel region"
  )
})

test_that("a second attach finds the peer liveness lock held", {
  p <- channel_pair()
  on.exit(channel_end(p))
  expect_error(
    .Call(
      mizu:::mizu_channel_attach,
      .Call(mizu:::mizu_channel_suffix, p[["host"]]),
      NULL
    ),
    "cannot lock peer liveness file"
  )
})

test_that("attach rejects a preamble with a missing liveness dir", {
  host <- .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 4096, FALSE)
  on.exit(.Call(mizu:::mizu_channel_destroy, host))
  nm <- .Call(mizu:::mizu_channel_stat, host)[["name"]]
  rw <- .Call(mizu:::mizu_region_open, nm, TRUE)
  .Call(mizu:::mizu_poke, rw, 56L, raw(8L)) # livedir_size = 0
  expect_error(
    .Call(
      mizu:::mizu_channel_attach,
      .Call(mizu:::mizu_channel_suffix, host),
      NULL
    ),
    "liveness directory path"
  )
})

test_that("attach fails when the host pid cannot be watched", {
  # POSIX-only arm: on Windows an unopenable pid is ERROR_INVALID_PARAMETER,
  # which the core treats as already-gone (fires the watch, attach succeeds).
  skip_on_os("windows")
  host <- .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 4096, FALSE)
  on.exit(.Call(mizu:::mizu_channel_destroy, host))
  nm <- .Call(mizu:::mizu_channel_stat, host)[["name"]]
  rw <- .Call(mizu:::mizu_region_open, nm, TRUE)
  .Call(mizu:::mizu_poke, rw, 16L, raw(8L)) # host_pid = 0
  expect_error(
    .Call(
      mizu:::mizu_channel_attach,
      .Call(mizu:::mizu_channel_suffix, host),
      NULL
    ),
    "cannot watch host process"
  )
})

test_that("attach after the host died fails and unlinks the channel", {
  skip_if_no_child_mizu()
  f <- tfile()
  mizu:::mizu_spawn(sprintf(
    '
    library(mizu)
    host <- .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 0L, FALSE)
    writeLines(c(as.character(Sys.getpid()),
                 .Call(mizu:::mizu_channel_suffix, host)), %s)
    Sys.sleep(60)
  ',
    deparse(f)
  ))
  expect_true(wait_for_file(f, timeout = 30))
  expect_true(wait_until(length(readLines(f)) == 2L))
  lines <- readLines(f)
  kill_hard(as.integer(lines[1L]))
  expect_true(wait_until(!pid_alive(as.integer(lines[1L])), timeout = 15))
  # Windows destroys the region with the host's handle table, so the
  # attach fails earlier, at the region open
  expect_error(
    .Call(mizu:::mizu_channel_attach, lines[2L], NULL),
    if (.Platform[["OS.type"]] == "windows") {
      "cannot open channel region"
    } else {
      "host died before the channel was established"
    }
  )
})

test_that("the close protocol rendezvouses and releases both ends", {
  p <- channel_pair()
  # the peer signals close (the epilogue primitive: flush + bit + wake)
  mizu_send(p[["peer"]], "parting")
  .Call(mizu:::mizu_channel_close_signal, p[["peer"]])
  # drain-before-verdict: published messages come out before mizu_closed
  expect_identical(mizu_recv(p[["host"]], 5), "parting")
  expect_s3_class(mizu_recv(p[["host"]], 5), "mizu_closed")
  expect_s3_class(mizu_send(p[["host"]], 1), "mizu_closed")
  # host's close finds the peer bit already set, and vice versa
  expect_true(mizu_close(p[["host"]], timeout = 5))
  expect_true(mizu_close(p[["peer"]], timeout = 5))
  # released handles: verbs error, close is idempotent, alive is FALSE
  expect_error(mizu_recv(p[["host"]], 0), "channel handle is closed")
  expect_error(mizu_send(p[["host"]], 1), "channel handle is closed")
  expect_true(mizu_close(p[["host"]]))
  expect_false(mizu_alive(p[["host"]]))
})

test_that("close times out with a warning when the peer never answers", {
  p <- channel_pair()
  expect_warning(ok <- mizu_close(p[["host"]], timeout = 0.2), "timed out")
  expect_false(ok)
  # the handle survives a timed-out close; the peer sees the closed word
  expect_s3_class(mizu_recv(p[["peer"]], 0.2), "mizu_closed")
  expect_true(mizu_close(p[["peer"]], timeout = 5)) # host bit already set
  expect_true(mizu_close(p[["host"]], timeout = 5)) # now rendezvous succeeds
})

test_that("close releases the region name on rendezvous", {
  skip_on_os("windows") # kernel objects have no unlink step to observe
  p <- channel_pair()
  nm <- .Call(mizu:::mizu_channel_stat, p[["host"]])[["name"]]
  expect_no_error(.Call(mizu:::mizu_region_open, nm, FALSE))
  .Call(mizu:::mizu_channel_close_signal, p[["peer"]])
  expect_true(mizu_close(p[["host"]], timeout = 5))
  expect_error(.Call(mizu:::mizu_region_open, nm, FALSE), "cannot open")
})

test_that("non-channel handles are rejected across the verb surface", {
  expect_error(mizu_send(new.env(), 1), "not a channel handle")
  expect_error(mizu_recv("x"), "not a channel handle")
  expect_error(mizu_close(42), "not a channel handle")
  expect_error(mizu_alive(NULL), "not a channel handle")
})

test_that("sentinels are class-tagged package constants", {
  p <- channel_pair()
  t1 <- mizu_recv(p[["host"]], 0)
  t2 <- mizu_recv(p[["host"]], 0)
  expect_s3_class(t1, c("mizu_timeout", "mizu_sentinel"))
  expect_identical(t1, t2)
})

test_that("mizu_is_sentinel tests provenance, not class", {
  p <- channel_pair()
  t <- mizu_recv(p[["host"]], 0)
  expect_true(mizu_is_sentinel(t))
  # class alone is spoofable in-band; identity is not
  spoof <- structure("timeout", class = c("mizu_timeout", "mizu_sentinel"))
  expect_true(inherits(spoof, "mizu_sentinel"))
  expect_false(mizu_is_sentinel(spoof))
  # a forwarded sentinel arrives as a copy: a payload, not a verdict
  mizu_send(p[["host"]], t)
  fwd <- mizu_recv(p[["peer"]], 5)
  expect_s3_class(fwd, "mizu_timeout")
  expect_false(mizu_is_sentinel(fwd))
  expect_false(mizu_is_sentinel(NULL))
  expect_false(mizu_is_sentinel("timeout"))
})

test_that("a finite-timeout empty recv expires both parked and mid-spin", {
  p <- channel_pair()
  # parked: the wait outlives the 16 us spin budget, so the bounded sleep
  # runs out — the post-park deadline exit
  expect_s3_class(mizu_recv(p[["host"]], timeout = 0.05), "mizu_timeout")
  # mid-spin: a deadline inside the spin budget expires before any park
  expect_s3_class(mizu_recv(p[["host"]], timeout = 1e-6), "mizu_timeout")
  channel_end(p)
})

test_that("verbs reject an NA timeout instead of waiting forever (R-08)", {
  p <- channel_pair()
  expect_error(mizu_recv(p[["host"]], timeout = NA_real_), "not NA")
  expect_error(mizu_recv(p[["host"]], timeout = NA), "not NA")
  expect_error(mizu_recv_batch(p[["host"]], timeout = NA_real_), "not NA")
  expect_error(mizu_close(p[["host"]], timeout = NA_real_), "not NA")
  expect_error(
    .Call(mizu:::mizu_channel_ready_wait, p[["host"]], NA_real_),
    "not NA"
  )
  # the channel survives the rejections: never a wait entered
  mizu_send(p[["host"]], 42L)
  expect_identical(mizu_recv(p[["peer"]], 5), 42L)
  channel_end(p)
})

test_that("a forked child's GC does not destroy the parent's channel (R-14)", {
  skip_on_os("windows") # no fork
  skip_if_no_fork() # Positron/RStudio sessions block mcfork
  p <- channel_pair()
  mizu_send(p[["host"]], "before")
  expect_identical(mizu_recv(p[["peer"]], 5), "before")
  env <- environment()
  res <- parallel::mcparallel(
    {
      rm(list = ls(env), envir = env) # the child drops its inherited handles
      invisible(gc()) # their finalizers run here — abandon, not destroy
      TRUE
    },
    silent = TRUE
  )
  expect_identical(parallel::mccollect(res)[[1L]], TRUE)
  mizu_send(p[["host"]], "after")
  expect_identical(mizu_recv(p[["peer"]], 5), "after")
  channel_end(p)
})

test_that("a closure over a missing namespace reads as unserialize does", {
  ns <- new.env()
  info <- new.env()
  assign("spec", c(name = "zzmissing", version = "0.0.1"), envir = info)
  assign(".__NAMESPACE__.", info, envir = ns)
  f <- function(x) x + 1
  environment(f) <- ns
  p <- channel_pair()
  mizu_send(p[["host"]], f)
  mizu_send(p[["host"]], "after")
  g <- mizu_recv(p[["peer"]], 1)
  expect_identical(environment(g), globalenv())
  expect_identical(g(1), 2)
  expect_identical(environment(unserialize(serialize(f, NULL))), globalenv())
  expect_identical(mizu_recv(p[["peer"]], 1), "after")
  channel_end(p)
})

test_that("an overlong liveness directory is rejected before create", {
  old <- Sys.getenv("MIZU_LIVENESS_DIR", unset = NA)
  on.exit(
    if (is.na(old)) {
      Sys.unsetenv("MIZU_LIVENESS_DIR")
    } else {
      Sys.setenv(MIZU_LIVENESS_DIR = old)
    },
    add = TRUE
  )
  Sys.setenv(MIZU_LIVENESS_DIR = strrep("x", 1000))
  expect_error(
    .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 4096, FALSE),
    "liveness directory path too long"
  )
})

test_that("the test identity override validates its shape", {
  p <- channel_pair()
  on.exit(channel_end(p))
  suffix <- .Call(mizu:::mizu_channel_suffix, p$host)
  expect_error(
    .Call(mizu:::mizu_channel_attach, suffix, 42L),
    "identity pair"
  )
})

test_that("attach rejects a non-R drop and reads a source drop", {
  host <- .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 0L, FALSE)
  suffix <- .Call(mizu:::mizu_channel_suffix, host)
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_channel_stat, host)[["name"]],
    TRUE
  )
  doff <- i64v(.Call(mizu:::mizu_peek, rw, 32, 8))
  dsz <- i64v(.Call(mizu:::mizu_peek, rw, 40, 8))
  .Call(mizu:::mizu_poke, rw, doff, charToRaw("X"))
  expect_error(
    .Call(mizu:::mizu_channel_attach, suffix, NULL),
    "foreign channel drop"
  )
  invisible(gc()) # the failed attach's peer side tears down
  src <- charToRaw("x + 1")
  .Call(mizu:::mizu_poke, rw, doff, charToRaw("S"))
  .Call(mizu:::mizu_poke, rw, doff + 1L, src)
  .Call(mizu:::mizu_poke, rw, 40, i64raw(1L + length(src)))
  out <- .Call(mizu:::mizu_channel_attach, suffix, NULL)
  expect_identical(out[[2L]], "x + 1")
  .Call(mizu:::mizu_channel_close_signal, out[[1L]])
  .Call(mizu:::mizu_channel_close, host, 5)
  .Call(mizu:::mizu_channel_close, out[[1L]], 5)
})

test_that("create fails when the liveness directory is unusable", {
  old <- Sys.getenv("MIZU_LIVENESS_DIR", unset = NA)
  on.exit(
    if (is.na(old)) {
      Sys.unsetenv("MIZU_LIVENESS_DIR")
    } else {
      Sys.setenv(MIZU_LIVENESS_DIR = old)
    },
    add = TRUE
  )
  Sys.setenv(MIZU_LIVENESS_DIR = paste(rep("a", 950L), collapse = ""))
  expect_error(
    .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 4096, FALSE),
    "liveness directory path too long"
  )
  Sys.setenv(MIZU_LIVENESS_DIR = file.path(tempdir(), "no-such-dir"))
  expect_error(
    .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 4096, FALSE),
    "cannot lock host liveness file"
  )
})

test_that("a pre-ready host probes alive and destroys cleanly", {
  host <- .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 4096, FALSE)
  expect_true(.Call(mizu:::mizu_channel_alive, host))
  .Call(mizu:::mizu_channel_destroy, host)
  expect_false(.Call(mizu:::mizu_channel_alive, host))
})

test_that("destroying the host first releases the region name", {
  p <- channel_pair()
  .Call(mizu:::mizu_channel_destroy, p[["host"]])
  expect_false(.Call(mizu:::mizu_channel_alive, p[["host"]]))
  .Call(mizu:::mizu_channel_destroy, p[["peer"]])
})

test_that("a second peer attach fails on the held liveness lock", {
  p <- channel_pair()
  on.exit(channel_end(p))
  expect_error(
    .Call(
      mizu:::mizu_channel_attach,
      .Call(mizu:::mizu_channel_suffix, p[["host"]]),
      NULL
    ),
    "cannot lock peer liveness file"
  )
})

test_that("create resolves the default temp dir without TMPDIR", {
  old <- Sys.getenv("TMPDIR", unset = NA)
  skip_if(is.na(old))
  Sys.unsetenv("TMPDIR")
  on.exit(Sys.setenv(TMPDIR = old))
  host <- .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 4096, FALSE)
  expect_true(.Call(mizu:::mizu_channel_alive, host))
  .Call(mizu:::mizu_channel_destroy, host)
})
