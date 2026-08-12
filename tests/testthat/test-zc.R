# Zero-copy payload tiers (SHM_VEC / REF): a mori-layout object past the
# inline budget and the zc floor crosses as an ALTREP view over the spill
# region's own pages — no receive-side copy — with the refcount + ledger
# release protocol behind it (zc.c). The in-process harnesses make the
# refcount transitions deterministic; the death backstop is cross-process.

is_view <- function(x) .Call(kioto:::kio_zc_view_check, x)
rc_of <- function(x) .Call(kioto:::kio_zc_refcount, x) # c(refcount, flags)
chan_ledger <- function(ch) .Call(kioto:::kio_channel_stat, ch)$ledger_entries
chan_fl <- function(ch) .Call(kioto:::kio_channel_stat, ch)$fl_entries

test_that("tier selection: big atomic vectors cross as views, others copy", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000) # 800 KB: past budget and floor
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  expect_true(is_view(y))
  expect_identical(as.numeric(y), x)

  kio_send(p$host, 1:10) # small: RAWVEC copy
  expect_false(is_view(kio_recv(p$peer, 5)))

  kio_send(p$host, 1:2^27) # ALTREP input: stays a compact stream
  w <- kio_recv(p$peer, 30)
  expect_false(is_view(w))
  expect_equal(length(w), 2^27)

  xa <- runif(20000) # attributes ride the attrs blob
  names(xa) <- paste0("n", seq_along(xa))
  kio_send(p$host, xa)
  ya <- kio_recv(p$peer, 5)
  expect_true(is_view(ya))
  expect_identical(ya, xa)
  channel_end(p)
})

test_that("rc_of on a non-view returns integer(0) (no chain walk)", {
  expect_identical(rc_of(runif(10)), integer(0))
  expect_identical(rc_of(1:10), integer(0)) # ALTREP, but not a kioto view
  expect_identical(rc_of(NULL), integer(0))
})

test_that("a held view pins its region in the ledger until release", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  # the host's next send reaps the first keeper: count > 0 (y held), so the
  # region waits in the ledger instead of rejoining the free list
  kio_send(p$host, x)
  expect_identical(chan_ledger(p$host), 1L)
  y2 <- kio_recv(p$peer, 5)
  expect_true(is_view(y2))

  # release the views; the regions rejoin the free list (swept at the
  # reap) and the next sends pop them — fl_hits moves
  rm(y, y2)
  invisible(gc())
  hits <- .Call(kioto:::kio_channel_stat, p$host)$fl_hits
  kio_send(p$host, x)
  kio_send(p$host, x)
  expect_identical(chan_ledger(p$host), 0L)
  expect_gt(.Call(kioto:::kio_channel_stat, p$host)$fl_hits, hits)
  expect_identical(as.numeric(kio_recv(p$peer, 5)), x)
  expect_identical(as.numeric(kio_recv(p$peer, 5)), x)
  channel_end(p)
})

test_that("views are copy-on-write: mutation never disturbs the region", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  y[1] <- -1 # COW: materializes a private copy
  expect_identical(y[1], -1)
  expect_identical(as.numeric(y)[-1], x[-1])
  # the region's next payload is undisturbed
  kio_send(p$host, x)
  expect_identical(as.numeric(kio_recv(p$peer, 5)), x)
  channel_end(p)
})

test_that("COW materialization releases the region early (no GC needed)", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  kio_send(p$host, x) # reap: host loan drops; y held
  expect_identical(chan_ledger(p$host), 1L)
  y[1] <- -1 # COW: the release hook fires here
  kio_send(p$host, x) # reap: count 0 -> free list
  expect_identical(chan_ledger(p$host), 0L)
  channel_end(p)
})

test_that("a received view re-sends as REF — zero bytes move", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  kio_send(p$peer, y) # REF back to the host
  z <- kio_recv(p$host, 5)
  expect_true(is_view(z))
  expect_identical(as.numeric(z), x)
  channel_end(p)
})

test_that("pool results cross as views; a held result pins the worker's region", {
  p <- pool_pair()
  t <- kio_submit(p$ctrl, runif(100000))
  pool_step(p)
  r <- kio_collect(t, 5)
  expect_true(is_view(r))
  # the worker's keeper sweep releases its loan; the held view keeps the
  # region in the ledger
  pool_step(p)
  expect_identical(.Call(kioto:::kio_pool_zc_info, p$wk)[[2L]], 1L)
  rm(r)
  invisible(gc())
  t2 <- kio_submit(p$ctrl, runif(100000))
  pool_step(p)
  r2 <- kio_collect(t2, 5)
  expect_identical(.Call(kioto:::kio_pool_zc_info, p$wk)[[2L]], 0L)
  expect_true(is_view(r2))
  pool_end(p)
})

test_that("zc churn falls back to SHM_RAW: reuse resumes without any GC", {
  p <- pool_pair()
  x <- runif(20000) # 160 KB: past the zc floor, spills
  # collected views pin their regions until GC (none here), so SHM_VEC
  # would churn a fresh region per payload; the fallback's SHM_RAW
  # surrenders deterministically at consumer-done, restoring warm reuse
  for (i in seq_len(6L)) {
    t <- kio_submit(p$ctrl, x, x = x)
    pool_step(p)
    expect_identical(kio_collect(t, 5), x)
  }
  st <- kio_pool_stats(p$ctrl)$submitters
  st <- st[st$status == "live", ]
  expect_gt(st$spill_reuse, 0)
  pool_end(p)
})

test_that("the churn fallback clears once lent regions reclaim", {
  p <- pool_pair()
  x <- runif(20000)
  held <- vector("list", 4L) # held views pin their regions: churn
  for (i in seq_along(held)) {
    t <- kio_submit(p$ctrl, x, x = x)
    pool_step(p)
    held[[i]] <- kio_collect(t, 5)
  }
  rm(held)
  invisible(gc())
  for (i in seq_len(3L)) { # the sweeps reclaim; staging returns to SHM_VEC
    t <- kio_submit(p$ctrl, x, x = x)
    pool_step(p)
    r <- kio_collect(t, 5)
  }
  expect_true(is_view(r))
  pool_end(p)
})

test_that("REF round-trips at any viewed size", {
  p <- channel_pair(arena_size = 0)
  for (n in c(5000, 8 * 1000 * 1000)) {
    # 40 KB and 64 MiB views
    x <- runif(n)
    kio_send(p$host, x)
    y <- kio_recv(p$peer, 30)
    expect_true(is_view(y))
    kio_send(p$peer, y) # REF back
    z <- kio_recv(p$host, 30)
    expect_true(is_view(z))
    expect_identical(as.numeric(z), x)
  }
  channel_end(p)
})

test_that("string vectors cross as views (MORS), small ones copy", {
  p <- channel_pair(arena_size = 0)
  x <- rep(c("café", "naïve", NA_character_, "", "plain"), 10000) # ~1 MB layout
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  expect_true(is_view(y))
  expect_identical(y, x)

  kio_send(p$host, c("a", "b")) # small: inline copy
  expect_false(is_view(kio_recv(p$peer, 5)))

  xa <- paste0("v", 1:50000) # attributes ride the attrs blob
  names(xa) <- paste0("n", 1:50000)
  kio_send(p$host, xa)
  ya <- kio_recv(p$peer, 5)
  expect_true(is_view(ya))
  expect_identical(ya, xa)
  channel_end(p)
})

test_that("a held string view pins its region until GC", {
  p <- channel_pair(arena_size = 0)
  x <- paste0("s", 1:50000)
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  kio_send(p$host, x) # reap: the loan drops; y pins the region
  expect_identical(chan_ledger(p$host), 1L)
  rm(y)
  invisible(gc())
  kio_send(p$host, x) # reap: count 0 -> free list
  expect_identical(chan_ledger(p$host), 0L)
  channel_end(p)
})

test_that("string views duplicate on mutation: the region is undisturbed", {
  p <- channel_pair(arena_size = 0)
  x <- paste0("s", 1:50000)
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  y2 <- y # NAMED bump: subassignment duplicates (ALTSTRING has no Set_elt)
  y2[1] <- "mutated"
  expect_identical(y2[1], "mutated")
  expect_identical(y[1], x[1])
  kio_send(p$host, x)
  expect_identical(kio_recv(p$peer, 5), x)
  channel_end(p)
})

test_that("list trees cross as views (MORL): leaves arrive as views", {
  p <- channel_pair(arena_size = 0)
  x <- list(
    nums = runif(100000),
    strs = paste0("s", 1:5000),
    sub = list(a = seq_len(30000) + 0, b = NULL),
    f = sum,
    nil = NULL
  )
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  expect_true(is_view(y))
  expect_true(is_view(y$nums))
  expect_true(is_view(y$strs))
  expect_true(is_view(y$sub))
  expect_true(is_view(y$sub$a))
  expect_identical(as.numeric(y$nums), x$nums)
  expect_identical(y$strs, x$strs)
  expect_identical(as.numeric(y$sub$a), x$sub$a)
  expect_null(y$sub$b)
  expect_identical(y$f, sum) # a serialized leaf arrives as a plain object
  expect_null(y$nil)
  expect_identical(names(y), names(x))
  channel_end(p)
})

test_that("a data frame crosses as a view with its class and row names", {
  p <- channel_pair(arena_size = 0)
  x <- data.frame(a = runif(100000), b = paste0("s", 1:100000))
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  expect_true(is_view(y))
  expect_s3_class(y, "data.frame")
  expect_identical(nrow(y), 100000L)
  expect_identical(as.numeric(y$a), x$a)
  expect_identical(y$b, x$b)
  channel_end(p)
})

test_that("a list view re-sends as REF even after element access", {
  p <- channel_pair(arena_size = 0)
  x <- list(nums = runif(100000), strs = paste0("s", 1:5000))
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  expect_identical(rc_of(y), c(2L, 0L))
  invisible(y$nums[1]) # the element cache (data2) is not a materialization
  kio_send(p$peer, y)
  z <- kio_recv(p$host, 5)
  expect_true(is_view(z))
  expect_identical(rc_of(y), c(2L, 1L)) # counted; escape marked REFHELD
  expect_identical(as.numeric(z$nums), x$nums)
  channel_end(p)
})

test_that("an element extracted from a list view re-sends as path-form REF", {
  p <- channel_pair(arena_size = 0)
  x <- list(a = runif(100000), b = runif(100000))
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  kio_send(p$peer, y$a) # REF "/kio_...[1]"
  z <- kio_recv(p$host, 5)
  expect_true(is_view(z))
  expect_identical(as.numeric(z), x$a)
  channel_end(p)
})

test_that("an extracted element view pins the region past the root view's GC", {
  p <- channel_pair(arena_size = 0)
  x <- list(a = runif(100000), b = runif(100000))
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  sub <- y$a
  kio_send(p$host, x) # reap: the loan drops; the view chain pins the region
  expect_identical(chan_ledger(p$host), 1L)
  rm(y)
  invisible(gc())
  kio_send(p$host, x) # the element view still pins: no early release
  expect_identical(chan_ledger(p$host), 1L)
  expect_identical(as.numeric(sub), x$a)
  rm(sub)
  invisible(gc())
  kio_send(p$host, x) # count 0 -> free list
  expect_identical(chan_ledger(p$host), 0L)
  channel_end(p)
})

test_that("a view nested in a big list tree keeps the tree on the serialize tiers", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  kio_send(p$host, x)
  v <- kio_recv(p$peer, 5)
  # the tree's eligible leaf (400 KB) would stage as MORL alone; the nested
  # view rejects it — the view must cross by reference, never be copied in
  kio_send(p$peer, list(runif(50000), v))
  w <- kio_recv(p$host, 30)
  expect_false(is_view(w))
  expect_true(is_view(w[[2L]]))
  expect_identical(rc_of(v), c(2L, 1L))
  channel_end(p)
})

test_that("a foreign ALTREP leaf keeps the tree on the serialize tiers", {
  p <- channel_pair(arena_size = 0)
  x <- list(big = runif(100000), seq = 1:2^20)
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 30)
  expect_false(is_view(y))
  expect_equal(y$seq, 1:2^20) # the compact sequence stayed compact
  expect_identical(y$big, x$big)
  channel_end(p)
})

test_that("pairlists keep their type (the MORL layout coerces to VECSXP)", {
  p <- channel_pair(arena_size = 0)
  x <- pairlist(a = runif(50000), b = "x")
  kio_send(p$host, x)
  y <- kio_recv(p$peer, 5)
  expect_type(y, "pairlist")
  expect_identical(y, x)
  channel_end(p)
})

test_that("a mid-chain re-sender's death leaks + unlinks, never recycles under a live view", {
  skip_if_no_child_kioto()
  ch <- kio_channel(
    quote({
      x <- kio_recv(ch, timeout = 30) # a view over the host's region
      kio_send(ch, x) # REF it back (marks the region REFHELD)
      Sys.sleep(30)
    }),
    arena_size = 0
  )
  x <- runif(100000)
  kio_send(ch, x)
  z <- kio_recv(ch, 30) # the host's own view, via REF
  expect_true(is_view(z))
  kio_send(ch, x) # reap: the first region -> ledger
  expect_identical(chan_ledger(ch), 1L)
  nm <- .Call(kioto:::kio_channel_stat, ch)$name

  pid <- .Call(kioto:::kio_channel_stat, ch)$peer_pid
  kill_hard(pid)
  r <- kio_recv(ch, 30) # the verdict probes; force path runs
  expect_s3_class(r, "kio_peer_gone")
  # REFHELD: the region leaks + unlinks — it never rejoins the free list,
  # and the host's live view still reads its pages
  expect_identical(chan_ledger(ch), 0L)
  expect_identical(chan_fl(ch), 0L)
  expect_identical(as.numeric(z), x)
  kio_close(ch, 10)
})

test_that("a view nested in a larger payload resolves counted (channel)", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  kio_send(p$host, x)
  v <- kio_recv(p$peer, 5)
  expect_identical(rc_of(v), c(2L, 0L)) # the producer's loan + the view

  kio_send(p$host, x) # reap: the loan drops; v pins the region
  expect_identical(rc_of(v), c(1L, 0L))

  # the view never reaches tier selection nested in a list: it crosses on
  # the serialize-hook path, and the receive-side resolve is counted
  kio_send(p$peer, list("wrap", v))
  w <- kio_recv(p$host, 30)
  expect_true(is_view(w[[2L]]))
  expect_identical(rc_of(v), c(2L, 1L)) # resolved counted; escape marked REFHELD
  expect_identical(as.numeric(w[[2L]]), x)
  channel_end(p)
})

test_that("pool task args carry views counted; a returned view echoes by REF", {
  p <- pool_pair()
  t <- kio_submit(p$ctrl, runif(100000))
  pool_step(p)
  v <- kio_collect(t, 5)
  pool_step(p) # the worker's keeper sweep drops its loan
  expect_identical(rc_of(v), c(1L, 0L))

  # the view nests inside the task's argument list (the serialize-hook
  # path): the escape is marked at submit, the worker's resolve counted
  t2 <- kio_submit(p$ctrl, sum(x), x = v)
  expect_identical(rc_of(v)[2L], 1L)
  pool_step(p)
  expect_identical(rc_of(v)[1L], 2L) # v + the worker's resolved view
  expect_identical(kio_collect(t2, 5), sum(v))

  # a worker returning a received view stages it top-level: REF
  t3 <- kio_submit(p$ctrl, identity(x), x = v)
  pool_step(p)
  w <- kio_collect(t3, 5)
  expect_true(is_view(w))
  expect_identical(as.numeric(w), as.numeric(v))
  pool_end(p)
})

test_that("a worker re-submits a received view by reference (nested composition)", {
  p <- pool_pair()
  t <- kio_submit(p$ctrl, runif(100000))
  pool_step(p)
  v <- kio_collect(t, 5)

  # the worker resolves its arg counted, re-submits it nested (the
  # serialize-hook path again), and the chain returns a view of one region
  t2 <- kio_submit(
    p$ctrl,
    {
      s <- kio_submit(pool, identity(x), x = x)
      kio_collect(s, timeout = 5)
    },
    x = v
  )
  pool_step(p)
  w <- kio_collect(t2, 5)
  expect_true(is_view(w))
  expect_identical(as.numeric(w), as.numeric(v))
  pool_end(p)
})

test_that("foreign mori objects ride the serialize-hook path", {
  skip_if_not_installed("mori")
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  kio_send(p$host, mori::share(x))
  y <- kio_recv(p$peer, 30)
  expect_false(is_view(y)) # not kioto-native: no refcount protocol
  expect_true(mori::is_shared(y)) # resolved by mori's own hooks
  expect_identical(as.numeric(y), x)
  channel_end(p)
})

test_that("a mid-chain re-sender killed with a nested view outstanding leaks + unlinks", {
  skip_if_no_child_kioto()
  ch <- kio_channel(
    quote({
      x <- kio_recv(ch, timeout = 30) # a view over the host's region
      kio_send(ch, list("wrap", x)) # nested re-send: the emit marks REFHELD
      Sys.sleep(30)
    }),
    arena_size = 0
  )
  x <- runif(100000)
  kio_send(ch, x)
  w <- kio_recv(ch, 30) # w[[2]] resolved counted
  expect_true(is_view(w[[2L]]))
  kio_send(ch, x) # reap: the first region -> ledger
  expect_identical(chan_ledger(ch), 1L)

  pid <- .Call(kioto:::kio_channel_stat, ch)$peer_pid
  kill_hard(pid)
  r <- kio_recv(ch, 30) # the verdict probes; the force path runs
  expect_s3_class(r, "kio_peer_gone")
  # REFHELD: the region leaks + unlinks — the peer's count is never
  # reclaimed, and the host's live view still reads its pages
  expect_identical(chan_ledger(ch), 0L)
  expect_identical(chan_fl(ch), 0L)
  expect_identical(rc_of(w[[2L]]), c(2L, 1L))
  expect_identical(as.numeric(w[[2L]]), x)
  kio_close(ch, 10)
})

test_that("pool string and list results cross as views", {
  p <- pool_pair()
  t <- kio_submit(p$ctrl, paste0("w-", 1:30000))
  pool_step(p)
  r <- kio_collect(t, 5)
  expect_true(is_view(r))
  expect_identical(r, paste0("w-", 1:30000))

  t2 <- kio_submit(p$ctrl, list(a = runif(100000), b = paste0("x", 1:20000)))
  pool_step(p)
  r2 <- kio_collect(t2, 5)
  expect_true(is_view(r2))
  expect_true(is_view(r2$a))
  expect_true(is_view(r2$b))
  expect_identical(r2$b, paste0("x", 1:20000))
  pool_end(p)
})

test_that("a big vector task argument round-trips", {
  # the payload list(expr, args) stages as MORL past the floor: the worker's
  # arg is a view over the payload region, not an unserialized copy
  p <- pool_pair()
  v <- runif(200000)
  t <- kio_submit(p$ctrl, .Call(kioto:::kio_zc_view_check, v), v = v)
  pool_step(p)
  expect_true(kio_collect(t, 5))
  t2 <- kio_submit(p$ctrl, sum(v), v = v)
  pool_step(p)
  expect_identical(kio_collect(t2, 5), sum(v))
  pool_end(p)
})

test_that("string and list views round-trip cross-process", {
  skip_if_no_child_kioto()
  ch <- kio_channel(
    quote({
      is_view <- function(x) .Call(kioto:::kio_zc_view_check, x)
      s <- kio_recv(ch, timeout = 30)
      l <- kio_recv(ch, timeout = 30)
      kio_send(
        ch,
        list(is_view(s), is_view(l), length(s), s[[1L]], l$a[[1L]], length(l$b))
      )
    }),
    arena_size = 0
  )
  s <- paste0("s", 1:50000)
  l <- list(a = runif(100000), b = paste0("x", 1:20000))
  kio_send(ch, s)
  kio_send(ch, l)
  expect_identical(
    kio_recv(ch, 30),
    list(TRUE, TRUE, 50000L, "s1", l$a[[1L]], 20000L)
  )
  kio_close(ch, 10)
})

test_that("a peer killed holding a view is force-reclaimed after the verdict", {
  skip_if_no_child_kioto()
  ch <- kio_channel(
    quote({
      x <- kio_recv(ch, timeout = 30) # a view the peer holds onto
      kio_send(ch, length(x)) # ack: the view is in hand
      Sys.sleep(30) # hold the view; never release
    }),
    arena_size = 0
  )
  x <- runif(100000)
  kio_send(ch, x)
  expect_identical(kio_recv(ch, 30), 100000L) # the peer holds its view now
  kio_send(ch, x) # reaps the first keeper -> ledger
  expect_identical(chan_ledger(ch), 1L)

  pid <- .Call(kioto:::kio_channel_stat, ch)$peer_pid
  kill_hard(pid)
  # the recv probes the death verdict; the force-reclaim rides it
  r <- kio_recv(ch, 30)
  expect_s3_class(r, "kio_peer_gone")
  expect_identical(chan_ledger(ch), 0L)
  expect_gte(chan_fl(ch), 1L) # the lent region rejoined the list
  kio_close(ch, 10)
})
