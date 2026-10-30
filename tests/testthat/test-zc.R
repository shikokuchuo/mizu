# Zero-copy payload tiers (SHM_VEC / REF): a mori-layout object past the
# inline budget and the zc floor crosses as an ALTREP view over the spill
# region's own pages — no receive-side copy — with the refcount + ledger
# release protocol behind it (zc.c). The in-process harnesses make the
# refcount transitions deterministic; the death backstop is cross-process.

is_view <- function(x) .Call(mizu:::mizu_zc_view_check, x)
chan_ledger <- function(ch) {
  .Call(mizu:::mizu_channel_stat, ch)[["ledger_entries"]]
}
chan_fl <- function(ch) .Call(mizu:::mizu_channel_stat, ch)[["fl_entries"]]

test_that("tier selection: big atomic vectors cross as views, others copy", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000) # 800 KB: past budget and floor
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_identical(as.numeric(y), x)

  mizu_send(p[["host"]], 1:10) # small: RAWVEC copy
  expect_false(is_view(mizu_recv(p[["peer"]], 5)))

  mizu_send(p[["host"]], 1:2^27) # ALTREP input: stays a compact stream
  w <- mizu_recv(p[["peer"]], 30)
  expect_false(is_view(w))
  expect_equal(length(w), 2^27)

  xa <- runif(20000) # attributes ride the attrs blob
  names(xa) <- paste0("n", seq_along(xa))
  mizu_send(p[["host"]], xa)
  ya <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(ya))
  expect_identical(ya, xa)
  channel_end(p)
})

test_that("S4 objects cross as views with the bit intact", {
  methods::setClass("reiS4Int", contains = "integer")
  methods::setClass("reiS4Chr", contains = "character")
  methods::setClass("reiS4Cplx", contains = "complex")
  methods::setClass("reiS4Raw", contains = "raw")
  methods::setClass("reiS4List", contains = "list")
  p <- channel_pair(arena_size = 0)

  x <- methods::new("reiS4Int", as.integer(runif(100000) * 100)) # 400 KB
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_true(isS4(y))
  expect_identical(y, x)
  # held views pin their spill regions: release each before the next send
  # or the churn signal (Linux only) drops staging to the copy tiers
  rm(y)
  invisible(gc())

  xs <- methods::new("reiS4Chr", paste0("s", 1:20000))
  mizu_send(p[["host"]], xs)
  ys <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(ys))
  expect_true(isS4(ys))
  expect_identical(ys, xs)
  rm(ys)
  invisible(gc())

  xcp <- methods::new(
    "reiS4Cplx",
    complex(real = runif(30000), imaginary = runif(30000))
  )
  mizu_send(p[["host"]], xcp)
  ycp <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(ycp))
  expect_true(isS4(ycp))
  expect_identical(ycp, xcp)
  rm(ycp)
  invisible(gc())

  xrw <- methods::new("reiS4Raw", as.raw(runif(300000) * 255))
  mizu_send(p[["host"]], xrw)
  yrw <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(yrw))
  expect_true(isS4(yrw))
  expect_identical(yrw, xrw)
  rm(yrw)
  invisible(gc())

  xl <- methods::new("reiS4List", list(a = runif(30000), b = runif(20000)))
  mizu_send(p[["host"]], xl)
  yl <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(yl))
  expect_true(isS4(yl))
  expect_identical(yl, xl)

  xe <- list(
    s4 = methods::new("reiS4Int", as.integer(runif(30000) * 100)),
    plain = runif(30000)
  )
  mizu_send(p[["host"]], xe)
  ye <- mizu_recv(p[["peer"]], 5)
  expect_true(isS4(ye[["s4"]]))
  expect_identical(ye, xe)

  # a received S4 view re-sends as REF with the bit intact
  mizu_send(p[["peer"]], yl)
  z <- mizu_recv(p[["host"]], 5)
  expect_true(is_view(z))
  expect_true(isS4(z))

  # an S4 wrapper over a lazy ALTREP data part stays on the copy tiers
  xc <- methods::new("reiS4Int", 1:100000)
  mizu_send(p[["host"]], xc)
  yc <- mizu_recv(p[["peer"]], 5)
  expect_false(is_view(yc))
  expect_true(isS4(yc))
  expect_identical(yc, xc)
  channel_end(p)
})

test_that("pool results carry the S4 bit on the view tier", {
  methods::setClass("reiS4Pool", contains = "numeric")
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], methods::new("reiS4Pool", runif(100000)))
  pool_step(p)
  r <- mizu_collect(t, 5)
  expect_true(is_view(r))
  expect_true(isS4(r))
  expect_identical(length(r), 100000L)
  pool_end(p)
})

test_that("the channel's raw floor: mid-size vectors copy, big ones view", {
  p <- channel_pair(arena_size = 2 * 1024 * 1024)
  x <- runif(20000) # 160 KB: past the zc floor, under the raw floor
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_false(is_view(y)) # the arena's bare-bytes copy wins here
  expect_identical(y, x)

  big <- runif(100000) # 800 KB: past the raw floor — the view wins
  mizu_send(p[["host"]], big)
  z <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(z))
  expect_identical(as.numeric(z), big)
  channel_end(p)
})

test_that("rc_of on a non-view returns integer(0) (no chain walk)", {
  expect_identical(rc_of(runif(10)), integer(0))
  expect_identical(rc_of(1:10), integer(0)) # ALTREP, but not a mizu view
  expect_identical(rc_of(NULL), integer(0))
})

test_that("a held view pins its region in the ledger until release", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  # the host's next send reaps the first keeper: count > 0 (y held), so the
  # region waits in the ledger instead of rejoining the free list
  mizu_send(p[["host"]], x)
  expect_identical(chan_ledger(p[["host"]]), 1L)
  y2 <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y2))

  # release the views; the regions rejoin the free list (swept at the
  # reap) and the next sends pop them — fl_hits moves
  rm(y, y2)
  invisible(gc())
  hits <- .Call(mizu:::mizu_channel_stat, p[["host"]])[["fl_hits"]]
  mizu_send(p[["host"]], x)
  mizu_send(p[["host"]], x)
  expect_identical(chan_ledger(p[["host"]]), 0L)
  expect_gt(.Call(mizu:::mizu_channel_stat, p[["host"]])[["fl_hits"]], hits)
  expect_identical(as.numeric(mizu_recv(p[["peer"]], 5)), x)
  expect_identical(as.numeric(mizu_recv(p[["peer"]], 5)), x)
  channel_end(p)
})

test_that("views are copy-on-write: mutation never disturbs the region", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  y[1] <- -1 # COW: materializes a private copy
  expect_identical(y[1], -1)
  expect_identical(as.numeric(y)[-1], x[-1])
  # the region's next payload is undisturbed
  mizu_send(p[["host"]], x)
  expect_identical(as.numeric(mizu_recv(p[["peer"]], 5)), x)
  channel_end(p)
})

test_that("COW materialization releases the region early (no GC needed)", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["host"]], x) # reap: host loan drops; y held
  expect_identical(chan_ledger(p[["host"]]), 1L)
  y[1] <- -1 # COW: the release hook fires here
  mizu_send(p[["host"]], x) # reap: count 0 -> free list
  expect_identical(chan_ledger(p[["host"]]), 0L)
  channel_end(p)
})

test_that("integer, logical, raw and complex views are copy-on-write too", {
  p <- channel_pair(arena_size = 0)
  cases <- list(
    list(x = seq_len(100000) + 0L, mut = -1L),
    list(x = runif(100000) > 0.5, mut = FALSE),
    list(x = as.raw((seq_len(300000) - 1L) %% 256L), mut = as.raw(255L)),
    list(x = complex(real = runif(50000), imaginary = runif(50000)), mut = 0i)
  )
  for (case in cases) {
    x <- case[["x"]]
    mizu_send(p[["host"]], x)
    y <- mizu_recv(p[["peer"]], 5)
    expect_true(is_view(y))
    y[1] <- case[["mut"]] # COW: materializes a private copy
    expect_identical(y[1], case[["mut"]])
    expect_identical(y[2], x[2])
    mizu_send(p[["host"]], x) # the region's next payload is undisturbed
    expect_identical(mizu_recv(p[["peer"]], 5), x)
    rm(y)
    invisible(gc())
  }
  channel_end(p)
})

test_that("view summaries read off the region without materializing", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  rc <- rc_of(y)
  expect_equal(sum(y), sum(x))
  expect_equal(min(y), min(x))
  expect_equal(max(y), max(x))
  expect_identical(rc_of(y), rc) # region reads keep the loan

  xi <- as.integer(runif(100000) * 100)
  mizu_send(p[["host"]], xi)
  yi <- mizu_recv(p[["peer"]], 5)
  expect_equal(sum(yi), sum(xi))
  expect_equal(min(yi), min(xi))

  xl <- runif(100000) > 0.5
  mizu_send(p[["host"]], xl)
  yl <- mizu_recv(p[["peer"]], 5)
  expect_equal(sum(yl), sum(xl))
  expect_equal(max(yl), max(xl))

  xc <- complex(real = runif(100000), imaginary = runif(100000))
  mizu_send(p[["host"]], xc)
  yc <- mizu_recv(p[["peer"]], 5)
  expect_equal(sum(yc), sum(xc))
  channel_end(p)
})

test_that("element and range reads keep the view's loan (R-07)", {
  p <- channel_pair(arena_size = 0)
  set.seed(1)
  fresh <- function(x) {
    mizu_send(p[["host"]], x)
    v <- mizu_recv(p[["peer"]], 5)
    expect_true(is_view(v))
    v
  }
  # the Elt / Get_region methods: without them R's defaults issue a writable
  # DATAPTR request, COW-materializing the whole view on the first x[1]
  common <- list(
    function(v) v[1L],
    function(v) v[[1L]],
    function(v) v[1:10],
    function(v) v[c(TRUE, FALSE)],
    function(v) head(v),
    function(v) rev(v)[1L],
    function(v) length(v),
    function(v) as.list(v[1:3]),
    function(v) lapply(v[1:3], identity),
    function(v) vapply(v[1:2], identity, v[[1L]]),
    function(v) length(unique(v)),
    function(v) anyNA(v),
    function(v) {
      for (e in v) {
        break
      }
      e
    },
    function(v) capture.output(print(head(v, 2))),
    function(v) identical(v, v)
  )
  numeric_only <- list(
    function(v) sum(v),
    function(v) v + 1
  )
  # match() (and so %in%) requests a writable DATAPTR on R < 4.6,
  # COW-materializing the view — an R-version limitation, not a lost loan
  if (getRversion() >= "4.6.0") {
    numeric_only <- c(
      numeric_only,
      list(function(v) any(v %in% v[1L]), function(v) match(v[1L], v))
    )
  }
  for (x in list(runif(2e5), sample.int(1e6, 2e5), sprintf("s%d", 1:2e5))) {
    ops <- if (is.character(x)) common else c(common, numeric_only)
    for (op in ops) {
      v <- fresh(x)
      invisible(op(v))
      expect_identical(rc_of(v)[1L], 2L)
      rm(v)
      invisible(gc())
    }
  }
  # and the values read straight off the shared pages
  x <- runif(2e5)
  v <- fresh(x)
  expect_identical(v[1L], x[1L])
  expect_identical(v[[1L]], x[[1L]])
  expect_identical(v[1:10], x[1:10])
  expect_identical(head(v), head(x))
  channel_end(p)
})

test_that("a REF-sent view is not retained across later stages (R-09)", {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], runif(2e5))
  v <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["host"]], v) # REF: the claim records the view on the mark list
  v2 <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(v2))
  expect_identical(rc_of(v)[1L], 2L) # v + v2
  mizu_send(p[["host"]], 42L) # a raw-tier stage: the mark list resets at entry
  expect_identical(mizu_recv(p[["peer"]], 5), 42L)
  rm(v2)
  invisible(gc())
  expect_identical(rc_of(v)[1L], 1L) # v alone — the stale mark held v2 pre-fix
  channel_end(p)
})

test_that("an unknown layout flags word rejects as corrupt, the ring flows on (R-16, R-21)", {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], runif(2e5))
  v <- mizu_recv(p[["peer"]], 5)
  nm <- .Call(mizu:::mizu_zc_view_name, v)
  mizu_send(p[["host"]], v) # REF: the wrap re-validates the layout header
  rw <- .Call(mizu:::mizu_region_open, nm, TRUE)
  # the [32-35] format flags word: a bit past the S4 flag is corrupt-or-newer
  .Call(mizu:::mizu_poke, rw, 32L, as.raw(c(2L, 0L, 0L, 0L)))
  expect_error(mizu_recv(p[["peer"]], 5), "corrupt payload slot")
  # the raise out of the read hook was contained and consumed: no wedge
  mizu_send(p[["host"]], "tail")
  expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  channel_end(p)
})

test_that("a REF re-wrap validates the region's layout header", {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], runif(2e5))
  v <- mizu_recv(p[["peer"]], 5)
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_zc_view_name, v),
    TRUE
  )
  reflow <- function() {
    mizu_send(p[["host"]], v) # REF: the wrap re-validates the layout header
    expect_error(mizu_recv(p[["peer"]], 5), "corrupt payload slot")
    mizu_send(p[["host"]], "tail") # the raise was contained: no wedge
    expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  }
  .Call(mizu:::mizu_poke, rw, 8, i64raw(2L^30)) # the length outruns the region
  reflow()
  .Call(mizu:::mizu_poke, rw, 8, i64raw(200000L))
  .Call(mizu:::mizu_poke, rw, 16, i64raw(2L^30)) # attrs outrun the region
  reflow()
  .Call(mizu:::mizu_poke, rw, 16, i64raw(0L))
  .Call(mizu:::mizu_poke, rw, 48, i64raw(5L)) # an undefined header NA claim
  reflow()
  .Call(mizu:::mizu_poke, rw, 48, i64raw(0L))
  .Call(mizu:::mizu_poke, rw, 40, i64raw(8L)) # an unaligned validity offset
  reflow()
  .Call(mizu:::mizu_poke, rw, 40, i64raw(0L))
  .Call(mizu:::mizu_poke, rw, 0, as.raw(0L)) # unknown magic
  reflow()
  channel_end(p)
})

test_that("a REF re-wrap validates the string layout header", {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], sprintf("value-%06d", seq_len(5000L)))
  v <- mizu_recv(p[["peer"]], 5)
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_zc_view_name, v),
    TRUE
  )
  mizu_send(p[["host"]], v)
  .Call(mizu:::mizu_poke, rw, 16, i64raw(2L^30)) # the block outruns the region
  expect_error(mizu_recv(p[["peer"]], 5), "corrupt payload slot")
  mizu_send(p[["host"]], "tail")
  expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  channel_end(p)
})

test_that("a REF slot validates its identifier at read", {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], runif(2e5))
  v <- mizu_recv(p[["peer"]], 5)
  nm <- .Call(mizu:::mizu_channel_stat, p[["host"]])[["name"]]
  rw <- .Call(mizu:::mizu_region_open, nm, TRUE)
  mizu_send(p[["host"]], v) # REF into slot 1 of the host's ring
  .Call(
    mizu:::mizu_poke,
    rw,
    512L + 256L + 16L,
    charToRaw("x") # the literal prefix no longer matches
  )
  expect_error(mizu_recv(p[["peer"]], 5), "corrupt payload slot")
  mizu_send(p[["host"]], "tail")
  expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  channel_end(p)

  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], runif(2e5))
  v <- mizu_recv(p[["peer"]], 5)
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_channel_stat, p[["host"]])[["name"]],
    TRUE
  )
  mizu_send(p[["host"]], v) # REF into slot 1
  .Call(
    mizu:::mizu_poke,
    rw,
    512L + 256L + 4L,
    writeBin(0L, raw(), size = 4L, endian = "little") # a zero identifier length
  )
  expect_error(mizu_recv(p[["peer"]], 5), "corrupt payload slot")
  mizu_send(p[["host"]], "tail")
  expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  channel_end(p)
})

test_that("a SHM_VEC slot cross-checks its header against the aux word", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v1 <- mizu_recv(p[["peer"]], 5)
  nm <- .Call(mizu:::mizu_zc_view_name, v1)
  rm(v1)
  invisible(gc())
  mizu_send(p[["host"]], x * 2) # reuses the reclaimed region
  rw <- .Call(mizu:::mizu_region_open, nm, TRUE)
  # the staged extent no longer matches the slot's aux word
  .Call(mizu:::mizu_poke, rw, 8, i64raw(99999L))
  expect_error(mizu_recv(p[["peer"]], 5), "corrupt payload slot")
  mizu_send(p[["host"]], "tail")
  expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  channel_end(p)
})

test_that("SHM_VEC string and list slots cross-check the aux word too", {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], sprintf("value-%06d", seq_len(5000L))) # MIZS
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_channel_stat, p[["host"]])[["name"]],
    TRUE
  )
  # the exact used bytes no longer match the string block's header
  .Call(mizu:::mizu_poke, rw, 512L + 8L + 4L, as.raw(255L))
  expect_error(mizu_recv(p[["peer"]], 5), "corrupt payload slot")
  mizu_send(p[["host"]], "tail")
  expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  channel_end(p)

  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], list(runif(50000))) # MIZL
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_channel_stat, p[["host"]])[["name"]],
    TRUE
  )
  # the staged type is a list; the aux byte now says a vector
  .Call(mizu:::mizu_poke, rw, 512L + 8L, as.raw(14L))
  expect_error(mizu_recv(p[["peer"]], 5), "corrupt payload slot")
  mizu_send(p[["host"]], "tail")
  expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  channel_end(p)
})

test_that("a SHM_VEC slot validates its region-name length", {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], runif(2e5)) # SHM_VEC into slot 0
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_channel_stat, p[["host"]])[["name"]],
    TRUE
  )
  .Call(
    mizu:::mizu_poke,
    rw,
    512L + 4L,
    writeBin(0L, raw(), size = 4L, endian = "little")
  )
  expect_error(mizu_recv(p[["peer"]], 5), "corrupt payload slot")
  mizu_send(p[["host"]], "tail")
  expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  channel_end(p)
})

test_that("a REF naming a vanished region fails the read", {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], runif(100000))
  v <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["host"]], v) # REF into slot 1
  nm <- .Call(mizu:::mizu_channel_stat, p[["host"]])[["name"]]
  rw <- .Call(mizu:::mizu_region_open, nm, TRUE)
  id <- .Call(mizu:::mizu_peek, rw, 512L + 256L + 16L, 40L)
  hex0 <- 512L + 256L + 16L + which(id == charToRaw("_"))[1L]
  b <- .Call(mizu:::mizu_peek, rw, hex0, 1L)
  # a well-formed name that does not exist (one hex digit flipped)
  .Call(
    mizu:::mizu_poke,
    rw,
    hex0,
    if (b == charToRaw("a")) charToRaw("b") else charToRaw("a")
  )
  expect_error(mizu_recv(p[["peer"]], 5), "payload region vanished")
  channel_end(p)
})

test_that("a received view re-sends as REF — zero bytes move", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["peer"]], y) # REF back to the host
  z <- mizu_recv(p[["host"]], 5)
  expect_true(is_view(z))
  expect_identical(as.numeric(z), x)
  channel_end(p)
})

test_that("a data.frame that is itself a view declines the foreign frame gate", {
  df <- data.frame(x = runif(20000))
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p[["host"]], df)
  v <- mizu_recv(p[["peer"]], 5)
  f <- foreign_pair(caps = 22L) # ATTRS | MIZL | MIZL_REF
  on.exit(channel_end(f), add = TRUE)
  mizu_send(f[["host"]], v)
  expect_identical(mizu_recv(f[["peer"]], 5), df)
})

test_that("a REF slot with len past inline_max is rejected before reading", {
  p <- channel_pair(slot_size = 64L, arena_size = 0) # inline_max = 48
  x <- runif(10000)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["peer"]], y) # REF back to the host
  rname <- .Call(mizu:::mizu_channel_stat, p[["host"]])[["name"]]
  rw <- .Call(mizu:::mizu_region_open, rname, TRUE)
  ring <- 512L + 64L * 64L # the peer's tx ring (second ring)
  .Call(
    mizu:::mizu_poke,
    rw,
    ring + 4L,
    writeBin(60L, raw(4L), endian = "little")
  )
  expect_snapshot(mizu_recv(p[["host"]], 5), error = TRUE)
  channel_end(p)
})

test_that("pool results cross as views; a held result pins the worker's region", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], runif(100000))
  pool_step(p)
  r <- mizu_collect(t, 5)
  expect_true(is_view(r))
  # the worker's keeper sweep releases its loan; the held view keeps the
  # region in the ledger
  pool_step(p)
  expect_identical(.Call(mizu:::mizu_pool_zc_info, p[["wk"]])[[2L]], 1L)
  rm(r)
  invisible(gc())
  t2 <- mizu_submit(p[["ctrl"]], runif(100000))
  pool_step(p)
  r2 <- mizu_collect(t2, 5)
  expect_identical(.Call(mizu:::mizu_pool_zc_info, p[["wk"]])[[2L]], 0L)
  expect_true(is_view(r2))
  pool_end(p)
})

test_that("zc churn falls back past the lent budget and clears once lent regions reclaim", {
  skip_on_os(c("mac", "windows")) # the churn signal is Linux-only (spill_fl_pop)
  p <- pool_pair()
  x <- runif(1e6) # 8 MB: 34 held results cross the 256 MB lent budget
  # x + 0 stages a fresh worker-side region per result (an identity result
  # REFs the argument's region and never exercises the worker's own
  # ledger); the held collects pin them: past the budget the worker
  # degrades to SHM_RAW's deterministic reuse
  held <- vector("list", 40L)
  for (i in seq_along(held)) {
    t <- mizu_submit(p[["ctrl"]], x + 0, x = x)
    pool_step(p)
    held[[i]] <- mizu_collect(t, 5)
  }
  expect_true(is_view(held[[1L]]))
  expect_false(is_view(held[[40L]]))
  expect_identical(held[[40L]], x)
  rm(held)
  invisible(gc())
  for (i in seq_len(3L)) {
    # the sweeps reclaim; staging returns to SHM_VEC
    t <- mizu_submit(p[["ctrl"]], x + 0, x = x)
    pool_step(p)
    r <- mizu_collect(t, 5)
  }
  expect_true(is_view(r))
  pool_end(p)
})

test_that("the exec-entry boundary fires past the byte gate", {
  p <- pool_pair()
  armed <- function() .Call(mizu:::mizu_zc_armed_bytes)
  x <- runif(2^20) # 8 MB payload: one wrap arms past the 8 MB gate
  t <- mizu_submit(p[["ctrl"]], x, x = x)
  pool_step(p) # the arg wrap arms the gate (a residual counter fires first)
  r <- mizu_collect(t, 5) # the collect boundary fires; the REF wrap re-arms
  expect_true(is_view(r))
  expect_gt(armed(), 8 * 1024 * 1024)
  # the next exec entry fires the boundary; a value task arms nothing
  t2 <- mizu_submit(p[["ctrl"]], 1L)
  pool_step(p)
  expect_identical(armed(), 0)
  expect_identical(mizu_collect(t2, 5), 1L)
  pool_end(p)
})

test_that("the collect-entry boundary fires past the gate; dead views never leak", {
  p <- channel_pair(arena_size = 0)
  pp <- pool_pair()
  armed <- function() .Call(mizu:::mizu_zc_armed_bytes)
  x <- runif(2^20)
  t <- mizu_submit(pp[["ctrl"]], 1L) # a value task arms nothing
  pool_step(pp) # clears any residual counter through the exec boundary
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_gt(armed(), 8 * 1024 * 1024)
  # the collect veneer is the only boundary past the wrap: it fires, resets
  expect_identical(mizu_collect(t, 5), 1L)
  expect_identical(armed(), 0)
  # the held view pins its region; a full gc of the dead view drains it
  mizu_send(p[["host"]], x)
  expect_identical(chan_ledger(p[["host"]]), 1L)
  rm(y)
  invisible(gc())
  mizu_send(p[["host"]], x)
  expect_identical(chan_ledger(p[["host"]]), 0L)
  channel_end(p)
  pool_end(pp)
})

test_that("a zero mizu.gc_boundary disables the boundary collection", {
  p <- pool_pair()
  armed <- function() .Call(mizu:::mizu_zc_armed_bytes)
  set <- function(x) invisible(.Call(mizu:::mizu_zc_gc_boundary_set, x))
  on.exit(set(8 * 1024 * 1024))
  x <- runif(2^20) # 8 MB: one wrap arms past the default gate
  set(0)
  t <- mizu_submit(p[["ctrl"]], x, x = x)
  pool_step(p)
  r <- mizu_collect(t, 5)
  expect_true(is_view(r))
  t2 <- mizu_submit(p[["ctrl"]], 1L)
  pool_step(p) # the exec entry would fire an enabled boundary
  expect_gt(armed(), 8 * 1024 * 1024)
  expect_identical(mizu_collect(t2, 5), 1L)
  set(1) # a 1-byte gate fires at the next boundary
  t3 <- mizu_submit(p[["ctrl"]], 1L)
  pool_step(p)
  expect_identical(armed(), 0)
  expect_identical(mizu_collect(t3, 5), 1L)
  pool_end(p)
})

test_that("the mizu.gc_boundary setter rejects out-of-range values", {
  set <- function(x) invisible(.Call(mizu:::mizu_zc_gc_boundary_set, x))
  on.exit(set(8 * 1024 * 1024))
  expect_error(set(-1), "not exceeding 2^53", fixed = TRUE)
  expect_error(set(2^54), "not exceeding 2^53", fixed = TRUE)
  expect_error(set(NA_real_), "not exceeding 2^53", fixed = TRUE)
  expect_error(set(2^53), NA)
})

test_that("the mizu.gc_boundary option is validated at package load", {
  skip_if_no_child_mizu()
  f <- tfile()
  mizu:::mizu_spawn(sprintf(
    "options(mizu.gc_boundary = 0); library(mizu); file.create(%s)",
    deparse(f)
  ))
  expect_true(wait_for_file(f))
  unlink(f)
  g <- tfile()
  mizu:::mizu_spawn(sprintf(
    "options(mizu.gc_boundary = -1); tryCatch(library(mizu), error = function(e) file.create(%s))",
    deparse(g)
  ))
  expect_true(wait_for_file(g))
  unlink(g)
  h <- tfile()
  mizu:::mizu_spawn(sprintf(
    "options(mizu.gc_boundary = 2^54); tryCatch(library(mizu), error = function(e) file.create(%s))",
    deparse(h)
  ))
  expect_true(wait_for_file(h))
  unlink(h)
})

test_that(".onLoad validates and applies the mizu.gc_boundary option", {
  set <- function(x) invisible(.Call(mizu:::mizu_zc_gc_boundary_set, x))
  on.exit(set(8 * 1024 * 1024))
  opts <- options(mizu.gc_boundary = 2^20)
  on.exit(options(opts), add = TRUE)
  expect_no_error(mizu:::.onLoad())
  options(mizu.gc_boundary = -1)
  expect_snapshot(error = TRUE, mizu:::.onLoad())
})

test_that("channel zc churn falls back to the copy tiers past the lent budget and recovers", {
  skip_on_os(c("mac", "windows")) # the churn signal is Linux-only (spill_fl_pop)
  p <- channel_pair(arena_size = 0)
  x <- runif(1e6) # 8 MB: 34 held views cross the 256 MB lent budget
  held <- vector("list", 40L)
  for (i in seq_along(held)) {
    mizu_send(p[["host"]], x)
    held[[i]] <- mizu_recv(p[["peer"]], 5)
  }
  expect_true(is_view(held[[1L]]))
  expect_false(is_view(held[[40L]])) # past the budget: a materialized copy
  expect_identical(held[[40L]], x)
  rm(held)
  invisible(gc())
  mizu_send(p[["host"]], x) # the reap's sweep reclaims; staging returns to SHM_VEC
  invisible(mizu_recv(p[["peer"]], 5))
  mizu_send(p[["host"]], x)
  expect_true(is_view(mizu_recv(p[["peer"]], 5)))
  channel_end(p)
})

test_that("REF round-trips at any viewed size", {
  p <- channel_pair(arena_size = 0)
  for (n in c(5000, 8 * 1000 * 1000)) {
    # 40 KB and 64 MiB views
    x <- runif(n)
    mizu_send(p[["host"]], x)
    y <- mizu_recv(p[["peer"]], 30)
    expect_true(is_view(y))
    mizu_send(p[["peer"]], y) # REF back
    z <- mizu_recv(p[["host"]], 30)
    expect_true(is_view(z))
    expect_identical(as.numeric(z), x)
  }
  channel_end(p)
})

test_that("a uint64 frame column lands as the converting view and REFs", {
  skip_if_not_installed("arrow")
  uv <- bit64::as.integer64(rep(1, 6000))
  uv[2] <- bit64::as.integer64("9223372036854775807")
  tab <- arrow::arrow_table(u = arrow::Array$create(uv, type = arrow::uint64()))
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p[["host"]], tab)
  df <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(df[["u"]]))
  expect_identical(rc_of(df[["u"]]), c(2L, 0L))
  mizu_send(p[["peer"]], df[["u"]]) # REF back to the host
  z <- mizu_recv(p[["host"]], 5)
  expect_true(is_view(z))
  expect_identical(rc_of(z), c(2L, 1L)) # resolved counted; escape marked REFHELD
  # the u64 class: exact at the Arrow export (uint64, bit-identical)
  s <- mizu_as_arrow(data.frame(u = z))
  expect_identical(s$get_schema()$children$u$format, "L")
  # the value landing converts: double + NA past 2^53, warned at the touch
  ws <- grab_warnings(z + 0)
  expect_match(ws, "beyond ...2\\^53 convert to NA", all = FALSE)
  expect_identical(as.numeric(z)[1:3], c(1, NA_real_, 1))
})

test_that("string vectors cross as views (MORS), small ones copy", {
  p <- channel_pair(arena_size = 0)
  x <- rep(c("café", "naïve", NA_character_, "", "plain"), 10000) # ~1 MB layout
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_identical(y, x)

  mizu_send(p[["host"]], c("a", "b")) # small: inline copy
  expect_false(is_view(mizu_recv(p[["peer"]], 5)))

  xa <- paste0("v", 1:50000) # attributes ride the attrs blob
  names(xa) <- paste0("n", 1:50000)
  mizu_send(p[["host"]], xa)
  ya <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(ya))
  expect_identical(ya, xa)
  channel_end(p)
})

test_that("the MIZS string block is Arrow large_utf8-shaped", {
  p <- channel_pair(arena_size = 0)
  lat <- iconv("caf\u00e9", "UTF-8", "latin1")
  byt <- "\xff\xfe"
  Encoding(byt) <- "bytes"
  x <- c(rep("abc", 20000), NA_character_, "", lat, byt, "z") # past the floor
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_identical(y, x)

  n <- length(x)
  peek <- view_peek(y)
  hdr <- peek(0, 64)
  expect_identical(readBin(hdr[1:4], "integer", endian = "little"), 0x4D495A53L)
  expect_identical(i64v(hdr[9:16]), as.numeric(n))

  # sections, each 64-byte aligned from the block start (byte 64)
  off_offsets <- 64 + align64(ceiling(n / 8))
  off_enc <- off_offsets + align64(8 * (n + 1))
  off_data <- off_enc + align64(n)

  bits <- as.integer(rawToBits(peek(64, ceiling(n / 8))))[seq_len(n)]
  expect_identical(bits, as.integer(!is.na(x)))

  offsets <- i64v(peek(off_offsets, 8 * (n + 1)))
  expect_identical(offsets[[1L]], 0)
  nbytes <- ifelse(is.na(x), 0L, nchar(x, "bytes"))
  expect_identical(diff(offsets), as.numeric(nbytes))
  expect_identical(i64v(hdr[17:24]), off_data - 64 + offsets[[n + 1L]])

  ce <- c(unknown = 0L, `UTF-8` = 1L, latin1 = 2L, bytes = 3L)[Encoding(x)]
  ce[is.na(x)] <- 0L
  expect_identical(as.integer(peek(off_enc, n)), unname(ce))

  expect_identical(
    peek(off_data, offsets[[n + 1L]]),
    unlist(lapply(x[!is.na(x)], charToRaw))
  )
  channel_end(p)
})

test_that("a string leaf in a list view carries the same string block", {
  p <- channel_pair(arena_size = 0)
  s <- c(paste0("s", 1:20000), NA_character_)
  x <- list(a = runif(1000), s = s, e = character(0))
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_identical(y, x)

  n <- length(s)
  peek <- view_peek(y)
  entry <- peek(64 + 32, 32) # directory entry of element 2
  data_offset <- i64v(entry[1:8])
  expect_identical(readBin(entry[17:20], "integer", endian = "little"), 16L)
  expect_identical(i64v(entry[25:32]), as.numeric(n))

  bits <- as.integer(rawToBits(peek(data_offset, ceiling(n / 8))))[seq_len(n)]
  expect_identical(bits, as.integer(!is.na(s)))
  off_offsets <- data_offset + align64(ceiling(n / 8))
  offsets <- i64v(peek(off_offsets, 8 * (n + 1)))
  expect_identical(offsets[[1L]], 0)
  expect_identical(offsets[[n + 1L]], as.numeric(sum(nchar(s[-n], "bytes"))))

  empty <- peek(64 + 64, 32) # character(0): a 64-byte block, one zero offset
  expect_identical(i64v(empty[9:16]), 64)
  expect_identical(i64v(empty[25:32]), 0)
  channel_end(p)
})

test_that("a held string view pins its region until GC", {
  p <- channel_pair(arena_size = 0)
  x <- paste0("s", 1:50000)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["host"]], x) # reap: the loan drops; y pins the region
  expect_identical(chan_ledger(p[["host"]]), 1L)
  rm(y)
  invisible(gc())
  mizu_send(p[["host"]], x) # reap: count 0 -> free list
  expect_identical(chan_ledger(p[["host"]]), 0L)
  channel_end(p)
})

test_that("string views duplicate on mutation: the region is undisturbed", {
  p <- channel_pair(arena_size = 0)
  x <- paste0("s", 1:50000)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  y2 <- y # NAMED bump: subassignment duplicates (ALTSTRING has no Set_elt)
  y2[1] <- "mutated"
  expect_identical(y2[1], "mutated")
  expect_identical(y[1], x[1])
  mizu_send(p[["host"]], x)
  expect_identical(mizu_recv(p[["peer"]], 5), x)
  channel_end(p)
})

test_that("in-place element assignment on a string view errors", {
  p <- channel_pair(arena_size = 0)
  x <- paste0("s", 1:50000)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_error(y[1] <- "mutated", "Set_elt") # ALTSTRING provides no Set_elt
  expect_identical(y[1], x[1]) # the view is untouched
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
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_true(is_view(y[["nums"]]))
  expect_true(is_view(y[["strs"]]))
  expect_true(is_view(y[["sub"]]))
  expect_true(is_view(y[["sub"]][["a"]]))
  expect_identical(as.numeric(y[["nums"]]), x[["nums"]])
  expect_identical(y[["strs"]], x[["strs"]])
  expect_identical(as.numeric(y[["sub"]][["a"]]), x[["sub"]][["a"]])
  expect_null(y[["sub"]][["b"]])
  expect_identical(y[["f"]], sum) # a serialized leaf arrives as a plain object
  expect_null(y[["nil"]])
  expect_identical(names(y), names(x))
  channel_end(p)
})

test_that("the capped tree probe keeps decisions and values exact", {
  p <- channel_pair(arena_size = 0)
  leaf <- function(tag) sprintf("%s-%04d", tag, seq_len(2000))
  under <- list(a = leaf("a")) # one ~30 KB leaf: just under the 32 KB floor
  mizu_send(p[["host"]], under)
  expect_false(is_view(mizu_recv(p[["peer"]], 5)))
  # far past the floor: the first leaves clear the gate, the later
  # leaves' probes short-circuit — the values cross intact
  over <- lapply(c("a", "b", "c", "d", "e", "f"), leaf)
  mizu_send(p[["host"]], over)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_identical(y[], over)
  channel_end(p)
})

test_that("a remote leaf (tag 33) resolves to a view over its region", {
  x <- runif(50000)
  fx <- remote_leaf_pair(x)
  poke_remote(fx, len = length(x))

  before <- rc_of(fx[["ya"]])[1L]
  btree <- rc_of(fx[["yb"]])[1L]
  el <- fx[["yb"]][[1L]]
  expect_true(is_view(el))
  expect_identical(.Call(mizu:::mizu_zc_view_name, el), fx[["id"]])
  expect_identical(rc_of(fx[["ya"]])[1L], before + 1L) # one loan per extraction
  expect_identical(rc_of(fx[["yb"]])[1L], btree) # the tree loan unchanged
  expect_identical(el, x) # last: identical() materializes views on R < 4.6
  rm(el)
  fx[["yb"]] <- NULL # the list view's element cache holds the extraction
  invisible(gc())
  expect_identical(rc_of(fx[["ya"]])[1L], before)
  channel_end(fx[["p"]])
})

test_that("a remote leaf can reference a whole region (bare name)", {
  x <- runif(50000)
  fx <- remote_leaf_pair(x, wrap = FALSE)
  poke_remote(fx, len = length(x))

  el <- fx[["yb"]][[1L]]
  expect_true(is_view(el))
  expect_identical(.Call(mizu:::mizu_zc_view_name, el), fx[["id"]])
  expect_identical(el, x)
  channel_end(fx[["p"]])
})

test_that("remote-leaf declines are corrupt-or-newer shaped", {
  x <- runif(50000)

  fx <- remote_leaf_pair(x) # dangling: a well-formed name that does not exist
  id <- if (.Platform[["OS.type"]] == "windows") {
    "Local\\mizu_dead_beef"
  } else {
    "/mizu_dead_beef"
  }
  poke_remote(fx, id = id, len = length(x))
  expect_snapshot(
    fx[["yb"]][[1L]],
    error = TRUE,
    transform = function(l) gsub("Local\\mizu_", "/mizu_", l, fixed = TRUE)
  )
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # length claim mismatch
  poke_remote(fx, len = length(x) - 1L)
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # attrs_size claim mismatch (past the span)
  poke_remote(fx, len = length(x), attrs = 200L)
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # a {0,-1} claim the referenced leaf cannot back
  poke_remote(fx, len = length(x))
  .Call(
    mizu:::mizu_poke,
    fx[["rw"]],
    48,
    c(
      writeBin(-1L, raw(), size = 4L, endian = "little"),
      writeBin(-1L, raw(), size = 4L, endian = "little")
    )
  )
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # the S4 bit is never set on a remote leaf
  poke_remote(fx, len = length(x), sxp = bitwOr(33L, 0x40000000L))
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # the identifier span is 1-255 bytes
  poke_remote(fx, len = length(x), dsz = 0L)
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x)
  poke_remote(fx, len = length(x), dsz = 256L)
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])
})

test_that("remote-leaf header NA-claim rejections are corrupt-or-newer", {
  x <- runif(50000)

  fx <- remote_leaf_pair(x) # a header claim outside {0, 0} / {0, -1}
  poke_remote(fx, len = length(x))
  .Call(mizu:::mizu_poke, fx[["rw"]], 48, i64raw(5L))
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # a bitmap offset is region-local, never remote
  poke_remote(fx, len = length(x))
  .Call(mizu:::mizu_poke, fx[["rw"]], 40, i64raw(8L)) # unaligned
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # a table row claiming a bitmap of its own
  poke_remote(fx, len = length(x))
  .Call(mizu:::mizu_poke, fx[["rw"]], 40, i64raw(192L))
  .Call(mizu:::mizu_poke, fx[["rw"]], 192, i64raw(1L))
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])
})

test_that("remote-leaf identifier parse failures are corrupt-or-newer", {
  x <- runif(50000)
  aname <- function(fx) .Call(mizu:::mizu_zc_view_name, fx[["ya"]])

  fx <- remote_leaf_pair(x) # a path index past INT32_MAX
  poke_remote(fx, id = paste0(aname(fx), "[2147483648]"), len = length(x))
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # junk where a separator belongs
  poke_remote(fx, id = paste0(aname(fx), "[1;2]"), len = length(x))
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # trailing junk after the path
  poke_remote(fx, id = paste0(aname(fx), "[1]x"), len = length(x))
  expect_snapshot(fx[["yb"]][[1L]], error = TRUE)
  channel_end(fx[["p"]])
})

test_that("a remote leaf can reference a whole list region (bare name)", {
  x <- as.list(runif(10000))
  fx <- remote_leaf_pair(x, wrap = FALSE)
  poke_remote(fx, len = length(x))

  el <- fx[["yb"]][[1L]]
  expect_true(is_view(el))
  expect_identical(.Call(mizu:::mizu_zc_view_name, el), fx[["id"]])
  expect_identical(el, x)
  channel_end(fx[["p"]])
})

test_that("a remote leaf resolves a two-step path into a nested region", {
  v <- runif(50000)
  fx <- remote_leaf_pair(list(v)) # A = list(list(v)): element 0 nests
  id <- paste0(.Call(mizu:::mizu_zc_view_name, fx[["ya"]]), "[1,1]")
  poke_remote(fx, id = id, len = length(v))

  el <- fx[["yb"]][[1L]]
  expect_true(is_view(el))
  expect_identical(.Call(mizu:::mizu_zc_view_name, el), id)
  expect_identical(el, v)
  channel_end(fx[["p"]])
})

test_that("remote-leaf path walk rejections name the failure", {
  v <- runif(50000)

  fx <- remote_leaf_pair(list(v)) # a path hop out of bounds
  aname <- .Call(mizu:::mizu_zc_view_name, fx[["ya"]])
  poke_remote(fx, id = paste0(aname, "[5,1]"), len = length(v))
  expect_error(fx[["yb"]][[1L]], "path index out of bounds")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(v) # a path hop into an atomic leaf
  aname <- .Call(mizu:::mizu_zc_view_name, fx[["ya"]])
  poke_remote(fx, id = paste0(aname, "[1,1]"), len = length(v))
  expect_error(fx[["yb"]][[1L]], "path step is not a nested list")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(list(v)) # a hop whose span falls outside the region
  aname <- .Call(mizu:::mizu_zc_view_name, fx[["ya"]])
  rwa <- .Call(mizu:::mizu_region_open, aname, TRUE)
  .Call(mizu:::mizu_poke, rwa, 64, i64raw(2L^30))
  poke_remote(fx, id = paste0(aname, "[1,1]"), len = length(v))
  expect_error(fx[["yb"]][[1L]], "invalid nested region")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(list(v)) # the leaf step out of bounds
  aname <- .Call(mizu:::mizu_zc_view_name, fx[["ya"]])
  poke_remote(fx, id = paste0(aname, "[5]"), len = length(v))
  expect_error(fx[["yb"]][[1L]], "leaf index out of bounds")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(list(v)) # a hop entry's attrs fail the claims re-read
  aname <- .Call(mizu:::mizu_zc_view_name, fx[["ya"]])
  rwa <- .Call(mizu:::mizu_region_open, aname, TRUE)
  .Call(
    mizu:::mizu_poke,
    rwa,
    84,
    writeBin(4L, raw(), size = 4L, endian = "little")
  )
  poke_remote(fx, id = paste0(aname, "[1,1]"), len = length(v))
  expect_error(fx[["yb"]][[1L]], "corrupt or newer")
  channel_end(fx[["p"]])
})

test_that("a remote leaf's resolved region is re-validated on open", {
  x <- runif(50000)

  fx <- remote_leaf_pair(x, wrap = FALSE) # unknown magic
  rwa <- .Call(mizu:::mizu_region_open, fx[["id"]], TRUE)
  .Call(mizu:::mizu_poke, rwa, 0, as.raw(0L))
  poke_remote(fx, len = length(x))
  expect_error(fx[["yb"]][[1L]], "invalid or corrupted shared memory region")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x, wrap = FALSE) # the length outruns the mapping
  rwa <- .Call(mizu:::mizu_region_open, fx[["id"]], TRUE)
  .Call(mizu:::mizu_poke, rwa, 8, i64raw(2L^30))
  poke_remote(fx, len = length(x))
  expect_error(fx[["yb"]][[1L]], "invalid or corrupted shared memory region")
  channel_end(fx[["p"]])
})

test_that("a remote leaf's resolved string region is re-validated on open", {
  x <- sprintf("value-%06d", seq_len(5000L))

  fx <- remote_leaf_pair(x, wrap = FALSE) # the string block outruns the mapping
  rwa <- .Call(mizu:::mizu_region_open, fx[["id"]], TRUE)
  .Call(mizu:::mizu_poke, rwa, 16, i64raw(2L^30))
  poke_remote(fx, len = length(x))
  expect_error(fx[["yb"]][[1L]], "invalid or corrupted shared memory region")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x, wrap = FALSE) # more strings than the offsets fit
  rwa <- .Call(mizu:::mizu_region_open, fx[["id"]], TRUE)
  .Call(mizu:::mizu_poke, rwa, 8, i64raw(2L^20))
  poke_remote(fx, len = length(x))
  expect_error(fx[["yb"]][[1L]], "invalid string data")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x, wrap = FALSE) # the sections outrun the block
  rwa <- .Call(mizu:::mizu_region_open, fx[["id"]], TRUE)
  .Call(mizu:::mizu_poke, rwa, 16, i64raw(8L * length(x)))
  poke_remote(fx, len = length(x))
  expect_error(fx[["yb"]][[1L]], "invalid string data")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x, wrap = FALSE) # the first offset is always zero
  rwa <- .Call(mizu:::mizu_region_open, fx[["id"]], TRUE)
  offs <- 64L + align64((length(x) + 7L) %/% 8L)
  .Call(mizu:::mizu_poke, rwa, offs, i64raw(1L))
  poke_remote(fx, len = length(x))
  expect_error(fx[["yb"]][[1L]], "invalid string data")
  channel_end(fx[["p"]])
})

test_that("a corrupt string span rejects at element access", {
  p <- channel_pair(arena_size = 0)
  x <- sprintf("value-%06d", seq_len(5000L))
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(v))
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_zc_view_name, v),
    TRUE
  )
  offs <- 64L + align64((length(x) + 7L) %/% 8L)
  .Call(mizu:::mizu_poke, rw, offs, i64raw(100L)) # the span starts past its end
  expect_error(v[[1L]], "invalid string data")
  channel_end(p)
})

test_that("a nested list region is validated on lazy extraction", {
  x <- runif(50000)

  fx <- remote_leaf_pair(x) # a nested span smaller than a header
  poke_remote(fx, len = 0L, sxp = 19L, dsz = 32L)
  expect_error(fx[["yb"]][[1L]], "invalid nested list region")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # unknown magic at the nested base
  poke_remote(fx, len = 0L, sxp = 19L, dsz = 128L)
  .Call(mizu:::mizu_poke, fx[["rw"]], 128, as.raw(0L))
  expect_error(fx[["yb"]][[1L]], "invalid nested list region")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # an element count outrunning the span
  poke_remote(fx, len = 0L, sxp = 19L, dsz = 128L)
  .Call(
    mizu:::mizu_poke,
    fx[["rw"]],
    128,
    .Call(mizu:::mizu_peek, fx[["rw"]], 0, 4)
  )
  .Call(
    mizu:::mizu_poke,
    fx[["rw"]],
    132,
    writeBin(2L^30, raw(), size = 4L, endian = "little")
  )
  expect_error(fx[["yb"]][[1L]], "invalid nested list region")
  channel_end(fx[["p"]])
})

test_that("a corrupt directory entry rejects at element extraction", {
  x <- runif(50000)

  fx <- remote_leaf_pair(x) # the data span falls outside the region
  poke_remote(fx, len = 1L, sxp = 14L, dsz = 8L, doff = 2L^30)
  expect_error(fx[["yb"]][[1L]], "invalid element data")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # the data span is not 64-aligned
  poke_remote(fx, len = 1L, sxp = 14L, dsz = 8L, doff = 72L)
  expect_error(fx[["yb"]][[1L]], "invalid element data")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # a negative attrs size
  poke_remote(fx, len = 1L, sxp = 14L, dsz = 8L, attrs = -1L)
  expect_error(fx[["yb"]][[1L]], "invalid element data")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # attrs outrunning the data span
  poke_remote(fx, len = 1L, sxp = 14L, dsz = 8L, attrs = 100L)
  expect_error(fx[["yb"]][[1L]], "invalid element data")
  channel_end(fx[["p"]])

  fx <- remote_leaf_pair(x) # an element count outrunning the span
  poke_remote(fx, len = 2L, sxp = 14L, dsz = 8L)
  expect_error(fx[["yb"]][[1L]], "invalid element data")
  channel_end(fx[["p"]])
})

test_that("the wrap-time remote-leaf walk rejects corrupt entries", {
  # REF re-wrap re-runs the loan walk over the region as it is now
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["peer"]], list(runif(50000), v))
  w <- mizu_recv(p[["host"]], 5)
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_zc_view_name, w),
    TRUE
  )
  reflow <- function(msg) {
    mizu_send(p[["host"]], w) # REF: the wrap re-walks the tree
    expect_error(mizu_recv(p[["peer"]], 5), msg)
    mizu_send(p[["host"]], "tail") # the raise was contained: no wedge
    expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  }

  .Call(mizu:::mizu_poke, rw, 104, i64raw(300L)) # the identifier span caps at 255
  reflow("corrupt or newer")
  .Call(mizu:::mizu_poke, rw, 104, i64raw(40L))
  .Call(mizu:::mizu_poke, rw, 96, i64raw(2L^30)) # the span falls outside
  reflow("invalid element data")
  .Call(mizu:::mizu_poke, rw, 96, i64raw(72L)) # the span is not 64-aligned
  reflow("invalid element data")
  channel_end(p)
})

test_that("the wrap-time remote-leaf walk validates nested regions", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["peer"]], list(list(runif(50000)), v))
  w <- mizu_recv(p[["host"]], 5)
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_zc_view_name, w),
    TRUE
  )
  nested <- i64v(.Call(mizu:::mizu_peek, rw, 64, 8)) # entry 0's data offset
  reflow <- function(msg) {
    mizu_send(p[["host"]], w)
    expect_error(mizu_recv(p[["peer"]], 5), msg)
    mizu_send(p[["host"]], "tail")
    expect_identical(mizu_recv(p[["peer"]], 5), "tail")
  }

  .Call(mizu:::mizu_poke, rw, 72, i64raw(32L)) # smaller than a header
  reflow("invalid nested list region")
  .Call(mizu:::mizu_poke, rw, 72, i64raw(128L))
  .Call(mizu:::mizu_poke, rw, nested, as.raw(0L)) # unknown magic
  reflow("invalid nested list region")
  mg <- .Call(mizu:::mizu_peek, rw, 0, 4)
  .Call(mizu:::mizu_poke, rw, nested, mg) # an element count outrunning the span
  reflow("invalid nested list region")
  channel_end(p)
})

test_that("the wrap-time remote-leaf walk grows its stack past eight frames", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  tree <- c(list(v), replicate(9L, list(runif(40000)), simplify = FALSE))
  mizu_send(p[["peer"]], tree)
  w <- mizu_recv(p[["host"]], 5)
  expect_true(is_view(w))
  expect_identical(
    .Call(mizu:::mizu_zc_view_name, w[[1L]]),
    .Call(mizu:::mizu_zc_view_name, v)
  )
  expect_identical(w[[2L]][], tree[[2L]])
  rm(w)
  invisible(gc()) # the wrap's loans release with the view
  channel_end(p)
})


test_that("a map view nested in a big tree falls to the serialize tiers", {
  p <- pool_pair()
  st <- mizu:::map_stage(
    p[["ctrl"]],
    runif(100),
    identity,
    list(),
    template = numeric(1)
  )
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  mv <- mizu:::map_collect(st, collect = "view")
  expect_true(is_view(mv))
  ch <- channel_pair(arena_size = 0)
  # the map handle anchors the view: no shm terminus, so no identifier —
  # the remote-leaf gate rejects the tree (never a silent copy)
  mizu_send(ch[["host"]], list(mv, runif(50000)))
  w <- mizu_recv(ch[["peer"]], 5)
  expect_false(is_view(w))
  expect_identical(as.numeric(w[[1L]]), as.numeric(mv))
  channel_end(ch)
  pool_end(p)
})

test_that("a materialized or attributed view keeps the serialize path", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  v[1] <- -1 # COW: materialized, no longer remote-capable
  y_in <- list(a = runif(100000), b = v)
  mizu_send(p[["host"]], y_in)
  y <- mizu_recv(p[["peer"]], 5)
  expect_false(is_view(y)) # the serialize tiers, not MIZL
  expect_identical(y[["b"]][1], -1)
  expect_identical(y[["b"]][2], x[2])
  channel_end(p)
})

test_that("a view nested past the identifier depth serializes by value", {
  p <- channel_pair(arena_size = 0)
  on.exit(channel_end(p))
  deep <- c("leaf", "strings")
  for (i in 1:64) {
    deep <- list(deep)
  }
  mizu_send(p[["host"]], list(runif(100000), deep))
  v <- mizu_recv(p[["peer"]], 5)
  leaf <- v[[2L]]
  for (i in 1:64) {
    leaf <- leaf[[1L]]
  }
  expect_true(is_view(leaf))
  # the 65-step path no longer formats an identifier
  expect_null(.Call(mizu:::mizu_zc_view_name, leaf))
  out <- unserialize(serialize(leaf, NULL))
  expect_false(is_view(out))
  expect_identical(out, c("leaf", "strings"))
})

test_that("a list view nested past the identifier depth keeps value semantics", {
  p <- channel_pair(arena_size = 0)
  on.exit(channel_end(p))
  deep <- list("x")
  for (i in 1:64) {
    deep <- list(deep)
  }
  mizu_send(p[["host"]], list(runif(100000), deep))
  v <- mizu_recv(p[["peer"]], 5)
  sub <- v[[2L]]
  for (i in 1:64) {
    sub <- sub[[1L]]
  }
  expect_true(is_view(sub))
  expect_null(.Call(mizu:::mizu_zc_view_name, sub))
  out <- unserialize(serialize(sub, NULL))
  expect_false(is_view(out))
  expect_identical(out, list("x"))
  # re-sent in a zc-sized tree, the probe rejects the tree: by value,
  # never a bad REF
  mizu_send(p[["host"]], list(runif(100000), sub))
  back <- mizu_recv(p[["peer"]], 5)
  expect_false(is_view(back[[2L]]))
  expect_identical(back[[2L]], list("x"))
})

test_that("an attributed string view nested in a payload resolves as a view", {
  p <- channel_pair(arena_size = 0)
  x <- paste0("s", 1:50000)
  names(x) <- paste0("n", 1:50000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(v))
  fill <- runif(100000)
  mizu_send(p[["peer"]], list(v, fill)) # an attributed view rejects the tree
  y <- mizu_recv(p[["host"]], 5)
  expect_true(is_view(y[[1]])) # resolved against the region, attrs restored
  expect_identical(y[[1]], x)
  expect_identical(y[[2]], fill)
  channel_end(p)
})

test_that("a data frame crosses as a view with its class and row names", {
  p <- channel_pair(arena_size = 0)
  x <- data.frame(a = runif(100000), b = paste0("s", 1:100000))
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_s3_class(y, "data.frame")
  expect_identical(nrow(y), 100000L)
  expect_identical(as.numeric(y[["a"]]), x[["a"]])
  expect_identical(y[["b"]], x[["b"]])
  channel_end(p)
})

test_that("attributed views carry 'I' blobs: identical() across the shapes", {
  p <- channel_pair(arena_size = 0)

  # a standalone factor: an 'I' blob on the MIZH root, applied as-is
  f <- factor(rep(c("a", "b"), 20000), levels = c("a", "b"))
  mizu_send(p[["host"]], f)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  peek <- view_peek(y)
  expect_identical(peek(64 + 4 * length(f), 1), charToRaw("I"))
  expect_identical(y[], f)

  # a frame: the 'I' root blob located by the header's attrs offset
  df <- data.frame(a = runif(20000), b = rep("s", 20000))
  mizu_send(p[["host"]], df)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  peek <- view_peek(y)
  expect_identical(peek(i64v(peek(8, 8)), 1), charToRaw("I"))
  expect_identical(as.data.frame(y[]), df)

  # a frame with a list column, a matrix column and an ordered-factor
  # column: the root dict is the whitelisted shape, the columns are not —
  # the apply-as-is read must not reject it
  df2 <- data.frame(x = runif(5000), stringsAsFactors = FALSE)
  df2$mat <- matrix(runif(5000 * 4), 5000, 4)
  df2$ord <- ordered(rep(letters[1:4], length.out = 5000))
  df2$lst <- lapply(seq_len(5000), function(i) i)
  mizu_send(p[["host"]], df2)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_identical(as.data.frame(y[]), df2)

  # a Date-columned frame and an integer64-columned frame (the leaf gate:
  # the directory tag carries the integer64 class, no blob)
  df3 <- data.frame(
    d = as.Date("2022-03-21") + 0:19999,
    i64 = structure(runif(20000), class = "integer64")
  )
  mizu_send(p[["host"]], df3)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  peek <- view_peek(y)
  expect_identical(
    readBin(peek(64 + 16 + 32, 4), "integer", 1, 4, endian = "little"),
    32L
  ) # the int64 leaf's directory tag
  expect_identical(
    readBin(peek(64 + 16 + 32 + 4, 4), "integer", 1, 4, endian = "little"),
    0L
  ) # and no attrs tail
  expect_identical(as.data.frame(y[]), df3)
  channel_end(p)
})

test_that("a named vector and an environment-attributed vector keep their attrs", {
  p <- channel_pair(arena_size = 0)

  # a named vector: the {names} 'I' blob
  nv <- runif(20000)
  names(nv) <- paste0("n", seq_along(nv))
  mizu_send(p[["host"]], nv)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  peek <- view_peek(y)
  expect_identical(peek(64 + 8 * length(nv), 1), charToRaw("I"))
  expect_identical(y[], nv)

  # an attribute set the codec cannot carry (an environment): the blob
  # stays R_Serialize, applied verbatim
  v <- runif(20000)
  attr(v, "meta") <- new.env(parent = emptyenv())
  mizu_send(p[["host"]], v)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  peek <- view_peek(y)
  expect_false(identical(peek(64 + 8 * length(v), 1), charToRaw("I")))
  expect_true(is.environment(attr(y, "meta")))
  expect_identical(as.numeric(y), as.numeric(v))
  channel_end(p)
})

test_that("a list view re-sends as REF even after element access", {
  p <- channel_pair(arena_size = 0)
  x <- list(nums = runif(100000), strs = paste0("s", 1:5000))
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_identical(rc_of(y), c(2L, 0L))
  invisible(y[["nums"]][1]) # the element cache (data2) is not a materialization
  mizu_send(p[["peer"]], y)
  z <- mizu_recv(p[["host"]], 5)
  expect_true(is_view(z))
  expect_identical(rc_of(y), c(2L, 1L)) # counted; escape marked REFHELD
  expect_identical(as.numeric(z[["nums"]]), x[["nums"]])
  channel_end(p)
})

test_that("an element extracted from a list view re-sends as path-form REF", {
  p <- channel_pair(arena_size = 0)
  x <- list(a = runif(100000), b = runif(100000))
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["peer"]], y[["a"]]) # REF "/mizu_...[1]"
  z <- mizu_recv(p[["host"]], 5)
  expect_true(is_view(z))
  expect_identical(as.numeric(z), x[["a"]])
  channel_end(p)
})

test_that("an extracted element view pins the region past the root view's GC", {
  p <- channel_pair(arena_size = 0)
  x <- list(a = runif(100000), b = runif(100000))
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  sub <- y[["a"]]
  mizu_send(p[["host"]], x) # reap: the loan drops; the view chain pins the region
  expect_identical(chan_ledger(p[["host"]]), 1L)
  rm(y)
  invisible(gc())
  mizu_send(p[["host"]], x) # the element view still pins: no early release
  expect_identical(chan_ledger(p[["host"]]), 1L)
  expect_identical(as.numeric(sub), x[["a"]])
  rm(sub)
  invisible(gc())
  mizu_send(p[["host"]], x) # count 0 -> free list
  expect_identical(chan_ledger(p[["host"]]), 0L)
  channel_end(p)
})

test_that("list views duplicate on mutation; in-place assignment errors", {
  p <- channel_pair(arena_size = 0)
  x <- list(a = runif(100000), b = paste0("t", 1:5000))
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_error(y[["a"]] <- 0, "Set_elt") # ALTLIST provides no Set_elt
  y2 <- y # NAMED bump: subassignment duplicates instead
  y2[["a"]] <- 0
  expect_false(is_view(y2))
  expect_identical(y2[["a"]], 0)
  expect_true(is_view(y))
  expect_identical(as.numeric(y[["a"]]), x[["a"]])
  mizu_send(p[["host"]], x) # the region's next payload is undisturbed
  z <- mizu_recv(p[["peer"]], 5)
  expect_identical(as.numeric(z[["a"]]), x[["a"]])
  channel_end(p)
})

test_that("a view nested in a big list tree keeps the tree on the serialize tiers", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  # the tree's eligible leaf (400 KB) stages MORL; the nested REF-able view
  # rides as a remote leaf (F2.5) — by reference, never copied in
  mizu_send(p[["peer"]], list(runif(50000), v))
  w <- mizu_recv(p[["host"]], 30)
  expect_true(is_view(w))
  expect_true(is_view(w[[2L]]))
  expect_identical(
    .Call(mizu:::mizu_zc_view_name, w[[2L]]),
    .Call(mizu:::mizu_zc_view_name, v)
  )
  # v + the remote leaf's lazy resolve + the wrap-time loan that holds the
  # referenced region from the wrap (the lazy resolve alone would count
  # only after the consumer-done signal)
  expect_identical(rc_of(v), c(3L, 1L))
  channel_end(p)
})

test_that("a remote leaf's region is held from the root's wrap (R-02)", {
  ch1 <- channel_pair()
  ch2 <- channel_pair()
  set.seed(3)
  b1 <- as.double(sample(1e5))
  mizu_send(ch1$host, b1)
  vB <- mizu_recv(ch1$peer, 5)
  bname <- .Call(mizu:::mizu_zc_view_name, vB)
  mizu_send(ch2$host, list(vB, runif(2e4)))
  vA <- mizu_recv(ch2$peer, 5) # MIZL A with a remote leaf -> B, unresolved
  for (k in 1:3) {
    mizu_send(ch2$host, k)
    mizu_recv(ch2$peer, 5)
  } # A's pin reaped
  mizu_send(ch2$host, new.env())
  mizu_recv(ch2$peer, 5) # mark list reset
  rm(vB)
  invisible(gc())
  invisible(gc()) # B: only the producer's loan is left
  for (k in 1:2) {
    mizu_send(ch1$host, k)
    mizu_recv(ch1$peer, 5)
  } # the reaper runs; the wrap loan holds B
  reused <- FALSE
  for (k in 1:4) {
    bk <- rev(b1) * (k + 1)
    mizu_send(ch1$host, bk)
    vk <- mizu_recv(ch1$peer, 5)
    if (identical(.Call(mizu:::mizu_zc_view_name, vk), bname)) {
      reused <- TRUE
      break
    }
    rm(vk)
    invisible(gc())
  }
  expect_false(reused) # the wrap loan keeps B out of the free list
  e <- vA[[1L]]
  expect_true(is_view(e))
  expect_identical(sum(e), sum(b1))
  channel_end(ch1)
  channel_end(ch2)
})

test_that("a deeply nested payload raises at the guarded walkers (R-04)", {
  n <- cstack_trigger_depth()
  skip_if(n == 0L, "serialize recursion does not hit the C-stack limit here")
  p <- channel_pair()
  deep <- function(n) {
    x <- NULL
    for (i in seq_len(n)) {
      x <- list(x)
    }
    x
  }
  # a plain tryCatch: testthat's own frames leave no margin past the
  # C-stack limit for expect_error's handling
  r <- tryCatch(
    {
      mizu_send(p[["host"]], deep(n))
      "sent"
    },
    error = conditionMessage
  )
  expect_match(r, "C stack")
  # the stage is transactional: the channel still flows after the raise
  mizu_send(p[["host"]], 42L)
  expect_identical(mizu_recv(p[["peer"]], 5), 42L)
  channel_end(p)
})

test_that("a foreign ALTREP leaf keeps the tree on the serialize tiers", {
  p <- channel_pair(arena_size = 0)
  x <- list(big = runif(100000), seq = 1:2^20)
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 30)
  expect_false(is_view(y))
  expect_equal(y[["seq"]], 1:2^20) # the compact sequence stayed compact
  expect_identical(y[["big"]], x[["big"]])
  channel_end(p)
})

test_that("pairlists keep their type (the MORL layout coerces to VECSXP)", {
  p <- channel_pair(arena_size = 0)
  x <- pairlist(a = runif(50000), b = "x")
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_type(y, "pairlist")
  expect_identical(y, x)
  channel_end(p)
})

test_that("a mid-chain re-sender's death leaks + unlinks, never recycles under a live view", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(
    quote({
      x <- mizu_recv(ch, timeout = 30) # a view over the host's region
      mizu_send(ch, x) # REF it back (marks the region REFHELD)
      Sys.sleep(30)
    }),
    arena_size = 0
  )
  x <- runif(100000)
  mizu_send(ch, x)
  z <- mizu_recv(ch, 30) # the host's own view, via REF
  expect_true(is_view(z))
  mizu_send(ch, x) # reap: the first region -> ledger
  expect_identical(chan_ledger(ch), 1L)
  nm <- .Call(mizu:::mizu_channel_stat, ch)[["name"]]

  pid <- .Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]]
  kill_hard(pid)
  r <- mizu_recv(ch, 30) # the verdict probes; force path runs
  expect_s3_class(r, "mizu_peer_gone")
  # REFHELD: the region leaks + unlinks — it never rejoins the free list,
  # and the host's live view still reads its pages
  expect_identical(chan_ledger(ch), 0L)
  expect_identical(chan_fl(ch), 0L)
  expect_identical(as.numeric(z), x)
  mizu_close(ch, 10)
})

test_that("a view nested in a larger payload resolves counted (channel)", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  expect_identical(rc_of(v), c(2L, 0L)) # the producer's loan + the view

  mizu_send(p[["host"]], x) # reap: the loan drops; v pins the region
  expect_identical(rc_of(v), c(1L, 0L))

  # the view never reaches tier selection nested in a list: it crosses on
  # the serialize-hook path, and the receive-side resolve is counted
  mizu_send(p[["peer"]], list("wrap", v))
  w <- mizu_recv(p[["host"]], 30)
  expect_true(is_view(w[[2L]]))
  expect_identical(rc_of(v), c(2L, 1L)) # resolved counted; escape marked REFHELD
  expect_identical(as.numeric(w[[2L]]), x)
  channel_end(p)
})

test_that("wire resolves cluster on the mapping cache; counts stay per-view", {
  p <- channel_pair(arena_size = 0)
  x1 <- runif(100000)
  x2 <- runif(100000)
  mizu_send(p[["host"]], x1)
  v1 <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["host"]], x2)
  v2 <- mizu_recv(p[["peer"]], 5)
  b1 <- rc_of(v1)[1L]
  b2 <- rc_of(v2)[1L]

  # 50 references across 2 regions: 2 cached mappings but 50 counted adds —
  # a resolve hook fired per open instead of per resolve would read +2
  w <- unserialize(serialize(c(rep(list(v1), 25), rep(list(v2), 25)), NULL))
  expect_true(is_view(w[[1L]]))
  expect_true(is_view(w[[50L]]))
  expect_identical(rc_of(v1)[1L], b1 + 25L)
  expect_identical(rc_of(v2)[1L], b2 + 25L)
  expect_identical(as.numeric(w[[50L]]), x2)
  rm(w)
  invisible(gc())
  expect_identical(rc_of(v1)[1L], b1)
  expect_identical(rc_of(v2)[1L], b2)
  channel_end(p)
})

test_that("cache eviction never unmaps under live views; counts rebalance after GC", {
  p <- channel_pair(arena_size = 0)
  xs <- lapply(1:18, function(i) runif(100000))
  vs <- lapply(xs, function(xi) {
    mizu_send(p[["host"]], xi)
    mizu_recv(p[["peer"]], 5)
  })
  bs <- vapply(vs, function(v) rc_of(v)[1L], integer(1))

  # 18 regions against 16 cache slots: the first two wraps are evicted
  # mid-resolve — their views keep the mappings through their own chains.
  # Counts before values: on R < 4.6 identical() materializes ALTREP views
  # (writable DATAPTR), which fires the release early
  w <- unserialize(serialize(vs, NULL))
  expect_true(is_view(w[[1L]]))
  expect_identical(rc_of(vs[[1L]])[1L], bs[1L] + 1L)
  expect_identical(rc_of(vs[[18L]])[1L], bs[18L] + 1L)
  expect_identical(as.numeric(w[[1L]]), xs[[1L]])
  expect_identical(as.numeric(w[[18L]]), xs[[18L]])
  rm(w)
  invisible(gc())
  expect_identical(rc_of(vs[[1L]])[1L], bs[1L])
  expect_identical(rc_of(vs[[18L]])[1L], bs[18L])
  channel_end(p)
})

test_that("a nested-resolved view re-marks REFHELD through the shared mapping", {
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  mizu_send(p[["host"]], "reap") # the loan drops; v pins the region
  mizu_recv(p[["peer"]], 5) # drain the reap message (the ring is FIFO)
  expect_identical(rc_of(v), c(1L, 0L))

  # v crosses nested (the serialize-hook path): the host's resolve wraps it
  # over the vendored cache's mapping
  mizu_send(p[["peer"]], list("wrap", v))
  w <- mizu_recv(p[["host"]], 30)
  rv <- w[[2L]]
  expect_true(is_view(rv))
  expect_identical(rc_of(v), c(2L, 1L))

  # re-sending the resolved view nested fires the emit hook on a vendored
  # chain terminus: the flag store goes straight through the shared mapping.
  # The count assert precedes the value comparison: identical() materializes
  # the view on R < 4.6, firing the release early
  mizu_send(p[["host"]], list("wrap", rv))
  w2 <- mizu_recv(p[["peer"]], 30)
  expect_true(is_view(w2[[2L]]))
  expect_identical(rc_of(v), c(3L, 1L))
  expect_identical(as.numeric(w2[[2L]]), x)
  channel_end(p)
})

test_that("a forked child's resolve and GC leave the parent's count unmoved", {
  skip_on_os("windows") # no fork
  skip_if_no_fork() # Positron/RStudio sessions block mcfork
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  w <- unserialize(serialize(list(v), NULL)) # the entry the child inherits
  before <- rc_of(v)[1L]

  env <- environment()
  res <- parallel::mcparallel(
    {
      w2 <- unserialize(serialize(list(v), NULL)) # an inherited cache hit
      rm(w, envir = env) # the parent-armed record: the pid guard skips it
      rm(w2)
      invisible(gc())
      .Call(mizu:::mizu_zc_refcount, v)[1L]
    },
    silent = TRUE
  )
  expect_identical(parallel::mccollect(res)[[1L]], before)
  expect_identical(rc_of(v)[1L], before) # the child's GC subbed only its own
  expect_identical(as.numeric(v), x) # the parent's mapping intact
  rm(w)
  invisible(gc())
  channel_end(p)
})

test_that("nested references dedupe to one mapping per region (Linux VMAs)", {
  skip_on_os(c("mac", "windows")) # asserts on /proc/self/maps
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], x)
  v <- mizu_recv(p[["peer"]], 5)
  nmaps <- function() length(readLines("/proc/self/maps"))

  s <- serialize(rep(list(v), 200), NULL)
  before <- nmaps()
  w <- unserialize(s) # 200 resolves over one region
  after <- nmaps()
  # one cached split mapping (~2 VMAs), not two fresh mappings per reference
  expect_lte(after - before, 20L)
  expect_true(is_view(w[[1L]]))
  rm(w)
  invisible(gc())
  channel_end(p)
})

test_that("pool task args carry views counted; a returned view echoes by REF", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], runif(100000))
  pool_step(p)
  v <- mizu_collect(t, 5)
  pool_step(p) # the worker's keeper sweep drops its loan
  expect_identical(rc_of(v), c(1L, 0L))

  # the view nests inside the task's argument list (the serialize-hook
  # path): the escape is marked at submit, the worker's resolve counted
  t2 <- mizu_submit(p[["ctrl"]], sum(x), x = v)
  expect_identical(rc_of(v)[2L], 1L)
  pool_step(p)
  expect_identical(rc_of(v)[1L], 2L) # v + the worker's resolved view
  expect_identical(mizu_collect(t2, 5), sum(v))

  # a worker returning a received view stages it top-level: REF
  t3 <- mizu_submit(p[["ctrl"]], identity(x), x = v)
  pool_step(p)
  w <- mizu_collect(t3, 5)
  expect_true(is_view(w))
  expect_identical(as.numeric(w), as.numeric(v))
  pool_end(p)
})

test_that("a worker re-submits a received view by reference (nested composition)", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], runif(100000))
  pool_step(p)
  v <- mizu_collect(t, 5)

  # the worker resolves its arg counted, re-submits it nested (the
  # serialize-hook path again), and the chain returns a view of one region
  t2 <- mizu_submit(
    p[["ctrl"]],
    {
      s <- mizu_submit(mizu_current_pool(), identity(x), x = x)
      mizu_collect(s, timeout = 5)
    },
    x = v
  )
  pool_step(p)
  w <- mizu_collect(t2, 5)
  expect_true(is_view(w))
  expect_identical(as.numeric(w), as.numeric(v))
  pool_end(p)
})

test_that("a pool result holding a nested view keeps the pin until collect", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], runif(100000))
  pool_step(p)
  v <- mizu_collect(t, 5)

  # the result list holds a view: the codec declines and R_Serialize emits
  # the identifier through the wire hook, so the conditional pin must hold
  t2 <- mizu_submit(p[["ctrl"]], list(x), x = v)
  pool_step(p)
  w <- mizu_collect(t2, 5)
  expect_true(is_view(w[[1L]]))
  expect_identical(as.numeric(w[[1L]]), as.numeric(v))
  pool_end(p)
})

test_that("foreign mori objects ride the serialize-hook path", {
  skip_if_not_installed("mori")
  p <- channel_pair(arena_size = 0)
  x <- runif(100000)
  mizu_send(p[["host"]], mori::share(x))
  y <- mizu_recv(p[["peer"]], 30)
  expect_false(is_view(y)) # not mizu-native: no refcount protocol
  expect_true(mori::is_shared(y)) # resolved by mori's own hooks
  expect_identical(as.numeric(y), x)
  channel_end(p)
})

test_that("a mid-chain re-sender killed with a nested view outstanding leaks + unlinks", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(
    quote({
      x <- mizu_recv(ch, timeout = 30) # a view over the host's region
      mizu_send(ch, list("wrap", x)) # nested re-send: the emit marks REFHELD
      Sys.sleep(30)
    }),
    arena_size = 0
  )
  x <- runif(100000)
  mizu_send(ch, x)
  w <- mizu_recv(ch, 30) # w[[2]] resolved counted
  expect_true(is_view(w[[2L]]))
  mizu_send(ch, x) # reap: the first region -> ledger
  expect_identical(chan_ledger(ch), 1L)

  pid <- .Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]]
  kill_hard(pid)
  r <- mizu_recv(ch, 30) # the verdict probes; the force path runs
  expect_s3_class(r, "mizu_peer_gone")
  # REFHELD: the region leaks + unlinks — the peer's count is never
  # reclaimed, and the host's live view still reads its pages
  expect_identical(chan_ledger(ch), 0L)
  expect_identical(chan_fl(ch), 0L)
  expect_identical(rc_of(w[[2L]]), c(2L, 1L))
  expect_identical(as.numeric(w[[2L]]), x)
  mizu_close(ch, 10)
})

test_that("pool string and list results cross as views", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], paste0("w-", 1:30000))
  pool_step(p)
  r <- mizu_collect(t, 5)
  expect_true(is_view(r))
  expect_identical(r, paste0("w-", 1:30000))

  t2 <- mizu_submit(
    p[["ctrl"]],
    list(a = runif(100000), b = paste0("x", 1:20000))
  )
  pool_step(p)
  r2 <- mizu_collect(t2, 5)
  expect_true(is_view(r2))
  expect_true(is_view(r2[["a"]]))
  expect_true(is_view(r2[["b"]]))
  expect_identical(r2[["b"]], paste0("x", 1:20000))
  pool_end(p)
})

test_that("a big vector task argument round-trips", {
  # the payload list(expr, args) stages as MORL past the floor: the worker's
  # arg is a view over the payload region, not an unserialized copy
  p <- pool_pair()
  v <- runif(200000)
  t <- mizu_submit(p[["ctrl"]], .Call(mizu:::mizu_zc_view_check, v), v = v)
  pool_step(p)
  expect_true(mizu_collect(t, 5))
  t2 <- mizu_submit(p[["ctrl"]], sum(v), v = v)
  pool_step(p)
  expect_identical(mizu_collect(t2, 5), sum(v))
  pool_end(p)
})

test_that("string and list views round-trip cross-process", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(
    quote({
      is_view <- function(x) .Call(mizu:::mizu_zc_view_check, x)
      s <- mizu_recv(ch, timeout = 30)
      l <- mizu_recv(ch, timeout = 30)
      mizu_send(
        ch,
        list(
          is_view(s),
          is_view(l),
          length(s),
          s[[1L]],
          l[["a"]][[1L]],
          length(l[["b"]])
        )
      )
    }),
    arena_size = 0
  )
  s <- paste0("s", 1:50000)
  l <- list(a = runif(100000), b = paste0("x", 1:20000))
  mizu_send(ch, s)
  mizu_send(ch, l)
  expect_identical(
    mizu_recv(ch, 30),
    list(TRUE, TRUE, 50000L, "s1", l[["a"]][[1L]], 20000L)
  )
  mizu_close(ch, 10)
})

test_that("a peer killed holding a view is force-reclaimed after the verdict", {
  skip_if_no_child_mizu()
  ch <- mizu_channel(
    quote({
      x <- mizu_recv(ch, timeout = 30) # a view the peer holds onto
      mizu_send(ch, length(x)) # ack: the view is in hand
      Sys.sleep(30) # hold the view; never release
    }),
    arena_size = 0
  )
  x <- runif(100000)
  mizu_send(ch, x)
  expect_identical(mizu_recv(ch, 30), 100000L) # the peer holds its view now
  mizu_send(ch, x) # reaps the first keeper -> ledger
  expect_identical(chan_ledger(ch), 1L)

  pid <- .Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]]
  kill_hard(pid)
  # the recv probes the death verdict; the force-reclaim rides it
  r <- mizu_recv(ch, 30)
  expect_s3_class(r, "mizu_peer_gone")
  expect_identical(chan_ledger(ch), 0L)
  expect_gte(chan_fl(ch), 1L) # the lent region rejoined the list
  mizu_close(ch, 10)
})

test_that("a pairlist inside a list tree rides the layout as a list", {
  p <- channel_pair(arena_size = 0)
  x <- list(pl = as.pairlist(list(1, 2)), nums = runif(100000))
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_true(is_view(y))
  expect_identical(y[["pl"]], list(1, 2)) # MORL coerces LISTSXP to VECSXP
  expect_identical(as.numeric(y[["nums"]]), x[["nums"]])
  channel_end(p)
})

test_that("a re-sent map view degrades to a materializing copy", {
  p <- pool_pair()
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:1000 + 0,
    function(i) i * 2,
    list(),
    template = numeric(1)
  )
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  v <- mizu:::map_collect(st, mizu:::mono_time() + 30, collect = "view")
  expect_true(is_view(v))
  ch <- channel_pair(arena_size = 0)
  mizu_send(ch[["host"]], v)
  w <- mizu_recv(ch[["peer"]], 5)
  expect_false(is_view(w))
  expect_identical(w, 1:1000 * 2 + 0)
  channel_end(ch)
  pool_end(p)
})

test_that("vec views serve element, region, and serialize reads off the mapping", {
  p <- channel_pair()
  on.exit(channel_end(p))
  l <- rep(c(TRUE, FALSE, NA), length.out = 70000)
  mizu_send(p$host, l)
  lv <- mizu_recv(p$peer, 5)
  expect_identical(lv[[1]], TRUE)
  expect_identical(lv[[3]], NA)
  expect_true(any(lv, na.rm = TRUE))
  expect_identical(sum(lv[1:6], na.rm = TRUE), 2L)
  z <- complex(70000, 1, 2)
  mizu_send(p$host, z)
  cv <- mizu_recv(p$peer, 5)
  expect_identical(cv[[1]], 1 + 2i)
  expect_identical(sum(Re(cv[1:3])), 3)
  expect_identical(mean(cv), 1 + 2i)
  expect_true(all(Im(cv) == 2))
  r <- as.raw(seq_len(70000) %% 256L)
  mizu_send(p$host, r)
  rv <- mizu_recv(p$peer, 5)
  expect_identical(rv[[1]], as.raw(1))
  expect_identical(rv[1:4], r[1:4])
  i <- seq_len(70000) + 0L
  mizu_send(p$host, i)
  iv <- mizu_recv(p$peer, 5)
  expect_identical(iv[[7]], 7L)
  expect_identical(mean(iv), mean(i))
  expect_identical(unserialize(serialize(cv, NULL)), z)
  expect_identical(unserialize(serialize(rv, NULL)), r)
})

test_that("a verbatim uint64 frame column serves view reads", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  p <- channel_pair(ident = c(2L, 71L)) # MIZS + ATTRS + MIZL + U64V
  on.exit(channel_end(p))
  u <- arrow::Array$create(
    rep(c(42, 2^53, 7, 2^40), length.out = 9000)
  )$cast(arrow::uint64())
  df <- arrow::arrow_table(x = runif(9000), u = u)
  mizu_send(p$host, df)
  out <- mizu_recv(p$peer, 5)
  col <- out[["u"]]
  expect_true(is_view(col))
  expect_identical(length(col), 9000L)
  expect_identical(col[[1]], 42)
  expect_identical(col[[2]], 2^53)
  expect_identical(col[3:4], c(7, 2^40))
  expect_identical(col[1:4], c(42, 2^53, 7, 2^40))
  col[1] <- 99 # COW: the converted values move into data2
  expect_identical(col[[1]], 99)
  expect_identical(col[1:4], c(99, 2^53, 7, 2^40))
})

test_that("string views serve element, length, and serialize reads", {
  p <- channel_pair()
  on.exit(channel_end(p))
  s <- rep(c("alpha", "beta", NA, "héllo ✓"), length.out = 70000)
  mizu_send(p$host, s)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  expect_identical(v[[1]], "alpha")
  expect_identical(v[[3]], NA_character_)
  expect_identical(length(v), 70000L)
  expect_identical(v[1:4], s[1:4])
  expect_identical(unserialize(serialize(v, NULL)), s)
})

test_that("list views serve element and serialize reads", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- list(runif(30000), c("a", "b"), as.list(letters))
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  expect_identical(length(v), 3L)
  expect_identical(v[[2]], c("a", "b"))
  expect_identical(v[[3]], as.list(letters))
  expect_identical(unserialize(serialize(v, NULL)), x)
})

test_that("a compact sequence leaf writes off the region accessors", {
  p <- foreign_pair(caps = 4L) # MIZL: the foreign gate admits the ALTREP leaf
  on.exit(channel_end(p))
  x <- list(1:100000, runif(70000))
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  expect_identical(v[[1]], 1:100000)
  expect_identical(v[[2]], x[[2]])
})

test_that("the layout NA scan covers complex and integer64", {
  skip_if_not_installed("bit64")
  p <- channel_pair()
  on.exit(channel_end(p))
  z <- complex(70000, 1, 2)
  z[3] <- NA_complex_
  mizu_send(p$host, z)
  expect_identical(mizu_recv(p$peer, 5), z)
  i <- bit64::as.integer64(seq_len(70000))
  i[4] <- bit64::NA_integer64_
  mizu_send(p$host, i)
  expect_identical(mizu_recv(p$peer, 5), i)
})

test_that("an S4 pairlist keeps the serialize tiers", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- asS4(pairlist(1, "two"))
  mizu_send(p$host, x)
  expect_identical(mizu_recv(p$peer, 5), x)
})

test_that("the foreign zc gates decline to the 'I' copy per capability", {
  p <- foreign_pair() # caps 0
  on.exit(channel_end(p))
  s <- rep(c("alpha", "beta"), length.out = 70000)
  mizu_send(p$host, s)
  expect_identical(mizu_recv(p$peer, 5), s)
  x <- as.list(runif(70000))
  mizu_send(p$host, x)
  expect_identical(mizu_recv(p$peer, 5), x)
})

test_that("a foreign string vector stages as MIZS with the bit", {
  p <- foreign_pair(caps = 1L)
  on.exit(channel_end(p))
  s <- rep(c("alpha", "beta", "héllo ✓"), length.out = 70000)
  mizu_send(p$host, s)
  out <- mizu_recv(p$peer, 5)
  expect_true(is_view(out))
  expect_identical(out[], s)
})

test_that("an S4 string vector declines loudly on the foreign path", {
  methods::setClass("reiS4ChrF", contains = "character")
  p <- foreign_pair(caps = 1L) # MIZS: the gate's STRSXP branch
  on.exit(channel_end(p))
  x <- methods::new("reiS4ChrF", paste0("s", 1:20000))
  expect_error(mizu_send(p$host, x), class = "mizu_error_not_portable")
})

test_that("a foreign frame with a view column declines without MIZL_REF", {
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, runif(70000))
  v <- mizu_recv(p$peer, 5)
  df <- data.frame(x = runif(70000))
  df$v <- v
  p2 <- foreign_pair(caps = 7L) # MIZS + ATTRS + MIZL, no MIZL_REF
  on.exit(channel_end(p2), add = TRUE)
  mizu_send(p2$host, df)
  expect_identical(mizu_recv(p2$peer, 5), df)
})

test_that("the foreign tree walk verdicts per leaf shape and caps", {
  p <- foreign_pair(caps = 4L) # MIZL without ATTRS: a factor leaf rejects
  on.exit(channel_end(p))
  x <- list(factor(c("a", "b")), runif(70000))
  mizu_send(p$host, x)
  expect_identical(mizu_recv(p$peer, 5), x)
  p2 <- foreign_pair(caps = 6L) # MIZL + ATTRS
  on.exit(channel_end(p2), add = TRUE)
  z <- list(rep("x", 100), runif(70000)) # a string leaf short of MIZS
  mizu_send(p2$host, z)
  expect_identical(mizu_recv(p2$peer, 5), z)
  # a named atomic leaf rejects the walk, and the 'I' writer names it
  y <- list(stats::setNames(1:10, letters[1:10]), runif(70000))
  expect_error(mizu_send(p2$host, y), class = "mizu_error_not_portable")
  sa <- structure(rep("x", 100), foo = 1L) # an attributed string leaf
  w <- list(sa, runif(70000))
  expect_error(mizu_send(p2$host, w), class = "mizu_error_not_portable")
})

test_that("a view nested in a foreign tree keeps the tree on the copy tiers", {
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, runif(70000))
  v <- mizu_recv(p$peer, 5)
  p2 <- foreign_pair(caps = 6L)
  on.exit(channel_end(p2), add = TRUE)
  x <- list(v, runif(70000))
  mizu_send(p2$host, x)
  out <- mizu_recv(p2$peer, 5)
  expect_identical(out[[1]][], x[[1]][])
  expect_identical(out[[2]], x[[2]])
})

test_that("views re-sent foreign take the copy path short of capabilities", {
  p <- channel_pair()
  on.exit(channel_end(p))
  s <- rep(c("alpha", "beta"), length.out = 70000)
  mizu_send(p$host, s)
  v <- mizu_recv(p$peer, 5)
  p2 <- foreign_pair(caps = 0L) # no MIZS: the string view copies
  on.exit(channel_end(p2), add = TRUE)
  mizu_send(p2$host, v)
  expect_identical(mizu_recv(p2$peer, 5), s)
  xa <- runif(70000)
  names(xa) <- paste0("n", seq_along(xa))
  mizu_send(p$host, xa)
  va <- mizu_recv(p$peer, 5)
  expect_true(is_view(va))
  # the REF filter declines short of ATTRS, the 'I' writer naming the node
  p3 <- foreign_pair(caps = 0L)
  on.exit(channel_end(p3), add = TRUE)
  expect_error(mizu_send(p3$host, va), class = "mizu_error_not_portable")
})

test_that("a view's serialized state rejects a mangled identifier", {
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, runif(70000))
  v <- mizu_recv(p$peer, 5)
  nm <- .Call(mizu:::mizu_zc_view_name, v)
  ser <- serialize(v, NULL)
  at <- grepRaw(charToRaw(nm), ser, fixed = TRUE)
  expect_true(at > 0)
  ser[at:(at + nchar(nm) - 1L)] <- charToRaw(strrep("z", nchar(nm)))
  expect_error(unserialize(ser), "invalid serialized state")
  s <- rep(c("alpha", "beta"), length.out = 70000)
  mizu_send(p$host, s)
  sv <- mizu_recv(p$peer, 5)
  snm <- .Call(mizu:::mizu_zc_view_name, sv)
  sser <- serialize(sv, NULL)
  sat <- grepRaw(charToRaw(snm), sser, fixed = TRUE)
  expect_true(sat > 0)
  sser[sat:(sat + nchar(snm) - 1L)] <- charToRaw(strrep("z", nchar(snm)))
  expect_error(unserialize(sser), "invalid serialized state")
})

test_that("a nested view in an arena-tier payload pins its region", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- runif(70000)
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  mizu_send(p$peer, list(v, strrep("x", 1000)))
  out <- mizu_recv(p$host, 5)
  expect_identical(out[[1L]], x)
  expect_identical(out[[2L]], strrep("x", 1000))
})

test_that("the pin chain splices its tombstoned cells", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- runif(70000)
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  for (i in seq_len(80L)) {
    mizu_send(p$peer, list(v, strrep("x", 64)))
    expect_identical(mizu_recv(p$host, 5)[[1L]], x)
  }
})

test_that("a uint64 frame column serves summary, serialize, and COW reads", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  p <- channel_pair(ident = c(2L, 71L)) # MIZS + ATTRS + MIZL + U64V
  on.exit(channel_end(p))
  uv <- rep(c(42, 2^53, 7, 2^40), length.out = 9000)
  u <- arrow::Array$create(uv)$cast(arrow::uint64())
  mizu_send(p$host, arrow::arrow_table(u = u))
  col <- mizu_recv(p$peer, 5)[["u"]]
  expect_true(is_view(col))
  # summary ops read whole regions off the mapping through Get_region
  expect_identical(sum(col), sum(uv))
  expect_identical(mean(col), mean(uv))
  expect_false(anyNA(col))
  expect_identical(col + 0, uv)
  # the serialized state is the identifier; unserialize re-resolves a view
  col2 <- unserialize(serialize(col, NULL))
  expect_true(is_view(col2))
  expect_identical(sum(col2), sum(uv))
  # COW moves the converted values into data2; reads serve the copy
  col[1] <- 99
  expect_identical(length(col), 9000L)
  expect_identical(col[[1]], 99)
  expect_identical(sum(col), 99 - 42 + sum(uv))
  expect_identical(unserialize(serialize(col, NULL)), c(99, uv[-1]))
})

test_that("bulk summaries read every atomic view class through its region", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- runif(70000)
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  expect_identical(sum(v), sum(x))
  expect_identical(mean(v), mean(x))
  xi <- as.integer(sample(1e6, 70000, replace = TRUE))
  mizu_send(p$host, xi)
  vi <- mizu_recv(p$peer, 5)
  expect_true(is_view(vi))
  expect_identical(sum(vi), sum(xi))
  expect_identical(min(vi), min(xi))
  xl <- runif(70000) > 0.5
  mizu_send(p$host, xl)
  vl <- mizu_recv(p$peer, 5)
  expect_true(is_view(vl))
  expect_identical(any(vl), any(xl))
  expect_identical(all(vl), all(xl))
  xc <- complex(real = runif(70000), imaginary = runif(70000))
  mizu_send(p$host, xc)
  vc <- mizu_recv(p$peer, 5)
  expect_true(is_view(vc))
  expect_identical(vc[[1]], xc[[1]])
  expect_identical(sum(vc), sum(xc))
})

test_that("a list view subset materializes the element cache, reads after", {
  p <- channel_pair()
  on.exit(channel_end(p))
  l <- replicate(5L, runif(30000), simplify = FALSE)
  mizu_send(p$host, l)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  expect_identical(v[1:2], l[1:2])
  expect_true(is_view(v))
  expect_identical(v[[5]], l[[5]])
})

test_that("a serialized list view re-resolves as a view", {
  p <- channel_pair()
  on.exit(channel_end(p))
  l <- replicate(3L, runif(30000), simplify = FALSE)
  mizu_send(p$host, l)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  v2 <- unserialize(serialize(v, NULL))
  expect_true(is_view(v2))
  expect_identical(v2[1:2], l[1:2])
})

test_that("a compact sequence crosses by value", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- 1:100000L # ALTREP: the layout oracle rejects it, R_Serialize serves
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  expect_false(is_view(v))
  expect_identical(v, x)
})

test_that("a string view materializes on a radix sort, reads after", {
  p <- channel_pair()
  on.exit(channel_end(p))
  s <- rep(c("alpha", "beta", "gamma", NA), length.out = 70000)
  mizu_send(p$host, s)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  # radix sort and order read the vector through DATAPTR: materialization
  # is the view's one COW — the release fires, values are untouched
  expect_identical(sort(v, method = "radix"), sort(s))
  expect_identical(length(v), 70000L)
  expect_identical(v[[3]], "gamma")
  expect_identical(v[1:4], s[1:4])
  # a COW'd string view serializes as the plain value (VECSXP state form)
  expect_identical(unserialize(serialize(v, NULL)), s)
  mizu_send(p$host, s)
  v2 <- mizu_recv(p$peer, 5)
  expect_identical(order(v2), order(s))
  expect_identical(v2, s)
})

test_that("ns temporal blobs land as POSIXct and difftime on R peers", {
  p <- foreign_pair(caps = 2L) # ATTRS: the blob rides the MIZH region
  on.exit(channel_end(p))
  t <- .POSIXct(seq(1.7e9, by = 0.5, length.out = 70000), tz = "UTC")
  t[3] <- NA_real_
  mizu_send(p$host, t)
  expect_identical(mizu_recv(p$peer, 5), t)
  d <- as.difftime(seq(1, by = 1.5, length.out = 70000), units = "secs")
  d[5] <- NA
  mizu_send(p$host, d)
  expect_identical(mizu_recv(p$peer, 5), d)
  # a Date's blob is not the tsn form: attrs apply as-is
  dt <- as.Date("2024-01-01") + seq_len(70000)
  mizu_send(p$host, dt)
  expect_identical(mizu_recv(p$peer, 5), dt)
})

test_that("the foreign layout NA scan covers integer64 with and without NA", {
  skip_if_not_installed("bit64")
  p <- foreign_pair(caps = 6L) # ATTRS + MIZL: the frame carries the leaf
  on.exit(channel_end(p))
  df <- data.frame(i = bit64::as.integer64(seq_len(70000)))
  mizu_send(p$host, df)
  expect_identical(mizu_recv(p$peer, 5), df)
  df$i[4] <- bit64::NA_integer64_
  mizu_send(p$host, df)
  expect_identical(mizu_recv(p$peer, 5), df)
})

test_that("a foreign real sequence leaf writes off the region accessors", {
  p <- foreign_pair(caps = 4L) # MIZL
  on.exit(channel_end(p))
  x <- list(as.double(1:100000), runif(70000)) # a compact realseq leaf
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  expect_identical(v[[1]], x[[1]])
  expect_identical(v[[2]], x[[2]])
})

test_that("a remote leaf records its claim in the frame's validity table", {
  p <- foreign_pair(caps = 0L)
  on.exit(channel_end(p))
  mizu_send(p$host, runif(40000)) # a flat MIZH region: the claim is unknown
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  p2 <- foreign_pair(caps = 23L) # MIZS + ATTRS + MIZL + MIZL_REF
  on.exit(channel_end(p2), add = TRUE)
  n <- 40000L
  df <- data.frame(
    a = seq_len(n) + 0,
    s = rep(c("alpha", "beta"), length.out = n),
    r = as.raw(seq_len(n) %% 256L)
  )
  df$v <- v
  mizu_send(p2$host, df)
  out <- mizu_recv(p2$peer, 5)
  expect_true(is_view(out[["v"]]))
  expect_identical(
    .Call(mizu:::mizu_zc_view_name, out[["v"]]),
    .Call(mizu:::mizu_zc_view_name, v)
  )
  expect_identical(out[["v"]][], v[])
  expect_identical(out[["a"]], df[["a"]])
  expect_identical(out[["s"]], df[["s"]])
  expect_identical(out[["r"]], df[["r"]])
})

test_that("a view name is NULL for a non-view and a foreign-class view", {
  skip_if_not_installed("mori")
  expect_null(.Call(mizu:::mizu_zc_view_name, 1:10)) # a compact sequence
  expect_null(.Call(mizu:::mizu_zc_view_name, mori::share(runif(100))))
})

test_that("attributed string and list views re-send foreign as REF", {
  p <- channel_pair()
  on.exit(channel_end(p))
  s <- rep(c("alpha", "beta"), length.out = 70000)
  names(s) <- paste0("n", seq_along(s))
  mizu_send(p$host, s)
  sv <- mizu_recv(p$peer, 5)
  expect_true(is_view(sv))
  x <- list(runif(30000), rep("s", 1000))
  mizu_send(p$host, x)
  xv <- mizu_recv(p$peer, 5)
  expect_true(is_view(xv))
  p2 <- foreign_pair(caps = 7L) # MIZS + ATTRS + MIZL
  on.exit(channel_end(p2), add = TRUE)
  mizu_send(p2$host, sv)
  expect_identical(mizu_recv(p2$peer, 5), s)
  mizu_send(p2$host, xv)
  expect_identical(mizu_recv(p2$peer, 5), x)
})

test_that("a locally re-attributed view re-sends by value, its edit intact", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- runif(70000)
  names(x) <- paste0("n", seq_along(x))
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  # unmodified, the re-send is a REF: the receiver wraps the same region
  mizu_send(p$peer, v)
  back <- mizu_recv(p$host, 5)
  expect_true(is_view(back))
  expect_identical(rc_of(v)[1L], 2L) # v + back
  # a local attribute edit diverges from the region's set: the re-send
  # crosses by value with the local attributes intact (a REF resolves
  # the region's attributes and would silently drop the edit)
  nms <- paste0("m", seq_along(x))
  names(v) <- nms
  mizu_send(p$peer, v)
  out <- mizu_recv(p$host, 5)
  expect_identical(names(out), nms)
  expect_identical(out[], `names<-`(x, nms))
})

test_that("a locally re-attributed view crosses a foreign channel by value", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- runif(70000)
  names(x) <- paste0("n", seq_along(x))
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  p2 <- foreign_pair(caps = 7L) # MIZS + ATTRS + MIZL
  on.exit(channel_end(p2), add = TRUE)
  # unmodified: the ref leaf fires, the receiver wraps the same region
  mizu_send(p2$host, v)
  back <- mizu_recv(p2$peer, 5)
  expect_true(is_view(back))
  expect_identical(
    .Call(mizu:::mizu_zc_view_name, back),
    .Call(mizu:::mizu_zc_view_name, v)
  )
  # a local edit inside the subset: the value write, not the ref leaf
  names(v) <- NULL
  mizu_send(p2$host, v)
  expect_identical(mizu_recv(p2$peer, 5), unname(x))
  # one outside it: a loud decline, never a silent drop
  names(v) <- paste0("m", seq_along(x))
  expect_error(mizu_send(p2$host, v), class = "mizu_error_not_portable")
})

test_that("a tree view with a remote leaf re-sends by reference (REF loan walk)", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- runif(70000)
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  mizu_send(p$peer, list(v, runif(70000)))
  tree <- mizu_recv(p$host, 5)
  expect_true(is_view(tree))
  expect_true(is_view(tree[[1]]))
  # the whole tree re-stages as REF: the remote leaf's region joins the loan
  mizu_send(p$host, tree)
  back <- mizu_recv(p$peer, 5)
  expect_true(is_view(back))
  expect_identical(
    .Call(mizu:::mizu_zc_view_name, back[[1]]),
    .Call(mizu:::mizu_zc_view_name, v)
  )
  expect_identical(back[[1]][], x)
  expect_identical(back[[2]][], tree[[2]][])
})

test_that("an over-deep view chain declines the identifier, values cross", {
  p <- channel_pair()
  on.exit(channel_end(p))
  base <- runif(30000)
  deep <- base
  for (i in seq_len(66L)) {
    deep <- list(deep)
  }
  mizu_send(p$host, deep)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  el <- v
  for (i in seq_len(66L)) {
    el <- el[[1L]]
  }
  # the element's identifier would exceed the path cap: no remote form
  expect_true(is_view(el))
  expect_null(.Call(mizu:::mizu_zc_view_name, el))
  tail <- runif(70000)
  mizu_send(p$peer, list(el, tail))
  out <- mizu_recv(p$host, 5)
  expect_identical(out[[1]], base)
  expect_identical(out[[2]], tail)
})

test_that("an identifier past the readers' span cap falls back, values cross", {
  p <- channel_pair()
  on.exit(channel_end(p))
  base <- runif(30000)
  deep <- base
  for (i in seq_len(62L)) {
    deep <- c(rep(list(0), 99L), list(deep)) # the chain sits at index 100
  }
  mizu_send(p$host, deep)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  el <- v
  for (i in seq_len(62L)) {
    el <- el[[100L]]
  }
  expect_true(is_view(el))
  # the identifier formats (62 <= 64 hops) but its span exceeds 255 bytes
  expect_gt(nchar(.Call(mizu:::mizu_zc_view_name, el)), 255L)
  tail <- runif(70000)
  mizu_send(p$peer, list(el, tail))
  out <- mizu_recv(p$host, 5)
  expect_identical(out[[1]], base)
  expect_identical(out[[2]], tail)
})

test_that("verbatim ns temporal columns land as POSIXct and difftime", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  p <- foreign_pair(caps = 127L) # the full set incl. MIZL_TSN
  on.exit(channel_end(p))
  t0 <- as.POSIXct("2024-01-01 00:00:00.5", tz = "UTC") + 0:9999
  d0 <- as.difftime(0:9999 * 1.5, units = "secs")
  tab <- arrow::arrow_table(
    t = arrow::Array$create(t0, type = arrow::timestamp("ns", "UTC")),
    d = arrow::Array$create(d0, type = arrow::duration("ns"))
  )
  mizu_send(p$host, tab)
  out <- mizu_recv(p$peer, 5)
  expect_identical(class(out$t), c("POSIXct", "POSIXt"))
  expect_identical(attr(out$t, "tzone"), "UTC")
  expect_identical(out$t, as.data.frame(tab)$t)
  expect_identical(class(out$d), "difftime")
  expect_identical(attr(out$d, "units"), "secs")
  # the landing is int64 ns * 1e-9, byte-identical to the fallback form
  expect_identical(
    out$d,
    as.difftime(as.double(as.numeric(d0) * 1e9) * 1e-9, units = "secs")
  )
})

test_that("a lazy ALTREP atomic takes the foreign copy path", {
  p <- foreign_pair(caps = 0L)
  on.exit(channel_end(p))
  x <- as.double(1:400000) # a compact realseq: not directly readable
  mizu_send(p$host, x)
  expect_identical(mizu_recv(p$peer, 5), x)
})

test_that("a named tree declines the foreign layout short of ATTRS", {
  p <- foreign_pair(caps = 4L) # MIZL without ATTRS
  on.exit(channel_end(p))
  x <- list(a = runif(70000), b = runif(70000))
  mizu_send(p$host, x)
  expect_identical(mizu_recv(p$peer, 5), x)
})

test_that("a foreign-class string view takes the foreign copy path", {
  skip_if_not_installed("mori")
  p <- foreign_pair(caps = 1L) # MIZS
  on.exit(channel_end(p))
  x <- rep(c("alpha", "beta", "héllo ✓"), length.out = 70000)
  mizu_send(p$host, mori::share(x))
  expect_identical(mizu_recv(p$peer, 5), x)
})

test_that("an attributed string vector stages foreign by exact size", {
  p <- foreign_pair(caps = 3L) # MIZS + ATTRS
  on.exit(channel_end(p))
  s <- rep(c("alpha", "beta", "héllo ✓"), length.out = 70000)
  names(s) <- paste0("n", seq_along(s))
  mizu_send(p$host, s)
  out <- mizu_recv(p$peer, 5)
  expect_true(is_view(out))
  expect_identical(out[], s)
})

test_that("the foreign size gate uses the inline budget past the floor", {
  p <- foreign_pair(caps = 1L, slot_size = 131072L)
  on.exit(channel_end(p))
  s <- rep(c("alpha", "beta"), length.out = 70000)
  mizu_send(p$host, s)
  out <- mizu_recv(p$peer, 5)
  expect_true(is_view(out))
  expect_identical(out[], s)
  p2 <- foreign_pair(caps = 6L, slot_size = 131072L)
  on.exit(channel_end(p2), add = TRUE)
  df <- data.frame(x = runif(70000), i = seq_len(70000) + 0L)
  mizu_send(p2$host, df)
  expect_identical(mizu_recv(p2$peer, 5), df)
  p3 <- foreign_pair(caps = 4L, slot_size = 131072L)
  on.exit(channel_end(p3), add = TRUE)
  x <- list(runif(70000), runif(70000))
  mizu_send(p3$host, x)
  expect_identical(mizu_recv(p3$peer, 5), x)
})

test_that("a frame that is itself a view declines the foreign gate", {
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(x = runif(70000), i = seq_len(70000) + 0L)
  mizu_send(p$host, df)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  p2 <- foreign_pair(caps = 6L) # ATTRS + MIZL
  on.exit(channel_end(p2), add = TRUE)
  mizu_send(p2$host, v)
  expect_identical(mizu_recv(p2$peer, 5), df)
})

test_that("a lazy ALTREP column rides the foreign frame by value", {
  p <- foreign_pair(caps = 6L) # ATTRS + MIZL
  on.exit(channel_end(p))
  df <- data.frame(i = seq_len(70000), x = runif(70000))
  mizu_send(p$host, df)
  out <- mizu_recv(p$peer, 5)
  expect_identical(out, df)
})

test_that("an inner list view with a remote leaf re-sends as path-form REF", {
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- runif(70000)
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  mizu_send(p$peer, list(list(v, runif(70000)), runif(70000)))
  tree <- mizu_recv(p$host, 5)
  expect_true(is_view(tree))
  inner <- tree[[1L]]
  expect_true(is_view(inner))
  # the inner view re-stages as a path-form REF: the read loans the remote
  # leaf's region with the tree's own record
  mizu_send(p$host, inner)
  back <- mizu_recv(p$peer, 5)
  expect_true(is_view(back))
  expect_identical(
    .Call(mizu:::mizu_zc_view_name, back[[1L]]),
    .Call(mizu:::mizu_zc_view_name, v)
  )
  expect_identical(back[[1L]][], x)
  expect_identical(back[[2L]][], inner[[2L]][])
})

test_that("an unparseable remote-leaf identifier is skipped at the wrap loan walk", {
  x <- runif(50000)
  fx <- remote_leaf_pair(x)
  poke_remote(fx, id = "zzz", len = length(x))
  mizu_send(fx[["p"]][["peer"]], fx[["yb"]]) # REF: the re-wrap walks loans
  v <- mizu_recv(fx[["p"]][["host"]], 5)
  expect_true(is_view(v))
  channel_end(fx[["p"]])
})

test_that("an SHM_RAW payload naming a vanished region fails the read", {
  mk <- function() {
    p <- channel_pair(arena_size = 0)
    mizu_send(p[["host"]], setNames(as.list(1:50), paste0("k", 1:50)))
    rw <- .Call(
      mizu:::mizu_region_open,
      .Call(mizu:::mizu_channel_stat, p[["host"]])[["name"]],
      TRUE
    )
    list(p = p, rw = rw)
  }
  f <- mk() # a well-formed name that does not exist (one hex digit flipped)
  id <- .Call(mizu:::mizu_peek, f[["rw"]], 512L + 16L, 19L)
  hex0 <- 512L + 16L + which(id == charToRaw("_"))[1L]
  b <- .Call(mizu:::mizu_peek, f[["rw"]], hex0, 1L)
  .Call(
    mizu:::mizu_poke,
    f[["rw"]],
    hex0,
    if (b == charToRaw("a")) charToRaw("b") else charToRaw("a")
  )
  expect_error(mizu_recv(f[["p"]][["peer"]], 5), "payload region vanished")
  channel_end(f[["p"]])

  f <- mk() # a name outside the region-name grammar
  .Call(mizu:::mizu_poke, f[["rw"]], 512L + 16L + 1L, charToRaw("x"))
  expect_error(mizu_recv(f[["p"]][["peer"]], 5), "payload region vanished")
  channel_end(f[["p"]])
})

test_that("an S4 data-part wrapper stages as a zero-copy view", {
  p <- channel_pair()
  on.exit(channel_end(p))
  methods::setClass("zcS4Num", contains = "numeric")
  on.exit(methods::removeClass("zcS4Num"), add = TRUE)
  x <- methods::new("zcS4Num", runif(400000))
  mizu_send(p[["host"]], x)
  y <- mizu_recv(p[["peer"]], 5)
  expect_s4_class(y, "zcS4Num")
  expect_identical(as.numeric(y), as.numeric(x))
})
