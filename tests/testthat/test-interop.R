# The 'I' interchange codec: the R writer and builder (through the hooks
# and a foreign-hinted channel_pair) — the relay exactness the crosslang
# suite then pins over real peers. helper-interop.R has the notation
# parser; the corpus rows live in test-interop-corpus.R.

# A foreign channel: the host's peer reports Python with no capabilities,
# so the host end stages interop and the (R) peer end reads it back.
foreign_pair <- function(caps = 0L, ...) {
  channel_pair(ident = c(3L, as.integer(caps)), ...)
}

ix_rt <- function(p, x, timeout = 5) {
  mizu_send(p$host, x)
  mizu_recv(p$peer, timeout = timeout)
}

test_that("per-tag identical() round-trips over a foreign channel", {
  skip_if_not_installed("bit64")
  p <- foreign_pair()
  on.exit(channel_end(p))

  expect_identical(ix_rt(p, NULL), NULL)
  expect_identical(ix_rt(p, TRUE), TRUE)
  expect_identical(ix_rt(p, 1L), 1L)
  expect_identical(ix_rt(p, 1.5), 1.5)
  expect_identical(ix_rt(p, 1 + 2i), 1 + 2i)
  expect_identical(ix_rt(p, "héllo ✓"), "héllo ✓")
  expect_identical(ix_rt(p, as.raw(c(0x00, 0xff))), as.raw(c(0x00, 0xff)))
  expect_identical(ix_rt(p, c(TRUE, NA)), c(TRUE, NA))
  expect_identical(ix_rt(p, c(1L, NA, -3L)), c(1L, NA, -3L))
  expect_identical(ix_rt(p, c(1.5, NA)), c(1.5, NA))
  expect_identical(ix_rt(p, c("a", NA, "hé")), c("a", NA, "hé"))
  expect_identical(
    ix_rt(p, list(a = 1L, b = list(2.5, "x"))),
    list(a = 1L, b = list(2.5, "x"))
  )

  f <- factor(c("b", "a", NA), levels = c("a", "b"))
  expect_identical(ix_rt(p, f), f)
  df <- data.frame(
    n = c(1.5, 2.5),
    f = factor(c("a", NA)),
    i = bit64::as.integer64(c(5, NA)),
    r = as.raw(c(1, 2)),
    z = c(1 + 2i, 3 + 4i),
    s = c("x", "y")
  )
  expect_identical(ix_rt(p, df), df)
  dfr <- data.frame(x = 1:3, row.names = c(10L, 20L, 30L))
  expect_identical(ix_rt(p, dfr), dfr)
  dfc <- data.frame(x = 1:3, row.names = c("a", "b", "c"))
  expect_identical(ix_rt(p, dfc), dfc)
  df0 <- data.frame(a = integer(0), b = character(0))
  expect_identical(ix_rt(p, df0), df0)

  # latin1 strings, names and levels included: translated to UTF-8, and
  # identical() is encoding-insensitive
  lx <- iconv(c("héllo", "wörld"), from = "UTF-8", to = "latin1")
  expect_identical(ix_rt(p, lx), lx)
  ln <- iconv(c("namé", "lével"), from = "UTF-8", to = "latin1")
  lf <- factor(c("namé", "lével", "namé"), levels = ln)
  expect_identical(ix_rt(p, lf), lf)
  ll <- setNames(list(1L, 2L), ln)
  expect_identical(ix_rt(p, ll), ll)
  ldf <- data.frame(x = 1:2)
  names(ldf) <- ln[1L]
  expect_identical(ix_rt(p, ldf), ldf)
})

test_that("automatic row.names canonicalize off the ALTREP sequence", {
  p <- foreign_pair()
  on.exit(channel_end(p))

  # R >= 4.6 stores automatic row.names as an ALTREP compact sequence;
  # the writer reads the 1:n verdict off its info instead of scanning
  df <- data.frame(x = c(1, 2))
  expect_identical(ix_write(df), unname(ix_corpus()[["frame-1col"]]))
  expect_identical(ix_rt(p, df), df)

  # a compact sequence that is not 1:n writes as-is
  off <- data.frame(x = 1:3)
  row.names(off) <- 2:4
  ref <- data.frame(x = 1:3)
  attr(ref, "row.names") <- c(2L, 3L, 4L)
  expect_identical(ix_write(off), ix_write(ref))
  expect_identical(ix_rt(p, off), off)
})

test_that("the dim shape round-trips", {
  skip_if_not_installed("bit64")
  p <- foreign_pair()
  on.exit(channel_end(p))

  expect_identical(ix_rt(p, matrix(1:6, 2)), matrix(1:6, 2))
  expect_identical(ix_rt(p, array(1:8, c(2, 2, 2))), array(1:8, c(2, 2, 2)))
  im <- matrix(bit64::as.integer64(1:4), 2)
  expect_identical(ix_rt(p, im), im)
  dz <- array(integer(0), c(0, 3))
  expect_identical(ix_rt(p, dz), dz)
  expect_identical(ix_rt(p, 1:2), 1:2) # a length-1 dim: plain vector home
})

test_that("the temporal shapes round-trip", {
  p <- foreign_pair()
  on.exit(channel_end(p))

  dt <- as.Date("2022-01-15") + c(0, 1, NA)
  expect_identical(ix_rt(p, dt), dt)
  # μs-integral values relay bitwise, tzone UTC and absent
  pt <- as.POSIXct(1700000000 + c(0, 0.5, NA), tz = "UTC")
  expect_identical(ix_rt(p, pt), pt)
  ptn <- as.POSIXct(1700000000.25, tz = "")
  expect_identical(ix_rt(p, ptn), ptn)
  # a named zone survives in a frame column
  df <- data.frame(t = as.POSIXct(1700000000, tz = "America/New_York") + 0:1)
  expect_identical(ix_rt(p, df), df)
  # difftime: the value crosses in its declared units, NA kept
  td <- as.difftime(c(1.5, 2.25, NA), units = "mins")
  expect_identical(ix_rt(p, td), td)
  td0 <- as.difftime(numeric(0), units = "days")
  expect_identical(ix_rt(p, td0), td0)
  # a difftime frame column: same relay rule as the vector
  tdf <- data.frame(
    a = c(1.5, 2.5, 3.5),
    d = as.difftime(c(1, 2, NA), units = "hours")
  )
  expect_identical(ix_rt(p, tdf), tdf)
})

test_that("a difftime frame column crosses as MIZL on a foreign handle", {
  df <- data.frame(
    a = seq_len(300000) + 0,
    d = as.difftime(seq_len(300000) + 0.5, units = "secs")
  )
  p <- foreign_pair(caps = 6L) # ATTRS + MIZL
  on.exit(channel_end(p))
  got <- ix_rt(p, df)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[["a"]][], df[["a"]])
  expect_identical(got[["d"]][], df[["d"]])
})

test_that("NA mappings and the integer boundaries", {
  skip_if_not_installed("bit64")
  p <- foreign_pair()
  on.exit(channel_end(p))

  expect_identical(ix_rt(p, NA), NA)
  expect_identical(ix_rt(p, NA_integer_), NA_integer_)
  expect_identical(ix_rt(p, NA_real_), NA_real_)
  expect_identical(ix_rt(p, NA_character_), NA_character_)
  # 0x02 INT64_MIN reads back as NA_integer_
  expect_identical(ix_read(ix_write(NA_integer_)), NA_integer_)
  # -2^31 reads back as integer64, not NA
  x <- ix_read("49010200000080ffffffff")
  expect_true(bit64::is.integer64(x))
  expect_identical(x, bit64::as.integer64(-2147483648))
  # integer64 of length 1 and an integer64 NA nested in a list
  expect_identical(ix_rt(p, bit64::as.integer64(5)), bit64::as.integer64(5))
  expect_identical(
    ix_rt(p, list(bit64::as.integer64(NA), 1L)),
    list(bit64::as.integer64(NA), 1L)
  )
  expect_identical(
    ix_rt(p, bit64::as.integer64(c(2^53, NA))),
    bit64::as.integer64(c(2^53, NA))
  )
})

test_that("top-level length-1 atomics stage as scalar tags on a foreign handle", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  expect_identical(ix_rt(p, 1.5), 1.5)
  expect_identical(ix_rt(p, FALSE), FALSE)
  expect_identical(ix_rt(p, 42L), 42L)
  expect_identical(ix_rt(p, 2 + 3i), 2 + 3i)
  expect_identical(ix_rt(p, "s"), "s")
  # raw and integer64 keep RAWVEC on foreign handles too
  skip_if_not_installed("bit64")
  expect_identical(ix_rt(p, as.raw(7)), as.raw(7))
  expect_identical(ix_rt(p, bit64::as.integer64(9)), bit64::as.integer64(9))
})

test_that("same-language handles bypass the interop writer entirely", {
  p <- channel_pair()
  on.exit(channel_end(p))
  # environments and closures cross only on the private path (no 'I')
  e <- new.env()
  e$x <- 1
  mizu_send(p$host, e)
  expect_identical(mizu_recv(p$peer, timeout = 5)$x, 1)
  f <- function(x) x + 1
  mizu_send(p$host, f)
  expect_identical(mizu_recv(p$peer, timeout = 5)(1), 2)
  # named vectors, S4, ordered factors: all fine on the same-language path
  mizu_send(p$host, c(a = 1, b = 2))
  expect_identical(mizu_recv(p$peer, timeout = 5), c(a = 1, b = 2))
  of <- ordered(c("a", "b"))
  mizu_send(p$host, of)
  expect_identical(mizu_recv(p$peer, timeout = 5), of)
})

test_that("the decline set raises mizu_error_not_portable on a foreign handle", {
  skip_if_not_installed("bit64")
  p <- foreign_pair()
  on.exit(channel_end(p))

  expect_error(
    mizu_send(p$host, new.env()),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, function(x) x),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, quote(sym)),
    class = "mizu_error_not_portable"
  )
  dup <- structure(list(1, 2), names = c("a", "a"))
  expect_error(mizu_send(p$host, dup), class = "mizu_error_not_portable")
  expect_error(
    mizu_send(p$host, c(a = 1, b = 2)),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, array(1:4, c(2, 2), dimnames = list(c("a", "b"), NULL))),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, matrix(letters[1:4], 2)),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, ordered(c("a", "b"))),
    class = "mizu_error_not_portable"
  )
  tbl <- structure(
    data.frame(x = 1:2),
    class = c("tbl_df", "tbl", "data.frame")
  )
  expect_error(mizu_send(p$host, tbl), class = "mizu_error_not_portable")
  bs <- "héllo"
  Encoding(bs) <- "bytes"
  expect_error(mizu_send(p$host, bs), class = "mizu_error_not_portable")
  ni64 <- bit64::as.integer64(1:2)
  names(ni64) <- c("a", "b")
  expect_error(mizu_send(p$host, ni64), class = "mizu_error_not_portable")
  expect_error(
    mizu_send(p$host, as.POSIXlt(Sys.time())),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, structure(1, class = "difftime", units = "fortnights")),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, as.Date("2022-01-15") + 0.5),
    class = "mizu_error_not_portable"
  )
  dfx <- data.frame(x = 1:2)
  attr(dfx, "extra") <- TRUE
  expect_error(mizu_send(p$host, dfx), class = "mizu_error_not_portable")
  dfd <- data.frame(x = 1:2, y = 3:4)
  names(dfd) <- c("a", "a")
  expect_error(mizu_send(p$host, dfd), class = "mizu_error_not_portable")

  # the condition's fields: path, reason, and the named-vector remedy
  e <- tryCatch(mizu_send(p$host, c(a = 1)), error = function(e) e)
  expect_s3_class(e, "mizu_error_not_portable")
  expect_identical(e$reason, "a named atomic vector has no portable home")
  expect_identical(e$remedy, "as.list(x)")
})

test_that("the depth cap declines", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  x <- NULL
  for (i in seq_len(65)) {
    x <- list(x)
  }
  expect_error(mizu_send(p$host, x), class = "mizu_error_not_portable")
  e <- tryCatch(mizu_send(p$host, x), error = function(e) e)
  expect_match(e$reason, "depth")
})

test_that("corrupt and unknown streams raise informatively", {
  expect_snapshot(ix_read("49017f"), error = TRUE)
  expect_snapshot(ix_read("490200"), error = TRUE)
  expect_snapshot(ix_read("4901020102030405"), error = TRUE) # truncated
  # an unlisted first byte: the consumed decline condition
  r <- .Call(mizu:::mizu_stream_read_call, as.raw(0x7f))
  expect_s3_class(r, "mizu_error_python_payload")
  # an attr wrapping an attr (the cursor's error)
  expect_snapshot(ix_read("49010f0f"), error = TRUE)
  # a frame whose columns differ in length (the builder's error)
  expect_snapshot(ix_read(ix_corpus()[["err-frame-shape"]]), error = TRUE)
})

test_that("an attribute set outside the whitelist and a mismatched frame decline", {
  # builder-level: attribute set outside the whitelist
  bad <- paste0(
    "49010f",
    ix_write(1L) |> substring(5),
    "0d0200000000000000",
    "03000000666f6f",
    ix_write(2L) |> substring(5),
    "05000000636c617373",
    ix_write("bar") |> substring(5)
  )
  expect_snapshot(ix_read(bad), error = TRUE)
})

test_that("an unmarked native non-ASCII UTF-8 string round-trips", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  x <- "héllo ✓"
  Encoding(x) <- "unknown"
  expect_identical(ix_rt(p, x), x)
})

test_that("foreign sends build the validity section; same-language writes {0, 0}", {
  # a factor with NAs: the MIZH root's section maps the NA positions
  f <- factor(rep(c("a", NA, "b"), 20000), levels = c("a", "b"))
  p <- foreign_pair(caps = 2L) # MIZU_CAP_ATTRS
  got <- ix_rt(p, f)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  peek <- view_peek(got)
  hdr <- i64v(peek(40, 16))
  expect_gt(hdr[1], 0)
  expect_equal(hdr[1] %% 64, 0) # a 64-aligned bitmap
  expect_equal(hdr[2], 20000) # every third element NA
  # a, NA, b, a, NA, b, a, NA -> bits 1011 0110 (LSB first)
  expect_identical(peek(hdr[1], 1), as.raw(0x6d))
  expect_identical(got[], f)
  channel_end(p)

  # a clean factor: known-NA-free, no section materialized
  cf <- factor(rep(c("a", "b"), 20000), levels = c("a", "b"))
  p <- foreign_pair(caps = 2L)
  got <- ix_rt(p, cf)
  peek <- view_peek(got)
  expect_identical(
    readBin(peek(40, 16), "integer", 4, 4, endian = "little"),
    c(0L, 0L, -1L, -1L)
  )
  channel_end(p)

  # a frame with an NA column and a clean column: the MIZL header's table
  df <- data.frame(a = rep(c(1.5, NA), 10000), b = runif(20000))
  p <- foreign_pair(caps = 6L) # ATTRS + MIZL
  got <- ix_rt(p, df)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  peek <- view_peek(got)
  hdr <- i64v(peek(40, 16))
  expect_gt(hdr[1], 0)
  expect_equal(hdr[2], 10000)
  tab <- readBin(peek(hdr[1], 32), "integer", 8, 4, endian = "little")
  expect_equal(tab[3], 10000) # column a's null count
  expect_equal(tab[2], 0) # its bitmap offset's high half
  expect_identical(tab[5:8], c(0L, 0L, -1L, -1L)) # column b: known-NA-free
  expect_identical(as.data.frame(got[]), df)
  channel_end(p)

  # a clean frame: the header's known-NA-free, no table
  p <- foreign_pair(caps = 6L)
  got <- ix_rt(p, data.frame(b = runif(20000), c = runif(20000)))
  peek <- view_peek(got)
  expect_identical(
    readBin(peek(40, 16), "integer", 4, 4, endian = "little"),
    c(0L, 0L, -1L, -1L)
  )
  channel_end(p)

  # same-language sends never pay the build: the words stay {0, 0}
  q <- channel_pair(arena_size = 0)
  mizu_send(q[["host"]], df)
  got <- mizu_recv(q[["peer"]], 5)
  peek <- view_peek(got)
  expect_equal(i64v(peek(40, 16)), c(0, 0))
  channel_end(q)

  # a flat attribute-free vector (the core's reserve): {0, 0} even with NAs
  v <- runif(100000)
  v[5] <- NA
  p <- foreign_pair()
  got <- ix_rt(p, v)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  peek <- view_peek(got)
  expect_equal(i64v(peek(40, 16)), c(0, 0))
  channel_end(p)
})

test_that("a seq-columned frame crosses as MIZL on a foreign handle, sender compact", {
  df <- data.frame(id = 1:200000, x = runif(200000))
  p <- foreign_pair(caps = 7L) # MIZS + ATTRS + MIZL
  got <- ix_rt(p, df)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[["id"]][], df[["id"]])
  expect_identical(got[["x"]][], df[["x"]])
  # the sender's compact sequence was never expanded (a GET_REGION copy)
  expect_lt(length(serialize(df[["id"]], NULL)), 1000L)
  channel_end(p)
})

test_that("the foreign zero-copy filter gates the layouts by capability", {
  # a factor past the floor: SHM_VEC only when the peer advertises ATTRS
  big <- factor(rep(c("a", "b"), 20000), levels = c("a", "b"))
  p <- foreign_pair(caps = 0L)
  got <- ix_rt(p, big)
  expect_false(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got, big)
  channel_end(p)
  p <- foreign_pair(caps = 2L) # MIZU_CAP_ATTRS
  got <- ix_rt(p, big)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[], big[])
  channel_end(p)

  # a dim array past the floor: same ATTRS gate
  m <- matrix(rnorm(10000), 100)
  p <- foreign_pair(caps = 2L)
  got <- ix_rt(p, m)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[], m[])
  channel_end(p)

  # a plain frame: the ATTRS + MIZL conjunction
  df <- data.frame(x = rnorm(10000), y = rnorm(10000))
  p <- foreign_pair(caps = 2L) # ATTRS, no MIZL: an attr copy
  got <- ix_rt(p, df)
  expect_false(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got, df)
  channel_end(p)
  p <- foreign_pair(caps = 6L) # ATTRS + MIZL: a region
  got <- ix_rt(p, df)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(as.data.frame(got[]), df)
  channel_end(p)
  # a string-columned frame adds MIZS to the conjunction
  dfs <- data.frame(x = rnorm(10000), s = rep("a", 10000))
  p <- foreign_pair(caps = 6L) # ATTRS + MIZL, no MIZS: an attr copy
  got <- ix_rt(p, dfs)
  expect_false(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got, dfs)
  channel_end(p)

  # a MIZS-eligible string vector: only with the MIZS bit
  sv <- rep(c("alpha", "beta"), 20000)
  p <- foreign_pair(caps = 0L)
  got <- ix_rt(p, sv)
  expect_false(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got, sv)
  channel_end(p)
  p <- foreign_pair(caps = 1L) # MIZU_CAP_MIZS
  got <- ix_rt(p, sv)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[], sv[])
  channel_end(p)

  # a latin1 string vector past the floor crosses as a translated 'I'
  # copy, never MIZS; a bytes-marked one raises at send
  lv <- iconv(rep("héllo", 20000), from = "UTF-8", to = "latin1")
  p <- foreign_pair(caps = 1L)
  got <- ix_rt(p, lv)
  expect_false(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got, lv)
  channel_end(p)
  bv <- rep("héllo", 20000)
  Encoding(bv) <- "bytes"
  p <- foreign_pair(caps = 1L)
  expect_error(mizu_send(p$host, bv), class = "mizu_error_not_portable")
  channel_end(p)

  # a named vector past the floor has no shape: it raises at send
  nv <- setNames(rnorm(40000), paste0("n", seq_len(40000)))
  p <- foreign_pair(caps = 7L)
  expect_error(mizu_send(p$host, nv), class = "mizu_error_not_portable")
  channel_end(p)
})

test_that("interop traffic keeps the keeperless/reap discipline", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  ko <- function() .Call(mizu:::mizu_channel_keep_out, p$host)
  n0 <- ko()
  # within the inline budget: keeperless, no retain-table growth
  x <- list(a = 1L, b = "s")
  for (i in 1:4) {
    mizu_send(p$host, x)
  }
  expect_identical(ko(), n0)
  for (i in 1:4) {
    expect_identical(mizu_recv(p$peer, timeout = 5), x)
  }
  # past the budget but within the arena: one ARENA chunk, still no keeper
  big <- paste(rep("x", 500), collapse = "")
  mizu_send(p$host, big)
  expect_identical(ko(), n0)
  expect_identical(mizu_recv(p$peer, timeout = 5), big)
  # past the arena: one spill retain, reaped on the verb after consumption
  huge <- paste(rep("x", 50000), collapse = "")
  mizu_send(p$host, huge)
  expect_identical(ko(), n0 + 1L)
  expect_identical(mizu_recv(p$peer, timeout = 5), huge)
  mizu_send(p$host, "y") # the reap trigger
  expect_identical(ko(), n0)
})

test_that("ALTREP crosses by value on foreign handles", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  # a compact-sequence vector below the floor: an 'I' copy
  x <- 1:100000
  got <- ix_rt(p, x)
  expect_identical(got, x)
  expect_lt(length(serialize(x, NULL)), 1000L) # the sender is still compact
  # same-handle staging keeps the private codec (R->R compactness)
  q <- channel_pair()
  mizu_send(q$host, x)
  got2 <- mizu_recv(q$peer, timeout = 5)
  expect_identical(got2, x)
  channel_end(q)
})

test_that("a compact-sequence vector past the floor crosses as MIZH", {
  x <- 1:1e7
  p <- foreign_pair()
  got <- ix_rt(p, x)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[], x)
  expect_lt(length(serialize(x, NULL)), 1000L) # never expanded on the sender
  channel_end(p)
})

test_that("a seq-columned data.frame crosses foreign", {
  df <- data.frame(x = 1:1000, y = 1000:1)
  p <- foreign_pair()
  got <- ix_rt(p, df)
  expect_identical(got, df)
  channel_end(p)
})

test_that("a nested mizu view crosses by value", {
  x <- 1:1e6
  p <- foreign_pair()
  got <- ix_rt(p, x) # a view on the peer side
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  # send the received view back nested in a list: by value
  mizu_send(p$host, list(got, 1L))
  back <- mizu_recv(p$peer, timeout = 10)
  expect_identical(back, list(x, 1L))
  channel_end(p)
})

test_that("integer64 keeps RAWVEC at top level past the floor", {
  skip_if_not_installed("bit64")
  x <- bit64::as.integer64(seq_len(100000))
  p <- foreign_pair()
  got <- ix_rt(p, x)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[], x[])
  channel_end(p)
})

test_that("a class-only integer64 view re-crosses a foreign handle", {
  skip_if_not_installed("bit64")
  x <- bit64::as.integer64(seq_len(100000))
  p <- foreign_pair() # caps = 0: the class rides the wire tag, baseline
  on.exit(channel_end(p))
  mizu_send(p$peer, x)
  v <- mizu_recv(p$host, timeout = 5)
  expect_true(.Call(mizu:::mizu_zc_view_check, v))
  mizu_send(p$host, v) # the foreign end echoes by reference (REF)
  got <- mizu_recv(p$peer, timeout = 5)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[], x[])
  # materialized (COW'd), the same echo crosses by value as 0x0e
  mizu_send(p$peer, x)
  v <- mizu_recv(p$host, timeout = 5)
  v[1L] <- bit64::as.integer64(0L)
  mizu_send(p$host, v)
  got <- mizu_recv(p$peer, timeout = 5)
  expected <- x
  expected[1L] <- bit64::as.integer64(0L)
  expect_identical(got, expected)
})

test_that("the err tag reads as a mizu_error_remote value, index 1-based", {
  e <- ix_read(
    "49011100000a00000056616c75654572726f7204000000626f6f6d00000000"
  )
  expect_s3_class(e, "mizu_error_remote")
  expect_s3_class(e, "mizu_error")
  expect_identical(e[["remote_type"]], "ValueError")
  expect_identical(e[["message"]], "boom")
  expect_identical(e[["detail"]], "")
  ei <- ix_read(
    "490111010029000000000000000b000000576f726b65724572726f720e000000656c656d656e74206661696c656400000000"
  )
  expect_identical(ei[["index"]], 42L) # wire 41, 0-based
})

test_that("the err framer writes class, message and call text, bounded", {
  cond <- errorCondition(
    "region create failed",
    call = quote(f(x)),
    class = c("mizu_error_shm", "mizu_error")
  )
  e <- ix_read(ix_write_err(cond))
  expect_identical(e[["remote_type"]], "mizu_error_shm")
  expect_identical(e[["message"]], "region create failed")
  expect_identical(e[["detail"]], "f(x)")
  # the frame fits the slot by construction: strings truncate at shares
  long <- errorCondition(strrep("m", 500L), class = "custom_error")
  b <- .Call(mizu:::mizu_interop_write_err_call, long, 48L)
  expect_lte(length(b), 48L)
  e2 <- .Call(mizu:::mizu_interop_read_call, b)
  expect_identical(e2[["remote_type"]], "custom_error")
  expect_identical(nchar(e2[["message"]], type = "bytes"), 11L)
  # a multibyte string cuts at a character boundary
  uni <- errorCondition(strrep("é", 100L), class = "custom_error")
  b3 <- .Call(mizu:::mizu_interop_write_err_call, uni, 60L)
  expect_lte(length(b3), 60L)
  expect_identical(e2[["remote_type"]], "custom_error")
  m <- .Call(mizu:::mizu_interop_read_call, b3)[["message"]]
  expect_identical(m, strrep("é", 11L))
})

test_that("a remote error re-frames keeping its origin fields", {
  hex <- "49011100000a00000056616c75654572726f7204000000626f6f6d00000000"
  expect_identical(ix_write_err(ix_read(hex)), hex)
  hex_idx <- "490111010029000000000000000b000000576f726b65724572726f720e000000656c656d656e74206661696c656400000000"
  expect_identical(ix_write_err(ix_read(hex_idx)), hex_idx)
})

test_that("an err send crosses as a value on foreign and R channels alike", {
  cond <- errorCondition("boom", class = "custom_error")
  p <- foreign_pair()
  on.exit(channel_end(p))
  expect_true(.Call(mizu:::mizu_channel_send_error, p$peer, cond))
  e <- mizu_recv(p$host, timeout = 5)
  expect_s3_class(e, "mizu_error_remote")
  expect_identical(e[["message"]], "boom")
  expect_identical(e[["remote_type"]], "custom_error")
  # same-language: the pointer match bypasses the private codec, so the
  # host receives the same value a foreign peer would
  p2 <- channel_pair()
  on.exit(channel_end(p2))
  expect_true(.Call(mizu:::mizu_channel_send_error, p2$peer, cond))
  e2 <- mizu_recv(p2$host, timeout = 5)
  expect_s3_class(e2, "mizu_error_remote")
  expect_identical(e2[["remote_type"]], "custom_error")
  # a second send of the same condition stages as an ordinary value again
  mizu_send(p2$peer, 1L)
  expect_identical(mizu_recv(p2$host, timeout = 5), 1L)
})

test_that("the writer declines strings that are not writable as UTF-8", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  expect_error(
    mizu_send(p$host, rawToChar(as.raw(0x80))),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, rawToChar(as.raw(c(0xf0, 0x80, 0x80, 0x80)))),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, rawToChar(as.raw(c(0xf5, 0x80, 0x80, 0x80)))),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, c("ok", rawToChar(as.raw(0xc3)))),
    class = "mizu_error_not_portable"
  )
})

test_that("the writer declines non-portable frames and S4 objects", {
  methods::setClass("ixS4", contains = "integer")
  p <- foreign_pair()
  on.exit(channel_end(p))
  expect_error(
    mizu_send(p$host, data.frame()),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, data.frame(x = I(list(1, 2)))),
    class = "mizu_error_not_portable"
  )
  df <- data.frame(x = 1:3)
  attr(df, "row.names") <- c("a", "b") # length mismatch
  expect_error(mizu_send(p$host, df), class = "mizu_error_not_portable")
  expect_error(
    mizu_send(p$host, methods::new("ixS4", 1:3)),
    class = "mizu_error_not_portable"
  )
})

test_that("chunked seq scans and ALTREP regions write through", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  d <- structure(as.double(1:600), class = "Date") # ALTREP, past the chunk
  mizu_send(p$host, d)
  expect_identical(mizu_recv(p$peer, 5), d)
  big <- data.frame(x = seq_len(9000L)) # non-1:n row.names, past the chunk
  attr(big, "row.names") <- c(2L, seq_len(8999L))
  mizu_send(p$host, big)
  expect_identical(mizu_recv(p$peer, 5), big)
  x <- as.double(1:1e6) # compact real sequence: the GET_REGION write
  mizu_send(p$host, x)
  expect_identical(mizu_recv(p$peer, 5), x)
})

test_that("dict keys and task fields decline at the writer", {
  expect_error(
    ix_write(structure(list(1), names = NA_character_)),
    "not portable"
  )
  task <- list(
    code = "base::sqrt",
    kind = 0L,
    target = 2L,
    ident = 1,
    positional = list(2),
    named = list()
  )
  bad <- rawToChar(as.raw(0x80))
  expect_error(
    ix_write_task(modifyList(task, list(code = bad))),
    "not portable"
  )
  named_bad <- structure(list(1), names = rawToChar(as.raw(0x80)))
  expect_error(
    ix_write_task(modifyList(task, list(named = named_bad))),
    "not portable"
  )
  expect_error(
    ix_write_task(modifyList(task, list(code = 42))),
    "malformed mizu_call spec"
  )
})

test_that("the err writer degrades and annotates", {
  e1 <- ix_read(ix_write_err(errorCondition("boom", call = NULL)))
  expect_identical(e1[["remote_type"]], "error")
  expect_identical(e1[["detail"]], "")
  e2 <- ix_read(ix_write_err(structure(
    list(message = "m", call = NULL),
    class = character(0)
  )))
  expect_identical(e2[["remote_type"]], "error")
  e3 <- ix_read(ix_write_err(structure(
    list(message = "m", call = NULL),
    class = c(NA_character_, "error", "condition")
  )))
  expect_identical(e3[["remote_type"]], "error")
  e4 <- ix_read(ix_write_err(structure(
    list(message = "m", call = NULL, mizu_map_index = 4L),
    class = c("error", "condition")
  )))
  expect_identical(e4[["index"]], 4L)
  e5 <- ix_read(ix_write_err(structure(
    list(message = "m", call = NULL),
    class = c("mizu_error_remote", "mizu_error", "error", "condition")
  )))
  expect_identical(e5[["remote_type"]], "mizu_error_remote")
})

test_that("malformed task streams reject informatively", {
  good <- ix_write_task(list(
    code = "base::sqrt",
    kind = 0L,
    target = 2L,
    ident = 1,
    positional = list(2),
    named = list(a1 = 1, a2 = 2)
  ))
  r <- ix_hex_to_raw(good)
  dup <- r
  dup[[tail(which(dup == as.raw(0x31)), 1L)]] <- as.raw(0x32) # "a1" -> "a2"
  expect_error(ix_read_task(ix_raw_to_hex(dup)), "duplicate")
  expect_error(ix_read_task(paste0(good, "00")), "mizu:")
  kind <- r
  kind[[5L]] <- as.raw(0x09) # the kind byte: 0x49, ?, 0x12, target, kind
  expect_error(ix_read_task(ix_raw_to_hex(kind)), "mizu:")
  expect_error(ix_read_task(ix_write(42L)), "mizu:")
})

test_that("the runner stream writes and rejects as a plain task", {
  w <- .Call(mizu:::mizu_interop_runner_call, "/mizu_1_2", 1, NULL, 2L, 1)
  expect_identical(w[[1L]], as.raw(0x49)) # 'I' magic
  expect_identical(w[[3L]], as.raw(0x12)) # the task tag
  expect_identical(w[[5L]], as.raw(0x02)) # kind 2: a map runner
  expect_error(ix_read_task(ix_raw_to_hex(w)), "mizu:")
})

test_that("the stream read dispatches, declines and fails informatively", {
  x <- list(a = 1:3, b = "s")
  expect_identical(
    .Call(mizu:::mizu_stream_read_call, ix_hex_to_raw(ix_write(x))),
    x
  )
  r <- .Call(mizu:::mizu_stream_read_call, as.raw(c(0x50, 0x01, 0x02)))
  expect_s3_class(r, "mizu_error_python_payload")
  expect_error(.Call(mizu:::mizu_stream_read_call, raw(0)), "mizu:")
})
