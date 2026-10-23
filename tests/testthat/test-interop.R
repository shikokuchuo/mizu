# The 'I' interchange codec: the R writer and builder (through the hooks
# and a foreign-hinted channel_pair) — the relay exactness the crosslang
# suite then pins over real peers. helper-interop.R has the notation
# parser; the corpus rows live in test-interop-corpus.R.

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

  # the condition's fields: path, reason, and the named-character remedy
  e <- tryCatch(mizu_send(p$host, c(a = "x")), error = function(e) e)
  expect_s3_class(e, "mizu_error_not_portable")
  expect_identical(e$reason, "a named atomic vector has no portable home")
  expect_identical(e$remedy, "as.list(x)")
})

test_that("the attr decline names the offending attributes", {
  p <- foreign_pair()
  on.exit(channel_end(p))

  expect_snapshot(
    mizu_send(p$host, ts(1:12, frequency = 4, start = c(2020, 1))),
    error = TRUE
  )
  # a whitelisted-name set in a wrong combination names no attribute
  expect_snapshot(
    mizu_send(p$host, structure(1, class = "Date", tzone = "UTC")),
    error = TRUE
  )
})

test_that("the decline diagnoses a frame's failing column", {
  methods::setClass("ixS4col", contains = "integer")
  on.exit(methods::removeClass("ixS4col"), add = TRUE)
  p <- foreign_pair()
  on.exit(channel_end(p))

  # esoph-shaped: an ordered-factor column names itself
  expect_snapshot(
    mizu_send(
      p$host,
      data.frame(agegp = ordered(c("25-34", "35-44")), ncases = 0:1)
    ),
    error = TRUE
  )
  # freeny-shaped: a ts column names its attributes at the column
  expect_snapshot(
    mizu_send(
      p$host,
      structure(
        list(y = ts(1:4, frequency = 4, start = c(2020, 1)), x = 1:4),
        class = "data.frame",
        row.names = c(NA_integer_, -4L)
      )
    ),
    error = TRUE
  )
  # POSIXlt, list and S4 columns name the column and the type
  expect_snapshot(
    mizu_send(
      p$host,
      structure(
        list(a = 1:2, t = as.POSIXlt(.POSIXct(c(0, 86400), tz = "UTC"))),
        class = "data.frame",
        row.names = c(NA_integer_, -2L)
      )
    ),
    error = TRUE
  )
  expect_snapshot(
    mizu_send(
      p$host,
      structure(
        list(a = 1:2, l = list(1, 2)),
        class = "data.frame",
        row.names = c(NA_integer_, -2L)
      )
    ),
    error = TRUE
  )
  expect_snapshot(
    mizu_send(
      p$host,
      structure(
        list(a = 1:2, s = methods::new("ixS4col", 3:4)),
        class = "data.frame",
        row.names = c(NA_integer_, -2L)
      )
    ),
    error = TRUE
  )
  expect_snapshot(
    mizu_send(
      p$host,
      structure(
        list(v = 1:4, m = matrix(letters[1:4], 2)),
        class = "data.frame",
        row.names = c(NA_integer_, -4L)
      )
    ),
    error = TRUE
  )
  # the named-atomic column keeps the reason but drops the remedy
  bad <- structure(
    list(a = 1:2, v = c(x = 1, y = 2)),
    class = "data.frame",
    row.names = c(NA_integer_, -2L)
  )
  expect_snapshot(mizu_send(p$host, bad), error = TRUE)
  e <- tryCatch(mizu_send(p$host, bad), error = function(e) e)
  expect_identical(e$reason, "a named atomic vector has no portable home")
  expect_identical(e$remedy, character(0))
  # the extended frame's failure path rides the same column walk
  expect_snapshot(
    mizu_send(
      p$host,
      structure(
        data.frame(grp = ordered(c("a", "b"))),
        class = c("tbl_df", "tbl", "data.frame")
      )
    ),
    error = TRUE
  )
})

test_that("the decline diagnoses the frame's own shape and keeps prior text", {
  methods::setClass("ixS4top", contains = "integer")
  on.exit(methods::removeClass("ixS4top"), add = TRUE)
  p <- foreign_pair()
  on.exit(channel_end(p))

  expect_snapshot(mizu_send(p$host, matrix(letters[1:4], 2)), error = TRUE)
  expect_snapshot(
    mizu_send(
      p$host,
      structure(data.frame(x = 1:2), class = c("data.frame", "foo"))
    ),
    error = TRUE
  )
  dfd <- data.frame(x = 1:2, y = 3:4)
  names(dfd) <- c("a", "a")
  expect_snapshot(mizu_send(p$host, dfd), error = TRUE)
  df <- data.frame(x = 1:3)
  attr(df, "row.names") <- c("a", NA, "c")
  expect_snapshot(mizu_send(p$host, df), error = TRUE)

  # unchanged text: the pre-existing specific and attr-naming declines
  expect_snapshot(mizu_send(p$host, c(a = "x")), error = TRUE)
  expect_snapshot(mizu_send(p$host, ordered(c("a", "b"))), error = TRUE)
  gd <- data.frame(x = 1:3)
  attr(gd, "formula") <- y ~ x
  expect_snapshot(mizu_send(p$host, gd), error = TRUE)
  expect_snapshot(mizu_send(p$host, methods::new("ixS4top", 1:3)), error = TRUE)

  # a top-level POSIXlt names its POSIXct rewrite
  lt <- as.POSIXlt(.POSIXct(c(0, 86400), tz = "UTC"))
  expect_snapshot(mizu_send(p$host, lt), error = TRUE)
  e <- tryCatch(mizu_send(p$host, lt), error = function(e) e)
  expect_identical(e$remedy, "as.POSIXct(x)")
})

test_that("the labeled shapes round-trip on a labeled peer", {
  p <- foreign_pair(caps = 130L) # ATTRS + LABELED
  on.exit(channel_end(p))

  dfr <- data.frame(x = 1:2)
  attr(dfr, "reference") <- "A1.4, p. 270"
  expect_identical(ix_rt(p, dfr), dfr)
  m <- matrix(1:4, 2, dimnames = list(c("a", "b"), c("c", "d")))
  expect_identical(ix_rt(p, m), m)
  tbl <- structure(
    data.frame(x = 1:2),
    class = c("tbl_df", "tbl", "data.frame")
  )
  expect_identical(ix_rt(p, tbl), tbl)
  expect_identical(ix_rt(p, c(a = 1, b = 2)), c(a = 1, b = 2))
  # extent-1 label positions: never the 0x04 scalar collapse
  expect_identical(ix_rt(p, table("a")), table("a"))
  expect_identical(
    ix_rt(p, structure(1, names = NA_character_)),
    structure(1, names = NA_character_)
  )
  expect_identical(
    ix_rt(p, array(1, 1L, list(NA_character_))),
    array(1, 1L, list(NA_character_))
  )
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
  # a no-home attribute list whose keys overflow the message buffer: the
  # error text truncates, the stream reader must not overrun its stack
  u64 <- function(x) {
    c(writeBin(as.integer(x), raw(4), endian = "little"), raw(4))
  }
  key <- strrep("k", 600)
  s <- c(
    charToRaw("I"),
    as.raw(1),
    as.raw(0x0f),
    as.raw(0x07),
    u64(0),
    as.raw(0x0d),
    u64(1),
    writeBin(600L, raw(4), endian = "little"),
    charToRaw(key),
    as.raw(0x00)
  )
  expect_snapshot(.Call(mizu:::mizu_interop_read_call, s), error = TRUE)
})

test_that("the writer's path tracking cannot overflow its buffer", {
  # a path segment at the boundary: "[[100000]]" needs ten bytes where the
  # path has room for eight — the push must clamp, not run the length past
  k <- strrep("a", 118)
  inner <- as.list(rep(1, 100000))
  inner[[100000]] <- list(b = 1)
  x <- structure(list(inner), names = k)
  s <- .Call(mizu:::mizu_interop_write_call, x)
  expect_identical(.Call(mizu:::mizu_interop_read_call, s), x)
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

test_that("corrupt dim and row.names shapes decline at the writer", {
  # a zero-length vector's dim product overflows its length mid-walk
  expect_snapshot(
    ix_write(structure(integer(0), dim = c(2L, 0L))),
    error = TRUE
  )
  # a compact row.names whose count is not the frame's
  df <- data.frame(x = 1:3)
  attr(df, "row.names") <- c(NA_integer_, 7L)
  expect_error(ix_write(df), "not portable")
  # row.names that is neither an integer nor a character vector: poked in
  # the serialized node's flags word — R's own assignment validates it
  rn <- c(1357911L, 2468013L, 11235813L)
  attr(df, "row.names") <- rn
  buf <- serialize(df, NULL)
  h <- grepRaw(
    writeBin(rn, raw(), size = 4L, endian = "big"),
    buf,
    fixed = TRUE
  )
  buf[h - 5L] <- as.raw(10L) # INTSXP -> LGLSXP
  df <- unserialize(buf)
  expect_error(ix_write(df), "not portable")
})

test_that("a non-ALTREP compact row.names qualifies with the 1:n verdict", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  df <- data.frame(x = 1:3)
  attr(df, "row.names") <- c(NA_integer_, 3L)
  expect_identical(ix_rt(p, df), data.frame(x = 1:3))
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

test_that("sign-flipped NAs build the foreign validity section (C1)", {
  # -x flips every NA's sign bit: the na_present gate must mask it like
  # mizu_ext_na_real_bits, or the region is stamped known-NA-free {0, -1}
  x <- runif(20000) # 160 KB: past the zc floor, attributed -> mizh_write
  x[c(2L, 4L)] <- NA
  x <- -x
  dim(x) <- c(10000L, 2L)
  p <- foreign_pair(caps = 2L) # MIZU_CAP_ATTRS
  got <- ix_rt(p, x)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  peek <- view_peek(got)
  hdr <- readBin(peek(40, 16), "integer", 4, 4, endian = "little")
  expect_gt(hdr[1], 0L) # a bitmap section, not the {0, -1} NA-free stamp
  expect_identical(hdr[3:4], c(2L, 0L))
  # NAs at 0-based 1 and 3: bits 1111 0101 (LSB first)
  expect_identical(peek(as.numeric(hdr[1]), 1), as.raw(0xf5))
  expect_identical(got[], x)
  channel_end(p)

  # mixed sign-flipped + regular NAs: the bitmap marks both
  y <- runif(20000)
  y[2L] <- NA
  y <- -y
  y[4L] <- NA_real_
  dim(y) <- c(10000L, 2L)
  p <- foreign_pair(caps = 2L)
  got <- ix_rt(p, y)
  peek <- view_peek(got)
  hdr <- readBin(peek(40, 16), "integer", 4, 4, endian = "little")
  expect_identical(hdr[3:4], c(2L, 0L))
  expect_identical(peek(as.numeric(hdr[1]), 1), as.raw(0xf5))
  expect_identical(got[], y)
  channel_end(p)

  # complex: a sign-flipped NA in either part counts
  z <- complex(10000, runif(10000), runif(10000))
  z[c(2L, 4L)] <- NA_complex_
  z <- -z
  dim(z) <- c(5000L, 2L) # 160 KB
  p <- foreign_pair(caps = 2L)
  got <- ix_rt(p, z)
  peek <- view_peek(got)
  hdr <- readBin(peek(40, 16), "integer", 4, 4, endian = "little")
  expect_identical(hdr[3:4], c(2L, 0L))
  expect_identical(peek(as.numeric(hdr[1]), 1), as.raw(0xf5))
  expect_identical(got[], z)
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

  # a string column gates like a top-level vector: latin1 takes the 'I'
  # copy (translated), bytes declines at send — never a MIZL leaf one
  # hop late
  ldf <- data.frame(
    x = runif(10000),
    s = iconv(rep("héllo", 10000), from = "UTF-8", to = "latin1")
  )
  p <- foreign_pair(caps = 7L)
  got <- ix_rt(p, ldf)
  expect_false(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got, ldf)
  channel_end(p)
  bdf <- data.frame(x = runif(10000), s = rep("héllo", 10000))
  Encoding(bdf[["s"]]) <- "bytes"
  p <- foreign_pair(caps = 7L)
  expect_error(mizu_send(p$host, bdf), class = "mizu_error_not_portable")
  channel_end(p)

  # a named vector past the floor has no shape: it raises at send
  nv <- setNames(rnorm(40000), paste0("n", seq_len(40000)))
  p <- foreign_pair(caps = 7L)
  expect_error(mizu_send(p$host, nv), class = "mizu_error_not_portable")
  channel_end(p)
})

test_that("the presized size pass aligns with the eligibility walk", {
  # a string-columned frame: the recorded column sums retire the size
  # pass's string walk
  df <- data.frame(
    s = sprintf("value-%05d", seq_len(20000)),
    b = seq_len(20000) + 0L,
    c = runif(20000)
  )
  p <- foreign_pair(caps = 7L)
  got <- ix_rt(p, df)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(as.data.frame(got[]), df)
  channel_end(p)

  # a lazy ALTREP column: costs the probe 0, the foreign write copies it
  # — the recording never covered it and the size pass doesn't ask
  df_alt <- data.frame(s = sprintf("v%05d", seq_len(20000)))
  df_alt[["n"]] <- seq_len(20000) # stays a compact ALTREP column
  p <- foreign_pair(caps = 7L)
  got <- ix_rt(p, df_alt)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(as.data.frame(got[]), df_alt)
  channel_end(p)

  # a list column: string leaves nested one level down — the generic
  # walk records them in the size pass's own pre-order
  df_lc <- data.frame(s = c("a", "b", "c"))
  df_lc[["lc"]] <- lapply(
    c("p", "q", "r"),
    function(tag) sprintf("%s%05d", tag, seq_len(20000))
  )
  p <- foreign_pair(caps = 7L)
  got <- ix_rt(p, df_lc)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[["s"]][], df_lc[["s"]])
  expect_identical(got[["lc"]][], df_lc[["lc"]])
  channel_end(p)

  # a view column crosses as a remote leaf: no local walk, no sum — the
  # string columns' recorded sums keep their positions
  q <- channel_pair()
  mizu_send(q$host, sprintf("view-%05d", seq_len(20000)))
  v <- mizu_recv(q$peer, 30)
  df_ref <- data.frame(
    s = sprintf("value-%05d", seq_len(20000)),
    b = seq_len(20000) + 0L
  )
  df_ref[["v"]] <- v
  p <- foreign_pair(caps = 23L) # MIZS | ATTRS | MIZL | MIZL_REF
  got <- ix_rt(p, df_ref)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[["s"]][], df_ref[["s"]])
  expect_identical(got[["b"]], df_ref[["b"]])
  expect_identical(got[["v"]][], v[])
  channel_end(p)
  channel_end(q)

  # past the recorder's capacity the plain size pass takes over
  big <- lapply(seq_len(130), function(i) sprintf("l%d-%04d", i, seq_len(600)))
  names(big) <- paste0("e", seq_len(130))
  p <- foreign_pair(caps = 7L)
  got <- ix_rt(p, big)
  expect_true(.Call(mizu:::mizu_zc_view_check, got))
  expect_identical(got[], big)
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
  # the hook mirrors the submit veneer's validation (the writer cannot
  # decline): a short seed read out of bounds before
  expect_snapshot(
    .Call(mizu:::mizu_interop_runner_call, "nm", 1, 5, 2L, 1),
    error = TRUE
  )
  expect_snapshot(
    .Call(mizu:::mizu_interop_runner_call, "nm", 1, c(1, NA), 2L, 1),
    error = TRUE
  )
  expect_snapshot(
    .Call(mizu:::mizu_interop_runner_call, 42, 1, NULL, 2L, 1),
    error = TRUE
  )
  expect_snapshot(
    .Call(mizu:::mizu_interop_runner_call, "nm", NA_real_, NULL, 2L, 1),
    error = TRUE
  )
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

test_that("the foreign string gate declines latin1 and bytes marks", {
  p <- foreign_pair(caps = 1L) # MIZS
  on.exit(channel_end(p))
  lx <- iconv(
    rep(c("héllo", "wörld"), length.out = 70000),
    from = "UTF-8",
    to = "latin1"
  )
  # a latin1 vector takes the 'I' copy, translated
  mizu_send(p$host, lx)
  expect_identical(mizu_recv(p$peer, 5), lx)
  bx <- rep("ok", 70000)
  bx[1] <- "héllo"
  Encoding(bx[1]) <- "bytes"
  expect_error(mizu_send(p$host, bx), class = "mizu_error_not_portable")
  # a native-marked string whose bytes are not valid UTF-8
  expect_error(
    mizu_send(p$host, rep(rawToChar(as.raw(c(0x41, 0x80))), 70000)),
    class = "mizu_error_not_portable"
  )
  bv <- "héllo"
  Encoding(bv) <- "bytes"
  expect_error(
    mizu_send(p$host, list(bv, 1L)),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, c("ok", bv)),
    class = "mizu_error_not_portable"
  )
})

test_that("temporal qualification declines unknown or missing units", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  expect_error(
    mizu_send(p$host, structure(1, class = "difftime", units = NA_character_)),
    class = "mizu_error_not_portable"
  )
})

test_that("frame row.names qualify per shape", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  # the compact form
  old <- structure(
    list(x = 1:3),
    class = "data.frame",
    row.names = c(NA_integer_, -3L)
  )
  mizu_send(p$host, old)
  expect_identical(mizu_recv(p$peer, 5), old)
  # explicit integer row.names, sequence and not
  seqdf <- structure(list(x = 1:3), class = "data.frame", row.names = 1:3)
  mizu_send(p$host, seqdf)
  expect_identical(mizu_recv(p$peer, 5), seqdf)
  shuf <- structure(
    list(x = 1:3),
    class = "data.frame",
    row.names = c(3L, 1L, 2L)
  )
  mizu_send(p$host, shuf)
  expect_identical(mizu_recv(p$peer, 5), shuf)
  # a short row.names vector declines
  bad <- structure(list(x = 1:3), class = "data.frame", row.names = 1:2)
  expect_error(mizu_send(p$host, bad), class = "mizu_error_not_portable")
  # character row.names with an NA decline
  nadf <- structure(
    list(x = 1:3),
    class = "data.frame",
    row.names = c("a", NA, "c")
  )
  expect_error(mizu_send(p$host, nadf), class = "mizu_error_not_portable")
})

test_that("the row.names sequence scan runs past the flat threshold", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  big <- structure(
    list(x = runif(9000)),
    class = "data.frame",
    row.names = c(9000:1)
  )
  mizu_send(p$host, big)
  expect_identical(mizu_recv(p$peer, 5), big)
  # a materialized 1:n scans as the sequence at both scan sizes
  small <- structure(
    list(x = 1:3),
    class = "data.frame",
    row.names = 1:3 + 0L
  )
  mizu_send(p$host, small)
  expect_identical(mizu_recv(p$peer, 5), small)
  seqbig <- structure(
    list(x = runif(9000)),
    class = "data.frame",
    row.names = seq_len(9000) + 0L
  )
  mizu_send(p$host, seqbig)
  expect_identical(mizu_recv(p$peer, 5), seqbig)
})

test_that("a COW-materialized view crosses foreign by value", {
  p <- channel_pair()
  on.exit(channel_end(p))
  z <- complex(70000, 1, 2)
  mizu_send(p$host, z)
  cv <- mizu_recv(p$peer, 5)
  cv[1] <- 0 + 0i
  r <- as.raw(seq_len(70000) %% 256L)
  mizu_send(p$host, r)
  rv <- mizu_recv(p$peer, 5)
  rv[1] <- as.raw(0)
  p2 <- foreign_pair(caps = 0L)
  on.exit(channel_end(p2), add = TRUE)
  mizu_send(p2$host, cv)
  expect_identical(mizu_recv(p2$peer, 5), cv)
  mizu_send(p2$host, rv)
  expect_identical(mizu_recv(p2$peer, 5), rv)
})

test_that("nested length-1 ALTREP scalars write off the region accessors", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  x <- list(1:1, seq(1, 1, by = 1), 1L)
  mizu_send(p$host, x)
  expect_identical(mizu_recv(p$peer, 5), x)
})

test_that("nested S4-bit objects decline", {
  methods::setClass("track", representation(x = "numeric"))
  on.exit(methods::removeClass("track"), add = TRUE)
  p <- foreign_pair()
  on.exit(channel_end(p))
  expect_error(
    mizu_send(p$host, list(methods::new("track", x = 1), 1L)),
    class = "mizu_error_not_portable"
  )
  # the bit on an ordinary vector, no formal class needed
  expect_error(
    mizu_send(p$host, list(asS4(list(1L)), 1L)),
    class = "mizu_error_not_portable"
  )
  expect_error(
    mizu_send(p$host, list(asS4("x"), 1L)),
    class = "mizu_error_not_portable"
  )
})

test_that("the map descriptor writer validates its arguments", {
  spec <- mizu_call("stats::median", 1:3)
  expect_error(
    .Call(mizu:::mizu_interop_map_desc_call, "notaspec", NULL, 2L),
    "malformed mizu_call spec"
  )
  expect_error(
    .Call(mizu:::mizu_interop_map_desc_call, spec, 42, 2L),
    "malformed map descriptor x"
  )
})

test_that("the err writer frames call text and falls back on odd classes", {
  cond <- errorCondition("boom", call = quote(worker_fn(1:3)))
  s <- .Call(mizu:::mizu_interop_write_err_call, cond, 240L)
  expect_true(grepRaw("worker_fn", s, fixed = TRUE) > 0)
  odd <- structure(list(message = "odd"), class = NA_character_)
  s2 <- .Call(mizu:::mizu_interop_write_err_call, odd, 240L)
  expect_true(length(s2) > 0)
})

test_that("a zero-row frame's blob row.names normalize", {
  p <- foreign_pair(caps = 6L) # ATTRS + MIZL
  on.exit(channel_end(p))
  z <- structure(
    list(x = numeric(0)),
    class = "data.frame",
    row.names = integer(0)
  )
  mizu_send(p$host, z)
  expect_identical(mizu_recv(p$peer, 5), z)
})

test_that("the task writer's size pass counts a zero-copy candidate", {
  spec <- list(
    code = "f",
    kind = 0L,
    positional = list(runif(40000)),
    named = list(),
    target = 2L,
    ident = 1
  )
  s <- ix_write_task(spec)
  expect_true(nchar(s) > 0)
})

test_that("the attr shape builder rejects malformed attribute sets", {
  u64 <- function(x) {
    stopifnot(x >= 0 && x < 2^53)
    lo <- x %% 2^32
    hi <- (x - lo) / 2^32
    if (lo >= 2^31) {
      lo <- lo - 2^32
    }
    c(
      writeBin(as.integer(lo), raw(4L), endian = "little"),
      writeBin(as.integer(hi), raw(4L), endian = "little")
    )
  }
  u32 <- function(x) writeBin(as.integer(x), raw(4L), endian = "little")
  i64 <- function(x) {
    neg <- x < 0
    x <- abs(x)
    lo <- x %% 2^32
    hi <- (x - lo) / 2^32
    if (neg) {
      lo <- 2^32 - lo
      hi <- 2^32 - 1 - hi
    }
    c(
      writeBin(
        as.integer(if (lo >= 2^31) lo - 2^32 else lo),
        raw(4L),
        endian = "little"
      ),
      writeBin(
        as.integer(if (hi >= 2^31) hi - 2^32 else hi),
        raw(4L),
        endian = "little"
      )
    )
  }
  s_str <- function(s) c(u32(nchar(s, "bytes")), charToRaw(s))
  intv <- function(v) {
    if (!length(v)) {
      return(c(as.raw(0x07), u64(0)))
    }
    c(as.raw(0x07), u64(length(v)), do.call(c, lapply(v, u32)))
  }
  realv <- function(v) {
    c(as.raw(0x08), u64(length(v)), writeBin(v, raw(0), size = 8L))
  }
  strv <- function(v) {
    c(
      as.raw(0x0b),
      u64(length(v)),
      do.call(
        c,
        lapply(v, function(s) {
          if (is.na(s)) u32(-1L) else s_str(s)
        })
      )
    )
  }
  i_list <- function(...) {
    xs <- list(...)
    c(as.raw(0x0c), u64(length(xs)), do.call(c, xs))
  }
  i_dict <- function(...) {
    xs <- list(...)
    items <- do.call(c, lapply(names(xs), function(k) c(s_str(k), xs[[k]])))
    c(as.raw(0x0d), u64(length(xs)), items)
  }
  attr_of <- function(item, dict) c(as.raw(0x0f), item, dict)
  magic <- as.raw(c(0x49, 0x01))
  df_shape <- function(cols, names, rn) {
    attr_of(
      do.call(i_list, cols),
      i_dict(names = strv(names), class = strv("data.frame"), row.names = rn)
    )
  }
  rd <- function(bytes) .Call(mizu:::mizu_interop_read_call, c(magic, bytes))
  # duplicated column names
  expect_error(
    rd(df_shape(list(intv(1:2)), c("x", "x"), intv(c(NA, -2)))),
    "no portable home"
  )
  # a list column
  expect_error(
    rd(df_shape(list(i_list(intv(1L))), "x", intv(c(NA, -1)))),
    "no portable home"
  )
  # a column carrying an out-of-shape attribute
  expect_error(
    rd(df_shape(
      list(attr_of(intv(1:2), i_dict(foo = c(as.raw(0x02), i64(1))))),
      "x",
      intv(c(NA, -2))
    )),
    "no portable home"
  )
  # column lengths disagreeing
  expect_error(
    rd(df_shape(list(intv(1:2), intv(1L)), c("a", "b"), intv(c(NA, -2)))),
    "no portable home"
  )
  # a compact row.names naming the wrong count
  expect_error(
    rd(df_shape(list(intv(1:2)), "x", intv(c(NA, -5)))),
    "no portable home"
  )
  # an NA in character row.names
  expect_error(
    rd(df_shape(list(intv(1:2)), "x", strv(c("a", NA)))),
    "no portable home"
  )
  # a dim product disagreeing with the length
  expect_error(
    rd(attr_of(
      intv(1:3),
      i_dict(dim = intv(c(2L, 2L)))
    )),
    "no portable home"
  )
  # an NA dim entry
  expect_error(
    rd(attr_of(
      intv(1:2),
      i_dict(dim = intv(c(2L, NA)))
    )),
    "no portable home"
  )
  # a tzone that is not length-1
  expect_error(
    rd(attr_of(
      realv(1),
      i_dict(
        class = strv(c("POSIXct", "POSIXt")),
        tzone = strv(c("a", "b"))
      )
    )),
    "no portable home"
  )
  # a factor's non-integer value
  expect_error(
    rd(attr_of(
      realv(1),
      i_dict(levels = strv("a"), class = strv("factor"))
    )),
    "no portable home"
  )
  # a dim-attributed column: homes itself, out of the frame's whitelist
  expect_error(
    rd(df_shape(
      list(attr_of(intv(c(1L, 2L)), i_dict(dim = intv(2L)))),
      "x",
      intv(c(NA, -2))
    )),
    "no portable home"
  )
  # row.names of an unexpected type
  expect_error(
    rd(df_shape(list(intv(1:2)), "x", realv(c(1, 2)))),
    "no portable home"
  )
  # short character row.names
  expect_error(
    rd(df_shape(list(intv(1:2)), "x", strv("a"))),
    "no portable home"
  )
  # a dim array over a character value
  expect_error(
    rd(attr_of(strv(c("a", "b")), i_dict(dim = intv(2L)))),
    "no portable home"
  )
})

test_that("a malformed foreign factor declines", {
  # the writer's contract the cursor does not cover, enforced at the
  # reader: levels unique and non-NA, codes 1..k or NA
  ix_factor <- function(codes, levels) {
    paste0(
      "49010f",
      ix_write(codes) |> substring(5),
      "0d0200000000000000",
      "060000006c6576656c73",
      ix_write(levels) |> substring(5),
      "05000000636c617373",
      ix_write("factor") |> substring(5)
    )
  }
  expect_snapshot(ix_read(ix_factor(1:2, c("a", NA))), error = TRUE)
  expect_snapshot(ix_read(ix_factor(1:2, c("a", "a"))), error = TRUE)
  expect_snapshot(ix_read(ix_factor(c(1L, 5L), c("a", "b"))), error = TRUE)
  # a clean factor still homes
  expect_identical(
    ix_read(ix_factor(c(1L, 2L, NA), c("a", "b"))),
    factor(c("a", "b", NA), levels = c("a", "b"))
  )
})

test_that("a foreign-identity task publishes the foreign result formats", {
  p <- pool_pair()
  on.exit(pool_end(p))
  spec <- list("base::stop", 0L, list("boom"), list())
  t <- .Call(mizu:::mizu_pool_submit_spec, p$ctrl, spec, Inf, c(3L, 8L))
  pool_step(p)
  e <- tryCatch(mizu_collect(t, 5), error = function(e) e)
  expect_s3_class(e, "mizu_error_remote")
  ok <- .Call(
    mizu:::mizu_pool_submit_spec,
    p$ctrl,
    list("base::paste", 0L, list("a", "b"), list(sep = "-")),
    Inf,
    c(3L, 8L)
  )
  pool_step(p)
  expect_identical(mizu_collect(ok, 5), "a-b")
  bad <- .Call(
    mizu:::mizu_pool_submit_spec,
    p$ctrl,
    list("if (", 1L, list(), list()),
    Inf,
    c(3L, 8L)
  )
  pool_step(p)
  e2 <- tryCatch(mizu_collect(bad, 5), error = function(e) e)
  expect_s3_class(e2, "mizu_error_remote")
})

test_that("a compact sequence crosses foreign by value", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, 1:100000)
  expect_identical(mizu_recv(p$peer, 5), 1:100000)
})

test_that("a spec task stages a fitting candidate as a by-reference region", {
  p <- pool_pair()
  on.exit(pool_end(p))
  x <- runif(40000)
  t <- mizu_submit_call(p$ctrl, mizu_call("base::sum", x))
  pool_step(p)
  expect_identical(mizu_collect(t, 5), sum(x))
})

test_that("a spec task argument re-sends a view by reference", {
  p <- pool_pair()
  on.exit(pool_end(p))
  ch <- channel_pair()
  on.exit(channel_end(ch), add = TRUE)
  x <- runif(70000)
  mizu_send(ch$host, x)
  v <- mizu_recv(ch$peer, 5)
  t <- mizu_submit_call(p$ctrl, mizu_call("base::sum", v))
  pool_step(p)
  expect_identical(mizu_collect(t, 5), sum(x))
})

test_that("a spec task falls back to a value write short of ref caps", {
  ch <- channel_pair()
  on.exit(channel_end(ch))
  s <- rep(c("alpha", "beta"), length.out = 70000)
  mizu_send(ch$host, s)
  v <- mizu_recv(ch$peer, 5)
  p <- pool_pair(ident = c(2L, 0L)) # an R pool advertising no capabilities
  on.exit(pool_end(p), add = TRUE)
  t <- mizu_submit_call(p$ctrl, mizu_call("base::paste0", v, collapse = "|"))
  pool_step(p)
  expect_identical(
    mizu_collect(t, 5),
    paste(rep(c("alpha", "beta"), length.out = 70000), collapse = "|")
  )
})

test_that("a dim-attributed view crosses foreign through the region accessors", {
  p <- channel_pair()
  on.exit(channel_end(p))
  z <- matrix(complex(70000, 1, 2), nrow = 100)
  mizu_send(p$host, z)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  p2 <- foreign_pair(caps = 0L) # no ATTRS: the dim blob rides the value writer
  on.exit(channel_end(p2), add = TRUE)
  mizu_send(p2$host, v)
  expect_identical(mizu_recv(p2$peer, 5), z)
  r <- matrix(as.raw(seq_len(70000) %% 256L), nrow = 100)
  mizu_send(p$host, r)
  rv <- mizu_recv(p$peer, 5)
  expect_true(is_view(rv))
  mizu_send(p2$host, rv)
  expect_identical(mizu_recv(p2$peer, 5), r)
})

test_that("an ALTREP header at the inline boundary flips to the count pass", {
  p <- foreign_pair()
  on.exit(channel_end(p))
  x <- list(strrep("x", 220), 1:100)
  mizu_send(p$host, x)
  expect_identical(mizu_recv(p$peer, 5), x)
})

test_that("the err writer falls back when a class string is not UTF-8", {
  k <- "héllo"
  Encoding(k) <- "bytes"
  cond <- structure(list(message = "odd"), class = k)
  s <- .Call(mizu:::mizu_interop_write_err_call, cond, 240L)
  expect_true(grepRaw("error", s, fixed = TRUE) > 0)
})

test_that("a remote error's index past int32 crosses as a double", {
  fx <- ix_hex_to_raw(ix_corpus()[["err-index"]])
  pos <- which(fx == as.raw(0x29))[1L]
  fx[pos:(pos + 7L)] <- c(as.raw(c(0L, 0L, 0L, 128L)), raw(4L))
  cnd <- .Call(mizu:::mizu_interop_read_call, fx)
  expect_identical(cnd[["index"]], 2^31 + 1)
})

test_that("a large named list round-trips through the dict key set", {
  x <- setNames(as.list(seq_len(200L)), paste0("k", seq_len(200L)))
  expect_identical(
    .Call(
      mizu:::mizu_interop_read_call,
      .Call(mizu:::mizu_interop_write_call, x)
    ),
    x
  )
})

test_that("a task stream read as a value is not a value", {
  b <- .Call(
    mizu:::mizu_interop_write_task_call,
    mizu_call("base::sqrt", 4),
    2L,
    0,
    FALSE
  )
  expect_error(
    .Call(mizu:::mizu_interop_read_call, b),
    "an interop task is not a value"
  )
})

test_that("a materialized 1:n row names vector writes compactly (foreign)", {
  df <- data.frame(x = c(1, 2, 3))
  attr(df, "row.names") <- 1:3 + 0L
  expect_identical(
    .Call(
      mizu:::mizu_interop_read_call,
      .Call(mizu:::mizu_interop_write_call, df)
    ),
    df
  )
  big <- data.frame(x = runif(9000)) # past the 8192-element region chunk
  attr(big, "row.names") <- seq_len(9000) + 0L
  expect_identical(
    .Call(
      mizu:::mizu_interop_read_call,
      .Call(mizu:::mizu_interop_write_call, big)
    ),
    big
  )
})

test_that("the cursor rejects malformed streams", {
  rd <- function(...) {
    .Call(mizu:::mizu_interop_read_call, as.raw(c(0x49, 0x01, ...)))
  }
  u32 <- function(x) {
    writeBin(as.integer(x), raw(0), size = 4L, endian = "little")
  }
  u64 <- function(x) c(u32(x), as.raw(rep(0L, 4L)))
  expect_error(
    .Call(mizu:::mizu_interop_read_call, as.raw(c(0x4a, 0x01))),
    "bad magic"
  )
  expect_error(
    .Call(mizu:::mizu_interop_read_call, as.raw(c(0x49, 0x02))),
    "newer format"
  )
  expect_error(rd(), "truncated")
  expect_error(rd(0x00, 0x00), "bytes past the one value")
  expect_error(rd(0x01), "truncated") # lgl1, no value
  expect_error(rd(0x01, 0x03), "lgl1 value past 2")
  expect_error(rd(0x02, 0x01), "truncated") # int, no i64
  expect_error(rd(0x03, 0x01), "truncated") # real, no double
  expect_error(rd(0x10, 0x01, 0x02), "truncated") # cplx, half a pair
  expect_error(rd(0x04), "truncated") # str, no length
  expect_error(rd(0x04, u32(-2L)), "string length below -1")
  expect_error(rd(0x04, u32(100L), 0x61), "truncated") # str, short body
  expect_error(rd(0x04, u32(1L), 0xff), "invalid UTF-8")
  expect_error(rd(0xfe), "newer format") # an unlisted value tag
  expect_error(rd(0x05, 0x01), "truncated") # bytes, no count
  expect_error(rd(0x05, u64(100), 0x01), "truncated") # bytes, short body
  expect_error(rd(0x07, 0x01), "truncated") # intv, no count
  expect_error(rd(0x0c, 0x01), "truncated") # list, no count
  expect_error(rd(0x0b, u64(1), 0x01), "truncated") # strv elt, no length
  expect_error(rd(0x0b, u64(1), u32(-2L)), "string length below -1")
  expect_error(rd(0x0b, u64(1), u32(1L), 0xff), "invalid UTF-8")
  expect_error(rd(0x11, 0x01), "truncated") # err, no flags
  expect_error(rd(0x11, 0x01, 0x00, 0x01), "truncated") # err index, short
  expect_error(rd(0x11, 0x00, 0x00, 0x01), "truncated") # err string, no length
  expect_error(rd(0x11, 0x00, 0x00, u32(-1L)), "err string is NA")
  expect_error(rd(0x11, 0x00, 0x00, u32(100L), 0x61), "truncated")
  expect_error(rd(0x11, 0x00, 0x00, u32(1L), 0xff), "invalid UTF-8")
  expect_error(rd(0x12, 0x02, 0x00), "truncated") # task, short header
  expect_error(rd(0x13), "truncated") # ref, no length
  expect_error(rd(0x99), "unsupported interop tag")
})

test_that("the err framer homes a non-language call as empty detail", {
  cond <- structure(
    list(message = "x", call = 42L),
    class = c("error", "condition")
  )
  b <- .Call(mizu:::mizu_interop_write_err_call, cond, 240L)
  expect_identical(.Call(mizu:::mizu_interop_read_call, b)[["detail"]], "")
})

test_that("an integer64 matrix crosses a foreign channel with its dim", {
  skip_if_not_installed("bit64")
  m <- bit64::as.integer64(seq_len(8500))
  dim(m) <- c(50L, 170L)
  p <- foreign_pair(caps = 2L)
  on.exit(channel_end(p))
  mizu_send(p$host, m)
  out <- mizu_recv(p$peer, 5)
  expect_true(is_view(out))
  expect_identical(out, m)
})

test_that("a raw vector crosses a foreign channel as a MIZH leaf", {
  p <- foreign_pair(caps = 2L)
  on.exit(channel_end(p))
  r <- as.raw(seq_len(300000) %% 256L)
  mizu_send(p$host, r)
  out <- mizu_recv(p$peer, 5)
  expect_true(is_view(out))
  expect_identical(out, r)
})

test_that("an int64 view whose blob diverged re-sends by value", {
  skip_if_not_installed("bit64")
  x <- bit64::as.integer64(seq_len(8500))
  p <- foreign_pair(caps = 2L)
  on.exit(channel_end(p))
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  rw <- .Call(mizu:::mizu_region_open, .Call(mizu:::mizu_zc_view_name, v), TRUE)
  blob <- c(
    as.raw(c(0x49, 0x01, 0x0d)),
    i64raw(1L),
    i64raw(3L)[1:4],
    charToRaw("dim"),
    as.raw(0x07),
    i64raw(2L),
    i64raw(50L)[1:4],
    i64raw(170L)[1:4]
  )
  .Call(mizu:::mizu_poke, rw, 16L, i64raw(length(blob)))
  .Call(mizu:::mizu_poke, rw, 64L + 8500L * 8L, blob)
  # the injected blob diverges from the view's attributes (the wire-tag
  # class alone): the REF gate declines, and the view's own attribute set
  # crosses by value — the region's blob does not leak in
  mizu_send(p$peer, v)
  out <- mizu_recv(p$host, 5)
  expect_identical(out, x)
})

test_that("a foreign frame with many string columns sizes off the plain pass", {
  df <- as.data.frame(lapply(
    paste0("col", 1:130),
    function(nm) rep(paste0(nm, "_", 1:60), 1)
  ))
  p <- foreign_pair(caps = 7L)
  on.exit(channel_end(p))
  mizu_send(p$host, df)
  expect_identical(mizu_recv(p$peer, 5), df)
})

test_that("a lazy ALTREP leaf in a foreign tree copies through", {
  x <- list(runif(10000), as.double(1:10000))
  p <- foreign_pair(caps = 7L)
  on.exit(channel_end(p))
  mizu_send(p$host, x)
  expect_identical(mizu_recv(p$peer, 5)[], x)
})
