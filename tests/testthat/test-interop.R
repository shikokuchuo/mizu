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
  skip_if_not_installed("tibble")
  expect_error(
    mizu_send(p$host, tibble::tibble(x = 1:2)),
    class = "mizu_error_not_portable"
  )
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
    mizu_send(p$host, as.difftime(1, units = "days")),
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
