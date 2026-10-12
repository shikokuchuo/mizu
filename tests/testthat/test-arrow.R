test_that("a frame stages as one region past the gate, views at receive", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(
    x = runif(6000),
    s = rep(c("alpha", "beta", NA), length.out = 6000)
  )
  mizu_send(p$host, nanoarrow::as_nanoarrow_array_stream(df))
  out <- mizu_recv(p$peer, 5)
  expect_s3_class(out, "data.frame")
  expect_true(is_view(out))
  expect_true(is_view(out$x))
  expect_true(is_view(out$s))
  expect_identical(out, df)
})

test_that("the mapped families round-trip with NAs", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  m <- arrow_matrix()
  mizu_send(p$host, m$tab)
  expect_identical(mizu_recv(p$peer, 5), m$want)
})

test_that("a small table converts and claims no region (single-pass)", {
  skip_if_not_installed("arrow")
  df <- data.frame(a = 1:3, b = c("x", "y", NA))
  p <- channel_pair()
  on.exit(channel_end(p))
  rdr <- arrow::RecordBatchReader$create(arrow::record_batch(df))
  mizu_send(p$host, rdr)
  out <- mizu_recv(p$peer, 5)
  expect_identical(out, df)
  expect_false(is_view(out$a))
  p2 <- channel_pair()
  on.exit(channel_end(p2), add = TRUE)
  mizu_send(p2$host, nanoarrow::as_nanoarrow_array_stream(df))
  out2 <- mizu_recv(p2$peer, 5)
  expect_identical(out2, df)
  expect_false(is_view(out2$a))
})

test_that("a non-struct nanoarrow_array arrives as a vector", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, nanoarrow::as_nanoarrow_array(c(1L, NA, 3L)))
  expect_identical(mizu_recv(p$peer, 5), c(1L, NA, 3L))
  mizu_send(p$host, nanoarrow::as_nanoarrow_array(c("a", NA, "z")))
  expect_identical(mizu_recv(p$peer, 5), c("a", NA, "z"))
  f <- factor(c("b", "a", NA), levels = c("a", "b"))
  mizu_send(p$host, nanoarrow::as_nanoarrow_array(f))
  expect_identical(mizu_recv(p$peer, 5), f)
})

test_that("a bare integer64 array keeps its class", {
  skip_if_not_installed("bit64")
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- bit64::as.integer64(c(1, 2, NA))
  mizu_send(p$host, nanoarrow::as_nanoarrow_array(x))
  expect_identical(mizu_recv(p$peer, 5), x)
  big <- bit64::as.integer64(seq_len(40000))
  mizu_send(p$host, arrow::arrow_table(v = big))
  out <- mizu_recv(p$peer, 5)
  expect_true(is_view(out$v))
  expect_identical(out, data.frame(v = big))
})

test_that("an ordered dictionary array keeps its class", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  of <- ordered(c("b", "a"), levels = c("a", "b"))
  mizu_send(p$host, nanoarrow::as_nanoarrow_array(arrow::Array$create(of)))
  expect_identical(mizu_recv(p$peer, 5), of)
})

test_that("a zero-row table arrives empty", {
  skip_if_not_installed("arrow")
  z <- data.frame(
    f = factor(character(0), levels = c("a", "b")),
    x = integer(0),
    d = as.Date(character(0))
  )
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, arrow::arrow_table(z))
  out <- mizu_recv(p$peer, 5)
  expect_identical(nrow(out), 0L)
  expect_identical(levels(out$f), character(0)) # arrow exports no dictionary
  expect_s3_class(out$d, "Date")
  p2 <- channel_pair()
  on.exit(channel_end(p2), add = TRUE)
  mizu_send(p2$host, nanoarrow::as_nanoarrow_array_stream(z))
  expect_identical(mizu_recv(p2$peer, 5), z)
})

test_that("NaN stays NaN across the frame", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(x = c(NaN, NA, 1.5), p = runif(6000))
  mizu_send(p$host, arrow::arrow_table(df))
  out <- mizu_recv(p$peer, 5)
  expect_true(is.nan(out$x[1]))
  expect_true(is.na(out$x[2]) && !is.nan(out$x[2]))
})

test_that("the conversion counts warn at send", {
  skip_if_not_installed("arrow")
  uv <- bit64::as.integer64(rep(1, 6000))
  uv[2] <- bit64::as.integer64("9223372036854775807")
  p <- channel_pair()
  on.exit(channel_end(p))
  tab <- arrow::arrow_table(u = arrow::Array$create(uv, type = arrow::uint64()))
  ws <- grab_warnings(mizu_send(p$host, tab))
  expect_match(ws, "beyond ...2\\^53 convert to NA", all = FALSE)
  expect_identical(mizu_recv(p$peer, 5)$u[1:3], c(1, NA_real_, 1))
  iv <- rep(1, 6000)
  iv[2] <- -2147483648
  iv[3] <- NA_real_
  p2 <- channel_pair()
  on.exit(channel_end(p2), add = TRUE)
  tab2 <- arrow::arrow_table(
    i = arrow::Array$create(iv, type = arrow::int32()),
    p = runif(6000)
  )
  ws2 <- grab_warnings(mizu_send(p2$host, tab2))
  expect_match(ws2, "-2147483648 read as NA_integer_", all = FALSE)
  expect_identical(
    mizu_recv(p2$peer, 5)$i[1:3],
    c(1L, NA_integer_, NA_integer_)
  )
})

test_that("declines name the column", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  dec <- arrow::arrow_table(
    d = arrow::Array$create(1.25, type = arrow::decimal(5, 2))
  )
  expect_snapshot(mizu_send(p$host, dec), error = TRUE)
  ord <- arrow::arrow_table(data.frame(o = ordered(c("a", "b"))))
  expect_snapshot(mizu_send(p$host, ord), error = TRUE)
  u8 <- arrow::arrow_table(
    u = arrow::Array$create(c(1L, NA))$cast(arrow::uint8())
  )
  expect_error(
    mizu_send(p$host, u8),
    class = "mizu_error_not_portable"
  )
  expect_snapshot(mizu_send(p$host, u8), error = TRUE)
})

test_that("a decline under gctorture leaves the handle consistent", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  tab <- arrow::arrow_table(
    d = arrow::Array$create(1.25, type = arrow::decimal(5, 2))
  )
  gctorture()
  expect_error(mizu_send(p$host, tab), class = "mizu_error_not_portable")
  gctorture(FALSE)
  gc()
  mizu_send(p$host, 1:5)
  expect_identical(mizu_recv(p$peer, 5), 1:5)
})

test_that("chunked batches round-trip", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(
    x = as.double(seq_len(6000)),
    s = rep(c("a", "b", NA), length.out = 6000)
  )
  rbs <- lapply(0:3, function(k) {
    arrow::record_batch(df[(k * 1500 + 1):((k + 1) * 1500), ])
  })
  mizu_send(p$host, do.call(arrow::RecordBatchReader$create, rbs))
  expect_identical(mizu_recv(p$peer, 5), df)
})

test_that("a capable foreign peer takes the frame region; view_peek smoke", {
  skip_if_not_installed("arrow")
  p <- foreign_pair(caps = 7L) # MIZS + ATTRS + MIZL
  on.exit(channel_end(p))
  df <- data.frame(x = runif(6000), s = rep(c("a", "b", NA), length.out = 6000))
  mizu_send(p$host, arrow::arrow_table(df))
  out <- mizu_recv(p$peer, 5)
  expect_true(is_view(out))
  expect_identical(out, df)
  peek <- view_peek(out)
  expect_identical(
    readBin(peek(0, 4), "integer", endian = "little"),
    0x4D495A4CL
  )
})

test_that("a caps-less foreign peer takes the conversion bail", {
  skip_if_not_installed("arrow")
  p <- foreign_pair(caps = 0L)
  on.exit(channel_end(p))
  df <- data.frame(
    x = runif(6000),
    s = rep(c("a", "b", NA), length.out = 6000),
    f = factor(rep(c("a", "b", NA), length.out = 6000))
  )
  mizu_send(p$host, arrow::arrow_table(df))
  out <- mizu_recv(p$peer, 5)
  expect_identical(out, df)
  expect_false(is_view(out$x))
})

test_that("mizu_as_arrow borrows the borrowable columns", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(x = runif(6000), i = seq_len(6000) + 0L,
                   s = rep(c("alpha", "beta", NA), length.out = 6000))
  df$x[2] <- NA
  df$i[3] <- NA
  mizu_send(p$host, arrow::arrow_table(df))
  out <- mizu_recv(p$peer, 5)
  st <- mizu_as_arrow(out)
  expect_s3_class(st, "nanoarrow_array_stream")
  tab <- arrow::as_arrow_table(st)
  types <- vapply(tab$schema$fields, function(f) f$type$ToString(), "")
  expect_identical(unname(types), c("double", "int32", "large_string"))
  back <- as.data.frame(tab)
  expect_identical(back, df)
})

test_that("mizu_as_arrow holds the region past the frame", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(x = runif(6000), s = rep(c("a", "b", NA), length.out = 6000))
  mizu_send(p$host, arrow::arrow_table(df))
  out <- mizu_recv(p$peer, 5)
  v <- out$x # a second view on the same region: keeps the root alive
  base <- rc_of(v)[1L]
  st <- mizu_as_arrow(out)
  expect_identical(rc_of(v)[1L], base + 1L)
  rm(out, st)
  gc()
  gc()
  gc() # the release cascade runs one generation per pass
  expect_identical(rc_of(v)[1L], base)
})

test_that("mizu_as_arrow converts the copy-path classes", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  n <- 6000
  df <- data.frame(
    f = factor(rep(c("a", "b", NA), length.out = n)),
    d = as.Date("2024-01-01") + seq_len(n) %% 100,
    t = as.POSIXct("2024-01-01", tz = "UTC") + seq_len(n),
    dt = as.difftime(seq_len(n) %% 60, units = "secs"),
    l = rep(c(TRUE, FALSE, NA), length.out = n),
    x = runif(n)
  )
  df$d[4] <- NA
  mizu_send(p$host, arrow::arrow_table(df))
  out <- mizu_recv(p$peer, 5)
  tab <- arrow::as_arrow_table(mizu_as_arrow(out))
  types <- vapply(tab$schema$fields, function(f) f$type$ToString(), "")
  expect_identical(
    unname(types),
    c(
      "dictionary<values=string, indices=int32>",
      "date32[day]",
      "timestamp[us, tz=UTC]",
      "duration[us]",
      "bool",
      "double"
    )
  )
  back <- as.data.frame(tab)
  expect_identical(back, df)
})

test_that("mizu_as_arrow exports integer64 as int64", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("bit64")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(i64 = bit64::as.integer64(c(1:3, NA, seq_len(6000))), x = 1:6004)
  mizu_send(p$host, arrow::arrow_table(df))
  out <- mizu_recv(p$peer, 5)
  tab <- arrow::as_arrow_table(mizu_as_arrow(out))
  expect_identical(tab$schema$fields[[1]]$type$ToString(), "int64")
  expect_identical(as.numeric(tab$i64), as.numeric(df$i64))
})

test_that("mizu_as_arrow falls back for a plain data frame", {
  skip_if_not_installed("nanoarrow")
  df <- data.frame(x = 1:5, s = c("a", "b", "c", "d", "e"))
  st <- mizu_as_arrow(df)
  expect_s3_class(st, "nanoarrow_array_stream")
  expect_identical(nanoarrow::convert_array_stream(st), df)
})

test_that("mizu_as_arrow declines are informative", {
  skip_if_not_installed("arrow")
  expect_error(mizu_as_arrow(1:5), "expected a data frame")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(z = complex(6000, 1, 2), x = runif(6000))
  mizu_send(p$host, df) # complex has no Arrow type; a plain send frames it
  out <- mizu_recv(p$peer, 5)
  expect_error(mizu_as_arrow(out), "column 'z'")
})

test_that("a latin1 string column takes the copy path", {
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  s <- rep(c("héllo", "plain"), length.out = 6000)
  Encoding(s) <- ifelse(s == "héllo", "latin1", "unknown")
  df <- data.frame(s = s, x = runif(6000))
  mizu_send(p$host, arrow::arrow_table(df))
  out <- mizu_recv(p$peer, 5)
  st <- mizu_as_arrow(out)
  expect_s3_class(st, "nanoarrow_array_stream")
  back <- arrow::as_arrow_table(st)
  expect_identical(as.data.frame(back)$s, df$s)
})
