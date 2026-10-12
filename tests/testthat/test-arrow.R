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
  df <- data.frame(
    x = runif(6000),
    i = seq_len(6000) + 0L,
    s = rep(c("alpha", "beta", NA), length.out = 6000)
  )
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
  df <- data.frame(
    i64 = bit64::as.integer64(c(1:3, NA, seq_len(6000))),
    x = 1:6004
  )
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

test_that("a relayed frame borrows remote and local columns uniformly", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  x1 <- runif(20000)
  x2 <- seq_len(20000) + 0L
  mizu_send(p$host, list(x1, x2))
  ya <- mizu_recv(p$peer, 5)
  b <- runif(20000)
  # both views nest as remote leaves; b crosses as the frame's own leaf
  mizu_send(p$peer, data.frame(a = ya[[1L]], a2 = ya[[2L]], b = b))
  out <- mizu_recv(p$host, 5)
  invisible(out$a) # resolve + cache the remote views before the base read
  invisible(out$a2)
  base <- rc_of(ya[[1L]])[1L]
  st <- mizu_as_arrow(out)
  expect_identical(rc_of(ya[[1L]])[1L], base + 1L) # one loan for two remotes
  tab <- arrow::as_arrow_table(st)
  expect_identical(as.data.frame(tab), data.frame(a = x1, a2 = x2, b = b))
  rm(st, tab)
  gc()
  gc()
  gc() # the release cascade runs one generation per pass
  expect_identical(rc_of(ya[[1L]])[1L], base)
  ex <- arrow_export(out)
  expect_identical(ex$kinds, c(1L, 2L, 1L))
})

test_that("relayed root views borrow through the rc-0 arms", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("bit64")
  p <- channel_pair()
  on.exit(channel_end(p))
  # the MIZS arm: an unadorned string view crosses as a remote leaf
  v <- rep(c("alpha", "beta", NA, "héllo ✓"), length.out = 40000)
  mizu_send(p$host, v)
  sv <- mizu_recv(p$peer, 5)
  b <- runif(40000)
  mizu_send(p$peer, data.frame(s = sv, b = b))
  out <- mizu_recv(p$host, 5)
  ex <- arrow_export(out)
  expect_identical(ex$kinds, c(4L, 1L))
  expect_identical(
    array_value(ex$arrays[[1L]], nanoarrow::na_large_string()),
    v
  )
  expect_identical(array_value(ex$arrays[[2L]], nanoarrow::na_double()), b)
  rm(ex)
  # the MIZH arm: a classed (never REF-able) integer64 view rides the
  # serialize path and resolves as a bare root view
  i64 <- bit64::as.integer64(c(1, 2, NA, seq_len(40000)))
  mizu_send(p$host, i64)
  iv <- mizu_recv(p$peer, 5)
  b <- runif(40003)
  mizu_send(p$peer, data.frame(l = iv, b = b))
  out <- mizu_recv(p$host, 5)
  ex <- arrow_export(out)
  expect_identical(ex$kinds, c(3L, 0L))
  back <- array_value(ex$arrays[[1L]], nanoarrow::na_int64())
  expect_identical(sum(is.na(back)), 1L) # the sentinel scan's owned bitmap
  expect_identical(bit64::as.integer64(back), i64)
})

test_that("the export borrows a crafted remote column (path target)", {
  skip_if_not_installed("nanoarrow")
  x <- runif(50000)
  fx <- remote_leaf_pair(x)
  on.exit(channel_end(fx[["p"]]))
  poke_remote(fx, len = length(x))
  col <- fx[["yb"]][[1L]]
  ex <- arrow_export(list(col))
  expect_identical(ex$kinds, 1L)
  expect_identical(array_value(ex$arrays[[1L]], nanoarrow::na_double()), x)
})

test_that("the export borrows a crafted remote int64 column with NA", {
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("bit64")
  x <- bit64::as.integer64(c(1, 2, NA, seq_len(50000)))
  fx <- remote_leaf_pair(x)
  on.exit(channel_end(fx[["p"]]))
  poke_remote(fx, len = length(x))
  col <- fx[["yb"]][[1L]]
  ex <- arrow_export(list(col))
  expect_identical(ex$kinds, 3L)
  back <- array_value(ex$arrays[[1L]], nanoarrow::na_int64())
  expect_identical(sum(is.na(back)), 1L) # a same-language region: sentinel scan
  expect_identical(bit64::as.integer64(back), x)
})

test_that("the export borrows a crafted remote string column with NA", {
  skip_if_not_installed("nanoarrow")
  x <- rep(c("alpha", NA, "héllo ✓"), length.out = 50000)
  fx <- remote_leaf_pair(x)
  on.exit(channel_end(fx[["p"]]))
  poke_remote(fx, len = length(x))
  col <- fx[["yb"]][[1L]]
  ex <- arrow_export(list(col))
  expect_identical(ex$kinds, 4L)
  back <- array_value(ex$arrays[[1L]], nanoarrow::na_large_string())
  expect_identical(sum(is.na(back)), sum(is.na(x))) # the block-validity count
  expect_identical(back, x)
})

test_that("a depth-2 remote borrow flattens to the final region", {
  skip_if_not_installed("nanoarrow")
  x <- runif(50000)
  fx <- remote_leaf_pair(x)
  on.exit(channel_end(fx[["p"]]))
  poke_remote(fx, len = length(x)) # B's entry 0 -> aname[1]
  bname <- .Call(mizu:::mizu_zc_view_name, fx[["yb"]])
  mizu_send(fx[["p"]][["host"]], list(runif(40000)))
  yc <- mizu_recv(fx[["p"]][["peer"]], 5)
  rwc <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_zc_view_name, yc),
    TRUE
  )
  poke_remote(list(rw = rwc), id = paste0(bname, "[1]"), len = length(x))
  col <- yc[[1L]] # C -> bname[1] -> aname[1]: the resolve recursion flattens
  ex <- arrow_export(list(col))
  expect_identical(ex$kinds, 1L)
  expect_identical(array_value(ex$arrays[[1L]], nanoarrow::na_double()), x)
})

test_that("a mixed-region frame borrows each view column off its region", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- runif(6000)
  mizu_send(p$host, data.frame(x = x, s = rep(c("a", "b", NA), 2000)))
  f1 <- mizu_recv(p$peer, 5)
  y <- seq_len(6000) + 0
  mizu_send(p$host, data.frame(y = y, s = rep(c("a", "b", NA), 2000)))
  f2 <- mizu_recv(p$peer, 5)
  r1 <- rc_of(f1$x)[1L]
  r2 <- rc_of(f2$y)[1L]
  ex <- arrow_export(data.frame(a = f1$x, b = f2$y, c = y))
  expect_identical(ex$kinds, c(1L, 1L, 0L))
  expect_identical(rc_of(f1$x)[1L], r1 + 1L) # one loan per region
  expect_identical(rc_of(f2$y)[1L], r2 + 1L)
  expect_identical(array_value(ex$arrays[[1L]], nanoarrow::na_double()), x)
  expect_identical(array_value(ex$arrays[[2L]], nanoarrow::na_double()), y)
})

test_that("a subset frame borrows its live view columns", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(
    x = runif(6000),
    s = rep(c("a", "b", NA), length.out = 6000),
    z = seq_len(6000) + 0L
  )
  mizu_send(p$host, df)
  f <- mizu_recv(p$peer, 5)
  ex <- arrow_export(f[1:2]) # a materialized frame of live views
  expect_identical(ex$kinds, c(1L, 4L))
  expect_identical(array_value(ex$arrays[[1L]], nanoarrow::na_double()), df$x)
  expect_identical(
    array_value(ex$arrays[[2L]], nanoarrow::na_large_string()),
    df$s
  )
})

test_that("a received vector assembled into a data.frame borrows", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  v <- rep(c("alpha", NA, "héllo"), length.out = 40000)
  mizu_send(p$host, v)
  sv <- mizu_recv(p$peer, 5)
  ex <- arrow_export(data.frame(s = sv))
  expect_identical(ex$kinds, 4L)
  expect_identical(
    array_value(ex$arrays[[1L]], nanoarrow::na_large_string()),
    v
  )
})

test_that("duplicated columns borrow off one loan", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  x <- runif(6000)
  mizu_send(p$host, data.frame(x = x, s = rep(c("a", "b", NA), 2000)))
  f <- mizu_recv(p$peer, 5)
  base <- rc_of(f$x)[1L]
  ex <- arrow_export(data.frame(a = f$x, a2 = f$x))
  expect_identical(ex$kinds, c(1L, 1L))
  expect_identical(rc_of(f$x)[1L], base + 1L)
})

test_that("a latin1-marked remote string column copies", {
  skip_if_not_installed("arrow")
  x <- rep(c("héllo", "plain"), length.out = 50000)
  Encoding(x) <- ifelse(x == "héllo", "latin1", "unknown")
  fx <- remote_leaf_pair(x)
  on.exit(channel_end(fx[["p"]]))
  poke_remote(fx, len = length(x))
  col <- fx[["yb"]][[1L]]
  ex <- arrow_export(list(col))
  expect_identical(ex$kinds, 0L)
  back <- arrow::as_arrow_table(mizu_as_arrow(data.frame(s = col)))
  expect_identical(as.data.frame(back)$s, x)
})

test_that("a materialized remote column copies", {
  skip_if_not_installed("nanoarrow")
  x <- runif(50000)
  fx <- remote_leaf_pair(x)
  on.exit(channel_end(fx[["p"]]))
  poke_remote(fx, len = length(x))
  col <- fx[["yb"]][[1L]]
  col[1L] <- -col[1L] # COW: no longer the SHM-backed view
  x[1L] <- -x[1L]
  ex <- arrow_export(list(col))
  expect_identical(ex$kinds, 0L)
  expect_identical(col, x)
})

test_that("a dead referenced region degrades only its own columns", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  skip_if_not_installed("arrow")
  p1 <- channel_pair()
  x <- runif(50000)
  mizu_send(p1[["host"]], list(x))
  ya <- mizu_recv(p1[["peer"]], 5)
  p2 <- channel_pair()
  on.exit(channel_end(p2))
  b <- runif(50000)
  mizu_send(p2[["host"]], data.frame(a = ya[[1L]], b = b))
  frame <- mizu_recv(p2[["peer"]], 5)
  col <- frame$a # resolve before the teardown (the cache carries it)
  expect_true(is_view(col))
  channel_end(p1) # A unlinks; the received views' own mappings survive
  back <- as.data.frame(arrow::as_arrow_table(mizu_as_arrow(frame)))
  expect_identical(back$a, x) # values intact — assert values, never the locus
  expect_identical(back$b, b)
})

test_that("an orphaned referenced region still borrows", {
  skip_if_no_child_mizu()
  skip_if_not_installed("nanoarrow")
  ch <- mizu_channel(
    quote({
      mizu_send(ch, list(runif(100000)))
      Sys.sleep(30)
    }),
    arena_size = 0
  )
  ya <- mizu_recv(ch, 30)
  col <- ya[[1L]]
  expect_true(is_view(col))
  expected <- as.numeric(col)
  kill_hard(.Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]])
  expect_s3_class(mizu_recv(ch, 30), "mizu_peer_gone")
  ex <- arrow_export(list(col)) # the name survives a hard kill: the borrow
  expect_identical(ex$kinds, 1L)
  expect_identical(
    array_value(ex$arrays[[1L]], nanoarrow::na_double()),
    expected
  )
  expect_true(mizu_close(ch, 10))
})
