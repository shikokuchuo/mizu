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

test_that("a small table crosses below the gate, claiming no region", {
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

test_that("a below-gate frame round-trips per column kind", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("bit64")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(
    str = rep(c("alpha", NA, "γ"), 3),
    fct = factor(rep(c("b", "a", NA), 3), levels = c("a", "b", "zzz")),
    dat = as.Date("2024-01-01") + c(0, NA, 1, 2, 3, 4, 5, 6, 7),
    tms = as.POSIXct("2024-01-01 00:00:00.5", tz = "UTC") + 0:8,
    dur = as.difftime(rep(c(1, NA, 300), 3), units = "secs"),
    i64 = bit64::as.integer64(c(1, NA, -3, 4, 5, 6, 7, 8, 9))
  )
  mizu_send(p$host, arrow::arrow_table(df))
  out <- mizu_recv(p$peer, 5)
  expect_identical(out, df)
  expect_false(is_view(out$str))
})

test_that("a below-gate frame claims keeperless inline", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  ko <- function() .Call(mizu:::mizu_channel_keep_out, p$host)
  n0 <- ko()
  df <- data.frame(a = 1:3, b = c("x", "y", NA))
  mizu_send(p$host, arrow::arrow_table(df))
  expect_identical(ko(), n0)
  expect_identical(mizu_recv(p$peer, 5), df)
})

test_that("a below-gate frame crosses the arena and spill tiers", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  # the 'I' stream past the 256-byte slot but within the 4 KiB arena
  df <- data.frame(s = paste(rep("a", 500), collapse = ""), x = 1.5)
  mizu_send(p$host, arrow::arrow_table(df))
  expect_identical(mizu_recv(p$peer, 5), df)
  # past the arena but below the zc floor: the SHM_RAW spill
  big <- data.frame(s = paste(rep("b", 6000), collapse = ""), x = 2.5)
  mizu_send(p$host, arrow::arrow_table(big))
  expect_identical(mizu_recv(p$peer, 5), big)
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
  tab <- arrow::arrow_table(u = arrow::Array$create(uv, type = arrow::uint64()))
  p <- channel_pair()
  on.exit(channel_end(p))
  # a current peer reads verbatim uint64: no send warning — the landing
  # warns at the value's first touch instead
  expect_identical(grab_warnings(mizu_send(p$host, tab)), character())
  out <- mizu_recv(p$peer, 5)
  ws <- grab_warnings(out$u + 0)
  expect_match(ws, "beyond ...2\\^53 convert to NA", all = FALSE)
  expect_identical(as.numeric(out$u)[1:3], c(1, NA_real_, 1))
  # held views pin their spill regions: release each received frame before
  # the next send or the churn signal (Linux only) drops staging to the
  # copy tiers
  rm(out)
  invisible(gc())
  # an older-version peer (bits 0-5) gets the conversion — and the send warning
  fp <- foreign_pair(caps = 63L)
  on.exit(channel_end(fp), add = TRUE)
  ws <- grab_warnings(mizu_send(fp$host, tab))
  expect_match(ws, "beyond ...2\\^53 convert to NA", all = FALSE)
  expect_identical(mizu_recv(fp$peer, 5)$u[1:3], c(1, NA_real_, 1))
  iv <- rep(1, 6000)
  iv[2] <- -2147483648
  iv[3] <- NA_real_
  tab2 <- arrow::arrow_table(
    i = arrow::Array$create(iv, type = arrow::int32()),
    p = runif(6000)
  )
  ws2 <- grab_warnings(mizu_send(p$host, tab2))
  expect_match(ws2, "-2147483648 read as NA_integer_", all = FALSE)
  expect_identical(
    mizu_recv(p$peer, 5)$i[1:3],
    c(1L, NA_integer_, NA_integer_)
  )
  invisible(gc())
  # a genuine INT64_MIN on a masked int64 column
  lv <- c(1, -9.2233720368547758e18, NA, 3, runif(5996))
  tab3 <- arrow::arrow_table(
    l = arrow::Array$create(lv)$cast(arrow::int64(), safe = FALSE)
  )
  ws3 <- grab_warnings(mizu_send(p$host, tab3))
  expect_match(ws3, "-9223372036854775808 read as NA_integer64_", all = FALSE)
  expect_identical(
    mizu_recv(p$peer, 5)$l[1:4],
    bit64::as.integer64(c(1, NA, NA, 3))
  )
})

test_that("an int64 column's nulls and valid INT64_MIN cross exact", {
  skip_if_not_installed("arrow")
  lv <- c(1, -9.2233720368547758e18, NA, 3, runif(5996))
  orig <- arrow::Array$create(lv)$cast(arrow::int64(), safe = FALSE)
  p <- channel_pair()
  on.exit(channel_end(p))
  grab_warnings(mizu_send(p$host, arrow::arrow_table(l = orig)))
  df <- mizu_recv(p$peer, 5)
  s <- mizu_as_arrow(df)
  fmt <- s$get_schema()$children$l$format
  out <- arrow::as_arrow_table(s)$l
  expect_identical(fmt, "l")
  expect_identical(out$null_count, 1L)
  expect_identical(
    out$cast(arrow::utf8())$as_vector(),
    orig$cast(arrow::utf8())$as_vector()
  )
})

test_that("the uint64 view exports exact past reads; a write degrades it", {
  skip_if_not_installed("arrow")
  uv <- bit64::as.integer64(rep(1, 6000))
  uv[2] <- bit64::as.integer64("9223372036854775807")
  orig <- arrow::Array$create(uv, type = arrow::uint64())
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, arrow::arrow_table(u = orig))
  df <- mizu_recv(p$peer, 5)
  expect_true(is_view(df$u))
  # a read-only touch builds the conversion cache outside data2 — the
  # exact borrow still fires
  grab_warnings(df$u + 0)
  s <- mizu_as_arrow(df)
  fmt <- s$get_schema()$children$u$format
  back <- arrow::as_arrow_table(s)$u
  expect_identical(fmt, "L")
  expect_identical(
    back$cast(arrow::utf8())$as_vector(),
    orig$cast(arrow::utf8())$as_vector()
  )
  # a write COWs the column — the export degrades to the converted doubles
  df$u[1] <- 0
  expect_identical(mizu_as_arrow(df)$get_schema()$children$u$format, "g")
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

test_that("a decline leaves the handle consistent", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  tab <- arrow::arrow_table(
    d = arrow::Array$create(1.25, type = arrow::decimal(5, 2))
  )
  expect_error(mizu_send(p$host, tab), class = "mizu_error_not_portable")
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

test_that("a timestamp[ns] column lands as POSIXct double seconds", {
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("bit64")
  p <- channel_pair()
  on.exit(channel_end(p))
  na <- asNamespace("nanoarrow")
  n <- 9000L
  counts <- bit64::as.integer64(
    c("1700000000123456789", NA, "-3600000000001", seq_len(n))
  )
  sch <- na$na_struct(
    list(t = na$na_timestamp("ns", timezone = "UTC"), x = na$na_int32())
  )
  parent <- na$nanoarrow_array_init(sch)
  parent <- na$nanoarrow_array_modify(
    parent,
    list(
      children = list(
        na$as_nanoarrow_array(counts),
        na$as_nanoarrow_array(seq_len(n + 3L))
      ),
      length = n + 3L
    ),
    validate = FALSE
  )
  na$nanoarrow_array_set_schema(parent, sch, validate = FALSE)
  stream <- na$basic_array_stream(list(parent), schema = sch, validate = FALSE)
  mizu_send(p$host, stream)
  df <- mizu_recv(p$peer, 5)
  expect_identical(length(rc_of(df$x)), 2L)
  expect_identical(length(rc_of(df$t)), 2L)
  expect_identical(class(df$t), c("POSIXct", "POSIXt"))
  expect_identical(attr(df$t, "tzone"), "UTC")
  expect_null(attr(df$t, "tscale"))
  expect_type(df$t, "double")
  want <- suppressWarnings(as.double(counts)) * 1e-9
  expect_identical(as.double(df$t), want)
  st <- mizu_as_arrow(df)
  expect_identical(st$get_schema()$children$t$format, "tsu:UTC")
})

test_that("a duration[ns] column lands as difftime seconds", {
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("bit64")
  p <- channel_pair()
  on.exit(channel_end(p))
  na <- asNamespace("nanoarrow")
  n <- 9000L
  counts <- bit64::as.integer64(c("90", NA, "86400000000000", seq_len(n)))
  sch <- na$na_struct(list(u = na$na_duration("ns"), x = na$na_int32()))
  parent <- na$nanoarrow_array_init(sch)
  parent <- na$nanoarrow_array_modify(
    parent,
    list(
      children = list(
        na$as_nanoarrow_array(counts),
        na$as_nanoarrow_array(seq_len(n + 3L))
      ),
      length = n + 3L
    ),
    validate = FALSE
  )
  na$nanoarrow_array_set_schema(parent, sch, validate = FALSE)
  stream <- na$basic_array_stream(list(parent), schema = sch, validate = FALSE)
  mizu_send(p$host, stream)
  df <- mizu_recv(p$peer, 5)
  expect_identical(length(rc_of(df$x)), 2L)
  expect_identical(length(rc_of(df$u)), 2L)
  expect_identical(class(df$u), "difftime")
  expect_identical(attr(df$u, "units"), "secs")
  expect_null(attr(df$u, "tscale"))
  want <- suppressWarnings(as.double(counts)) * 1e-9
  expect_identical(as.double(df$u), want)
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

test_that("the string export's null count masks the tail byte", {
  skip_if_not_installed("nanoarrow")
  x <- rep(c("alpha", "beta", "héllo ✓"), length.out = 20001)
  x[c(2L, 1000L, 20001L)] <- NA_character_ # a null in the tail byte's used bit
  fx <- remote_leaf_pair(x)
  on.exit(channel_end(fx[["p"]]))
  poke_remote(fx, len = length(x))
  col <- fx[["yb"]][[1L]]
  ex <- arrow_export(list(col))
  expect_identical(ex$kinds, 4L)
  back <- array_value(ex$arrays[[1L]], nanoarrow::na_large_string())
  expect_identical(sum(is.na(back)), 3L)
  expect_identical(back, x)
})

test_that("a depth-2 remote borrow flattens to the final region", {
  skip_if_not_installed("nanoarrow")
  x <- runif(50000)
  fx <- remote_leaf_pair(x)
  on.exit(channel_end(fx[["p"]]))
  poke_remote(fx, len = length(x)) # B's entry 0 -> aname[1]
  bname <- .Call(mizu:::mizu_zc_view_name, fx[["yb"]])
  # a second pair for region C: the live views over A and B trip the first
  # handle's churn fallback on Linux
  p2 <- channel_pair(arena_size = 0)
  on.exit(channel_end(p2), add = TRUE)
  mizu_send(p2[["host"]], list(runif(40000)))
  yc <- mizu_recv(p2[["peer"]], 5)
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
  nm <- .Call(mizu:::mizu_zc_view_name, col)
  kill_hard(.Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]])
  # the killed peer staged the region: reap it once its PID frees, so the
  # orphan (and the macOS registry log) does not outlive the test run
  if (.Platform[["OS.type"]] != "windows" && reaper_ok()) {
    on.exit(
      {
        reaped <- character()
        wait_until({
          reaped <- c(reaped, .Call(mizu:::mizu_prune_call))
          nm %in% reaped
        })
      },
      add = TRUE
    )
  }
  expect_s3_class(mizu_recv(ch, 30), "mizu_peer_gone")
  ex <- arrow_export(list(col)) # the name survives a hard kill: the borrow
  expect_identical(ex$kinds, 1L)
  expect_identical(
    array_value(ex$arrays[[1L]], nanoarrow::na_double()),
    expected
  )
  expect_true(mizu_close(ch, 10))
})

test_that("a pool result past the gate stages as one frame region", {
  skip_if_not_installed("arrow")
  p <- pool_pair()
  on.exit(pool_end(p))
  m <- arrow_matrix()
  t <- mizu_submit(p$ctrl, arrow::arrow_table(w), w = m$want)
  pool_step(p)
  out <- mizu_collect(t, 5)
  expect_true(is_view(out))
  expect_true(is_view(out$f64))
  expect_identical(out, m$want)
  back <- as.data.frame(arrow::as_arrow_table(mizu_as_arrow(out)))
  # u8 (raw) reads back as integer on the Arrow side, as over channels
  expect_identical(back[names(back) != "u8"], m$want[names(m$want) != "u8"])
  expect_identical(back$u8, as.integer(m$want$u8))
})

test_that("a below-gate pool result crosses as a data.frame", {
  skip_if_not_installed("arrow")
  p <- pool_pair()
  on.exit(pool_end(p))
  df <- data.frame(a = 1:3, b = c("x", "y", NA))
  t <- mizu_submit(p$ctrl, arrow::arrow_table(df), df = df)
  pool_step(p)
  out <- mizu_collect(t, 5)
  expect_identical(out, df)
  expect_false(is_view(out$a))
  # a result stream past the result-slot budget spills SHM_RAW
  big <- data.frame(s = paste(rep("b", 2000), collapse = ""), x = 2.5)
  t2 <- mizu_submit(p$ctrl, arrow::arrow_table(big), big = big)
  pool_step(p)
  expect_identical(mizu_collect(t2, 5), big)
})

test_that("a non-struct nanoarrow payload re-enters the pool tiers", {
  skip_if_not_installed("nanoarrow")
  p <- pool_pair()
  on.exit(pool_end(p))
  # as the task result
  t <- mizu_submit(
    p$ctrl,
    identity(nanoarrow::as_nanoarrow_array(c(1L, NA, 3L)))
  )
  pool_step(p)
  expect_identical(mizu_collect(t, 5), c(1L, NA, 3L))
  # nested as an argument
  t <- mizu_submit(
    p$ctrl,
    identity(x),
    x = nanoarrow::as_nanoarrow_array_stream(
      nanoarrow::as_nanoarrow_array(c("a", NA, "z"))
    )
  )
  pool_step(p)
  expect_identical(mizu_collect(t, 5), c("a", NA, "z"))
})

test_that("a table spliced as the task expression round-trips by reference", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  skip_if_not_installed("arrow")
  p <- pool_pair()
  on.exit(pool_end(p))
  m <- arrow_matrix()
  t <- do.call(mizu_submit, list(p$ctrl, m$tab))
  pool_step(p)
  out <- mizu_collect(t, 5)
  expect_true(is_view(out))
  expect_identical(out, m$want)
  rc <- rc_of(out)
  expect_identical(bitwAnd(rc[2L], 1L), 1L) # REFHELD: the reference path
})

test_that("a nested table arg crosses as a frame leaf; a second converts", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  skip_if_not_installed("arrow")
  p <- pool_pair(slot_size = 1024L)
  on.exit(pool_end(p))
  m <- arrow_matrix()
  small <- data.frame(a = 1:3, b = c("x", "y", NA))
  t <- mizu_submit(
    p$ctrl,
    list(
      .Call(mizu:::mizu_zc_view_check, a),
      .Call(mizu:::mizu_zc_view_check, b),
      a,
      b
    ),
    a = m$tab,
    b = arrow::arrow_table(small)
  )
  pool_step(p)
  out <- mizu_collect(t, 5)
  expect_true(out[[1L]])
  expect_false(out[[2L]])
  expect_true(is_view(out[[3L]]))
  expect_identical(out[[3L]], m$want)
  expect_identical(out[[4L]], small)
})

test_that("a converting table's lazy list-column elements cross by value", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  skip_if_not_installed("arrow")
  p <- pool_pair()
  on.exit(pool_end(p))
  big <- data.frame(v = runif(6000))
  # the frame takes a; b converts: the conversion's lazy string column
  # materializes at the bail, the list column's elements cross by value
  t <- mizu_submit(
    p$ctrl,
    list(a, b),
    a = arrow::arrow_table(big),
    b = arrow::arrow_table(
      s = c("x", "y", NA),
      x = arrow::Array$create(list(1:2, 3L, 4:6))
    )
  )
  pool_step(p)
  out <- mizu_collect(t, 5)
  expect_true(is_view(out[[1L]]))
  expect_identical(out[[2L]][["s"]], c("x", "y", NA))
  expect_identical(as.list(out[[2L]][["x"]]), list(1:2, 3L, 4:6))
})

test_that("a RecordBatchReader nested as an arg crosses as a frame leaf", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  skip_if_not_installed("arrow")
  p <- pool_pair()
  on.exit(pool_end(p))
  df <- data.frame(
    x = as.double(seq_len(6000)),
    s = rep(c("a", "b", NA), length.out = 6000)
  )
  rbs <- lapply(0:3, function(k) {
    arrow::record_batch(df[(k * 1500 + 1):((k + 1) * 1500), ])
  })
  rdr <- do.call(arrow::RecordBatchReader$create, rbs)
  t <- mizu_submit(
    p$ctrl,
    list(.Call(mizu:::mizu_zc_view_check, a), a),
    a = rdr
  )
  pool_step(p)
  out <- mizu_collect(t, 5)
  expect_true(out[[1L]])
  expect_identical(out[[2L]], df)
  # the pull materializes the reader at send: nothing left to read
  expect_null(rdr$read_next_batch())
})

test_that("an over-budget stream converts the table and still crosses", {
  skip_if_not_installed("arrow")
  p <- pool_pair()
  on.exit(pool_end(p))
  m <- arrow_matrix()
  # past the slot's inline budget but below the zc floor, so the payload
  # reaches the codec tier (a bigger vector stages SHM_VEC ahead of it)
  v <- runif(200)
  t <- mizu_submit(
    p$ctrl,
    list(.Call(mizu:::mizu_zc_view_check, a), a, v),
    a = m$tab,
    v = v
  )
  pool_step(p)
  out <- mizu_collect(t, 5)
  expect_false(out[[1L]])
  # the by-value conversion lands uint8 as integer (R raw has no NA),
  # where the frame region's landing is raw
  want <- m$want
  want$u8 <- as.integer(want$u8)
  expect_identical(out[[2L]], want)
  expect_identical(out[[3L]], v)
})

test_that("a nested table crosses a channel as a frame leaf", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  m <- arrow_matrix()
  mizu_send(p$host, list(m$tab, 1L))
  out <- mizu_recv(p$peer, 5)
  expect_true(is_view(out[[1L]]))
  expect_identical(out[[1L]], m$want)
  expect_identical(out[[2L]], 1L)
})

test_that("a non-subset sibling takes the fallback with nothing consumed", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(x = 1:3)
  rdr <- arrow::RecordBatchReader$create(arrow::record_batch(df))
  e <- new.env()
  e$a <- 1
  f <- function() e$a # a closure over a local environment: the codec declines
  mizu_send(p$host, list(rdr, f))
  out <- mizu_recv(p$peer, 5)
  expect_true(is.function(out[[2L]]))
  expect_identical(out[[2L]](), 1)
  # the serialize fallback shipped the reader's shell; the sender's stream
  # is still readable
  expect_identical(
    nanoarrow::convert_array_stream(nanoarrow::as_nanoarrow_array_stream(rdr)),
    df
  )
})

test_that("conversion counts fire at a pool publish after the write half", {
  skip_if_not_installed("arrow")
  p <- pool_pair()
  on.exit(pool_end(p))
  iv <- rep(1, 6000)
  iv[2] <- -2147483648
  iv[3] <- NA_real_
  expr <- quote(arrow::arrow_table(
    i = arrow::Array$create(iv, type = arrow::int32()),
    p = runif(6000)
  ))
  t <- do.call(mizu_submit, list(p$ctrl, expr, iv = iv))
  ws <- grab_warnings(pool_step(p))
  expect_match(ws, "-2147483648 read as NA_integer_", all = FALSE)
  out <- mizu_collect(t, 5)
  expect_true(is_view(out))
  expect_identical(out$i[1:3], c(1L, NA_integer_, NA_integer_))
})

test_that("a nested table at or under the gate converts by value", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("bit64")
  p <- channel_pair()
  on.exit(channel_end(p))
  df <- data.frame(a = 1:3, b = bit64::as.integer64(c(4, 5, NA)))
  mizu_send(p$host, list(arrow::arrow_table(df), "x"))
  out <- mizu_recv(p$peer, 5)
  expect_identical(out, list(df, "x"))
  expect_false(is_view(out[[1L]]))
})

test_that("a table occurring twice dedupes to one frame leaf", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  m <- arrow_matrix()
  mizu_send(p$host, list(m$tab, m$tab))
  out <- mizu_recv(p$peer, 5)
  expect_identical(out[[1L]], m$want)
  expect_identical(out[[2L]], m$want)
})

test_that("an ALTREP sibling of a table falls back to R_Serialize", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  m <- arrow_matrix()
  mizu_send(p$host, list(m$tab, 1:5))
  out <- mizu_recv(p$peer, 5)
  expect_true(inherits(out[[1L]], "ArrowTabular"))
  expect_identical(out[[2L]], 1:5)
})

test_that("a frame declined at the stage converts and crosses by value", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair(ident = c(2L, 0L)) # an R peer with no capabilities
  on.exit(channel_end(p))
  df <- data.frame(
    x = runif(6000),
    s = rep(c("alpha", "beta", NA), length.out = 6000)
  )
  mizu_send(p$host, nanoarrow::as_nanoarrow_array_stream(df))
  out <- mizu_recv(p$peer, 5)
  expect_identical(out[], df)
})

test_that("a released stream raises the pull failure", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  st <- nanoarrow::as_nanoarrow_array_stream(data.frame(x = 1:3))
  st$release()
  expect_error(mizu_send(p$host, st), "the Arrow stream failed")
})

test_that("the export call validates its arguments", {
  skip_if_not_installed("nanoarrow")
  arr <- nanoarrow::nanoarrow_allocate_array()
  expect_error(
    .Call(mizu:::mizu_arrow_export, 1:3, list(arr)),
    "corrupt arrow export call"
  )
  expect_error(
    .Call(mizu:::mizu_arrow_export, data.frame(a = 1), list(arr, arr)),
    "corrupt arrow export call"
  )
})

test_that("a cleared array extptr fails the export pre-fill", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, runif(40000))
  v <- mizu_recv(p$peer, 5)
  dud <- unserialize(serialize(nanoarrow::nanoarrow_allocate_array(), NULL))
  expect_error(
    .Call(mizu:::mizu_arrow_export, data.frame(x = v), list(dud)),
    "corrupt arrow export call"
  )
})

test_that("the export borrows fixed-width views across the NA forms", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  # sentinel-derived bitmap: the NAs scanned off the data
  x <- c(runif(39999), NA_real_)
  mizu_send(p$host, x)
  vx <- mizu_recv(p$peer, 5)
  ex <- arrow_export(data.frame(x = vx))
  expect_true(ex$kinds[[1L]] != 0L)
  expect_identical(array_value(ex$arrays[[1L]], nanoarrow::na_double()), x)
  # no NAs: the built bitmap scans all-valid and frees
  y <- runif(40000)
  mizu_send(p$host, y)
  vy <- mizu_recv(p$peer, 5)
  ey <- arrow_export(data.frame(y = vy))
  expect_true(ey$kinds[[1L]] != 0L)
  expect_identical(array_value(ey$arrays[[1L]], nanoarrow::na_double()), y)
})

test_that("a frame column's export borrows its tail bitmap verbatim", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  # standalone: sending past other live views trips the Linux churn fallback
  df <- data.frame(x = runif(6000))
  df$x[[13]] <- NA
  mizu_send(p$host, nanoarrow::as_nanoarrow_array_stream(df))
  vf <- mizu_recv(p$peer, 5)
  ef <- arrow_export(vf)
  expect_true(ef$kinds[[1L]] != 0L)
  expect_identical(array_value(ef$arrays[[1L]], nanoarrow::na_double()), df$x)
})

test_that("the string borrow gates on the wire encodings", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  # latin1 marks cross same-language but decline the Arrow borrow
  s <- iconv(
    rep(c("héllo", "wörld"), length.out = 70000),
    from = "UTF-8",
    to = "latin1"
  )
  mizu_send(p$host, s)
  v <- mizu_recv(p$peer, 5)
  expect_true(is_view(v))
  expect_identical(arrow_export(data.frame(s = v))$kinds[[1L]], 0L)
  # a native mark failing UTF-8 validation declines likewise
  mizu_send(p$host, rep(rawToChar(as.raw(c(0x41, 0x80))), 70000))
  v2 <- mizu_recv(p$peer, 5)
  expect_true(is_view(v2))
  expect_identical(arrow_export(data.frame(s = v2))$kinds[[1L]], 0L)
})

test_that("a list-view column takes the copy path", {
  skip_if_not_installed("nanoarrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, list(runif(30000), runif(30000), runif(30000)))
  lv <- mizu_recv(p$peer, 5)
  expect_true(is_view(lv))
  df <- data.frame(a = 1:3)
  df$b <- lv
  expect_identical(arrow_export(df)$kinds[[2L]], 0L)
})

test_that("a dead producer degrades only its own columns to the copy path", {
  skip_on_os("windows") # kernel objects have no unlink step to observe
  skip_if_not_installed("nanoarrow")
  skip_if_no_child_mizu()
  ch <- mizu_channel(quote({
    set.seed(42)
    mizu_send(ch, runif(40000))
    set.seed(43)
    mizu_send(ch, runif(40000))
    # hold until the host has both views — exiting earlier unlinks the
    # regions ahead of a slow host's recv (valgrind CI gate)
    mizu_recv(ch, 30)
  }))
  on.exit(mizu_close(ch))
  v1 <- mizu_recv(ch, 30)
  v2 <- mizu_recv(ch, 30)
  expect_true(is_view(v1))
  mizu_send(ch, NULL)
  # the peer quits after its sends; its region names unlink at exit
  expect_true(wait_until(
    inherits(
      tryCatch(
        .Call(
          mizu:::mizu_region_open,
          .Call(mizu:::mizu_zc_view_name, v1),
          FALSE
        ),
        error = function(e) e
      ),
      "error"
    )
  ))
  df <- data.frame(a = v1, b = v2)
  ex <- arrow_export(df)
  expect_identical(ex$kinds, c(0L, 0L))
  # the values still read off the surviving consumer mappings
  set.seed(42)
  expect_identical(df$a[], runif(40000))
  set.seed(43)
  expect_identical(df$b[], runif(40000))
})

test_that("arrow_ptype keeps dictionary and int64 column forms", {
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("bit64")
  sch <- nanoarrow::infer_nanoarrow_schema(
    data.frame(f = factor("a"), i = bit64::as.integer64(1))
  )
  expect_identical(mizu:::arrow_ptype(sch, TRUE)[[1L]], factor())
  expect_identical(
    mizu:::arrow_ptype(sch, FALSE)[[2L]],
    bit64::integer64()
  )
})

test_that("a uint64 column crosses verbatim past the gate", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("bit64")
  uv <- bit64::as.integer64(rep(1, 9000))
  uv[2] <- bit64::as.integer64("9223372036854775807")
  orig <- arrow::Array$create(uv, type = arrow::uint64())
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, arrow::arrow_table(u = orig))
  df <- mizu_recv(p$peer, 5)
  expect_true(is_view(df$u))
  s <- mizu_as_arrow(df)
  expect_identical(s$get_schema()$children$u$format, "L")
  back <- arrow::as_arrow_table(s)$u
  expect_identical(
    back$cast(arrow::utf8())$as_vector(),
    orig$cast(arrow::utf8())$as_vector()
  )
})

test_that("a clean large_string column crosses past the gate", {
  skip_if_not_installed("arrow")
  x <- paste0("s", seq_len(6000))
  tab <- arrow::arrow_table(
    l = arrow::Array$create(x)$cast(arrow::large_utf8())
  )
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, tab)
  expect_identical(mizu_recv(p$peer, 5), data.frame(l = x))
})

test_that("a clean bool column crosses past the gate", {
  skip_if_not_installed("arrow")
  x <- rep(c(TRUE, FALSE), 2500)
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, arrow::arrow_table(b = x))
  expect_identical(mizu_recv(p$peer, 5), data.frame(b = x))
})

test_that("a column with a run of 64+ nulls crosses", {
  skip_if_not_installed("arrow")
  x <- rep(c(2.5, rep(NA_real_, 65)), 80)
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, arrow::arrow_table(x = x))
  expect_identical(mizu_recv(p$peer, 5), data.frame(x = x))
})

test_that("a sliced table crosses with its column offsets", {
  skip_if_not_installed("arrow")
  df <- data.frame(
    x = c(1.5, rep(c(2.5, NA), 4000)),
    s = c("skip", rep(c("a", "b", NA), length.out = 8000))
  )
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, arrow::arrow_table(df)$Slice(1))
  expect_identical(
    mizu_recv(p$peer, 5),
    data.frame(x = df$x[-1L], s = df$s[-1L])
  )
})

test_that("a factor with a single level crosses", {
  skip_if_not_installed("arrow")
  f <- factor(rep("a", 8000), levels = "a")
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, arrow::arrow_table(f = f))
  expect_identical(mizu_recv(p$peer, 5), data.frame(f = f))
})

test_that("dictionary index widths round-trip", {
  skip_if_not_installed("arrow")
  f <- factor(rep(c("a", "b", NA), 3000), levels = c("a", "b"))
  p <- channel_pair()
  on.exit(channel_end(p))
  for (ty in list(
    arrow::int8(),
    arrow::uint8(),
    arrow::int16(),
    arrow::uint16(),
    arrow::uint32(),
    arrow::int64(),
    arrow::uint64()
  )) {
    da <- arrow::Array$create(f)$cast(
      arrow::dictionary(index_type = ty, value_type = arrow::utf8())
    )
    mizu_send(p$host, arrow::arrow_table(v = da))
    expect_identical(mizu_recv(p$peer, 5), data.frame(v = f))
  }
})

test_that("dictionary declines name the reason", {
  skip_if_not_installed("arrow")
  p <- channel_pair()
  on.exit(channel_end(p))
  iv <- arrow::arrow_table(
    v = arrow::DictionaryArray$create(rep(c(0L, 1L), 4000), c(10L, 20L))
  )
  expect_snapshot(mizu_send(p$host, iv), error = TRUE)
  nl <- arrow::arrow_table(
    v = arrow::DictionaryArray$create(rep(c(0L, 1L), 4000), c("a", NA))
  )
  expect_snapshot(mizu_send(p$host, nl), error = TRUE)
  rb1 <- arrow::record_batch(
    data.frame(f = factor(rep(c("a", "b"), 2500), levels = c("a", "b")))
  )
  rb2 <- arrow::record_batch(
    data.frame(f = factor(rep(c("c", "d"), 2500), levels = c("c", "d")))
  )
  expect_snapshot(
    mizu_send(p$host, arrow::RecordBatchReader$create(rb1, rb2)),
    error = TRUE
  )
  mizu_send(p$host, 1:5)
  expect_identical(mizu_recv(p$peer, 5), 1:5)
})

test_that("a binary column declines", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  sch <- nanoarrow::na_struct(list(v = nanoarrow::na_binary()))
  parent <- nanoarrow::nanoarrow_array_init(sch)
  parent <- nanoarrow::nanoarrow_array_modify(
    parent,
    list(
      children = list(nanoarrow::as_nanoarrow_array(
        rep(list(as.raw(1:9)), 3000),
        nanoarrow::na_binary()
      )),
      length = 3000
    ),
    validate = FALSE
  )
  nanoarrow::nanoarrow_array_set_schema(parent, sch, validate = FALSE)
  s <- nanoarrow::basic_array_stream(
    list(parent),
    schema = sch,
    validate = FALSE
  )
  p <- channel_pair()
  on.exit(channel_end(p))
  expect_snapshot(mizu_send(p$host, s), error = TRUE)
  mizu_send(p$host, 1:5)
  expect_identical(mizu_recv(p$peer, 5), 1:5)
})

test_that("a zero-column table declines", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  mk <- function() {
    sch <- nanoarrow::na_struct(list())
    parent <- nanoarrow::nanoarrow_array_init(sch)
    parent <- nanoarrow::nanoarrow_array_modify(
      parent,
      list(length = 5),
      validate = FALSE
    )
    nanoarrow::nanoarrow_array_set_schema(parent, sch, validate = FALSE)
    nanoarrow::basic_array_stream(list(parent), schema = sch, validate = FALSE)
  }
  p <- channel_pair()
  on.exit(channel_end(p))
  expect_error(mizu_send(p$host, mk()), class = "mizu_error_not_portable")
  expect_snapshot(mizu_send(p$host, mk()), error = TRUE)
  mizu_send(p$host, 1:5)
  expect_identical(mizu_recv(p$peer, 5), 1:5)
})

test_that("the export degrades a column whose root header changed", {
  skip_if_not_installed("nanoarrow")
  x <- runif(50000)
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  rw <- .Call(mizu:::mizu_region_open, .Call(mizu:::mizu_zc_view_name, v), TRUE)
  .Call(
    # the wire type word no longer matches
    mizu:::mizu_poke,
    rw,
    4L,
    writeBin(13L, raw(), size = 4L, endian = "little")
  )
  df <- data.frame(a = v)
  ex <- arrow_export(df)
  expect_identical(ex$kinds, 0L)
  rm(df, ex)
  channel_end(p)
  p <- channel_pair()
  on.exit(channel_end(p), add = TRUE)
  mizu_send(p$host, x)
  v <- mizu_recv(p$peer, 5)
  rw <- .Call(mizu:::mizu_region_open, .Call(mizu:::mizu_zc_view_name, v), TRUE)
  .Call(mizu:::mizu_poke, rw, 0L, as.raw(0L)) # unknown magic
  df <- data.frame(a = v)
  ex <- arrow_export(df)
  expect_identical(ex$kinds, 0L)
  expect_identical(df$a[], x) # the values still read off the consumer mapping
})

test_that("the export degrades a remote column whose path has no entry", {
  skip_if_not_installed("nanoarrow")
  x <- runif(50000)
  fx <- remote_leaf_pair(x)
  on.exit(channel_end(fx[["p"]]))
  poke_remote(fx, len = length(x))
  col <- fx[["yb"]][[1L]]
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_zc_view_name, fx[["ya"]]),
    TRUE
  )
  # the referenced region's directory no longer reaches the column's leaf
  .Call(mizu:::mizu_poke, rw, 4L, raw(4L))
  ex <- arrow_export(list(col))
  expect_identical(ex$kinds, 0L)
  expect_identical(col[], x) # the extraction's own mapping still reads
})

test_that("the bail converts int64- and uint64-index dictionary tables", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("bit64")
  f <- factor(rep(c("a", "b", NA), 3000), levels = c("a", "b"))
  tab_of <- function(ty) {
    da <- arrow::Array$create(f)$cast(
      arrow::dictionary(index_type = ty, value_type = arrow::utf8())
    )
    arrow::arrow_table(v = da, w = bit64::as.integer64(1:9000))
  }
  # a dictionary child never takes the index's int64 ptype
  st <- nanoarrow::as_nanoarrow_array_stream(tab_of(arrow::int64()))
  sch <- st$get_schema()
  expect_identical(class(mizu:::arrow_ptype(sch, FALSE)[[1L]]), "character")
  expect_identical(
    mizu:::arrow_ptype(sch, FALSE)[[2L]],
    bit64::integer64()
  )
  for (ty in list(arrow::int64(), arrow::uint64())) {
    st <- nanoarrow::as_nanoarrow_array_stream(tab_of(ty))
    out <- mizu:::arrow_batches_to_value(
      st$get_schema(),
      nanoarrow::collect_array_stream(st)
    )
    expect_identical(out, data.frame(v = f, w = bit64::as.integer64(1:9000)))
  }
})
