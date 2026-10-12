test_that("a Python peer echoes vectors and strings across the tiers", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    py_echo,
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  x <- c(1.5, 2.5, 3.5)
  mizu_send(ch, x)
  expect_identical(mizu_recv(ch, 30), x)
  big <- as.numeric(seq_len(200000)) + 0
  mizu_send(ch, big)
  expect_identical(mizu_recv(ch, 30), big)
  i <- seq_len(100L) + 0L
  mizu_send(ch, i)
  expect_identical(mizu_recv(ch, 30), i)
  r <- as.raw(0:255)
  mizu_send(ch, r)
  expect_identical(mizu_recv(ch, 30), r)
  mizu_send(ch, "hello")
  expect_identical(mizu_recv(ch, 30), "hello")
  mizu_send(ch, NA_character_)
  expect_null(mizu_recv(ch, 30))
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a Python peer echoes a view by reference", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    py_echo,
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  x <- runif(2e5)
  mizu_send(ch, x)
  y <- mizu_recv(ch, 30)
  expect_true(.Call(mizu:::mizu_zc_view_check, y))
  expect_identical(.Call(mizu:::mizu_zc_refcount, y)[[2L]] %% 2L, 1L)
  expect_identical(y, x)
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a Python decline raises at the Python send, the channel usable", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    "
import pymizu
try:
    ch.send({1, 2, 3})
except pymizu.DeclinedError:
    ch.send('declined set')
ch.send(42)
ch.send(1.5)
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_identical(mizu_recv(ch, 30), "declined set")
  expect_identical(mizu_recv(ch, 30), 42L)
  expect_identical(mizu_recv(ch, 30), 1.5)
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a batch receive of interop values arrives whole", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    "
import pymizu, numpy as np
ch.send(42)
ch.recv(30)
ch.send(np.array([2.0]))
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_identical(mizu_recv_batch(ch, 2L, 30), list(42L))
  mizu_send(ch, 0)
  expect_identical(mizu_recv(ch, 30), 2.0)
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("Python-side declines skip nothing the peer can send", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    "
import pymizu
ch.send('a')
try:
    ch.send({1, 2})
except pymizu.DeclinedError:
    ch.send('b')
ch.send('c')
ch.recv(30)
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_identical(mizu_recv(ch, 30), "a")
  expect_identical(mizu_recv(ch, 30), "b")
  expect_identical(mizu_recv(ch, 30), "c")
  mizu_send(ch, 0)
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("R -> Python -> R relay exactness across the tag families", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    py_echo,
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  rt <- function(x, expect = x) {
    mizu_send(ch, x)
    expect_identical(mizu_recv(ch, 30), expect)
  }
  rt(NULL)
  rt(TRUE)
  rt(1L)
  rt(1.5)
  rt(1 + 2i)
  rt("héllo ✓")
  rt(as.raw(c(0x00, 0xff)))
  rt(c(1.5, 2.5))
  rt(c(1L, 2L, 3L))
  rt(c(1 + 2i, -3 + 0.5i))
  rt(list(1L, "a", list(TRUE, 2.5)))
  rt(list(a = 1L, b = list(z = NULL)))
  rt(matrix(1:6, 2, 3))
  rt(matrix(c(1 + 1i, 2 + 2i, 3 + 3i, 4 + 4i), 2))
  rt(matrix(as.raw(1:6), 2, 3))
  rt(array(1:8, c(2, 2, 2)))
  rt(list(b = c(TRUE, FALSE)))
  rt(as.Date("2022-03-21") + 0:2)
  rt(.POSIXct(c(1700000000, 1700000000.5), tz = "UTC"))
  rt(as.difftime(c(1.5, 2), units = "secs"))
  rt(as.difftime(c(1, NA), units = "secs"))
  skip_if_not_installed("bit64")
  rt(bit64::as.integer64(c(1, -1, 2^53 + 1)))
  rt(bit64::as.integer64(5))
  im <- bit64::as.integer64(1:6)
  dim(im) <- c(2L, 3L)
  rt(im)
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("R -> Python -> R documented shifts", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    py_echo,
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  rt <- function(x, expect) {
    mizu_send(ch, x)
    expect_identical(mizu_recv(ch, 30), expect)
  }
  rt(c("a", "bc"), list("a", "bc")) # a strv returns as a list
  rt(factor(c("b", "a"), levels = c("a", "b")), list("b", "a"))
  rt(NA, NULL)
  rt(NA_integer_, NULL)
  rt(NA_character_, NULL)
  rt(array(1:2, 2), 1:2) # a length-1 dim: a plain vector
  rt(.POSIXct(1700000000, tz = ""), .POSIXct(1700000000, tz = "UTC"))
  rt(
    .POSIXct(1700000000, tz = "Europe/Paris"),
    .POSIXct(1700000000, tz = "UTC")
  )
  rt(c(TRUE, NA), c(1L, NA)) # logical with NA -> integer
  # difftime units normalize to seconds across the boundary
  rt(
    as.difftime(c(1.5, 2.25), units = "mins"),
    as.difftime(c(90, 135), units = "secs")
  )
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a data.frame relays through the received Frame's own export", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    py_echo,
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  df <- data.frame(
    n = c(1.5, 2.5, 3.5),
    f = factor(c("a", "b", "a")),
    s = c("x", "y", "z"),
    i = 1:3,
    row.names = c("r1", "r2", "r3")
  )
  mizu_send(ch, df)
  expect_identical(mizu_recv(ch, 30), df)
  dfd <- data.frame(
    d = as.Date("2020-01-01") + 0:2,
    t = .POSIXct(c(1700000000, 1700000001, 1700000002), tz = "UTC")
  )
  mizu_send(ch, dfd)
  expect_identical(mizu_recv(ch, 30), dfd)
  # a difftime column relays, units normalized to seconds
  dft <- data.frame(
    d = as.difftime(c(1.5, 2.25, NA), units = "mins"),
    x = 1:3
  )
  mizu_send(ch, dft)
  expect_identical(
    mizu_recv(ch, 30),
    data.frame(
      d = as.difftime(c(90, 135, NA), units = "secs"),
      x = 1:3
    )
  )
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a mutated frame relays hop-by-hop: unmodified columns as remote leaves", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  py <- skip_if_no_pymizu()
  testthat::skip_if_not(
    system2(
      py,
      c("-c", shQuote("import polars")),
      stdout = FALSE,
      stderr = FALSE
    ) ==
      0L,
    "polars not available"
  )
  ch <- mizu_channel(
    "
import polars as pl
f = ch.recv(60)
df = pl.DataFrame(f).with_columns((pl.col('x') * 2).alias('x'))
ch.send(df)
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  df <- data.frame(i = 1:300000, x = runif(300000), l = 1:300000)
  mizu_send(ch, df)
  y <- mizu_recv(ch, 60)
  # the unmodified columns: views over the sent frame's own region —
  # views first, refcount/REFHELD before any identical() (R < 4.6
  # materializes views on deep comparison)
  expect_true(.Call(mizu:::mizu_zc_view_check, y[["i"]]))
  expect_true(.Call(mizu:::mizu_zc_view_check, y[["l"]]))
  nmi <- .Call(mizu:::mizu_zc_view_name, y[["i"]])
  nml <- .Call(mizu:::mizu_zc_view_name, y[["l"]])
  expect_true(grepl("[", nmi, fixed = TRUE))
  expect_identical(sub("\\[.*", "", nmi), sub("\\[.*", "", nml)) # one source
  yroot <- .Call(mizu:::mizu_zc_view_name, y)
  expect_false(identical(sub("\\[.*", "", nmi), yroot)) # not y's own region
  rc <- .Call(mizu:::mizu_zc_refcount, y[["i"]])
  expect_true(rc[[1]] >= 2L) # the producer loan plus the resolved view
  expect_identical(rc[[2]] %% 2L, 1L) # REFHELD
  expect_identical(y[["i"]], df[["i"]])
  expect_identical(y[["l"]], df[["l"]])
  expect_identical(y[["x"]], 2 * df[["x"]]) # the computed column, laid out
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a frame with a view column crosses to Python as a remote leaf (F2.5)", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    py_echo,
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  x <- runif(300000)
  mizu_send(ch, x)
  v <- mizu_recv(ch, 30) # the echo: a view of x's own region (REF)
  df <- data.frame(a = runif(300000), b = seq_len(300000) + 0)
  df[["b"]] <- v
  mizu_send(ch, df)
  z <- mizu_recv(ch, 30) # the echoed frame: the remote leaf chained back
  zb <- z[["b"]]
  expect_true(.Call(mizu:::mizu_zc_view_check, zb))
  expect_identical(
    .Call(mizu:::mizu_zc_view_name, zb),
    .Call(mizu:::mizu_zc_view_name, v)
  )
  expect_identical(zb, x) # last: identical() materializes views on R < 4.6
  expect_identical(z[["a"]], df[["a"]])
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a foreign MIZS write bulk-fills UTF-8 marks, zero at NAs", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    py_echo,
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  x <- rep(c("alpha", "beta", NA_character_), length.out = 20000)
  mizu_send(ch, x)
  y <- mizu_recv(ch, 30) # the echo: a view of x's own region (REF)
  expect_true(.Call(mizu:::mizu_zc_view_check, y))

  n <- length(x)
  peek <- view_peek(y)
  off_enc <- 64 + align64(ceiling(n / 8)) + align64(8 * (n + 1))
  expect_identical(as.integer(peek(off_enc, n)), as.integer(!is.na(x)))
  expect_identical(y, x) # last: identical() materializes views on R < 4.6
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("the identity exchange: both ends report foreign", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    "
assert ch._h._peer_ident() == (2, 31)   # MIZU_LANG_R, MIZS | ATTRS | MIZL | TASKREF | MIZL_REF
ch.send('ok')
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_identical(mizu_recv(ch, 30), "ok")
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a Python peer error reaches R as a mizu_error_remote value", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    "
ch.send(1.5)
raise ValueError('boom')
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_identical(mizu_recv(ch, 30), 1.5)
  e <- mizu_recv(ch, 30)
  expect_s3_class(e, "mizu_error_remote")
  expect_identical(e[["remote_type"]], "ValueError")
  expect_identical(e[["message"]], "boom")
  expect_true(nzchar(e[["detail"]])) # the Python traceback text
  expect_s3_class(mizu_recv(ch, 30), "mizu_closed")
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a Python peer's sys.exit is an orderly close, no error value", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    "
import sys
ch.send(1.5)
sys.exit(3)
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_identical(mizu_recv(ch, 30), 1.5)
  expect_s3_class(mizu_recv(ch, 30), "mizu_closed")
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("an Arrow table crosses to Python as a Frame and echoes back", {
  py <- skip_if_no_pymizu()
  skip_if_no_pyarrow(py)
  skip_if_not_installed("arrow")
  ch <- mizu_channel(
    "
import pymizu
import pyarrow as pa
f = ch.recv(30)
t = pa.table(f)
ok = (
    t.num_rows == 3
    and t.column_names == ['i', 's']
    and t.column('i').type == pa.int32()
    and t.column('s').type == pa.string()
    and t.column('i').to_pylist() == [1, None, 3]
    and t.column('s').to_pylist() == ['x', 'y', None]
)
ch.send(ok)
ch.send(t)
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  df <- data.frame(i = c(1L, NA, 3L), s = c("x", "y", NA))
  mizu_send(ch, arrow::arrow_table(df))
  expect_true(mizu_recv(ch, 30))
  expect_identical(mizu_recv(ch, 30), df)
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a Python peer mutates the frame and returns it", {
  py <- skip_if_no_pymizu()
  skip_if_no_pyarrow(py)
  skip_if_not_installed("arrow")
  ch <- mizu_channel(
    "
import pymizu
import pyarrow as pa
import numpy as np
f = ch.recv(30)
t = pa.table(f)
t = t.append_column('y', pa.array(np.asarray(t['x']) * 2))
ch.send(t)
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  df <- data.frame(x = c(1.5, 2.5, 3.5))
  mizu_send(ch, arrow::arrow_table(df))
  expect_identical(mizu_recv(ch, 30), cbind(df, y = df$x * 2))
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("Python Arrow sends read as data.frames (front-end and Frame)", {
  py <- skip_if_no_pymizu()
  skip_if_no_pyarrow(py)
  ch <- mizu_channel(
    "
import pymizu
import pyarrow as pa
ch.send(pa.table({'x': [1.5, 2.5], 's': ['a', None]}))
t = pa.table({'t': pa.array([1704067200500000, None], type=pa.timestamp('us'))})
ch.send(pymizu.Frame.from_arrow(t))
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_identical(
    mizu_recv(ch, 30),
    data.frame(x = c(1.5, 2.5), s = c("a", NA))
  )
  expect_identical(
    mizu_recv(ch, 30),
    data.frame(
      t = as.POSIXct(
        c(1704067200.5, NA_real_),
        origin = "1970-01-01",
        tz = "UTC"
      )
    )
  )
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a received Python frame exports to Arrow zero-copy", {
  py <- skip_if_no_pymizu()
  skip_if_no_pyarrow(py)
  skip_if_not_installed("arrow")
  ch <- mizu_channel(
    "
import pyarrow as pa
t = pa.table({
    'x': [1.5, None, 3.5] * 3000,
    'i': pa.array([1, None, 3] * 3000, type=pa.int32()),
    's': ['a', 'b', None] * 3000,
    't': pa.array([1704067200500000, None, 1704067200500002] * 3000,
                  type=pa.timestamp('us')),
})
ch.send(t)
ch.recv(30)
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  df <- mizu_recv(ch, 30)
  st <- mizu_as_arrow(df)
  tab <- arrow::as_arrow_table(st)
  types <- vapply(tab$schema$fields, function(f) f$type$ToString(), "")
  expect_identical(
    unname(types),
    c("double", "int32", "large_string", "timestamp[us, tz=UTC]")
  )
  back <- as.data.frame(tab)
  expect_identical(nrow(back), 9000L)
  expect_identical(sum(is.na(back$x)), 3000L)
  expect_identical(sum(is.na(back$t)), 3000L)
  expect_identical(
    back$t[1],
    as.POSIXct(1704067200.5, origin = "1970-01-01", tz = "UTC")
  )
  mizu_send(ch, 0L)
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("mizu_as_arrow borrows pymizu's matched remote columns", {
  skip_on_os("linux") # multiple live views trip the churn fallback there
  py <- skip_if_no_pymizu()
  skip_if_no_pyarrow(py)
  skip_if_not_installed("arrow")
  ch <- mizu_channel(
    "
import pyarrow as pa
import pyarrow.compute as pc
f = ch.recv(60)
t = pa.table(f)                    # the registered export
x2 = pc.multiply(t.column('x'), 2)
t2 = t.set_column(t.schema.get_field_index('x'), 'x', x2)
ch.send(t2)                        # i and l cross as remote leaves
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  df <- data.frame(i = 1:300000, x = runif(300000), l = 1:300000)
  mizu_send(ch, arrow::arrow_table(df))
  y <- mizu_recv(ch, 60)
  expect_true(.Call(mizu:::mizu_zc_view_check, y[["i"]]))
  expect_true(.Call(mizu:::mizu_zc_view_check, y[["l"]]))
  invisible(y[["x"]]) # resolve + cache before the base read
  base <- rc_of(y[["i"]])[1L]
  st <- mizu_as_arrow(y)
  # one export loan on the sent frame's region across both matched columns
  expect_identical(rc_of(y[["i"]])[1L], base + 1L)
  tab <- arrow::as_arrow_table(st)
  back <- as.data.frame(tab)
  expect_identical(back[["i"]], df[["i"]])
  expect_identical(back[["l"]], df[["l"]])
  expect_identical(back[["x"]], 2 * df[["x"]])
  rm(st, tab, back)
  expect_true(mizu_close(ch, timeout = 10))
})
