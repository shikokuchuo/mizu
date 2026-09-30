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
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("the identity exchange: both ends report foreign", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    "
assert ch._h._peer_ident() == (2, 7)   # MIZU_LANG_R, MIZS | ATTRS | MIZL
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
