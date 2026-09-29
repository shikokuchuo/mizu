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

test_that("a foreign payload is declined and consumed, not wedging the ring", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    "
import pymizu, numpy as np
ch.send({1, 2, 3})
ch.send(42)
ch.send(np.array([1.5]))
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_error(mizu_recv(ch, 30), class = "mizu_error_python_payload")
  expect_error(mizu_recv(ch, 30), class = "mizu_error_python_payload")
  expect_identical(mizu_recv(ch, 30), 1.5)
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a batch receive raises on a foreign payload and stays usable", {
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
  expect_error(mizu_recv_batch(ch, 2L, 30), class = "mizu_error_python_payload")
  mizu_send(ch, 0)
  expect_identical(mizu_recv(ch, 30), 2.0)
  expect_true(mizu_close(ch, timeout = 10))
})

test_that("a batch receive keeps messages read before a foreign payload", {
  py <- skip_if_no_pymizu()
  ch <- mizu_channel(
    "
ch.send('a')
ch.send({1, 2})
ch.send('c')
ch.recv(30)
",
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_identical(mizu_recv_batch(ch, 3L, 30), list("a"))
  expect_error(mizu_recv(ch, 30), class = "mizu_error_python_payload")
  expect_identical(mizu_recv(ch, 30), "c")
  mizu_send(ch, 0)
  expect_true(mizu_close(ch, timeout = 10))
})
