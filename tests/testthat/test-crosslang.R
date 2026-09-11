test_that("a Python peer echoes vectors and strings across the tiers", {
  py <- skip_if_no_pyrei()
  ch <- rei_channel(
    py_echo,
    launcher = rei_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  x <- c(1.5, 2.5, 3.5)
  rei_send(ch, x)
  expect_identical(rei_recv(ch, 30), x)
  big <- as.numeric(seq_len(200000)) + 0
  rei_send(ch, big)
  expect_identical(rei_recv(ch, 30), big)
  i <- seq_len(100L) + 0L
  rei_send(ch, i)
  expect_identical(rei_recv(ch, 30), i)
  r <- as.raw(0:255)
  rei_send(ch, r)
  expect_identical(rei_recv(ch, 30), r)
  rei_send(ch, "hello")
  expect_identical(rei_recv(ch, 30), "hello")
  rei_send(ch, NA_character_)
  expect_null(rei_recv(ch, 30))
  expect_true(rei_close(ch, timeout = 10))
})

test_that("a foreign payload is declined and consumed, not wedging the ring", {
  py <- skip_if_no_pyrei()
  ch <- rei_channel(
    "
import pyrei, numpy as np
ch.send({1, 2, 3})
ch.send(42)
ch.send(np.array([1.5]))
",
    launcher = rei_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_error(rei_recv(ch, 30), "Python payload")
  expect_error(rei_recv(ch, 30), "Python payload")
  expect_identical(rei_recv(ch, 30), 1.5)
  expect_true(rei_close(ch, timeout = 10))
})

test_that("a batch receive raises on a foreign payload and stays usable", {
  py <- skip_if_no_pyrei()
  ch <- rei_channel(
    "
import pyrei, numpy as np
ch.send(42)
ch.recv(30)
ch.send(np.array([2.0]))
",
    launcher = rei_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  expect_error(rei_recv_batch(ch, 2L, 30), "Python payload")
  rei_send(ch, 0)
  expect_identical(rei_recv(ch, 30), 2.0)
  expect_true(rei_close(ch, timeout = 10))
})
