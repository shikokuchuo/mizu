# mov_serialize_bounded: single-pass stream flipping to count-only mode on
# overflow — one pass when the payload fits, an exact total size when it
# doesn't, so the spill allocation never needs a second counting pass.

test_that("payloads within the limit serialize and round-trip in one pass", {
  x <- list(a = 1:10, b = "hello", c = c(pi, exp(1)))
  r <- .Call(mov:::mov_bounded_call, x, 4096)
  expect_type(r[[2]], "raw")
  expect_identical(length(r[[2]]), as.integer(r[[1]]))
  expect_identical(.Call(mov:::mov_unserialize_call, r[[2]]), x)
})

test_that("overflow flips to count-only and still reports the exact size", {
  x <- list(a = 1:10, b = "hello", c = c(pi, exp(1)))
  n <- .Call(mov:::mov_bounded_call, x, 0)[[1]]

  over <- .Call(mov:::mov_bounded_call, x, 16)
  expect_identical(over[[1]], n)
  expect_null(over[[2]])

  under_by_one <- .Call(mov:::mov_bounded_call, x, n - 1)
  expect_identical(under_by_one[[1]], n)
  expect_null(under_by_one[[2]])

  exact <- .Call(mov:::mov_bounded_call, x, n)
  expect_identical(exact[[1]], n)
  expect_identical(.Call(mov:::mov_unserialize_call, exact[[2]]), x)
})

test_that("a zero limit counts without writing", {
  r <- .Call(mov:::mov_bounded_call, NULL, 0)
  expect_gt(r[[1]], 0)
  expect_null(r[[2]])
})

test_that("large payloads round-trip byte-exactly", {
  x <- runif(1e5)
  r <- .Call(mov:::mov_bounded_call, x, 1e7)
  expect_identical(.Call(mov:::mov_unserialize_call, r[[2]]), x)
})

test_that("unserialize rejects non-raw input", {
  expect_error(.Call(mov:::mov_unserialize_call, "bytes"), "raw vector")
})
