# int64 as a native wire type: a class-only integer64 vector (bit64's exact
# layout, constructed without bit64) stages as bare int64 bytes on every raw
# tier — the class rides the wire tag and is re-applied at receive. NA is
# INT64_MIN both directions (documented sentinel). Attributed (names/dim)
# integer64 keeps the codec tier. Cross-language coverage (numpy int64) is
# in test-crosslang.R.

# in-process map driver, as in test-map.R
run_map <- function(p, x, f, dots = list(), ..., steps = 256L) {
  st <- rei:::map_stage(p[["ctrl"]], x, f, dots, ...)
  rei:::map_submit(p[["ctrl"]], st)
  for (i in seq_len(steps)) {
    if (pool_step(p) != 1L) break
  }
  rei:::map_collect(st, deadline = rei:::mono_time() + 30)
}

test_that("class-only integer64 round-trips inline with bit patterns intact", {
  skip_if_not_installed("bit64")
  p <- channel_pair()
  x <- bit64::as.integer64(c("0", "1", "-1", "9007199254740993"))
  rei_send(p[["host"]], x)
  expect_identical(rei_recv(p[["peer"]], 5), x)
  channel_end(p)
})

test_that("the NA sentinel (INT64_MIN) round-trips", {
  skip_if_not_installed("bit64")
  p <- channel_pair()
  x <- c(bit64::as.integer64("42"), bit64::NA_integer64_)
  rei_send(p[["host"]], x)
  y <- rei_recv(p[["peer"]], 5)
  expect_identical(y, x)
  expect_true(is.na(y[2L]))
  channel_end(p)
})

test_that("a mid-size integer64 takes the channel arena raw spill", {
  skip_if_not_installed("bit64")
  p <- channel_pair(arena_size = 65536)
  x <- bit64::as.integer64(seq_len(1000L) * 4294967296)
  rei_send(p[["host"]], x)
  expect_identical(rei_recv(p[["peer"]], 5), x)
  channel_end(p)
})

test_that("a large integer64 crosses as a classed view (SHM_VEC)", {
  skip_if_not_installed("bit64")
  p <- channel_pair()
  x <- bit64::as.integer64(seq_len(65536L)) * bit64::as.integer64(1000003)
  rei_send(p[["host"]], x)
  y <- rei_recv(p[["peer"]], 5)
  expect_true(.Call(rei:::rei_zc_view_check, y))
  expect_identical(class(y), "integer64")
  expect_identical(y, x)
  channel_end(p)
})

test_that("a re-sent integer64 view resolves by reference with its class", {
  skip_if_not_installed("bit64")
  p <- channel_pair()
  x <- bit64::as.integer64(seq_len(65536L))
  rei_send(p[["host"]], x)
  y <- rei_recv(p[["peer"]], 5)
  rei_send(p[["peer"]], y)
  z <- rei_recv(p[["host"]], 5)
  expect_true(.Call(rei:::rei_zc_view_check, z))
  expect_identical(class(z), "integer64")
  expect_identical(z, x)
  channel_end(p)
})

test_that("an integer64 view nested in a serialized payload keeps its class", {
  skip_if_not_installed("bit64")
  p <- channel_pair()
  x <- bit64::as.integer64(seq_len(65536L))
  rei_send(p[["host"]], x)
  y <- rei_recv(p[["peer"]], 5)
  rei_send(p[["peer"]], list(y, "tag"))
  w <- rei_recv(p[["host"]], 5)
  expect_identical(class(w[[1L]]), "integer64")
  expect_identical(w[[1L]], x)
  expect_identical(w[[2L]], "tag")
  channel_end(p)
})

test_that("an integer64 leaf in a list-tree view keeps its class", {
  skip_if_not_installed("bit64")
  p <- channel_pair()
  x <- bit64::as.integer64(seq_len(65536L))
  pad <- runif(40000L)
  rei_send(p[["host"]], list(v = x, pad = pad))
  y <- rei_recv(p[["peer"]], 5)
  expect_identical(class(y[["v"]]), "integer64")
  expect_identical(y[["v"]], x)
  expect_identical(y[["pad"]], pad)
  channel_end(p)
})

test_that("attributed (names) integer64 keeps the codec tier", {
  skip_if_not_installed("bit64")
  p <- channel_pair()
  x <- setNames(bit64::as.integer64(c("7", "-8")), c("a", "b"))
  rei_send(p[["host"]], x)
  y <- rei_recv(p[["peer"]], 5)
  expect_identical(y, x)
  channel_end(p)
})

test_that("integer64 is structurally valid without bit64 loaded", {
  p <- channel_pair()
  x <- structure(c(1, -2, 2^53 + 2), class = "integer64")
  rei_send(p[["host"]], x)
  y <- rei_recv(p[["peer"]], 5)
  expect_identical(y, x)
  expect_type(y, "double")
  channel_end(p)
})

test_that("pool results carry classed integer64 over the raw spill tier", {
  skip_if_not_installed("bit64")
  p <- pool_pair()
  x <- bit64::as.integer64(seq_len(100L) * 4294967296) # 800 B > 512 B slot
  t <- rei_submit(p[["ctrl"]], identity(v), v = x)
  pool_step(p)
  expect_identical(rei_collect(t, 5), x)
  pool_end(p)
})

test_that("rei_map slices classed integer64 chunks from the raw x section", {
  skip_if_not_installed("bit64")
  p <- pool_pair()
  x <- bit64::as.integer64(seq_len(100L))
  pm <- rei_map_prepare(p[["ctrl"]], x, identity)
  expect_true(pm[["st"]][["xraw"]])
  expect_identical(
    .Call(rei:::rei_map_slice, pm[["st"]][["wrap"]], 2, 5),
    x[2:5]
  )
  # raw-section results match the descriptor-path baseline (an integer64 x
  # with names rides the descriptor stream)
  res_raw <- run_map(p, x, function(xi) xi)
  res_desc <- run_map(p, setNames(x, paste0("e", seq_along(x))), function(xi) {
    xi
  })
  expect_identical(res_raw, unname(res_desc))
  pool_end(p)
})

test_that("a prepared-map x swap respects the int64 wire type", {
  skip_if_not_installed("bit64")
  p <- pool_pair()
  x1 <- bit64::as.integer64(1:6)
  pm <- rei_map_prepare(p[["ctrl"]], x1, function(v) v)
  name1 <- pm[["st"]][["name"]]
  # same wire type and length: in-place swap, same region
  rei:::map_swap_x(pm, bit64::as.integer64(6:1))
  expect_identical(pm[["st"]][["name"]], name1)
  # the C primitive rejects a plain double against an int64-staged section
  expect_error(
    .Call(rei:::rei_map_swap_x, pm[["st"]][["wrap"]], 1:6 + 0),
    "must match the staged type and length"
  )
  # and the R surface turns the int64 <-> double change into a restage
  rei:::map_swap_x(pm, 1:6 + 0)
  expect_null(pm[["st"]])
  pool_end(p)
})
