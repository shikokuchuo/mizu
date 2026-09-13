# The map region .Call surface: staging validation, the worker-side header
# validator (checked at attach, before any shared word is touched), and the
# slice / write / gather / swap guards. Corruption uses the region peek /
# poke test surface, as test-preamble.R does for channel regions.

test_that("staging validates geometry before creating anything", {
  desc <- list(identity, list())
  expect_error(
    .Call(rei:::rei_map_stage, desc, NULL, NULL, 0, NULL, 1),
    "invalid map length"
  )
  expect_error(
    .Call(rei:::rei_map_stage, desc, NULL, NULL, 4, NULL, 8),
    "invalid map morsel size"
  )
  # a character template has no fixed element size
  expect_error(
    .Call(rei:::rei_map_stage, desc, NULL, NULL, 4, "chr", 1),
    "invalid map template"
  )
  # sizes past the 2^46 budget refuse before any allocation
  expect_error(
    .Call(rei:::rei_map_stage, desc, NULL, NULL, 9e15, numeric(1), 1),
    "map region too large"
  )
  expect_error(
    .Call(rei:::rei_map_stage, desc, NULL, 2^47, 4, NULL, 1),
    "map region too large"
  )
  # an in-budget size no platform can back fails at create — or, where a
  # sparse filesystem accepts it, at the descriptor write-back check
  expect_error(
    .Call(rei:::rei_map_stage, desc, NULL, 2^45, 4, NULL, 1),
    "cannot create map region|descriptor size changed"
  )
})

test_that("attach validates each corrupted map header field", {
  st <- .Call(
    rei:::rei_map_stage,
    list(identity, list()),
    seq_len(8) + 0,
    NULL,
    8,
    1.0,
    2
  )
  nm <- st[[1L]]
  rw <- .Call(rei:::rei_region_open, nm, TRUE)
  hdr <- .Call(rei:::rei_peek, rw, 0, 128)
  corrupt <- function(off, bytes, msg) {
    .Call(rei:::rei_poke, rw, off, as.raw(bytes))
    expect_error(.Call(rei:::rei_map_open, nm, TRUE), msg)
    .Call(rei:::rei_poke, rw, 0, hdr)
  }
  corrupt(0, 0, "bad magic")
  corrupt(4, 99, "ABI version mismatch")
  corrupt(32, 0, "element count out of range") # n -> 0
  corrupt(40, 0, "descriptor lies outside") # desc_off -> 0
  corrupt(88, 0, "morsel geometry is inconsistent") # morsel_size -> 0
  corrupt(28, 0, "morsel state section") # claim_n -> 0
  corrupt(64, 1, "x section lies outside") # x_len mismatch
  corrupt(12, 7, "unknown x section kind")
  corrupt(80, 0, "output area lies outside") # out_m -> 0
  # the intact header still admits the passive read-only attach
  ro <- .Call(rei:::rei_map_open, nm, FALSE)
  expect_identical(.Call(rei:::rei_map_info, ro)[["cursor"]], 0)
  expect_error(
    .Call(rei:::rei_map_open, "/rei_nonexistent_0", TRUE),
    "cannot open map region"
  )
})

test_that("slice, write, gather and swap guard their preconditions", {
  desc <- list(identity, list())
  # an x-section region: slice works, output-area verbs refuse
  stx <- .Call(
    rei:::rei_map_stage,
    desc,
    c(1.5, 2.5, 3.5, 4.5),
    NULL,
    4,
    NULL,
    2
  )
  h <- stx[[2L]]
  expect_identical(.Call(rei:::rei_map_slice, h, 2, 3), c(2.5, 3.5))
  expect_error(.Call(rei:::rei_map_slice, h, 0, 2), "slice out of range")
  expect_error(.Call(rei:::rei_map_slice, h, 2, 5), "slice out of range")
  expect_error(.Call(rei:::rei_map_write, h, 1, 1.0), "no output area")
  expect_error(.Call(rei:::rei_map_gather, h), "no output area")
  expect_error(
    .Call(rei:::rei_map_swap_x, h, c(1, 2)),
    "must match the staged type and length"
  )
  # a descriptor-carried x has no x section to slice or swap
  std <- .Call(
    rei:::rei_map_stage,
    list(identity, list(), as.list(1:4)),
    NULL,
    NULL,
    4,
    as.raw(0),
    2
  )
  h2 <- std[[2L]]
  expect_error(.Call(rei:::rei_map_slice, h2, 1, 2), "no x section")
  expect_error(.Call(rei:::rei_map_swap_x, h2, c(1, 2)), "no x section")
  expect_error(
    .Call(rei:::rei_map_write, h2, 9, as.raw(1)),
    "element index out of range"
  )
  expect_error(.Call(rei:::rei_map_write, h2, 1, 1L), "type 'raw'")
  expect_error(.Call(rei:::rei_map_info, new.env()), "not a map handle")
  expect_error(
    .Call(rei:::rei_map_claim_state, h2, 99L),
    "runner ordinal out of range"
  )
  # cancel-set is total: a non-handle no-ops instead of raising
  expect_null(.Call(rei:::rei_map_cancel_set, new.env()))
})

test_that("an armed trim reads a flipped claim as abandoned thereafter", {
  st <- .Call(
    rei:::rei_map_stage,
    list(identity, list()),
    NULL,
    NULL,
    4,
    NULL,
    2
  )
  h <- st[[2L]]
  .Call(rei:::rei_map_cancel_set, h) # arms the trim trigger
  expect_identical(.Call(rei:::rei_map_abandon, h, 0L), 2L)
  # the second read finds the word already flipped
  expect_identical(.Call(rei:::rei_map_abandon, h, 0L), 2L)
  expect_identical(
    .Call(rei:::rei_map_claim_state, h, 0L)[["state"]],
    "abandoned"
  )
})

test_that("the map batch entry validates its arguments", {
  eic <- numeric(1)
  env <- new.env()
  expect_error(
    .Call(
      rei:::rei_map_batch,
      NULL,
      identity,
      NULL,
      1:10,
      0,
      0,
      5,
      NULL,
      eic,
      env
    ),
    "invalid map batch range"
  )
  expect_error(
    .Call(
      rei:::rei_map_batch,
      NULL,
      identity,
      1,
      1:10,
      0,
      1,
      5,
      NULL,
      eic,
      env
    ),
    "invalid map dots"
  )
  expect_error(
    .Call(
      rei:::rei_map_batch,
      NULL,
      identity,
      NULL,
      1:10,
      0,
      1,
      5,
      NULL,
      "x",
      env
    ),
    "invalid element-index cell"
  )
  expect_error(
    .Call(
      rei:::rei_map_batch,
      NULL,
      identity,
      NULL,
      1:10,
      0,
      1,
      5,
      NULL,
      eic,
      NULL
    ),
    "invalid evaluation environment"
  )
  expect_error(
    .Call(
      rei:::rei_map_batch,
      NULL,
      identity,
      NULL,
      1:10,
      0,
      1,
      5,
      1L,
      eic,
      env
    ),
    "invalid RNG stream state"
  )
})

test_that("map_open and gather_view validate their inputs", {
  expect_error(
    .Call(rei:::rei_map_open, 1L, TRUE),
    "expected a map region name"
  )
  st <- .Call(
    rei:::rei_map_stage,
    list(identity, list()),
    seq_len(8) + 0,
    NULL,
    8,
    NULL,
    2
  )
  h <- .Call(rei:::rei_map_open, st[[1L]], TRUE)
  expect_error(
    .Call(rei:::rei_map_gather_view, h, NULL, NULL),
    "no output area"
  )
})

test_that("splice places batch values by element position", {
  out <- vector("list", 10L)
  results <- list(
    list(c(0, 2), c(1, 1), list(list("a", "b"), list("c", "d"))),
    list(1, 1, list(list("x", "y")))
  )
  .Call(rei:::rei_map_splice, out, results, 2)
  expect_identical(
    out,
    c(list("a", "b", "x", "y", "c", "d"), rep(list(NULL), 4L))
  )
  # a batch range or length that disagrees with the geometry is corruption
  expect_error(
    .Call(rei:::rei_map_splice, out, list(list(0, 2, list(list("a")))), 2),
    "map batch result length mismatch"
  )
  expect_error(
    .Call(rei:::rei_map_splice, out, list(list("a")), 2),
    "invalid map runner result"
  )
})
