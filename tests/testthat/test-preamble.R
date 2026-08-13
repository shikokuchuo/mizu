# KIOC magic + version preamble and its validator: the peer's first read of a
# channel region, checked before any shared atomic is touched.

new_channel_region <- function() {
  xp <- .Call(kioto:::kio_region_create, 4096)
  # rings 2 * 4 * 64 = 512 B; drop slot and liveness-dir in the tail
  .Call(
    kioto:::kio_preamble_write_call,
    xp,
    4L,
    64L,
    0,
    c(2048, 100),
    c(2148, 50)
  )
  xp
}

test_that("a written preamble validates and round-trips its fields", {
  xp <- new_channel_region()
  p <- .Call(kioto:::kio_preamble_validate_call, xp)
  expect_identical(p[["version"]], 2) # ABI 2: SHM_VEC / REF payload kinds
  expect_identical(p[["cap"]], 4)
  expect_identical(p[["slot"]], 64)
  expect_identical(p[["host_pid"]], as.double(Sys.getpid()))
  expect_identical(p[["arena_size"]], 0)
  expect_identical(p[["drop_offset"]], 2048)
  expect_identical(p[["drop_size"]], 100)
  expect_identical(p[["livedir_offset"]], 2148)
  expect_identical(p[["livedir_size"]], 50)
})

test_that("a consumer validates the host-written preamble", {
  xp <- new_channel_region()
  ro <- .Call(
    kioto:::kio_region_open,
    .Call(kioto:::kio_region_name, xp),
    FALSE
  )
  p <- .Call(kioto:::kio_preamble_validate_call, ro)
  expect_identical(p[["host_pid"]], as.double(Sys.getpid()))
})

test_that("each corruption is caught before the ring protocol is engaged", {
  poke <- function(xp, off, bytes) .Call(kioto:::kio_poke, xp, off, bytes)

  xp <- new_channel_region()
  poke(xp, 0, as.raw(0)) # magic
  expect_error(.Call(kioto:::kio_preamble_validate_call, xp), "bad magic")

  xp <- new_channel_region()
  poke(xp, 4, as.raw(99)) # version
  expect_error(
    .Call(kioto:::kio_preamble_validate_call, xp),
    "ABI version mismatch"
  )

  xp <- new_channel_region()
  poke(xp, 8, as.raw(3)) # cap -> 3
  expect_error(
    .Call(kioto:::kio_preamble_validate_call, xp),
    "capacity is not a power of two"
  )

  xp <- new_channel_region()
  poke(xp, 12, as.raw(65)) # slot -> 65
  expect_error(
    .Call(kioto:::kio_preamble_validate_call, xp),
    "slot size is not a power of two"
  )

  xp <- new_channel_region()
  poke(xp, 24, as.raw(1)) # arena -> 1
  expect_error(
    .Call(kioto:::kio_preamble_validate_call, xp),
    "not a multiple of 64"
  )

  xp <- new_channel_region()
  poke(xp, 8, as.raw(c(0, 0, 0, 1))) # cap -> 2^24: rings overflow
  expect_error(.Call(kioto:::kio_preamble_validate_call, xp), "rings exceed")

  xp <- new_channel_region()
  poke(xp, 24, as.raw(c(0x00, 0x08))) # arena -> 2048: arenas overflow
  expect_error(.Call(kioto:::kio_preamble_validate_call, xp), "arenas exceed")

  xp <- new_channel_region()
  poke(xp, 36, as.raw(0x01)) # drop offset past region end
  expect_error(
    .Call(kioto:::kio_preamble_validate_call, xp),
    "drop slot lies outside"
  )

  xp <- new_channel_region()
  poke(xp, 52, as.raw(0x01)) # livedir offset past region end
  expect_error(
    .Call(kioto:::kio_preamble_validate_call, xp),
    "liveness-dir string lies outside"
  )
})

test_that("a region below the fixed layout cannot carry a preamble", {
  xp <- .Call(kioto:::kio_region_create, 256)
  expect_error(
    .Call(kioto:::kio_preamble_write_call, xp, 4L, 64L, 0, c(0, 0), c(0, 0)),
    "too small"
  )
  expect_error(
    .Call(kioto:::kio_preamble_validate_call, xp),
    "smaller than the fixed channel layout"
  )
})
