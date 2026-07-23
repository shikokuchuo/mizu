# SPSC ring mechanics over an in-process pair: slot kinds (INLINE, the RAW
# family, ARENA, SHM_RAW), batched publication, the spill arena's FIFO
# byte-ring, back-pressure, and the keeper lifetime discipline.

test_that("the RAW fast path round-trips atomic vectors byte-identically", {
  p <- channel_pair()
  for (x in list(c(1.5, 2.5, NA_real_, Inf, -Inf, NaN),
                 c(1L, NA_integer_, -2147483647L),
                 c(TRUE, FALSE, NA),
                 as.raw(0:255)[1:100],
                 complex(real = c(1, NA), imaginary = c(-1, 2)),
                 integer(0),
                 3.14)) {
    kio_send(p$host, x)
    kio_flush(p$host)
    expect_identical(kio_recv(p$peer, 5), x)
  }
})

test_that("attributes, S4, and ALTREP take the serialize path and survive", {
  p <- channel_pair()
  x <- c(a = 1, b = 2)                     # attributes -> INLINE
  kio_send(p$host, x); kio_flush(p$host)
  expect_identical(kio_recv(p$peer, 5), x)

  m <- matrix(1:4, 2)                      # dim attribute
  kio_send(p$host, m); kio_flush(p$host)
  expect_identical(kio_recv(p$peer, 5), m)

  cs <- 1:10                               # ALTREP compact sequence
  kio_send(p$host, cs); kio_flush(p$host)
  expect_identical(kio_recv(p$peer, 5), 1:10)
})

test_that("INLINE carries arbitrary R objects, NULL included", {
  p <- channel_pair()
  for (x in list(list(a = 1L, b = "two", c = list(3)),
                 "a string",
                 c("multi", NA, "élément"),
                 quote(f(x, y)),
                 NULL,
                 factor(c("a", "b")))) {
    kio_send(p$host, x)
    kio_flush(p$host)
    expect_identical(kio_recv(p$peer, 5), x)
  }
})

test_that("mid-size payloads spill to the arena and wrap its byte-ring", {
  p <- channel_pair(capacity = 8L, arena_size = 4096)
  x <- raw(1000)                            # + attr -> ~1KB serialized
  attr(x, "label") <- "spilled"
  # 50 send/recv cycles push the alloc cursor through several wraps and
  # exercise the straddle pad + reap-driven free cursor
  for (i in 1:50) {
    expect_true(kio_send(p$host, x))
    kio_flush(p$host)
    expect_identical(kio_recv(p$peer, 5), x)
  }
})

test_that("payloads past the arena fall back to fresh regions (SHM_RAW)", {
  p <- channel_pair(arena_size = 4096)
  big <- runif(10000)                       # ~80KB > arena
  kio_send(p$host, big)
  kio_flush(p$host)
  expect_identical(kio_recv(p$peer, 5), big)
})

test_that("a disabled arena sends every spill through a region", {
  p <- channel_pair(arena_size = 0)
  x <- as.list(1:200)                       # > inline budget
  kio_send(p$host, x)
  kio_flush(p$host)
  expect_identical(kio_recv(p$peer, 5), x)
})

test_that("in-flight spill exhausting the arena degrades, never errors", {
  p <- channel_pair(capacity = 16L, arena_size = 4096)
  x <- raw(1000)
  attr(x, "pad") <- "x"
  # unflushed sends cannot be reaped: the fourth chunk exhausts 4096 bytes
  # and must fall back to a region create, not an error
  for (i in 1:8) expect_true(kio_send(p$host, x))
  kio_flush(p$host)
  for (i in 1:8) expect_identical(kio_recv(p$peer, 5), x)
})

test_that("a full ring surfaces kio_full and frees on the consumer's drain", {
  p <- channel_pair(capacity = 4L)
  for (i in 1:4) expect_true(kio_send(p$host, i))
  expect_s3_class(kio_send(p$host, 5L), "kio_full")
  kio_flush(p$host)
  # a partial drain publishes nothing (head moves every K, on drain-empty,
  # and before parking — not per message), so the ring still reads full
  expect_identical(kio_recv(p$peer, 5), 1L)
  expect_s3_class(kio_send(p$host, 5L), "kio_full")
  # draining to empty publishes; the producer's reap then frees all slots
  for (i in 2:4) expect_identical(kio_recv(p$peer, 5), i)
  expect_true(kio_send(p$host, 5L))
})

test_that("messages stay unpublished until flush", {
  p <- channel_pair()
  kio_send(p$host, "staged")
  expect_s3_class(kio_recv(p$peer, 0), "kio_timeout")
  kio_flush(p$host)
  expect_identical(kio_recv(p$peer, 5), "staged")
})

test_that("batch verbs amortize the call boundary", {
  p <- channel_pair(capacity = 64L)
  expect_identical(kio_send_batch(p$host, as.list(1:50)), 50L)
  got <- kio_recv_batch(p$peer, n = 64L, timeout = 5)
  expect_identical(got, as.list(1:50))
  # a full ring stops the batch at the count accepted
  expect_identical(kio_send_batch(p$host, as.list(1:100)), 64L)
  expect_length(kio_recv_batch(p$peer, n = 32L, timeout = 5), 32L)
  expect_length(kio_recv_batch(p$peer, n = 200L, timeout = 5), 32L)
  # sentinel discipline matches recv
  expect_s3_class(kio_recv_batch(p$peer, n = 200L, timeout = 0), "kio_timeout")
  expect_error(kio_recv_batch(p$peer, n = 0L, timeout = 0), "at least 1")
  expect_error(kio_send_batch(p$host, "not a list"), "expected a list")
})

test_that("keepers pin sent payloads across the sender's GC", {
  p <- channel_pair(arena_size = 0)
  big <- runif(100000)
  csum <- sum(big)
  kio_send(p$host, big)                     # SHM_RAW: region + keeper
  kio_flush(p$host)
  rm(big)
  gc()                                      # keeper is the only reference
  y <- kio_recv(p$peer, 5)
  expect_identical(sum(y), csum)
})

test_that("pure-spin mode moves messages without parking", {
  p <- channel_pair(spin = TRUE)
  expect_true(.Call(kioto:::kio_channel_stat, p$host)$spin)
  kio_send(p$host, 42L)
  kio_flush(p$host)
  expect_identical(kio_recv(p$peer, 5), 42L)
  expect_s3_class(kio_recv(p$peer, 0.1), "kio_timeout")
})

test_that("mori-shared payloads ride the hooks and map zero-copy", {
  skip_if_not_installed("mori")
  p <- channel_pair()
  x <- mori::share(runif(1000))
  kio_send(p$host, x)
  kio_flush(p$host)
  y <- kio_recv(p$peer, 5)
  expect_true(mori::is_shared(y))
  expect_identical(mori::shared_name(y), mori::shared_name(x))
  expect_identical(as.numeric(y), as.numeric(x))

  # embedded inside a larger payload, and sub-object references too
  l <- mori::share(list(a = runif(100), b = 1:10))
  kio_send(p$host, list(wrapped = l$a, tag = "x"))
  kio_flush(p$host)
  z <- kio_recv(p$peer, 5)
  expect_true(mori::is_shared(z$wrapped))
  expect_identical(as.numeric(z$wrapped), as.numeric(l$a))
})
