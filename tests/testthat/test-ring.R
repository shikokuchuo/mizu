# SPSC ring mechanics over an in-process pair: slot kinds (INLINE, the RAW
# family, ARENA, SHM_RAW), batched publication, the spill arena's FIFO
# byte-ring, back-pressure, and the keeper lifetime discipline.

test_that("the RAW fast path round-trips atomic vectors byte-identically", {
  p <- channel_pair()
  for (x in list(
    c(1.5, 2.5, NA_real_, Inf, -Inf, NaN),
    c(1L, NA_integer_, -2147483647L),
    c(TRUE, FALSE, NA),
    as.raw(0:255)[1:100],
    complex(real = c(1, NA), imaginary = c(-1, 2)),
    integer(0),
    3.14
  )) {
    sora_send(p[["host"]], x)
    expect_identical(sora_recv(p[["peer"]], 5), x)
  }
})

test_that("NULL rides the NIL immediate kind in both directions", {
  p <- channel_pair()
  sora_send(p[["host"]], NULL)
  expect_null(sora_recv(p[["peer"]], 5))
  sora_send(p[["peer"]], NULL)
  expect_null(sora_recv(p[["host"]], 5))
  # interleaved with pinning kinds, then through the batch verbs
  sora_send(p[["host"]], "inline")
  sora_send(p[["host"]], NULL)
  expect_identical(sora_recv(p[["peer"]], 5), "inline")
  expect_null(sora_recv(p[["peer"]], 5))
  expect_identical(sora_send_batch(p[["host"]], list(NULL, 1L, NULL)), 3L)
  expect_identical(sora_recv_batch(p[["peer"]], 3L, 5), list(NULL, 1L, NULL))
  channel_end(p)
})

test_that("length-1 strings ride the STR1 fast path byte-identically", {
  p <- channel_pair()
  utf8 <- "héllo"
  Encoding(utf8) <- "UTF-8"
  for (x in list("hello world", "", NA_character_, utf8)) {
    sora_send(p[["host"]], x)
    expect_identical(sora_recv(p[["peer"]], 5), x)
    sora_send(p[["peer"]], x)
    expect_identical(sora_recv(p[["host"]], 5), x)
  }
  # attributes fall through to the serialize path, value intact
  sora_send(p[["host"]], c(a = "x"))
  expect_identical(sora_recv(p[["peer"]], 5), c(a = "x"))
  # past the inline budget (slot 256) a long string spills to the arena
  long <- paste(rep("x", 300), collapse = "")
  sora_send(p[["host"]], long)
  expect_identical(sora_recv(p[["peer"]], 5), long)
  expect_identical(sora_send_batch(p[["host"]], list("a", NA_character_)), 2L)
  expect_identical(
    sora_recv_batch(p[["peer"]], 2L, 5),
    list("a", NA_character_)
  )
  channel_end(p)
})

test_that("attributes, S4, and ALTREP take the serialize path and survive", {
  p <- channel_pair()
  x <- c(a = 1, b = 2) # attributes -> INLINE
  sora_send(p[["host"]], x)
  expect_identical(sora_recv(p[["peer"]], 5), x)

  m <- matrix(1:4, 2) # dim attribute
  sora_send(p[["host"]], m)
  expect_identical(sora_recv(p[["peer"]], 5), m)

  cs <- 1:10 # ALTREP compact sequence
  sora_send(p[["host"]], cs)
  expect_identical(sora_recv(p[["peer"]], 5), 1:10)
})

test_that("INLINE carries arbitrary R objects, NULL included", {
  p <- channel_pair()
  for (x in list(
    list(a = 1L, b = "two", c = list(3)),
    "a string",
    c("multi", NA, "élément"),
    quote(f(x, y)),
    NULL,
    factor(c("a", "b"))
  )) {
    sora_send(p[["host"]], x)
    expect_identical(sora_recv(p[["peer"]], 5), x)
  }
})

test_that("mid-size payloads spill to the arena and wrap its byte-ring", {
  p <- channel_pair(capacity = 8L, arena_size = 4096)
  x <- raw(1000) # + attr -> ~1KB serialized
  attr(x, "label") <- "spilled"
  # 50 send/recv cycles push the alloc cursor through several wraps and
  # exercise the straddle pad + reap-driven free cursor
  for (i in 1:50) {
    expect_true(sora_send(p[["host"]], x))
    expect_identical(sora_recv(p[["peer"]], 5), x)
  }
})

test_that("payloads past the arena fall back to fresh regions (SHM_RAW)", {
  p <- channel_pair(arena_size = 4096)
  big <- runif(10000) # ~80KB > arena
  sora_send(p[["host"]], big)
  expect_identical(sora_recv(p[["peer"]], 5), big)
})

test_that("mid-size atomic vectors ride the arena as bare bytes (RAWSPILL)", {
  p <- channel_pair(capacity = 8L, arena_size = 8192)
  # past the 240 B inline budget, under the zc floor: no serialize pass
  for (x in list(
    runif(100), # 800 B
    1L + seq_len(1000L) * 1L, # 4 KB int
    as.raw(1:200),
    rep(c(TRUE, NA), 500),
    (1:300) + 1i
  )) {
    sora_send(p[["host"]], x)
    expect_identical(sora_recv(p[["peer"]], 5), x)
    sora_send(p[["peer"]], x)
    expect_identical(sora_recv(p[["host"]], 5), x)
  }
  # 4 KB chunks in an 8 KB arena: the byte-ring wraps every other send
  x <- runif(500)
  for (i in 1:10) {
    expect_true(sora_send(p[["host"]], x))
    expect_identical(sora_recv(p[["peer"]], 5), x)
  }
  channel_end(p)
})

test_that("an arena-full raw spill degrades to a region, never an error", {
  p <- channel_pair(capacity = 16L, arena_size = 4096)
  x <- runif(500) # 4 KB chunks: the second in-flight send overflows
  for (i in 1:4) {
    expect_true(sora_send(p[["host"]], x))
  }
  for (i in 1:4) {
    expect_identical(sora_recv(p[["peer"]], 5), x)
  }
  channel_end(p)
})

test_that("a disabled arena sends every spill through a region", {
  p <- channel_pair(arena_size = 0)
  x <- as.list(1:200) # > inline budget
  sora_send(p[["host"]], x)
  expect_identical(sora_recv(p[["peer"]], 5), x)
})

test_that("in-flight spill exhausting the arena degrades, never errors", {
  p <- channel_pair(capacity = 16L, arena_size = 4096)
  x <- raw(1000)
  attr(x, "pad") <- "x"
  # sends the consumer has not drained cannot be reaped: the fourth chunk
  # exhausts 4096 bytes and must fall back to a region create, not an error
  for (i in 1:8) {
    expect_true(sora_send(p[["host"]], x))
  }
  for (i in 1:8) {
    expect_identical(sora_recv(p[["peer"]], 5), x)
  }
})

test_that("a full ring surfaces sora_full and frees on the consumer's drain", {
  p <- channel_pair(capacity = 4L)
  for (i in 1:4) {
    expect_true(sora_send(p[["host"]], i))
  }
  expect_s3_class(sora_send(p[["host"]], 5L), "sora_full")
  # a partial drain publishes nothing (head moves every K, on drain-empty,
  # and before parking — not per message), so the ring still reads full
  expect_identical(sora_recv(p[["peer"]], 5), 1L)
  expect_s3_class(sora_send(p[["host"]], 5L), "sora_full")
  # draining to empty publishes; the producer's reap then frees all slots
  for (i in 2:4) {
    expect_identical(sora_recv(p[["peer"]], 5), i)
  }
  expect_true(sora_send(p[["host"]], 5L))
})

test_that("a send is published the moment it returns", {
  p <- channel_pair()
  sora_send(p[["host"]], "published")
  # a zero-timeout poll sees it: no flush step exists between send and recv
  expect_identical(sora_recv(p[["peer"]], 0), "published")
})

test_that("batch verbs amortize the call boundary", {
  p <- channel_pair(capacity = 64L)
  expect_identical(sora_send_batch(p[["host"]], as.list(1:50)), 50L)
  got <- sora_recv_batch(p[["peer"]], n = 64L, timeout = 5)
  expect_identical(got, as.list(1:50))
  # a full ring stops the batch at the count accepted
  expect_identical(sora_send_batch(p[["host"]], as.list(1:100)), 64L)
  expect_length(sora_recv_batch(p[["peer"]], n = 32L, timeout = 5), 32L)
  expect_length(sora_recv_batch(p[["peer"]], n = 200L, timeout = 5), 32L)
  # sentinel discipline matches recv
  expect_s3_class(
    sora_recv_batch(p[["peer"]], n = 200L, timeout = 0),
    "sora_timeout"
  )
  expect_error(sora_recv_batch(p[["peer"]], n = 0L, timeout = 0), "at least 1")
  expect_error(sora_send_batch(p[["host"]], "not a list"), "expected a list")
})

test_that("keepers pin sent payloads across the sender's GC", {
  p <- channel_pair(arena_size = 0)
  big <- runif(100000)
  csum <- sum(big)
  sora_send(p[["host"]], big) # SHM_RAW: region + keeper
  rm(big)
  gc() # keeper is the only reference
  y <- sora_recv(p[["peer"]], 5)
  expect_identical(sum(y), csum)
})

test_that("pure-spin mode moves messages without parking", {
  p <- channel_pair(spin = TRUE)
  expect_true(.Call(sora:::sora_channel_stat, p[["host"]])[["spin"]])
  sora_send(p[["host"]], 42L)
  expect_identical(sora_recv(p[["peer"]], 5), 42L)
  expect_s3_class(sora_recv(p[["peer"]], 0.1), "sora_timeout")
})

test_that("mori-shared payloads ride the hooks and map zero-copy", {
  skip_if_not_installed("mori")
  p <- channel_pair()
  x <- mori::share(runif(1000))
  sora_send(p[["host"]], x)
  y <- sora_recv(p[["peer"]], 5)
  expect_true(mori::is_shared(y))
  expect_identical(mori::shared_name(y), mori::shared_name(x))
  expect_identical(as.numeric(y), as.numeric(x))

  # embedded inside a larger payload, and sub-object references too
  l <- mori::share(list(a = runif(100), b = 1:10))
  sora_send(p[["host"]], list(wrapped = l[["a"]], tag = "x"))
  z <- sora_recv(p[["peer"]], 5)
  expect_true(mori::is_shared(z[["wrapped"]]))
  expect_identical(as.numeric(z[["wrapped"]]), as.numeric(l[["a"]]))
})
