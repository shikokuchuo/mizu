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
    rei_send(p[["host"]], x)
    expect_identical(rei_recv(p[["peer"]], 5), x)
  }
})

test_that("NULL rides the NIL immediate kind in both directions", {
  p <- channel_pair()
  rei_send(p[["host"]], NULL)
  expect_null(rei_recv(p[["peer"]], 5))
  rei_send(p[["peer"]], NULL)
  expect_null(rei_recv(p[["host"]], 5))
  # interleaved with pinning kinds, then through the batch verbs
  rei_send(p[["host"]], "inline")
  rei_send(p[["host"]], NULL)
  expect_identical(rei_recv(p[["peer"]], 5), "inline")
  expect_null(rei_recv(p[["peer"]], 5))
  expect_identical(rei_send_batch(p[["host"]], list(NULL, 1L, NULL)), 3L)
  expect_identical(rei_recv_batch(p[["peer"]], 3L, 5), list(NULL, 1L, NULL))
  channel_end(p)
})

test_that("length-1 strings ride the STR1 fast path byte-identically", {
  p <- channel_pair()
  utf8 <- "héllo"
  Encoding(utf8) <- "UTF-8"
  for (x in list("hello world", "", NA_character_, utf8)) {
    rei_send(p[["host"]], x)
    expect_identical(rei_recv(p[["peer"]], 5), x)
    rei_send(p[["peer"]], x)
    expect_identical(rei_recv(p[["host"]], 5), x)
  }
  # attributes fall through to the serialize path, value intact
  rei_send(p[["host"]], c(a = "x"))
  expect_identical(rei_recv(p[["peer"]], 5), c(a = "x"))
  # past the inline budget (slot 256) a long string spills to the arena
  long <- paste(rep("x", 300), collapse = "")
  rei_send(p[["host"]], long)
  expect_identical(rei_recv(p[["peer"]], 5), long)
  expect_identical(rei_send_batch(p[["host"]], list("a", NA_character_)), 2L)
  expect_identical(
    rei_recv_batch(p[["peer"]], 2L, 5),
    list("a", NA_character_)
  )
  channel_end(p)
})

test_that("attributed and ALTREP payloads survive the channel round trip", {
  p <- channel_pair()
  x <- c(a = 1, b = 2) # attributes -> INLINE
  rei_send(p[["host"]], x)
  expect_identical(rei_recv(p[["peer"]], 5), x)

  m <- matrix(1:4, 2) # dim attribute
  rei_send(p[["host"]], m)
  expect_identical(rei_recv(p[["peer"]], 5), m)

  cs <- 1:10 # ALTREP compact sequence
  rei_send(p[["host"]], cs)
  expect_identical(rei_recv(p[["peer"]], 5), 1:10)
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
    rei_send(p[["host"]], x)
    expect_identical(rei_recv(p[["peer"]], 5), x)
  }
})

test_that("the compact codec carries the serialize subset across every tier", {
  p <- channel_pair(arena_size = 4096)
  # inline: attributed vectors, lists, calls
  for (x in list(
    c(a = 1, b = 2),
    list(a = 1L, b = "two", c = list(3)),
    quote(f(x, a = 1:3 + 0L)),
    factor(c("a", "b"))
  )) {
    rei_send(p[["host"]], x)
    expect_identical(rei_recv(p[["peer"]], 5), x)
  }
  # past the 240 B inline budget: the arena carries a codec stream
  mid <- structure(lapply(1:50, function(i) c(i, i^2)), label = "arena")
  rei_send(p[["host"]], mid)
  expect_identical(rei_recv(p[["peer"]], 5), mid)
  # past the arena: a call tree (zc-ineligible) spills as a codec SHM_RAW
  big <- as.call(c(list(quote(f)), replicate(2000, quote(x + 1))))
  rei_send(p[["host"]], big)
  expect_identical(rei_recv(p[["peer"]], 5), big)
  channel_end(p)
})

test_that("codec-ineligible payloads still cross by R_Serialize", {
  p <- channel_pair()
  f <- function(x) x + 1L # a closure
  rei_send(p[["host"]], f)
  expect_identical(rei_recv(p[["peer"]], 5)(1L), 2L)
  rei_send(p[["host"]], list(f, 1:5)) # closure and ALTREP inside a list
  y <- rei_recv(p[["peer"]], 5)
  expect_identical(y[[1]](2L), 3L)
  expect_identical(y[[2]], 1:5)
  channel_end(p)
})

test_that("mid-size payloads spill to the arena and wrap its byte-ring", {
  p <- channel_pair(capacity = 8L, arena_size = 4096)
  x <- raw(1000) # + attr -> ~1KB serialized
  attr(x, "label") <- "spilled"
  # 50 send/recv cycles push the alloc cursor through several wraps and
  # exercise the straddle pad + reap-driven free cursor
  for (i in 1:50) {
    expect_true(rei_send(p[["host"]], x))
    expect_identical(rei_recv(p[["peer"]], 5), x)
  }
})

test_that("payloads past the arena fall back to fresh regions (SHM_RAW)", {
  p <- channel_pair(arena_size = 4096)
  big <- runif(10000) # ~80KB > arena
  rei_send(p[["host"]], big)
  expect_identical(rei_recv(p[["peer"]], 5), big)
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
    rei_send(p[["host"]], x)
    expect_identical(rei_recv(p[["peer"]], 5), x)
    rei_send(p[["peer"]], x)
    expect_identical(rei_recv(p[["host"]], 5), x)
  }
  # 4 KB chunks in an 8 KB arena: the byte-ring wraps every other send
  x <- runif(500)
  for (i in 1:10) {
    expect_true(rei_send(p[["host"]], x))
    expect_identical(rei_recv(p[["peer"]], 5), x)
  }
  channel_end(p)
})

test_that("an arena-full raw spill degrades to a region, never an error", {
  p <- channel_pair(capacity = 16L, arena_size = 4096)
  x <- runif(500) # 4 KB chunks: the second in-flight send overflows
  for (i in 1:4) {
    expect_true(rei_send(p[["host"]], x))
  }
  for (i in 1:4) {
    expect_identical(rei_recv(p[["peer"]], 5), x)
  }
  channel_end(p)
})

test_that("a disabled arena sends every spill through a region", {
  p <- channel_pair(arena_size = 0)
  x <- as.list(1:200) # > inline budget
  rei_send(p[["host"]], x)
  expect_identical(rei_recv(p[["peer"]], 5), x)
})

test_that("in-flight spill exhausting the arena degrades, never errors", {
  p <- channel_pair(capacity = 16L, arena_size = 4096)
  x <- raw(1000)
  attr(x, "pad") <- "x"
  # sends the consumer has not drained cannot be reaped: the fourth chunk
  # exhausts 4096 bytes and must fall back to a region create, not an error
  for (i in 1:8) {
    expect_true(rei_send(p[["host"]], x))
  }
  for (i in 1:8) {
    expect_identical(rei_recv(p[["peer"]], 5), x)
  }
})

test_that("a full ring surfaces rei_full and frees on the consumer's drain", {
  p <- channel_pair(capacity = 4L)
  for (i in 1:4) {
    expect_true(rei_send(p[["host"]], i))
  }
  expect_s3_class(rei_send(p[["host"]], 5L), "rei_full")
  # a partial drain publishes nothing (head moves every K, on drain-empty,
  # and before parking — not per message), so the ring still reads full
  expect_identical(rei_recv(p[["peer"]], 5), 1L)
  expect_s3_class(rei_send(p[["host"]], 5L), "rei_full")
  # draining to empty publishes; the producer's reap then frees all slots
  for (i in 2:4) {
    expect_identical(rei_recv(p[["peer"]], 5), i)
  }
  expect_true(rei_send(p[["host"]], 5L))
})

test_that("a send is published the moment it returns", {
  p <- channel_pair()
  rei_send(p[["host"]], "published")
  # a zero-timeout poll sees it: no flush step exists between send and recv
  expect_identical(rei_recv(p[["peer"]], 0), "published")
})

test_that("batch verbs amortize the call boundary", {
  p <- channel_pair(capacity = 64L)
  expect_identical(rei_send_batch(p[["host"]], as.list(1:50)), 50L)
  got <- rei_recv_batch(p[["peer"]], n = 64L, timeout = 5)
  expect_identical(got, as.list(1:50))
  # a full ring stops the batch at the count accepted
  expect_identical(rei_send_batch(p[["host"]], as.list(1:100)), 64L)
  expect_length(rei_recv_batch(p[["peer"]], n = 32L, timeout = 5), 32L)
  expect_length(rei_recv_batch(p[["peer"]], n = 200L, timeout = 5), 32L)
  # sentinel discipline matches recv
  expect_s3_class(
    rei_recv_batch(p[["peer"]], n = 200L, timeout = 0),
    "rei_timeout"
  )
  expect_error(rei_recv_batch(p[["peer"]], n = 0L, timeout = 0), "at least 1")
  expect_error(rei_send_batch(p[["host"]], "not a list"), "expected a list")
})

test_that("keepers pin sent payloads across the sender's GC", {
  p <- channel_pair(arena_size = 0)
  big <- runif(100000)
  csum <- sum(big)
  rei_send(p[["host"]], big) # SHM_RAW: region + keeper
  rm(big)
  gc() # keeper is the only reference
  y <- rei_recv(p[["peer"]], 5)
  expect_identical(sum(y), csum)
})

test_that("the same pinned payload crosses twice, collected one at a time", {
  p <- channel_pair()
  e <- new.env()
  e$x <- 42L
  rei_send(p[["host"]], e) # serialize fallback: one pin per send
  rei_send(p[["host"]], e)
  gc()
  expect_identical(rei_recv(p[["peer"]], 5)$x, 42L)
  gc()
  expect_identical(rei_recv(p[["peer"]], 5)$x, 42L)
  channel_end(p)
})

test_that("pinned payloads round-trip under gctorture", {
  p <- channel_pair(arena_size = 0)
  e <- new.env()
  e$x <- "pin me"
  x <- runif(100000)
  # torture the verbs only: per-allocation GC makes expectation machinery
  # (and any large-heap run) cost minutes, and hours under valgrind
  on.exit(gctorture(FALSE), add = TRUE)
  gctorture(TRUE)
  rei_send(p[["host"]], e)
  got <- rei_recv(p[["peer"]], 5)
  rei_send(p[["host"]], x)
  y <- rei_recv(p[["peer"]], 5)
  rei_send(p[["peer"]], y) # a received view re-sends as REF, pinning the view
  back <- rei_recv(p[["host"]], 5)
  gctorture(FALSE)
  expect_identical(got$x, "pin me")
  expect_identical(as.numeric(back), x)
  channel_end(p)
})

test_that("channel teardown releases outstanding pins to GC", {
  p <- channel_pair()
  flag <- new.env()
  flag$n <- 0L
  for (i in seq_len(3)) {
    e <- new.env()
    reg.finalizer(e, function(x) flag$n <- flag$n + 1L, onexit = TRUE)
    rei_send(p[["host"]], e)
  }
  rm(e)
  channel_end(p) # the destroy drops the unreceived sends' pins
  expect_identical(
    wait_until({
      gc()
      flag$n == 3L
    }),
    TRUE
  )
})

test_that("pinned payloads cycle intact across many rounds", {
  p <- channel_pair()
  for (i in seq_len(200)) {
    e <- new.env()
    e$i <- i
    rei_send(p[["host"]], e)
    expect_identical(rei_recv(p[["peer"]], 5)$i, i)
  }
  channel_end(p)
})

test_that("a consumer-done pin releases the staged object to GC", {
  p <- channel_pair()
  flag <- new.env()
  flag$done <- FALSE
  e <- new.env()
  reg.finalizer(e, function(x) flag$done <- TRUE, onexit = TRUE)
  rei_send(p[["host"]], e)
  invisible(rei_recv(p[["peer"]], 5)) # the received copy carries no finalizer
  rm(e)
  # the release fires only at the producer's next verb (the per-verb reap),
  # so poll nudge + gc(): a drop that never fires fails on timeout
  expect_identical(
    wait_until({
      rei_send(p[["host"]], NULL)
      gc()
      flag$done
    }),
    TRUE
  )
  channel_end(p)
})

test_that("pure-spin mode moves messages without parking", {
  p <- channel_pair(spin = TRUE)
  expect_true(.Call(rei:::rei_channel_stat, p[["host"]])[["spin"]])
  rei_send(p[["host"]], 42L)
  expect_identical(rei_recv(p[["peer"]], 5), 42L)
  expect_s3_class(rei_recv(p[["peer"]], 0.1), "rei_timeout")
})

test_that("mori-shared payloads ride the hooks and map zero-copy", {
  skip_if_not_installed("mori")
  p <- channel_pair()
  x <- mori::share(runif(1000))
  rei_send(p[["host"]], x)
  y <- rei_recv(p[["peer"]], 5)
  expect_true(mori::is_shared(y))
  expect_identical(mori::shared_name(y), mori::shared_name(x))
  expect_identical(as.numeric(y), as.numeric(x))

  # embedded inside a larger payload, and sub-object references too
  l <- mori::share(list(a = runif(100), b = 1:10))
  rei_send(p[["host"]], list(wrapped = l[["a"]], tag = "x"))
  z <- rei_recv(p[["peer"]], 5)
  expect_true(mori::is_shared(z[["wrapped"]]))
  expect_identical(as.numeric(z[["wrapped"]]), as.numeric(l[["a"]]))
})
