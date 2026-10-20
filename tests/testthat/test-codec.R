# The compact codec (src/codec.c): the self-describing stream framing ahead of
# R_Serialize on the INLINE / ARENA / SHM_RAW tiers. Round-trips are exact for
# the supported subset; anything else declines (NULL from the write surface)
# and falls back to R_Serialize at the staging call sites.

codec_rt <- function(x) {
  b <- .Call(mizu:::mizu_codec_write_call, x)
  if (is.null(b)) {
    return(b)
  }
  .Call(mizu:::mizu_codec_read_call, b)
}

test_that("atomic vectors round-trip byte-exactly", {
  expect_identical(codec_rt(1L), 1L)
  expect_identical(codec_rt(seq_len(1000) + 0), seq_len(1000) + 0)
  expect_identical(
    codec_rt(c(1.5, NA, NaN, Inf, -Inf)),
    c(1.5, NA, NaN, Inf, -Inf)
  )
  expect_identical(codec_rt(c(TRUE, FALSE, NA)), c(TRUE, FALSE, NA))
  expect_identical(codec_rt(complex(3, 1, 2)), complex(3, 1, 2))
  expect_identical(codec_rt(raw(0)), raw(0))
  expect_identical(codec_rt(integer(0)), integer(0))
  x <- runif(1e5)
  expect_identical(codec_rt(x), x)
})

test_that("strings round-trip with encodings and NA", {
  expect_identical(codec_rt("hello"), "hello")
  expect_identical(
    codec_rt(c("a", NA_character_, "b")),
    c("a", NA_character_, "b")
  )
  expect_identical(codec_rt(enc2utf8("héllo")), enc2utf8("héllo"))
  expect_identical(codec_rt(character(0)), character(0))
  expect_identical(
    codec_rt(structure("x", class = "bytes")),
    structure("x", class = "bytes")
  )
})

test_that("lists, calls, and pairlists round-trip", {
  expect_identical(codec_rt(list()), list())
  expect_identical(codec_rt(list(1, "a", NULL)), list(1, "a", NULL))
  expect_identical(codec_rt(list(a = 1L, b = "x")), list(a = 1L, b = "x"))
  expect_identical(codec_rt(quote(f(x))), quote(f(x)))
  expect_identical(
    codec_rt(quote(f(x, a = 1:3 + 0L))),
    quote(f(x, a = 1:3 + 0L))
  )
  # a sourced file's `{` block carries srcref attributes (an attributed
  # pairlist node — the fallback below); as.call builds one without
  blk <- as.call(list(quote(`{`), quote(a <- 1), quote(a + 2)))
  expect_identical(codec_rt(blk), blk)
  expect_identical(codec_rt(pairlist(a = 1, 2)), pairlist(a = 1, 2))
  expect_identical(codec_rt(quote(f(, 1))), quote(f(, 1)))
  expect_identical(codec_rt(quote(f)), quote(f))
  expect_identical(codec_rt(expression(1 + 1, f(x))), expression(1 + 1, f(x)))
  expect_identical(codec_rt(as.list(seq_len(100))), as.list(seq_len(100)))
})

test_that("attributes round-trip: names, dim, class, object bit", {
  expect_identical(codec_rt(c(a = 1L, b = 2L)), c(a = 1L, b = 2L))
  expect_identical(
    codec_rt(matrix(1:6, 2, dimnames = list(c("r1", "r2"), NULL))),
    matrix(1:6, 2, dimnames = list(c("r1", "r2"), NULL))
  )
  expect_identical(codec_rt(factor(c("a", "b", "a"))), factor(c("a", "b", "a")))
  expect_identical(
    codec_rt(structure(1L, class = "foo")),
    structure(1L, class = "foo")
  )
  # a class attribute that is not the last pair: the deferred class value
  # must stay anchored while later attributes allocate (it once dropped
  # off the protect stack and a collection reused its node)
  x <- structure(1:3, class = "foo", bar = "baz")
  expect_identical(names(attributes(x)), c("class", "bar"))
  for (i in 1:20) {
    expect_identical(codec_rt(x), x)
  }
})

test_that("S4 objects round-trip with the bit, slots, and class intact", {
  # a slots-only object: the data-less S4SXP form
  methods::setClass(
    "reiSlots",
    representation(x = "numeric", id = "character")
  )
  on.exit(methods::removeClass("reiSlots"), add = TRUE)
  a <- methods::new("reiSlots", x = c(1.5, 2.5), id = "a")
  expect_identical(codec_rt(a), a)
  expect_identical(codec_rt(list(a, 1L)), list(a, 1L))
  # a non-slot attribute rides along
  b <- a
  attr(b, "note") <- "n"
  expect_identical(codec_rt(b), b)
  # a data-part class: the S4 flag on an atomic vector node
  methods::setClass(
    "reiVec",
    contains = "numeric",
    representation(tag = "character")
  )
  on.exit(methods::removeClass("reiVec"), add = TRUE)
  v <- methods::new("reiVec", c(1, 2), tag = "t")
  expect_identical(codec_rt(v), v)
  # a matrix data part: the S4 flag alongside a dim attribute
  methods::setClass("reiMat", contains = "matrix")
  on.exit(methods::removeClass("reiMat"), add = TRUE)
  m <- methods::new("reiMat", matrix(c(1, 2, 3, 4), 2))
  expect_identical(codec_rt(m), m)
  # a function data part: the S4 flag on a closure node
  methods::setClass("reiFun", contains = "function")
  on.exit(methods::removeClass("reiFun"), add = TRUE)
  f <- methods::new("reiFun", eval(quote(function(x) x + 1), globalenv()))
  expect_identical(codec_rt(f), f)
})

test_that("primitives as values round-trip by name", {
  expect_identical(codec_rt(base::sum), base::sum)
  expect_identical(codec_rt(`+`), `+`)
  expect_identical(codec_rt(base::`if`), base::`if`)
  expect_identical(codec_rt(.Primitive), .Primitive)
  expect_identical(
    codec_rt(list(f = base::sum, x = 1L)),
    list(f = base::sum, x = 1L)
  )
})

test_that("closures round-trip with by-reference environments", {
  # eval in globalenv(): the test file's own environment is a local one
  f <- eval(quote(function(x, y = 1, ...) x + y), globalenv())
  expect_identical(codec_rt(f), f)
  expect_identical(codec_rt(list(f = f, x = 1L)), list(f = f, x = 1L))
  g <- structure(eval(quote(function(x) f(x)), globalenv()), note = "n")
  expect_identical(codec_rt(g), g)
  b <- eval(quote(function(x) x), globalenv())
  environment(b) <- baseenv()
  expect_identical(codec_rt(b), b)
  ns <- eval(quote(function(x) x), globalenv())
  environment(ns) <- asNamespace("stats")
  expect_identical(codec_rt(ns), ns)
  e <- eval(quote(function(x) x), globalenv())
  environment(e) <- emptyenv()
  expect_identical(codec_rt(e), e)
})

test_that("keep.source closures cross with srcrefs dropped", {
  # the test harness parses with keep.source: the closure and its {
  # body carry srcrefs; under plain parsing the strip is a no-op
  f <- eval(
    quote(function(x) {
      x + 1
    }),
    globalenv()
  )
  rt <- codec_rt(f)
  expect_null(attr(rt, "srcref"))
  expect_null(attr(body(rt), "srcref"))
  expect_identical(rt, f)
  # a nested function literal (the parser's fourth-element srcref)
  n <- eval(
    quote(function(x) {
      g <- function(y) y + 1
      g(x)
    }),
    globalenv()
  )
  expect_identical(codec_rt(n), n)
})

test_that("keep.source language trees cross with srcrefs dropped", {
  # quote({ ... }) under the harness's keep.source parsing carries a
  # srcref on the block node; under plain parsing the strip is a no-op
  blk <- quote({
    a <- 1
    a + 2
  })
  rt <- codec_rt(blk)
  expect_null(attr(rt, "srcref"))
  expect_identical(rt, blk)
  # a nested block strips on CAR recursion, the outer call rides along
  expect_identical(
    codec_rt(quote(f({
      x
    }))),
    quote(f({
      x
    }))
  )
})

test_that("a data.frame's automatic row.names write as the compact form", {
  df <- data.frame(x = c(1L, 2L, 3L), y = c("a", "b", "c"))
  expect_identical(codec_rt(df), df)
  big <- data.frame(x = seq_len(1e5) + 0)
  expect_identical(codec_rt(big), big)
  # non-automatic row.names keep the plain write
  chr <- data.frame(x = c(1L, 2L, 3L))
  row.names(chr) <- c("a", "b", "c")
  expect_identical(codec_rt(chr), chr)
  off <- data.frame(x = c(1L, 2L, 3L))
  row.names(off) <- c(1L, 2L, 4L)
  expect_identical(codec_rt(off), off)
})

test_that("the subset declines cleanly: NULL from the write surface", {
  expect_null(.Call(mizu:::mizu_codec_write_call, 1:5)) # ALTREP
  expect_null(.Call(mizu:::mizu_codec_write_call, globalenv())) # environment
  # a closure over a local environment (the function-factory case)
  loc <- local({
    y <- 1
    eval(quote(function(x) x + y))
  })
  expect_null(.Call(mizu:::mizu_codec_write_call, loc))
  # a byte-compiled closure body
  expect_null(.Call(
    mizu:::mizu_codec_write_call,
    compiler::cmpfun(eval(quote(function(x) x + 1)))
  ))
  # an attributed language node in the body (a non-srcref attribute
  # survives the strip, and attributed pairlist nodes decline)
  src <- eval(quote(function(x) f(x)), globalenv())
  body(src) <- structure(body(src), note = 1)
  expect_null(.Call(mizu:::mizu_codec_write_call, src))
  # a language node with a non-srcref attribute still declines
  expect_null(.Call(
    mizu:::mizu_codec_write_call,
    structure(quote(f(x)), note = 1)
  ))
  # an S4 object with an out-of-subset slot declines with it
  methods::setClass("reiEnv", representation(e = "environment"))
  on.exit(methods::removeClass("reiEnv"), add = TRUE)
  expect_null(.Call(
    mizu:::mizu_codec_write_call,
    methods::new("reiEnv", e = new.env())
  ))
  # an S4 object with an ALTREP slot
  methods::setClass("reiAlt", representation(x = "integer"))
  on.exit(methods::removeClass("reiAlt"), add = TRUE)
  expect_null(.Call(
    mizu:::mizu_codec_write_call,
    methods::new("reiAlt", x = 1:3)
  ))
})

test_that("a CDR cycle declines the codec instead of hanging", {
  # the seam builds the cycle in C — no R-level spell exists
  expect_identical(.Call(mizu:::mizu_codec_cycle_call), 0L)
})

test_that("the reader rejects malformed streams", {
  expect_error(.Call(mizu:::mizu_codec_read_call, raw(0)), "corrupt")
  expect_error(.Call(mizu:::mizu_codec_read_call, as.raw(0x42)), "corrupt")
  b <- .Call(mizu:::mizu_codec_write_call, list(a = 1L, b = "x"))
  expect_error(
    .Call(mizu:::mizu_codec_read_call, b[seq_len(length(b) - 1)]),
    "corrupt"
  )
  expect_error(.Call(mizu:::mizu_codec_read_call, c(b, raw(1))), "corrupt")
  bad <- b
  bad[2] <- as.raw(0x7f)
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # a corrupt closure environment kind byte
  cf <- .Call(
    mizu:::mizu_codec_write_call,
    eval(quote(function(x) x), globalenv())
  )
  bad <- cf
  bad[3] <- as.raw(0xff)
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # the S4 flag on a symbol, a type that never carries it
  bad <- .Call(mizu:::mizu_codec_write_call, quote(x))
  bad[2] <- as.raw(0x41)
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # the primitive flag on a non-symbol
  bad <- .Call(mizu:::mizu_codec_write_call, 1L)
  bad[2] <- as.raw(0x83)
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # a primitive name that resolves to no primitive
  bad <- .Call(mizu:::mizu_codec_write_call, quote(x))
  bad[2] <- as.raw(0x81)
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
})

test_that("the reader validates stream bounds and shapes", {
  u32 <- function(x) writeBin(as.integer(x), raw(4L), endian = "little")
  u64 <- function(x) {
    stopifnot(x >= 0 && x < 2^53)
    lo <- x %% 2^32
    hi <- (x - lo) / 2^32
    if (lo >= 2^31) {
      lo <- lo - 2^32 # writeBin takes the signed interpretation
    }
    c(
      writeBin(as.integer(lo), raw(4L), endian = "little"),
      writeBin(as.integer(hi), raw(4L), endian = "little")
    )
  }
  magic <- as.raw(0x52)
  # a string element's length past INT32_MAX
  bad <- c(magic, as.raw(0x07), u64(1), u32(0), u32(4))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # an attribute count past the stream's remaining bytes
  bad <- c(magic, as.raw(0x13), u64(1), u32(0x7fffffff))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # an NA attribute name
  bad <- c(magic, as.raw(0x13), u64(1), u32(1), u32(-1))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # an atomic vector's length past the stream's remaining bytes
  bad <- c(magic, as.raw(0x04), u64(2^40))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # a string vector's length past the stream's remaining bytes
  bad <- c(magic, as.raw(0x07), u64(2^40))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # a list's length past the stream's remaining bytes
  bad <- c(magic, as.raw(0x08), u64(2^40))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # a primitive name resolving to a non-primitive base object
  bad <- c(magic, as.raw(0x81), u32(8), u32(0), charToRaw(".Machine"))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # a namespace environment reference with an NA name
  bad <- c(magic, as.raw(0x0e), as.raw(0x03), u32(-1))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # a frame leaf with a zero-length region name
  expect_error(
    .Call(mizu:::mizu_codec_read_call, c(magic, as.raw(0x28), as.raw(0))),
    "corrupt"
  )
  # a frame leaf whose name overruns the stream
  bad <- c(magic, as.raw(0x28), as.raw(10), as.raw(1:3))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # the depth cap
  deep <- c(magic, rep(c(as.raw(0x08), u64(1)), 501), as.raw(0))
  expect_error(.Call(mizu:::mizu_codec_read_call, deep), "corrupt")
})

test_that("row.names that are not the 1:n sequence decline the codec", {
  df <- data.frame(x = c(1L, 2L, 3L))
  row.names(df) <- 2:4
  expect_null(.Call(mizu:::mizu_codec_write_call, df))
  # through a channel the same frame falls back to R_Serialize intact
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p$host, df)
  expect_identical(mizu_recv(p$peer, 5), df)
})

test_that("the reader rejects flag bits a type never carries", {
  # SC_HASTAG on an atomic vector node
  bad <- .Call(mizu:::mizu_codec_write_call, 1.5)
  bad[2] <- as.raw(0x24)
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # SC_HASS4 on NIL, a type outside the S4 whitelist
  bad <- .Call(mizu:::mizu_codec_write_call, NULL)
  bad[2] <- as.raw(0x40)
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
  # SC_HASATTR on a symbol, a type that never carries attributes
  bad <- .Call(mizu:::mizu_codec_write_call, quote(x))
  bad[2] <- as.raw(0x11)
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
})

test_that("the reader rejects an NA attribute name", {
  bad <- .Call(mizu:::mizu_codec_write_call, structure(1, foo = 2))
  at <- grepRaw("foo", bad, fixed = TRUE)
  expect_true(at > 0)
  # the name's u32 length precedes its cetype word: 0xFFFFFFFF marks NA
  bad[(at - 8L):(at - 5L)] <- as.raw(rep(0xff, 4L))
  expect_error(.Call(mizu:::mizu_codec_read_call, bad), "corrupt")
})

test_that("a data frame's expanded 1:n row names write compactly", {
  df <- data.frame(x = c(1, 2, 3))
  expect_identical(codec_rt(df), df)
  big <- data.frame(x = runif(9000)) # past the 8192-element region chunk
  expect_identical(codec_rt(big), big)
})

test_that("the reader rejects flag combinations the writer never produces", {
  w <- function(x) .Call(mizu:::mizu_codec_write_call, x)
  r <- function(b) .Call(mizu:::mizu_codec_read_call, b)

  b <- w(1L) # SC_HASTAG is only written on chain nodes
  b[2L] <- b[2L] | as.raw(0x20)
  expect_error(r(b), "corrupt payload stream")

  b <- w(NULL) # SC_HASS4 is never written on NIL
  b[2L] <- b[2L] | as.raw(0x40)
  expect_error(r(b), "corrupt payload stream")

  b <- w(1L) # SC_PRIM is only written on symbols
  b[2L] <- b[2L] | as.raw(0x80)
  expect_error(r(b), "corrupt payload stream")

  b <- w(NULL) # attributes are never written on NIL
  b[2L] <- b[2L] | as.raw(0x10)
  expect_error(r(b), "corrupt payload stream")

  b <- w(quote(zzz_not_a_base_binding)) # a primitive that is not
  b[2L] <- b[2L] | as.raw(0x80)
  expect_error(r(b), "corrupt payload stream")

  b <- w(quote(ls)) # a base binding that is not a primitive
  b[2L] <- b[2L] | as.raw(0x80)
  expect_error(r(b), "corrupt payload stream")

  b <- w(function(x) x) # an unknown environment kind byte
  b[3L] <- as.raw(99L)
  expect_error(r(b), "corrupt payload stream")
})

test_that("a namespace closure whose name is unreadable rejects", {
  f <- eval(quote(function(x) x), asNamespace("utils"))
  b <- .Call(mizu:::mizu_codec_write_call, f)
  expect_false(is.null(b))
  b[4L:7L] <- as.raw(255L) # the namespace name's length field
  expect_error(.Call(mizu:::mizu_codec_read_call, b), "corrupt payload stream")
})

test_that("a frame leaf resolving to a non-list view rejects", {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], runif(100000))
  v <- mizu_recv(p[["peer"]], 5)
  aname <- .Call(mizu:::mizu_zc_view_name, v)
  # a frame leaf (SC_FRAME tag, u8 name length, the name) naming the
  # vector's region: its MIZH view is not the list a frame resolves to
  bad <- c(
    as.raw(0x52),
    as.raw(0x28),
    as.raw(nchar(aname, "bytes")),
    charToRaw(aname)
  )
  expect_error(
    .Call(mizu:::mizu_codec_read_call, bad),
    "corrupt payload stream"
  )
  channel_end(p)
})

test_that("a call with a nested call tail round-trips", {
  x <- quote(f(g(x), h(1)))
  expect_identical(
    .Call(mizu:::mizu_codec_read_call, .Call(mizu:::mizu_codec_write_call, x)),
    x
  )
})

test_that("a mixed LANGSXP/LISTSXP chain round-trips its cell types", {
  # R builds no mixed chains at the language level: the first argument
  # cell's type word is poked LISTSXP -> LANGSXP in the serialized form
  x <- quote(f(a, b))
  buf <- serialize(x, NULL)
  h <- grepRaw(
    writeBin(2L, raw(), size = 4L, endian = "big"),
    buf,
    fixed = TRUE
  )
  buf[h + 3L] <- as.raw(6L)
  x <- unserialize(buf)
  expect_identical(
    .Call(mizu:::mizu_codec_read_call, .Call(mizu:::mizu_codec_write_call, x)),
    x
  )
})

test_that("a primitive resolving to a non-base name is rejected", {
  b <- .Call(mizu:::mizu_codec_write_call, .Primitive("+"))
  b[length(b)] <- charToRaw("z")
  expect_error(
    .Call(mizu:::mizu_codec_read_call, b),
    "corrupt payload stream"
  )
})

test_that("a data.frame's ALTREP row.names write the compact form", {
  skip_if(getRversion() < "4.6.0")
  x <- data.frame(a = c(2L, 1L, 5L, 3L, 4L), b = letters[1:5])
  attr(x, "row.names") <- 1:5
  b <- .Call(mizu:::mizu_codec_write_call, x)
  expect_false(is.null(b))
  expect_identical(.Call(mizu:::mizu_codec_read_call, b), x)
})

test_that("an unbound-value node reads back", {
  # the single element's tag byte re-pointed at SC_UNBOUND (13)
  b <- c(.Call(mizu:::mizu_codec_write_call, list(1L))[1:10], as.raw(13L))
  x <- .Call(mizu:::mizu_codec_read_call, b)
  expect_identical(length(x), 1L)
  expect_error(v <- x[[1L]], "Unbound")
})
