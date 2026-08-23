# The compact codec (src/codec.c): the self-describing stream framing ahead of
# R_Serialize on the INLINE / ARENA / SHM_RAW tiers. Round-trips are exact for
# the supported subset; anything else declines (NULL from the write surface)
# and falls back to R_Serialize at the staging call sites.

codec_rt <- function(x) {
  b <- .Call(rei:::rei_codec_write_call, x)
  if (is.null(b)) {
    return(b)
  }
  .Call(rei:::rei_codec_read_call, b)
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

test_that("the subset declines cleanly: NULL from the write surface", {
  expect_null(.Call(rei:::rei_codec_write_call, 1:5)) # ALTREP
  expect_null(.Call(rei:::rei_codec_write_call, globalenv())) # environment
  # a closure over a local environment (the function-factory case)
  loc <- local({
    y <- 1
    eval(quote(function(x) x + y))
  })
  expect_null(.Call(rei:::rei_codec_write_call, loc))
  # a byte-compiled closure body
  expect_null(.Call(
    rei:::rei_codec_write_call,
    compiler::cmpfun(eval(quote(function(x) x + 1)))
  ))
  # an attributed language node in the body (a non-srcref attribute
  # survives the strip, and attributed pairlist nodes decline)
  src <- eval(quote(function(x) f(x)), globalenv())
  body(src) <- structure(body(src), note = 1)
  expect_null(.Call(rei:::rei_codec_write_call, src))
  # a data.frame's row.names are an ALTREP compact sequence
  expect_null(.Call(rei:::rei_codec_write_call, data.frame(x = 1:3)))
  # a language node with a non-srcref attribute still declines
  expect_null(.Call(
    rei:::rei_codec_write_call,
    structure(quote(f(x)), note = 1)
  ))
  # an S4 object with an out-of-subset slot declines with it
  methods::setClass("reiEnv", representation(e = "environment"))
  on.exit(methods::removeClass("reiEnv"), add = TRUE)
  expect_null(.Call(
    rei:::rei_codec_write_call,
    methods::new("reiEnv", e = new.env())
  ))
  # an S4 object with an ALTREP slot
  methods::setClass("reiAlt", representation(x = "integer"))
  on.exit(methods::removeClass("reiAlt"), add = TRUE)
  expect_null(.Call(
    rei:::rei_codec_write_call,
    methods::new("reiAlt", x = 1:3)
  ))
})

test_that("the reader rejects malformed streams", {
  expect_error(.Call(rei:::rei_codec_read_call, raw(0)), "corrupt")
  expect_error(.Call(rei:::rei_codec_read_call, as.raw(0x42)), "corrupt")
  b <- .Call(rei:::rei_codec_write_call, list(a = 1L, b = "x"))
  expect_error(
    .Call(rei:::rei_codec_read_call, b[seq_len(length(b) - 1)]),
    "corrupt"
  )
  expect_error(.Call(rei:::rei_codec_read_call, c(b, raw(1))), "corrupt")
  bad <- b
  bad[2] <- as.raw(0x7f)
  expect_error(.Call(rei:::rei_codec_read_call, bad), "corrupt")
  # a corrupt closure environment kind byte
  cf <- .Call(
    rei:::rei_codec_write_call,
    eval(quote(function(x) x), globalenv())
  )
  bad <- cf
  bad[3] <- as.raw(0xff)
  expect_error(.Call(rei:::rei_codec_read_call, bad), "corrupt")
  # the S4 flag on a symbol, a type that never carries it
  bad <- .Call(rei:::rei_codec_write_call, quote(x))
  bad[2] <- as.raw(0x41)
  expect_error(.Call(rei:::rei_codec_read_call, bad), "corrupt")
  # the primitive flag on a non-symbol
  bad <- .Call(rei:::rei_codec_write_call, 1L)
  bad[2] <- as.raw(0x83)
  expect_error(.Call(rei:::rei_codec_read_call, bad), "corrupt")
  # a primitive name that resolves to no primitive
  bad <- .Call(rei:::rei_codec_write_call, quote(x))
  bad[2] <- as.raw(0x81)
  expect_error(.Call(rei:::rei_codec_read_call, bad), "corrupt")
})
