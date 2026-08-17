# The compact codec (src/codec.c): the self-describing stream framing ahead of
# R_Serialize on the INLINE / ARENA / SHM_RAW tiers. Round-trips are exact for
# the supported subset; anything else declines (NULL from the write surface)
# and falls back to R_Serialize at the staging call sites.

codec_rt <- function(x) {
  b <- .Call(sora:::sora_codec_write_call, x)
  if (is.null(b)) {
    return(b)
  }
  .Call(sora:::sora_codec_read_call, b)
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

test_that("the subset declines cleanly: NULL from the write surface", {
  expect_null(.Call(sora:::sora_codec_write_call, 1:5)) # ALTREP
  expect_null(.Call(sora:::sora_codec_write_call, function(x) x)) # closure
  expect_null(.Call(sora:::sora_codec_write_call, globalenv())) # environment
  expect_null(.Call(sora:::sora_codec_write_call, base::sum)) # builtin
  # a data.frame's row.names are an ALTREP compact sequence
  expect_null(.Call(sora:::sora_codec_write_call, data.frame(x = 1:3)))
  # an attributed pairlist node (a sourced `{` block's srcref)
  expect_null(.Call(
    sora:::sora_codec_write_call,
    quote({
      1
    })
  ))
})

test_that("the reader rejects malformed streams", {
  expect_error(.Call(sora:::sora_codec_read_call, raw(0)), "corrupt")
  expect_error(.Call(sora:::sora_codec_read_call, as.raw(0x42)), "corrupt")
  b <- .Call(sora:::sora_codec_write_call, list(a = 1L, b = "x"))
  expect_error(
    .Call(sora:::sora_codec_read_call, b[seq_len(length(b) - 1)]),
    "corrupt"
  )
  expect_error(.Call(sora:::sora_codec_read_call, c(b, raw(1))), "corrupt")
  bad <- b
  bad[2] <- as.raw(0x7f)
  expect_error(.Call(sora:::sora_codec_read_call, bad), "corrupt")
})
