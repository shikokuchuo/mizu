# Streaming maps: .stream = TRUE feeds fixed x-slices through ordinary
# chunk tasks under a sliding submit/collect window, so shared-memory
# residency is bounded by window x slice instead of sizeof(x). The
# in-process tests drive the composable internals (stage / prime / step /
# turn) against pool_pair for deterministic single-stepping — the fused
# run would block in collect with nobody to step the worker; the blocking
# mizu_map() surface, out-of-order completion, and worker death go over
# spawned workers.

# in-process streaming driver: one step-drain per turn keeps a published
# result ahead of every collect, so turns never park
run_stream <- function(p, st, collect = "copy") {
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  while (length(st[["oi"]]) && !st[["timed_out"]]) {
    while (pool_step(p) == 1L) {
      NULL
    }
    mizu:::map_stream_turn(p[["ctrl"]], st, Inf)
  }
  mizu:::map_assemble(st, st[["out"]], collect)
}

test_that("a streaming map slices an integer64 view classed (R-06)", {
  skip_if_not_installed("bit64")
  p <- pool_pair()
  t <- mizu_submit(p$ctrl, bit64::as.integer64(seq_len(2e4)))
  pool_step(p)
  v <- mizu_collect(t, 5)
  expect_true(.Call(mizu:::mizu_zc_view_check, v))
  st <- mizu:::map_stage_stream(p$ctrl, v, function(i) format(i), list())
  r <- run_stream(p, st)
  # the strict int64 gate would drop the class off the ALTREP slice and
  # hand f bare doubles holding int64 bit patterns
  expect_identical(r[[2L]], "2")
  pool_end(p)
})

test_that("streaming stages a descriptor-only region and balanced chunks", {
  p <- pool_pair(workers = 2L)
  st <- mizu:::map_stage_stream(p[["ctrl"]], 1:100, identity, list())
  # C = min(n, 32 x live workers); W = min(C, 2 x live, free slots)
  expect_identical(st[["C"]], 64L)
  expect_identical(st[["W"]], 4L)
  # 100 over 64 chunks: 36 of size 2, then 28 of size 1
  expect_identical(st[["lo"]][1:3], c(1, 3, 5))
  expect_identical(st[["hi"]][36], 72)
  expect_identical(st[["lo"]][37], 73)
  expect_identical(st[["hi"]][64], 100)
  expect_identical(sum(st[["hi"]] - st[["lo"]] + 1), 100)
  expect_identical(.Call(mizu:::mizu_map_info, st[["wrap"]])[["n"]], 100)
  # no x section: slices come from the submitter, not the region
  expect_error(
    .Call(mizu:::mizu_map_slice, st[["wrap"]], 1, 2),
    "no x section"
  )
  # .n_chunks overrides outright, bounded only by n
  st2 <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:100,
    identity,
    list(),
    chunks = 3
  )
  expect_identical(st2[["C"]], 3L)
  expect_identical(st2[["W"]], 3L)
  st3 <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:5,
    identity,
    list(),
    chunks = 10
  )
  expect_identical(st3[["C"]], 5L)
  # n = 1 floors the geometry at a single one-element chunk
  st4 <- mizu:::map_stage_stream(p[["ctrl"]], 1, identity, list())
  expect_identical(c(st4[["C"]], st4[["W"]]), c(1L, 1L))
  pool_end(p)
})

test_that("a spec .f streams over R workers (the same-language shape)", {
  p <- pool_pair(workers = 2L)
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:20,
    mizu_call("base::abs"),
    list()
  )
  expect_identical(run_stream(p, st), as.list(abs(1:20)))
  # a bare atomic x slices through the per-chunk raw x sections
  st2 <- mizu:::map_stage_stream(
    p[["ctrl"]],
    as.double(1:20),
    mizu_call("base::abs"),
    list()
  )
  expect_identical(run_stream(p, st2), as.list(abs(as.double(1:20))))
  # the template path accumulates each chunk's gather
  st3 <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:20,
    mizu_call("base::abs"),
    list(),
    template = numeric(1)
  )
  expect_identical(run_stream(p, st3), as.double(abs(1:20)))
  pool_end(p)
})

test_that("a streaming spec map keeps seed invariance over R workers", {
  p <- pool_pair(workers = 2L)
  f <- mizu_call(.source = "runif(1)")
  st <- mizu:::map_stage_stream(p[["ctrl"]], 1:30, f, list(), seed = 42)
  want <- run_stream(p, st)
  for (chunks in list(1, 7, 30)) {
    st2 <- mizu:::map_stage_stream(
      p[["ctrl"]],
      1:30,
      f,
      list(),
      chunks = chunks,
      seed = 42
    )
    expect_identical(run_stream(p, st2), want)
  }
  pool_end(p)
})

test_that("a spec stream declines a seed offset escaping exact arithmetic (L33)", {
  p <- pool_pair(workers = 2L)
  f <- mizu_call(.source = "runif(1)")
  # the refill composes each chunk's offset in doubles — exact only to
  # 2^53, where the native path's jump composition never rounds
  expect_snapshot(
    error = TRUE,
    mizu:::map_stage_stream(
      p[["ctrl"]],
      1:30,
      f,
      list(),
      seed = c(42, 2^53 - 14)
    )
  )
  # the boundary stages and stays chunking-invariant:
  # 2^53 - 29 + (30 - 1) == 2^53 exactly
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:30,
    f,
    list(),
    chunks = 1,
    seed = c(42, 2^53 - 29)
  )
  want <- run_stream(p, st)
  st2 <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:30,
    f,
    list(),
    chunks = 3,
    seed = c(42, 2^53 - 29)
  )
  expect_identical(run_stream(p, st2), want)
  pool_end(p)
})

test_that("view collect errors on a streaming spec map, informatively", {
  p <- pool_pair()
  expect_snapshot(
    mizu_collect(mizu_map(
      p[["ctrl"]],
      1:4,
      mizu_call("base::abs"),
      .template = numeric(1),
      .collect = "view",
      .stream = TRUE
    )),
    error = TRUE
  )
  expect_snapshot(
    mizu_map_prepare(p[["ctrl"]], 1:4, mizu_call("base::abs"), .stream = TRUE),
    error = TRUE
  )
  pool_end(p)
})

test_that("a fully occupied subrange errors before any region exists", {
  p <- pool_pair()
  held <- lapply(1:8, function(i) mizu_submit(p[["ctrl"]], v, v = i))
  expect_error(
    mizu:::map_stage_stream(p[["ctrl"]], 1:10, identity, list()),
    "result slots exhausted"
  )
  while (pool_step(p) == 1L) {
    NULL
  }
  for (i in 1:8) {
    expect_identical(mizu_collect(held[[i]], timeout = 5), i)
  }
  pool_end(p)
})

test_that("mizu_map_xslice slices every supported shape, names dropped", {
  x <- setNames(seq_len(100L) + 0.5, paste0("e", 1:100))
  s <- .Call(mizu:::mizu_map_xslice, x, 3, 7)
  expect_identical(s, (seq_len(100L) + 0.5)[3:7] |> unname())
  expect_null(names(s))
  expect_identical(.Call(mizu:::mizu_map_xslice, 1:10L, 1, 10), 1:10L)
  expect_identical(
    .Call(mizu:::mizu_map_xslice, as.raw(1:10), 2, 4),
    as.raw(2:4)
  )
  expect_identical(
    .Call(mizu:::mizu_map_xslice, c(TRUE, FALSE, TRUE), 2, 3),
    c(FALSE, TRUE)
  )
  expect_identical(
    .Call(mizu:::mizu_map_xslice, c(1 + 2i, 3 - 1i, 0i), 2, 3),
    c(3 - 1i, 0i)
  )
  # STRSXP per element, NAs intact
  cx <- c("a", NA, "c", "d")
  expect_identical(.Call(mizu:::mizu_map_xslice, cx, 2, 4), c(NA, "c", "d"))
  # VECSXP shallow: the slice aliases the same element objects
  lx <- list(a = 1:3, b = "x", c = NULL)
  sl <- .Call(mizu:::mizu_map_xslice, lx, 1, 2)
  expect_identical(sl, unname(lx[1:2]))
  expect_true(identical(sl[[1L]], lx[[1L]]))
  # compact ALTREP sequences slice per element, never materializing
  expect_identical(.Call(mizu:::mizu_map_xslice, 1:10^9, 5, 8), 5:8)
  expect_identical(
    .Call(mizu:::mizu_map_xslice, as.double(1:10^9), 5, 8),
    as.double(5:8)
  )
  expect_identical(
    .Call(mizu:::mizu_map_xslice, seq(1, 10, by = 0.5), 2, 4),
    c(1.5, 2, 2.5)
  )
  # range validation and unsupported types
  expect_error(.Call(mizu:::mizu_map_xslice, 1:10, 0, 2), "slice range")
  expect_error(.Call(mizu:::mizu_map_xslice, 1:10, 2, 11), "slice range")
  expect_error(.Call(mizu:::mizu_map_xslice, 1:10, 5, 4), "slice range")
  expect_error(
    .Call(mizu:::mizu_map_xslice, quote(x), 1, 1),
    "unsupported stream slice type"
  )
})

test_that("mizu_map_vsplice validates its arguments and writes in place", {
  out <- vector("list", 4L)
  expect_error(
    .Call(mizu:::mizu_map_vsplice, out, 1, "x"),
    "invalid map splice arguments"
  )
  expect_error(
    .Call(mizu:::mizu_map_vsplice, "x", 1, list(1)),
    "invalid map splice arguments"
  )
  expect_error(
    .Call(mizu:::mizu_map_vsplice, out, 0, list(1)),
    "invalid map splice range"
  )
  expect_error(
    .Call(mizu:::mizu_map_vsplice, out, 4, list(1, 2)),
    "invalid map splice range"
  )
  .Call(mizu:::mizu_map_vsplice, out, 2, list("a", "b"))
  expect_identical(out, list(NULL, "a", "b", NULL))
})

test_that("mizu_map_xslice re-classes class-only integer64", {
  x <- structure(c(1, -2, 2^53 + 2, 42), class = "integer64")
  s <- .Call(mizu:::mizu_map_xslice, x, 2, 4)
  expect_identical(s, structure(c(-2, 2^53 + 2, 42), class = "integer64"))
})

test_that("a received view slices read-only through DATAPTR_OR_NULL", {
  cp <- channel_pair()
  big <- seq_len(100000) + 0.5
  mizu_send(cp[["host"]], big)
  v <- mizu_recv(cp[["peer"]], 5)
  expect_true(.Call(mizu:::mizu_zc_view_check, v))
  mizu_send(cp[["host"]], "tickle") # reaps the producer's loan
  invisible(mizu_recv(cp[["peer"]], 5))
  # the refcount reads first: a value comparison that materialized the
  # view (a COW through the writable accessors) would release it early
  expect_identical(.Call(mizu:::mizu_zc_refcount, v), c(1L, 0L))
  s <- .Call(mizu:::mizu_map_xslice, v, 101, 200)
  expect_identical(.Call(mizu:::mizu_zc_refcount, v), c(1L, 0L))
  expect_false(.Call(mizu:::mizu_zc_view_check, s)) # an owning copy
  expect_identical(s, big[101:200])
  channel_end(cp)
})

test_that("window refill splices by position with names reapplied", {
  p <- pool_pair()
  x <- setNames(1:40, paste0("e", 1:40))
  st <- mizu:::map_stage_stream(p[["ctrl"]], x, function(i) i * 2L, list())
  expect_identical(run_stream(p, st), as.list(setNames(1:40 * 2L, names(x))))
  # identical to the non-streaming map on the same inputs
  st2 <- mizu:::map_stage(
    p[["ctrl"]],
    x,
    function(i) i * 2L,
    list(),
    chunks = 7
  )
  mizu:::map_submit(p[["ctrl"]], st2)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(
    mizu:::map_collect(st2, mizu:::mono_time() + 30),
    as.list(setNames(1:40 * 2L, names(x)))
  )
  pool_end(p)
})

test_that("template results land in the output area; value and view collect", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:50 + 0.5,
    function(i) i * 2,
    list(),
    template = numeric(1)
  )
  expect_true(st[["direct"]])
  expect_identical(run_stream(p, st), (1:50 + 0.5) * 2)
  # view collect wraps the same output area, COW intact
  st2 <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:50 + 0.5,
    function(i) i * 2,
    list(),
    template = numeric(1)
  )
  res <- run_stream(p, st2, "view")
  expect_true(.Call(mizu:::mizu_zc_view_check, res))
  expect_true(st2[["consumed"]])
  expect_identical(as.numeric(res), (1:50 + 0.5) * 2)
  res[1] <- -1
  expect_identical(res[[1]], -1)
  expect_identical(as.numeric(res)[-1], ((1:50 + 0.5) * 2)[-1])
  pool_end(p)
})

test_that("m > 1 templates gather a matrix and character templates assemble", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:10,
    function(i) c(i, -i),
    list(),
    template = c(a = 0L, b = 0L)
  )
  expect_identical(
    run_stream(p, st),
    matrix(
      c(1:10, -(1:10)),
      nrow = 2L,
      byrow = TRUE,
      dimnames = list(c("a", "b"), NULL)
    )
  )
  st2 <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:5,
    function(i) paste0("v", i),
    list(),
    template = character(1)
  )
  expect_false(st2[["direct"]])
  expect_identical(run_stream(p, st2), paste0("v", 1:5))
  pool_end(p)
})

test_that("seed invariance holds across chunk counts and window sizes", {
  f <- function(i) rnorm(1L)
  p1 <- pool_pair(workers = 1L)
  s1 <- run_stream(
    p1,
    mizu:::map_stage_stream(p1[["ctrl"]], 1:30, f, list(), seed = 42L)
  )
  pool_end(p1)
  p2 <- pool_pair(workers = 2L)
  for (chunks in list(NULL, 3L, 30L)) {
    s <- run_stream(
      p2,
      mizu:::map_stage_stream(
        p2[["ctrl"]],
        1:30,
        f,
        list(),
        chunks = chunks,
        seed = 42L
      )
    )
    expect_identical(s, s1)
  }
  # and identical to the non-streaming map at the same seed
  st <- mizu:::map_stage(p2[["ctrl"]], 1:30, f, list(), seed = 42L)
  mizu:::map_submit(p2[["ctrl"]], st)
  while (pool_step(p2) == 1L) {
    NULL
  }
  expect_identical(mizu:::map_collect(st, mizu:::mono_time() + 30), s1)
  pool_end(p2)
})

test_that("fail-fast reports the failing element and cancels the window", {
  p <- pool_pair()
  # chunks of 2; f errors at element 3 (chunk 2)
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:8,
    function(i) if (i == 3L) stop("boom") else i,
    list(),
    chunks = 4
  )
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  expect_identical(pool_step(p), 1L) # chunk 1
  mizu:::map_stream_turn(p[["ctrl"]], st, Inf) # collects 1, refills 3
  expect_identical(pool_step(p), 1L) # chunk 2 errors
  e <- tryCatch(
    {
      mizu:::map_stream_turn(p[["ctrl"]], st, Inf)
      NULL
    },
    error = function(e) e
  )
  expect_identical(conditionMessage(e), "boom")
  expect_identical(e[["mizu_map_index"]], 3)
  # the window was cancelled: chunk 3 never ran and reads cancel
  d <- mizu_pool_dump(p[["ctrl"]])
  expect_false("pending" %in% d[["tasks"]][["status"]])
  expect_true("cancel" %in% d[["tasks"]][["status"]])
  pool_end(p)
})

test_that("sibling errors drain into the minimum element index selection", {
  p <- pool_pair()
  # chunk 2 errors at 3, chunk 3 at 5: the turn observes chunk 2's error
  # (collect_any scans in submission order), the drain harvests chunk 3's
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:8,
    function(i) if (i %in% c(3L, 5L)) stop(paste0("e", i)) else i,
    list(),
    chunks = 4
  )
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  expect_identical(pool_step(p), 1L) # chunk 1
  mizu:::map_stream_turn(p[["ctrl"]], st, Inf) # collects 1, refills 3
  expect_identical(pool_step(p), 1L) # chunk 2 errors at 3
  expect_identical(pool_step(p), 1L) # chunk 3 errors at 5
  e <- tryCatch(
    {
      mizu:::map_stream_turn(p[["ctrl"]], st, Inf)
      NULL
    },
    error = function(e) e
  )
  expect_identical(e[["mizu_map_index"]], 3)
  expect_identical(conditionMessage(e), "e3")
  pool_end(p)
})

test_that("an infrastructure error on a drained sibling stays fatal", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:4,
    function(i) if (i == 1L) stop("boom") else i,
    list(),
    chunks = 2
  )
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  # a sibling whose chunk names a region that does not resolve: the
  # worker's ctx attach fails outside f — not f's error, not a death, and
  # it must re-raise rather than drain silently
  h <- .Call(
    mizu:::mizu_pool_submit_try,
    p[["ctrl"]],
    list(
      quote(mizu:::map_stream_chunk("mizu-nonexistent", c(1, 2), 1:2)),
      list()
    ),
    Inf,
    0L
  )
  st[["handles"]][[3L]] <- h
  st[["oi"]] <- c(st[["oi"]], 3L)
  expect_identical(pool_step(p), 1L) # chunk 1 errors at element 1
  expect_identical(pool_step(p), 1L) # chunk 2 ok
  expect_identical(pool_step(p), 1L) # the bogus chunk's attach fails
  e <- tryCatch(
    {
      mizu:::map_stream_turn(p[["ctrl"]], st, Inf)
      NULL
    },
    error = function(e) e
  )
  expect_s3_class(e, "mizu_error")
  expect_false(inherits(e, "mizu_error_worker_died"))
  expect_null(e[["mizu_map_index"]])
  pool_end(p)
})

test_that("the drain ignores a sibling whose cancel the worker consumed", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:3 + 0,
    function(i) if (i == 2) stop("bad") else i,
    list()
  )
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  expect_identical(pool_step(p), 1L) # chunk 1
  mizu:::map_stream_turn(p[["ctrl"]], st, Inf) # collects 1, refills 3
  expect_identical(pool_step(p), 1L) # chunk 2 errors at 2
  v <- tryCatch(
    .Call(mizu:::mizu_pool_collect_any, st[["handles"]][st[["oi"]]], 5),
    error = function(e) e
  )
  k <- st[["oi"]][[v[["index"]]]]
  st[["oi"]] <- st[["oi"]][-v[["index"]]]
  st[["handles"]][k] <- list(NULL)
  # a cancelled sibling's slot is freed by the worker's skip, not by a
  # collect — the drain must not read it as already collected
  .Call(mizu:::mizu_pool_cancel, st[["handles"]][[3L]])
  pool_step(p)
  expect_identical(
    .Call(mizu:::mizu_pool_task_state, st[["handles"]][[3L]]),
    "collected"
  )
  expect_error(mizu:::map_stream_fail(st, v, k, FALSE), "bad")
  pool_end(p)
})

test_that("one attach per worker per map; the ctx cache serves chunk 2", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(p[["ctrl"]], 1:10, function(i) i, list())
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  expect_identical(pool_step(p), 1L)
  expect_identical(pool_step(p), 1L) # same worker, second chunk
  expect_identical(
    ls(.Call(mizu:::mizu_pool_map_cache, p[["wk"]])),
    st[["name"]]
  )
  while (length(st[["oi"]])) {
    while (pool_step(p) == 1L) {
      NULL
    }
    mizu:::map_stream_turn(p[["ctrl"]], st, Inf)
  }
  expect_identical(st[["out"]], as.list(1:10))
  pool_end(p)
})

test_that(".n_chunks = n runs one element per task", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:6,
    function(i) i * 3L,
    list(),
    chunks = 6
  )
  expect_identical(st[["C"]], 6L)
  expect_identical(run_stream(p, st), as.list(1:6 * 3L))
  pool_end(p)
})

test_that("a list x streams element-wise", {
  p <- pool_pair()
  x <- list(a = 1:3, b = "x", c = list(2))
  st <- mizu:::map_stage_stream(p[["ctrl"]], x, function(i) length(i), list())
  expect_identical(run_stream(p, st), list(a = 3L, b = 1L, c = 1L))
  # a data.frame coerces to its column list, as lapply
  st2 <- mizu:::map_stage_stream(
    p[["ctrl"]],
    data.frame(a = 1:3, b = 4:6),
    sum,
    list()
  )
  expect_identical(run_stream(p, st2), list(a = 6L, b = 15L))
  pool_end(p)
})

test_that("class-only integer64 streams with classed slices and elements", {
  skip_if_not_installed("bit64")
  p <- pool_pair()
  x <- bit64::as.integer64(seq_len(100L) * 4294967296)
  st <- mizu:::map_stage_stream(p[["ctrl"]], x, identity, list())
  expect_identical(st[["x"]], x) # not list-coerced
  r <- run_stream(p, st)
  expect_identical(r, as.list(x))
  # the slice itself carries the class (the wire-tag discipline)
  s <- .Call(mizu:::mizu_map_xslice, x, 2, 5)
  expect_identical(s, x[2:5])
  pool_end(p)
})

test_that(".n_chunks validates on the streaming path", {
  p <- pool_pair()
  expect_error(
    mizu:::map_stage_stream(p[["ctrl"]], 1:4, identity, list(), chunks = 0),
    "positive number"
  )
  expect_error(
    mizu:::map_stage_stream(p[["ctrl"]], 1:4, identity, list(), chunks = NA),
    "positive number"
  )
  expect_error(
    mizu:::map_stage_stream(
      p[["ctrl"]],
      1:4,
      identity,
      list(),
      chunks = c(1, 2)
    ),
    "positive number"
  )
  pool_end(p)
})

test_that("a deadline expired at prime marks the run timed out, nothing submitted", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(p[["ctrl"]], 1:8, identity, list())
  mizu:::map_submit_stream(p[["ctrl"]], st, mizu:::mono_time() - 1)
  r <- mizu:::map_collect_stream(st, mizu:::mono_time() - 1)
  expect_s3_class(r, "mizu_timeout")
  expect_true(st[["timed_out"]])
  expect_length(st[["oi"]], 0L)
  pool_end(p)
})

test_that("a deadline expired at a turn marks the run timed out", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(p[["ctrl"]], 1:8, identity, list())
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  mizu:::map_stream_turn(p[["ctrl"]], st, mizu:::mono_time() - 1)
  expect_true(st[["timed_out"]])
  mizu:::map_cancel(st)
  pool_end(p)
})

test_that("a ring-full submit past the deadline marks the run timed out", {
  p <- pool_pair(workers = 2L, injection_cap = 2L)
  st <- mizu:::map_stage_stream(p[["ctrl"]], 1:8, identity, list())
  # W = 4 but the two-slot ring: chunk 3's submit blocks with the workers
  # never stepped
  mizu:::map_submit_stream(p[["ctrl"]], st, mizu:::mono_time() + 0.05)
  r <- mizu:::map_collect_stream(st, mizu:::mono_time() + 0.05)
  expect_s3_class(r, "mizu_timeout")
  expect_true(st[["timed_out"]])
  mizu:::map_cancel(st)
  while (pool_step(p) == 1L) {
    NULL
  }
  pool_end(p)
})

test_that("an unannotated chunk outcome stays fatal", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(p[["ctrl"]], 1:8, identity, list(), chunks = 4)
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  .Call(mizu:::mizu_pool_cancel, st[["handles"]][[1L]])
  expect_error(
    mizu:::map_stream_turn(p[["ctrl"]], st, Inf),
    "task cancelled or pool stopped",
    class = "mizu_error_cancelled"
  )
  pool_end(p)
})

test_that("a deadline expiring mid-run cancels the window, sentinel returned", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(p[["ctrl"]], 1:8, identity, list())
  mizu:::map_submit_stream(p[["ctrl"]], st, mizu:::mono_time() + 0.05)
  r <- mizu:::map_collect_stream(st, mizu:::mono_time() + 0.05)
  expect_s3_class(r, "mizu_timeout")
  expect_true(st[["timed_out"]])
  mizu:::map_cancel(st)
  expect_true(all(
    mizu_pool_dump(p[["ctrl"]])[["tasks"]][["status"]] == "cancel"
  ))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 6L)
  )
  pool_end(p)
})

test_that("mizu_map(.stream) end-to-end and out-of-order completion", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  # chunk 1's first element sleeps, so chunks 2+ complete first — the
  # splice still assembles in input order
  f <- function(i) {
    if (i == 1L) {
      Sys.sleep(0.3)
    }
    i * 2L
  }
  r <- mizu_collect(mizu_map(p, 1:32, f, .stream = TRUE, .n_chunks = 8L))
  expect_identical(r, as.list((1:32) * 2L))
  # identical to the non-streaming map on the same inputs
  expect_identical(r, mizu_collect(mizu_map(p, 1:32, f, .n_chunks = 8L)))
  v <- mizu_collect(mizu_map(
    p,
    1:32 + 0,
    function(i) i * 2,
    .template = numeric(1),
    .stream = TRUE
  ))
  expect_identical(v, (1:32 + 0) * 2)
  s <- mizu_collect(mizu_map(
    p,
    1:32,
    function(i) rnorm(1),
    .seed = 7L,
    .stream = TRUE
  ))
  expect_identical(
    s,
    mizu_collect(mizu_map(p, 1:32, function(i) rnorm(1), .seed = 7L))
  )
  # timeout surfaces the sentinel, never raises
  t <- mizu_collect(
    mizu_map(
      p,
      1:8,
      function(i) {
        Sys.sleep(2)
        i
      },
      .stream = TRUE
    ),
    timeout = 0.3
  )
  expect_s3_class(t, "mizu_timeout")
  expect_true(mizu_pool_stop(p))
})

test_that("mizu_map(.stream) needs no pool round-trip for empty x", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L)
  expect_identical(
    mizu_collect(mizu_map(p, integer(0), identity, .stream = TRUE)),
    list()
  )
  expect_true(mizu_pool_stop(p))
})

test_that("worker death fails a streaming map with the chunk's exact range", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  # parked before submit, as the blob-path death test: exactly one chunk
  # ends up sleeping in elements 3-4 while the other publishes ok
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  st <- mizu:::map_stage_stream(
    p,
    1:4,
    function(i) {
      if (i > 2L) {
        Sys.sleep(30)
      }
      i
    },
    list(),
    chunks = 2
  )
  mizu:::map_stream_prime(p, st, Inf)
  victim <- -1
  expect_true(wait_until(
    {
      d <- mizu_pool_dump(p)
      pend <- d[["tasks"]][d[["tasks"]][["status"]] == "pending", ]
      hit <- any(d[["tasks"]][["status"]] == "ok") &&
        nrow(pend) == 1L &&
        pend[["worker"]] >= 0L
      if (hit) {
        victim <- d[["workers"]][["pid"]][pend[["worker"]] + 1L]
      }
      hit
    },
    timeout = 10
  ))
  if (!(victim > 0)) {
    stop("no victim pid")
  } # a failed gate must never reach kill(-1)
  kill_hard(victim)
  e <- tryCatch(
    {
      while (length(st[["oi"]]) && !st[["timed_out"]]) {
        mizu:::map_stream_turn(p, st, mizu:::mono_time() + 30)
      }
      NULL
    },
    error = identity
  )
  expect_s3_class(e, "mizu_error_worker_died")
  # streaming chunks are fixed ranges: the lost set is the dead chunk,
  # exactly (the blob-path shape)
  expect_identical(e[["elements"]], cbind(lo = 3, hi = 4))
  # the pool remains serviceable on the survivor
  expect_identical(
    mizu_collect(mizu_map(
      p,
      1:4,
      function(i) i + 1L,
      .stream = TRUE,
      .timeout = 30
    )),
    as.list(2:5)
  )
  expect_true(mizu_pool_stop(p))
})

test_that("a drained worker death wins over the observed error", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  # chunk 1 errors fast; chunk 2 sleeps in elements 3-4 until killed below
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  st <- mizu:::map_stage_stream(
    p,
    1:4,
    function(i) {
      if (i <= 2L) {
        stop("boom")
      }
      Sys.sleep(30)
    },
    list(),
    chunks = 2
  )
  mizu:::map_stream_prime(p, st, Inf)
  victim <- -1
  expect_true(wait_until(
    {
      d <- mizu_pool_dump(p)
      pend <- d[["tasks"]][d[["tasks"]][["status"]] == "pending", ]
      hit <- any(d[["tasks"]][["status"]] == "err") &&
        nrow(pend) == 1L &&
        pend[["worker"]] >= 0L
      if (hit) {
        victim <- d[["workers"]][["pid"]][pend[["worker"]] + 1L]
      }
      hit
    },
    timeout = 10
  ))
  if (!(victim > 0)) {
    stop("no victim pid")
  } # a failed gate must never reach kill(-1)
  kill_hard(victim)
  # the drain sweeps non-blockingly: the death verdict must have landed
  expect_true(wait_until(
    !any(mizu_pool_dump(p)[["tasks"]][["status"]] == "pending"),
    timeout = 10
  ))
  e <- tryCatch(
    {
      mizu:::map_stream_turn(p, st, mizu:::mono_time() + 30)
      NULL
    },
    error = identity
  )
  expect_s3_class(e, "mizu_error_worker_died")
  expect_identical(e[["elements"]], cbind(lo = 3, hi = 4))
  expect_true(mizu_pool_stop(p))
})

test_that("deaths on both workers accumulate every lost range", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  # both chunks sleep: each worker holds one when both die
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  st <- mizu:::map_stage_stream(
    p,
    1:4,
    function(i) Sys.sleep(30),
    list(),
    chunks = 2
  )
  mizu:::map_stream_prime(p, st, Inf)
  victims <- integer(0)
  expect_true(wait_until(
    {
      d <- mizu_pool_dump(p)
      pend <- d[["tasks"]][d[["tasks"]][["status"]] == "pending", ]
      hit <- nrow(pend) == 2L && all(pend[["worker"]] >= 0L)
      if (hit) {
        victims <- d[["workers"]][["pid"]][pend[["worker"]] + 1L]
      }
      hit
    },
    timeout = 10
  ))
  if (any(!(victims > 0))) {
    stop("no victim pid")
  } # a failed gate must never reach kill(-1)
  kill_hard(victims[[1L]])
  kill_hard(victims[[2L]])
  expect_true(wait_until(
    !any(mizu_pool_dump(p)[["tasks"]][["status"]] == "pending"),
    timeout = 10
  ))
  e <- tryCatch(
    {
      mizu:::map_stream_turn(p, st, mizu:::mono_time() + 30)
      NULL
    },
    error = identity
  )
  expect_s3_class(e, "mizu_error_worker_died")
  expect_identical(e[["elements"]], cbind(lo = c(1, 3), hi = c(2, 4)))
  expect_true(mizu_pool_stop(p))
})

test_that("prepared streaming re-arms and matches mizu_map exactly", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  x <- 1:16 + 0
  pm <- mizu_map_prepare(p, x, function(i) rnorm(2L), .stream = TRUE)
  name1 <- pm[["st"]][["name"]]
  r1 <- mizu_collect(mizu_map_run(pm, .seed = 7L))
  r2 <- mizu_collect(mizu_map_run(pm, .seed = 7L))
  expect_identical(r1, r2) # invariance across re-runs
  expect_identical(pm[["st"]][["name"]], name1) # one region throughout
  expect_identical(
    r1,
    mizu_collect(mizu_map(
      p,
      x,
      function(i) rnorm(2L),
      .seed = 7L,
      .stream = TRUE
    ))
  )
  expect_true(mizu_pool_stop(p))
})

test_that("an unclean prepared run restages into a fresh region", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L)
  pm <- mizu_map_prepare(
    p,
    1:4 + 0,
    function(i) {
      Sys.sleep(i / 10)
      i
    },
    .stream = TRUE
  )
  name1 <- pm[["st"]][["name"]]
  r <- mizu_collect(mizu_map_run(pm), timeout = 0.05)
  expect_s3_class(r, "mizu_timeout")
  expect_null(pm[["st"]]) # stale: the next run restages
  r <- mizu_collect(mizu_map_run(pm), timeout = 60)
  expect_identical(r, as.list(1:4 + 0))
  expect_false(identical(pm[["st"]][["name"]], name1))
  # an error in f is unclean too — and restaging reproduces it cleanly
  pm2 <- mizu_map_prepare(
    p,
    1:3 + 0,
    function(i) {
      if (i == 2) stop("bad") else i
    },
    .stream = TRUE
  )
  expect_error(mizu_collect(mizu_map_run(pm2)), "bad")
  expect_null(pm2[["st"]])
  expect_error(mizu_collect(mizu_map_run(pm2)), "bad")
  expect_true(mizu_pool_stop(p))
})

test_that("a prepared streaming x replacement re-slices; template lengths restage", {
  p <- pool_pair()
  pm <- mizu_map_prepare(
    p[["ctrl"]],
    1:10,
    function(i) i * 2L,
    .stream = TRUE
  )
  name1 <- pm[["st"]][["name"]]
  expect_identical(run_stream(p, pm[["st"]]), as.list(1:10 * 2L))
  # any shape works by re-slicing: the region is x-independent
  mizu:::map_swap_x_stream(pm, 1:4)
  expect_identical(pm[["st"]][["name"]], name1)
  expect_identical(pm[["st"]][["n"]], 4L)
  expect_identical(sum(pm[["st"]][["hi"]] - pm[["st"]][["lo"]] + 1), 4)
  expect_identical(run_stream(p, pm[["st"]]), as.list(1:4 * 2L))
  # even a list replacement needs no restage
  mizu:::map_swap_x_stream(pm, list(1, 2, 3))
  expect_identical(pm[["st"]][["name"]], name1)
  expect_identical(run_stream(p, pm[["st"]]), list(2, 4, 6))
  # an object x coerces on swap, exactly as at stage
  mizu:::map_swap_x_stream(pm, data.frame(a = 1:2, b = 3:4))
  expect_identical(pm[["x"]], list(a = 1:2, b = 3:4))
  # a swap with no staged state is a no-op beyond the x update
  pm2 <- mizu_map_prepare(p[["ctrl"]], 1:4, identity, .stream = TRUE)
  pm2[["st"]] <- NULL
  mizu:::map_swap_x_stream(pm2, 1:6)
  expect_identical(pm2[["x"]], 1:6)
  expect_null(pm2[["st"]])
  # a length change under .template restages (the output area sizes off n)
  pm2 <- mizu_map_prepare(
    p[["ctrl"]],
    1:10,
    function(i) i * 2,
    .template = numeric(1),
    .stream = TRUE
  )
  name2 <- pm2[["st"]][["name"]]
  mizu:::map_swap_x_stream(pm2, 1:6)
  expect_null(pm2[["st"]])
  pool_end(p)
})

test_that("mizu_map_run swaps x on the streaming path ahead of the run", {
  p <- pool_pair()
  pm <- mizu_map_prepare(p[["ctrl"]], 1:10, function(i) i * 2L, .stream = TRUE)
  # the swap lands before the run starts: even a timed-out run keeps it
  r <- mizu_collect(mizu_map_run(pm, .x = 1:4), timeout = 0.001)
  expect_s3_class(r, "mizu_timeout")
  expect_identical(pm[["x"]], 1:4)
  expect_null(pm[["st"]]) # the unclean run's state is not re-attached
  pool_end(p)
})

test_that("a streaming rearm with a full subrange errors before any reset", {
  p <- pool_pair()
  pm <- mizu_map_prepare(p[["ctrl"]], 1:4, identity, .stream = TRUE)
  held <- lapply(1:8, function(i) mizu_submit(p[["ctrl"]], v, v = i))
  expect_error(
    mizu:::map_rearm_stream(p[["ctrl"]], pm[["st"]], NULL),
    "result slots exhausted"
  )
  while (pool_step(p) == 1L) {
    NULL
  }
  for (i in 1:8) {
    expect_identical(mizu_collect(held[[i]], timeout = 5), i)
  }
  pool_end(p)
})

test_that("a streaming turn surfaces an infrastructure error, not a subscript error (R-17)", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], 1 + 1)
  pool_step(p)
  expect_identical(mizu_collect(t, 5), 2)
  # a stale handle makes collect_any raise pool-side ("already collected"),
  # which carries no window-position stamp: pre-fix the turn indexed with
  # NULL and the map abort masked the cause as a subscript error
  st <- list(oi = 1L, handles = list(t))
  expect_error(
    mizu:::map_stream_turn(p[["ctrl"]], st, mizu:::mono_time() + 5),
    "already collected"
  )
  pool_end(p)
})

test_that("map_stream_fail's spec arm classifies on mizu_map_index only (R-18)", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], 1)
  # collect_any stamps every raised outcome with "index" the window
  # position: an infrastructure error (no element index) must take the
  # stop arm as-is, not the errs branch with a fabricated element index
  st <- list(lo = 1, hi = 1, oi = 1L, handles = list(t), spec = TRUE)
  v <- simpleError("infrastructure boom")
  err <- tryCatch(
    mizu:::map_stream_fail(st, v, 1L, TRUE),
    error = function(e) e
  )
  expect_identical(conditionMessage(err), "infrastructure boom")
  expect_null(err[["mizu_map_index"]])
  expect_null(err[["index"]])
  pool_end(p)
})

test_that("a spec .f streams a bare atomic x through the pre-built descriptor", {
  p <- pool_pair(workers = 2L)
  x <- runif(20)
  st <- mizu:::map_stage_stream(p[["ctrl"]], x, mizu_call("base::abs"), list())
  expect_identical(run_stream(p, st), as.list(abs(x)))
  pool_end(p)
})

test_that("a failing streaming spec map reports the element index", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:8,
    mizu_call(.source = 'if (x == 3L) stop("boom") else x'),
    list(),
    chunks = 4
  )
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  expect_identical(pool_step(p), 1L) # chunk 1
  mizu:::map_stream_turn(p[["ctrl"]], st, Inf) # collects 1, refills 3
  expect_identical(pool_step(p), 1L) # chunk 2 errors at 3
  e <- tryCatch(
    {
      mizu:::map_stream_turn(p[["ctrl"]], st, Inf)
      NULL
    },
    error = function(e) e
  )
  expect_identical(conditionMessage(e), "boom")
  expect_identical(e[["mizu_map_index"]], 3)
  pool_end(p)
})

test_that("a worker death in a streaming spec map fails with its range", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  on.exit(mizu_pool_stop(p))
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  st <- mizu:::map_stage_stream(
    p,
    1:8,
    mizu_call(.source = "if (x %in% 3:4) Sys.sleep(30) else x"),
    list(),
    chunks = 4
  )
  mizu:::map_stream_prime(p, st, Inf)
  victim <- -1
  expect_true(wait_until(
    {
      d <- mizu_pool_dump(p)
      pend <- d[["tasks"]][
        d[["tasks"]][["status"]] == "pending" & d[["tasks"]][["worker"]] >= 0L,
      ]
      hit <- any(d[["tasks"]][["status"]] == "ok") && nrow(pend) >= 1L
      if (hit) {
        victim <- d[["workers"]][["pid"]][pend[["worker"]][[1L]] + 1L]
      }
      hit
    },
    timeout = 10
  ))
  if (!(victim > 0)) {
    stop("no victim pid")
  }
  kill_hard(victim)
  e <- tryCatch(
    {
      for (i in 1:4) {
        mizu:::map_stream_turn(p, st, mizu:::mono_time() + 30)
      }
      NULL
    },
    error = function(e) e
  )
  expect_s3_class(e, "mizu_error_worker_died")
})

test_that("a death drained beside an error takes the death verdict", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  on.exit(mizu_pool_stop(p))
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  st <- mizu:::map_stage_stream(
    p,
    1:8,
    mizu_call(
      .source = 'if (x == 1L) stop("boom") else if (x %in% 3:4) Sys.sleep(30) else x'
    ),
    list(),
    chunks = 4
  )
  mizu:::map_stream_prime(p, st, Inf)
  victim <- -1
  expect_true(wait_until(
    {
      d <- mizu_pool_dump(p)
      pend <- d[["tasks"]][
        d[["tasks"]][["status"]] == "pending" & d[["tasks"]][["worker"]] >= 0L,
      ]
      hit <- any(d[["tasks"]][["status"]] == "err") && nrow(pend) >= 1L
      if (hit) {
        victim <- d[["workers"]][["pid"]][pend[["worker"]][[1L]] + 1L]
      }
      hit
    },
    timeout = 10
  ))
  if (!(victim > 0)) {
    stop("no victim pid")
  }
  kill_hard(victim)
  # the turn's drain is non-blocking: the death verdict must have landed
  expect_true(wait_until(
    "died" %in% mizu_pool_dump(p)[["tasks"]][["status"]],
    timeout = 10
  ))
  e <- tryCatch(
    mizu:::map_stream_turn(p, st, mizu:::mono_time() + 30),
    error = function(e) e
  )
  expect_s3_class(e, "mizu_error_worker_died")
})

test_that("the map descriptor reader validates the 'I' shape", {
  u64 <- function(x) {
    lo <- x %% 2^32
    hi <- (x - lo) / 2^32
    c(
      writeBin(as.integer(lo), raw(4L), endian = "little"),
      writeBin(as.integer(hi), raw(4L), endian = "little")
    )
  }
  magic <- as.raw(c(0x49, 0x01))
  desc_stage <- function(bytes) {
    .Call(mizu:::mizu_map_stage, bytes, NULL, NULL, 10, NULL, 1)[[2L]]
  }
  # not a two-element list
  h <- desc_stage(c(magic, as.raw(0x0c), u64(1), as.raw(0)))
  expect_error(.Call(mizu:::mizu_map_desc, h), "two-element list")
  # no task tag in the first slot
  h2 <- desc_stage(c(magic, as.raw(0x0c), u64(2), as.raw(0), as.raw(0)))
  expect_error(.Call(mizu:::mizu_map_desc, h2), "no task tag")
  # a kind-2 (runner) task is not a call spec
  runner <- .Call(mizu:::mizu_interop_runner_call, "nm", 1, NULL, 2L, 1)
  h3 <- desc_stage(c(magic, as.raw(0x0c), u64(2), runner[-c(1, 2)], as.raw(0)))
  expect_error(.Call(mizu:::mizu_map_desc, h3), "not a call spec")
  # trailing bytes after a valid descriptor
  spec <- mizu_call("base::abs")
  good <- .Call(mizu:::mizu_interop_map_desc_call, spec, NULL, 2L)
  h4 <- desc_stage(c(good, as.raw(0)))
  expect_error(.Call(mizu:::mizu_map_desc, h4), "mizu")
})

test_that("a streaming spec map re-classes an integer64 template gather", {
  skip_if_not_installed("bit64")
  p <- pool_pair()
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:8,
    mizu_call(.source = "bit64::as.integer64(x)"),
    list(),
    template = bit64::as.integer64(1)
  )
  out <- run_stream(p, st)
  expect_true(bit64::is.integer64(out))
  expect_identical(out, bit64::as.integer64(1:8))
  pool_end(p)
})

test_that("an infrastructure error on a drained spec sibling stays fatal", {
  p <- pool_pair()
  st <- mizu:::map_stage_stream(
    p[["ctrl"]],
    1:4,
    mizu_call(.source = 'if (x == 1L) stop("boom") else x'),
    list(),
    chunks = 2
  )
  mizu:::map_stream_prime(p[["ctrl"]], st, Inf)
  # a sibling whose chunk names a region that does not resolve: the
  # worker's ctx attach fails outside f — not f's error, not a death, and
  # it must re-raise rather than drain silently
  h <- .Call(
    mizu:::mizu_pool_submit_try,
    p[["ctrl"]],
    list(
      quote(mizu:::map_stream_chunk("mizu-nonexistent", c(1, 2), 1:2)),
      list()
    ),
    Inf,
    0L
  )
  st[["handles"]][[3L]] <- h
  st[["oi"]] <- c(st[["oi"]], 3L)
  # drive the window: chunk 1 errors, chunk 2 ok, the bogus chunk's
  # attach fails — claim order does not matter to the drain
  for (i in seq_len(6L)) {
    if (pool_step(p) != 1L) {
      break
    }
  }
  e <- tryCatch(
    {
      mizu:::map_stream_turn(p[["ctrl"]], st, Inf)
      NULL
    },
    error = function(e) e
  )
  expect_s3_class(e, "mizu_error")
  expect_false(inherits(e, "mizu_error_worker_died"))
  expect_null(e[["mizu_map_index"]])
  pool_end(p)
})
