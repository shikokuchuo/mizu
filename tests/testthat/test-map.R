# mizu_map over the in-process harness. The map stages are composable
# precisely for this file: pool_pair() is single-process, so a monolithic
# mizu_map would block in collect with nobody to step the worker — tests
# drive map_stage / map_submit / map_collect and interleave pool_step()
# between submit and collect. Cross-process maps (spawned workers, worker
# death, stealing) are test-map-process.R.

# collect under a file-wide 30s guard: a chunk missed by the stepping
# above fails the test loudly instead of hanging CI on an infinite wait
collect30 <- function(pool, st) {
  mizu:::map_collect(st, deadline = mizu:::mono_time() + 30)
}

run_map <- function(p, x, f, dots = list(), ..., steps = 256L) {
  st <- mizu:::map_stage(p[["ctrl"]], x, f, dots, ...)
  mizu:::map_submit(p[["ctrl"]], st)
  for (i in seq_len(steps)) {
    if (pool_step(p) != 1L) break
  }
  collect30(p[["ctrl"]], st)
}

test_that("strip_srcref strips source references, original and attrs intact", {
  f <- eval(parse(
    text = "function(x, d = {\n  1\n}) {\n  if (x > 0) {\n    log(x)\n  } else {\n    0\n  }\n}",
    keep.source = TRUE
  ))
  attr(f, "foo") <- "bar"
  z <- mizu:::strip_srcref(f)
  expect_null(attributes(body(z)[[2L]][[3L]]))
  expect_null(attributes(formals(z)[["d"]]))
  expect_identical(attr(z, "foo"), "bar")
  expect_identical(z, utils::removeSource(f))
  expect_s3_class(attr(f, "srcref"), "srcref")
  expect_identical(z(2), log(2))
  g <- eval(quote(function(x) x))
  expect_identical(mizu:::strip_srcref(g), g)
})

test_that("strip_srcref drops nested function literals' inline srcrefs", {
  f <- eval(parse(
    text = "function(x) {\n  g <- function(y) y + 1\n  g(x)\n}",
    keep.source = TRUE
  ))
  z <- mizu:::strip_srcref(f)
  expect_length(body(f)[[2L]][[3L]], 4L)
  expect_length(body(z)[[2L]][[3L]], 3L)
  expect_identical(z(1), 2)
  h <- eval(parse(
    text = "function(x) {\n  (function(y) y + 1)(x)\n}",
    keep.source = TRUE
  ))
  zh <- mizu:::strip_srcref(h)
  expect_length(body(zh)[[2L]][[1L]][[2L]], 3L)
  expect_identical(zh(1), 2)
})

test_that("strip_srcref makes the staged form keep.source-invariant", {
  sourced <- eval(parse(
    text = "function(x) {\n  g <- function(y) y + 1\n  g(x)\n}",
    keep.source = TRUE
  ))
  plain <- eval(parse(
    text = "function(x) {\n  g <- function(y) y + 1\n  g(x)\n}",
    keep.source = FALSE
  ))
  expect_identical(
    serialize(mizu:::strip_srcref(sourced), NULL),
    serialize(mizu:::strip_srcref(plain), NULL)
  )
})

test_that("strip_srcref handles edge bodies and non-closures", {
  expect_identical(mizu:::strip_srcref(1:3), 1:3)
  expect_identical(mizu:::strip_srcref(function() 1)(), 1)
  fc <- compiler::cmpfun(function(x) {
    y <- x + 1
    y
  })
  expect_identical(mizu:::strip_srcref(fc)(2), 3)
  cl <- quote(function(x) x)
  cl[[3L]] <- expression(1 + 1)
  expect_identical(body(mizu:::strip_srcref(eval(cl))), expression(1 + 1))
})

test_that("map staging is keep.source-invariant", {
  p <- pool_pair()
  sourced <- eval(parse(
    text = "function(i) {\n  g <- function(j) j + 1\n  g(i)\n}",
    keep.source = TRUE
  ))
  plain <- eval(parse(
    text = "function(i) {\n  g <- function(j) j + 1\n  g(i)\n}",
    keep.source = FALSE
  ))
  environment(sourced) <- environment(plain) <- globalenv()
  st1 <- mizu:::map_stage(p[["ctrl"]], list(1), sourced, list())
  st2 <- mizu:::map_stage(p[["ctrl"]], list(1), plain, list())
  expect_type(st1[["blob"]], "raw")
  expect_identical(st1[["blob"]], st2[["blob"]])
  pool_end(p)
})

test_that("a map returns results in input order with names reapplied", {
  p <- pool_pair()
  x <- setNames(1:10, letters[1:10])
  st <- mizu:::map_stage(
    p[["ctrl"]],
    x,
    function(i) i * 2L,
    list(),
    chunks = 3
  )
  # a named atomic x stays on the RAWVEC path: names never cross the wire
  expect_true(st[["xraw"]])
  # .chunks is the morsel count: ceiling-sized morsels cover [1, n]
  expect_identical(st[["nm"]], 3)
  expect_identical(st[["ms"]], 4)
  expect_identical(.Call(mizu:::mizu_map_info, st[["wrap"]])[["n_morsels"]], 3)
  # one runner per live worker, not one task per morsel
  expect_identical(st[["R"]], 1L)
  mizu:::map_submit(p[["ctrl"]], st)
  expect_identical(mizu_pool_status(p[["ctrl"]])[["injection"]], 1)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(
    collect30(p[["ctrl"]], st),
    as.list(setNames(1:10 * 2L, letters[1:10]))
  )
  pool_end(p)
})

test_that("every atomic x type maps, RAWVEC or descriptor as gated", {
  p <- pool_pair()
  for (x in list(
    c(TRUE, FALSE, NA),
    c(1L, 5L, 3L),
    c(1.5, 2.5),
    c(1i, 2i),
    as.raw(1:4)
  )) {
    st <- mizu:::map_stage(p[["ctrl"]], x, identity, list())
    expect_true(st[["xraw"]])
    mizu:::map_submit(p[["ctrl"]], st)
    while (pool_step(p) == 1L) {
      NULL
    }
    expect_identical(collect30(p[["ctrl"]], st), lapply(x, identity))
  }
  # character, list, and classed vectors ride the descriptor
  for (x in list(
    letters[1:5],
    list(1:3, "a"),
    factor(c("a", "b")),
    as.Date("2026-07-24") + 0:2
  )) {
    st <- mizu:::map_stage(p[["ctrl"]], x, identity, list())
    expect_false(st[["xraw"]])
    mizu:::map_submit(p[["ctrl"]], st)
    while (pool_step(p) == 1L) {
      NULL
    }
    expect_identical(collect30(p[["ctrl"]], st), lapply(x, identity))
  }
  # an extra attribute (beyond names) also disqualifies bare-byte slicing,
  # as does ALTREP: a compact 1:n serializes as its compact form — smaller
  # than its bytes — and a mori-shared x must reduce to its identifier
  x <- structure(c(1L, 2L), extra = "attr")
  expect_false(mizu:::map_stage(p[["ctrl"]], x, identity, list())[["xraw"]])
  expect_false(mizu:::map_stage(p[["ctrl"]], 1:5, identity, list())[["xraw"]])
  pool_end(p)
})

test_that("constant dots stage once and reach every element", {
  p <- pool_pair()
  r <- run_map(p, list(1:3, 4:6), function(v, w) sum(v) + w, list(w = 100L))
  expect_identical(r, list(106L, 115L))
  pool_end(p)
})

test_that("constant names never collide with the map formals ahead of ...", {
  p <- pool_pair()
  # an empty .x returns before the blocking run: mizu_map() itself matches
  expect_identical(
    mizu_map(p[["ctrl"]], list(), function(i, x) i + x, x = 10L),
    list()
  )
  # values through mizu_map_prepare(), which shares the signature
  pm <- mizu_map_prepare(p[["ctrl"]], 1:3, function(i, x) i + x, x = 10L)
  mizu:::map_submit(p[["ctrl"]], pm[["st"]])
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], pm[["st"]]), list(11L, 12L, 13L))
  pm <- mizu_map_prepare(p[["ctrl"]], 1:3, function(i, p) i * p, p = 2L)
  mizu:::map_submit(p[["ctrl"]], pm[["st"]])
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], pm[["st"]]), list(2L, 4L, 6L))
  pm <- mizu_map_prepare(p[["ctrl"]], 1:3, function(i, f) i * f, f = 2L)
  mizu:::map_submit(p[["ctrl"]], pm[["st"]])
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], pm[["st"]]), list(2L, 4L, 6L))
  pool_end(p)
})

test_that("a data.frame x maps over its columns, as lapply", {
  p <- pool_pair()
  df <- data.frame(a = 1:3, b = 4:6)
  expect_identical(run_map(p, df, sum), lapply(df, sum))
  pool_end(p)
})

test_that("the template path gathers exactly what vapply returns", {
  p <- pool_pair()
  # same-type memcpy
  expect_identical(
    run_map(p, 1:8, function(i) i * 2L, template = integer(1)),
    vapply(1:8, function(i) i * 2L, integer(1))
  )
  # upward widening: integer (and logical) results into a double template
  expect_identical(
    run_map(p, 1:8, function(i) i, template = numeric(1)),
    vapply(1:8, function(i) i, numeric(1))
  )
  expect_identical(
    run_map(p, 1:4, function(i) i > 2L, template = numeric(1)),
    vapply(1:4, function(i) i > 2L, numeric(1))
  )
  # NA semantics ride R's own coercion
  expect_identical(
    run_map(p, 1:3, function(i) c(NA, i)[1L], template = numeric(1)),
    vapply(1:3, function(i) c(NA, i)[1L], numeric(1))
  )
  # m > 1 gathers a matrix, template names as rownames, names(x) as colnames
  f <- function(i) c(a = i * 1, b = i * 2)
  x <- setNames(1:5, letters[1:5])
  expect_identical(
    run_map(p, x, f, template = c(lo = 0, hi = 0)),
    vapply(x, f, c(lo = 0, hi = 0))
  )
  pool_end(p)
})

test_that("character templates assemble through the generic path", {
  p <- pool_pair()
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:4,
    function(i) letters[i],
    list(),
    template = character(1)
  )
  expect_false(st[["direct"]]) # no output area: chunks return lists
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(
    collect30(p[["ctrl"]], st),
    vapply(1:4, function(i) letters[i], character(1))
  )
  pool_end(p)
})

test_that("a view x rides the descriptor by reference and reads off the shared pages", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], seq_len(100000) + 0.5)
  pool_step(p)
  v <- mizu_collect(t, 5)
  pool_step(p) # the worker's keeper sweep drops its loan
  expect_identical(.Call(mizu:::mizu_zc_refcount, v), c(1L, 0L))

  # globalenv keeps the descriptor to f + dots + the ~30-byte identifier;
  # pad overflows the entry budget, forcing the region path
  f <- function(i, pad) i * 2
  environment(f) <- globalenv()
  st <- mizu:::map_stage(p[["ctrl"]], v, f, list(pad = runif(50)))
  expect_false(st[["xraw"]]) # ALTREP: the descriptor carries the identifier
  expect_null(st[["blob"]])
  expect_identical(.Call(mizu:::mizu_zc_refcount, v)[2L], 1L) # REFHELD
  mizu:::map_submit(p[["ctrl"]], st)
  expect_identical(pool_step(p), 1L) # the one runner executes whole
  # the worker's resolve was counted and its element reads never
  # materialized the view (a COW materialize would release the count).
  # Read before any empty step: the idle sweep drops the context cache
  expect_identical(.Call(mizu:::mizu_zc_refcount, v), c(2L, 1L))
  expect_identical(
    collect30(p[["ctrl"]], st),
    as.list((seq_len(100000) + 0.5) * 2)
  )
  pool_end(p)
})

test_that("a small map over a view stays inline and reads off the pages", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], seq_len(10000) + 0)
  pool_step(p)
  v <- mizu_collect(t, 5)
  pool_step(p)
  expect_identical(.Call(mizu:::mizu_zc_refcount, v), c(1L, 0L))

  f <- function(i) i * 2
  environment(f) <- globalenv()
  st <- mizu:::map_stage(p[["ctrl"]], v, f, list())
  expect_type(st[["blob"]], "raw") # the identifier keeps the descriptor inline
  mizu:::map_submit(p[["ctrl"]], st)
  gc() # the count check below assumes no GC finalizes the chunk's view first
  expect_identical(pool_step(p), 1L) # the first chunk
  # the chunk's resolve was counted and its reads did not materialize the
  # view (a COW materialize would already have released the count)
  expect_identical(.Call(mizu:::mizu_zc_refcount, v), c(2L, 1L))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(
    collect30(p[["ctrl"]], st),
    as.list((seq_len(10000) + 0) * 2)
  )
  pool_end(p)
})

test_that(".collect = \"view\" wraps the output area, names applied, COW intact", {
  p <- pool_pair()
  t <- mizu_submit(
    p[["ctrl"]],
    setNames(seq_len(10000) + 0.5, paste0("e", 1:10000))
  )
  pool_step(p)
  v <- mizu_collect(t, 5)
  st <- mizu:::map_stage(
    p[["ctrl"]],
    v,
    function(i) i * 2.5,
    list(),
    template = numeric(1)
  )
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  res <- mizu:::map_collect(st, mizu:::mono_time() + 30, "view")
  expect_true(.Call(mizu:::mizu_zc_view_check, res))
  expect_true(st[["consumed"]])
  expect_identical(names(res), paste0("e", 1:10000))
  expect_identical(as.numeric(res), (seq_len(10000) + 0.5) * 2.5)
  # COW: a write materializes a private copy, the rest undisturbed
  res[1] <- -1
  expect_identical(res[[1]], -1)
  expect_identical(as.numeric(res)[-1], (seq_len(10000) + 0.5)[-1] * 2.5)
  pool_end(p)
})

test_that(".collect = \"view\" gathers a matrix view for m > 1", {
  p <- pool_pair()
  x <- setNames(1:6, letters[1:6])
  f <- function(i) c(a = i * 1, b = i * 2)
  st <- mizu:::map_stage(p[["ctrl"]], x, f, list(), template = c(a = 0, b = 0))
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  res <- mizu:::map_collect(st, mizu:::mono_time() + 30, "view")
  expect_true(.Call(mizu:::mizu_zc_view_check, res))
  expect_identical(dim(res), c(2L, 6L))
  expect_identical(dimnames(res), list(c("a", "b"), letters[1:6]))
  expect_identical(res[], vapply(x, f, c(a = 0, b = 0)))
  pool_end(p)
})

test_that("a held view gates map-region teardown; a prepared re-run restages", {
  p <- pool_pair()
  pm <- mizu_map_prepare(
    p[["ctrl"]],
    1:100,
    function(i) i * 2.5,
    .template = numeric(1)
  )
  # drive mizu_map_run's stages by hand (the harness can't block in collect)
  st <- pm[["st"]]
  mizu:::map_rearm(p[["ctrl"]], st, NULL)
  pm[["st"]] <- NULL
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  v1 <- mizu:::map_collect(st, mizu:::mono_time() + 30, "view")
  expect_true(st[["consumed"]])
  nm <- st[["name"]]
  rm(st) # the map state drops; the view alone pins the region now
  invisible(gc())
  h <- .Call(mizu:::mizu_map_open, nm, TRUE) # still there: teardown gated
  expect_identical(.Call(mizu:::mizu_map_info, h)[["n"]], 100)
  rm(h)

  # a prepared re-run restages into a fresh region rather than re-arming
  # pages v1 still reads (a different f makes a reuse visible)
  st2 <- mizu:::map_stage(
    p[["ctrl"]],
    pm[["x"]],
    function(i) i * 100,
    pm[["dots"]],
    pm[["template"]],
    pm[["chunks"]]
  )
  expect_false(identical(st2[["name"]], nm))
  mizu:::map_submit(p[["ctrl"]], st2)
  while (pool_step(p) == 1L) {
    NULL
  }
  v2 <- mizu:::map_collect(st2, mizu:::mono_time() + 30, "view")
  expect_identical(as.numeric(v1), 1:100 * 2.5)
  expect_identical(as.numeric(v2), 1:100 * 100)
  pool_end(p)
})

test_that(".collect validates against the template", {
  p <- pool_pair()
  expect_snapshot(
    error = TRUE,
    mizu_map(p[["ctrl"]], 1:4, identity, .collect = "view")
  )
  expect_snapshot(
    error = TRUE,
    mizu_map(
      p[["ctrl"]],
      1:4,
      identity,
      .template = character(1),
      .collect = "view"
    )
  )
  expect_snapshot(
    error = TRUE,
    mizu_map(p[["ctrl"]], 1:4, identity, .collect = "all")
  )
  pm <- mizu_map_prepare(p[["ctrl"]], 1:4, identity)
  expect_snapshot(error = TRUE, mizu_map_run(pm, .collect = "view"))
  # an empty map short-circuits before any region exists
  expect_identical(
    mizu_map(
      p[["ctrl"]],
      numeric(0),
      identity,
      .template = numeric(1),
      .collect = "view"
    ),
    numeric(0)
  )
  pool_end(p)
})

test_that("template type and length checks fail per element, with index", {
  p <- pool_pair()
  # wrong length
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:6,
    function(i) rep(i, i),
    list(),
    template = integer(1),
    chunks = 2
  )
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  e <- tryCatch(collect30(p[["ctrl"]], st), error = identity)
  expect_match(conditionMessage(e), "type 'integer' and length 1")
  expect_identical(e[["mizu_map_index"]], 2) # first element whose length isn't 1
  # downward type (double into integer template) is refused, as vapply
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:4,
    function(i) i * 1.5,
    list(),
    template = integer(1)
  )
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  e <- tryCatch(collect30(p[["ctrl"]], st), error = identity)
  expect_match(conditionMessage(e), "type 'integer'")
  pool_end(p)
})

test_that("an error in f re-signals with the failing element's index", {
  p <- pool_pair()
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:10,
    function(i) if (i == 7L) stop("boom ", i) else i,
    list(),
    chunks = 2
  )
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  e <- tryCatch(collect30(p[["ctrl"]], st), error = identity)
  expect_s3_class(e, "simpleError")
  expect_identical(conditionMessage(e), "boom 7")
  expect_identical(e[["mizu_map_index"]], 7)
  pool_end(p)
})

test_that("classed conditions from f survive the crossing, index attached", {
  p <- pool_pair()
  cond <- structure(
    list(message = "typed", call = NULL),
    class = c("mizu_test_error", "error", "condition")
  )
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:4,
    function(i, cond) if (i == 3L) stop(cond) else i,
    list(cond = cond),
    chunks = 2
  )
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  e <- tryCatch(collect30(p[["ctrl"]], st), error = identity)
  expect_s3_class(e, "mizu_test_error")
  expect_identical(e[["mizu_map_index"]], 3)
  pool_end(p)
})

test_that("a runner error stores the cancel word before its ERR publish", {
  p <- pool_pair(workers = 2L)
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:9,
    function(i) if (i == 1L) stop("now"),
    list(),
    chunks = 3
  )
  expect_identical(st[["R"]], 2L)
  mizu:::map_submit(p[["ctrl"]], st)
  # one step: runner 0's first transition sees the doorbell (rung by the
  # submits — no in-process worker ever parks) and re-homes runner 1 onto
  # its own deque (a join ticket, never help-executed); runner 0 then
  # errors at element 1
  pool_step(p)
  # the erroring runner stored the cancel word itself, before its ERR
  # publish: peers observe it without collect being involved
  expect_true(.Call(mizu:::mizu_map_cancel_get, st[["wrap"]]))
  # collect re-raises the minimum-index error among the runners that ran;
  # the cancel word arms the trim, so re-homed, never-claimed runner 1
  # resolves through abandon and its handle is cancelled
  e <- tryCatch(collect30(p[["ctrl"]], st), error = identity)
  expect_identical(conditionMessage(e), "now")
  expect_identical(e[["mizu_map_index"]], 1)
  # the drain pops the re-homed entry, which drops at the CANCEL skip
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("a doorbell help beat re-homes a runner instead of nesting it", {
  p <- pool_pair(workers = 2L)
  ev <- character()
  mizu_pool_trace(p[["wk"]], function(e, id) {
    if (e == "rehome") ev <<- c(ev, id)
  })
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:2,
    function(i) i * 10L,
    list(),
    chunks = 2
  )
  expect_identical(st[["R"]], 2L)
  mizu:::map_submit(p[["ctrl"]], st)
  # one step: wk1 claims runner 0; its first help beat (the submits rang
  # the bell — no in-process worker ever parks) claims runner 1 and moves
  # it onto wk1's own deque instead of executing the join ticket nested —
  # the pre-fix swallow ran both runners on wk1 (tasks 2,0)
  expect_identical(pool_step(p), 1L)
  expect_identical(length(ev), 1L)
  expect_identical(mizu_pool_status(p[["ctrl"]])[["deque"]][[1L]], 1)
  d <- mizu_pool_dump(p[["ctrl"]])
  expect_identical(d[["tasks"]][["status"]], c("ok", "pending"))
  # the re-homed runner is stealable: wk2 claims and completes it
  expect_identical(pool_step(p, wk = p[["wks"]][[2L]]), 1L)
  expect_identical(
    mizu_pool_dump(p[["ctrl"]])[["tasks"]][["status"]],
    c("ok", "ok")
  )
  # empty steps mirror the counters: one task each, the re-home counted as
  # a ring claim (pool_claim_rings) but never as a help (nothing executed)
  pool_step(p)
  pool_step(p, wk = p[["wks"]][[2L]])
  w <- mizu_pool_stats(p[["ctrl"]])[["workers"]]
  expect_identical(w[["tasks"]], c(1, 1))
  expect_identical(w[["steals"]], c(0, 1))
  expect_identical(w[["helps"]], c(0, 0))
  expect_identical(collect30(p[["ctrl"]], st), list(10L, 20L))
  pool_end(p)
})

test_that("help re-homes runners but executes ordinary tasks inline", {
  p <- pool_pair(workers = 2L)
  st <- mizu:::map_stage(p[["ctrl"]], 1:6, function(i) i, list(), chunks = 6)
  expect_identical(st[["R"]], 2L)
  mizu:::map_submit(p[["ctrl"]], st)
  h <- mizu_submit(p[["ctrl"]], quote("ordinary")) # queued behind runner 1
  # one step: runner 0's first help beat re-homes runner 1 — clearing the
  # FIFO head — and restores the bell for the entry behind it; the second
  # beat claims the ordinary task and executes it inline, mid-map
  expect_identical(pool_step(p), 1L)
  d <- mizu_pool_dump(p[["ctrl"]])
  expect_identical(d[["tasks"]][["status"]], c("ok", "pending", "ok"))
  expect_identical(mizu_pool_status(p[["ctrl"]])[["deque"]][[1L]], 1)
  expect_identical(mizu_collect(h), "ordinary")
  # drain wk1's re-homed runner; the closing empty step mirrors the
  # counters — helps counted the ordinary execute only, not the re-home
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(
    mizu_pool_stats(p[["ctrl"]])[["workers"]][["helps"]][[1L]],
    1
  )
  expect_identical(collect30(p[["ctrl"]], st), as.list(1:6))
  pool_end(p)
})

test_that("a full deque falls back to executing the claimed runner inline", {
  # deque_cap = 2: the fallback is reachable by configuration, not only
  # under nested-submit saturation
  p <- pool_pair(workers = 2L, per_worker_cap = 2L)
  h1 <- mizu_submit(p[["ctrl"]], quote(1L))
  h2 <- mizu_submit(p[["ctrl"]], quote(2L))
  expect_identical(pool_pull(p, 2L), 2L) # wk1's deque now at cap
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:2,
    function(i) i * 10L,
    list(),
    chunks = 2
  )
  expect_identical(st[["R"]], 2L)
  mizu:::map_submit(p[["ctrl"]], st)
  # a direct help beat claims runner 0 with no deque space to re-home
  # into: the inline fallback executes it nested, announce intact
  expect_true(.Call(mizu:::mizu_pool_help_once, p[["wk"]]))
  expect_identical(.Call(mizu:::mizu_map_info, st[["wrap"]])[["cursor"]], 2)
  expect_identical(mizu_pool_status(p[["ctrl"]])[["deque"]][[1L]], 2)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(h1), 1L)
  expect_identical(mizu_collect(h2), 2L)
  expect_identical(collect30(p[["ctrl"]], st), list(10L, 20L))
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("collect trims never-claimed runners once the cursor exhausts", {
  p <- pool_pair(workers = 2L)
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:4,
    function(i) i * 2L,
    list(),
    template = integer(1),
    chunks = 4
  )
  expect_identical(st[["R"]], 2L)
  mizu:::map_submit(p[["ctrl"]], st)
  # a third CLAIM lane drains the cursor through the protocol directly —
  # the stand-in for peers finishing the map while both runner tasks sit
  # queued behind busy workers (which never step here)
  repeat {
    nx <- .Call(mizu:::mizu_map_next, st[["wrap"]], 2L, 0, NULL, 64, NULL)
    if (is.null(nx)) {
      break
    }
    for (e in nx[[3L]]:nx[[4L]]) {
      .Call(mizu:::mizu_map_write, st[["wrap"]], e, e * 2L)
    }
  }
  expect_identical(.Call(mizu:::mizu_map_info, st[["wrap"]])[["cursor"]], 4)
  # collect must not park on the never-claimed handles: the exhausted
  # cursor arms the trim, both resolve through abandon, and the gathered
  # output area carries the results
  expect_identical(
    collect30(p[["ctrl"]], st),
    vapply(1:4, function(i) i * 2L, integer(1))
  )
  for (r in 0:1) {
    expect_identical(
      .Call(mizu:::mizu_map_claim_state, st[["wrap"]], r)[["state"]],
      "abandoned"
    )
  }
  # the trimmed runner tasks were cancelled and drop at their pops
  d <- mizu_pool_dump(p[["ctrl"]])
  expect_identical(d[["tasks"]][["status"]], c("cancel", "cancel"))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("n = 0, n = 1, and n < chunks all behave", {
  p <- pool_pair(slot_size = 256L) # under the blob budget: the region path
  # empty maps return immediately: no region, no tasks
  expect_identical(mizu_map(p[["ctrl"]], list(), sqrt), list())
  expect_identical(
    mizu_map(p[["ctrl"]], setNames(integer(), character()), sqrt),
    setNames(list(), character())
  )
  expect_identical(
    mizu_map(p[["ctrl"]], character(), identity, .template = character(1)),
    character()
  )
  expect_identical(
    mizu_map(p[["ctrl"]], integer(), identity, .template = c(a = 0L, b = 0L)),
    vapply(integer(), identity, c(a = 0L, b = 0L))
  )
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  expect_identical(run_map(p, 5, function(i) i * 2), list(10))
  # more morsels requested than elements: the count clamps to n
  st <- mizu:::map_stage(p[["ctrl"]], 1:3, identity, list(), chunks = 8)
  expect_identical(st[["nm"]], 3)
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), as.list(1:3))
  pool_end(p)
})

test_that("the runner count clamps to live workers and free slots", {
  p <- pool_pair(workers = 4L, slot_size = 256L) # 64 slots / 8 submitters
  st <- mizu:::map_stage(p[["ctrl"]], 1:100, identity, list())
  expect_identical(st[["R"]], 4L) # min(n_morsels, 4 live, 8 free, 64)
  # occupy five slots: the runner count clamps to the 3 free
  held <- lapply(1:5, function(i) mizu_submit(p[["ctrl"]], v, v = i))
  st <- mizu:::map_stage(p[["ctrl"]], 1:100, identity, list())
  expect_identical(st[["R"]], 3L)
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), as.list(1:100))
  for (i in 1:5) {
    expect_identical(mizu_collect(held[[i]], timeout = 5), i)
  }
  pool_end(p)
})

test_that("a fully occupied subrange errors before any region exists", {
  p <- pool_pair()
  held <- lapply(1:8, function(i) mizu_submit(p[["ctrl"]], v, v = i))
  expect_error(mizu_map(p[["ctrl"]], 1:10, identity), "result slots exhausted")
  while (pool_step(p) == 1L) {
    NULL
  }
  for (i in 1:8) {
    expect_identical(mizu_collect(held[[i]], timeout = 5), i)
  }
  pool_end(p)
})

test_that("zero live workers floors R at 1; a rejoined worker drains it", {
  p <- pool_pair()
  suffix <- .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  .Call(mizu:::mizu_pool_leave, p[["wk"]])
  expect_identical(mizu_pool_status(p[["ctrl"]])[["workers"]], "free")
  st <- mizu:::map_stage(p[["ctrl"]], 1:6, function(i) i + 1L, list())
  expect_identical(st[["R"]], 1L)
  mizu:::map_submit(p[["ctrl"]], st) # queues in the injection ring
  wk <- .Call(mizu:::mizu_pool_worker_join, suffix, 0L, NULL)
  .Call(mizu:::mizu_pool_set_eval, wk)
  while (pool_step(p, wk = wk) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), as.list(2:7))
  .Call(mizu:::mizu_pool_leave, wk)
  .Call(mizu:::mizu_pool_destroy, p[["ctrl"]])
})

test_that("a small map rides entirely inline, no region", {
  p <- pool_pair()
  f <- function(i) i + 1L
  environment(f) <- globalenv()
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, f, list(), chunks = 2)
  expect_null(st[["name"]])
  expect_type(st[["blob"]], "raw")
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), as.list(2:5))
  pool_end(p)
})

test_that("the region runner wrapper fits a slot_size = 256 entry budget", {
  p <- pool_pair(slot_size = 256L) # 224-byte entry inline budget
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:100,
    sqrt,
    list(),
    template = numeric(1)
  )
  sz <- .Call(mizu:::mizu_bounded_call, mizu:::runner_payload(st, 0L), 224L)[[
    1L
  ]]
  expect_lte(sz, 224)
  # and indeed nothing spilled over a whole template map: runner wrappers
  # stayed inline, and the template path publishes only batch histories
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), vapply(1:100, sqrt, numeric(1)))
  expect_identical(
    mizu_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1L],
    0
  )
  pool_end(p)
})

test_that("the idle sweep evicts the context cache; runners re-attach", {
  p <- pool_pair()
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:4,
    function(i) i * 10L,
    list(),
    chunks = 2
  )
  # drive the two runners by hand with an empty step between them: the
  # empty return runs the full idle sweep, which drops prot[5]
  h1 <- .Call(
    mizu:::mizu_pool_submit,
    p[["ctrl"]],
    mizu:::runner_payload(st, 0L),
    Inf,
    1L
  )
  expect_identical(pool_step(p), 1L)
  expect_identical(
    ls(.Call(mizu:::mizu_pool_map_cache, p[["wk"]])),
    st[["name"]]
  )
  expect_identical(pool_step(p), 0L) # empty: sweep clears the cache
  expect_length(ls(.Call(mizu:::mizu_pool_map_cache, p[["wk"]])), 0L)
  h2 <- .Call(
    mizu:::mizu_pool_submit,
    p[["ctrl"]],
    mizu:::runner_payload(st, 1L),
    Inf,
    1L
  )
  expect_identical(pool_step(p), 1L) # re-attaches the same region
  expect_identical(
    ls(.Call(mizu:::mizu_pool_map_cache, p[["wk"]])),
    st[["name"]]
  )
  # runner 0 drained the whole cursor; runner 1 came up empty-handed
  r1 <- .Call(mizu:::mizu_pool_collect, h1, 5)
  expect_identical(r1[[3L]], list(list(10L, 20L), list(30L, 40L)))
  r2 <- .Call(mizu:::mizu_pool_collect, h2, 5)
  expect_length(r2[[1L]], 0L)
  pool_end(p)
})

test_that("two submitters' maps hold two contexts on one worker", {
  p <- pool_pair(max_submitters = 4L, result_slots = 32L)
  s1 <- .Call(
    mizu:::mizu_pool_attach_call,
    .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  )
  st1 <- mizu:::map_stage(
    p[["ctrl"]],
    1:4,
    function(i) i + 1L,
    list(),
    chunks = 2
  )
  st2 <- mizu:::map_stage(s1, 1:4, function(i) i + 100L, list(), chunks = 2)
  mizu:::map_submit(p[["ctrl"]], st1)
  mizu:::map_submit(s1, st2)
  # first step: map 1's runner runs, and its doorbell help beat re-homes
  # map 2's runner onto this worker's own deque (a join ticket is never
  # executed nested); the second step pops it — both contexts resident,
  # with no empty step (and so no idle sweep) in between
  expect_identical(pool_step(p), 1L)
  expect_identical(pool_step(p), 1L)
  expect_setequal(
    ls(.Call(mizu:::mizu_pool_map_cache, p[["wk"]])),
    c(st1[["name"]], st2[["name"]])
  )
  expect_identical(collect30(p[["ctrl"]], st1), as.list(2:5))
  expect_identical(collect30(s1, st2), as.list(101:104))
  pool_end(p)
})

test_that(".timeout expiring mid-submit cancels and returns the sentinel", {
  p <- pool_pair(workers = 2L, injection_cap = 2L, slot_size = 256L)
  filler <- mizu_submit(p[["ctrl"]], "filler") # ring holds 2: one slot left
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, identity, list(), chunks = 2)
  expect_identical(st[["R"]], 2L) # runner 2 will block on the full ring
  t0 <- mizu:::mono_time()
  mizu:::map_submit(p[["ctrl"]], st, deadline = t0 + 0.2)
  expect_true(st[["timed_out"]])
  expect_true(all(vapply(st[["handles"]], is.null, NA))) # references dropped
  d <- mizu_pool_dump(p[["ctrl"]])
  expect_identical(sort(d[["tasks"]][["status"]]), c("cancel", "pending"))
  # and mizu_map itself surfaces this as the sentinel
  r <- mizu_map(p[["ctrl"]], 1:4, identity, .chunks = 2, .timeout = 0.2)
  expect_s3_class(r, "mizu_timeout")
  expect_s3_class(r, "mizu_sentinel")
  while (pool_step(p) == 1L) {
    NULL
  } # filler runs, dropped chunks free
  expect_identical(mizu_collect(filler, timeout = 5), "filler")
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that(".timeout expiring in collect cancels the outstanding chunks", {
  p <- pool_pair()
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, identity, list(), chunks = 2)
  mizu:::map_submit(p[["ctrl"]], st)
  r <- mizu:::map_collect(st, deadline = mizu:::mono_time() + 0.05)
  expect_s3_class(r, "mizu_timeout")
  expect_true(all(
    mizu_pool_dump(p[["ctrl"]])[["tasks"]][["status"]] == "cancel"
  ))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("the jump kernel agrees with set.seed and parallel", {
  base <- .Call(mizu:::mizu_map_rng_base, 42L)
  old_seed <- get0(".Random.seed", envir = globalenv(), inherits = FALSE)
  old_kind <- RNGkind()
  set.seed(42, kind = "L'Ecuyer-CMRG")
  expect_identical(.Random.seed[2:7], base)
  RNGkind(old_kind[1L], old_kind[2L], old_kind[3L])
  if (!is.null(old_seed)) {
    assign(".Random.seed", old_seed, envir = globalenv())
  }
  # one seek jump == parallel::nextRNGStream; k jumps == k applications
  s <- c(10407L, base)
  for (k in 1:5) {
    s <- parallel::nextRNGStream(s)
    expect_identical(.Call(mizu:::mizu_map_rng_seek, base, k), s[2:7])
  }
})

test_that(".seed gives identical results for any chunking", {
  p <- pool_pair()
  f <- function(i) rnorm(2)
  r1 <- run_map(p, 1:6, f, chunks = 1, seed = 42L)
  r2 <- run_map(p, 1:6, f, chunks = 3, seed = 42L)
  r3 <- run_map(p, 1:6, f, chunks = 6, seed = 42L)
  expect_identical(r1, r2)
  expect_identical(r1, r3)
  # the template path draws the same streams
  r4 <- run_map(p, 1:6, f, template = numeric(2), chunks = 2, seed = 42L)
  expect_identical(unname(r4[, 4L]), r1[[4L]])
  # a different seed diverges
  expect_false(identical(run_map(p, 1:6, f, chunks = 3, seed = 43L), r1))
  pool_end(p)
})

test_that("a vector .seed shifts the per-element streams", {
  p <- pool_pair()
  f <- function(i) rnorm(2)
  full <- run_map(p, 1:6, f, chunks = 2, seed = 42L)
  # c(s, 0) is identical to s
  expect_identical(run_map(p, 1:6, f, chunks = 2, seed = c(42L, 0)), full)
  # two windowed runs reproduce the streams of one uninterrupted run
  expect_identical(
    c(
      run_map(p, 1:3, f, chunks = 1, seed = c(42L, 0)),
      run_map(p, 4:6, f, chunks = 1, seed = c(42L, 3))
    ),
    full
  )
  pool_end(p)
})

test_that("map_rearm accepts a vector .seed", {
  p <- pool_pair()
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, identity, list())
  mizu:::map_rearm(p[["ctrl"]], st, c(42L, 3L))
  base <- .Call(mizu:::mizu_map_rng_base, 42L)
  expect_identical(st[["seed_state"]], .Call(mizu:::mizu_map_rng_seek, base, 3))
  pool_end(p)
})

test_that("a vector .seed is validated", {
  p <- pool_pair()
  expect_snapshot(error = TRUE, run_map(p, 1:4, identity, seed = c(1, 2, 3)))
  expect_snapshot(error = TRUE, run_map(p, 1:4, identity, seed = c(42, -1)))
  expect_snapshot(error = TRUE, run_map(p, 1:4, identity, seed = c(42, 0.5)))
  expect_snapshot(error = TRUE, run_map(p, 1:4, identity, seed = "x"))
  # past the 2^53 ceiling the error comes from the C seek
  expect_snapshot(
    error = TRUE,
    run_map(p, 1:4, identity, seed = c(42, 2^53 + 2))
  )
  pool_end(p)
})

test_that("seeded chunks restore the evaluating process's RNG state", {
  p <- pool_pair()
  # the harness worker is this process: a chunk's .Random.seed install must
  # not leak. With a pre-existing state, it is restored exactly ...
  set.seed(1)
  before <- .Random.seed
  invisible(run_map(p, 1:4, function(i) runif(1), chunks = 2, seed = 9L))
  expect_identical(.Random.seed, before)
  # ... and a worker with no .Random.seed yet (workers seed lazily) is
  # left with none
  rm(".Random.seed", envir = globalenv())
  invisible(run_map(p, 1:4, function(i) runif(1), chunks = 2, seed = 9L))
  expect_false(exists(".Random.seed", envir = globalenv(), inherits = FALSE))
  pool_end(p)
})

test_that("an unseeded map pays no RNG plumbing", {
  p <- pool_pair()
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, identity, list())
  expect_null(st[["seed_state"]])
  pool_end(p)
})

test_that("a nested map runs on the worker's own deque, help-collected", {
  p <- pool_pair()
  t <- mizu_submit(
    p[["ctrl"]],
    mizu_map(mizu_current_pool(), 1:6, function(i) i + 1L, .chunks = 3L)
  )
  expect_identical(pool_step(p), 1L) # one step: chunks push + help-collect
  expect_identical(mizu_collect(t, timeout = 5), as.list(2:7))
  # the worker claimed a submitter slot on first nested use
  expect_identical(
    sum(mizu_pool_status(p[["ctrl"]])[["submitters"]] == "live"),
    2L
  )
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("large generic chunk results spill and materialize intact", {
  p <- pool_pair() # 216-byte result budget: these results must spill
  r <- run_map(p, 1:4, function(i) rep(i * 1.0, 500), chunks = 2)
  expect_identical(r, lapply(1:4, function(i) rep(i * 1.0, 500)))
  expect_gte(mizu_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1L], 1)
  pool_end(p)
})

test_that("a deadline already expired at submit stages no tasks", {
  p <- pool_pair()
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, identity, list())
  mizu:::map_submit(p[["ctrl"]], st, deadline = mizu:::mono_time() - 1)
  expect_true(st[["timed_out"]])
  expect_true(all(vapply(st[["handles"]], is.null, NA)))
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("blob-path collect times out, cancels, and re-signals f's errors", {
  p <- pool_pair()
  f <- function(i) stop("blob boom") # a conditional f overflows the budget
  environment(f) <- globalenv()
  # a deadline expiring before the loop enters cancels everything
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, f, list(), chunks = 2)
  expect_type(st[["blob"]], "raw")
  mizu:::map_submit(p[["ctrl"]], st)
  r <- mizu:::map_collect(st, deadline = mizu:::mono_time() - 1)
  expect_s3_class(r, "mizu_timeout")
  while (pool_step(p) == 1L) {
    NULL
  } # dropped chunks free their slots
  # one expiring while parked on an unpublished chunk returns it too
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, f, list(), chunks = 2)
  mizu:::map_submit(p[["ctrl"]], st)
  r <- mizu:::map_collect(st, deadline = mizu:::mono_time() + 0.05)
  expect_s3_class(r, "mizu_timeout")
  while (pool_step(p) == 1L) {
    NULL
  }
  # f's error re-signals from the blob path with its element index
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, f, list(), chunks = 2)
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  e <- tryCatch(collect30(p[["ctrl"]], st), error = identity)
  expect_identical(conditionMessage(e), "blob boom")
  expect_identical(e[["mizu_map_index"]], 1)
  gc() # chunk 2's discarded ERR result frees with its dropped handle
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("a hostile condition from f flattens, keeping class and index", {
  p <- pool_pair()
  f <- function(i) {
    stop(structure(
      list(message = paste("boom", i), call = NULL, payload = new.env()),
      class = c("mizu_test_map_error", "error", "condition")
    ))
  }
  environment(f) <- globalenv()
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, f, list(), chunks = 2)
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  e <- tryCatch(collect30(p[["ctrl"]], st), error = identity)
  expect_s3_class(e, "mizu_test_map_error")
  expect_identical(conditionMessage(e), "boom 1")
  expect_identical(e[["mizu_map_index"]], 1)
  expect_identical(e[["dropped_fields"]], "payload")
  pool_end(p)
})

test_that("seeded blob-path chunks draw the same per-element streams", {
  p <- pool_pair()
  f <- function(i) rnorm(2)
  environment(f) <- globalenv()
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, f, list(), chunks = 2, seed = 42L)
  expect_type(st[["blob"]], "raw")
  set.seed(1)
  before <- .Random.seed
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  r <- collect30(p[["ctrl"]], st)
  # the evaluating process's RNG state is restored exactly ...
  expect_identical(.Random.seed, before)
  # ... and the draws match the region (template) path's for the same seed
  rt <- run_map(p, 1:4, f, template = numeric(2), chunks = 4, seed = 42L)
  for (j in 1:4) {
    expect_identical(unname(rt[, j]), r[[j]])
  }
  # a worker with no .Random.seed yet is left with none, results unchanged
  rm(".Random.seed", envir = globalenv())
  st <- mizu:::map_stage(p[["ctrl"]], 1:4, f, list(), chunks = 2, seed = 42L)
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), r)
  expect_false(exists(".Random.seed", envir = globalenv(), inherits = FALSE))
  pool_end(p)
})

test_that("a ninth resident map context clears the worker cache whole", {
  p <- pool_pair(slot_size = 256L) # region path: the blob path has no cache
  # nine maps with no empty step in between: no idle sweep runs, so the
  # ninth miss finds eight resident contexts and drops them all first
  for (k in 1:9) {
    st <- mizu:::map_stage(p[["ctrl"]], 1:2, identity, list())
    mizu:::map_submit(p[["ctrl"]], st)
    expect_identical(pool_step(p), 1L)
    expect_identical(collect30(p[["ctrl"]], st), list(1L, 2L))
  }
  expect_identical(
    ls(.Call(mizu:::mizu_pool_map_cache, p[["wk"]])),
    st[["name"]]
  )
  pool_end(p)
})

test_that("a runner failure outside f is fatal to the collect", {
  p <- pool_pair(slot_size = 256L) # region path: st[["name"]] must exist
  st <- mizu:::map_stage(p[["ctrl"]], 1:8, identity, list())
  rw <- .Call(mizu:::mizu_region_open, st[["name"]], TRUE)
  .Call(mizu:::mizu_poke, rw, 0, as.raw(0)) # corrupt the region magic
  mizu:::map_submit(p[["ctrl"]], st)
  pool_step(p) # map_ctx's attach fails: no element index
  e <- tryCatch(collect30(p[["ctrl"]], st), error = identity)
  expect_match(conditionMessage(e), "invalid map region")
  expect_null(e[["mizu_map_index"]])
  while (pool_step(p) == 1L) {
    NULL
  }
  pool_end(p)
})

test_that("RNG stream arguments are validated; an empty lost set labels", {
  base <- .Call(mizu:::mizu_map_rng_base, 1L)
  expect_error(
    .Call(mizu:::mizu_map_rng_seek, 1:5, 1),
    "invalid RNG stream state"
  )
  expect_error(
    .Call(mizu:::mizu_map_rng_install, 1:5),
    "invalid RNG stream state"
  )
  expect_error(
    .Call(mizu:::mizu_map_rng_seek, base, -1),
    "invalid stream index"
  )
  expect_identical(
    mizu:::map_ranges_label(cbind(lo = numeric(0), hi = numeric(0))),
    "(none)"
  )
})

test_that("mizu_map validates its arguments", {
  p <- pool_pair()
  expect_error(
    mizu_map(p[["ctrl"]], 1:4, identity, .template = list()),
    ".template must be"
  )
  expect_error(
    mizu_map(p[["ctrl"]], 1:4, identity, .template = integer()),
    ".template must be"
  )
  expect_error(
    mizu_map(p[["ctrl"]], 1:4, identity, .chunks = NA),
    ".chunks must be"
  )
  expect_error(
    mizu_map(p[["ctrl"]], 1:4, identity, .seed = "x"),
    ".seed must be"
  )
  expect_error(mizu_map(p[["ctrl"]], 1:4, "no such function"), "not found")
  expect_error(mizu_map("not a pool", 1:4, identity), "not a pool handle")
  pool_end(p)
})

test_that("an attributed x leaves the raw section and rides the descriptor", {
  p <- pool_pair()
  st <- mizu:::map_stage(
    p[["ctrl"]],
    structure(1:6, foo = "bar"),
    function(i) i * 2L,
    list(),
    chunks = 2
  )
  expect_false(st[["xraw"]])
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), as.list(1:6 * 2L))
  pool_end(p)
})

test_that("a template type mismatch names the template's type", {
  p <- pool_pair()
  expect_error(
    run_map(p, 1:4, function(i) 1.5, template = logical(1)),
    "must be type 'logical'"
  )
  expect_error(
    run_map(p, 1:4, function(i) 1.5, template = integer(1)),
    "must be type 'integer'"
  )
  expect_error(
    run_map(p, 1:4, function(i) "x", template = numeric(1)),
    "must be type 'double'"
  )
  pool_end(p)
})

test_that("a name-kind spec map takes the region path and returns in order", {
  p <- pool_pair()
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:10,
    mizu_call("base::sqrt"),
    list(),
    chunks = 2
  )
  expect_true(st[["spec"]])
  expect_false(st[["pymap"]])
  # a spec f always takes the region path: the inline chunk tasks are
  # same-language private frames
  expect_null(st[["blob"]])
  # 1:10 is ALTREP: x rides the descriptor as the element list
  expect_false(st[["xraw"]])
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), as.list(sqrt(1:10)))
  pool_end(p)
})

test_that("a raw x section rides a spec map as bare bytes", {
  p <- pool_pair()
  x <- 1:8 + 0L
  st <- mizu:::map_stage(
    p[["ctrl"]],
    x,
    mizu_call("base::log"),
    list(),
    chunks = 2
  )
  expect_true(st[["xraw"]])
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), as.list(log(x)))
  pool_end(p)
})

test_that("spec constant arguments ride the spec, named and positional", {
  p <- pool_pair()
  expect_identical(
    run_map(p, 1:5, mizu_call("base::round", digits = 1L)),
    as.list(round(1:5, digits = 1L))
  )
  expect_identical(
    run_map(p, 1:5, mizu_call(.name = NULL, 10L, .source = "x * ..1")),
    as.list(1:5 * 10L)
  )
  expect_identical(
    run_map(p, as.list(1:5), mizu_call(.source = "x + k", k = 5L)),
    as.list(1:5 + 5L)
  )
  pool_end(p)
})

test_that("a spec f rejects constant arguments in ...", {
  p <- pool_pair()
  expect_error(
    mizu_map(p[["ctrl"]], 1:4, mizu_call("base::sqrt"), digits = 1L),
    "must \\s*be empty with a spec",
    perl = TRUE
  )
  expect_error(
    mizu_map_prepare(p[["ctrl"]], 1:4, mizu_call("base::sqrt"), 1L),
    "be empty with a spec"
  )
  pool_end(p)
})

test_that("a spec f held in a variable bypasses match.fun", {
  p <- pool_pair()
  f <- mizu_call("base::sqrt")
  expect_identical(run_map(p, 1:4, f), as.list(sqrt(1:4)))
  pool_end(p)
})

test_that("a native f on a foreign pool fails fast, a spec passes the guard", {
  p <- pool_pair(ident = c(3L, 7L))
  expect_error(
    mizu_map(p[["ctrl"]], 1:4, identity),
    "workers are not R"
  )
  expect_error(
    mizu_map(p[["ctrl"]], 1:4, mizu_call("numpy.mean"), extra = 1),
    "be empty with a spec"
  )
  pool_end(p)
})

test_that("a spec map needs a joined worker", {
  ctrl <- .Call(
    mizu:::mizu_pool_create,
    1L,
    8L,
    64L,
    64L,
    64L,
    512L
  )
  expect_error(
    mizu_map(ctrl, 1:4, mizu_call("base::sqrt")),
    "no worker has joined"
  )
  .Call(mizu:::mizu_pool_destroy, ctrl)
})

test_that("a spec map stages the template path and gathers a view", {
  p <- pool_pair()
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:6,
    mizu_call("base::log"),
    list(),
    template = numeric(1),
    chunks = 2
  )
  expect_true(st[["direct"]])
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(collect30(p[["ctrl"]], st), log(1:6))
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:6,
    mizu_call("base::log"),
    list(),
    template = numeric(1)
  )
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  v <- mizu:::map_collect(st, collect = "view")
  expect_equal(as.numeric(v), log(1:6))
  pool_end(p)
})

test_that("a spec map's seed matches the native derivation, chunk-invariant", {
  p <- pool_pair()
  f <- mizu_call(.source = "runif(1)")
  a <- run_map(p, 1:20, f, seed = 42)
  b <- run_map(p, 1:20, f, seed = 42, chunks = 7)
  expect_identical(a, b)
  d <- run_map(p, 1:20, f, seed = c(42, 20))
  e <- run_map(p, 1:20, function(x) runif(1), seed = c(42, 20))
  expect_identical(unlist(d), unlist(e))
  expect_error(
    mizu_map(p[["ctrl"]], 1:4, f, .seed = 2^31),
    ".seed\\[1\\] must be an integer"
  )
  pool_end(p)
})

test_that("a prepared spec map re-runs under fresh seeds without restaging", {
  p <- pool_pair()
  pm <- mizu_map_prepare(p[["ctrl"]], 1:12, mizu_call(.source = "runif(1)"))
  st <- pm[["st"]]
  name1 <- st[["name"]]
  mizu:::map_rearm(p[["ctrl"]], st, 1)
  mizu:::map_submit(p[["ctrl"]], st)
  expect_identical(pool_step(p), 1L)
  r1 <- collect30(p[["ctrl"]], st)
  # run 2: the same region and name (no restage), a fresh seed on the
  # re-armed runner fields
  mizu:::map_rearm(p[["ctrl"]], st, 2)
  expect_identical(st[["name"]], name1)
  mizu:::map_submit(p[["ctrl"]], st)
  expect_identical(pool_step(p), 1L)
  r2 <- collect30(p[["ctrl"]], st)
  expect_false(identical(unlist(r1), unlist(r2)))
  mizu:::map_rearm(p[["ctrl"]], st, 1)
  mizu:::map_submit(p[["ctrl"]], st)
  expect_identical(pool_step(p), 1L)
  expect_identical(collect30(p[["ctrl"]], st), r1)
  pool_end(p)
})

test_that("a spec map error re-signals with the failing element's index", {
  p <- pool_pair()
  st <- mizu:::map_stage(
    p[["ctrl"]],
    1:10,
    mizu_call(.source = "if (x == 7) stop(\"boom\") else x"),
    list(),
    chunks = 2
  )
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  e <- tryCatch(collect30(p[["ctrl"]], st), error = identity)
  expect_identical(e[["mizu_map_index"]], 7)
  expect_match(conditionMessage(e), "boom")
  pool_end(p)
})

test_that("the pymap adapter splices element ranges and converts histories", {
  # the foreign runner shape: (hist, vals) with 0-based half-open element
  # ranges, as pymizu's _runner publishes them
  runs <- list(
    list(
      list(list(0L, 3L), list(3L, 6L)),
      list(as.list(1:3), as.list(4:6))
    ),
    list(list(list(6L, 8L)), list(as.list(7:8)))
  )
  out <- vector("list", 8L)
  out <- mizu:::map_splice_ix(out, runs)
  expect_identical(out, as.list(1:8))
  # template path publishes no values
  expect_identical(
    mizu:::map_splice_ix(vector("list", 2L), list(list(list(), NULL))),
    vector("list", 2L)
  )
  # histories convert back to morsel pairs (morsel size 3, partial grant)
  hist <- mizu:::map_hist_ix(runs, 3)
  expect_identical(hist[[1L]], list(c(0, 1), c(1, 1)))
  expect_identical(hist[[2L]], list(2, 1))
})

test_that("a spec map declines a non-portable constant at stage time", {
  p <- pool_pair()
  expect_error(
    mizu_map(p[["ctrl"]], 1:4, mizu_call("base::identity", new.env())),
    class = "mizu_error_not_portable"
  )
  pool_end(p)
})
