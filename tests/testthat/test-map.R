# kio_map over the in-process harness. The map stages are composable
# precisely for this file: pool_pair() is single-process, so a monolithic
# kio_map would block in collect with nobody to step the worker — tests
# drive map_stage / map_submit / map_collect and interleave pool_step()
# between submit and collect. Cross-process maps (spawned workers, worker
# death, stealing) are test-map-process.R.

# collect under a file-wide 30s guard: a chunk missed by the stepping
# above fails the test loudly instead of hanging CI on an infinite wait
collect30 <- function(pool, st)
  kioto:::map_collect(pool, st, deadline = kioto:::mono_time() + 30)

run_map <- function(p, x, f, dots = list(), ..., steps = 256L) {
  st <- kioto:::map_stage(p$ctrl, x, f, dots, ...)
  kioto:::map_submit(p$ctrl, st)
  for (i in seq_len(steps)) if (pool_step(p) != 1L) break
  collect30(p$ctrl, st)
}

test_that("a map returns results in input order with names reapplied", {
  p <- pool_pair()
  x <- setNames(1:10, letters[1:10])
  st <- kioto:::map_stage(p$ctrl, x, function(i) i * 2L, list(), chunks = 3)
  # a named atomic x stays on the RAWVEC path: names never cross the wire
  expect_true(st$xraw)
  expect_identical(st$C, 3L)
  # the chunk ranges partition [1, n]
  expect_identical(st$lo[[1L]], 1)
  expect_identical(st$hi[[st$C]], 10)
  expect_identical(st$lo[-1L], st$hi[-st$C] + 1)
  kioto:::map_submit(p$ctrl, st)
  expect_identical(kio_pool_status(p$ctrl)$injection, 3)
  while (pool_step(p) == 1L) NULL
  expect_identical(collect30(p$ctrl, st),
                   as.list(setNames(1:10 * 2L, letters[1:10])))
  pool_end(p)
})

test_that("every atomic x type maps, RAWVEC or descriptor as gated", {
  p <- pool_pair()
  for (x in list(c(TRUE, FALSE, NA), c(1L, 5L, 3L), c(1.5, 2.5), c(1i, 2i),
                 as.raw(1:4))) {
    st <- kioto:::map_stage(p$ctrl, x, identity, list())
    expect_true(st$xraw)
    kioto:::map_submit(p$ctrl, st)
    while (pool_step(p) == 1L) NULL
    expect_identical(collect30(p$ctrl, st), lapply(x, identity))
  }
  # character, list, and classed vectors ride the descriptor
  for (x in list(letters[1:5], list(1:3, "a"), factor(c("a", "b")),
                 as.Date("2026-07-24") + 0:2)) {
    st <- kioto:::map_stage(p$ctrl, x, identity, list())
    expect_false(st$xraw)
    kioto:::map_submit(p$ctrl, st)
    while (pool_step(p) == 1L) NULL
    expect_identical(collect30(p$ctrl, st), lapply(x, identity))
  }
  # an extra attribute (beyond names) also disqualifies bare-byte slicing,
  # as does ALTREP: a compact 1:n serializes as its compact form — smaller
  # than its bytes — and a mori-shared x must reduce to its identifier
  x <- structure(c(1L, 2L), extra = "attr")
  expect_false(kioto:::map_stage(p$ctrl, x, identity, list())$xraw)
  expect_false(kioto:::map_stage(p$ctrl, 1:5, identity, list())$xraw)
  pool_end(p)
})

test_that("constant dots stage once and reach every element", {
  p <- pool_pair()
  r <- run_map(p, list(1:3, 4:6), function(v, w) sum(v) + w,
               list(w = 100L))
  expect_identical(r, list(106L, 115L))
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
  expect_identical(run_map(p, 1:8, function(i) i * 2L, template = integer(1)),
                   vapply(1:8, function(i) i * 2L, integer(1)))
  # upward widening: integer (and logical) results into a double template
  expect_identical(run_map(p, 1:8, function(i) i, template = numeric(1)),
                   vapply(1:8, function(i) i, numeric(1)))
  expect_identical(run_map(p, 1:4, function(i) i > 2L, template = numeric(1)),
                   vapply(1:4, function(i) i > 2L, numeric(1)))
  # NA semantics ride R's own coercion
  expect_identical(run_map(p, 1:3, function(i) c(NA, i)[1L],
                           template = numeric(1)),
                   vapply(1:3, function(i) c(NA, i)[1L], numeric(1)))
  # m > 1 gathers a matrix, template names as rownames, names(x) as colnames
  f <- function(i) c(a = i * 1, b = i * 2)
  x <- setNames(1:5, letters[1:5])
  expect_identical(run_map(p, x, f, template = c(lo = 0, hi = 0)),
                   vapply(x, f, c(lo = 0, hi = 0)))
  pool_end(p)
})

test_that("character templates assemble through the generic path", {
  p <- pool_pair()
  st <- kioto:::map_stage(p$ctrl, 1:4, function(i) letters[i], list(),
                          template = character(1))
  expect_false(st$direct)   # no output area: chunks return lists
  kioto:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) NULL
  expect_identical(collect30(p$ctrl, st),
                   vapply(1:4, function(i) letters[i], character(1)))
  pool_end(p)
})

test_that("template type and length checks fail per element, with index", {
  p <- pool_pair()
  # wrong length
  st <- kioto:::map_stage(p$ctrl, 1:6, function(i) rep(i, i), list(),
                          template = integer(1), chunks = 2)
  kioto:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) NULL
  e <- tryCatch(collect30(p$ctrl, st), error = identity)
  expect_match(conditionMessage(e), "type 'integer' and length 1")
  expect_identical(e$kio_map_index, 2)   # first element whose length isn't 1
  # downward type (double into integer template) is refused, as vapply
  st <- kioto:::map_stage(p$ctrl, 1:4, function(i) i * 1.5, list(),
                          template = integer(1))
  kioto:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) NULL
  e <- tryCatch(collect30(p$ctrl, st), error = identity)
  expect_match(conditionMessage(e), "type 'integer'")
  pool_end(p)
})

test_that("an error in f re-signals with the failing element's index", {
  p <- pool_pair()
  st <- kioto:::map_stage(p$ctrl, 1:10,
                          function(i) if (i == 7L) stop("boom ", i) else i,
                          list(), chunks = 2)
  kioto:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) NULL
  e <- tryCatch(collect30(p$ctrl, st), error = identity)
  expect_s3_class(e, "simpleError")
  expect_identical(conditionMessage(e), "boom 7")
  expect_identical(e$kio_map_index, 7)
  pool_end(p)
})

test_that("classed conditions from f survive the crossing, index attached", {
  p <- pool_pair()
  cond <- structure(list(message = "typed", call = NULL),
                    class = c("kio_test_error", "error", "condition"))
  st <- kioto:::map_stage(p$ctrl, 1:4,
                          function(i, cond) if (i == 3L) stop(cond) else i,
                          list(cond = cond), chunks = 2)
  kioto:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) NULL
  e <- tryCatch(collect30(p$ctrl, st), error = identity)
  expect_s3_class(e, "kio_test_error")
  expect_identical(e$kio_map_index, 3)
  pool_end(p)
})

test_that("an observed chunk error cancels the uncollected chunks", {
  p <- pool_pair()
  st <- kioto:::map_stage(p$ctrl, 1:9, function(i) if (i == 1L) stop("now"),
                          list(), chunks = 3)
  kioto:::map_submit(p$ctrl, st)
  pool_step(p)   # only chunk 1 runs — and errors
  expect_error(collect30(p$ctrl, st), "now")
  d <- kio_pool_dump(p$ctrl)
  expect_identical(sort(d$tasks$status), c("cancel", "cancel"))
  while (pool_step(p) == 1L) NULL   # cancelled chunks drop at their pop
  expect_identical(unname(kio_pool_status(p$ctrl)$tasks), rep(0L, 5L))
  pool_end(p)
})

test_that("n = 0, n = 1, and n < chunks all behave", {
  p <- pool_pair()
  # empty maps return immediately: no region, no tasks
  expect_identical(kio_map(p$ctrl, list(), sqrt), list())
  expect_identical(kio_map(p$ctrl, setNames(integer(), character()), sqrt),
                   setNames(list(), character()))
  expect_identical(kio_map(p$ctrl, character(), identity,
                           .template = character(1)), character())
  expect_identical(kio_map(p$ctrl, integer(), identity,
                           .template = c(a = 0L, b = 0L)),
                   vapply(integer(), identity, c(a = 0L, b = 0L)))
  expect_identical(unname(kio_pool_status(p$ctrl)$tasks), rep(0L, 5L))
  expect_identical(run_map(p, 5, function(i) i * 2), list(10))
  # more chunks requested than elements: C clamps to n
  st <- kioto:::map_stage(p$ctrl, 1:3, identity, list(), chunks = 8)
  expect_identical(st$C, 3L)
  kioto:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) NULL
  expect_identical(collect30(p$ctrl, st), as.list(1:3))
  pool_end(p)
})

test_that("the chunk count clamps to the free result-slot subrange", {
  p <- pool_pair()   # 64 result slots / 8 submitters = 8 per subrange
  st <- kioto:::map_stage(p$ctrl, 1:100, identity, list())
  expect_identical(st$C, 8L)   # min(n, 8 * 1 live worker, 8 free, 64)
  # occupy five slots: both the default and an explicit .chunks clamp to 3
  held <- lapply(1:5, function(i) kio_submit(p$ctrl, v, v = i))
  expect_identical(kioto:::map_stage(p$ctrl, 1:100, identity, list())$C, 3L)
  st <- kioto:::map_stage(p$ctrl, 1:100, identity, list(), chunks = 8)
  expect_identical(st$C, 3L)
  kioto:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) NULL
  expect_identical(collect30(p$ctrl, st), as.list(1:100))
  for (i in 1:5) expect_identical(kio_collect(held[[i]], timeout = 5), i)
  pool_end(p)
})

test_that("a fully occupied subrange errors before any region exists", {
  p <- pool_pair()
  held <- lapply(1:8, function(i) kio_submit(p$ctrl, v, v = i))
  expect_error(kio_map(p$ctrl, 1:10, identity),
               "result slots exhausted")
  while (pool_step(p) == 1L) NULL
  for (i in 1:8) expect_identical(kio_collect(held[[i]], timeout = 5), i)
  pool_end(p)
})

test_that("zero live workers floors C at 1; a rejoined worker drains it", {
  p <- pool_pair()
  suffix <- .Call(kioto:::kio_pool_suffix, p$ctrl)
  .Call(kioto:::kio_pool_leave, p$wk)
  expect_identical(kio_pool_status(p$ctrl)$workers, "free")
  st <- kioto:::map_stage(p$ctrl, 1:6, function(i) i + 1L, list())
  expect_identical(st$C, 1L)
  kioto:::map_submit(p$ctrl, st)   # queues in the injection ring
  wk <- .Call(kioto:::kio_pool_worker_join, suffix, 0L)
  .Call(kioto:::kio_pool_set_eval, wk)
  while (pool_step(p, wk = wk) == 1L) NULL
  expect_identical(collect30(p$ctrl, st), as.list(2:7))
  .Call(kioto:::kio_pool_leave, wk)
  .Call(kioto:::kio_pool_destroy, p$ctrl)
})

test_that("a small map rides entirely inline, no region", {
  p <- pool_pair(slot_size = 512L)   # the default 480-byte entry budget
  f <- function(i) i + 1L
  environment(f) <- globalenv()
  st <- kioto:::map_stage(p$ctrl, 1:4, f, list(), chunks = 2)
  expect_null(st$name)
  expect_type(st$blob, "raw")
  kioto:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) NULL
  expect_identical(collect30(p$ctrl, st), as.list(2:5))
  pool_end(p)
})

test_that("the region chunk wrapper fits a slot_size = 256 entry budget", {
  p <- pool_pair()   # slot_size 256: 224-byte entry inline budget
  st <- kioto:::map_stage(p$ctrl, 1:100, sqrt, list(),
                          template = numeric(1))
  sz <- .Call(kioto:::kio_bounded_call,
              kioto:::map_payload(st, c(1, 100, 1)), 224L)[[1L]]
  expect_lte(sz, 224)
  # and indeed nothing spilled over a whole template map: chunk wrappers
  # stayed inline, and the template path publishes NULL chunk results
  kioto:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) NULL
  expect_identical(collect30(p$ctrl, st),
                   vapply(1:100, sqrt, numeric(1)))
  expect_identical(kio_pool_stats(p$ctrl)$submitters$spills[1L], 0)
  pool_end(p)
})

test_that("the idle sweep evicts the context cache; chunks re-attach", {
  p <- pool_pair()
  st <- kioto:::map_stage(p$ctrl, 1:4, function(i) i * 10L, list(),
                          chunks = 2)
  # drive the two chunks by hand with an empty step between them: the
  # empty return runs the full idle sweep, which drops prot[5]
  h1 <- .Call(kioto:::kio_pool_submit, p$ctrl,
              kioto:::map_payload(st, c(st$lo[[1L]], st$hi[[1L]])), Inf)
  expect_identical(pool_step(p), 1L)
  expect_identical(ls(.Call(kioto:::kio_pool_map_cache, p$wk)), st$name)
  expect_identical(pool_step(p), 0L)   # empty: sweep clears the cache
  expect_length(ls(.Call(kioto:::kio_pool_map_cache, p$wk)), 0L)
  h2 <- .Call(kioto:::kio_pool_submit, p$ctrl,
              kioto:::map_payload(st, c(st$lo[[2L]], st$hi[[2L]])), Inf)
  expect_identical(pool_step(p), 1L)   # re-attaches the same region
  expect_identical(ls(.Call(kioto:::kio_pool_map_cache, p$wk)), st$name)
  expect_identical(.Call(kioto:::kio_pool_collect, h1, 5), list(10L, 20L))
  expect_identical(.Call(kioto:::kio_pool_collect, h2, 5), list(30L, 40L))
  pool_end(p)
})

test_that("two submitters' maps hold two contexts on one worker", {
  p <- pool_pair(max_submitters = 4L, result_slots = 32L)
  s1 <- .Call(kioto:::kio_pool_attach_call,
              .Call(kioto:::kio_pool_suffix, p$ctrl))
  st1 <- kioto:::map_stage(p$ctrl, 1:4, function(i) i + 1L, list(),
                           chunks = 2)
  st2 <- kioto:::map_stage(s1, 1:4, function(i) i + 100L, list(), chunks = 2)
  kioto:::map_submit(p$ctrl, st1)
  kioto:::map_submit(s1, st2)
  # exactly four steps: an extra empty step would sweep the cache away
  for (i in 1:4) expect_identical(pool_step(p), 1L)
  expect_setequal(ls(.Call(kioto:::kio_pool_map_cache, p$wk)),
                  c(st1$name, st2$name))
  expect_identical(collect30(p$ctrl, st1), as.list(2:5))
  expect_identical(collect30(s1, st2), as.list(101:104))
  pool_end(p)
})

test_that(".timeout expiring mid-submit cancels and returns the sentinel", {
  p <- pool_pair(injection_cap = 2L)
  filler <- kio_submit(p$ctrl, "filler")   # ring holds 2: one slot left
  st <- kioto:::map_stage(p$ctrl, 1:4, identity, list(), chunks = 2)
  t0 <- kioto:::mono_time()
  kioto:::map_submit(p$ctrl, st, deadline = t0 + 0.2)
  expect_true(st$timed_out)
  expect_true(all(vapply(st$handles, is.null, NA)))   # references dropped
  d <- kio_pool_dump(p$ctrl)
  expect_identical(sort(d$tasks$status), c("cancel", "pending"))
  # and kio_map itself surfaces this as the sentinel
  r <- kio_map(p$ctrl, 1:4, identity, .chunks = 2, .timeout = 0.2)
  expect_s3_class(r, "kio_timeout")
  expect_s3_class(r, "kio_condition")
  while (pool_step(p) == 1L) NULL   # filler runs, dropped chunks free
  expect_identical(kio_collect(filler, timeout = 5), "filler")
  expect_identical(unname(kio_pool_status(p$ctrl)$tasks), rep(0L, 5L))
  pool_end(p)
})

test_that(".timeout expiring in collect cancels the outstanding chunks", {
  p <- pool_pair()
  st <- kioto:::map_stage(p$ctrl, 1:4, identity, list(), chunks = 2)
  kioto:::map_submit(p$ctrl, st)
  r <- kioto:::map_collect(p$ctrl, st,
                           deadline = kioto:::mono_time() + 0.05)
  expect_s3_class(r, "kio_timeout")
  expect_true(all(kio_pool_dump(p$ctrl)$tasks$status == "cancel"))
  while (pool_step(p) == 1L) NULL
  expect_identical(unname(kio_pool_status(p$ctrl)$tasks), rep(0L, 5L))
  pool_end(p)
})

test_that("the jump kernel agrees with set.seed and parallel", {
  base <- .Call(kioto:::kio_map_rng_base, 42L)
  old_seed <- get0(".Random.seed", envir = globalenv(), inherits = FALSE)
  old_kind <- RNGkind()
  set.seed(42, kind = "L'Ecuyer-CMRG")
  expect_identical(.Random.seed[2:7], base)
  RNGkind(old_kind[1L], old_kind[2L], old_kind[3L])
  if (!is.null(old_seed)) assign(".Random.seed", old_seed,
                                 envir = globalenv())
  # one seek jump == parallel::nextRNGStream; k jumps == k applications
  s <- c(10407L, base)
  for (k in 1:5) {
    s <- parallel::nextRNGStream(s)
    expect_identical(.Call(kioto:::kio_map_rng_seek, base, k), s[2:7])
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
  expect_false(exists(".Random.seed", envir = globalenv(),
                      inherits = FALSE))
  pool_end(p)
})

test_that("an unseeded map pays no RNG plumbing", {
  p <- pool_pair()
  st <- kioto:::map_stage(p$ctrl, 1:4, identity, list())
  expect_null(st$seed_state)
  pool_end(p)
})

test_that("a nested map runs on the worker's own deque, help-collected", {
  p <- pool_pair()
  t <- kio_submit(p$ctrl, kio_map(pool, 1:6, function(i) i + 1L,
                                  .chunks = 3L))
  expect_identical(pool_step(p), 1L)   # one step: chunks push + help-collect
  expect_identical(kio_collect(t, timeout = 5), as.list(2:7))
  # the worker claimed a submitter slot on first nested use
  expect_identical(sum(kio_pool_status(p$ctrl)$submitters == "live"), 2L)
  expect_identical(unname(kio_pool_status(p$ctrl)$tasks), rep(0L, 5L))
  pool_end(p)
})

test_that("large generic chunk results spill and materialize intact", {
  p <- pool_pair()   # 216-byte result budget: these results must spill
  r <- run_map(p, 1:4, function(i) rep(i * 1.0, 500), chunks = 2)
  expect_identical(r, lapply(1:4, function(i) rep(i * 1.0, 500)))
  expect_gte(kio_pool_stats(p$ctrl)$submitters$spills[1L], 1)
  pool_end(p)
})

test_that("kio_map validates its arguments", {
  p <- pool_pair()
  expect_error(kio_map(p$ctrl, 1:4, identity, .template = list()),
               ".template must be")
  expect_error(kio_map(p$ctrl, 1:4, identity, .template = integer()),
               ".template must be")
  expect_error(kio_map(p$ctrl, 1:4, identity, .chunks = NA),
               ".chunks must be")
  expect_error(kio_map(p$ctrl, 1:4, identity, .seed = "x"),
               ".seed must be")
  expect_error(kio_map(p$ctrl, 1:4, "no such function"), "not found")
  expect_error(kio_map("not a pool", 1:4, identity), "not a pool handle")
  pool_end(p)
})
