# In-process morsel-protocol tests: sora_map_next / sora_map_abandon /
# sora_map_cancel_* / sora_map_reset are .Calls against a region staged from
# this process — the deterministic harness for issue order, exhaustion,
# the CLAIM handshake and generation fencing, no children needed. The
# region's producer handle maps writable, so this process can drive the
# shared state from both sides.

stage_h <- function(n, morsel = 1, x = NULL, template = NULL)
  .Call(sora:::sora_map_stage, list(identity, list()), x, NULL, n,
        template, morsel)[[2L]]

# pinned-k transition (the test entry bypassing the sizing policy)
nxt <- function(h, r = 0L, gen = 0, k = 1, now = NULL)
  .Call(sora:::sora_map_next, h, r, gen, NULL, k, now)

# adaptive transition under a forced clock
anxt <- function(h, now, r = 0L, gen = 0)
  .Call(sora:::sora_map_next, h, r, gen, NULL, NULL, now)

minfo <- function(h) .Call(sora:::sora_map_info, h)
claim <- function(h, r) .Call(sora:::sora_map_claim_state, h, r)
abandon <- function(h, r) .Call(sora:::sora_map_abandon, h, r)

test_that("stage lays out morsel geometry and a zeroed state section", {
  h <- stage_h(100, 8)
  i <- minfo(h)
  expect_identical(i[["n"]], 100)
  expect_identical(i[["morsel_size"]], 8)
  expect_identical(i[["n_morsels"]], 13)          # ceiling(100 / 8)
  expect_identical(i[["claim_n"]], 64L)
  expect_identical(i[["generation"]], 0)
  expect_identical(i[["cursor"]], 0)
  expect_false(i[["cancel"]])
  expect_identical(claim(h, 0L)[["state"]], "idle")
  expect_identical(claim(h, 63L)[["state"]], "idle")
  expect_error(stage_h(5, 10), "invalid map morsel size")
})

test_that("pinned-k issue walks the cursor with a final partial grant", {
  h <- stage_h(10, 1)
  b <- nxt(h, k = 3)
  expect_identical(unlist(b[1:4]), c(0, 3, 1, 3))
  b <- nxt(h, k = 4)
  expect_identical(unlist(b[1:4]), c(3, 4, 4, 7))
  b <- nxt(h, k = 64)                        # clamps to the end
  expect_identical(unlist(b[1:4]), c(7, 3, 8, 10))
  expect_null(nxt(h))                        # exhausted
  expect_identical(minfo(h)[["cursor"]], 10)      # clamped past the overshoot
})

test_that("element ranges track morsel geometry, last morsel partial", {
  h <- stage_h(10, 4)                        # morsels [1,4] [5,8] [9,10]
  b <- nxt(h, k = 2)
  expect_identical(c(b[[3L]], b[[4L]]), c(1, 8))
  b <- nxt(h, k = 2)                         # grant clamps to one morsel
  expect_identical(b[[2L]], 1)
  expect_identical(c(b[[3L]], b[[4L]]), c(9, 10))
  expect_null(nxt(h))
})

test_that("multiple runners issue disjoint batches off one cursor", {
  h <- stage_h(6, 1)
  a <- nxt(h, 0L, k = 2)
  b <- nxt(h, 1L, k = 2)
  c3 <- nxt(h, 0L, k = 2)
  got <- sort(c(a[[3L]]:a[[4L]], b[[3L]]:b[[4L]], c3[[3L]]:c3[[4L]]))
  expect_identical(got, 1:6)
  expect_null(nxt(h, 1L, k = 2))
  expect_identical(claim(h, 0L)[["state"]], "running")
  expect_identical(claim(h, 1L)[["state"]], "running")
})

test_that("a batch carries its x slice from the RAWVEC section", {
  x <- as.double(1:20) * 1.5
  h <- stage_h(20, 4, x = x)
  b <- nxt(h, k = 2)
  expect_identical(b[[5L]], x[b[[3L]]:b[[4L]]])
  b <- nxt(h, k = 64)
  expect_identical(b[[5L]], x[b[[3L]]:b[[4L]]])
  # descriptor-carried x: no slice rides the return
  h2 <- stage_h(5, 1)
  expect_null(nxt(h2)[[5L]])
})

test_that("the cancel word stops issue before any claim and arms the trim", {
  h <- stage_h(10, 1)
  nxt(h, k = 2)
  # refusal while morsels remain and cancel is clear reports the state
  expect_identical(abandon(h, 1L), "idle")
  expect_identical(claim(h, 1L)[["state"]], "idle")
  .Call(sora:::sora_map_cancel_set, h)
  expect_true(.Call(sora:::sora_map_cancel_get, h))
  cur <- minfo(h)[["cursor"]]
  # a cancelled region issues nothing more, even with morsels left
  expect_null(nxt(h, k = 2))
  expect_identical(minfo(h)[["cursor"]], cur)     # NULL landed before any issue
  # the cancel arm lets teardown trim queued never-started runners
  expect_identical(abandon(h, 1L), "abandoned")
  expect_identical(claim(h, 1L)[["state"]], "abandoned")
  # an abandoned runner's first call returns NULL without issuing
  expect_null(nxt(h, 1L, k = 2))
  expect_identical(minfo(h)[["cursor"]], cur)
})

test_that("the trim fires on exhaustion and loses to a RUNNING claim", {
  h <- stage_h(4, 1)
  b <- nxt(h, 0L, k = 4)                     # r0 drains the cursor
  expect_identical(b[[2L]], 4)
  expect_identical(abandon(h, 0L), "running")  # won CAS beats a late abandon
  expect_identical(abandon(h, 1L), "abandoned")  # never started: trimmed
  expect_null(nxt(h, 1L))
  # an exhausted runner's word stays RUNNING for collect
  expect_null(nxt(h, 0L))
  expect_identical(claim(h, 0L)[["state"]], "running")
})

test_that("reset re-arms every CLAIM word under a bumped generation", {
  h <- stage_h(6, 1)
  nxt(h, k = 6)
  abandon(h, 1L)                             # exhausted: trims
  .Call(sora:::sora_map_cancel_set, h)
  gen2 <- .Call(sora:::sora_map_reset, h)
  expect_identical(gen2, 1)
  i <- minfo(h)
  expect_identical(i[["generation"]], 1)
  expect_identical(i[["cursor"]], 0)
  expect_false(i[["cancel"]])
  for (r in c(0L, 1L, 5L)) {
    cs <- claim(h, r)
    expect_identical(cs[["state"]], "idle")
    expect_identical(cs[["generation"]], 1)
  }
  # a stale payload from the prior run fails its first-call CAS against
  # the re-armed word — NULL before any issue, cursor untouched
  expect_null(nxt(h, 0L, gen = 0, k = 2))
  expect_identical(minfo(h)[["cursor"]], 0)
  expect_identical(claim(h, 0L)[["state"]], "idle")
  # the new run's payload claims and issues normally
  b <- nxt(h, 0L, gen = 1, k = 2)
  expect_identical(b[[1L]], 0)
  expect_identical(claim(h, 0L)[["state"]], "running")
  # any non-current generation is fenced, not just the previous one
  expect_null(nxt(h, 1L, gen = 99, k = 2))
})

test_that("batch sizing grows <=2x toward the target and settles", {
  h <- stage_h(1e6, 1)
  t <- 0
  b <- anxt(h, t)
  expect_identical(b[[2L]], 1)               # first batch k = 1
  # per-morsel 8us against the 200us target: double until 16, settle at 25
  ks <- numeric(8)
  for (i in seq_along(ks)) {
    t <- t + b[[2L]] * 8e-6
    b <- anxt(h, t)
    ks[i] <- b[[2L]]
  }
  expect_identical(ks, c(2, 4, 8, 16, 25, 25, 25, 25))
})

test_that("cheap morsels ramp to the batch cap; overshoot shrinks at once", {
  h <- stage_h(1e6, 1)
  t <- 0
  b <- anxt(h, t)
  for (i in 1:8) {                           # 0.1us/morsel: want 2000, cap 64
    t <- t + b[[2L]] * 1e-7
    b <- anxt(h, t)
  }
  expect_identical(b[[2L]], 64)
  t <- t + b[[2L]] * 100e-3                  # abrupt cost jump
  b <- anxt(h, t)
  expect_identical(b[[2L]], 1)               # immediate shrink on overshoot
})

test_that("the sizing ramp resets to k = 1 at a run boundary", {
  h <- stage_h(1e6, 1)
  t <- 0
  b <- anxt(h, t)
  for (i in 1:6) {
    t <- t + b[[2L]] * 1e-6
    b <- anxt(h, t)
  }
  expect_gt(b[[2L]], 1)
  .Call(sora:::sora_map_reset, h)
  b <- anxt(h, t, gen = 1)                   # new run, same ctx: fresh ramp
  expect_identical(b[[2L]], 1)
})

test_that("an all-busy publish rings the doorbell; help_once consumes and restores", {
  p <- pool_pair()
  # no worker is ever parked in the in-process harness, so a submit's
  # wake finds an empty mask and rings the bell
  h1 <- sora_submit(p[["ctrl"]], quote(1L))
  expect_true(sora_pool_dump(p[["ctrl"]])[["help"]])
  h2 <- sora_submit(p[["ctrl"]], quote(2L))
  # one help beat: claim + execute one entry, then restore the bell for
  # the entry still queued (clear -> re-check -> restore)
  expect_true(.Call(sora:::sora_pool_help_once, p[["wk"]]))
  expect_true(sora_pool_dump(p[["ctrl"]])[["help"]])
  expect_identical(sora_collect(h1), 1L)
  expect_true(.Call(sora:::sora_pool_help_once, p[["wk"]]))
  expect_false(sora_pool_dump(p[["ctrl"]])[["help"]])   # nothing queued: stays clear
  expect_identical(sora_collect(h2), 2L)
  expect_false(.Call(sora:::sora_pool_help_once, p[["wk"]]))
  pool_end(p)
})

test_that("sora_map_next consumes pool signals: help flag, skip rule, shutdown", {
  p <- pool_pair()
  sig <- .Call(sora:::sora_pool_signals, p[["wk"]])
  h <- stage_h(1e4, 1)
  b <- .Call(sora:::sora_map_next, h, 0L, 0, sig, NULL, 0)
  expect_false(b[[6L]])                      # quiet pool: no help flag
  sora_submit(p[["ctrl"]], quote(1L))              # all busy: bell rings
  b <- .Call(sora:::sora_map_next, h, 0L, 0, sig, NULL, 1e-6)
  expect_true(b[[6L]])                       # help flag rides the return
  expect_identical(b[[2L]], 2)               # this interval still updated
  # the two intervals below contain (nominal) foreign-task time: the cost
  # estimate must not absorb them — growth continues off the old estimate
  # instead of collapsing to k = 1 against the huge elapsed times
  b <- .Call(sora:::sora_map_next, h, 0L, 0, sig, NULL, 0.5)
  expect_identical(b[[2L]], 4)
  b <- .Call(sora:::sora_map_next, h, 0L, 0, sig, NULL, 1.0)
  expect_identical(b[[2L]], 8)
  # shutdown observed at the next transition: NULL, mid-cursor
  .Call(sora:::sora_pool_destroy, p[["ctrl"]])
  expect_null(.Call(sora:::sora_map_next, h, 0L, 0, sig, NULL, 1.1))
  .Call(sora:::sora_pool_leave, p[["wk"]])
})
