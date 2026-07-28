# Producer-side spill-region reuse: a handle recycles its own retired
# SHM_RAW regions through a bounded per-handle free list instead of paying
# region churn per payload. Surrender happens only at the protocol's
# consumer-done release points — collect, result-slot reuse, the worker
# keeper sweep — which the pool_pair harness makes deterministic. The
# cross-process counter (kio_pool_stats $submitters$spill_reuse) is the
# observable; correctness of recycled bytes is asserted on the values.

reuse_of <- function(p, slot = 1L)
  kio_pool_stats(p$ctrl)$submitters$spill_reuse[slot]

test_that("collect surrenders the task region; an equal-size spill pops it", {
  p <- pool_pair()                          # 256 B slots: these all spill
  v1 <- runif(100000)
  t1 <- kio_submit(p$ctrl, sum(v), v = v1)
  pool_step(p)
  expect_identical(kio_collect(t1, 5), sum(v1))
  expect_identical(reuse_of(p), 0)          # cold list: t1 created fresh

  # t1's collect released its region to the free list; t2 pops it
  v2 <- runif(100000)
  t2 <- kio_submit(p$ctrl, sum(v), v = v2)
  expect_identical(reuse_of(p), 1)
  pool_step(p)
  expect_identical(kio_collect(t2, 5), sum(v2))   # recycled bytes are v2's

  # a smaller stream rides the same region: aux delimits it under the slack
  v3 <- runif(50000)
  t3 <- kio_submit(p$ctrl, v, v = v3)
  expect_identical(reuse_of(p), 2)
  pool_step(p)
  expect_identical(kio_collect(t3, 5), v3)
  pool_end(p)
})

test_that("a cancelled task's region recycles at slot reuse, not at cancel", {
  p <- pool_pair()                          # 8 result slots per submitter
  v <- runif(100000)
  t1 <- kio_submit(p$ctrl, sum(v), v = v)
  kio_cancel(t1)
  expect_identical(pool_step(p), 1L)        # the claim consumes the cancel
  # CANCEL is no release point: the region stays pinned by its keeper
  expect_identical(reuse_of(p), 0)

  # walk the 8-slot subrange around; reallocating t1's slot drops its
  # keeper first, so the same submit's spill pops the surrendered region
  for (i in 1:7) {
    t <- kio_submit(p$ctrl, NULL)
    pool_step(p)
    kio_collect(t, 5)
  }
  t2 <- kio_submit(p$ctrl, sum(v), v = v)
  expect_identical(reuse_of(p), 1)
  pool_step(p)
  expect_identical(kio_collect(t2, 5), sum(v))
  pool_end(p)
})

test_that("the keeper sweep surrenders a collected result's region", {
  p <- pool_pair()
  # an 800 KB result spills at publish; collect frees the slot, and the
  # worker's next sweep observes the FREE and surrenders the region
  t1 <- kio_submit(p$ctrl, seq_len(n) + 0, n = 100000L)
  pool_step(p)
  r1 <- kio_collect(t1, 5)
  expect_identical(reuse_of(p), 0)
  expect_identical(pool_step(p), 0L)        # empty step: full keeper sweep
  t2 <- kio_submit(p$ctrl, seq_len(n) + 0, n = 100000L)
  pool_step(p)                              # publish pops the recycled region
  expect_identical(kio_collect(t2, 5), r1)
  expect_identical(reuse_of(p), 1)
  pool_end(p)
})

test_that("nested submits recycle through the worker handle's own list", {
  p <- pool_pair()
  t <- kio_submit(p$ctrl, {
    r1 <- kio_collect(kio_submit(pool, sum(v), v = w), timeout = 5)
    r2 <- kio_collect(kio_submit(pool, sum(v), v = w), timeout = 5)
    c(r1, r2)
  }, w = runif(100000))
  pool_step(p)
  r <- kio_collect(t, 5)
  expect_identical(r[1], r[2])
  # the worker claimed submitter slot 1 lazily; its first nested spill was
  # fresh, the second popped the region the first's nested collect released
  expect_identical(reuse_of(p, slot = 2L), 1)
  pool_end(p)
})

test_that("channel spill fallback recycles at the send-side reap", {
  p <- channel_pair(arena_size = 0)   # no arena: big payloads take SHM_RAW
  v1 <- runif(100000)
  kio_send(p$host, v1)
  expect_identical(kio_recv(p$peer, 5), v1)
  # the peer's head publish released v1's region; the next send's spill
  # reaps first, so the same send pops it
  v2 <- runif(100000)
  kio_send(p$host, v2)
  expect_identical(kio_recv(p$peer, 5), v2)   # recycled bytes are v2's
  expect_identical(.Call(kioto:::kio_channel_stat, p$host)$fl_hits, 1)
  # a smaller stream rides the same region under its slack
  v3 <- runif(50000)
  kio_send(p$host, v3)
  expect_identical(kio_recv(p$peer, 5), v3)
  expect_identical(.Call(kioto:::kio_channel_stat, p$host)$fl_hits, 2)
  # the peer opened the region once; later reads hit its mapping cache
  st <- .Call(kioto:::kio_channel_stat, p$peer)
  expect_identical(st$open_misses, 1)
  expect_identical(st$open_hits, 2)
})

test_that("consumer mapping caches skip the open once names repeat", {
  p <- pool_pair()
  # identity tasks: both directions recycle one region each after the
  # first round trip, so from round 2 the worker's entry read and the
  # submitter's result read hit their per-handle mapping caches
  v <- runif(100000)
  for (i in 1:3) {
    t <- kio_submit(p$ctrl, v, v = v)
    pool_step(p)
    expect_identical(kio_collect(t, 5), v)
    pool_step(p)                       # empty step: sweep frees the result
  }
  expect_identical(reuse_of(p), 4)     # rounds 2-3, both directions
  wk <- kio_pool_dump(p$wk)$local
  ctrl <- kio_pool_dump(p$ctrl)$local
  expect_identical(wk$open_misses, 1)  # the arg region, opened once
  expect_identical(wk$open_hits, 2)
  expect_identical(ctrl$open_misses, 1)
  expect_identical(ctrl$open_hits, 2)
  pool_end(p)
})
