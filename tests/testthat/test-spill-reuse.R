# Producer-side spill-region reuse: a handle recycles its own retired
# SHM_RAW regions through a bounded per-handle free list instead of paying
# region churn per payload. Surrender happens only at the protocol's
# consumer-done release points — collect, result-slot reuse, the worker
# keeper sweep — which the pool_pair harness makes deterministic. The
# cross-process counter (mizu_pool_stats $submitters$spill_reuse) is the
# observable; correctness of recycled bytes is asserted on the values.

# A payload that always takes the SHM_RAW spill tier: the compact-ALTREP
# member fails the zero-copy layout oracle, so the object serializes in
# full. SHM_VEC payloads recycle on the view refcount discipline instead —
# deterministic only across an explicit gc() — see test-zc.R.
big_obj <- function(n) list(runif(n), 1:1000)

reuse_of <- function(p, slot = 1L) {
  mizu_pool_stats(p[["ctrl"]])[["submitters"]][["spill_reuse"]][slot]
}

test_that("collect surrenders the task region; an equal-size spill pops it", {
  p <- pool_pair() # 512 B slots: these all spill
  v1 <- big_obj(100000)
  t1 <- mizu_submit(p[["ctrl"]], sum(v[[1]]), v = v1)
  pool_step(p)
  expect_identical(mizu_collect(t1, 5), sum(v1[[1]]))
  expect_identical(reuse_of(p), 0) # cold list: t1 created fresh

  # t1's collect released its region to the free list; t2 pops it
  v2 <- big_obj(100000)
  t2 <- mizu_submit(p[["ctrl"]], sum(v[[1]]), v = v2)
  expect_identical(reuse_of(p), 1)
  pool_step(p)
  expect_identical(mizu_collect(t2, 5), sum(v2[[1]])) # recycled bytes are v2's

  # a smaller stream rides the same region: aux delimits it under the slack
  v3 <- big_obj(50000)
  t3 <- mizu_submit(p[["ctrl"]], v, v = v3)
  expect_identical(reuse_of(p), 2)
  pool_step(p)
  expect_identical(mizu_collect(t3, 5), v3)
  pool_end(p)
})

test_that("a cancelled task's region recycles at slot reuse, not at cancel", {
  p <- pool_pair() # 8 result slots per submitter
  v <- big_obj(100000)
  t1 <- mizu_submit(p[["ctrl"]], sum(v[[1]]), v = v)
  mizu_cancel(t1)
  expect_identical(pool_step(p), 1L) # the claim consumes the cancel
  # CANCEL is no release point: the region stays pinned by its keeper
  expect_identical(reuse_of(p), 0)

  # walk the 8-slot subrange around; reallocating t1's slot drops its
  # keeper first, so the same submit's spill pops the surrendered region
  for (i in 1:7) {
    t <- mizu_submit(p[["ctrl"]], NULL)
    pool_step(p)
    mizu_collect(t, 5)
  }
  t2 <- mizu_submit(p[["ctrl"]], sum(v[[1]]), v = v)
  expect_identical(reuse_of(p), 1)
  pool_step(p)
  expect_identical(mizu_collect(t2, 5), sum(v[[1]]))
  pool_end(p)
})

test_that("the keeper sweep surrenders a collected result's region", {
  # 512 B slots keep the task payload inline so only the result spills;
  # the result's compact-ALTREP member keeps it on SHM_RAW (deterministic)
  p <- pool_pair(slot_size = 512L)
  # an ~800 KB result spills at publish; collect frees the slot, and the
  # worker's next sweep observes the FREE and surrenders the region
  t1 <- mizu_submit(p[["ctrl"]], list(seq_len(n) + 0, 1:1000), n = 100000)
  pool_step(p)
  r1 <- mizu_collect(t1, 5)
  expect_identical(reuse_of(p), 0)
  expect_identical(pool_step(p), 0L) # empty step: full keeper sweep
  t2 <- mizu_submit(p[["ctrl"]], list(seq_len(n) + 0, 1:1000), n = 100000)
  pool_step(p) # publish pops the recycled region
  expect_identical(mizu_collect(t2, 5), r1)
  expect_identical(reuse_of(p), 1)
  pool_end(p)
})

test_that("nested submits recycle through the worker handle's own list", {
  p <- pool_pair()
  t <- mizu_submit(
    p[["ctrl"]],
    {
      r1 <- mizu_collect(mizu_submit(pool, sum(v[[1]]), v = w), timeout = 5)
      r2 <- mizu_collect(mizu_submit(pool, sum(v[[1]]), v = w), timeout = 5)
      c(r1, r2)
    },
    w = big_obj(100000)
  )
  pool_step(p)
  r <- mizu_collect(t, 5)
  expect_identical(r[1], r[2])
  # the worker claimed submitter slot 1 lazily; its first nested spill was
  # fresh, the second popped the region the first's nested collect released
  expect_identical(reuse_of(p, slot = 2L), 1)
  pool_end(p)
})

test_that("channel spill fallback recycles at the send-side reap", {
  p <- channel_pair(arena_size = 0) # no arena: big payloads take SHM_RAW
  v1 <- big_obj(100000)
  mizu_send(p[["host"]], v1)
  expect_identical(mizu_recv(p[["peer"]], 5), v1)
  # the peer's head publish released v1's region; the next send's spill
  # reaps first, so the same send pops it
  v2 <- big_obj(100000)
  mizu_send(p[["host"]], v2)
  expect_identical(mizu_recv(p[["peer"]], 5), v2) # recycled bytes are v2's
  expect_identical(.Call(mizu:::mizu_channel_stat, p[["host"]])[["fl_hits"]], 1)
  # a smaller stream rides the same region under its slack
  v3 <- big_obj(50000)
  mizu_send(p[["host"]], v3)
  expect_identical(mizu_recv(p[["peer"]], 5), v3)
  expect_identical(.Call(mizu:::mizu_channel_stat, p[["host"]])[["fl_hits"]], 2)
  # the peer opened the region once; later reads hit its mapping cache
  st <- .Call(mizu:::mizu_channel_stat, p[["peer"]])
  expect_identical(st[["open_misses"]], 1)
  expect_identical(st[["open_hits"]], 2)
})

test_that("consumer mapping caches skip the open once names repeat", {
  p <- pool_pair()
  # identity tasks: both directions recycle one region each after the
  # first round trip, so from round 2 the worker's entry read and the
  # submitter's result read hit their per-handle mapping caches
  v <- big_obj(100000)
  for (i in 1:3) {
    t <- mizu_submit(p[["ctrl"]], v, v = v)
    pool_step(p)
    expect_identical(mizu_collect(t, 5), v)
    pool_step(p) # empty step: sweep frees the result
  }
  expect_identical(reuse_of(p), 4) # rounds 2-3, both directions
  wk <- mizu_pool_dump(p[["wk"]])[["local"]]
  ctrl <- mizu_pool_dump(p[["ctrl"]])[["local"]]
  expect_identical(wk[["open_misses"]], 1) # the arg region, opened once
  expect_identical(wk[["open_hits"]], 2)
  expect_identical(ctrl[["open_misses"]], 1)
  expect_identical(ctrl[["open_hits"]], 2)
  pool_end(p)
})

test_that("surrender past the free list's byte cap evicts the oldest", {
  p <- pool_pair()
  v <- big_obj(3e6) # a ~24 MB stream lands in the 32 MB size class
  # both submits find an empty list, so two distinct regions exist at once
  t1 <- mizu_submit(p[["ctrl"]], sum(v[[1]]), v = v)
  t2 <- mizu_submit(p[["ctrl"]], sum(v[[1]]), v = v)
  pool_step(p)
  pool_step(p)
  expect_identical(mizu_collect(t1, 5), sum(v[[1]]))
  expect_identical(mizu_collect(t2, 5), sum(v[[1]]))
  # t2's surrender would put the list past its 32 MB cap: t1's region —
  # equal size, older stamp — was evicted to make room
  local <- mizu_pool_dump(p[["ctrl"]])[["local"]]
  expect_identical(local[["fl_entries"]], 1L)
  expect_lte(local[["fl_bytes"]], 32 * 2^20)
  pool_end(p)
})

test_that("a seventeenth distinct region evicts from the mapping cache", {
  p <- pool_pair(result_slots = 256L) # 32 per submitter: all outstanding
  v <- runif(100)
  # no collect until the end: no surrender, so every spill is a fresh
  # region and the worker's 16-entry mapping cache must evict at the 17th
  hs <- lapply(1:17, function(i) {
    mizu_submit(p[["ctrl"]], sum(v) + i, v = v, i = i)
  })
  for (i in 1:17) {
    pool_step(p)
  }
  expect_identical(mizu_pool_dump(p[["wk"]])[["local"]][["open_misses"]], 17)
  for (i in 1:17) {
    expect_identical(mizu_collect(hs[[i]], 5), sum(v) + i)
  }
  pool_end(p)
})

test_that("eviction between equal-size regions takes the older stamp", {
  p <- pool_pair()
  v <- big_obj(1.5e6) # ~12 MB stream: the 16 MB size class, two fit the cap
  t1 <- mizu_submit(p[["ctrl"]], sum(v[[1]]), v = v)
  t2 <- mizu_submit(p[["ctrl"]], sum(v[[1]]), v = v)
  t3 <- mizu_submit(p[["ctrl"]], sum(v[[1]]), v = v)
  pool_step(p)
  pool_step(p)
  pool_step(p)
  expect_identical(mizu_collect(t1, 5), sum(v[[1]]))
  expect_identical(mizu_collect(t2, 5), sum(v[[1]]))
  expect_identical(mizu_collect(t3, 5), sum(v[[1]]))
  # t3's surrender puts the list past its 32 MB cap: one of the two
  # equal-size residents is evicted on the stamp tie-break
  local <- mizu_pool_dump(p[["ctrl"]])[["local"]]
  expect_identical(local[["fl_entries"]], 2L)
  expect_lte(local[["fl_bytes"]], 32 * 2^20)
  pool_end(p)
})
