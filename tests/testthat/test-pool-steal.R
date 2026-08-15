# The Phase 2 tiers over the in-process harness: Chase-Lev deque mechanics
# (owner LIFO pop, thief FIFO steal), REAPING consumption of an orphaned
# deque, and the fairness tick. Deques are populated with pool_pull — the
# test stand-in for Phase 3's nested submit — so every claim here is
# deterministic: a worker runs only when the test steps it.

test_that("the owner pops its deque LIFO down to the last-element CAS", {
  p <- pool_pair(workers = 1L, max_submitters = 1L)
  ta <- sora_submit(p[["ctrl"]], "a")
  tb <- sora_submit(p[["ctrl"]], "b")
  tc <- sora_submit(p[["ctrl"]], "c")
  expect_identical(pool_pull(p, 3L), 3L)
  st <- sora_pool_status(p[["ctrl"]])
  expect_identical(st[["injection"]], 0)
  expect_identical(st[["deque"]], 3)

  # bottom order is claim order a, b, c: pops surface c first
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(tc, timeout = 5), "c")
  expect_s3_class(sora_collect(ta, timeout = 0), "sora_timeout")
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(tb, timeout = 5), "b")
  # the final pop is the last element: the owner wins the top CAS
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(ta, timeout = 5), "a")
  expect_identical(sora_pool_status(p[["ctrl"]])[["deque"]], 0)
  pool_end(p)
})

test_that("an idle worker steals from a peer's deque top", {
  p <- pool_pair(workers = 2L, max_submitters = 1L)
  ta <- sora_submit(p[["ctrl"]], "a")
  tb <- sora_submit(p[["ctrl"]], "b")
  expect_identical(pool_pull(p, 2L), 2L)

  # the thief takes the top — the FIFO end — while the owner keeps b
  expect_identical(pool_step(p, wk = p[["wks"]][[2L]]), 1L)
  expect_identical(sora_collect(ta, timeout = 5), "a")
  expect_s3_class(sora_collect(tb, timeout = 0), "sora_timeout")
  expect_identical(sora_pool_status(p[["ctrl"]])[["deque"]], c(1, 0))
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(tb, timeout = 5), "b")
  pool_end(p)
})

test_that("a leaving worker's deque is consumed in place: REAPING to FREE", {
  p <- pool_pair(workers = 2L, max_submitters = 1L)
  ta <- sora_submit(p[["ctrl"]], "a")
  tb <- sora_submit(p[["ctrl"]], "b")
  expect_identical(pool_pull(p, 2L), 2L)
  .Call(sora:::sora_pool_leave, p[["wk"]])
  expect_identical(sora_pool_status(p[["ctrl"]])[["workers"]], c("reaping", "live"))

  # the survivor drains the orphaned deque through the ordinary steal path;
  # observing it drained returns the slot to FREE
  expect_identical(pool_step(p, wk = p[["wks"]][[2L]]), 1L)
  expect_identical(pool_step(p, wk = p[["wks"]][[2L]]), 1L)
  expect_identical(sora_collect(ta, timeout = 5), "a")
  expect_identical(sora_collect(tb, timeout = 5), "b")
  expect_identical(sora_pool_status(p[["ctrl"]])[["workers"]], c("free", "live"))
  pool_end(p)
})

test_that("a leaving worker with an empty deque frees its slot directly", {
  p <- pool_pair(workers = 2L, max_submitters = 1L)
  .Call(sora:::sora_pool_leave, p[["wk"]])
  expect_identical(sora_pool_status(p[["ctrl"]])[["workers"]], c("free", "live"))
  pool_end(p)
})

test_that("the fairness tick claims injection ahead of local work", {
  p <- pool_pair(workers = 1L, max_submitters = 1L, injection_cap = 128L,
                 per_worker_cap = 128L, result_slots = 128L)
  local <- lapply(1:61, function(i) sora_submit(p[["ctrl"]], i, i = i))
  expect_identical(pool_pull(p, 61L), 61L)
  ext <- sora_submit(p[["ctrl"]], "external")

  # claims 1..60 pop the deque; claim 61 is the tick's full scan, which
  # takes the ring entry while 1 local entry still queues below it
  for (i in 1:60) expect_identical(pool_step(p), 1L)
  expect_s3_class(sora_collect(ext, timeout = 0), "sora_timeout")
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(ext, timeout = 5), "external")
  expect_identical(sora_pool_status(p[["ctrl"]])[["deque"]], 1)
  while (pool_step(p) == 1L) NULL
  pool_end(p)
})

test_that("pull stops at deque capacity and leaves the rest queued", {
  p <- pool_pair(workers = 1L, max_submitters = 1L, injection_cap = 128L,
                 per_worker_cap = 4L, result_slots = 128L)
  tasks <- lapply(1:6, function(i) sora_submit(p[["ctrl"]], i, i = i))
  expect_identical(pool_pull(p, 6L), 4L)
  st <- sora_pool_status(p[["ctrl"]])
  expect_identical(st[["deque"]], 4)
  expect_identical(st[["injection"]], 2)
  while (pool_step(p) == 1L) NULL
  for (i in 1:6) expect_identical(sora_collect(tasks[[i]], timeout = 5), i)
  pool_end(p)
})
