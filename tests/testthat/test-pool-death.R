# Death handling. Workers killed at any stage are detected by the
# controller's death listener — or by the probes that ride wakes which
# happen anyway (collect's PENDING-wake backstop, the stop sweep) — with
# the kernel-released liveness lock as the only verdict. A dead worker's
# claimed task fails as "worker died"; its queued deque work is consumed
# in place by survivors. Dead submitters release their result slots; a
# dead controller's survivors tear the orphan pool down themselves. Kill
# targets are always spawned processes, never children of fork.

test_that("a worker killed mid-task fails exactly that task", {
  skip_if_no_child_mov()
  p <- mov_pool(n_workers = 2L)
  t <- mov_submit(p, {
    Sys.sleep(30)
    "never"
  })
  t2 <- mov_submit(p, "survivor")
  expect_identical(mov_collect(t2, timeout = 10), "survivor")
  expect_true(wait_until(any(mov_pool_dump(p)$workers$in_flight != -1L)))
  d <- mov_pool_dump(p)
  claimant <- which(d$workers$in_flight != -1L)
  tools::pskill(d$workers$pid[claimant], tools::SIGKILL)

  # the listener's callback reap runs off the R thread: DIED appears and
  # the empty-dequed slot frees without this process calling anything
  expect_true(wait_until(mov_pool_status(p)$tasks[["died"]] == 1L))
  expect_true(wait_until(mov_pool_status(p)$workers[claimant] == "free"))
  err <- tryCatch(mov_collect(t, timeout = 10), error = identity)
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "worker died")
  # the pool remains fully serviceable on the surviving worker
  t3 <- mov_submit(p, "after the reap")
  expect_identical(mov_collect(t3, timeout = 10), "after the reap")
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("a worker killed while parked frees its slot for respawn", {
  skip_if_no_child_mov()
  p <- mov_pool(n_workers = 1L, max_workers = 1L)
  expect_true(wait_until(mov_pool_status(p)$parked == 1L))
  tools::pskill(mov_pool_dump(p)$workers$pid[1L], tools::SIGKILL)
  expect_true(wait_until(mov_pool_status(p)$workers == "free"))
  # elastic respawn reclaims the reaped slot
  expect_identical(mov_spawn_workers(p, 1L), 0L)
  t <- mov_submit(p, "respawned")
  expect_identical(mov_collect(t, timeout = 10), "respawned")
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("collect's backstop probe reaps with no listener registered", {
  skip_if_no_child_mov()
  # controller built without ready_wait: no death watches exist, so the
  # only reaper is the probe piggybacked on collect's PENDING wakes
  ctrl <- .Call(mov:::mov_pool_create, 1L, 8L, 64L, 64L, 64L, 256L,
                tempdir())
  mov:::spawn_worker(.Call(mov:::mov_pool_suffix, ctrl), 0L)
  expect_true(wait_until(mov_pool_status(ctrl)$workers == "live"))
  t <- mov_submit(ctrl, Sys.sleep(30))
  expect_true(wait_until(mov_pool_dump(ctrl)$workers$in_flight[1L] != -1L))
  tools::pskill(mov_pool_dump(ctrl)$workers$pid[1L], tools::SIGKILL)
  err <- tryCatch(mov_collect(t, timeout = 10), error = identity)
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "worker died")
  expect_identical(mov_pool_status(ctrl)$workers, "free")
  .Call(mov:::mov_pool_destroy, ctrl)
})

test_that("a dead worker's queued deque work is consumed in place", {
  skip_if_no_child_mov()
  p <- mov_pool(n_workers = 2L)
  d <- tfile()
  dir.create(d)
  # occupy the second worker so the nested pushes stay on the first's deque
  blocker <- mov_submit(p, Sys.sleep(1.5))
  Sys.sleep(0.2)
  t <- mov_submit(p, {
    for (i in 1:3) mov_submit(pool, file.create(f), f = file.path(d, i))
    Sys.sleep(30)
  }, d = d)
  expect_true(wait_until({
    dm <- mov_pool_dump(p)
    any(dm$workers$bottom - dm$workers$top == 3)
  }))
  dm <- mov_pool_dump(p)
  victim <- which(dm$workers$bottom - dm$workers$top == 3)
  tools::pskill(dm$workers$pid[victim], tools::SIGKILL)

  # the survivor drains the orphaned REAPING deque through ordinary steals
  expect_true(wait_until(length(dir(d)) == 3L, timeout = 15))
  expect_true(wait_until(mov_pool_status(p)$workers[victim] == "free"))
  err <- tryCatch(mov_collect(t, timeout = 10), error = identity)
  expect_match(conditionMessage(err), "worker died")
  expect_identical(mov_collect(blocker, timeout = 10), NULL)
  expect_true(mov_pool_stop(p, timeout = 10))
  unlink(d, recursive = TRUE)
})

test_that("a worker's failed publish reaps the dead submitter", {
  skip_if_no_child_mov()
  p <- mov_pool(n_workers = 1L)
  f <- tfile()
  g <- tfile()
  mov:::mov_spawn(sprintf('
    q <- mov::mov_pool_attach("%s")
    t <- mov::mov_submit(q, {
      Sys.sleep(2)
      "slow"
    })
    writeLines("submitted", %s)
    for (i in 1:600) if (file.exists(%s)) break else Sys.sleep(0.05)
  ', .Call(mov:::mov_pool_suffix, p), deparse(f), deparse(g)))
  expect_true(wait_for_file(f, timeout = 30))
  # hold the child until the worker is mid-eval: its CANCEL pre-check has
  # passed while the slot was still PENDING, so the publish CAS is the one
  # that meets the CANCEL — and it lands ~2s after the child's quick death
  expect_true(wait_until(mov_pool_dump(p)$workers$in_flight[1L] != -1L))
  file.create(g)
  # the child exits: R shutdown finalizes the handle (PENDING -> CANCEL)
  # and the kernel releases its submitter lock. The worker's publish CAS
  # fails on the CANCEL, probes the owning submitter, and reaps in-line.
  expect_true(wait_until(
    all(mov_pool_status(p)$submitters[-1L] == "free"), timeout = 20))
  expect_identical(unname(mov_pool_status(p)$tasks), rep(0L, 5L))
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("the stop sweep reaps a killed submitter's published results", {
  skip_if_no_child_mov()
  p <- mov_pool(n_workers = 1L)
  f <- tfile()
  mov:::mov_spawn(sprintf('
    q <- mov::mov_pool_attach("%s")
    t <- mov::mov_submit(q, "orphaned result")
    Sys.sleep(0.5)                       # give the worker time to publish
    writeLines(as.character(Sys.getpid()), %s)
    Sys.sleep(60)
  ', .Call(mov:::mov_pool_suffix, p), deparse(f)))
  expect_true(wait_for_file(f, timeout = 30))
  expect_true(wait_until(mov_pool_status(p)$tasks[["ok"]] == 1L))
  tools::pskill(as.integer(readLines(f)[1L]), tools::SIGKILL)
  # nothing rides on submitter death until the teardown sweep frees its
  # slots and releases the producing worker's keeper
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("a killed controller's worker tears the orphan pool down", {
  skip_if_no_child_mov()
  f <- tfile()
  mov:::mov_spawn(sprintf('
    library(mov)
    p <- mov_pool(n_workers = 1L)
    writeLines(c(as.character(Sys.getpid()),
                 as.character(mov_pool_dump(p)$workers$pid[1L])), %s)
    Sys.sleep(60)
  ', deparse(f)))
  expect_true(wait_for_file(f, timeout = 30))
  expect_true(wait_until(length(readLines(f)) == 2L))
  pids <- as.integer(readLines(f))
  expect_true(isTRUE(tools::pskill(pids[2L], 0L)))   # worker alive
  tools::pskill(pids[1L], tools::SIGKILL)            # controller dies
  # the worker's owner watch fires; it acquires the owner lock, broadcasts
  # shutdown, cleans up, and exits — no process is leaked
  expect_true(wait_until(!isTRUE(tools::pskill(pids[2L], 0L)), timeout = 15))
})

test_that("retire frees the slot; the pool keeps working and respawns", {
  skip_if_no_child_mov()
  p <- mov_pool(n_workers = 2L)
  mov_retire_worker(p, 1L)
  expect_true(wait_until(mov_pool_status(p)$workers[2L] == "free"))
  t <- mov_submit(p, "still works")
  expect_identical(mov_collect(t, timeout = 10), "still works")
  expect_identical(mov_spawn_workers(p, 1L), 1L)
  expect_identical(mov_pool_status(p)$workers, c("live", "live"))
  expect_error(mov_retire_worker(p, 5L), "out of range")
  expect_error(mov_spawn_workers(p, 1L), "not enough free worker slots")
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("a retiree lingers as the anchor for its uncollected result", {
  skip_if_no_child_mov()
  p <- mov_pool(n_workers = 1L)
  t <- mov_submit(p, Sys.getpid())
  expect_true(wait_until(mov_pool_status(p)$tasks[["ok"]] == 1L))
  pid <- mov_pool_dump(p)$workers$pid[1L]
  mov_retire_worker(p, 0L)
  expect_true(wait_until(mov_pool_status(p)$workers == "free"))
  # slot released, but the process anchors the uncollected result
  Sys.sleep(2)
  expect_true(isTRUE(tools::pskill(pid, 0L)))
  expect_identical(mov_collect(t, timeout = 10), as.integer(pid))
  # the keeper drop ends the lame-duck loop on its next beat
  expect_true(wait_until(!isTRUE(tools::pskill(pid, 0L)), timeout = 15))
  expect_true(mov_pool_stop(p, timeout = 10))
})
