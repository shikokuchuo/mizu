# Death handling. Workers killed at any stage are detected by the
# controller's death listener — or by the probes that ride wakes which
# happen anyway (collect's PENDING-wake backstop, the stop sweep) — with
# the kernel-released liveness lock as the only verdict. A dead worker's
# claimed task fails as "worker died"; its queued deque work is consumed
# in place by survivors — executed while its payloads remain reachable,
# failed as DIED where they vanished with the enqueuer (Win32 mappings
# cannot outlive their creator). Dead submitters release their result slots; a
# dead controller's survivors tear the orphan pool down themselves. Kill
# targets are always spawned processes, never children of fork.

test_that("a worker killed mid-task fails exactly that task", {
  skip_on_cran() # host + 2 workers exceeds 2 cores
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  t <- mizu_submit(p, {
    Sys.sleep(30)
    "never"
  })
  t2 <- mizu_submit(p, "survivor")
  expect_identical(mizu_collect(t2, timeout = 10), "survivor")
  expect_true(wait_until(any(
    mizu_pool_dump(p)[["workers"]][["in_flight"]] != -1L
  )))
  d <- mizu_pool_dump(p)
  claimant <- which(d[["workers"]][["in_flight"]] != -1L)
  kill_hard(d[["workers"]][["pid"]][claimant])

  # the listener's callback reap runs off the R thread: DIED appears and
  # the empty-dequed slot frees without this process calling anything
  expect_true(wait_until(mizu_pool_status(p)[["tasks"]][["died"]] == 1L))
  expect_true(wait_until(mizu_pool_status(p)[["workers"]][claimant] == "free"))
  err <- tryCatch(mizu_collect(t, timeout = 10), error = identity)
  expect_s3_class(err, "mizu_error_worker_died")
  expect_match(conditionMessage(err), "worker died")
  expect_identical(err[["slot"]], claimant - 1L)
  expect_identical(err[["pid"]], d[["workers"]][["pid"]][claimant])
  # the pool remains fully serviceable on the surviving worker
  t3 <- mizu_submit(p, "after the reap")
  expect_identical(mizu_collect(t3, timeout = 10), "after the reap")
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("death inside a nested help-collect fails outer and inner", {
  skip_if_no_child_mizu()
  # the nested claim's announce replaced the outer task's, so the reaper's
  # in-flight branch fails only the inner: the worker_slot sweep is what
  # fails the outer, which otherwise stayed PENDING forever
  p <- mizu_pool(n_workers = 1L)
  t <- mizu_submit(p, {
    s <- mizu_submit(mizu_current_pool(), Sys.sleep(30))
    mizu_collect(s, timeout = 60)
  })
  # both slots stamped by worker 0: help mode has claimed the inner off
  # the own deque and is blocked in its eval
  expect_true(wait_until({
    d <- mizu_pool_dump(p)
    nrow(d[["tasks"]]) == 2L && all(d[["tasks"]][["worker"]] == 0L)
  }))
  pid <- mizu_pool_dump(p)[["workers"]][["pid"]][1L]
  kill_hard(pid)
  expect_true(wait_until(mizu_pool_status(p)[["tasks"]][["died"]] == 2L))
  err <- tryCatch(mizu_collect(t, timeout = 10), error = identity)
  expect_s3_class(err, "mizu_error_worker_died")
  expect_identical(err[["slot"]], 0L)
  expect_identical(err[["pid"]], pid)
  # the stop sweep frees the dead worker's orphaned inner slot
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("a worker killed while parked frees its slot for respawn", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L, max_workers = 1L)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 1L))
  kill_hard(mizu_pool_dump(p)[["workers"]][["pid"]][1L])
  expect_true(wait_until(mizu_pool_status(p)[["workers"]] == "free"))
  # elastic respawn reclaims the reaped slot
  expect_identical(mizu_spawn_workers(p, 1L), 0L)
  t <- mizu_submit(p, "respawned")
  expect_identical(mizu_collect(t, timeout = 10), "respawned")
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("collect's backstop probe reaps with no listener registered", {
  skip_if_no_child_mizu()
  # controller built without ready_wait: no death watches exist, so the
  # only reaper is the probe piggybacked on collect's PENDING wakes
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  mizu:::spawn_worker(.Call(mizu:::mizu_pool_suffix, ctrl), 0L)
  expect_true(wait_until(mizu_pool_status(ctrl)[["workers"]] == "live"))
  t <- mizu_submit(ctrl, Sys.sleep(30))
  expect_true(wait_until(
    mizu_pool_dump(ctrl)[["workers"]][["in_flight"]][1L] != -1L
  ))
  kill_hard(mizu_pool_dump(ctrl)[["workers"]][["pid"]][1L])
  err <- tryCatch(mizu_collect(t, timeout = 10), error = identity)
  expect_s3_class(err, "mizu_error_worker_died")
  expect_match(conditionMessage(err), "worker died")
  expect_identical(mizu_pool_status(ctrl)[["workers"]], "free")
  .Call(mizu:::mizu_pool_destroy, ctrl)
})

test_that("a dead worker's queued deque work is consumed in place", {
  skip_on_cran()
  skip_if_no_child_mizu()
  # entries must stay inline for in-place consumption to be possible
  # everywhere: an out-of-line payload dies with its enqueuer on Windows
  # (see the spilled-payload test below), and R CMD check's deep tempdir
  # pushes these closures past the default slot's inline budget
  p <- mizu_pool(n_workers = 2L, slot_size = 1024L)
  d <- tfile()
  dir.create(d)
  # the blocker holds the second worker on a file gate until the reap has
  # run, so the nested pushes stay on the first's deque and orphan whole
  g <- tfile()
  blocker <- mizu_submit(
    p,
    for (i in 1:1200) {
      if (file.exists(g)) break else Sys.sleep(0.05)
    },
    g = g
  )
  expect_true(wait_until(any(
    mizu_pool_dump(p)[["workers"]][["in_flight"]] != -1L
  )))
  t <- mizu_submit(
    p,
    {
      for (i in 1:3) {
        mizu_submit(mizu_current_pool(), file.create(f), f = file.path(d, i))
      }
      Sys.sleep(30)
    },
    d = d
  )
  expect_true(wait_until({
    dm <- mizu_pool_dump(p)
    any(dm[["workers"]][["bottom"]] - dm[["workers"]][["top"]] == 3)
  }))
  dm <- mizu_pool_dump(p)
  victim <- which(dm[["workers"]][["bottom"]] - dm[["workers"]][["top"]] == 3)
  kill_hard(dm[["workers"]][["pid"]][victim])

  # the reap has run once the in-flight task fails; with the survivor still
  # gated, the deque is REAPING with all three entries orphaned
  expect_true(wait_until(mizu_pool_status(p)[["tasks"]][["died"]] == 1L))
  file.create(g)
  # the released survivor drains the orphaned REAPING deque through
  # ordinary steals
  expect_true(wait_until(length(dir(d)) == 3L, timeout = 15))
  expect_true(wait_until(mizu_pool_status(p)[["workers"]][victim] == "free"))
  err <- tryCatch(mizu_collect(t, timeout = 10), error = identity)
  expect_match(conditionMessage(err), "worker died")
  expect_identical(mizu_collect(blocker, timeout = 10), NULL)
  expect_true(mizu_pool_stop(p, timeout = 10))
  unlink(d, recursive = TRUE)
})

test_that("orphaned entries with spilled payloads drain without thief loss", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  d <- tfile()
  dir.create(d)
  # the blocker holds the second worker on a file gate until the reap has
  # run, so the nested pushes stay on the first's deque and orphan whole
  g <- tfile()
  blocker <- mizu_submit(
    p,
    for (i in 1:1200) {
      if (file.exists(g)) break else Sys.sleep(0.05)
    },
    g = g
  )
  expect_true(wait_until(any(
    mizu_pool_dump(p)[["workers"]][["in_flight"]] != -1L
  )))
  # the blob forces each nested entry's payload out-of-line, into regions
  # the victim creates and takes down with it on Windows
  t <- mizu_submit(
    p,
    {
      for (i in 1:3) {
        mizu_submit(
          mizu_current_pool(),
          {
            length(x)
            file.create(f)
          },
          x = blob,
          f = file.path(d, i)
        )
      }
      Sys.sleep(30)
    },
    d = d,
    blob = as.raw(seq_len(70000) %% 256)
  )
  expect_true(wait_until({
    dm <- mizu_pool_dump(p)
    any(dm[["workers"]][["bottom"]] - dm[["workers"]][["top"]] == 3)
  }))
  dm <- mizu_pool_dump(p)
  victim <- which(dm[["workers"]][["bottom"]] - dm[["workers"]][["top"]] == 3)
  kill_hard(dm[["workers"]][["pid"]][victim])

  # the reap has run once the in-flight task fails; with the survivor still
  # gated, the deque is REAPING with all three entries orphaned
  expect_true(wait_until(mizu_pool_status(p)[["tasks"]][["died"]] == 1L))
  file.create(g)
  # POSIX regions outlive their creator, so the drain executes the orphans;
  # on Windows they vanished with the victim and the drain fails each as
  # DIED — either way the deque empties, the slot frees, and the thief
  # survives to keep serving the pool
  if (.Platform[["OS.type"]] != "windows") {
    expect_true(wait_until(length(dir(d)) == 3L, timeout = 15))
  }
  expect_true(wait_until(
    mizu_pool_status(p)[["workers"]][victim] == "free",
    timeout = 15
  ))
  err <- tryCatch(mizu_collect(t, timeout = 10), error = identity)
  expect_match(conditionMessage(err), "worker died")
  expect_identical(mizu_collect(blocker, timeout = 10), NULL)
  t2 <- mizu_submit(p, "alive")
  expect_identical(mizu_collect(t2, timeout = 10), "alive")
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("a worker's failed publish reaps the dead submitter", {
  skip_on_cran() # host + worker + attached submitter exceeds 2 cores
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L)
  f <- tfile()
  g <- tfile()
  k <- tfile()
  mizu:::mizu_spawn(sprintf(
    '
    q <- mizu::mizu_pool_attach("%s")
    t <- mizu::mizu_submit(q, {
      for (i in 1:1200) if (file.exists(%s)) break else Sys.sleep(0.05)
      "slow"
    })
    writeLines(as.character(Sys.getpid()), %s)
    for (i in 1:600) if (file.exists(%s)) break else Sys.sleep(0.05)
  ',
    .Call(mizu:::mizu_pool_suffix, p),
    deparse(k),
    deparse(f),
    deparse(g)
  ))
  expect_true(wait_for_file(f, timeout = 30))
  pid <- as.integer(readLines(f))
  # hold the child until the worker is mid-eval: its CANCEL pre-check has
  # passed while the slot was still PENDING, so the publish CAS is the one
  # that meets the CANCEL
  expect_true(wait_until(
    mizu_pool_dump(p)[["workers"]][["in_flight"]][1L] != -1L
  ))
  file.create(g)
  # the child exits: R shutdown finalizes the handle (PENDING -> CANCEL)
  # and the kernel releases its submitter lock — both implied by its death
  expect_true(wait_until(!pid_alive(pid), timeout = 20))
  # only now release the worker's eval: the publish CAS fails on the
  # CANCEL, probes the owning submitter, and reaps in-line
  file.create(k)
  expect_true(wait_until(
    all(mizu_pool_status(p)[["submitters"]][-1L] == "free"),
    timeout = 20
  ))
  expect_identical(unname(mizu_pool_status(p)[["tasks"]]), rep(0L, 5L))
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("the stop sweep reaps a killed submitter's published results", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L)
  f <- tfile()
  mizu:::mizu_spawn(sprintf(
    '
    q <- mizu::mizu_pool_attach("%s")
    t <- mizu::mizu_submit(q, "orphaned result")
    Sys.sleep(0.5)                       # give the worker time to publish
    writeLines(as.character(Sys.getpid()), %s)
    Sys.sleep(60)
  ',
    .Call(mizu:::mizu_pool_suffix, p),
    deparse(f)
  ))
  expect_true(wait_for_file(f, timeout = 30))
  expect_true(wait_until(mizu_pool_status(p)[["tasks"]][["ok"]] == 1L))
  kill_hard(as.integer(readLines(f)[1L]))
  # nothing rides on submitter death until the teardown sweep frees its
  # slots and releases the producing worker's keeper
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("a killed controller's worker tears the orphan pool down", {
  skip_on_cran() # host + spawned controller + its worker exceeds 2 cores
  skip_if_no_child_mizu()
  f <- tfile()
  mizu:::mizu_spawn(sprintf(
    '
    library(mizu)
    p <- mizu_pool(n_workers = 1L)
    writeLines(c(as.character(Sys.getpid()),
                 as.character(mizu_pool_dump(p)$workers$pid[1L])), %s)
    Sys.sleep(60)
  ',
    deparse(f)
  ))
  expect_true(wait_for_file(f, timeout = 30))
  expect_true(wait_until(length(readLines(f)) == 2L))
  pids <- as.integer(readLines(f))
  expect_true(pid_alive(pids[2L])) # worker alive
  kill_hard(pids[1L]) # controller dies
  # the worker's owner watch fires; it acquires the owner lock, broadcasts
  # shutdown, cleans up, and exits — no process is leaked
  expect_true(wait_until(!pid_alive(pids[2L]), timeout = 15))
})

test_that("retire frees the slot; the pool keeps working and respawns", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  mizu_retire_worker(p, 1L)
  expect_true(wait_until(mizu_pool_status(p)[["workers"]][2L] == "free"))
  t <- mizu_submit(p, "still works")
  expect_identical(mizu_collect(t, timeout = 10), "still works")
  expect_identical(mizu_spawn_workers(p, 1L), 1L)
  expect_identical(mizu_pool_status(p)[["workers"]], c("live", "live"))
  expect_error(mizu_retire_worker(p, 5L), "out of range")
  expect_error(mizu_spawn_workers(p, 1L), "not enough free worker slots")
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("a retiree lingers as the anchor for its uncollected result", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L)
  t <- mizu_submit(p, runif(100000L)) # a spilled result needs its anchor
  expect_true(wait_until(mizu_pool_status(p)[["tasks"]][["ok"]] == 1L))
  pid <- mizu_pool_dump(p)[["workers"]][["pid"]][1L]
  mizu_retire_worker(p, 0L)
  expect_true(wait_until(mizu_pool_status(p)[["workers"]] == "free"))
  # slot released, but the process anchors the uncollected result's region
  Sys.sleep(2)
  expect_true(pid_alive(pid))
  expect_length(mizu_collect(t, timeout = 10), 100000L)
  # the keeper drop ends the lame-duck loop on its next beat
  expect_true(wait_until(!pid_alive(pid), timeout = 15))
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("a retiree with only inline results exits without lingering", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L)
  t <- mizu_submit(p, Sys.getpid())
  expect_true(wait_until(mizu_pool_status(p)[["tasks"]][["ok"]] == 1L))
  pid <- mizu_pool_dump(p)[["workers"]][["pid"]][1L]
  mizu_retire_worker(p, 0L)
  expect_true(wait_until(mizu_pool_status(p)[["workers"]] == "free"))
  # inline results live in the pool region itself: nothing to anchor
  expect_true(wait_until(!pid_alive(pid), timeout = 15))
  expect_identical(mizu_collect(t, timeout = 10), as.integer(pid))
  expect_true(mizu_pool_stop(p, timeout = 10))
})
