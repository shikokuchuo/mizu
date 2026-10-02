# mizu_map across real worker processes: work distribution and stealing,
# worker death mid-map on both result paths, timeout under executing
# chunks, nested maps, .seed invariance across worker counts, and shared-x
# interop. The blocking mizu_map() surface is exercised end-to-end here;
# where a test must act mid-map (killing a claimant), it drives the
# composable stages instead. Distribution assertions rendezvous the
# workers via a check-in directory and read stats only once both workers
# are parked, so they never race the drain or a lagging counter mirror.
# Tests that need each runner task claimed by a distinct worker also park
# both workers *before* submitting: a submit that catches the fresh
# workers still in their first scan finds nobody parked and rings the
# doorbell instead of waking anyone — a help beat then re-homes the sibling
# runner onto the first claimant's own deque, which restores joinability
# but not assignment: which worker ends up executing it stays timing-
# dependent, so the gates remain for distribution determinism.

test_that("a map's chunks spread across the workers", {
  skip_on_cran() # host + 2 workers exceeds 2 cores
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  # The first-claimed chunk holds its worker until a second pid checks in
  # — only a chunk claimed by the other worker can supply one — so a
  # starved worker can't lose every claim to a fast drain. Bounded, with a
  # give-up marker, so a crippled pool fails below rather than hangs.
  rdv <- tfile()
  dir.create(rdv)
  r <- mizu_map(
    p,
    1:32,
    function(i, rdv) {
      file.create(file.path(rdv, Sys.getpid()))
      t0 <- Sys.time()
      while (
        length(list.files(rdv)) < 2L &&
          difftime(Sys.time(), t0, units = "secs") < 10
      ) {
        Sys.sleep(0.05)
      }
      if (length(list.files(rdv)) < 2L) {
        file.create(file.path(rdv, "gave-up"))
      }
      i * 2L
    },
    rdv = rdv,
    .chunks = 16L,
    .timeout = 60
  )
  expect_identical(r, as.list(1:32 * 2L))
  # counters mirror into the region at park: a row read mid-drain can lag
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  st <- mizu_pool_stats(p)
  expect_true(all(st[["workers"]][["tasks"]] > 0)) # both workers claimed chunks
  expect_true(mizu_pool_stop(p))
  unlink(rdv, recursive = TRUE)
})

test_that("an imbalanced map still returns in order, work balanced", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  # front-loaded cost — the first elements are slow, the rest instant —
  # under the same rendezvous and stats gate as above
  rdv <- tfile()
  dir.create(rdv)
  r <- mizu_map(
    p,
    1:16,
    function(i, rdv) {
      file.create(file.path(rdv, Sys.getpid()))
      t0 <- Sys.time()
      while (
        length(list.files(rdv)) < 2L &&
          difftime(Sys.time(), t0, units = "secs") < 10
      ) {
        Sys.sleep(0.05)
      }
      if (length(list.files(rdv)) < 2L) {
        file.create(file.path(rdv, "gave-up"))
      }
      if (i <= 4L) {
        Sys.sleep(0.1)
      }
      i
    },
    rdv = rdv,
    .chunks = 16L,
    .timeout = 60
  )
  expect_identical(r, as.list(1:16))
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  expect_true(all(mizu_pool_stats(p)[["workers"]][["tasks"]] > 0))
  expect_true(mizu_pool_stop(p))
  unlink(rdv, recursive = TRUE)
})

test_that("a worker killed mid-chunk fails the map with its element range", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  # parked before submit: with each push waking its own worker, exactly
  # one runner ends up sleeping in elements 3-4 while the other publishes
  # ok — nested doorbell help would leave both tasks pending on one worker
  # and the gate below unsatisfiable
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  st <- mizu:::map_stage(
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
  mizu:::map_submit(p, st)
  # Deterministic victim: once the fast runner (elements 1-2, or none) has
  # published, the one pending task holds elements 3-4 and its worker
  # field names its executor. Gating on in-flight counts instead is racy
  # on slow runners — a poll can catch elements 1-2 mid-execution (killing
  # the wrong worker), or a re-read can catch both workers in flight
  # (killing both: pskill is vectorized), leaving the follow-up map to
  # hang a workerless pool.
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
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    error = identity
  )
  expect_s3_class(e, "mizu_error_worker_died")
  expect_match(conditionMessage(e), "worker died while executing map elements")
  # the lost set is runner-granular and conservative: it always contains
  # the elements the dead worker was executing (3-4), may include the
  # dead runner's completed batches (its history died unpublished), and
  # never a surviving runner's published batches or anything outside n
  el <- e[["elements"]]
  expect_true(is.matrix(el) && ncol(el) == 2L)
  lost <- unlist(lapply(seq_len(nrow(el)), function(r) {
    seq.int(el[r, 1L], el[r, 2L])
  }))
  expect_true(all(c(3, 4) %in% lost))
  expect_true(all(lost %in% 1:4))
  # the pool remains serviceable on the survivor
  expect_identical(
    mizu_map(p, 1:4, function(i) i + 1L, .timeout = 30),
    as.list(2:5)
  )
  expect_true(mizu_pool_stop(p))
})

test_that("worker death fails a blob-path map with the chunk's exact range", {
  skip_on_cran()
  skip_if_no_child_mizu()
  # slot_size 1024: this f's chunk payload overflows the default budget
  p <- mizu_pool(n_workers = 2L, slot_size = 1024L)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  f <- function(i) {
    if (i > 2L) {
      Sys.sleep(30)
    }
    i
  }
  environment(f) <- globalenv()
  st <- mizu:::map_stage(p, 1:4, f, list(), chunks = 2)
  expect_type(st[["blob"]], "raw")
  mizu:::map_submit(p, st)
  # the same park-gated submit and deterministic victim selection as the
  # region-path test above
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
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    error = identity
  )
  expect_s3_class(e, "mizu_error_worker_died")
  # blob chunks are fixed ranges: the lost set is the dead chunk, exactly
  expect_identical(e[["elements"]], cbind(lo = 3, hi = 4))
  expect_true(mizu_pool_stop(p))
})

test_that("worker death on the template path never exposes partial output", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  st <- mizu:::map_stage(
    p,
    1:4,
    function(i) {
      if (i > 2L) {
        Sys.sleep(30)
      }
      i
    },
    list(),
    template = integer(1),
    chunks = 2
  )
  mizu:::map_submit(p, st)
  # the same park-gated submit and deterministic victim selection as the
  # generic-path test above
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
  # completed writes landed in the output area, but the map errors as a
  # whole: nothing is ever gathered, and durably written elements from
  # the dead runner report conservatively as lost
  e <- tryCatch(
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    error = identity
  )
  expect_match(conditionMessage(e), "worker died while executing map elements")
  el <- e[["elements"]]
  lost <- unlist(lapply(seq_len(nrow(el)), function(r) {
    seq.int(el[r, 1L], el[r, 2L])
  }))
  expect_true(all(c(3, 4) %in% lost))
  expect_true(mizu_pool_stop(p))
})

test_that("a lone worker's death reports the whole issued range as lost", {
  skip_if_no_child_mizu()
  # single worker: the only runner dies unpublished, so no history survives
  # — the died branch must still raise mizu_error_worker_died (regression:
  # order(NULL) turned this into a bare "argument 1 is not a vector")
  p <- mizu_pool(n_workers = 1L)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 1L))
  x <- seq_len(40) + 0 # non-ALTREP doubles: the region path
  st <- mizu:::map_stage(
    p,
    x,
    function(i) {
      Sys.sleep(0.1)
      i
    },
    list()
  )
  mizu:::map_submit(p, st)
  # kill only once the runner has claimed off the cursor, so the issued
  # range is non-empty and the kill lands mid-map
  expect_true(wait_until(
    .Call(mizu:::mizu_map_info, st[["wrap"]])[["cursor"]] > 0
  ))
  pid <- mizu_pool_dump(p)[["workers"]][["pid"]][1L]
  kill_hard(pid)
  cur <- .Call(mizu:::mizu_map_info, st[["wrap"]])[["cursor"]] # frozen by the kill
  e <- tryCatch(
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    error = identity
  )
  expect_s3_class(e, "mizu_error_worker_died")
  expect_match(conditionMessage(e), "worker died while executing map elements")
  expect_identical(e[["slot"]], 0L)
  expect_identical(e[["pid"]], pid)
  # with no history the lost set is the whole issued range, in one block
  el <- e[["elements"]]
  expect_true(is.matrix(el) && nrow(el) == 1L)
  expect_equal(el[[1L, 1L]], 1)
  expect_equal(el[[1L, 2L]], min(40, cur * st[["ms"]]))
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("a runner's announce lost to a help beat still fails as died", {
  skip_if_no_child_mizu()
  # a help beat's nested claim overwrites the runner's announce and its
  # publish clears it, so the reaper's in-flight branch finds -1: only the
  # worker_slot sweep fails the runner's slot — before it, this map hung
  # to its deadline instead of raising mizu_error_worker_died
  p <- mizu_pool(n_workers = 1L)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 1L))
  x <- seq_len(40) + 0 # non-ALTREP doubles: the region path
  st <- mizu:::map_stage(
    p,
    x,
    function(i) {
      Sys.sleep(0.1)
      i
    },
    list()
  )
  mizu:::map_submit(p, st)
  expect_true(wait_until(
    .Call(mizu:::mizu_map_info, st[["wrap"]])[["cursor"]] > 0
  ))
  # the lone worker is inside the runner, so this task can only complete
  # through a doorbell help beat — its result proves one ran
  expect_identical(mizu_collect(mizu_submit(p, "quick"), timeout = 5), "quick")
  # the announce is gone: the kill below lands in the lost-announce state
  expect_true(wait_until(
    mizu_pool_dump(p)[["workers"]][["in_flight"]][1L] == -1L
  ))
  pid <- mizu_pool_dump(p)[["workers"]][["pid"]][1L]
  kill_hard(pid)
  e <- tryCatch(
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    error = identity
  )
  expect_s3_class(e, "mizu_error_worker_died")
  expect_match(conditionMessage(e), "worker died while executing map elements")
  expect_identical(e[["slot"]], 0L)
  expect_identical(e[["pid"]], pid)
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that(".timeout under executing chunks returns the sentinel, cleans up", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L)
  r <- mizu_map(
    p,
    1:2,
    function(i) {
      Sys.sleep(1)
      i
    },
    .chunks = 2L,
    .timeout = 0.3
  )
  expect_s3_class(r, "mizu_timeout")
  # the executing chunk finishes, its publish CAS consumes the CANCEL; the
  # queued chunk drops at claim: every slot frees without a collect
  expect_true(wait_until(
    identical(unname(mizu_pool_status(p)[["tasks"]]), rep(0L, 5L)),
    timeout = 10
  ))
  expect_identical(
    mizu_map(p, 1:2, function(i) i + 1L, .timeout = 30),
    as.list(2:3)
  )
  expect_true(mizu_pool_stop(p))
})

test_that("a nested map fans out over the deque and peers steal it", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  # nested chunks live on the outer worker's own deque, so the peer's only
  # route to its rendezvous check-in is a steal: the count is deterministic
  rdv <- tfile()
  dir.create(rdv)
  t <- mizu_submit(
    p,
    mizu_map(
      mizu_current_pool(),
      1:16,
      function(i, rdv) {
        file.create(file.path(rdv, Sys.getpid()))
        t0 <- Sys.time()
        while (
          length(list.files(rdv)) < 2L &&
            difftime(Sys.time(), t0, units = "secs") < 10
        ) {
          Sys.sleep(0.05)
        }
        if (length(list.files(rdv)) < 2L) {
          file.create(file.path(rdv, "gave-up"))
        }
        i * 10L
      },
      rdv = rdv,
      .chunks = 8L
    ),
    rdv = rdv
  )
  expect_identical(mizu_collect(t, timeout = 30), as.list(1:16 * 10L))
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  # the outer worker's chunks were stolen by its idle peer
  expect_gte(sum(mizu_pool_stats(p)[["workers"]][["steals"]]), 1)
  expect_true(mizu_pool_stop(p))
  unlink(rdv, recursive = TRUE)
})

test_that(".seed maps are identical across worker counts and chunkings", {
  skip_on_cran()
  skip_if_no_child_mizu()
  f <- function(i) rnorm(2L)
  p1 <- mizu_pool(n_workers = 1L)
  r1 <- mizu_map(p1, 1:8, f, .seed = 7L, .timeout = 60)
  expect_true(mizu_pool_stop(p1))
  p2 <- mizu_pool(n_workers = 2L)
  r2 <- mizu_map(p2, 1:8, f, .seed = 7L, .chunks = 8L, .timeout = 60)
  expect_identical(r1, r2)
  # and the draws really are per-element streams: no two elements collide
  expect_identical(anyDuplicated(vapply(r1, paste, "", collapse = ",")), 0L)
  expect_true(mizu_pool_stop(p2))
})

test_that("a mori-shared x rides the descriptor as its identifier", {
  skip_on_cran()
  skip_if_no_child_mizu()
  skip_if_not_installed("mori")
  x <- mori::share(as.numeric(1:100) * 0.5)
  p <- mizu_pool(n_workers = 2L)
  st <- mizu:::map_stage(p, x, identity, list())
  expect_false(st[["xraw"]]) # ALTREP: reduces via the hooks, never memcpy'd
  r <- mizu_map(p, x, function(v) v * 2, .timeout = 60)
  expect_identical(r, lapply(as.numeric(1:100) * 0.5, function(v) v * 2))
  expect_true(mizu_pool_stop(p))
})

test_that("a pool-result view maps by reference; view collect across workers", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  v <- mizu_collect(mizu_submit(p, seq_len(100000) + 0.5), 60)
  r <- mizu_map(p, v, function(i) i * 2, .template = numeric(1), .timeout = 60)
  expect_identical(r, (seq_len(100000) + 0.5) * 2)

  rv <- mizu_map(
    p,
    1:1000,
    function(i) i * 2.5,
    .template = numeric(1),
    .collect = "view",
    .timeout = 60
  )
  expect_true(.Call(mizu:::mizu_zc_view_check, rv))
  expect_identical(as.numeric(rv), 1:1000 * 2.5)

  # a view-collected prepared run restages: v1's pages are never re-armed
  pm <- mizu_map_prepare(p, 1:100, function(i) i * 2.5, .template = numeric(1))
  v1 <- mizu_map_run(pm, .collect = "view", .timeout = 60)
  v2 <- mizu_map_run(pm, .x = (1:100) * 2, .collect = "view", .timeout = 60)
  expect_identical(as.numeric(v1), 1:100 * 2.5)
  expect_identical(as.numeric(v2), (1:100) * 2 * 2.5)
  expect_true(mizu_pool_stop(p))
})

test_that("a foreign task lands mid-map within ~a batch (doorbell help)", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  st <- mizu:::map_stage(
    p,
    1:40,
    function(i) {
      Sys.sleep(0.05)
      i
    },
    list()
  )
  mizu:::map_submit(p, st)
  Sys.sleep(0.3) # both workers deep inside the map
  t0 <- mizu:::mono_time()
  h <- mizu_submit(p, "quick")
  expect_identical(mizu_collect(h, timeout = 30), "quick")
  # picked up at a batch boundary (~one 50 ms element via the doorbell),
  # not at map end (~0.7 s away)
  expect_lt(mizu:::mono_time() - t0, 0.5)
  expect_identical(
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    as.list(1:40)
  )
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  expect_gte(sum(mizu_pool_stats(p)[["workers"]][["helps"]]), 1)
  expect_true(mizu_pool_stop(p))
})

test_that("a map submitted into a busy pool regains freed workers", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  # pin both workers asymmetrically, then submit the map into the busy
  # pool: nobody is parked, so the runner pushes ring the doorbell. The
  # first worker to free claims runner 0 and its first help beat re-homes
  # runner 1 onto its own deque — where the second worker steals it on
  # freeing, instead of finding an empty ring and parking for the rest of
  # the map (the pre-fix nested swallow serialized the map on one worker,
  # leaving the other at a single task)
  pin1 <- mizu_submit(p, Sys.sleep(0.3))
  pin2 <- mizu_submit(p, Sys.sleep(1))
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 0L))
  x <- seq_len(40) + 0 # non-ALTREP doubles: the region path
  st <- mizu:::map_stage(
    p,
    x,
    function(i) {
      Sys.sleep(0.05)
      i
    },
    list()
  )
  mizu:::map_submit(p, st)
  expect_identical(
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    as.list(x)
  )
  expect_null(mizu_collect(pin1, timeout = 30))
  expect_null(mizu_collect(pin2, timeout = 30))
  # both workers executed map work: one pin plus at least one map share
  # each — no wall-time assertions (CI timing, see test-benchmark.R), and
  # the counters mirror only at park cadence
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  expect_true(all(mizu_pool_stats(p)[["workers"]][["tasks"]] >= 2))
  expect_true(mizu_pool_stop(p))
})

test_that("killing the re-homer leaves no wedge: the survivor drains", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  # asymmetric pins as above, sized so the re-homer is killed while its
  # peer is still pinned — the re-homed runner must sit unexecuted in the
  # dead worker's deque when the kill lands
  pin1 <- mizu_submit(p, Sys.sleep(1))
  pin2 <- mizu_submit(p, Sys.sleep(5))
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 0L))
  x <- seq_len(8) + 0 # non-ALTREP doubles: the region path
  st <- mizu:::map_stage(
    p,
    x,
    function(i) {
      Sys.sleep(30) # element 1 holds the window open
      i
    },
    list()
  )
  mizu:::map_submit(p, st)
  # the worker freed by pin1 claims runner 0 and re-homes runner 1 (its
  # deque depth reaching 1 is the observable); it then enters element 1's
  # sleep — kill it there
  victim <- -1
  expect_true(wait_until(
    {
      d <- mizu_pool_dump(p)
      dq <- d[["workers"]][["bottom"]] - d[["workers"]][["top"]]
      hit <- any(dq == 1)
      if (hit) {
        victim <- d[["workers"]][["pid"]][which(dq == 1)[1L]]
      }
      hit
    },
    timeout = 10
  ))
  if (!(victim > 0)) {
    stop("no victim pid")
  } # a failed gate must never reach kill(-1)
  kill_hard(victim)
  # the re-homer died mid-runner: its unpublished batch history is lost
  # and collect raises — the regression under test is no wedge, not
  # completion
  e <- tryCatch(
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    error = identity
  )
  expect_s3_class(e, "mizu_error_worker_died")
  expect_null(mizu_collect(pin1, timeout = 30)) # published before the kill
  expect_null(mizu_collect(pin2, timeout = 30))
  # the survivor drains the orphaned deque — the re-homed entry drops at
  # the CANCEL skip once collect's cancel lands — and the pool empties
  expect_true(wait_until(
    identical(unname(mizu_pool_status(p)[["tasks"]]), rep(0L, 5L)),
    timeout = 30
  ))
  d <- mizu_pool_dump(p)
  expect_identical(nrow(d[["tasks"]]), 0L)
  expect_true(all(d[["workers"]][["bottom"]] - d[["workers"]][["top"]] <= 0))
  expect_true(mizu_pool_stop(p))
})

test_that("mizu_pool_stop mid-map returns clean within ~a batch", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  st <- mizu:::map_stage(
    p,
    1:200,
    function(i) {
      Sys.sleep(0.05)
      i
    },
    list()
  )
  mizu:::map_submit(p, st)
  Sys.sleep(0.3)
  t0 <- mizu:::mono_time()
  # runners consume the shutdown word at each batch transition and unwind
  # to the step loop's clean exit — not at cursor exhaustion, ~4.5 s away
  expect_true(mizu_pool_stop(p))
  expect_lt(mizu:::mono_time() - t0, 2)
})

test_that("a short map completes on free workers while a peer is pinned", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  pin <- mizu_submit(p, Sys.sleep(2))
  Sys.sleep(0.2) # the sleeper is claimed
  t0 <- mizu:::mono_time()
  expect_identical(
    mizu_map(p, 1:100, function(i) i + 1L, .timeout = 30),
    as.list(2:101)
  )
  # bounded by the map's own work on the free worker: the second runner
  # resolves through the exhausted-cursor trim, never a wait on the peer
  expect_lt(mizu:::mono_time() - t0, 1)
  expect_null(mizu_collect(pin, timeout = 30))
  expect_true(mizu_pool_stop(p))
})

test_that("a mid-map worker death reports the gap between survivor batches", {
  skip_on_cran()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 3L)
  # all workers parked before submit: the three morsels claim in one wave,
  # so the slow middle morsel (elements 3-4) is still executing when the
  # fast outer ones have published — their histories straddle its range
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 3L))
  st <- mizu:::map_stage(
    p,
    1:6,
    function(i) {
      if (i > 2L && i < 5L) {
        Sys.sleep(30)
      }
      i
    },
    list(),
    chunks = 3
  )
  mizu:::map_submit(p, st)
  victim <- -1
  expect_true(wait_until(
    {
      d <- mizu_pool_dump(p)
      pend <- d[["tasks"]][d[["tasks"]][["status"]] == "pending", ]
      hit <- sum(d[["tasks"]][["status"]] == "ok") == 2L &&
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
    mizu:::map_collect(st, deadline = mizu:::mono_time() + 30),
    error = identity
  )
  expect_s3_class(e, "mizu_error_worker_died")
  el <- e[["elements"]]
  lost <- unlist(lapply(seq_len(nrow(el)), function(r) {
    seq.int(el[r, 1L], el[r, 2L])
  }))
  expect_true(all(c(3, 4) %in% lost))
  expect_true(all(lost %in% 1:6))
  expect_true(mizu_pool_stop(p))
})
