# kio_map across real worker processes: work distribution and stealing,
# worker death mid-map on both result paths, timeout under executing
# chunks, nested maps, .seed invariance across worker counts, and shared-x
# interop. The blocking kio_map() surface is exercised end-to-end here;
# where a test must act mid-map (killing a claimant), it drives the
# composable stages instead. Distribution assertions rendezvous the
# workers via a check-in directory and read stats only once both workers
# are parked, so they never race the drain or a lagging counter mirror.

test_that("a map's chunks spread across the workers", {
  skip_if_no_child_kioto()
  p <- kio_pool(n_workers = 2L)
  # The first-claimed chunk holds its worker until a second pid checks in
  # — only a chunk claimed by the other worker can supply one — so a
  # starved worker can't lose every claim to a fast drain. Bounded, with a
  # give-up marker, so a crippled pool fails below rather than hangs.
  rdv <- tfile()
  dir.create(rdv)
  r <- kio_map(p, 1:32, function(i, rdv) {
    file.create(file.path(rdv, Sys.getpid()))
    t0 <- Sys.time()
    while (length(list.files(rdv)) < 2L &&
           difftime(Sys.time(), t0, units = "secs") < 10) Sys.sleep(0.05)
    if (length(list.files(rdv)) < 2L) file.create(file.path(rdv, "gave-up"))
    i * 2L
  }, rdv = rdv, .chunks = 16L, .timeout = 60)
  expect_identical(r, as.list(1:32 * 2L))
  # counters mirror into the region at park: a row read mid-drain can lag
  expect_true(wait_until(kio_pool_status(p)$parked == 2L))
  st <- kio_pool_stats(p)
  expect_true(all(st$workers$tasks > 0))   # both workers claimed chunks
  expect_true(kio_pool_stop(p))
  unlink(rdv, recursive = TRUE)
})

test_that("an imbalanced map still returns in order, work balanced", {
  skip_if_no_child_kioto()
  p <- kio_pool(n_workers = 2L)
  # front-loaded cost — the first elements are slow, the rest instant —
  # under the same rendezvous and stats gate as above
  rdv <- tfile()
  dir.create(rdv)
  r <- kio_map(p, 1:16, function(i, rdv) {
    file.create(file.path(rdv, Sys.getpid()))
    t0 <- Sys.time()
    while (length(list.files(rdv)) < 2L &&
           difftime(Sys.time(), t0, units = "secs") < 10) Sys.sleep(0.05)
    if (length(list.files(rdv)) < 2L) file.create(file.path(rdv, "gave-up"))
    if (i <= 4L) Sys.sleep(0.1)
    i
  }, rdv = rdv, .chunks = 16L, .timeout = 60)
  expect_identical(r, as.list(1:16))
  expect_true(wait_until(kio_pool_status(p)$parked == 2L))
  expect_true(all(kio_pool_stats(p)$workers$tasks > 0))
  expect_true(kio_pool_stop(p))
  unlink(rdv, recursive = TRUE)
})

test_that("a worker killed mid-chunk fails the map with its element range", {
  skip_if_no_child_kioto()
  p <- kio_pool(n_workers = 2L)
  st <- kioto:::map_stage(p, 1:4, function(i) {
    if (i > 2L) Sys.sleep(30)
    i
  }, list(), chunks = 2)
  kioto:::map_submit(p, st)
  # Deterministic victim: once chunk 1 (elements 1-2) has published, the
  # one pending task is chunk 2 (elements 3-4) and its worker field names
  # its executor. Gating on in-flight counts instead is racy on slow
  # runners — a poll can catch chunk 1 mid-execution (killing the wrong
  # worker), or a re-read can catch both workers in flight (killing both:
  # pskill is vectorized), leaving the follow-up map to hang a workerless
  # pool.
  victim <- -1
  expect_true(wait_until({
    d <- kio_pool_dump(p)
    pend <- d$tasks[d$tasks$status == "pending", ]
    hit <- any(d$tasks$status == "ok") && nrow(pend) == 1L &&
      pend$worker >= 0L
    if (hit) victim <- d$workers$pid[pend$worker + 1L]
    hit
  }, timeout = 10))
  stopifnot(victim > 0)   # a failed gate must never reach kill(-1)
  kill_hard(victim)
  e <- tryCatch(kioto:::map_collect(p, st,
                                    deadline = kioto:::mono_time() + 30),
                error = identity)
  expect_s3_class(e, "kio_error_worker_died")
  expect_match(conditionMessage(e),
               "worker died while executing map elements 3-4")
  expect_equal(e$elements, c(3, 4))
  # the pool remains serviceable on the survivor
  expect_identical(kio_map(p, 1:4, function(i) i + 1L, .timeout = 30),
                   as.list(2:5))
  expect_true(kio_pool_stop(p))
})

test_that("worker death on the template path never exposes partial output", {
  skip_if_no_child_kioto()
  p <- kio_pool(n_workers = 2L)
  st <- kioto:::map_stage(p, 1:4, function(i) {
    if (i > 2L) Sys.sleep(30)
    i
  }, list(), template = integer(1), chunks = 2)
  kioto:::map_submit(p, st)
  # the same deterministic victim selection as the generic-path test above
  victim <- -1
  expect_true(wait_until({
    d <- kio_pool_dump(p)
    pend <- d$tasks[d$tasks$status == "pending", ]
    hit <- any(d$tasks$status == "ok") && nrow(pend) == 1L &&
      pend$worker >= 0L
    if (hit) victim <- d$workers$pid[pend$worker + 1L]
    hit
  }, timeout = 10))
  stopifnot(victim > 0)   # a failed gate must never reach kill(-1)
  kill_hard(victim)
  # chunk 1's writes landed in the output area, but the map errors as a
  # whole: nothing is ever gathered
  e <- tryCatch(kioto:::map_collect(p, st,
                                    deadline = kioto:::mono_time() + 30),
                error = identity)
  expect_match(conditionMessage(e),
               "worker died while executing map elements 3-4")
  expect_true(kio_pool_stop(p))
})

test_that(".timeout under executing chunks returns the sentinel, cleans up", {
  skip_if_no_child_kioto()
  p <- kio_pool(n_workers = 1L)
  r <- kio_map(p, 1:2, function(i) {
    Sys.sleep(1)
    i
  }, .chunks = 2L, .timeout = 0.3)
  expect_s3_class(r, "kio_timeout")
  # the executing chunk finishes, its publish CAS consumes the CANCEL; the
  # queued chunk drops at claim: every slot frees without a collect
  expect_true(wait_until(
    identical(unname(kio_pool_status(p)$tasks), rep(0L, 5L)), timeout = 10))
  expect_identical(kio_map(p, 1:2, function(i) i + 1L, .timeout = 30),
                   as.list(2:3))
  expect_true(kio_pool_stop(p))
})

test_that("a nested map fans out over the deque and peers steal it", {
  skip_if_no_child_kioto()
  p <- kio_pool(n_workers = 2L)
  # nested chunks live on the outer worker's own deque, so the peer's only
  # route to its rendezvous check-in is a steal: the count is deterministic
  rdv <- tfile()
  dir.create(rdv)
  t <- kio_submit(p, kio_map(pool, 1:16, function(i, rdv) {
    file.create(file.path(rdv, Sys.getpid()))
    t0 <- Sys.time()
    while (length(list.files(rdv)) < 2L &&
           difftime(Sys.time(), t0, units = "secs") < 10) Sys.sleep(0.05)
    if (length(list.files(rdv)) < 2L) file.create(file.path(rdv, "gave-up"))
    i * 10L
  }, rdv = rdv, .chunks = 8L), rdv = rdv)
  expect_identical(kio_collect(t, timeout = 30), as.list(1:16 * 10L))
  expect_true(wait_until(kio_pool_status(p)$parked == 2L))
  # the outer worker's chunks were stolen by its idle peer
  expect_gte(sum(kio_pool_stats(p)$workers$steals), 1)
  expect_true(kio_pool_stop(p))
  unlink(rdv, recursive = TRUE)
})

test_that(".seed maps are identical across worker counts and chunkings", {
  skip_if_no_child_kioto()
  f <- function(i) rnorm(2L)
  p1 <- kio_pool(n_workers = 1L)
  r1 <- kio_map(p1, 1:8, f, .seed = 7L, .timeout = 60)
  expect_true(kio_pool_stop(p1))
  p2 <- kio_pool(n_workers = 2L)
  r2 <- kio_map(p2, 1:8, f, .seed = 7L, .chunks = 8L, .timeout = 60)
  expect_identical(r1, r2)
  # and the draws really are per-element streams: no two elements collide
  expect_identical(anyDuplicated(vapply(r1, paste, "", collapse = ",")), 0L)
  expect_true(kio_pool_stop(p2))
})

test_that("a mori-shared x rides the descriptor as its identifier", {
  skip_if_no_child_kioto()
  skip_if_not_installed("mori")
  x <- mori::share(as.numeric(1:100) * 0.5)
  p <- kio_pool(n_workers = 2L)
  st <- kioto:::map_stage(p, x, identity, list())
  expect_false(st$xraw)   # ALTREP: reduces via the hooks, never memcpy'd
  r <- kio_map(p, x, function(v) v * 2, .timeout = 60)
  expect_identical(r, lapply(as.numeric(1:100) * 0.5, function(v) v * 2))
  expect_true(kio_pool_stop(p))
})
