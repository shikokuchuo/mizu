# Cross-process pools: spawned workers driving the full submit -> execute ->
# collect path (the end-to-end SHM round-trip the phase must prove from R),
# the startup rendezvous and its walk-back, blocked collectors woken by
# publish, cross-process submitter attach, and orderly stop.

test_that("a spawned worker round-trips every payload kind", {
  skip_if_no_child_mov()
  p <- mov_pool()
  tasks <- list(
    mov_submit(p, x * 2L, x = 21L),                     # RAWVEC in and out
    mov_submit(p, c(l, list(b = 2)), l = list(a = 1)),  # INLINE
    mov_submit(p, sum(v), v = runif(100000)),           # SHM_RAW task
    mov_submit(p, seq_len(n) + 0, n = 100000L)          # SHM_RAW result
  )
  expect_identical(mov_collect(tasks[[1L]], timeout = 30), 42L)
  expect_identical(mov_collect(tasks[[2L]], timeout = 30),
                   list(a = 1, b = 2))
  expect_type(mov_collect(tasks[[3L]], timeout = 30), "double")
  expect_identical(mov_collect(tasks[[4L]], timeout = 30),
                   as.double(seq_len(100000L)))
  st <- mov_pool_status(p)
  expect_identical(st$workers, "live")
  expect_identical(unname(st$tasks), rep(0L, 4L))
  expect_true(mov_pool_stop(p, timeout = 10))
  expect_error(mov_submit(p, 1), "pool handle is closed")
})

test_that("a blocked collect is woken by the worker's publish", {
  skip_if_no_child_mov()
  p <- mov_pool()
  t <- mov_submit(p, {
    Sys.sleep(0.3)
    "woken"
  })
  # parked well before the result publishes; the directed unpark ends it
  expect_identical(mov_collect(t, timeout = 30), "woken")
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("a task error carries its condition across processes", {
  skip_if_no_child_mov()
  p <- mov_pool()
  t <- mov_submit(p, stop("worker-side ", x), x = "failure")
  err <- tryCatch(mov_collect(t, timeout = 30), error = identity)
  expect_s3_class(err, "error")
  expect_identical(conditionMessage(err), "worker-side failure")
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("a second process attaches as a submitter and collects", {
  skip_if_no_child_mov()
  p <- mov_pool()
  suffix <- .Call(mov:::mov_pool_suffix, p)
  f <- tfile()
  mov:::mov_spawn(sprintf('
    q <- mov::mov_pool_attach("%s")
    t <- mov::mov_submit(q, x * 2L, x = 21L)
    writeLines(as.character(mov::mov_collect(t, timeout = 30)), %s)
  ', suffix, deparse(f)))
  expect_true(wait_for_file(f, timeout = 30))
  expect_true(wait_until(identical(readLines(f), "42")))
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("mori-shared task arguments map zero-copy in the worker", {
  skip_if_no_child_mov()
  skip_if_not_installed("mori")
  p <- mov_pool()
  x <- mori::share(runif(1000))
  t <- mov_submit(p, list(shared = mori::is_shared(x),
                          name = mori::shared_name(x),
                          total = sum(x)), x = x)
  info <- mov_collect(t, timeout = 30)
  expect_true(info$shared)
  expect_identical(info$name, mori::shared_name(x))
  expect_identical(info$total, sum(x))
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("the startup deadline walks the pool back", {
  t0 <- proc.time()[[3]]
  err <- tryCatch(
    mov_pool(launcher = function(suffix, slot) NULL, startup_timeout = 0.5),
    error = identity)
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "failed to attach")
  expect_lt(proc.time()[[3]] - t0, 10)
})

test_that("a custom launcher receives the suffix and slot", {
  skip_if_no_child_mov()
  seen <- NULL
  p <- mov_pool(launcher = function(suffix, slot) {
    seen <<- list(suffix = suffix, slot = slot)
    mov:::spawn_worker(suffix, slot)
  })
  expect_match(seen$suffix, "^[0-9a-f]+_[0-9a-f]+$")
  expect_identical(seen$slot, 0L)
  t <- mov_submit(p, "via launcher")
  expect_identical(mov_collect(t, timeout = 30), "via launcher")
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("stop cancels a pending task and the worker exits cleanly", {
  skip_if_no_child_mov()
  p <- mov_pool()
  t <- mov_submit(p, Sys.sleep(0.2))
  t2 <- mov_submit(p, "queued behind")
  # the worker is mid-sleep on t; t2 is still queued when stop broadcasts
  expect_true(mov_pool_stop(p, timeout = 10))
  expect_error(mov_collect(t2, timeout = 5), "pool handle is closed")
})

test_that("a second worker picks up tasks while the first is busy", {
  skip_if_no_child_mov()
  p <- mov_pool(n_workers = 2L)
  expect_identical(mov_pool_status(p)$workers, c("live", "live"))
  slow <- mov_submit(p, {
    Sys.sleep(1)
    Sys.getpid()
  })
  Sys.sleep(0.2)
  quick <- lapply(1:5, function(i) mov_submit(p, Sys.getpid()))
  pids <- vapply(quick, mov_collect, integer(1), timeout = 30)
  expect_identical(length(unique(c(pids, mov_collect(slow, timeout = 30)))),
                   2L)
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("repeated submit/collect cycles park and wake without loss", {
  skip_if_no_child_mov()
  p <- mov_pool(n_workers = 2L)
  # an idle pool parks both workers; each cycle below is a fresh wake —
  # a lost wake in the handshake surfaces as a collect timeout
  expect_true(wait_until(mov_pool_status(p)$parked == 2L))
  for (i in 1:50) {
    t <- mov_submit(p, x + 1L, x = i)
    expect_identical(mov_collect(t, timeout = 10), i + 1L)
  }
  expect_true(mov_pool_stop(p, timeout = 10))
})

test_that("a full ring parks the submitter until a worker's pop wakes it", {
  skip_if_no_child_mov()
  p <- mov_pool(injection_cap = 4L, result_slots = 256L)
  # 32 submissions through a 4-slot ring: most block on full_waiters and
  # are woken directly by the consuming worker
  tasks <- lapply(1:32, function(i) mov_submit(p, i * 2L, i = i,
                                               .timeout = 30))
  vals <- vapply(tasks, mov_collect, integer(1), timeout = 30)
  expect_identical(vals, (1:32) * 2L)
  expect_true(mov_pool_stop(p, timeout = 10))
})
