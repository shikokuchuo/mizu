# Pool region and protocol lifecycle: creation parameters, header
# validation, the worker and submitter join protocols (liveness lock before
# status CAS), registry states, and teardown. Task flow is test-pool-task.R;
# spawned workers and cross-process submitters are test-pool-process.R.

test_that("kio_pool_create validates its parameters", {
  expect_error(.Call(kioto:::kio_pool_create, 0L, 8L, 64L, 64L, 64L, 256L),
               "max_workers must be")
  expect_error(.Call(kioto:::kio_pool_create, 1L, 65L, 64L, 64L, 64L, 256L),
               "max_submitters must be")
  expect_error(.Call(kioto:::kio_pool_create, 1L, 8L, 63L, 64L, 64L, 256L),
               "injection_cap must be a power of two")
  expect_error(.Call(kioto:::kio_pool_create, 1L, 8L, 64L, 100L, 64L, 256L),
               "per_worker_cap must be a power of two")
  expect_error(.Call(kioto:::kio_pool_create, 1L, 8L, 64L, 64L, 64L, 32L),
               "slot_size must be a power of two")
  # 64 is a power of two but under the floor: a result slot's inline budget
  # (slot - 40) could not hold a spilled region name
  expect_error(.Call(kioto:::kio_pool_create, 1L, 8L, 64L, 64L, 64L, 64L),
               "slot_size must be a power of two between 128")
  expect_error(.Call(kioto:::kio_pool_create, 1L, 8L, 64L, 64L, 4L, 256L),
               "result_slots must be")
})

test_that("the R-level constructor validates the worker count", {
  expect_error(kio_pool(n_workers = 0L), "at least 1")
  expect_error(kio_pool(n_workers = 2L, max_workers = 1L),
               "exceeds max_workers")
})

test_that("a fresh pool reports its layout and registry state", {
  p <- pool_pair(max_submitters = 4L, injection_cap = 32L,
                 result_slots = 32L, slot_size = 512L)
  st <- kio_pool_status(p$ctrl)
  prefix <- if (.Platform$OS.type == "windows") "Local\\kio_" else "/kio_"
  expect_true(startsWith(st$name, prefix))
  expect_identical(st$role, "controller")
  expect_identical(st$max_workers, 1L)
  expect_identical(st$max_submitters, 4L)
  expect_identical(st$injection_cap, 32L)
  expect_identical(st$result_slots, 32L)
  expect_identical(st$slot_size, 512L)
  expect_identical(st$workers, "live")
  expect_identical(st$submitters, c("live", rep("free", 3L)))
  expect_identical(st$injection, 0)
  expect_identical(unname(st$tasks), rep(0L, 5L))
  expect_false(st$shutdown)
  expect_identical(kio_pool_status(p$wk)$role, "worker")
  expect_s3_class(p$ctrl, "kio_pool")
  pool_end(p)
})

test_that("attach validates the region: absent, malformed, or not a pool", {
  expect_error(.Call(kioto:::kio_pool_attach_call, "0_0"), "cannot open")
  expect_error(.Call(kioto:::kio_pool_attach_call, "evil'; echo pwned"),
               "malformed region-name suffix")

  # a raw kioto region is not a pool: zeroed bytes fail the magic check
  xp <- .Call(kioto:::kio_region_create, 4096)
  nm <- .Call(kioto:::kio_region_name, xp)
  suffix <- sub("^.*kio_", "", nm)
  expect_error(.Call(kioto:::kio_pool_attach_call, suffix),
               "invalid pool region")
  # a channel region fails the same check
  ch <- channel_pair()
  expect_error(
    .Call(kioto:::kio_pool_attach_call, .Call(kioto:::kio_channel_suffix,
                                            ch$host)),
    "invalid pool region")
  .Call(kioto:::kio_channel_close_signal, ch$peer)
  kio_close(ch$host, timeout = 5)
  kio_close(ch$peer, timeout = 5)
})

test_that("worker join is lock-before-CAS and rejects a held or taken slot", {
  p <- pool_pair()
  suffix <- .Call(kioto:::kio_pool_suffix, p$ctrl)
  # slot 0 is held by p$wk in this same process: the flock fails first
  expect_error(.Call(kioto:::kio_pool_worker_join, suffix, 0L),
               "already held")
  expect_error(.Call(kioto:::kio_pool_worker_join, suffix, 1L),
               "out of range")
  pool_end(p)
})

test_that("submitters claim distinct slots and subranges", {
  p <- pool_pair(max_submitters = 4L, result_slots = 32L)
  suffix <- .Call(kioto:::kio_pool_suffix, p$ctrl)
  s1 <- .Call(kioto:::kio_pool_attach_call, suffix)
  s2 <- .Call(kioto:::kio_pool_attach_call, suffix)
  st <- kio_pool_status(p$ctrl)
  expect_identical(st$submitters, c("live", "live", "live", "free"))
  expect_identical(kio_pool_status(s1)$role, "submitter")

  # each submitter's tasks flow through its own ring to the same worker
  t1 <- kio_submit(s1, i + 1L, i = 10L)
  t2 <- kio_submit(s2, i + 2L, i = 20L)
  expect_identical(kio_pool_status(p$ctrl)$injection, 2)
  while (pool_step(p) == 1L) NULL
  expect_identical(kio_collect(t1, timeout = 5), 11L)
  expect_identical(kio_collect(t2, timeout = 5), 22L)
  pool_end(p)
})

test_that("the submitter registry reports full", {
  p <- pool_pair(max_submitters = 1L, result_slots = 8L)
  suffix <- .Call(kioto:::kio_pool_suffix, p$ctrl)
  expect_error(.Call(kioto:::kio_pool_attach_call, suffix),
               "submitter registry full")
  pool_end(p)
})

test_that("destroy releases the region name and poisons late attaches", {
  skip_on_os("windows")   # kernel objects have no unlink step to observe
  p <- pool_pair()
  nm <- kio_pool_status(p$ctrl)$name
  expect_no_error(.Call(kioto:::kio_region_open, nm, FALSE))
  suffix <- .Call(kioto:::kio_pool_suffix, p$ctrl)
  .Call(kioto:::kio_pool_leave, p$wk)
  .Call(kioto:::kio_pool_destroy, p$ctrl)
  expect_error(.Call(kioto:::kio_region_open, nm, FALSE), "cannot open")
  expect_error(.Call(kioto:::kio_pool_attach_call, suffix), "cannot open")
})

test_that("a stopped or destroyed handle is dead across the verb surface", {
  p <- pool_pair()
  pool_end(p)
  expect_error(kio_submit(p$ctrl, 1), "pool handle is closed")
  expect_error(kio_pool_status(p$ctrl), "pool handle is closed")
  # stop is idempotent on a released handle
  expect_true(.Call(kioto:::kio_pool_stop_call, p$ctrl, 0))
})

test_that("an attached submitter observes shutdown at its next verb", {
  p <- pool_pair()
  s <- .Call(kioto:::kio_pool_attach_call, .Call(kioto:::kio_pool_suffix,
                                               p$ctrl))
  pool_end(p)
  expect_error(kio_submit(s, 1), "pool stopped")
})

test_that("non-pool handles are rejected across the verb surface", {
  expect_error(kio_submit(new.env(), 1), "not a pool handle")
  expect_error(kio_pool_status("x"), "not a pool handle")
  expect_error(kio_pool_stop(42), "not a pool handle")
  expect_error(kio_collect(NULL), "not a task handle")
  expect_error(kio_cancel(1L), "not a task handle")
})

test_that("attach rejects each corrupted pool header field", {
  ctrl <- .Call(kioto:::kio_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  suffix <- .Call(kioto:::kio_pool_suffix, ctrl)
  rw <- .Call(kioto:::kio_region_open, kio_pool_status(ctrl)$name, TRUE)
  hdr <- .Call(kioto:::kio_peek, rw, 0, 64)
  corrupt <- function(off, bytes, msg) {
    .Call(kioto:::kio_poke, rw, off, as.raw(bytes))
    expect_error(.Call(kioto:::kio_pool_attach_call, suffix), msg)
    .Call(kioto:::kio_poke, rw, 0, hdr)
  }
  corrupt(4, 99, "ABI version mismatch")
  corrupt(8, 0, "registry capacities out of range")           # max_workers 0
  corrupt(16, 3, "not valid powers of two")                    # inj_cap 3
  corrupt(24, 63, "not a multiple of the submitter capacity")  # result_slots
  corrupt(24, c(0, 0, 0, 1), "sections exceed the mapped region") # 2^24 slots
  corrupt(48, 0, "liveness-dir string")                        # livedir_size 0
  .Call(kioto:::kio_pool_destroy, ctrl)
})

test_that("dropping the controller handle shuts the pool down at GC", {
  ctrl <- .Call(kioto:::kio_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  suffix <- .Call(kioto:::kio_pool_suffix, ctrl)
  rm(ctrl)
  gc()
  expect_error(.Call(kioto:::kio_pool_attach_call, suffix), "cannot open")
})

test_that("stop warns and reports FALSE when workers outlive the wait", {
  p <- pool_pair()
  # the in-process worker cannot exit: the bounded wait must expire
  expect_warning(ok <- kio_pool_stop(p$ctrl, timeout = 0.2), "timed out")
  expect_false(ok)
  # the worker observes shutdown and leaves on its own path, as worker_main
  expect_identical(.Call(kioto:::kio_pool_step, p$wk, 0), -1L)
  .Call(kioto:::kio_pool_leave, p$wk)
})

test_that("kio_spawn_workers validates n and walks back a failed startup", {
  ctrl <- .Call(kioto:::kio_pool_create, 2L, 8L, 64L, 64L, 64L, 256L)
  expect_error(kio_spawn_workers(ctrl, 0L), "at least 1")
  expect_error(kio_spawn_workers(ctrl, 3L), "not enough free worker slots")
  expect_error(
    kio_spawn_workers(ctrl, 1L, launcher = function(token, slot) NULL,
                      startup_timeout = 0.2),
    class = "kio_error_startup")
  # the slot was never claimed: a later spawn can still take it
  expect_identical(kio_pool_status(ctrl)$workers, c("free", "free"))
  .Call(kioto:::kio_pool_destroy, ctrl)
})

test_that("worker-only entry points reject unfit handles", {
  ctrl <- .Call(kioto:::kio_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  expect_error(.Call(kioto:::kio_pool_set_eval, ctrl), "not a worker handle")
  # a worker that never registered an evaluator refuses to step
  wk <- .Call(kioto:::kio_pool_worker_join,
              .Call(kioto:::kio_pool_suffix, ctrl), 0L)
  expect_error(.Call(kioto:::kio_pool_step, wk, 0), "no evaluator registered")
  .Call(kioto:::kio_pool_leave, wk)
  .Call(kioto:::kio_pool_destroy, ctrl)
})
