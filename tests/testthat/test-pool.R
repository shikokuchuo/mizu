# Pool region and protocol lifecycle: creation parameters, header
# validation, the worker and submitter join protocols (liveness lock before
# status CAS), registry states, and teardown. Task flow is test-pool-task.R;
# spawned workers and cross-process submitters are test-pool-process.R.

test_that("sora_pool_create validates its parameters", {
  expect_error(
    .Call(sora:::sora_pool_create, 0L, 8L, 64L, 64L, 64L, 256L),
    "max_workers must be"
  )
  expect_error(
    .Call(sora:::sora_pool_create, 1L, 65L, 64L, 64L, 64L, 256L),
    "max_submitters must be"
  )
  expect_error(
    .Call(sora:::sora_pool_create, 1L, 8L, 63L, 64L, 64L, 256L),
    "injection_cap must be a power of two"
  )
  expect_error(
    .Call(sora:::sora_pool_create, 1L, 8L, 64L, 100L, 64L, 256L),
    "per_worker_cap must be a power of two"
  )
  expect_error(
    .Call(sora:::sora_pool_create, 1L, 8L, 64L, 64L, 64L, 32L),
    "slot_size must be a power of two"
  )
  # 64 is a power of two but under the floor: a result slot's inline budget
  # (slot - 40) could not hold a spilled region name
  expect_error(
    .Call(sora:::sora_pool_create, 1L, 8L, 64L, 64L, 64L, 64L),
    "slot_size must be a power of two between 128"
  )
  expect_error(
    .Call(sora:::sora_pool_create, 1L, 8L, 64L, 64L, 4L, 256L),
    "result_slots must be"
  )
})

test_that("the R-level constructor validates the worker count", {
  expect_error(sora_pool(n_workers = 0L), "at least 1")
  expect_error(
    sora_pool(n_workers = 2L, max_workers = 1L),
    "exceeds max_workers"
  )
})

test_that("a fresh pool reports its layout and registry state", {
  p <- pool_pair(
    max_submitters = 4L,
    injection_cap = 32L,
    result_slots = 32L,
    slot_size = 512L
  )
  st <- sora_pool_status(p[["ctrl"]])
  prefix <- if (.Platform[["OS.type"]] == "windows") {
    "Local\\sora_"
  } else {
    "/sora_"
  }
  expect_true(startsWith(st[["name"]], prefix))
  expect_identical(st[["role"]], "controller")
  expect_identical(st[["max_workers"]], 1L)
  expect_identical(st[["max_submitters"]], 4L)
  expect_identical(st[["injection_cap"]], 32L)
  expect_identical(st[["result_slots"]], 32L)
  expect_identical(st[["slot_size"]], 512L)
  expect_identical(st[["workers"]], "live")
  expect_identical(st[["submitters"]], c("live", rep("free", 3L)))
  expect_identical(st[["injection"]], 0)
  expect_identical(unname(st[["tasks"]]), rep(0L, 5L))
  expect_false(st[["shutdown"]])
  expect_identical(sora_pool_status(p[["wk"]])[["role"]], "worker")
  expect_s3_class(p[["ctrl"]], "sora_pool")
  pool_end(p)
})

test_that("attach validates the region: absent, malformed, or not a pool", {
  expect_error(.Call(sora:::sora_pool_attach_call, "0_0"), "cannot open")
  expect_error(
    .Call(sora:::sora_pool_attach_call, "evil'; echo pwned"),
    "malformed region-name suffix"
  )

  # a raw sora region is not a pool: zeroed bytes fail the magic check
  xp <- .Call(sora:::sora_region_create, 4096)
  nm <- .Call(sora:::sora_region_name, xp)
  suffix <- sub("^.*sora_", "", nm)
  expect_error(
    .Call(sora:::sora_pool_attach_call, suffix),
    "invalid pool region"
  )
  # a channel region fails the same check
  ch <- channel_pair()
  expect_error(
    .Call(
      sora:::sora_pool_attach_call,
      .Call(sora:::sora_channel_suffix, ch[["host"]])
    ),
    "invalid pool region"
  )
  .Call(sora:::sora_channel_close_signal, ch[["peer"]])
  sora_close(ch[["host"]], timeout = 5)
  sora_close(ch[["peer"]], timeout = 5)
})

test_that("worker join is lock-before-CAS and rejects a held or taken slot", {
  p <- pool_pair()
  suffix <- .Call(sora:::sora_pool_suffix, p[["ctrl"]])
  # slot 0 is held by p$wk in this same process: the flock fails first
  expect_error(.Call(sora:::sora_pool_worker_join, suffix, 0L), "already held")
  expect_error(.Call(sora:::sora_pool_worker_join, suffix, 1L), "out of range")
  pool_end(p)
})

test_that("submitters claim distinct slots and subranges", {
  p <- pool_pair(max_submitters = 4L, result_slots = 32L)
  suffix <- .Call(sora:::sora_pool_suffix, p[["ctrl"]])
  s1 <- .Call(sora:::sora_pool_attach_call, suffix)
  s2 <- .Call(sora:::sora_pool_attach_call, suffix)
  st <- sora_pool_status(p[["ctrl"]])
  expect_identical(st[["submitters"]], c("live", "live", "live", "free"))
  expect_identical(sora_pool_status(s1)[["role"]], "submitter")

  # each submitter's tasks flow through its own ring to the same worker
  t1 <- sora_submit(s1, i + 1L, i = 10L)
  t2 <- sora_submit(s2, i + 2L, i = 20L)
  expect_identical(sora_pool_status(p[["ctrl"]])[["injection"]], 2)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(sora_collect(t1, timeout = 5), 11L)
  expect_identical(sora_collect(t2, timeout = 5), 22L)
  pool_end(p)
})

test_that("the submitter registry reports full", {
  p <- pool_pair(max_submitters = 1L, result_slots = 8L)
  suffix <- .Call(sora:::sora_pool_suffix, p[["ctrl"]])
  expect_error(
    .Call(sora:::sora_pool_attach_call, suffix),
    "submitter registry full"
  )
  pool_end(p)
})

test_that("destroy releases the region name and poisons late attaches", {
  skip_on_os("windows") # kernel objects have no unlink step to observe
  p <- pool_pair()
  nm <- sora_pool_status(p[["ctrl"]])[["name"]]
  expect_no_error(.Call(sora:::sora_region_open, nm, FALSE))
  suffix <- .Call(sora:::sora_pool_suffix, p[["ctrl"]])
  .Call(sora:::sora_pool_leave, p[["wk"]])
  .Call(sora:::sora_pool_destroy, p[["ctrl"]])
  expect_error(.Call(sora:::sora_region_open, nm, FALSE), "cannot open")
  expect_error(.Call(sora:::sora_pool_attach_call, suffix), "cannot open")
})

test_that("a stopped or destroyed handle is dead across the verb surface", {
  p <- pool_pair()
  pool_end(p)
  expect_error(sora_submit(p[["ctrl"]], 1), "pool handle is closed")
  expect_error(sora_pool_status(p[["ctrl"]]), "pool handle is closed")
  # stop is idempotent on a released handle
  expect_true(.Call(sora:::sora_pool_stop_call, p[["ctrl"]], 0))
})

test_that("an attached submitter observes shutdown at its next verb", {
  p <- pool_pair()
  s <- .Call(
    sora:::sora_pool_attach_call,
    .Call(sora:::sora_pool_suffix, p[["ctrl"]])
  )
  pool_end(p)
  expect_error(sora_submit(s, 1), "pool stopped")
})

test_that("non-pool handles are rejected across the verb surface", {
  expect_error(sora_submit(new.env(), 1), "not a pool handle")
  expect_error(sora_pool_status("x"), "not a pool handle")
  expect_error(sora_pool_stop(42), "not a pool handle")
  expect_error(sora_collect(NULL), "not a task handle")
  expect_error(sora_cancel(1L), "not a task handle")
})

test_that("attach rejects each corrupted pool header field", {
  ctrl <- .Call(sora:::sora_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  suffix <- .Call(sora:::sora_pool_suffix, ctrl)
  rw <- .Call(sora:::sora_region_open, sora_pool_status(ctrl)[["name"]], TRUE)
  hdr <- .Call(sora:::sora_peek, rw, 0, 64)
  corrupt <- function(off, bytes, msg) {
    .Call(sora:::sora_poke, rw, off, as.raw(bytes))
    expect_error(.Call(sora:::sora_pool_attach_call, suffix), msg)
    .Call(sora:::sora_poke, rw, 0, hdr)
  }
  corrupt(4, 99, "ABI version mismatch")
  corrupt(8, 0, "registry capacities out of range") # max_workers 0
  corrupt(16, 3, "not valid powers of two") # inj_cap 3
  corrupt(24, 63, "not a multiple of the submitter capacity") # result_slots
  corrupt(24, c(0, 0, 0, 1), "sections exceed the mapped region") # 2^24 slots
  corrupt(48, 0, "liveness-dir string") # livedir_size 0
  .Call(sora:::sora_pool_destroy, ctrl)
})

test_that("dropping the controller handle shuts the pool down at GC", {
  ctrl <- .Call(sora:::sora_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  suffix <- .Call(sora:::sora_pool_suffix, ctrl)
  rm(ctrl)
  gc()
  expect_error(.Call(sora:::sora_pool_attach_call, suffix), "cannot open")
})

test_that("stop warns and reports FALSE when workers outlive the wait", {
  p <- pool_pair()
  # the in-process worker cannot exit: the bounded wait must expire
  expect_warning(ok <- sora_pool_stop(p[["ctrl"]], timeout = 0.2), "timed out")
  expect_false(ok)
  # the worker observes shutdown and leaves on its own path, as worker_main
  expect_identical(.Call(sora:::sora_pool_step, p[["wk"]], 0), -1L)
  .Call(sora:::sora_pool_leave, p[["wk"]])
})

test_that("sora_spawn_workers validates n and walks back a failed startup", {
  ctrl <- .Call(sora:::sora_pool_create, 2L, 8L, 64L, 64L, 64L, 256L)
  expect_error(sora_spawn_workers(ctrl, 0L), "at least 1")
  expect_error(sora_spawn_workers(ctrl, 3L), "not enough free worker slots")
  expect_error(
    sora_spawn_workers(
      ctrl,
      1L,
      launcher = function(token, slot) NULL,
      startup_timeout = 0.2
    ),
    class = "sora_error_startup"
  )
  # the slot was never claimed: a later spawn can still take it
  expect_identical(sora_pool_status(ctrl)[["workers"]], c("free", "free"))
  .Call(sora:::sora_pool_destroy, ctrl)
})

test_that("worker-only entry points reject unfit handles", {
  ctrl <- .Call(sora:::sora_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  expect_error(.Call(sora:::sora_pool_set_eval, ctrl), "not a worker handle")
  # a worker that never registered an evaluator refuses to step
  wk <- .Call(
    sora:::sora_pool_worker_join,
    .Call(sora:::sora_pool_suffix, ctrl),
    0L
  )
  expect_error(.Call(sora:::sora_pool_step, wk, 0), "no evaluator registered")
  .Call(sora:::sora_pool_leave, wk)
  .Call(sora:::sora_pool_destroy, ctrl)
})

test_that("a pool region past the size budget refuses before allocating", {
  expect_error(
    .Call(sora:::sora_pool_create, 64L, 64L, 256L, 2^24, 256L, 2^20),
    "pool region too large"
  )
})

test_that("controller-only entries reject worker handles", {
  p <- pool_pair()
  expect_error(
    .Call(sora:::sora_pool_destroy, p[["wk"]]),
    "only the controller can destroy"
  )
  expect_error(
    .Call(sora:::sora_pool_stop_call, p[["wk"]], 5),
    "only the controller can stop"
  )
  expect_error(
    .Call(sora:::sora_pool_retire, p[["wk"]], 0L),
    "only the controller can retire"
  )
  expect_error(
    .Call(sora:::sora_pool_ready_wait, p[["wk"]], 0L, 5),
    "only the controller can wait"
  )
  pool_end(p)
})

test_that("worker-only entries reject the controller handle", {
  p <- pool_pair()
  expect_error(
    .Call(sora:::sora_pool_leave, p[["ctrl"]]),
    "not a worker handle"
  )
  expect_error(
    .Call(sora:::sora_pool_step, p[["ctrl"]], 0),
    "not a worker handle"
  )
  expect_error(
    .Call(sora:::sora_pool_deque_pull, p[["ctrl"]], 1L),
    "not a worker handle"
  )
  expect_error(
    .Call(sora:::sora_pool_help_once, p[["ctrl"]]),
    "not a worker handle"
  )
  expect_error(
    .Call(sora:::sora_pool_map_cache, p[["ctrl"]]),
    "not a worker handle"
  )
  expect_error(
    .Call(sora:::sora_pool_run_outcome, p[["ctrl"]], NULL),
    "not a worker handle"
  )
  pool_end(p)
})

test_that("worker wait/retire validate their slot arguments", {
  p <- pool_pair()
  expect_error(
    .Call(sora:::sora_pool_ready_wait, p[["ctrl"]], "x", 5),
    "expected worker slot indices"
  )
  expect_error(
    .Call(sora:::sora_pool_ready_wait, p[["ctrl"]], 99L, 5),
    "worker slot index out of range"
  )
  expect_error(
    sora_retire_worker(p[["ctrl"]], 99L),
    "worker slot index out of range"
  )
  pool_end(p)
})

test_that("retiring a departed worker errors", {
  p <- pool_pair()
  sora_retire_worker(p[["ctrl"]], 0L)
  expect_identical(pool_step(p), -2L)
  .Call(sora:::sora_pool_leave, p[["wk"]])
  expect_error(sora_retire_worker(p[["ctrl"]], 0L), "worker slot 0 is not live")
  pool_end(p)
})

test_that("the map capacity probe requires a live pool and a submitter slot", {
  p <- pool_pair(max_submitters = 1L)
  expect_error(
    .Call(sora:::sora_pool_map_caps, p[["wk"]]),
    "submitter registry full"
  )
  pool_end(p)
  p <- pool_pair()
  expect_warning(sora_pool_stop(p[["ctrl"]], timeout = 0), "timed out")
  # stop releases the controller's handle; a joined worker's lives on and
  # sees the shutdown word
  expect_error(
    .Call(sora:::sora_pool_map_caps, p[["wk"]]),
    class = "sora_error_stopped"
  )
  .Call(sora:::sora_pool_leave, p[["wk"]])
})

test_that("collect_any returns the first terminal task with its index", {
  p <- pool_pair()
  t1 <- sora_submit(p[["ctrl"]], "one")
  t2 <- sora_submit(p[["ctrl"]], "two")
  expect_s3_class(sora_collect_any(list(t1, t2), timeout = 0), "sora_timeout")
  pool_step(p)
  expect_identical(
    sora_collect_any(list(t1, t2), timeout = 5),
    list(index = 1L, value = "one")
  )
  # the index is the position in the argument list, not submission order
  pool_step(p)
  t3 <- sora_submit(p[["ctrl"]], "three")
  expect_identical(
    sora_collect_any(list(t3, t2), timeout = 5),
    list(index = 2L, value = "two")
  )
  pool_step(p)
  expect_identical(sora_collect(t3, timeout = 5), "three")
  pool_end(p)
})

test_that("collect_any parks until any result publishes", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], "done")
  expect_s3_class(sora_collect_any(list(t), timeout = 0.1), "sora_timeout")
  pool_step(p)
  expect_identical(
    sora_collect_any(list(t), timeout = 5),
    list(index = 1L, value = "done")
  )
  pool_end(p)
})

test_that("collect_any re-signals a task error with its index", {
  p <- pool_pair()
  t1 <- sora_submit(p[["ctrl"]], "ok")
  t2 <- sora_submit(p[["ctrl"]], stop("boom"))
  pool_step(p)
  pool_step(p)
  err <- tryCatch(
    sora_collect_any(list(t2, t1), timeout = 5),
    error = identity
  )
  expect_s3_class(err, "simpleError")
  expect_identical(conditionMessage(err), "boom")
  expect_identical(err$index, 1L)
  expect_identical(sora_collect(t1, timeout = 5), "ok")
  pool_end(p)
})

test_that("collect_any reports a cancellation with its index", {
  p <- pool_pair()
  t1 <- sora_submit(p[["ctrl"]], "runs")
  t2 <- sora_submit(p[["ctrl"]], "never")
  expect_true(sora_cancel(t2))
  err <- tryCatch(
    sora_collect_any(list(t1, t2), timeout = 5),
    error = identity
  )
  expect_s3_class(err, "sora_error_cancelled")
  expect_identical(err$index, 2L)
  pool_step(p)
  expect_identical(sora_collect(t1, timeout = 5), "runs")
  pool_end(p)
})

test_that("collect_any validates its task list", {
  p <- pool_pair()
  expect_error(sora_collect_any(list()), "non-empty")
  expect_error(sora_collect_any(list(1)), "not a task handle")
  p2 <- pool_pair()
  t1 <- sora_submit(p[["ctrl"]], 1L)
  t2 <- sora_submit(p2[["ctrl"]], 2L)
  expect_error(sora_collect_any(list(t1, t2), timeout = 0), "same pool")
  pool_step(p)
  expect_identical(sora_collect(t1, timeout = 5), 1L)
  expect_error(sora_collect_any(list(t1), timeout = 0), "already collected")
  pool_step(p2)
  expect_identical(sora_collect(t2, timeout = 5), 2L)
  pool_end(p)
  pool_end(p2)
})

test_that("collect_any helps instead of parking in a nested wait", {
  p <- pool_pair()
  a <- sora_submit(p[["wk"]], "a")
  b <- sora_submit(p[["wk"]], "b")
  # help mode pops LIFO: b executes first and reports at its position
  expect_identical(
    sora_collect_any(list(a, b), timeout = 0),
    list(index = 2L, value = "b")
  )
  expect_identical(sora_collect(a, timeout = 0), "a")
  pool_end(p)
})

test_that("collect_any reports completion order across real workers", {
  skip_if_no_child_sora()
  p <- sora_pool(2L)
  slow <- sora_submit(p, {
    Sys.sleep(0.5)
    "slow"
  })
  fast <- sora_submit(p, "fast")
  expect_identical(
    sora_collect_any(list(slow, fast), timeout = 30),
    list(index = 2L, value = "fast")
  )
  expect_identical(sora_collect(slow, timeout = 30), "slow")
  expect_true(sora_pool_stop(p, timeout = 10))
})
