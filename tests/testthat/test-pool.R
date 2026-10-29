# Pool region and protocol lifecycle: creation parameters, header
# validation, the worker and submitter join protocols (liveness lock before
# status CAS), registry states, and teardown. Task flow is test-pool-task.R;
# spawned workers and cross-process submitters are test-pool-process.R.

test_that("mizu_pool_create validates its parameters", {
  expect_error(
    .Call(mizu:::mizu_pool_create, 0L, 8L, 64L, 64L, 64L, 256L),
    "max_workers must be"
  )
  expect_error(
    .Call(mizu:::mizu_pool_create, 1L, 65L, 64L, 64L, 64L, 256L),
    "max_submitters must be"
  )
  expect_error(
    .Call(mizu:::mizu_pool_create, 1L, 8L, 63L, 64L, 64L, 256L),
    "injection_cap must be a power of two"
  )
  expect_error(
    .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 100L, 64L, 256L),
    "per_worker_cap must be a power of two"
  )
  expect_error(
    .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 32L),
    "slot_size must be a power of two"
  )
  # 64 is a power of two but under the floor: a result slot's inline budget
  # (slot - 40) could not hold a spilled region name
  expect_error(
    .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 64L),
    "slot_size must be a power of two between 128"
  )
  expect_error(
    .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 4L, 256L),
    "result_slots must be"
  )
})

test_that("mizu_pool_create rejects fractional and NA geometry", {
  expect_snapshot(
    error = TRUE,
    .Call(mizu:::mizu_pool_create, 2.5, 8L, 64L, 64L, 64L, 256L)
  )
  expect_snapshot(
    error = TRUE,
    .Call(mizu:::mizu_pool_create, 1L, 8L, 64.5, 64L, 64L, 256L)
  )
  expect_snapshot(
    error = TRUE,
    .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, NA_real_, 256L)
  )
})

test_that("the pool attach and worker join entries reject an NA suffix", {
  expect_snapshot(
    error = TRUE,
    .Call(mizu:::mizu_pool_attach_call, NA_character_)
  )
  expect_snapshot(
    error = TRUE,
    .Call(mizu:::mizu_pool_worker_join, NA_character_, 0L, NULL)
  )
})

test_that("the R-level constructor validates the worker count", {
  expect_error(mizu_pool(n_workers = 0L), "at least 1")
  expect_error(
    mizu_pool(n_workers = 2L, max_workers = 1L),
    "exceeds max_workers"
  )
})

test_that("the R-level constructor validates max_workers (L35)", {
  expect_snapshot(error = TRUE, mizu_pool(max_workers = NA))
  expect_snapshot(error = TRUE, mizu_pool(max_workers = 0L))
  expect_error(
    suppressWarnings(mizu_pool(max_workers = 1e20)),
    "max_workers must be at least 1"
  )
})

test_that("the pool constructors reject misspelled and positional options", {
  expect_snapshot(error = TRUE, mizu_pool(1L, 2L))
  expect_snapshot(error = TRUE, mizu_pool(slot_sise = 1024L))
  expect_snapshot(error = TRUE, mizu_spawn_workers(NULL, 1L, 2L))
})

test_that("a fresh pool reports its layout and registry state", {
  p <- pool_pair(
    max_submitters = 4L,
    injection_cap = 32L,
    result_slots = 32L,
    slot_size = 512L
  )
  st <- mizu_pool_status(p[["ctrl"]])
  prefix <- if (.Platform[["OS.type"]] == "windows") {
    "Local\\mizu_"
  } else {
    "/mizu_"
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
  expect_identical(unname(st[["tasks"]]), rep(0L, 6L))
  expect_false(st[["shutdown"]])
  expect_identical(mizu_pool_status(p[["wk"]])[["role"]], "worker")
  expect_s3_class(p[["ctrl"]], "mizu_pool")
  pool_end(p)
})

test_that("mizu_pool_attach validates the name", {
  expect_error(mizu_pool_attach(42), "must be a character string")
  expect_error(mizu_pool_attach(c("a", "b")), "must be a character string")
  expect_error(mizu_pool_attach(NA_character_), "must be a character string")
})

test_that("mizu_pool_dump remaps the worker language", {
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 512L)
  expect_identical(mizu_pool_dump(ctrl)[["language"]], "none")
  .Call(mizu:::mizu_pool_destroy, ctrl)
  p <- pool_pair(ident = c(9L, 0L))
  expect_identical(mizu_pool_dump(p[["ctrl"]])[["language"]], "unknown")
  pool_end(p)
})

test_that("attach validates the region: absent, malformed, or not a pool", {
  expect_error(
    .Call(mizu:::mizu_pool_attach_call, 42),
    "expected a region-name suffix"
  )
  expect_error(
    .Call(mizu:::mizu_pool_worker_join, 42, 0L, NULL),
    "expected a region-name suffix"
  )
  expect_error(.Call(mizu:::mizu_pool_attach_call, "0_0"), "cannot open")
  expect_error(
    .Call(mizu:::mizu_pool_attach_call, "evil'; echo pwned"),
    "malformed region-name suffix"
  )

  # a raw mizu region is not a pool: zeroed bytes fail the magic check
  xp <- .Call(mizu:::mizu_region_create, 4096)
  nm <- .Call(mizu:::mizu_region_name, xp)
  suffix <- sub(".*?([0-9a-f]+_[0-9a-f]+)$", "\\1", nm)
  expect_error(
    .Call(mizu:::mizu_pool_attach_call, suffix),
    "invalid pool region"
  )
  # a channel region fails the same check
  ch <- channel_pair()
  expect_error(
    .Call(
      mizu:::mizu_pool_attach_call,
      .Call(mizu:::mizu_channel_suffix, ch[["host"]])
    ),
    "invalid pool region"
  )
  .Call(mizu:::mizu_channel_close_signal, ch[["peer"]])
  mizu_close(ch[["host"]], timeout = 5)
  mizu_close(ch[["peer"]], timeout = 5)
})

test_that("worker join is lock-before-CAS and rejects a held or taken slot", {
  p <- pool_pair()
  suffix <- .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  # slot 0 is held by p$wk in this same process: the flock fails first
  expect_error(
    .Call(mizu:::mizu_pool_worker_join, suffix, 0L, NULL),
    "already held"
  )
  expect_error(
    .Call(mizu:::mizu_pool_worker_join, suffix, 1L, NULL),
    "out of range"
  )
  pool_end(p)
})

test_that("the test identity override validates its shape", {
  p <- pool_pair()
  on.exit(pool_end(p))
  suffix <- .Call(mizu:::mizu_pool_suffix, p$ctrl)
  expect_error(
    .Call(mizu:::mizu_pool_worker_join, suffix, 0L, 42L),
    "identity pair"
  )
})

test_that("worker join rejects a mismatched language word", {
  ctrl <- .Call(mizu:::mizu_pool_create, 2L, 8L, 64L, 64L, 64L, 512L)
  suffix <- .Call(mizu:::mizu_pool_suffix, ctrl)
  wk <- .Call(mizu:::mizu_pool_worker_join, suffix, 0L, NULL)
  expect_error(
    .Call(mizu:::mizu_pool_worker_join, suffix, 1L, c(3L, 0L)),
    "cannot join"
  )
  wk2 <- .Call(mizu:::mizu_pool_worker_join, suffix, 1L, NULL)
  .Call(mizu:::mizu_pool_leave, wk)
  .Call(mizu:::mizu_pool_leave, wk2)
  .Call(mizu:::mizu_pool_destroy, ctrl)
})

test_that("the last worker's leave finalizes an orphaned deque for respawn", {
  p <- pool_pair()
  on.exit(pool_end(p))
  suffix <- .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  t1 <- mizu_submit(p[["ctrl"]], 1 + 1)
  t2 <- mizu_submit(p[["ctrl"]], 2 + 2)
  expect_identical(pool_pull(p, 2L), 2L)
  .Call(mizu:::mizu_pool_leave, p[["wk"]])
  # no live worker can drain the deque: the leave fails the queued tasks
  # and frees the slot instead of wedging it REAPING forever
  expect_error(mizu_collect(t1, timeout = 5), class = "mizu_error_worker_died")
  expect_error(mizu_collect(t2, timeout = 5), class = "mizu_error_worker_died")
  expect_identical(mizu_pool_status(p[["ctrl"]])[["workers"]], "free")
  wk2 <- .Call(mizu:::mizu_pool_worker_join, suffix, 0L, NULL)
  .Call(mizu:::mizu_pool_set_eval, wk2)
  t3 <- mizu_submit(p[["ctrl"]], 3 + 3)
  expect_identical(.Call(mizu:::mizu_pool_step, wk2, 0), 1L)
  expect_identical(mizu_collect(t3, timeout = 5), 6)
  .Call(mizu:::mizu_pool_leave, wk2)
})


test_that("submitters claim distinct slots and subranges", {
  p <- pool_pair(max_submitters = 4L, result_slots = 32L)
  suffix <- .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  s1 <- .Call(mizu:::mizu_pool_attach_call, suffix)
  s2 <- .Call(mizu:::mizu_pool_attach_call, suffix)
  st <- mizu_pool_status(p[["ctrl"]])
  expect_identical(st[["submitters"]], c("live", "live", "live", "free"))
  expect_identical(mizu_pool_status(s1)[["role"]], "submitter")

  # each submitter's tasks flow through its own ring to the same worker
  t1 <- mizu_submit(s1, i + 1L, i = 10L)
  t2 <- mizu_submit(s2, i + 2L, i = 20L)
  expect_identical(mizu_pool_status(p[["ctrl"]])[["injection"]], 2)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(t1, timeout = 5), 11L)
  expect_identical(mizu_collect(t2, timeout = 5), 22L)
  pool_end(p)
})

test_that("batch submit flows tasks and returns handles in order", {
  p <- pool_pair()
  ts <- mizu_submit_batch(p[["ctrl"]], list(quote(1L + 1L), quote(2L * 3L), 1L))
  expect_length(ts, 3L)
  expect_s3_class(ts[[1L]], "mizu_task")
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect_all(ts, timeout = 5), list(2L, 6L, 1L))
  pool_end(p)
})

test_that("batch submit shares the ... bindings across tasks", {
  p <- pool_pair()
  ts <- mizu_submit_batch(
    p[["ctrl"]],
    list(quote(x + 1L), quote(x * 2L)),
    x = 10L
  )
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect_all(ts, timeout = 5), list(11L, 20L))
  pool_end(p)
})

test_that("a batched task's view constant stays anchored until collect", {
  ch <- channel_pair(arena_size = 0)
  on.exit(channel_end(ch), add = TRUE)
  x <- runif(2e5)
  y <- runif(2e5)
  mizu_send(ch[["host"]], x)
  v1 <- mizu_recv(ch[["peer"]], 5)
  mizu_send(ch[["host"]], y)
  v2 <- mizu_recv(ch[["peer"]], 5)
  expect_true(is_view(v1) && is_view(v2))
  p <- pool_pair()
  on.exit(pool_end(p), add = TRUE)
  ts <- mizu_submit_batch(
    p[["ctrl"]],
    list(bquote(.(v1) + 0), bquote(.(v2) + 0))
  )
  rm(v1, v2)
  invisible(gc())
  # recycle pressure: a region freed by a dead pin is popped and rewritten
  for (i in 1:2) {
    mizu_send(ch[["host"]], runif(2e5))
    invisible(mizu_recv(ch[["peer"]], 5))
  }
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect_all(ts, timeout = 5), list(x, y))
})

test_that("task argument names never collide with the formals ahead of ...", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], e * 2L, e = 3L)
  pool_step(p)
  expect_identical(mizu_collect(t, 5), 6L)
  t <- mizu_submit(p[["ctrl"]], base^exp, base = 2, exp = 3)
  pool_step(p)
  expect_identical(mizu_collect(t, 5), 8)
  t <- mizu_submit(p[["ctrl"]], pool * 2L, pool = 4L)
  pool_step(p)
  expect_identical(mizu_collect(t, 5), 8L)
  ts <- mizu_submit_batch(p[["ctrl"]], list(quote(e * 2L)), e = 3L)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect_all(ts, timeout = 5), list(6L))
  pool_end(p)
})

test_that("batch submit validates its inputs", {
  p <- pool_pair()
  expect_identical(mizu_submit_batch(p[["ctrl"]], list()), list())
  expect_error(mizu_submit_batch(p[["ctrl"]], 1L), "list of expressions")
  expect_error(
    mizu_submit_batch(p[["ctrl"]], list(quote(x)), 10L),
    "must be named"
  )
  pool_end(p)
})

test_that("batch submit returns a short list when the ring fills", {
  p <- pool_pair(injection_cap = 4L)
  ts <- mizu_submit_batch(p[["ctrl"]], rep(list(quote(1L)), 8L), .timeout = 0)
  expect_length(ts, 4L)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect_all(ts, timeout = 5), as.list(rep(1L, 4L)))
  pool_end(p)
})

test_that("batch submit on a stopped pool raises stopped", {
  p <- pool_pair()
  on.exit(pool_end(p))
  suffix <- .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  sub <- .Call(mizu:::mizu_pool_attach_call, suffix)
  expect_warning(mizu_pool_stop(p[["ctrl"]], timeout = 0.1), "timed out")
  expect_error(mizu_submit_batch(sub, list(quote(1 + 1))), "pool stopped")
})

test_that("batch submit fails mid-batch when result slots exhaust", {
  p <- pool_pair(result_slots = 8L)
  on.exit(pool_end(p))
  expect_error(
    mizu_submit_batch(p[["ctrl"]], list(quote(1 + 1), quote(2 + 2))),
    "result slots exhausted"
  )
  expect_identical(pool_step(p), 1L)
})

test_that("a reused result slot releases a kept zero-copy task payload", {
  p <- pool_pair(result_slots = 8L)
  on.exit(pool_end(p))
  x <- runif(20000) # 160 KB: past the zc floor, stages SHM_VEC
  t <- mizu_submit(p[["ctrl"]], x, x = x)
  pool_step(p)
  rm(t)
  gc()
  t2 <- mizu_submit(p[["ctrl"]], 1L)
  pool_step(p)
  expect_identical(mizu_collect(t2, 5), 1L)
})

test_that("a corrupt ring entry is skipped without a claim", {
  p <- pool_pair()
  on.exit(pool_end(p))
  mizu_submit(p[["ctrl"]], 1 + 1)
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  # ring 0 entry 0 (pool_pair geometry): the rs_index word
  base <- 128L + 256L + 8L * 128L + 256L + 256L
  .Call(
    mizu:::mizu_poke,
    rw,
    base + 16L,
    writeBin(9999L, raw(), size = 4L, endian = "little")
  )
  expect_identical(pool_step(p), 0L)
})

test_that("a corrupt deque entry is skipped without a claim", {
  p <- pool_pair()
  on.exit(pool_end(p))
  mizu_submit(p[["ctrl"]], 1 + 1)
  pool_pull(p, 1L) # the injection entry rides the worker's own deque now
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  # wk[0]'s deque_buf_off is its slot word at 152; entry 0's rs_index word
  doff <- i64v(.Call(mizu:::mizu_peek, rw, 152, 8))
  .Call(
    mizu:::mizu_poke,
    rw,
    doff + 16L,
    writeBin(9999L, raw(), size = 4L, endian = "little")
  )
  expect_identical(pool_step(p), 0L)
})

test_that("a corrupt stolen entry aborts the steal", {
  p <- pool_pair(workers = 2L)
  on.exit(pool_end(p))
  mizu_submit(p[["ctrl"]], 1 + 1)
  pool_pull(p, 1L, wk = p[["wks"]][[1L]])
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  doff <- i64v(.Call(mizu:::mizu_peek, rw, 152, 8))
  .Call(
    mizu:::mizu_poke,
    rw,
    doff + 16L,
    writeBin(9999L, raw(), size = 4L, endian = "little")
  )
  expect_identical(pool_step(p, wk = p[["wks"]][[2L]]), 0L)
})

test_that("a vanished task payload publishes LOST to the collector", {
  p <- pool_pair()
  on.exit(pool_end(p))
  x <- paste(rep("x", 3000), collapse = "")
  t <- mizu_submit(p[["ctrl"]], x, x = x)
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  # ring 0 entry 0 (pool_pair geometry): the SHM_RAW payload is the name
  base <- 128L + 256L + 8L * 128L + 256L + 256L
  id <- .Call(mizu:::mizu_peek, rw, base + 40L, 40L)
  hex0 <- base + 40L + which(id == charToRaw("_"))[1L] + 1L
  b <- .Call(mizu:::mizu_peek, rw, hex0, 1L)
  # a well-formed name that does not exist (one hex digit flipped)
  .Call(
    mizu:::mizu_poke,
    rw,
    hex0,
    if (b == charToRaw("a")) charToRaw("b") else charToRaw("a")
  )
  pool_step(p)
  expect_error(mizu_collect(t, 5), "payload region vanished")
})

test_that("a rawspill-framed task whose region vanished reads as lost", {
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], 1L)
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  # ring 0 entry 0 reframed as RAWSPILL: the claim's byte resolve skips the
  # kind (vector data, not a stream), so the gone verdict lands past the
  # claim, in the exec's generic read
  base <- 128L + 256L + 8L * 128L + 256L + 256L
  u32 <- function(v) writeBin(v, raw(), size = 4L, endian = "little")
  nm <- charToRaw("/mizu_deadbeef_1234") # well-formed, does not exist
  .Call(mizu:::mizu_poke, rw, base + 24L, as.raw(8L)) # kind RAWSPILL
  .Call(mizu:::mizu_poke, rw, base + 28L, u32(8L)) # one double
  .Call(
    mizu:::mizu_poke,
    rw,
    base + 32L,
    c(u32(as.integer(length(nm) * 256L + 14L)), raw(4L)) # aux: name len, REALSXP
  )
  .Call(mizu:::mizu_poke, rw, base + 40L, nm)
  pool_step(p)
  expect_error(mizu_collect(t, 5), "task lost")
})

test_that("a task on a worker without an evaluator is refused up front", {
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 512L)
  suffix <- .Call(mizu:::mizu_pool_suffix, ctrl)
  wk <- .Call(mizu:::mizu_pool_worker_join, suffix, 0L, NULL)
  p <- list(ctrl = ctrl, wk = wk, wks = list(wk))
  mizu_submit(ctrl, identity(1))
  expect_error(pool_step(p), "no evaluator registered")
  .Call(mizu:::mizu_pool_leave, wk)
  .Call(mizu:::mizu_pool_destroy, ctrl)
})

test_that("the submitter registry reports full", {
  p <- pool_pair(max_submitters = 1L, result_slots = 8L)
  suffix <- .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  expect_error(
    .Call(mizu:::mizu_pool_attach_call, suffix),
    "submitter registry full"
  )
  pool_end(p)
})

test_that("destroy releases the region name and poisons late attaches", {
  skip_on_os("windows") # kernel objects have no unlink step to observe
  p <- pool_pair()
  nm <- mizu_pool_status(p[["ctrl"]])[["name"]]
  expect_no_error(.Call(mizu:::mizu_region_open, nm, FALSE))
  suffix <- .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  .Call(mizu:::mizu_pool_leave, p[["wk"]])
  .Call(mizu:::mizu_pool_destroy, p[["ctrl"]])
  expect_error(.Call(mizu:::mizu_region_open, nm, FALSE), "cannot open")
  expect_error(.Call(mizu:::mizu_pool_attach_call, suffix), "cannot open")
})

test_that("a stopped or destroyed handle is dead across the verb surface", {
  p <- pool_pair()
  pool_end(p)
  expect_error(mizu_submit(p[["ctrl"]], 1), "pool handle is closed")
  expect_error(mizu_pool_status(p[["ctrl"]]), "pool handle is closed")
  # stop is idempotent on a released handle
  expect_true(.Call(mizu:::mizu_pool_stop_call, p[["ctrl"]], 0))
})

test_that("pool introspection on a stopped pool names the closed handle", {
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  mizu_pool_stop(ctrl, timeout = 0)
  expect_error(mizu_pool_status(ctrl), "pool handle is closed")
  expect_error(mizu_pool_stats(ctrl), "pool handle is closed")
  expect_error(mizu_pool_dump(ctrl), "pool handle is closed")
  expect_error(.Call(mizu:::mizu_pool_suffix, ctrl), "pool handle is closed")
})

test_that("an attached submitter observes shutdown at its next verb", {
  p <- pool_pair()
  s <- .Call(
    mizu:::mizu_pool_attach_call,
    .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  )
  pool_end(p)
  expect_error(mizu_submit(s, 1), "pool stopped")
})

test_that("non-pool handles are rejected across the verb surface", {
  expect_error(mizu_submit(new.env(), 1), "not a pool handle")
  expect_error(mizu_pool_status("x"), "not a pool handle")
  expect_error(mizu_pool_stop(42), "not a pool handle")
  expect_error(mizu_collect(NULL), "not a task handle")
  expect_error(mizu_cancel(1L), "not a task handle")
})

test_that("attach rejects each corrupted pool header field", {
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  suffix <- .Call(mizu:::mizu_pool_suffix, ctrl)
  rw <- .Call(mizu:::mizu_region_open, mizu_pool_status(ctrl)[["name"]], TRUE)
  hdr <- .Call(mizu:::mizu_peek, rw, 0, 64)
  corrupt <- function(off, bytes, msg) {
    .Call(mizu:::mizu_poke, rw, off, as.raw(bytes))
    expect_error(.Call(mizu:::mizu_pool_attach_call, suffix), msg)
    .Call(mizu:::mizu_poke, rw, 0, hdr)
  }
  corrupt(4, 99, "ABI version mismatch")
  corrupt(8, 0, "registry capacities out of range") # max_workers 0
  corrupt(16, 3, "not valid powers of two") # inj_cap 3
  corrupt(24, 63, "not a multiple of the submitter capacity") # result_slots
  corrupt(24, c(0, 0, 0, 1), "sections exceed the mapped region") # 2^24 slots
  corrupt(48, 0, "liveness-dir string") # livedir_size 0
  .Call(mizu:::mizu_pool_destroy, ctrl)
})

test_that("dropping the controller handle shuts the pool down at GC", {
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  suffix <- .Call(mizu:::mizu_pool_suffix, ctrl)
  rm(ctrl)
  gc()
  expect_error(.Call(mizu:::mizu_pool_attach_call, suffix), "cannot open")
})

test_that("verbs reject an NA timeout instead of waiting forever (R-08)", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], 1 + 1)
  expect_error(mizu_collect(t, timeout = NA_real_), "not NA")
  expect_error(mizu_collect_any(list(t), timeout = NA), "not NA")
  expect_error(mizu_collect_all(list(t), timeout = NA_real_), "not NA")
  expect_error(mizu_pool_stop(p[["ctrl"]], timeout = NA_real_), "not NA")
  expect_error(
    .Call(mizu:::mizu_pool_ready_wait, p[["ctrl"]], 0L, NA_real_),
    "not NA"
  )
  # the rejections consumed nothing: the task still runs and collects
  pool_step(p)
  expect_identical(mizu_collect(t, 5), 2)
  pool_end(p)
})

test_that("a forked child's GC does not destroy the parent's pool (R-14)", {
  skip_on_os("windows") # no fork
  skip_if_no_fork() # Positron/RStudio sessions block mcfork
  p <- pool_pair()
  env <- environment()
  res <- parallel::mcparallel(
    {
      rm(list = ls(env), envir = env) # the child drops its inherited handles
      invisible(gc()) # their finalizers run here — abandon, not destroy
      TRUE
    },
    silent = TRUE
  )
  expect_identical(parallel::mccollect(res)[[1L]], TRUE)
  t <- mizu_submit(p[["ctrl"]], 40 + 2)
  pool_step(p)
  expect_identical(mizu_collect(t, 5), 42)
  pool_end(p)
})

test_that("pool teardown releases outstanding submitter pins", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], runif(100000))
  pool_step(p)
  v <- mizu_collect(t, 5)
  flag <- new.env()
  flag$n <- 0L
  for (i in seq_len(3)) {
    e <- new.env()
    reg.finalizer(e, function(x) flag$n <- flag$n + 1L, onexit = TRUE)
    # serialize fallback; the nested view fires the wire hook, so it pins
    mizu_submit(p[["ctrl"]], identity, x = list(e, v))
  }
  rm(e)
  pool_end(p) # the destroy drops the unclaimed tasks' pins
  expect_identical(
    wait_until({
      gc()
      flag$n == 3L
    }),
    TRUE
  )
})

test_that("a collected result's worker-side pin releases to GC", {
  p <- pool_pair()
  tv <- mizu_submit(p[["ctrl"]], runif(100000))
  pool_step(p)
  v <- mizu_collect(tv, 5)
  f <- tfile()
  on.exit(unlink(f), add = TRUE)
  t <- mizu_submit(
    p[["ctrl"]],
    local({
      e <- new.env()
      reg.finalizer(e, function(x) file.create(path), onexit = TRUE)
      list(e, x) # the nested view forces the serialize-hook path: it pins
    }),
    path = f,
    x = v
  )
  pool_step(p)
  invisible(mizu_collect(t, 5)) # delivers a finalizer-free copy
  # the worker's next idle step sweeps the collected result's keeper entry
  expect_identical(
    wait_until({
      pool_step(p)
      gc()
      file.exists(f)
    }),
    TRUE
  )
  pool_end(p)
})

test_that("a serialize-tier result with no nested view pins nothing", {
  p <- pool_pair()
  f <- tfile()
  on.exit(unlink(f), add = TRUE)
  t <- mizu_submit(
    p[["ctrl"]],
    local({
      e <- new.env()
      reg.finalizer(e, function(x) file.create(path), onexit = TRUE)
      function(x) x + 1L # a closure over a local env: the codec declines
    }),
    path = f
  )
  pool_step(p)
  g <- mizu_collect(t, 5)
  expect_identical(g(1L), 2L)
  # no reference rode the result stream, so no pin was taken: the worker's
  # task env finalizes on a plain GC, no keeper sweep (no pool_step) needed
  expect_identical(
    wait_until({
      gc()
      file.exists(f)
    }),
    TRUE
  )
  pool_end(p)
})

test_that("a task returning a closure over a missing namespace collects as globalenv", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], {
    ns <- new.env()
    info <- new.env()
    assign("spec", c(name = "zzmissing", version = "0.0.1"), envir = info)
    assign(".__NAMESPACE__.", info, envir = ns)
    f <- function(x) x + 1
    environment(f) <- ns
    f
  })
  pool_step(p)
  g <- mizu_collect(t, timeout = 5)
  expect_identical(environment(g), globalenv())
  expect_identical(g(1), 2)
  pool_end(p)
})

test_that("stop warns and reports FALSE when workers outlive the wait", {
  p <- pool_pair()
  # the in-process worker cannot exit: the bounded wait must expire
  expect_warning(ok <- mizu_pool_stop(p[["ctrl"]], timeout = 0.2), "timed out")
  expect_false(ok)
  # the worker observes shutdown and leaves on its own path, as worker_main
  expect_identical(.Call(mizu:::mizu_pool_step, p[["wk"]], 0), -1L)
  .Call(mizu:::mizu_pool_leave, p[["wk"]])
})

test_that("mizu_spawn_workers validates n and walks back a failed startup", {
  ctrl <- .Call(mizu:::mizu_pool_create, 2L, 8L, 64L, 64L, 64L, 256L)
  expect_error(mizu_spawn_workers(ctrl, 0L), "at least 1")
  expect_error(mizu_spawn_workers(ctrl, 3L), "not enough free worker slots")
  expect_error(
    mizu_spawn_workers(
      ctrl,
      1L,
      launcher = function(token, slot) NULL,
      startup_timeout = 0.2
    ),
    class = "mizu_error_startup"
  )
  # the slot was never claimed: a later spawn can still take it
  expect_identical(mizu_pool_status(ctrl)[["workers"]], c("free", "free"))
  .Call(mizu:::mizu_pool_destroy, ctrl)
})

test_that("mizu_spawn_workers is controller-only, spawning nothing otherwise (L36)", {
  ctrl <- .Call(mizu:::mizu_pool_create, 2L, 8L, 64L, 64L, 64L, 256L)
  sub <- mizu_pool_attach(mizu_pool_status(ctrl)[["name"]])
  spawned <- FALSE
  expect_snapshot(
    error = TRUE,
    mizu_spawn_workers(
      sub,
      1L,
      launcher = function(token, slot) spawned <<- TRUE,
      startup_timeout = 0.2
    )
  )
  expect_false(spawned)
  expect_identical(mizu_pool_status(ctrl)[["workers"]], c("free", "free"))
  mizu_pool_stop(ctrl, timeout = 0)
})

test_that("worker-only entry points reject unfit handles", {
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 256L)
  expect_error(.Call(mizu:::mizu_pool_set_eval, ctrl), "not a worker handle")
  # a worker that never registered an evaluator refuses to step
  wk <- .Call(
    mizu:::mizu_pool_worker_join,
    .Call(mizu:::mizu_pool_suffix, ctrl),
    0L,
    NULL
  )
  expect_error(.Call(mizu:::mizu_pool_step, wk, 0), "no evaluator registered")
  .Call(mizu:::mizu_pool_leave, wk)
  .Call(mizu:::mizu_pool_destroy, ctrl)
})

test_that("a pool region past the size budget refuses before allocating", {
  expect_error(
    .Call(mizu:::mizu_pool_create, 64L, 64L, 256L, 2^24, 256L, 2^20),
    "pool region too large"
  )
})

test_that("controller-only entries reject worker handles", {
  p <- pool_pair()
  expect_error(
    .Call(mizu:::mizu_pool_destroy, p[["wk"]]),
    "only the controller can destroy"
  )
  expect_error(
    .Call(mizu:::mizu_pool_stop_call, p[["wk"]], 5),
    "only the controller can stop"
  )
  expect_error(
    .Call(mizu:::mizu_pool_retire, p[["wk"]], 0L),
    "only the controller can retire"
  )
  expect_error(
    .Call(mizu:::mizu_pool_ready_wait, p[["wk"]], 0L, 5),
    "only the controller can wait"
  )
  pool_end(p)
})

test_that("worker-only entries reject the controller handle", {
  p <- pool_pair()
  expect_error(
    .Call(mizu:::mizu_pool_leave, p[["ctrl"]]),
    "not a worker handle"
  )
  expect_error(
    .Call(mizu:::mizu_pool_step, p[["ctrl"]], 0),
    "not a worker handle"
  )
  expect_error(
    .Call(mizu:::mizu_pool_deque_pull, p[["ctrl"]], 1L),
    "not a worker handle"
  )
  expect_error(
    .Call(mizu:::mizu_pool_help_once, p[["ctrl"]]),
    "not a worker handle"
  )
  expect_error(
    .Call(mizu:::mizu_pool_map_cache, p[["ctrl"]]),
    "not a worker handle"
  )
  expect_error(
    .Call(mizu:::mizu_pool_run_outcome, p[["ctrl"]], NULL),
    "not a worker handle"
  )
  pool_end(p)
})

test_that("worker wait/retire validate their slot arguments", {
  p <- pool_pair()
  expect_error(
    .Call(mizu:::mizu_pool_ready_wait, p[["ctrl"]], "x", 5),
    "expected worker slot indices"
  )
  expect_error(
    .Call(mizu:::mizu_pool_ready_wait, p[["ctrl"]], 99L, 5),
    "worker slot index out of range"
  )
  expect_error(
    mizu_retire_worker(p[["ctrl"]], 99L),
    "worker slot index out of range"
  )
  pool_end(p)
})

test_that("retiring a departed worker errors", {
  p <- pool_pair()
  mizu_retire_worker(p[["ctrl"]], 0L)
  expect_identical(pool_step(p), -2L)
  .Call(mizu:::mizu_pool_leave, p[["wk"]])
  expect_error(mizu_retire_worker(p[["ctrl"]], 0L), "worker slot 0 is not live")
  pool_end(p)
})

test_that("the map capacity probe requires a live pool and a submitter slot", {
  p <- pool_pair(max_submitters = 1L)
  expect_error(
    .Call(mizu:::mizu_pool_map_caps, p[["wk"]]),
    "submitter registry full"
  )
  pool_end(p)
  p <- pool_pair()
  expect_warning(mizu_pool_stop(p[["ctrl"]], timeout = 0), "timed out")
  # stop releases the controller's handle; a joined worker's lives on and
  # sees the shutdown word
  expect_error(
    .Call(mizu:::mizu_pool_map_caps, p[["wk"]]),
    class = "mizu_error_stopped"
  )
  .Call(mizu:::mizu_pool_leave, p[["wk"]])
})

test_that("collect_any returns the first terminal task with its index", {
  p <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], "one")
  t2 <- mizu_submit(p[["ctrl"]], "two")
  expect_s3_class(mizu_collect_any(list(t1, t2), timeout = 0), "mizu_timeout")
  pool_step(p)
  expect_identical(
    mizu_collect_any(list(t1, t2), timeout = 5),
    list(index = 1L, value = "one")
  )
  # the index is the position in the argument list, not submission order
  pool_step(p)
  t3 <- mizu_submit(p[["ctrl"]], "three")
  expect_identical(
    mizu_collect_any(list(t3, t2), timeout = 5),
    list(index = 2L, value = "two")
  )
  pool_step(p)
  expect_identical(mizu_collect(t3, timeout = 5), "three")
  pool_end(p)
})

test_that("collect_any and collect_all preserve a foreign map error's index", {
  p <- pool_pair()
  # a hand-built remote error carrying the element index, as a pymizu
  # runner's crosses: the ERR flatten keeps classes and named fields
  cond <- structure(
    list(
      message = "boom",
      call = NULL,
      remote_type = "ValueError",
      index = 7L
    ),
    class = c("mizu_error_remote", "mizu_error", "error", "condition")
  )
  t1 <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  cnd <- tryCatch(
    mizu_collect_any(list(t1), timeout = 5),
    mizu_error_remote = function(c) c
  )
  # the position stamp re-homes the element index, not replaces it
  expect_identical(cnd[["index"]], 1L)
  expect_identical(cnd[["mizu_map_index"]], 7L)
  t2 <- mizu_submit(p[["ctrl"]], "ok")
  t3 <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  pool_step(p)
  cnd2 <- tryCatch(
    mizu_collect_all(list(t2, t3), timeout = 5),
    mizu_error_remote = function(c) c
  )
  expect_identical(cnd2[["index"]], 2L)
  expect_identical(cnd2[["mizu_map_index"]], 7L)
  expect_identical(mizu_collect(t2, timeout = 5), "ok")
  pool_end(p)
})

test_that("collect_any and collect_all time out without parking at timeout 0", {
  p <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], 1 + 1)
  t2 <- mizu_submit(p[["ctrl"]], 2 + 2)
  expect_s3_class(mizu_collect_any(list(t1, t2), timeout = 0), "mizu_timeout")
  expect_s3_class(mizu_collect_all(list(t1, t2), timeout = 0), "mizu_timeout")
  pool_step(p)
  pool_step(p)
  expect_identical(mizu_collect(t1, timeout = 5), 2)
  expect_identical(mizu_collect(t2, timeout = 5), 4)
  pool_end(p)
})

test_that("collect_any parks until any result publishes", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], "done")
  expect_s3_class(mizu_collect_any(list(t), timeout = 0.1), "mizu_timeout")
  pool_step(p)
  expect_identical(
    mizu_collect_any(list(t), timeout = 5),
    list(index = 1L, value = "done")
  )
  pool_end(p)
})

test_that("collect_any wakes off the park when a result publishes", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L)
  t1 <- mizu_submit(p, {
    Sys.sleep(1)
    "slow"
  })
  t2 <- mizu_submit(p, {
    Sys.sleep(2)
    "slower"
  })
  expect_identical(
    mizu_collect_any(list(t1, t2), timeout = 10),
    list(index = 1L, value = "slow")
  )
  expect_identical(mizu_collect(t2, timeout = 10), "slower")
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("collect_all wakes off the park as results publish", {
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 1L)
  t1 <- mizu_submit(p, {
    Sys.sleep(0.5)
    1L
  })
  t2 <- mizu_submit(p, {
    Sys.sleep(1)
    2L
  })
  expect_identical(mizu_collect_all(list(t1, t2), timeout = 10), list(1L, 2L))
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("collect_any re-signals a task error with its index", {
  p <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], "ok")
  t2 <- mizu_submit(p[["ctrl"]], stop("boom"))
  pool_step(p)
  pool_step(p)
  err <- tryCatch(
    mizu_collect_any(list(t2, t1), timeout = 5),
    error = identity
  )
  expect_s3_class(err, "simpleError")
  expect_identical(conditionMessage(err), "boom")
  expect_identical(err$index, 1L)
  expect_identical(mizu_collect(t1, timeout = 5), "ok")
  pool_end(p)
})

test_that("collect_any and collect_all attach index to a flattened condition", {
  p <- pool_pair()
  t1 <- mizu_submit(
    p[["ctrl"]],
    stop(structure(
      list(message = "typed", call = NULL, payload = new.env()),
      class = c("mizu_test_error", "error", "condition")
    ))
  )
  t2 <- mizu_submit(p[["ctrl"]], "ok")
  pool_step(p)
  pool_step(p)
  err <- tryCatch(
    mizu_collect_any(list(t1, t2), timeout = 5),
    mizu_test_error = identity
  )
  expect_identical(err[["index"]], 1L)
  expect_identical(err[["dropped_fields"]], "payload")
  expect_identical(mizu_collect(t2, timeout = 5), "ok")

  t3 <- mizu_submit(p[["ctrl"]], "ok")
  t4 <- mizu_submit(
    p[["ctrl"]],
    stop(structure(
      list(message = "typed", call = NULL, payload = new.env()),
      class = c("mizu_test_error", "error", "condition")
    ))
  )
  pool_step(p)
  pool_step(p)
  err <- tryCatch(
    mizu_collect_all(list(t3, t4), timeout = 5),
    mizu_test_error = identity
  )
  expect_identical(err[["index"]], 2L)
  expect_identical(err[["dropped_fields"]], "payload")
  pool_end(p)
})

test_that("collect_any reports a cancellation with its index", {
  p <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], "runs")
  t2 <- mizu_submit(p[["ctrl"]], "never")
  expect_true(mizu_cancel(t2))
  err <- tryCatch(
    mizu_collect_any(list(t1, t2), timeout = 5),
    error = identity
  )
  expect_s3_class(err, "mizu_error_cancelled")
  expect_identical(err$index, 2L)
  pool_step(p)
  expect_identical(mizu_collect(t1, timeout = 5), "runs")
  pool_end(p)
})

test_that("a blocking collect on a cancelled task raises mizu_error_cancelled", {
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], "never")
  expect_true(mizu_cancel(t))
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_error_cancelled")
  expect_identical(
    conditionMessage(err),
    "mizu: task cancelled or pool stopped"
  )
})

test_that("collect_any validates its task list", {
  p <- pool_pair()
  expect_error(mizu_collect_any(list()), "non-empty")
  expect_error(mizu_collect_any(list(1)), "not a task handle")
  p2 <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  t2 <- mizu_submit(p2[["ctrl"]], 2L)
  expect_error(mizu_collect_any(list(t1, t2), timeout = 0), "same pool")
  pool_step(p)
  expect_identical(mizu_collect(t1, timeout = 5), 1L)
  expect_error(mizu_collect_any(list(t1), timeout = 0), "already collected")
  pool_step(p2)
  expect_identical(mizu_collect(t2, timeout = 5), 2L)
  pool_end(p)
  pool_end(p2)
})

test_that("collect_any helps instead of parking in a nested wait", {
  p <- pool_pair()
  a <- mizu_submit(p[["wk"]], "a")
  b <- mizu_submit(p[["wk"]], "b")
  # help mode pops LIFO: b executes first and reports at its position
  expect_identical(
    mizu_collect_any(list(a, b), timeout = 0),
    list(index = 2L, value = "b")
  )
  expect_identical(mizu_collect(a, timeout = 0), "a")
  pool_end(p)
})

test_that("collect_all returns results in input order with names", {
  p <- pool_pair()
  x <- runif(100) # past the inline budget: the spill tier
  tasks <- list(
    nil = mizu_submit(p[["ctrl"]], NULL),
    str = mizu_submit(p[["ctrl"]], "one"),
    vec = mizu_submit(p[["ctrl"]], 1:10),
    inl = mizu_submit(p[["ctrl"]], list(a = 1, b = "x")),
    spl = mizu_submit(p[["ctrl"]], x, x = x)
  )
  for (i in seq_along(tasks)) {
    pool_step(p)
  }
  expect_identical(
    mizu_collect_all(tasks, timeout = 5),
    list(
      nil = NULL,
      str = "one",
      vec = 1:10,
      inl = list(a = 1, b = "x"),
      spl = x
    )
  )
  pool_end(p)
})

test_that("collect_all parks until every result publishes", {
  p <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], "one")
  t2 <- mizu_submit(p[["ctrl"]], "two")
  expect_s3_class(mizu_collect_all(list(t1, t2), timeout = 0), "mizu_timeout")
  pool_step(p)
  # one still pending: the wait runs to the deadline and consumes nothing
  expect_s3_class(mizu_collect_all(list(t1, t2), timeout = 0.1), "mizu_timeout")
  pool_step(p)
  expect_identical(
    mizu_collect_all(list(t1, t2), timeout = 5),
    list("one", "two")
  )
  pool_end(p)
})

test_that("collect_all re-raises the first task error with its index", {
  p <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], "ok")
  t2 <- mizu_submit(p[["ctrl"]], stop("boom"))
  t3 <- mizu_submit(p[["ctrl"]], stop("later boom"))
  pool_step(p)
  pool_step(p)
  pool_step(p)
  err <- tryCatch(
    mizu_collect_all(list(t1, t2, t3), timeout = 5),
    error = identity
  )
  expect_s3_class(err, "simpleError")
  expect_identical(conditionMessage(err), "boom")
  expect_identical(err$index, 2L)
  # only the reported handle is consumed: the earlier result still
  # collects, and later handles stay collectible
  expect_identical(mizu_collect(t1, timeout = 5), "ok")
  err3 <- tryCatch(mizu_collect(t3, timeout = 5), error = identity)
  expect_identical(conditionMessage(err3), "later boom")
  pool_end(p)
})

test_that("collect_all reports a cancellation with its index", {
  p <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], "runs")
  t2 <- mizu_submit(p[["ctrl"]], "never")
  expect_true(mizu_cancel(t2))
  pool_step(p)
  err <- tryCatch(
    mizu_collect_all(list(t1, t2), timeout = 5),
    error = identity
  )
  expect_s3_class(err, "mizu_error_cancelled")
  expect_identical(err$index, 2L)
  pool_end(p)
})

test_that("collect_all validates its task list", {
  p <- pool_pair()
  expect_error(mizu_collect_all(list()), "non-empty")
  expect_error(mizu_collect_all(list(1)), "not a task handle")
  p2 <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  t2 <- mizu_submit(p2[["ctrl"]], 2L)
  expect_error(mizu_collect_all(list(t1, t2), timeout = 0), "same pool")
  pool_step(p)
  expect_identical(mizu_collect_all(list(t1), timeout = 5), list(1L))
  expect_error(mizu_collect_all(list(t1), timeout = 0), "already collected")
  pool_step(p2)
  expect_identical(mizu_collect(t2, timeout = 5), 2L)
  pool_end(p)
  pool_end(p2)
})

test_that("the default pool round-trips, clears, and returns its predecessor", {
  expect_null(mizu_default_pool())
  old <- mizu_set_default_pool(NULL)
  p <- pool_pair()
  expect_invisible(mizu_set_default_pool(p[["ctrl"]]))
  expect_identical(mizu_default_pool(), p[["ctrl"]])
  prev <- mizu_set_default_pool(NULL)
  expect_identical(prev, p[["ctrl"]])
  expect_null(mizu_default_pool())
  pool_end(p)
  mizu_set_default_pool(old)
})

test_that("the default pool validates its type", {
  old <- mizu_set_default_pool(NULL)
  expect_snapshot(mizu_set_default_pool(42), error = TRUE)
  expect_error(mizu_with_pool(42, NULL), "pool handle")
  expect_error(mizu_local_pool(42), "pool handle")
  expect_null(mizu_default_pool())
  mizu_set_default_pool(old)
})

test_that("the default registry anchors the handle past rm() and gc()", {
  old <- mizu_set_default_pool(NULL)
  p <- pool_pair()
  mizu_set_default_pool(p[["ctrl"]])
  p[["ctrl"]] <- NULL
  gc()
  d <- mizu_default_pool()
  expect_s3_class(d, "mizu_pool")
  t <- mizu_submit(d, 1L + 1L)
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), 2L)
  p[["ctrl"]] <- d
  pool_end(p)
  mizu_set_default_pool(old)
})

test_that("a forked process reads the default as unset without clearing it", {
  old <- mizu_set_default_pool(NULL)
  p <- pool_pair()
  mizu_set_default_pool(p[["ctrl"]])
  reg <- mizu:::.default_pool
  pid <- reg$pid
  reg$pid <- pid + 1L
  expect_null(mizu_default_pool())
  expect_identical(reg$pool, p[["ctrl"]])
  reg$pid <- pid
  expect_identical(mizu_default_pool(), p[["ctrl"]])
  pool_end(p)
  mizu_set_default_pool(old)
})

test_that("mizu_with_pool scopes the default and restores the previous one", {
  old <- mizu_set_default_pool(NULL)
  p <- pool_pair()
  q <- pool_pair()
  mizu_set_default_pool(p[["ctrl"]])
  expect_identical(
    mizu_with_pool(q[["ctrl"]], mizu_default_pool()),
    q[["ctrl"]]
  )
  expect_identical(mizu_default_pool(), p[["ctrl"]])
  mizu_with_pool(NULL, expect_null(mizu_default_pool()))
  expect_identical(mizu_default_pool(), p[["ctrl"]])
  expect_error(mizu_with_pool(q[["ctrl"]], stop("boom")), "boom")
  expect_identical(mizu_default_pool(), p[["ctrl"]])
  expect_identical(mizu_with_pool(NULL, 1L + 1L), 2L)
  pool_end(p)
  pool_end(q)
  mizu_set_default_pool(old)
})

test_that("mizu_local_pool restores at the calling frame's exit, LIFO", {
  old <- mizu_set_default_pool(NULL)
  p <- pool_pair()
  q <- pool_pair()
  mizu_set_default_pool(p[["ctrl"]])
  f <- function() {
    prev <- mizu_local_pool(q[["ctrl"]])
    expect_identical(prev, p[["ctrl"]])
    expect_identical(mizu_default_pool(), q[["ctrl"]])
  }
  f()
  expect_identical(mizu_default_pool(), p[["ctrl"]])
  g <- function() {
    mizu_local_pool(q[["ctrl"]])
    mizu_local_pool(NULL)
    expect_null(mizu_default_pool())
  }
  g()
  expect_identical(mizu_default_pool(), p[["ctrl"]])
  h <- function() {
    mizu_local_pool(q[["ctrl"]])
    stop("boom")
  }
  expect_error(h(), "boom")
  expect_identical(mizu_default_pool(), p[["ctrl"]])
  pool_end(p)
  pool_end(q)
  mizu_set_default_pool(old)
})

test_that("collect_any reports completion order across real workers", {
  skip_if_no_child_mizu()
  p <- mizu_pool(2L)
  slow <- mizu_submit(p, {
    Sys.sleep(0.5)
    "slow"
  })
  fast <- mizu_submit(p, "fast")
  expect_identical(
    mizu_collect_any(list(slow, fast), timeout = 30),
    list(index = 2L, value = "fast")
  )
  expect_identical(mizu_collect(slow, timeout = 30), "slow")
  expect_true(mizu_pool_stop(p, timeout = 10))
})

test_that("submit validates the task argument list", {
  p <- pool_pair()
  on.exit(pool_end(p))
  expect_error(
    .Call(mizu:::mizu_pool_submit_expr, p$ctrl, quote(1), "x", 0, 0L),
    "list of task arguments"
  )
})

test_that("submit_call validates the spec shape", {
  p <- pool_pair()
  on.exit(pool_end(p))
  bad <- structure(list("x", "notint", list(), list()), class = "mizu_call")
  expect_error(mizu_submit_call(p$ctrl, bad), "mizu_call")
})

test_that("a map runner submit needs a joined worker", {
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 512L)
  on.exit(.Call(mizu:::mizu_pool_destroy, ctrl))
  expect_error(
    .Call(mizu:::mizu_pool_submit_map_runner, ctrl, list(), Inf),
    "no worker has joined"
  )
})

test_that("pool_run refuses a worker handle with no evaluator", {
  ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 512L)
  on.exit(.Call(mizu:::mizu_pool_destroy, ctrl))
  suffix <- .Call(mizu:::mizu_pool_suffix, ctrl)
  wk <- .Call(mizu:::mizu_pool_worker_join, suffix, 0L, NULL)
  on.exit(.Call(mizu:::mizu_pool_leave, wk), add = TRUE)
  expect_error(.Call(mizu:::mizu_pool_run, wk, 0), "no evaluator registered")
})

test_that("a collected task probes as collected", {
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p$ctrl, 1 + 1)
  pool_step(p)
  expect_identical(mizu_collect(t, 5), 2)
  expect_identical(.Call(mizu:::mizu_pool_task_state, t), "collected")
})

test_that("a map runner submit validates the spec shape", {
  p <- pool_pair()
  on.exit(pool_end(p))
  expect_error(
    .Call(
      mizu:::mizu_pool_submit_map_runner,
      p$ctrl,
      list("x", "g", NULL),
      Inf
    ),
    "malformed map runner spec"
  )
})

test_that("a spec task past the inline budget spills its stream", {
  p <- pool_pair()
  on.exit(pool_end(p))
  x <- runif(13000) # past the zc floor: a recorded candidate
  s <- strrep("x", 1000) # the remainder fits no inline write
  t <- mizu_submit_call(p$ctrl, mizu_call("base::list", x, s))
  pool_step(p)
  expect_identical(mizu_collect(t, 5), list(x, s))
})

test_that("collect_all attaches the consumed prefix to a mid-batch read failure", {
  p <- pool_pair()
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  t2 <- mizu_submit(p[["ctrl"]], list(1, 2))
  pool_step(p)
  pool_step(p)
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  # result slots follow the header, worker/submitter tables, the injection
  # rings, and the worker deques (pool_pair's default geometry)
  off <- 128L + 256L + 8L * 128L + 256L + 8L * (256L + 64L * 512L) + 64L * 512L
  status_at <- function(k) {
    readBin(
      .Call(mizu:::mizu_peek, rw, off + k * 512L, 4L),
      "integer",
      size = 4L,
      endian = "little"
    )
  }
  expect_identical(c(status_at(0L), status_at(1L)), c(2L, 2L)) # both OK
  # t2's result payload reads as a language-private stream at collect
  .Call(mizu:::mizu_poke, rw, off + 512L + 40L, charToRaw("P"))
  cnd <- tryCatch(
    mizu_collect_all(list(t1, t2), timeout = 5),
    error = function(c) c
  )
  expect_match(conditionMessage(cnd), "cannot interpret")
  expect_identical(cnd[["index"]], 2L)
  expect_identical(cnd[["values"]], list(1L))
  pool_end(p)
})

test_that("corrupt rawspill result slots reject at collect", {
  off <- 128L + 256L + 8L * 128L + 256L + 8L * (256L + 64L * 512L) + 64L * 512L
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], as.raw(rep(1L, 10000)))
  pool_step(p)
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  .Call(mizu:::mizu_poke, rw, off + 24L + 8L, raw(8L)) # aux: a zero wire type
  expect_error(mizu_collect(t, 5), "corrupt payload slot")
  pool_end(p)

  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], as.raw(rep(1L, 10000)))
  pool_step(p)
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  .Call(
    # the byte count outruns its region
    mizu:::mizu_poke,
    rw,
    off + 24L + 4L,
    writeBin(2L^30, raw(), size = 4L, endian = "little")
  )
  expect_error(mizu_collect(t, 5), "corrupt payload slot")
  pool_end(p)
})

test_that("the worker run loop reports its exit code", {
  p <- pool_pair()
  .Call(mizu:::mizu_pool_destroy, p[["ctrl"]])
  expect_identical(.Call(mizu:::mizu_pool_run, p[["wk"]], 0), -1L)
  .Call(mizu:::mizu_pool_leave, p[["wk"]])

  p <- pool_pair()
  mizu_retire_worker(p[["ctrl"]], 0L)
  expect_identical(.Call(mizu:::mizu_pool_run, p[["wk"]], 0), -2L)
  .Call(mizu:::mizu_pool_leave, p[["wk"]])
  .Call(mizu:::mizu_pool_destroy, p[["ctrl"]])

  p <- pool_pair()
  expect_identical(.Call(mizu:::mizu_pool_run_outcome, p[["wk"]], -1L), -1L)
  pool_end(p)
})

test_that("a name-kind spec accepts an unlisted worker language at submit", {
  # the exec-side check then refuses the mismatched target
  p <- pool_pair(ident = c(9L, 0L))
  t <- mizu_submit_call(p[["ctrl"]], mizu_call("base::sqrt", 16))
  pool_step(p)
  cnd <- tryCatch(mizu_collect(t, 5), error = function(c) c)
  expect_s3_class(cnd, "mizu_error_remote")
  expect_match(conditionMessage(cnd), "language mismatch")
  pool_end(p)
})

test_that("the lame-duck probe answers on live and dead handles", {
  p <- pool_pair()
  # a non-worker probe answers done without releasing the handle — only the
  # controller's destroy unlinks the region and liveness files
  expect_type(.Call(mizu:::mizu_pool_lame_duck, p[["ctrl"]]), "logical")
  pool_end(p)
  expect_true(.Call(mizu:::mizu_pool_lame_duck, p[["ctrl"]]))
})

test_that("a malformed ref identifier in a result fails the read", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], 1L)
  pool_step(p)
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p$ctrl)[["name"]],
    TRUE
  )
  off <- 128L + 256L + 8L * 128L + 256L + 8L * (256L + 64L * 512L) + 64L * 512L
  # rewrite slot 0's payload as a 0x13 ref leaf with a malformed identifier
  b <- ix_hex_to_raw(ix_corpus()[["err-ref-value"]])
  .Call(mizu:::mizu_poke, rw, off + 24L, raw(4L)) # kind INLINE
  .Call(
    mizu:::mizu_poke,
    rw,
    off + 24L + 4L,
    writeBin(length(b), raw(), size = 4L, endian = "little")
  )
  .Call(mizu:::mizu_poke, rw, off + 40L, b)
  expect_error(mizu_collect(t, 5), "malformed interop ref identifier")
  pool_end(p)
})

test_that("a task error with a non-language call flattens without detail text", {
  p <- pool_pair()
  t <- mizu_submit(
    p[["ctrl"]],
    stop(structure(
      list(message = "boom", call = 42L),
      class = c("simpleError", "error", "condition")
    ))
  )
  pool_step(p)
  cnd <- tryCatch(mizu_collect(t, 5), error = function(c) c)
  expect_match(conditionMessage(cnd), "boom")
  pool_end(p)
})

test_that("create rejects an oversized geometry and a bad liveness directory", {
  expect_error(
    .Call(
      mizu:::mizu_pool_create,
      1L,
      64L,
      16777216L,
      16777216L,
      16777216L,
      1048576L
    ),
    "pool region too large"
  )
  old <- Sys.getenv("MIZU_LIVENESS_DIR", unset = NA)
  on.exit(
    if (is.na(old)) {
      Sys.unsetenv("MIZU_LIVENESS_DIR")
    } else {
      Sys.setenv(MIZU_LIVENESS_DIR = old)
    },
    add = TRUE
  )
  Sys.setenv(MIZU_LIVENESS_DIR = paste(rep("a", 950L), collapse = ""))
  expect_error(
    .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 512L),
    "liveness directory path too long"
  )
  Sys.setenv(MIZU_LIVENESS_DIR = file.path(tempdir(), "no-such-dir"))
  expect_error(
    .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 512L),
    "cannot create liveness file"
  )
})

test_that("attach rejects a corrupt pool header", {
  p <- pool_pair()
  on.exit(pool_end(p))
  suffix <- .Call(mizu:::mizu_pool_suffix, p[["ctrl"]])
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  poke_u32 <- function(off, v) {
    old <- .Call(mizu:::mizu_peek, rw, off, 4L)
    .Call(
      mizu:::mizu_poke,
      rw,
      off,
      writeBin(v, raw(), size = 4L, endian = "little")
    )
    expect_error(mizu_pool_attach(suffix), pat)
    .Call(mizu:::mizu_poke, rw, off, old)
  }
  pat <- "bad magic"
  poke_u32(0L, 0L)
  pat <- "ABI version mismatch"
  poke_u32(4L, 999L)
  pat <- "registry capacities out of range"
  poke_u32(8L, 0L)
  pat <- "queue capacities or slot size are not valid powers of two"
  poke_u32(16L, 3L)
  pat <- "result slots are out of range"
  poke_u32(24L, 3L)
  pat <- "pool sections exceed the mapped region"
  poke_u32(8L, 64L)
  # livedir_size (u64 at 48): an implausible span outside the region
  old <- .Call(mizu:::mizu_peek, rw, 48L, 8L)
  .Call(
    mizu:::mizu_poke,
    rw,
    48L,
    writeBin(c(-1L, -1L), raw(), size = 4L, endian = "little")
  )
  expect_error(mizu_pool_attach(suffix), "liveness-dir string")
  .Call(mizu:::mizu_poke, rw, 48L, old)
})

test_that("retire and ready_wait validate their worker slot", {
  ctrl <- .Call(mizu:::mizu_pool_create, 2L, 8L, 64L, 64L, 64L, 512L)
  on.exit(.Call(mizu:::mizu_pool_destroy, ctrl))
  expect_error(
    .Call(mizu:::mizu_pool_retire, ctrl, 2L),
    "worker slot index out of range"
  )
  expect_error(
    .Call(mizu:::mizu_pool_retire, ctrl, 0L),
    "worker slot 0 is not live"
  )
  expect_error(
    .Call(mizu:::mizu_pool_ready_wait, ctrl, 2L, 0),
    "worker slot index out of range"
  )
})

test_that("a second join of a held worker slot fails", {
  p <- pool_pair()
  on.exit(pool_end(p))
  expect_error(
    .Call(
      mizu:::mizu_pool_worker_join,
      .Call(mizu:::mizu_pool_suffix, p[["ctrl"]]),
      0L,
      NULL
    ),
    "already held"
  )
})

test_that("claim copies tolerate torn entry kind bytes", {
  p <- pool_pair()
  on.exit(pool_end(p))
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  base <- 128L + 256L + 8L * 128L + 256L + 256L
  kinds <- raw(4L)
  for (i in 0:3) {
    mizu_submit(p[["ctrl"]], i)
    kinds[i + 1L] <- .Call(mizu:::mizu_peek, rw, base + i * 512L + 24L, 1L)
  }
  # NIL: the copy is the header only
  .Call(mizu:::mizu_poke, rw, base + 24L, as.raw(6L))
  # RAWSPILL: the framed name length rides aux >> 8
  .Call(mizu:::mizu_poke, rw, base + 512L + 24L, as.raw(8L))
  .Call(mizu:::mizu_poke, rw, base + 512L + 33L, as.raw(16L))
  # an over-long RAWSPILL name and an unknown kind take the full slot
  .Call(mizu:::mizu_poke, rw, base + 2L * 512L + 24L, as.raw(8L))
  .Call(mizu:::mizu_poke, rw, base + 2L * 512L + 34L, as.raw(64L))
  .Call(mizu:::mizu_poke, rw, base + 3L * 512L + 24L, as.raw(238L))
  expect_identical(pool_pull(p, 4L), 4L)
  for (i in 0:3) {
    .Call(mizu:::mizu_poke, rw, base + i * 512L + 24L, kinds[i + 1L])
  }
})

test_that("a reaping worker's deque stays visible until drained", {
  p <- pool_pair(workers = 2L)
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], 1 + 1)
  pool_pull(p, 1L, wk = p[["wks"]][[1L]])
  .Call(mizu:::mizu_pool_leave, p[["wks"]][[1L]])
  # the orphaned deque is help-unreachable: the survivor's work hints keep
  # it spinning until the fairness tick claims the entry
  expect_identical(pool_step(p, timeout = 10, wk = p[["wks"]][[2L]]), 1L)
  expect_identical(mizu_collect(t, 5), 2)
})

test_that("a lost task reports its state before collect", {
  p <- pool_pair()
  on.exit(pool_end(p))
  x <- paste(rep("x", 3000), collapse = "")
  t <- mizu_submit(p[["ctrl"]], x, x = x)
  rw <- .Call(
    mizu:::mizu_region_open,
    mizu_pool_status(p[["ctrl"]])[["name"]],
    TRUE
  )
  base <- 128L + 256L + 8L * 128L + 256L + 256L
  id <- .Call(mizu:::mizu_peek, rw, base + 40L, 40L)
  hex0 <- base + 40L + which(id == charToRaw("_"))[1L] + 1L
  b <- .Call(mizu:::mizu_peek, rw, hex0, 1L)
  # a well-formed name that does not exist (one hex digit flipped)
  .Call(
    mizu:::mizu_poke,
    rw,
    hex0,
    if (b == charToRaw("a")) charToRaw("b") else charToRaw("a")
  )
  pool_step(p)
  expect_identical(.Call(mizu:::mizu_pool_task_state, t), "lost")
  expect_error(mizu_collect(t, 5), "payload region vanished")
})

test_that("the task finalizer leaves a cancelled slot alone", {
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], 1 + 1)
  expect_true(mizu_cancel(t))
  rm(t)
  gc()
  expect_identical(
    mizu_pool_status(p[["ctrl"]])[["tasks"]][["cancel"]],
    1L
  )
})
