test_that("mizu_call builds name and source specs", {
  spec <- mizu_call("stats::quantile", 1:10, probs = c(0.25, 0.75))
  expect_true(inherits(spec, "mizu_call"))
  expect_identical(spec$code, "stats::quantile")
  expect_identical(spec$kind, 0L)
  expect_identical(spec$positional, list(1:10))
  expect_identical(spec$named, list(probs = c(0.25, 0.75)))
  src <- mizu_call(.source = "x + 1", x = 41L)
  expect_identical(src$kind, 1L)
  expect_identical(src$named, list(x = 41L))
  expect_identical(src$positional, list())
  expect_error(mizu_call(), "exactly one")
  expect_error(mizu_call("f", .source = "x"), "exactly one")
  expect_error(mizu_call(1L), "single string")
})

test_that("spec constant names never collide with the formals ahead of ...", {
  spec <- mizu_call("fn", n = 3L)
  expect_identical(spec$code, "fn")
  expect_identical(spec$named, list(n = 3L))
  src <- mizu_call(.source = "x", source = 1L)
  expect_identical(src$code, "x")
  expect_identical(src$named, list(source = 1L))
})

test_that("spec tasks run on a same-language pool, errors stay rich", {
  p <- mizu_pool(1L)
  on.exit(mizu_pool_stop(p))
  t <- mizu_submit_call(
    p,
    mizu_call("stats::quantile", 1:100, probs = c(0.25, 0.75), names = FALSE)
  )
  expect_equal(mizu_collect(t), c(25.75, 75.25))
  s <- mizu_submit_call(p, mizu_call(.source = "y <- x * 2\ny + 1", x = 20))
  expect_identical(mizu_collect(s), 41)
  e <- mizu_submit_call(p, mizu_call("base::log", "x"))
  cnd <- tryCatch(mizu_collect(e), error = function(c) c)
  expect_false(inherits(cnd, "mizu_error_remote"))
  expect_match(conditionMessage(cnd), "non-numeric")
})

test_that("a pool no worker has joined: spec and map error, plain submit queues", {
  ctrl <- .Call(
    mizu:::mizu_pool_create,
    1L,
    8L,
    64L,
    64L,
    64L,
    512L
  )
  suffix <- .Call(mizu:::mizu_pool_suffix, ctrl)
  att <- .Call(mizu:::mizu_pool_attach_call, suffix)
  spec <- mizu_call("base::sqrt", 2)
  expect_error(mizu_submit_call(ctrl, spec), "no worker has joined")
  expect_error(mizu_submit_call(att, spec), "no worker has joined")
  expect_error(mizu_map(ctrl, 1:3, identity), "no worker has joined")
  expect_error(mizu_map(ctrl, 1:3, spec), "no worker has joined")
  expect_error(mizu_map_prepare(ctrl, 1:3, identity), "no worker has joined")
  t <- mizu_submit(ctrl, 1 + 1)
  wk <- .Call(mizu:::mizu_pool_worker_join, suffix, 0L, NULL)
  .Call(mizu:::mizu_pool_set_eval, wk)
  p <- list(ctrl = ctrl, wk = wk, wks = list(wk))
  t2 <- mizu_submit_call(ctrl, spec)
  t3 <- mizu_submit_call(att, spec)
  pool_step(p)
  pool_step(p)
  pool_step(p)
  expect_equal(mizu_collect(t), 2)
  expect_equal(mizu_collect(t2), sqrt(2))
  expect_equal(mizu_collect(t3), sqrt(2))
  pool_end(p)
})

test_that("private verbs fail fast on a foreign pool", {
  p <- pool_pair(ident = c(3L, 7L))
  expect_error(mizu_submit(p$ctrl, 1 + 1), "mizu_submit_call")
  expect_error(
    mizu_submit_batch(p$ctrl, list(quote(1 + 1))),
    "mizu_submit_call"
  )
  expect_error(mizu_map(p$ctrl, 1:3, identity), "mizu_call")
  expect_error(mizu_map_prepare(p$ctrl, 1:3, identity), "mizu_call")
  d <- mizu_pool_dump(p$ctrl)
  expect_identical(d[["language"]], "Python")
  expect_identical(d[["capabilities"]], 7L)
  pool_end(p)
})

test_that("a task targeting another language fails with a mismatch", {
  p <- pool_pair(ident = c(3L, 7L))
  t <- mizu_submit_call(p$ctrl, mizu_call("builtins.len", 1:3))
  pool_step(p)
  cnd <- tryCatch(mizu_collect(t), mizu_error_remote = function(c) c)
  expect_s3_class(cnd, "mizu_error_remote")
  expect_match(conditionMessage(cnd), "task language mismatch")
  pool_end(p)
})

test_that("the qualifier check is syntax-shaped per worker language", {
  p <- pool_pair()
  expect_error(
    mizu_submit_call(p$ctrl, mizu_call("length", 1:3)),
    "qualified name"
  )
  pool_end(p)
  p <- pool_pair(ident = c(3L, 7L))
  expect_error(
    mizu_submit_call(p$ctrl, mizu_call("stats::quantile", 1:3)),
    "qualified name"
  )
  expect_error(
    mizu_submit_call(p$ctrl, mizu_call("numpy.mean", environment())),
    class = "mizu_error_not_portable"
  )
  pool_end(p)
})

test_that("foreign-submitter tasks: results, err streams, the result gate", {
  p <- pool_pair()
  submit <- function(spec, ident) {
    .Call(mizu:::mizu_pool_submit_spec, p$ctrl, spec, Inf, ident)
  }
  t1 <- submit(mizu_call("base::sqrt", 16), c(3L, 7L))
  t2 <- submit(mizu_call(.source = "stop(\"boom\")"), c(3L, 7L))
  t3 <- submit(mizu_call(.source = "lm(mpg ~ wt, mtcars)"), c(3L, 7L))
  t4 <- submit(mizu_call("base::sqrt", 25), c(3L, 15L))
  pool_step(p)
  pool_step(p)
  pool_step(p)
  pool_step(p)
  expect_identical(mizu_collect(t1), 4)
  c2 <- tryCatch(mizu_collect(t2), mizu_error_remote = function(c) c)
  expect_s3_class(c2, "mizu_error_remote")
  expect_equal(c2$remote_type, "simpleError")
  expect_match(c2$message, "boom")
  c3 <- tryCatch(mizu_collect(t3), mizu_error_remote = function(c) c)
  expect_s3_class(c3, "mizu_error_remote")
  expect_equal(c3$remote_type, "mizu_error_not_portable")
  expect_match(c3$message, "not portable")
  # unknown capability bits in the submitter identity are ignored
  expect_identical(mizu_collect(t4), 5)
  pool_end(p)
})

test_that("a large foreign result is a view only when caps admit", {
  p <- pool_pair()
  x <- strrep("abcdefghij", 10000)
  spec <- mizu_call(.source = "strrep(\"abcdefghij\", 10000)")
  t1 <- .Call(mizu:::mizu_pool_submit_spec, p$ctrl, spec, Inf, c(3L, 7L))
  t2 <- .Call(mizu:::mizu_pool_submit_spec, p$ctrl, spec, Inf, c(3L, 0L))
  pool_step(p)
  pool_step(p)
  v1 <- mizu_collect(t1)
  v2 <- mizu_collect(t2)
  expect_true(.Call(mizu:::mizu_zc_view_check, v1))
  expect_false(.Call(mizu:::mizu_zc_view_check, v2))
  expect_identical(v1, x)
  expect_identical(v2, x)
  pool_end(p)
})

test_that("a foreign private frame gets the neutral err stream", {
  p <- pool_pair()
  t1 <- .Call(mizu:::mizu_pool_submit, p$ctrl, charToRaw("Ptask"), Inf, 0L)
  t2 <- .Call(
    mizu:::mizu_pool_submit,
    p$ctrl,
    as.raw(c(0x80, 0x04, 0x00)),
    Inf,
    0L
  )
  pool_step(p)
  pool_step(p)
  c1 <- tryCatch(mizu_collect(t1), mizu_error_remote = function(c) c)
  c2 <- tryCatch(mizu_collect(t2), mizu_error_remote = function(c) c)
  expect_s3_class(c1, "mizu_error_remote")
  expect_s3_class(c2, "mizu_error_remote")
  expect_match(c1$message, "foreign private codec")
  expect_match(c2$message, "foreign private codec")
  pool_end(p)
})

test_that("a wrong-shape task stream fails naming the shape", {
  p <- pool_pair()
  # the corpus fixture with the target and submitter bytes patched to
  # R workers and a Python submitter
  b <- ix_hex_to_raw(ix_corpus()[["err-taskdec-shape"]])
  b[4L] <- as.raw(2L)
  b[8L] <- as.raw(3L)
  t <- .Call(mizu:::mizu_pool_submit, p$ctrl, b, Inf, 0L)
  pool_step(p)
  cnd <- tryCatch(mizu_collect(t), mizu_error_remote = function(c) c)
  expect_s3_class(cnd, "mizu_error_remote")
  expect_match(cnd$message, "the code field is not a string")
  pool_end(p)
})

test_that("a valid foreign task stream executes (corpus bytes)", {
  p <- pool_pair()
  b <- ix_hex_to_raw(ix_corpus()[["task-name-py2r"]])
  t <- .Call(mizu:::mizu_pool_submit, p$ctrl, b, Inf, 0L)
  pool_step(p)
  expect_identical(mizu_collect(t), c(1.5, 2, 2.5))
  pool_end(p)
})

test_that("a foreign task nests a same-language submit", {
  p <- pool_pair()
  src <- paste0(
    "t <- mizu_submit(mizu_current_pool(), x * 2, x = 21L)\n",
    "mizu_collect(t)"
  )
  t <- .Call(
    mizu:::mizu_pool_submit_spec,
    p$ctrl,
    mizu_call(.source = src),
    Inf,
    c(3L, 7L)
  )
  pool_step(p)
  expect_identical(mizu_collect(t), 42)
  pool_end(p)
})

test_that("collect_any and collect_all cross languages", {
  p <- pool_pair()
  submit <- function(spec) {
    .Call(mizu:::mizu_pool_submit_spec, p$ctrl, spec, Inf, c(3L, 7L))
  }
  t1 <- submit(mizu_call(.source = "1 + 1"))
  t2 <- submit(mizu_call(.source = "stop(\"boom\")"))
  pool_step(p)
  pool_step(p)
  r <- mizu_collect_any(list(t1, t2))
  expect_identical(r[["index"]], 1L)
  expect_identical(r[["value"]], 2)
  t3 <- submit(mizu_call(.source = "1 + 1"))
  t4 <- submit(mizu_call(.source = "stop(\"boom\")"))
  pool_step(p)
  pool_step(p)
  cnd <- tryCatch(
    mizu_collect_all(list(t3, t4)),
    mizu_error_remote = function(c) c
  )
  expect_s3_class(cnd, "mizu_error_remote")
  expect_equal(cnd[["index"]], 2L)
  expect_identical(mizu_collect(t3), 2)
  pool_end(p)
})

test_that("task arguments cross by reference: the SHM_VEC stage and the REF re-send", {
  p <- pool_pair()
  on.exit(pool_end(p))
  big <- runif(200000L) # 1.6 MB — past the zero-copy floor
  t1 <- mizu_submit_call(p$ctrl, mizu_call("base::mean", big))
  pool_step(p)
  expect_equal(mizu_collect(t1), mean(big))

  # a received view re-sent as an argument: REFHELD set, the loan balanced
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  mizu_send(ch$host, big)
  xv <- mizu_recv(ch$peer, Inf)
  expect_true(.Call(mizu:::mizu_zc_view_check, xv))
  rc0 <- .Call(mizu:::mizu_zc_refcount, xv)
  t2 <- mizu_submit_call(p$ctrl, mizu_call("base::mean", xv))
  pool_step(p)
  expect_equal(mizu_collect(t2), mean(big))
  rc1 <- .Call(mizu:::mizu_zc_refcount, xv)
  expect_identical(rc1[2L], 1L) # REFHELD
  expect_identical(rc1[1L], rc0[1L])
  channel_end(ch)
})

test_that("multiple ref args and a nested view cross, loans balanced", {
  p <- pool_pair()
  on.exit(pool_end(p))
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  a <- runif(100000L)
  b <- runif(100000L)
  mizu_send(ch$host, a)
  va <- mizu_recv(ch$peer, Inf)
  mizu_send(ch$host, b)
  vb <- mizu_recv(ch$peer, Inf)
  rc0a <- .Call(mizu:::mizu_zc_refcount, va)[1L]
  rc0b <- .Call(mizu:::mizu_zc_refcount, vb)[1L]
  # two positional, a named, and views nested inside list args
  t <- mizu_submit_call(
    p$ctrl,
    mizu_call(
      NULL,
      va,
      vb,
      x = list(va),
      y = vb,
      .source = "gc(); sum(..1) + sum(..2) + sum(x[[1]]) + sum(y)"
    )
  )
  pool_step(p)
  expect_equal(mizu_collect(t), sum(a) + sum(b) + sum(a) + sum(b))
  expect_identical(.Call(mizu:::mizu_zc_refcount, va)[1L], rc0a)
  expect_identical(.Call(mizu:::mizu_zc_refcount, vb)[1L], rc0b)
  channel_end(ch)
})

test_that("a list-tree view arg resolves to the element view", {
  p <- pool_pair()
  on.exit(pool_end(p))
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  big <- replicate(2000L, runif(100L), simplify = FALSE)
  mizu_send(ch$host, big)
  lv <- mizu_recv(ch$peer, Inf)
  expect_true(.Call(mizu:::mizu_zc_view_check, lv))
  ev <- lv[[2L]]
  expect_true(.Call(mizu:::mizu_zc_view_check, ev))
  expect_true(grepl("[", .Call(mizu:::mizu_zc_view_name, ev), fixed = TRUE))
  t <- mizu_submit_call(p$ctrl, mizu_call("base::mean", ev))
  pool_step(p)
  expect_equal(mizu_collect(t), mean(big[[2L]]))
  channel_end(ch)
})

test_that("result-is-the-arg keeps the loan through the publish (D5)", {
  p <- pool_pair()
  on.exit(pool_end(p))
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  big <- runif(100000L)
  mizu_send(ch$host, big)
  xv <- mizu_recv(ch$peer, Inf)
  rc0 <- .Call(mizu:::mizu_zc_refcount, xv)
  t <- mizu_submit_call(p$ctrl, mizu_call("base::identity", xv))
  pool_step(p)
  res <- mizu_collect(t)
  expect_true(.Call(mizu:::mizu_zc_view_check, res))
  # the write emitted the ref, so the view kept its loan; the count
  # elevates by the submitter's own add — no zero window (the worker's
  # result pin drops at its next keeper sweep, the view's at GC)
  pool_step(p) # the empty step drives the idle sweep
  gc()
  expect_identical(.Call(mizu:::mizu_zc_refcount, res)[1L], rc0[1L] + 1L)
  expect_identical(res, big)
  channel_end(ch)
})

test_that("the emitted set keeps loans: attribute and condition-field probes", {
  p <- pool_pair()
  on.exit(pool_end(p))
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  big <- runif(100000L)
  mizu_send(ch$host, big)
  xv <- mizu_recv(ch$peer, Inf)
  rc0 <- .Call(mizu:::mizu_zc_refcount, xv)

  # the argument view rides the result in an attribute: the attr blob's
  # emission keeps the loan — the view arrives intact, refcounts balanced
  t1 <- mizu_submit_call(
    p$ctrl,
    mizu_call(NULL, xv, .source = "structure(list(42), payload = ..1)")
  )
  pool_step(p)
  r1 <- mizu_collect(t1)
  pool_step(p) # the empty step drives the idle sweep
  gc()
  expect_identical(.Call(mizu:::mizu_zc_refcount, xv)[1L], rc0[1L] + 1L)
  expect_identical(attr(r1, "payload"), big)

  # the argument view rides a raised condition's field: the flatten's
  # codec gate drops it, and the release pass frees the loan anyway (the
  # baseline re-reads the count: the collected r1 view above lives on)
  rc0 <- .Call(mizu:::mizu_zc_refcount, xv)
  t2 <- mizu_submit_call(
    p$ctrl,
    mizu_call(
      NULL,
      xv,
      .source = paste(
        "stop(structure(list(message = \"boom\", payload = ..1),",
        "class = c(\"myerr\", \"error\", \"condition\")))"
      )
    )
  )
  pool_step(p)
  c2 <- tryCatch(mizu_collect(t2), error = function(e) e)
  expect_match(conditionMessage(c2), "boom")
  expect_true("payload" %in% c2[["dropped_fields"]])
  pool_step(p) # the empty step drives the idle sweep
  gc()
  expect_identical(.Call(mizu:::mizu_zc_refcount, xv)[1L], rc0[1L])
  channel_end(ch)
})

test_that("a ref candidate to a pool without TASKREF declines locally", {
  p <- pool_pair(ident = c(2L, 7L)) # R workers without the ref reader
  on.exit(pool_end(p))
  big <- runif(200000L)
  expect_snapshot(
    mizu_submit_call(p$ctrl, mizu_call("base::mean", big)),
    error = TRUE
  )
  # an ordinary argument submits unchanged
  t <- mizu_submit_call(p$ctrl, mizu_call("base::mean", 1:10))
  pool_step(p)
  expect_equal(mizu_collect(t), 5.5)
})

test_that("a view whose layout the peer cannot read crosses by value", {
  p <- pool_pair(ident = c(2L, 13L)) # TASKREF without ATTRS
  on.exit(pool_end(p))
  m <- matrix(runif(200000L), nrow = 400L)
  t <- mizu_submit_call(p$ctrl, mizu_call("base::mean", m))
  pool_step(p)
  expect_equal(mizu_collect(t), mean(m))
})

test_that("a ref to a vanished region fails the task informatively", {
  p <- pool_pair()
  on.exit(pool_end(p))
  id <- if (.Platform[["OS.type"]] == "windows") {
    "Local\\\\mizu_0_0" # notation-escaped; parses to Local\mizu_0_0
  } else {
    "/mizu_0_0"
  }
  x <- ix_parse(paste0(
    "task(2, 0, 30064771075, str(\"base::mean\"), ",
    "list(ref(\"",
    id,
    "\")), dict())"
  ))
  # a hand-crafted task stream rides a RAWVEC entry to the worker's
  # task-stream dispatch
  buf <- ix_hex_to_raw(ix_write_task(x))
  t <- .Call(mizu:::mizu_pool_submit_try, p$ctrl, buf, Inf, 0L)
  pool_step(p)
  cnd <- tryCatch(mizu_collect(t), mizu_error_remote = function(c) c)
  expect_s3_class(cnd, "mizu_error_remote")
  expect_match(cnd[["message"]], "not found")
})

test_that("the no_zc degrade writes a large argument by value", {
  big <- runif(100000L)
  x <- list(
    target = 2L,
    kind = 0L,
    ident = 30064771075,
    code = "base::mean",
    positional = list(big),
    named = list()
  )
  # without a live stage there is no checkout either way — by value
  expect_identical(ix_write_task(x, no_zc = TRUE), ix_write_task(x))
})

test_that("a view x crosses as a ref on a spec map, read off the pages (D6)", {
  p <- pool_pair()
  on.exit(pool_end(p))
  big <- runif(100000L)
  ch <- channel_pair(capacity = 128L, slot_size = 512L, arena_size = 8192)
  mizu_send(ch$host, big)
  xv <- mizu_recv(ch$peer, Inf)
  rc0 <- .Call(mizu:::mizu_zc_refcount, xv)
  st <- mizu:::map_stage(p$ctrl, xv, mizu_call("base::sqrt"), list())
  mizu:::map_submit(p$ctrl, st)
  while (pool_step(p) == 1L) {
    NULL
  }
  res <- mizu:::map_collect(st, deadline = mizu:::mono_time() + 30)
  expect_equal(unlist(res), sqrt(big))
  rc1 <- .Call(mizu:::mizu_zc_refcount, xv)
  expect_identical(rc1[2L], 1L) # REFHELD
  # the worker's map-context view releases at the idle sweep
  expect_true(wait_until({
    gc()
    .Call(mizu:::mizu_zc_refcount, xv)[1L] == rc0[1L]
  }))
  channel_end(ch)
})

test_that("mizu_submit_call validates the spec and the name shape", {
  expect_error(mizu_submit_call(NULL, list()), "must be a mizu_call")
  expect_error(mizu_submit_call(NULL, "base::sqrt"), "must be a mizu_call")
  p <- pool_pair()
  expect_error(
    mizu_submit_call(p$ctrl, mizu_call("base::1bad", 1:3)),
    "qualified name"
  )
  pool_end(p)
})
