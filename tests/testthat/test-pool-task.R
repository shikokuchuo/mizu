# Task flow over the in-process harness: submit into the injection ring,
# single-step worker execution, result publication and collect, the cancel
# state machine, and the payload-lifetime keeper discipline. Every wait here
# is deterministic — the worker runs only when the test steps it.

test_that("a task round-trips every result payload kind", {
  p <- pool_pair()
  tasks <- list(
    nil = mizu_submit(p[["ctrl"]], NULL), # NIL
    raw = mizu_submit(p[["ctrl"]], x * 2L, x = 21L), # RAWVEC
    str = mizu_submit(p[["ctrl"]], paste0("task-", x), x = 1), # STR1
    na_str = mizu_submit(p[["ctrl"]], NA_character_), # STR1 (NA)
    inline = mizu_submit(p[["ctrl"]], list(a = x, b = "y"), x = 1), # INLINE
    shm = mizu_submit(p[["ctrl"]], seq_len(n) + 0, n = 100000L) # SHM_RAW
  )
  expect_identical(mizu_pool_status(p[["ctrl"]])[["injection"]], 6)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_null(mizu_collect(tasks[["nil"]], timeout = 5))
  expect_identical(mizu_collect(tasks[["raw"]], timeout = 5), 42L)
  expect_identical(mizu_collect(tasks[["str"]], timeout = 5), "task-1")
  expect_identical(mizu_collect(tasks[["na_str"]], timeout = 5), NA_character_)
  expect_identical(
    mizu_collect(tasks[["inline"]], timeout = 5),
    list(a = 1, b = "y")
  )
  expect_identical(
    mizu_collect(tasks[["shm"]], timeout = 5),
    as.double(seq_len(100000L))
  )
  pool_end(p)
})

test_that("codec streams carry task args and attributed results", {
  p <- pool_pair()
  # named args cross as a codec stream (the envelope is a named list)
  t <- mizu_submit(p[["ctrl"]], x + 1, x = c(a = 1, b = 2))
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), c(a = 2, b = 3))
  # an attributed result crosses as a codec stream and needs no keeper wake
  t <- mizu_submit(p[["ctrl"]], factor(rep(x, 2L)), x = c("a", "b"))
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), factor(rep(c("a", "b"), 2L)))
  # a primitive arg crosses by name on the codec
  t <- mizu_submit(p[["ctrl"]], f(x), f = sum, x = c(1, 2, 3))
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), 6)
  # a codec-ineligible arg (a closure) falls back to R_Serialize inline
  t <- mizu_submit(p[["ctrl"]], f(3L), f = function(x) x * 2L)
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), 6L)
  pool_end(p)
})

test_that("mid-size vector results spill as bare bytes (RAWSPILL)", {
  p <- pool_pair() # 256 B slots: a 4 KB result spills to a region
  t <- mizu_submit(p[["ctrl"]], x * 2, x = seq_len(500L) + 0)
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), (seq_len(500L) + 0) * 2)
  # recycled region, same discipline: a second equal-size spill pops it
  t2 <- mizu_submit(p[["ctrl"]], x + 1, x = seq_len(500L) + 0)
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), seq_len(500L) + 1)
  pool_end(p)
})

test_that("task arguments arrive as the only bindings", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], sort(ls(environment())), a = 1, b = 2)
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), c("a", "b"))
  expect_error(mizu_submit(p[["ctrl"]], x + 1, 5), "must be named")
  pool_end(p)
})

test_that("large task payloads travel by region and round-trip", {
  p <- pool_pair() # default 256 B slots: this spills to SHM_RAW
  big <- runif(100000)
  t <- mizu_submit(p[["ctrl"]], sum(v), v = big)
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), sum(big))
  pool_end(p)
})

test_that("large inline entries survive ring and deque claims", {
  p <- pool_pair(
    max_submitters = 1L,
    injection_cap = 2L,
    per_worker_cap = 2L,
    result_slots = 2L,
    slot_size = 1048576L
  )
  big <- runif(100000)
  t <- mizu_submit(p[["ctrl"]], v, v = big)
  expect_identical(pool_pull(p, 1L), 1L)
  expect_identical(pool_step(p), 1L)
  expect_identical(mizu_collect(t, timeout = 5), big)
  pool_end(p)
})

test_that("a task error is published and re-signalled at collect", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], stop("boom ", x), x = "today")
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "simpleError")
  expect_identical(conditionMessage(err), "boom today")
  # the slot released with the collect: reusable immediately
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("a task frame that fails to decode is the task's ERR, not worker death", {
  p <- pool_pair()
  ns <- new.env()
  info <- new.env()
  assign("spec", c(name = "zzmissing", version = "0.0.1"), envir = info)
  assign(".__NAMESPACE__.", info, envir = ns)
  f <- function(x) x + 1
  environment(f) <- ns
  t <- mizu_submit(p[["ctrl"]], f(1), f = f)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "packageNotFoundError")
  # the worker survived: later tasks run
  t2 <- mizu_submit(p[["ctrl"]], 1 + 1)
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), 2)
  pool_end(p)
})

test_that("a decode failure on demand is the task's ERR (exec fault flag)", {
  p <- pool_pair()
  .Call(mizu:::mizu_pool_exec_fail, p[["wk"]], TRUE)
  t <- mizu_submit(p[["ctrl"]], 1 + 1)
  pool_step(p)
  .Call(mizu:::mizu_pool_exec_fail, p[["wk"]], FALSE)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "simpleError")
  expect_identical(conditionMessage(err), "mizu: corrupt task payload")
  t2 <- mizu_submit(p[["ctrl"]], 2 + 2)
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), 4)
  pool_end(p)
})

test_that("a nested collect keeps the outer task's eval mark on unwind", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], {
    s <- mizu_submit(mizu_current_pool(), 40 + 2)
    mizu_collect(s, timeout = 5)
    stop("outer boom")
  })
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "simpleError")
  expect_identical(conditionMessage(err), "outer boom")
  t2 <- mizu_submit(p[["ctrl"]], 1L)
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), 1L)
  pool_end(p)
})

test_that("a classed condition re-signals with class and fields intact", {
  p <- pool_pair()
  cond <- structure(
    list(message = "typed", call = NULL, data = 42L),
    class = c("mizu_test_error", "error", "condition")
  )
  t <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_identical(class(err), c("mizu_test_error", "error", "condition"))
  expect_identical(conditionMessage(err), "typed")
  expect_identical(err[["data"]], 42L)
  pool_end(p)
})

test_that("an untransportable condition field is dropped and named, worker stays live", {
  p <- pool_pair()
  # built on the worker: an environment never crosses as a field (it would
  # zombie or worse on the serialize tiers) — it is dropped and named
  t <- mizu_submit(p[["ctrl"]], {
    stop(structure(
      list(message = "typed", call = NULL, payload = new.env()),
      class = c("mizu_test_error", "error", "condition")
    ))
  })
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_identical(conditionMessage(err), "typed")
  expect_identical(err[["dropped_fields"]], "payload")
  # the publish cannot fail: the worker runs on
  t2 <- mizu_submit(p[["ctrl"]], 42L)
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), 42L)
  pool_end(p)
})

test_that("codec-eligible fields cross intact alongside the original classes", {
  p <- pool_pair()
  df <- data.frame(x = c(1L, 2L, 3L))
  attr(df, "row.names") <- c(1L, 2L, 4L) # a sequence would be ALTREP
  cond <- structure(
    list(
      message = "typed",
      call = NULL,
      frame = df,
      tree = list(a = c(1L, 2L), b = list(c = "leaf"))
    ),
    class = c("mizu_test_error", "error", "condition")
  )
  t <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_identical(err[["frame"]], df)
  expect_identical(err[["tree"]], list(a = c(1L, 2L), b = list(c = "leaf")))
  expect_null(err[["dropped_fields"]])
  pool_end(p)
})

test_that("a custom conditionMessage method is bypassed at transport", {
  p <- pool_pair()
  conditionMessage.mizu_test_method <- function(e) "method output"
  cond <- structure(
    list(message = "raw field", call = NULL),
    class = c("mizu_test_method", "error", "condition")
  )
  t <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  # the raw message field crosses; the method still dispatches collect-side
  expect_identical(err[["message"]], "raw field")
  expect_identical(conditionMessage(err), "method output")
  pool_end(p)
})

test_that("a field past the budget is dropped and named; smaller fields arrive", {
  p <- pool_pair()
  cond <- structure(
    list(message = "typed", call = NULL, big = numeric(1000), small = 42L),
    class = c("mizu_test_error", "error", "condition")
  )
  t <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_null(err[["big"]])
  expect_identical(err[["small"]], 42L)
  expect_identical(err[["dropped_fields"]], "big")
  pool_end(p)
})

test_that("a message past its budget share arrives truncated at a character boundary", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], {
    msg <- paste(rep("x", 1000), collapse = "")
    stop(structure(
      list(message = msg, call = NULL),
      class = c("mizu_test_error", "error", "condition")
    ))
  })
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_identical(nchar(err[["message"]]), 236L) # half the 472 budget
  # multibyte characters are never split
  t2 <- mizu_submit(p[["ctrl"]], {
    msg <- paste(rep("\u20ac", 134), collapse = "")
    stop(structure(
      list(message = msg, call = NULL),
      class = c("mizu_test_error", "error", "condition")
    ))
  })
  pool_step(p)
  err2 <- tryCatch(mizu_collect(t2, timeout = 5), error = identity)
  expect_identical(nchar(err2[["message"]]), 78L) # 236 bytes backs off to 234
  pool_end(p)
})

test_that("a whole condition past the budget falls back, and still stages inline", {
  p <- pool_pair(slot_size = 256L) # a 216-byte budget forces the fallback
  t <- mizu_submit(p[["ctrl"]], {
    msg <- paste(rep("x", 1000), collapse = "")
    stop(structure(
      list(message = msg, call = NULL),
      class = c("mizu_test_error", "error", "condition")
    ))
  })
  spills_before <- mizu_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1]
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_identical(
    conditionMessage(err),
    "mizu: task error (untransportable condition)"
  )
  # the publish staged inline: no spill counted across the step
  expect_identical(
    mizu_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1],
    spills_before
  )
  t2 <- mizu_submit(p[["ctrl"]], "alive")
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), "alive")
  pool_end(p)
})

test_that("codec-ineligible fields are dropped and named, not zombied", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], {
    stop(structure(
      list(
        message = "typed",
        call = NULL,
        ptr = mizu_current_pool(),
        seq = 1:100000
      ),
      class = c("mizu_test_error", "error", "condition")
    ))
  })
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_null(err[["ptr"]])
  expect_null(err[["seq"]])
  expect_identical(err[["dropped_fields"]], c("ptr", "seq"))
  pool_end(p)
})

test_that("a non-list condition crosses as the fallback message with its class", {
  p <- pool_pair()
  # stop() can signal any classed object; an environment survives its
  # conditionMessage/conditionCall probes where an atomic vector cannot
  t <- mizu_submit(
    p[["ctrl"]],
    stop(structure(
      new.env(),
      class = c("mizu_test_error", "error", "condition")
    ))
  )
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_identical(class(err), c("mizu_test_error", "error", "condition"))
  expect_identical(
    conditionMessage(err),
    "mizu: task error (untransportable condition)"
  )
  expect_null(err[["dropped_fields"]])
  pool_end(p)
})

test_that("a non-scalar message field crosses as the fallback", {
  p <- pool_pair()
  cond <- structure(
    list(message = c("a", "b"), call = NULL),
    class = c("mizu_test_error", "error", "condition")
  )
  t <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_identical(
    conditionMessage(err),
    "mizu: task error (untransportable condition)"
  )
  pool_end(p)
})

test_that("a class vector past the budget spills the fallback out of line", {
  p <- pool_pair()
  klass <- c(paste0("cls", 1:1000), "mizu_test_error", "error", "condition")
  # no classed condition fits the budget: the terminal fallback crosses
  # out of line through the tiered stage, the class carried verbatim
  t <- mizu_submit(p[["ctrl"]], {
    stop(structure(
      list(message = "typed", call = NULL),
      class = c(
        paste0("cls", 1:1000),
        "mizu_test_error",
        "error",
        "condition"
      )
    ))
  })
  spills_before <- mizu_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1]
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_identical(class(err), klass)
  expect_identical(
    conditionMessage(err),
    "mizu: task error (untransportable condition)"
  )
  expect_identical(
    mizu_pool_stats(p[["ctrl"]])[["submitters"]][["spills"]][1],
    spills_before + 1
  )
  t2 <- mizu_submit(p[["ctrl"]], 42L)
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), 42L)
  pool_end(p)
})

test_that("the terminal fallback itself spills at the minimum slot size", {
  p <- pool_pair(slot_size = 128L) # an 88-byte budget: the fallback spills
  t <- mizu_submit(p[["ctrl"]], {
    stop(structure(
      list(message = "typed", call = NULL),
      class = c("mizu_test_error", "error", "condition")
    ))
  })
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_identical(
    conditionMessage(err),
    "mizu: task error (untransportable condition)"
  )
  t2 <- mizu_submit(p[["ctrl"]], "alive")
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), "alive")
  pool_end(p)
})

test_that("a non-UTF-8 message crosses translated to UTF-8", {
  p <- pool_pair()
  lat <- iconv("café au lait", from = "UTF-8", to = "latin1")
  cond <- structure(
    list(message = lat, call = NULL),
    class = c("mizu_test_error", "error", "condition")
  )
  t <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_identical(conditionMessage(err), "café au lait")
  expect_identical(Encoding(err[["message"]]), "UTF-8")
  pool_end(p)
})

test_that("a codec-ineligible call is dropped and named", {
  p <- pool_pair()
  # a closure constant embedded in the call is codec-ineligible
  cond <- structure(
    list(message = "typed", call = call("identity", function(x) x)),
    class = c("mizu_test_error", "error", "condition")
  )
  t <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_identical(conditionMessage(err), "typed")
  expect_identical(err[["dropped_fields"]], "call")
  pool_end(p)
})

test_that("an NA message falls back; unnamed elements drop silently", {
  p <- pool_pair()
  cond <- structure(
    list(message = NA_character_, call = NULL, 42L),
    class = c("mizu_test_error", "error", "condition")
  )
  t <- mizu_submit(p[["ctrl"]], stop(cond), cond = cond)
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_identical(
    conditionMessage(err),
    "mizu: task error (untransportable condition)"
  )
  # the unnamed element cannot be named in dropped_fields
  expect_null(err[["dropped_fields"]])
  pool_end(p)
})

test_that("an over-deep field is dropped and named, worker stays live", {
  p <- pool_pair()
  # past the codec's recursion bound the vet fails cleanly — no crash, no
  # zombie: the field drops and is named
  t <- mizu_submit(p[["ctrl"]], {
    deep <- list(1L)
    for (i in 1:600) {
      deep <- list(deep)
    }
    stop(structure(
      list(message = "typed", call = NULL, deep = deep),
      class = c("mizu_test_error", "error", "condition")
    ))
  })
  pool_step(p)
  err <- tryCatch(mizu_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "mizu_test_error")
  expect_identical(err[["dropped_fields"]], "deep")
  t2 <- mizu_submit(p[["ctrl"]], 42L)
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), 42L)
  pool_end(p)
})

test_that("only error conditions fail a task: a warning passes through", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], {
    warning("advisory only")
    "completed"
  })
  expect_warning(pool_step(p), "advisory only")
  expect_identical(mizu_collect(t, timeout = 5), "completed")
  pool_end(p)
})

test_that("collect times out with the sentinel and later succeeds", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], "done")
  expect_s3_class(
    mizu_collect(t, timeout = 0),
    c("mizu_timeout", "mizu_sentinel")
  )
  expect_s3_class(mizu_collect(t, timeout = 0.1), "mizu_timeout")
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), "done")
  pool_end(p)
})

test_that("a handle collects exactly once", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], 1L)
  pool_step(p)
  expect_identical(mizu_collect(t, timeout = 5), 1L)
  expect_error(mizu_collect(t, timeout = 5), "already collected")
  pool_end(p)
})

test_that("cancel discards a still-queued task; the worker frees the slot", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], stop("never runs"))
  expect_true(mizu_cancel(t))
  expect_false(mizu_cancel(t)) # already cancelled
  expect_error(mizu_collect(t, timeout = 5), "cancelled")
  expect_identical(mizu_pool_status(p[["ctrl"]])[["tasks"]][["cancel"]], 1L)
  # the queued entry is consumed later; only then does CANCEL become FREE
  pool_step(p)
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("cancel is too late once the task has completed", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], "ran")
  pool_step(p)
  expect_false(mizu_cancel(t))
  expect_identical(mizu_collect(t, timeout = 5), "ran")
  pool_end(p)
})

test_that("a dropped handle cancels its pending task at finalization", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], "orphaned")
  rm(t)
  gc()
  expect_identical(mizu_pool_status(p[["ctrl"]])[["tasks"]][["cancel"]], 1L)
  pool_step(p) # consume + free
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("a dropped handle frees an uncollected published result", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], "never collected")
  pool_step(p)
  expect_identical(mizu_pool_status(p[["ctrl"]])[["tasks"]][["ok"]], 1L)
  rm(t)
  gc() # OK -> FREE without mapping
  expect_identical(
    unname(mizu_pool_status(p[["ctrl"]])[["tasks"]]),
    rep(0L, 5L)
  )
  pool_end(p)
})

test_that("result slots are bounded per submitter and reused after collect", {
  p <- pool_pair(max_submitters = 8L, result_slots = 16L) # 2 per submitter
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  t2 <- mizu_submit(p[["ctrl"]], 2L)
  expect_error(mizu_submit(p[["ctrl"]], 3L), "result slots exhausted")
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(t1, timeout = 5), 1L)
  # collect released a slot: the allocator reuses it
  t3 <- mizu_submit(p[["ctrl"]], 3L)
  pool_step(p)
  expect_identical(mizu_collect(t2, timeout = 5), 2L)
  expect_identical(mizu_collect(t3, timeout = 5), 3L)
  pool_end(p)
})

test_that("injection back-pressure is per-submitter and error-bounded", {
  p <- pool_pair(injection_cap = 2L)
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  t2 <- mizu_submit(p[["ctrl"]], 2L)
  expect_error(
    mizu_submit(p[["ctrl"]], 3L, .timeout = 0.2),
    "submission timed out"
  )
  # a worker pop frees exactly this ring's space
  pool_step(p)
  t3 <- mizu_submit(p[["ctrl"]], 3L, .timeout = 0)
  while (pool_step(p) == 1L) {
    NULL
  }
  for (t in list(t1, t2, t3)) {
    expect_no_error(mizu_collect(t, timeout = 5))
  }
  pool_end(p)
})

test_that("queued task payloads stay pinned across the sender's GC", {
  p <- pool_pair()
  v <- runif(100000) # SHM_RAW task payload
  s <- sum(v)
  t <- mizu_submit(p[["ctrl"]], sum(v), v = v)
  rm(v)
  gc() # without the task keeper, the payload region would unlink here
  pool_step(p) # the worker's open must succeed
  expect_identical(mizu_collect(t, timeout = 5), s)
  pool_end(p)
})

test_that("published results stay pinned across the worker's GC", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], seq_len(n) + 0, n = 100000L) # SHM_RAW result
  pool_step(p)
  gc() # without the result keeper, the result region would unlink here
  expect_identical(mizu_collect(t, timeout = 5), as.double(seq_len(100000L)))
  pool_end(p)
})

test_that("a destroyed pool invalidates outstanding task handles", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], "never run") # no step: stays PENDING
  pool_end(p) # broadcast CANCELs it
  expect_error(mizu_collect(t, timeout = 5), "pool handle is closed")
})

test_that("collect_try boxes a cancellation instead of raising", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], 1L)
  mizu_cancel(t)
  v <- .Call(mizu:::mizu_pool_collect_try, t, 0)
  expect_s3_class(v, "mizu_caught")
  expect_s3_class(v[[1L]], "mizu_error_cancelled")
  pool_step(p)
  pool_end(p)
})

test_that("a corrupt task payload is the task's ERR, not worker death", {
  p <- pool_pair()
  t1 <- .Call(
    mizu:::mizu_pool_submit,
    p[["ctrl"]],
    list(quote(1L), "args"),
    Inf,
    0L
  )
  t2 <- .Call(
    mizu:::mizu_pool_submit,
    p[["ctrl"]],
    list(quote(1L), list(2L)),
    Inf,
    0L
  ) # unnamed argument list
  # the eval mark arms at exec entry, ahead of the decode: a shape-check
  # failure unwinds as this task's ERR, and the worker keeps serving
  pool_step(p)
  expect_error(mizu_collect(t1, timeout = 5), "corrupt task payload")
  pool_step(p)
  expect_error(mizu_collect(t2, timeout = 5), "corrupt task payload")
  t3 <- mizu_submit(p[["ctrl"]], "alive")
  pool_step(p)
  expect_identical(mizu_collect(t3, timeout = 5), "alive")
  pool_end(p)
})

test_that("a trace-hook error outside any task eval is not attributed", {
  p <- pool_pair()
  mizu_pool_trace(p[["wk"]], function(event, id) {
    if (event == "done") stop("hook boom")
  })
  t <- mizu_submit(p[["ctrl"]], 42L)
  # the hook fires after the publish: run_outcome refuses the error and
  # the step's caller must treat it as infrastructure failure
  expect_error(pool_step(p), "hook boom")
  expect_identical(mizu_collect(t, timeout = 5), 42L)
  mizu_pool_trace(p[["wk"]], NULL)
  pool_end(p)
})

test_that("a stale task handle reads collected and errors on collect", {
  p <- pool_pair(result_slots = 8L) # one result slot per submitter
  t1 <- mizu_submit(p[["ctrl"]], 1 + 1)
  pool_step(p)
  expect_identical(mizu_collect(t1, 5), 2)
  t2 <- mizu_submit(p[["ctrl"]], 2 + 2) # reuses the one slot, bumping its sequence
  expect_identical(.Call(mizu:::mizu_pool_task_state, t1), "collected")
  expect_false(mizu_cancel(t1))
  expect_error(mizu_collect(t1, 5), "already collected or invalidated")
  pool_step(p)
  expect_identical(mizu_collect(t2, 5), 4)
  pool_end(p)
})

test_that("cancel is FALSE once collected and once the pool is gone", {
  p <- pool_pair()
  t <- mizu_submit(p[["ctrl"]], 1 + 1)
  pool_step(p)
  expect_identical(mizu_collect(t, 5), 2)
  expect_false(mizu_cancel(t))
  pool_end(p)
  expect_false(mizu_cancel(t))
})

test_that("task_state rejects non-task handles", {
  expect_error(.Call(mizu:::mizu_pool_task_state, NULL), "not a task handle")
})
