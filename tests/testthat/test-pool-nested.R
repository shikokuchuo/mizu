# Worker-side nested submit and collect-time help mode over the in-process
# harness. Tasks see their worker's handle bound as `pool`; nested submits
# land on the worker's own deque (or run inline when it is full) and
# allocate result slots from a lazily claimed submitter slot. A worker
# blocked in collect helps — pops its own bottom, steals — instead of
# parking, which is what makes every wait here deterministic and
# single-stepped.

test_that("a direct nested submit lands on the deque and help-collects", {
  p <- pool_pair()
  sub <- sora_submit(p[["wk"]], "direct")
  st <- sora_pool_status(p[["ctrl"]])
  expect_identical(st[["deque"]], 1)
  # the worker claimed a submitter slot on first nested use
  expect_identical(sum(st[["submitters"]] == "live"), 2L)
  # a poll collect makes progress through help mode: pop, execute, return
  expect_identical(sora_collect(sub, timeout = 0), "direct")
  expect_identical(sora_pool_status(p[["ctrl"]])[["deque"]], 0)
  pool_end(p)
})

test_that("help mode runs non-awaited work queued above the awaited entry", {
  p <- pool_pair()
  a <- sora_submit(p[["wk"]], "a")
  b <- sora_submit(p[["wk"]], "b")
  # collecting a pops LIFO: b executes first, then a publishes
  expect_identical(sora_collect(a, timeout = 0), "a")
  expect_identical(sora_collect(b, timeout = 0), "b")
  pool_end(p)
})

test_that("a task submits and collects a nested subtask in one step", {
  p <- pool_pair()
  t <- sora_submit(
    p[["ctrl"]],
    {
      s <- sora_submit(pool, x + 1L, x = x)
      sora_collect(s, timeout = 5) * 2L
    },
    x = 20L
  )
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(t, timeout = 5), 42L)
  pool_end(p)
})

test_that("a full deque runs nested subtasks inline (work-first)", {
  p <- pool_pair(per_worker_cap = 2L, max_submitters = 2L, result_slots = 64L)
  t <- sora_submit(p[["ctrl"]], {
    subs <- lapply(1:4, function(i) sora_submit(pool, i * 10L, i = i))
    sum(vapply(subs, function(s) sora_collect(s, timeout = 5), integer(1)))
  })
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(t, timeout = 5), 100L)
  expect_identical(unname(sora_pool_status(p[["ctrl"]])[["tasks"]]), rep(0L, 5L))
  pool_end(p)
})

test_that("help mode contains an erroring subtask at its own boundary", {
  p <- pool_pair()
  # the subtask's error must publish as the subtask's ERR result and
  # re-signal at the nested collect — never escape into the outer task's
  # frames, whose own result stays OK
  t <- sora_submit(p[["ctrl"]], {
    s <- sora_submit(pool, stop("sub boom"))
    paste(
      "caught:",
      tryCatch(sora_collect(s, timeout = 5), error = conditionMessage)
    )
  })
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(t, timeout = 5), "caught: sub boom")
  expect_identical(unname(sora_pool_status(p[["ctrl"]])[["tasks"]]), rep(0L, 5L))
  pool_end(p)
})

test_that("a full deque's inline execution contains subtask errors", {
  p <- pool_pair(per_worker_cap = 2L, max_submitters = 2L, result_slots = 64L)
  # the first two subtasks queue, the rest run inline at submit: both
  # execution paths contain the error at the subtask boundary
  t <- sora_submit(p[["ctrl"]], {
    subs <- lapply(1:4, function(i) sora_submit(pool, stop("boom ", i), i = i))
    vapply(
      subs,
      function(s) {
        tryCatch(sora_collect(s, timeout = 5), error = conditionMessage)
      },
      ""
    )
  })
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(t, timeout = 5), paste("boom", 1:4))
  expect_identical(unname(sora_pool_status(p[["ctrl"]])[["tasks"]]), rep(0L, 5L))
  pool_end(p)
})

test_that("a task error after nested activity publishes to its own slot", {
  p <- pool_pair()
  # the inner execute retires the worker's shm announce: the unwind path
  # must publish from the process-local claim identity, not the announce
  t <- sora_submit(p[["ctrl"]], {
    s <- sora_submit(pool, "inner ok")
    if (!identical(sora_collect(s, timeout = 5), "inner ok")) {
      stop("inner collect mismatch")
    }
    stop("outer boom")
  })
  expect_identical(pool_step(p), 1L)
  err <- tryCatch(sora_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "simpleError")
  expect_identical(conditionMessage(err), "outer boom")
  expect_identical(unname(sora_pool_status(p[["ctrl"]])[["tasks"]]), rep(0L, 5L))
  pool_end(p)
})

test_that("classed conditions survive help-mode containment", {
  p <- pool_pair()
  cond <- structure(
    list(message = "typed sub", call = NULL),
    class = c("sora_test_error", "error", "condition")
  )
  # the outer tryCatch keys on the custom class: it must survive the
  # containment, the wire crossing, and the re-signal
  t <- sora_submit(
    p[["ctrl"]],
    {
      s <- sora_submit(pool, stop(cond), cond = cond)
      tryCatch(sora_collect(s, timeout = 5), sora_test_error = function(e) {
        paste("typed:", conditionMessage(e))
      })
    },
    cond = cond
  )
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(t, timeout = 5), "typed: typed sub")
  pool_end(p)
})

test_that("nested collect helps through recursion past the depth limit", {
  p <- pool_pair(max_submitters = 2L, result_slots = 128L)
  # each level nested-submits the next and collects it: help depth climbs
  # past HELP_DEPTH_LIMIT (32), where only ownerless work — here the own
  # deque bottom holding the child — remains eligible
  countdown <- function(m, f, p) {
    if (m <= 0L) {
      return(0L)
    }
    s <- sora_submit(p, f(m, f, pool), m = m - 1L, f = f)
    sora_collect(s, timeout = 5) + 1L
  }
  environment(countdown) <- globalenv()
  t <- sora_submit(p[["ctrl"]], f(m, f, pool), m = 40L, f = countdown)
  expect_identical(pool_step(p), 1L)
  expect_identical(sora_collect(t, timeout = 5), 40L)
  pool_end(p)
})

test_that("a cancelled nested entry frees at its later pop", {
  p <- pool_pair()
  t <- sora_submit(p[["ctrl"]], {
    s <- sora_submit(pool, "never runs")
    sora_cancel(s)
    tryCatch(sora_collect(s, timeout = 5), error = conditionMessage)
  })
  expect_identical(pool_step(p), 1L)
  expect_identical(
    sora_collect(t, timeout = 5),
    "sora: task cancelled or pool stopped"
  )
  # the cancelled entry still queues on the deque; the next pop frees it
  expect_identical(sora_pool_status(p[["ctrl"]])[["deque"]], 1)
  expect_identical(pool_step(p), 1L)
  expect_identical(unname(sora_pool_status(p[["ctrl"]])[["tasks"]]), rep(0L, 5L))
  pool_end(p)
})

test_that("a full submitter registry surfaces as the nested task's error", {
  p <- pool_pair(max_submitters = 1L) # the controller holds the only slot
  t <- sora_submit(p[["ctrl"]], {
    s <- sora_submit(pool, 1L)
    sora_collect(s, timeout = 5)
  })
  expect_identical(pool_step(p), 1L)
  err <- tryCatch(sora_collect(t, timeout = 5), error = identity)
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "submitter registry full")
  pool_end(p)
})

test_that("sora_pool_dump snapshots registries, deques, and result slots", {
  p <- pool_pair(workers = 2L, max_submitters = 2L)
  t1 <- sora_submit(p[["ctrl"]], "queued")
  s1 <- sora_submit(p[["wk"]], "nested")
  d <- sora_pool_dump(p[["ctrl"]])
  prefix <- if (.Platform[["OS.type"]] == "windows") "Local\\sora_" else "/sora_"
  expect_true(startsWith(d[["name"]], prefix))
  expect_false(d[["shutdown"]])
  expect_identical(d[["workers"]][["slot"]], 0:1)
  expect_identical(d[["workers"]][["status"]], c("live", "live"))
  expect_identical(d[["workers"]][["park_state"]], c("running", "running"))
  expect_identical(d[["workers"]][["parked"]], c(FALSE, FALSE))
  expect_identical(d[["workers"]][["bottom"]] - d[["workers"]][["top"]], c(1, 0))
  expect_identical(d[["workers"]][["in_flight"]], c(-1L, -1L))
  expect_identical(d[["submitters"]][["status"]], c("live", "live"))
  expect_identical(d[["submitters"]][["queued"]], c(1, 0))
  expect_identical(d[["submitters"]][["rs_start"]], c(0L, 32L))
  expect_identical(nrow(d[["tasks"]]), 2L)
  expect_identical(d[["tasks"]][["status"]], c("pending", "pending"))
  expect_identical(d[["tasks"]][["worker"]], c(-1L, -1L))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(sora_collect(t1, timeout = 5), "queued")
  expect_identical(sora_collect(s1, timeout = 0), "nested")
  expect_identical(nrow(sora_pool_dump(p[["ctrl"]])[["tasks"]]), 0L)
  pool_end(p)
})
