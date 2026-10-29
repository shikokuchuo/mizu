# Event-driven promises and handlers (R/promise.R over the core's notify
# endpoints). In-process tests use the channel_pair() / pool_pair()
# harnesses; cross-process tests spawn real peers and workers.

test_that("a pending recv promise resolves after a publish, once armed", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  o <- observe(mizu_recv_async(p[["host"]]))
  expect_false(settled(o))
  mizu_send(p[["peer"]], 42L)
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 42L)
  expect_null(o$error)
})

test_that("arm-after-data resolves from the arm's synchronous drain", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p[["peer"]], 43L)
  o <- observe(mizu_recv_async(p[["host"]]))
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 43L)
})

test_that("pending recvs resolve in FIFO call order, one message each", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  os <- lapply(1:3, function(i) observe(mizu_recv_async(p[["host"]])))
  for (i in 1:3) {
    mizu_send(p[["peer"]], i * 10L)
  }
  expect_true(pump_until(function() all(vapply(os, settled, TRUE))))
  expect_identical(lapply(os, function(o) o$value), list(10L, 20L, 30L))
})

test_that("a timed-out promise leaves the FIFO; the next message goes to the next waiter", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  o1 <- observe(mizu_recv_async(p[["host"]], timeout = 0.1))
  o2 <- observe(mizu_recv_async(p[["host"]], timeout = 5))
  expect_true(pump_until(function() settled(o1)))
  expect_s3_class(o1$error, "mizu_timeout")
  expect_s3_class(o1$error, "mizu_error")
  mizu_send(p[["peer"]], 7L)
  expect_true(pump_until(function() settled(o2)))
  expect_identical(o2$value, 7L)
})

test_that("a channel disarms when its last pending promise times out", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  o <- observe(mizu_recv_async(p[["host"]], timeout = 0.05))
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "mizu_timeout")
  # disarmed: this publish pays no signal — the message waits for the next arm
  mizu_send(p[["peer"]], 9L)
  later::run_now()
  expect_identical(
    .Call(mizu:::mizu_channel_stat, p[["peer"]])[["notify_sends"]],
    0
  )
  o2 <- observe(mizu_recv_async(p[["host"]]))
  expect_true(pump_until(function() settled(o2)))
  expect_identical(o2$value, 9L) # resolved from the arm's synchronous drain
})

test_that("a drain queued behind a timeout disarm leaves the message in the ring", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  o <- observe(mizu_recv_async(p[["host"]], timeout = 0.05))
  Sys.sleep(0.1) # the timer is now due, but the loop has not run it
  mizu_send(p[["peer"]], 3L) # the publish schedules a drain behind the timer
  Sys.sleep(0.1) # let the watch's execLaterNative2 land in the queue
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "mizu_timeout")
  expect_identical(mizu_recv(p[["host"]], 0), 3L)
})

test_that("a handler installed over queued messages drains them from the arm", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_send(p[["peer"]], 5L)
  got <- list()
  mizu_recv_handler(p[["host"]], function(x) got[[length(got) + 1L]] <<- x)
  expect_identical(got, list(5L)) # delivered by the arm's synchronous drain
  mizu_recv_handler(p[["host"]], NULL)
})

test_that("the handler form delivers a burst and receives the terminal condition", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(.Call(mizu:::mizu_channel_destroy, p[["host"]]))
  on.exit(.Call(mizu:::mizu_channel_destroy, p[["peer"]]), add = TRUE)
  got <- new.env(parent = emptyenv())
  got$xs <- list()
  mizu_recv_handler(p[["host"]], function(x) {
    got$xs[[length(got$xs) + 1L]] <- x
  })
  for (i in 1:3) {
    mizu_send(p[["peer"]], i)
  }
  expect_true(pump_until(function() length(got$xs) == 3L))
  expect_identical(unlist(got$xs), 1:3)
  .Call(mizu:::mizu_channel_close_signal, p[["peer"]])
  expect_true(pump_until(function() length(got$xs) == 4L))
  expect_s3_class(got$xs[[4L]], "mizu_closed")
  expect_s3_class(got$xs[[4L]], "mizu_error")
})

test_that("handler and promise on one channel are mutually exclusive", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  mizu_recv_handler(p[["host"]], function(x) invisible())
  expect_error(mizu_recv_async(p[["host"]]), "mutually exclusive")
  mizu_recv_handler(p[["host"]], NULL)
  o <- observe(mizu_recv_async(p[["host"]]))
  expect_error(
    mizu_recv_handler(p[["host"]], function(x) invisible()),
    "mutually exclusive"
  )
  mizu_send(p[["peer"]], 1L)
  expect_true(pump_until(function() settled(o)))
})

test_that("a blocking recv mixed with a pending promise consumes in arrival order", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  o <- observe(mizu_recv_async(p[["host"]]))
  mizu_send(p[["peer"]], 1L)
  expect_identical(mizu_recv(p[["host"]], 5), 1L) # the blocking call takes it
  mizu_send(p[["peer"]], 2L)
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 2L) # the promise waited for the next
})

test_that("a blocking recv mixed with a handler consumes in arrival order", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  got <- new.env(parent = emptyenv())
  got$xs <- list()
  mizu_recv_handler(p[["host"]], function(x) {
    got$xs[[length(got$xs) + 1L]] <- x
  })
  mizu_send(p[["peer"]], 1L)
  expect_identical(mizu_recv(p[["host"]], 5), 1L) # the blocking call takes it
  mizu_send(p[["peer"]], 2L)
  expect_true(pump_until(function() length(got$xs) == 1L))
  expect_identical(got$xs[[1L]], 2L) # the handler sees the next
})

test_that("a terminal observed by a blocking recv reaches the consumer too", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  o <- observe(mizu_recv_async(p[["host"]]))
  .Call(mizu:::mizu_channel_close_signal, p[["peer"]])
  expect_s3_class(mizu_recv(p[["host"]], 5), "mizu_closed")
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "mizu_closed")
})

test_that("the channel async verbs validate their arguments", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  expect_snapshot(error = TRUE, mizu_recv_async(p[["host"]], NA))
  expect_snapshot(error = TRUE, mizu_recv_async(p[["host"]], "5"))
  expect_snapshot(error = TRUE, mizu_recv_handler(p[["host"]], 1))
})

test_that("the async verbs error informatively without later/promises installed", {
  skip_if_no_promises()
  skip_if_no_child_mizu()
  # isolation needs later/promises out of .Library (include.site = FALSE
  # drops the site library; the system library cannot be hidden)
  syslib <- normalizePath(.Library)
  in_syslib <- vapply(
    c("later", "promises"),
    function(pkg) identical(normalizePath(dirname(find.package(pkg))), syslib),
    TRUE
  )
  skip_if(any(in_syslib), "later/promises installed in the system library")
  # an installed mizu in a private library: later/promises stay invisible
  mizu_home <- .libPaths()[file.exists(
    file.path(.libPaths(), "mizu", "Meta", "package.rds")
  )][1L]
  lib1 <- gsub("\\\\", "/", tempfile("lib"))
  dir.create(lib1)
  file.copy(file.path(mizu_home, "mizu"), lib1, recursive = TRUE)
  lib2 <- gsub("\\\\", "/", tempfile("lib"))
  dir.create(lib2)
  for (pkg in c("later", "Rcpp", "rlang")) {
    file.copy(find.package(pkg), lib2, recursive = TRUE)
  }
  src <- tfile()
  writeLines(
    c(
      sprintf('.libPaths("%s", include.site = FALSE)', lib1),
      'library(mizu)',
      'e1 <- tryCatch(mizu_recv_async(NULL), error = conditionMessage)',
      'e2 <- tryCatch(',
      '  mizu_recv_handler(NULL, function(x) NULL), error = conditionMessage',
      ')',
      sprintf('.libPaths(c("%s", "%s"), include.site = FALSE)', lib1, lib2),
      'e3 <- tryCatch(mizu_recv_async(NULL), error = conditionMessage)',
      'writeLines(c(e1, e2, e3))'
    ),
    src
  )
  out <- system2(file.path(R.home("bin"), "Rscript"), src, stdout = TRUE)
  expect_length(out, 3L)
  expect_match(
    out[1L],
    "the async verbs require the 'later' package",
    fixed = TRUE
  )
  expect_match(
    out[2L],
    "mizu_recv_handler() requires the 'later' package",
    fixed = TRUE
  )
  expect_match(
    out[3L],
    "the async verbs require the 'promises' package",
    fixed = TRUE
  )
})

test_that("the S3 promise protocol methods dispatch on handles", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  expect_true(promises::is.promising(p[["host"]]))
  o <- observe(promises::as.promise(p[["host"]]))
  mizu_send(p[["peer"]], 1L)
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 1L)
  q <- pool_pair()
  on.exit(pool_end(q), add = TRUE)
  t <- mizu_submit(q[["ctrl"]], 2L)
  expect_true(promises::is.promising(t))
  pool_step(q)
  expect_identical(mizu_collect(t, 5), 2L)
  m <- mizu_map(q[["ctrl"]], 1:2, function(i) i)
  expect_true(promises::is.promising(m))
  mizu_cancel(m)
})

test_that("arming a spin channel errors", {
  skip_if_no_promises()
  p <- channel_pair(spin = TRUE)
  on.exit(channel_end(p))
  expect_error(mizu_recv_async(p[["host"]]), class = "mizu_error")
})

test_that("edge trigger: N publishes while armed send exactly one signal", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  sends <- function() {
    .Call(mizu:::mizu_channel_stat, p[["peer"]])[["notify_sends"]]
  }
  expect_identical(sends(), 0)
  o <- observe(mizu_recv_async(p[["host"]]))
  for (i in 1:5) {
    mizu_send(p[["peer"]], i)
  }
  expect_identical(sends(), 1)
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 1L)
  # the FIFO emptied, so the consumer disarmed: publishes pay no syscall
  expect_identical(mizu_recv_batch(p[["host"]], 10L, 5), as.list(2:5))
  mizu_send(p[["peer"]], 6L)
  expect_identical(sends(), 1)
  # re-arm: the queued message resolves from the sync drain
  o2 <- observe(mizu_recv_async(p[["host"]]))
  expect_true(pump_until(function() settled(o2)))
  expect_identical(o2$value, 6L)
  # the next publish pays the one signal of this arm
  o3 <- observe(mizu_recv_async(p[["host"]]))
  mizu_send(p[["peer"]], 7L)
  expect_identical(sends(), 2)
  expect_true(pump_until(function() settled(o3)))
  expect_identical(o3$value, 7L)
})

test_that("close rejects a pending promise and delivers a handler's terminal", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(.Call(mizu:::mizu_channel_destroy, p[["host"]]))
  on.exit(.Call(mizu:::mizu_channel_destroy, p[["peer"]]), add = TRUE)
  # the peer-end close signals the endpoint: rejection without mizu_close()
  o <- observe(mizu_recv_async(p[["host"]]))
  .Call(mizu:::mizu_channel_close_signal, p[["peer"]])
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "mizu_closed")
  # and the local destroy (mizu_close) flushes anything still pending
  p2 <- channel_pair()
  on.exit(.Call(mizu:::mizu_channel_destroy, p2[["peer"]]), add = TRUE)
  o2 <- observe(mizu_recv_async(p2[["host"]]))
  .Call(mizu:::mizu_channel_close_signal, p2[["peer"]])
  mizu_close(p2[["host"]])
  expect_true(pump_until(function() settled(o2)))
  expect_s3_class(o2$error, "mizu_closed")
})

test_that("a notify record freed mid-registry leaves the list intact", {
  skip_if_no_promises()
  p1 <- channel_pair()
  p2 <- channel_pair()
  p3 <- channel_pair()
  on.exit(.Call(mizu:::mizu_channel_destroy, p1[["peer"]]))
  on.exit(.Call(mizu:::mizu_channel_destroy, p2[["host"]]), add = TRUE)
  on.exit(.Call(mizu:::mizu_channel_destroy, p2[["peer"]]), add = TRUE)
  on.exit(.Call(mizu:::mizu_channel_destroy, p3[["host"]]), add = TRUE)
  on.exit(.Call(mizu:::mizu_channel_destroy, p3[["peer"]]), add = TRUE)
  got <- new.env(parent = emptyenv())
  got$xs <- list()
  mizu_recv_handler(p1[["host"]], function(x) invisible())
  mizu_recv_handler(p2[["host"]], function(x) invisible())
  mizu_recv_handler(
    p3[["host"]],
    function(x) got$xs[[length(got$xs) + 1L]] <- x
  )
  # freeing p1's record walks past p3's and p2's to splice it out
  .Call(mizu:::mizu_channel_destroy, p1[["host"]])
  mizu_send(p3[["peer"]], 5L)
  expect_true(pump_until(function() length(got$xs) == 1L))
  expect_identical(got$xs[[1L]], 5L)
})

test_that("batch handler: a burst over the quantum splits with order and no loss", {
  skip_if_no_promises()
  K <- mizu:::notify_quantum_n
  n <- K + 10L
  p <- channel_pair(capacity = 4096L)
  on.exit(channel_end(p))
  got <- new.env(parent = emptyenv())
  got$bursts <- list()
  got$probe <- FALSE
  mizu_recv_handler(
    p[["host"]],
    function(xs) {
      got$bursts[[length(got$bursts) + 1L]] <- xs
      if (length(got$bursts) == 1L) {
        # registered mid-drain: runs between quanta, proving the loop is
        # not starved by a saturated ring
        later::later(function() got$probe <- TRUE, 0)
      }
    },
    batch = TRUE
  )
  for (i in seq_len(n)) {
    mizu_send(p[["peer"]], i)
  }
  expect_true(pump_until(function() length(got$bursts) >= 2L))
  expect_true(got$probe)
  expect_identical(length(got$bursts[[1L]]), K)
  expect_identical(unlist(got$bursts), seq_len(n))
  mizu_recv_handler(p[["host"]], NULL)
})

test_that("a re-scheduled drain behind a handler removal exits cleanly", {
  skip_if_no_promises()
  n <- 5000L # > 2 quanta: two passes leave the ring non-empty, re-scheduling
  p <- channel_pair(capacity = 8192L)
  on.exit(channel_end(p))
  got <- 0L
  mizu_recv_handler(p[["host"]], function(x) got <<- got + 1L)
  for (i in seq_len(n)) {
    mizu_send(p[["peer"]], i)
  }
  expect_true(pump_until(function() got > 0L))
  later::run_now() # a second quantum: re-schedules the drain once more
  rest <- got
  mizu_recv_handler(p[["host"]], NULL)
  later::run_now() # the re-scheduled drain finds the consumer gone and exits
  later::run_now()
  expect_identical(got, rest)
  expect_identical(mizu_recv(p[["host"]], 0), rest + 1L) # the rest is untouched
})

test_that("a handler removing itself mid-drain ends the pass cleanly", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  got <- new.env(parent = emptyenv())
  got$xs <- list()
  mizu_recv_handler(p[["host"]], function(x) {
    got$xs[[length(got$xs) + 1L]] <- x
    mizu_recv_handler(p[["host"]], NULL)
  })
  mizu_send_batch(p[["peer"]], list(1L, 2L, 3L))
  expect_true(pump_until(function() length(got$xs) == 1L))
  later::run_now()
  expect_identical(got$xs, list(1L)) # no NULL-fn call, no zombie re-arm
  o <- observe(mizu_recv_async(p[["host"]]))
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 2L) # the ring's remainder reaches the next consumer
})

test_that("a promise FIFO larger than the quantum resolves in FIFO order", {
  skip_if_no_promises()
  K <- mizu:::notify_quantum_n
  n <- K + 10L
  p <- channel_pair(capacity = 4096L)
  on.exit(channel_end(p))
  os <- lapply(seq_len(n), function(i) observe(mizu_recv_async(p[["host"]])))
  for (i in seq_len(n)) {
    mizu_send(p[["peer"]], i)
  }
  expect_true(pump_until(function() all(vapply(os, settled, TRUE)), 30))
  expect_identical(vapply(os, function(o) o$value, 1L), seq_len(n))
})

test_that("an erroring handler is removed and its error surfaces", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  calls <- 0L
  mizu_recv_handler(p[["host"]], function(x) {
    calls <<- calls + 1L
    stop("handler boom")
  })
  mizu_send(p[["peer"]], 1L)
  e <- tryCatch(
    {
      pump_until(function() FALSE, 0.2)
      NULL
    },
    error = identity
  )
  expect_s3_class(e, "error")
  expect_match(conditionMessage(e), "handler boom")
  # removed: a second message is not delivered, the ring holds it
  mizu_send(p[["peer"]], 2L)
  later::run_now()
  expect_identical(calls, 1L)
  expect_identical(mizu_recv(p[["host"]], 5), 2L)
})

# Pool forms -----------------------------------------------------------------

test_that("a task promise resolves, is cached, and rejects with the sync classes", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], 42L)
  p1 <- promises::as.promise(t)
  expect_identical(p1, promises::as.promise(t))
  o <- observe(p1)
  pool_step(p)
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 42L)
  t2 <- mizu_submit(p[["ctrl"]], stop("boom"))
  o2 <- observe(promises::as.promise(t2))
  pool_step(p)
  expect_true(pump_until(function() settled(o2)))
  expect_s3_class(o2$error, "simpleError")
  expect_match(conditionMessage(o2$error), "boom")
  # a task already terminal at enrollment resolves from the re-check
  t3 <- mizu_submit(p[["ctrl"]], 9L)
  pool_step(p)
  o3 <- observe(promises::as.promise(t3))
  expect_true(pump_until(function() settled(o3)))
  expect_identical(o3$value, 9L)
})

test_that("then() dispatches on a task handle directly", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], 21L)
  v <- NULL
  promises::then(t, function(x) v <<- x * 2L)
  pool_step(p)
  expect_true(pump_until(function() !is.null(v)))
  expect_identical(v, 42L)
})

test_that("a promise-armed task refuses a blocking collect; the promise resolves", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], 1L) # unstepped: stays pending
  o <- observe(promises::as.promise(t))
  expect_error(mizu_collect(t, 0), "pending consumer")
  pool_step(p)
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 1L)
  # settled: the refusal never outlives its consumer
  expect_error(mizu_collect(t, 0), "already collected")
})

test_that("as.promise on an already-collected task errors", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], 1L)
  pool_step(p)
  expect_identical(mizu_collect(t, 5), 1L)
  expect_error(promises::as.promise(t), "already collected")
})

test_that("collect_all_async resolves once, input order with names carried", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  ts <- list(
    a = mizu_submit(p[["ctrl"]], 1L),
    b = mizu_submit(p[["ctrl"]], 2L)
  )
  o <- observe(mizu_collect_all_async(ts))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, list(a = 1L, b = 2L))
})

test_that("collect_all_async rejects the first erroring by position; the rest collectible", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  ts <- list(
    mizu_submit(p[["ctrl"]], stop("e1")),
    mizu_submit(p[["ctrl"]], stop("e2")),
    mizu_submit(p[["ctrl"]], 5L)
  )
  o <- observe(mizu_collect_all_async(ts))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "simpleError")
  expect_identical(o$error[["index"]], 1L)
  expect_match(conditionMessage(o$error), "e1")
  expect_error(mizu_collect(ts[[2L]], 5), "e2")
  expect_identical(mizu_collect(ts[[3L]], 5), 5L)
})

test_that("collect_all_async timeout rejects and consumes nothing", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  ts <- list(mizu_submit(p[["ctrl"]], 1L), mizu_submit(p[["ctrl"]], 2L))
  o <- observe(mizu_collect_all_async(ts, timeout = 0.1))
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "mizu_timeout")
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(ts[[1L]], 5), 1L)
  expect_identical(mizu_collect(ts[[2L]], 5), 2L)
})

test_that("collect_any_async resolves list(index, value); errors carry index", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  ts <- list(
    mizu_submit(p[["ctrl"]], "first"),
    mizu_submit(p[["ctrl"]], "second")
  )
  o <- observe(mizu_collect_any_async(ts))
  pool_step(p)
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, list(index = 1L, value = "first"))
  # the loser returned to blocking-collectible
  pool_step(p)
  expect_identical(mizu_collect(ts[[2L]], 5), "second")
  # an erroring first terminal rejects with the index field
  ts2 <- list(
    mizu_submit(p[["ctrl"]], stop("nope")),
    mizu_submit(p[["ctrl"]], 1L)
  )
  o2 <- observe(mizu_collect_any_async(ts2))
  pool_step(p)
  expect_true(pump_until(function() settled(o2)))
  expect_s3_class(o2$error, "simpleError")
  expect_identical(o2$error[["index"]], 1L)
  pool_step(p)
  expect_identical(mizu_collect(ts2[[2L]], 5), 1L)
})

test_that("collect_any_async timeout rejects and consumes nothing", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  ts <- list(mizu_submit(p[["ctrl"]], 1L), mizu_submit(p[["ctrl"]], 2L))
  o <- observe(mizu_collect_any_async(ts, timeout = 0.1))
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "mizu_timeout")
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(ts[[1L]], 5), 1L)
  expect_identical(mizu_collect(ts[[2L]], 5), 2L)
})

test_that("enrollment conflicts are errors naming the pending consumer", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], 1L)
  o <- observe(promises::as.promise(t))
  expect_error(mizu_collect_all_async(list(t)), "pending consumer")
  expect_error(mizu_collect_any(list(t), 0), "pending consumer")
  expect_error(mizu_collect_all(list(t), 0), "pending consumer")
  pool_step(p)
  expect_true(pump_until(function() settled(o)))
  # a collection-enrolled task refuses a promise
  t2 <- mizu_submit(p[["ctrl"]], 2L)
  o2 <- observe(mizu_collect_all_async(list(t2, mizu_submit(p[["ctrl"]], 3L))))
  expect_error(promises::as.promise(t2), "second promise")
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() settled(o2)))
})

test_that("the collective async verbs validate their arguments", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  t <- mizu_submit(p[["ctrl"]], 1L)
  expect_snapshot(error = TRUE, mizu_collect_all_async(list(t), NA))
  expect_snapshot(error = TRUE, mizu_collect_all_async(list()))
  expect_snapshot(error = TRUE, mizu_collect_any_async(list()))
  expect_snapshot(error = TRUE, mizu_collect_any_async(list(t), NA))
  mizu_cancel(t)
})

test_that("unrelated waiters block neither new enrollments nor blocking collects", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  o1 <- observe(promises::as.promise(t1))
  t2 <- mizu_submit(p[["ctrl"]], 2L)
  o2 <- observe(mizu_collect_all_async(list(t2))) # a second enrollment coexists
  t3 <- mizu_submit(p[["ctrl"]], 3L)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(t3, 5), 3L) # not enrolled: collects fine
  expect_true(pump_until(function() settled(o1) && settled(o2)))
  expect_identical(o1$value, 1L)
  expect_identical(o2$value, list(2L))
})

test_that("collect_all_async on a stale task raises the pool's recorded error", {
  skip_if_no_promises()
  p <- pool_pair(result_slots = 8L, injection_cap = 8L) # one slot per submitter
  on.exit(pool_end(p))
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  pool_step(p)
  expect_identical(mizu_collect(t1, 5), 1L)
  t2 <- mizu_submit(p[["ctrl"]], 2L) # reclaims the slot, invalidating t1
  expect_error(
    mizu_collect_all_async(list(t1)),
    "collected or invalidated",
    class = "mizu_error"
  )
  mizu_cancel(t2)
})

test_that("a pool drain over the quantum reschedules and settles every waiter", {
  skip_if_no_promises()
  p <- pool_pair(
    max_submitters = 2L,
    injection_cap = 4096L,
    result_slots = 8192L
  )
  on.exit(pool_end(p))
  n <- mizu:::notify_quantum_n + 10L
  ts <- lapply(seq_len(n), function(i) mizu_submit(p[["ctrl"]], i, i = i))
  os <- lapply(ts, function(t) observe(promises::as.promise(t)))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() all(vapply(os, settled, TRUE)), 60))
  expect_identical(vapply(os, function(o) o$value, 1L), seq_len(n))
})

test_that("a timed-out collection releases its tasks for re-enrollment", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  ts <- list(mizu_submit(p[["ctrl"]], 1L), mizu_submit(p[["ctrl"]], 2L))
  o <- observe(mizu_collect_all_async(ts, timeout = 0.1))
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "mizu_timeout")
  o2 <- observe(promises::as.promise(ts[[1L]]))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() settled(o2)))
  expect_identical(o2$value, 1L)
  expect_identical(mizu_collect(ts[[2L]], 5), 2L)
})

test_that("an already-terminal task settles from the call site, never enrolled", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  ctrl <- p[["ctrl"]]
  t1 <- mizu_submit(ctrl, 1L)
  pool_step(p) # published before the enroll: the terminal-first probe
  o1 <- observe(promises::as.promise(t1))
  expect_identical(.Call(mizu:::mizu_pool_notify_count, ctrl), 0L)
  expect_true(pump_until(function() settled(o1)))
  expect_identical(o1$value, 1L)
  t2 <- mizu_submit(ctrl, 2L)
  t3 <- mizu_submit(ctrl, 3L)
  o2 <- observe(promises::as.promise(t2))
  o3 <- observe(mizu_collect_all_async(list(t3)))
  expect_identical(.Call(mizu:::mizu_pool_notify_count, ctrl), 2L)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() settled(o2) && settled(o3)))
  expect_identical(o2$value, 2L)
  expect_identical(o3$value, list(3L))
  expect_identical(.Call(mizu:::mizu_pool_notify_count, ctrl), 0L)
})

test_that("a pending task registers at the enroll and the drain unlinks it", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  ctrl <- p[["ctrl"]]
  # pending at the enroll: registered and armed; the publish wakes the
  # drain, whose scan settles and unlinks it
  t <- mizu_submit(ctrl, 7L)
  o <- observe(promises::as.promise(t))
  expect_identical(.Call(mizu:::mizu_pool_notify_count, ctrl), 1L)
  pool_step(p)
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 7L)
  expect_identical(.Call(mizu:::mizu_pool_notify_count, ctrl), 0L)
})

test_that("the notify .Call entries validate their handles", {
  p <- pool_pair()
  on.exit(pool_end(p))
  ctrl <- p[["ctrl"]]
  st <- mizu:::notify_pool_state(ctrl)
  w <- new.env()
  t1 <- mizu_submit(ctrl, 1L)
  q <- pool_pair()
  on.exit(pool_end(q), add = TRUE)
  t2 <- mizu_submit(q[["ctrl"]], 2L)
  enroll <- function(...) .Call(mizu:::mizu_pool_notify_enroll, ...)
  expect_error(enroll(1, st, w, list(t1), 0L, NULL), "not a pool handle")
  expect_error(enroll(ctrl, 1, w, list(t1), 0L, NULL), "waiter env")
  expect_error(enroll(ctrl, st, w, list(), 0L, NULL), "non-empty list")
  expect_error(enroll(ctrl, st, w, list(1), 0L, NULL), "not a task handle")
  expect_error(enroll(ctrl, st, w, list(t2), 0L, NULL), "same pool handle")
  expect_error(enroll(ctrl, st, w, list(t1), 4L, NULL), "invalid waiter mode")
  expect_error(.Call(mizu:::mizu_pool_notify_find, 1), "not a task handle")
  expect_error(.Call(mizu:::mizu_pool_notify_scan, 1, 64L), "not a pool handle")
  expect_error(.Call(mizu:::mizu_pool_notify_remove, 1, w), "not a pool handle")
  expect_error(.Call(mizu:::mizu_pool_notify_count, 1), "not a pool handle")
  expect_identical(.Call(mizu:::mizu_pool_notify_scan, ctrl, 64L), list())
  expect_false(.Call(mizu:::mizu_pool_notify_remove, ctrl, w))
  expect_null(.Call(mizu:::mizu_pool_notify_find, t1))
  expect_error(.Call(mizu:::mizu_task_pool, 1), "not a task handle")
  expect_error(.Call(mizu:::mizu_chan_notify_peek, 1), "not a channel handle")
  expect_error(
    .Call(mizu:::mizu_chan_notify_set, 1, new.env()),
    "not a channel handle"
  )
  expect_error(.Call(mizu:::mizu_pool_notify_peek, 1), "not a pool handle")
  expect_error(
    .Call(mizu:::mizu_pool_notify_set, 1, new.env()),
    "not a pool handle"
  )
  mizu_cancel(t1)
  mizu_cancel(t2)
})

test_that("a destroyed pool refuses an enroll and scans empty", {
  skip_if_no_promises()
  p <- pool_pair()
  ctrl <- p[["ctrl"]]
  st <- mizu:::notify_pool_state(ctrl)
  t <- mizu_submit(ctrl, 1L)
  o <- observe(promises::as.promise(t))
  pool_end(p)
  w <- new.env()
  expect_error(
    .Call(mizu:::mizu_pool_notify_enroll, ctrl, st, w, list(t), 0L, NULL),
    "pool handle is closed"
  )
  expect_identical(.Call(mizu:::mizu_pool_notify_scan, ctrl, 64L), list())
  expect_identical(.Call(mizu:::mizu_pool_notify_count, ctrl), 0L)
  expect_null(.Call(mizu:::mizu_pool_notify_find, t))
  expect_false(settled(o))
})

test_that("several workers publishing to one armed submitter send one signal", {
  skip_if_no_promises()
  p <- pool_pair(workers = 2L)
  on.exit(pool_end(p))
  t1 <- mizu_submit(p[["ctrl"]], 1L)
  t2 <- mizu_submit(p[["ctrl"]], 2L)
  o1 <- observe(promises::as.promise(t1))
  o2 <- observe(promises::as.promise(t2))
  pool_step(p, wk = p[["wks"]][[1L]])
  pool_step(p, wk = p[["wks"]][[2L]])
  expect_true(pump_until(function() settled(o1) && settled(o2)))
  sends <- sum(vapply(
    p[["wks"]],
    function(wk) mizu_pool_dump(wk)[["local"]][["notify_sends"]],
    0
  ))
  expect_identical(sends, 1)
})

# Handle-first map -------------------------------------------------------------

test_that("mizu_map returns a handle; mizu_collect gathers in input order", {
  p <- pool_pair()
  on.exit(pool_end(p))
  m <- mizu_map(p[["ctrl"]], 1:4, function(i) i * 2L)
  expect_s3_class(m, "mizu_map")
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(m), as.list((1:4) * 2L))
  expect_error(mizu_collect(m), "already collected")
})

test_that("a map promise resolves with the gather and refuses a blocking collect", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  m <- mizu_map(p[["ctrl"]], 1:4, function(i) i * 2L)
  o <- observe(promises::as.promise(m))
  expect_error(mizu_collect(m), "pending promise")
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, as.list((1:4) * 2L))
  # then() dispatches on the handle directly
  m2 <- mizu_map(p[["ctrl"]], 1:4, function(i) i + 1L)
  v <- NULL
  promises::then(m2, function(x) v <<- unlist(x))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() !is.null(v)))
  expect_identical(v, (1:4) + 1L)
})

test_that("mizu_cancel on a map handle: a promise rejects, a blocking collect raises", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  m <- mizu_map(p[["ctrl"]], 1:4, function(i) i * 2L)
  o <- observe(promises::as.promise(m))
  mizu_cancel(m)
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "mizu_error_cancelled")
  m2 <- mizu_map(p[["ctrl"]], 1:4, function(i) i * 2L)
  mizu_cancel(m2)
  expect_error(mizu_collect(m2), class = "mizu_error_cancelled")
})

test_that("mizu_map_run returns the handle form and re-arms after a clean collect", {
  p <- pool_pair()
  on.exit(pool_end(p))
  pm <- mizu_map_prepare(p[["ctrl"]], 1:4, function(i) i * 3L)
  m <- mizu_map_run(pm)
  expect_s3_class(m, "mizu_map")
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(m), as.list((1:4) * 3L))
  expect_false(is.null(pm[["st"]])) # clean: re-arms rather than restages
  m2 <- mizu_map_run(pm)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(m2), as.list((1:4) * 3L))
})

test_that("an empty map is a handle whose collect and promise resolve empty", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  m <- mizu_map(p[["ctrl"]], integer(0), identity)
  expect_s3_class(m, "mizu_map")
  expect_identical(mizu_collect(m), list())
  m2 <- mizu_map(p[["ctrl"]], integer(0), identity, .template = integer(1))
  o <- observe(promises::as.promise(m2))
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, integer(0))
})

test_that("a map promise is cached; collected and streaming handles refuse one", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  m <- mizu_map(p[["ctrl"]], 1:4, function(i) i * 2L)
  expect_identical(promises::as.promise(m), promises::as.promise(m))
  expect_output(print(m), "promise pending", fixed = TRUE)
  while (pool_step(p) == 1L) {
    NULL
  }
  o <- observe(promises::as.promise(m))
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, as.list((1:4) * 2L))
  m2 <- mizu_map(p[["ctrl"]], 1:4, function(i) i)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(m2), as.list(1:4))
  expect_error(promises::as.promise(m2), "already collected")
  ms <- mizu_map(p[["ctrl"]], 1:4, function(i) i, .stream = TRUE)
  expect_error(promises::as.promise(ms), "streaming map handle")
  mizu_cancel(ms)
})

test_that("a prepared map's clean promise collect hands the state back for re-arm", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  pm <- mizu_map_prepare(p[["ctrl"]], 1:4, function(i) i * 3L)
  m <- mizu_map_run(pm)
  expect_null(pm[["st"]]) # pessimistic while the run pends
  o <- observe(promises::as.promise(m))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, as.list((1:4) * 3L))
  expect_false(is.null(pm[["st"]])) # clean: the state is back for re-arm
  m2 <- mizu_map_run(pm)
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_identical(mizu_collect(m2), as.list((1:4) * 3L))
})

test_that("a map promise scans mixed runner states before the full settle", {
  skip_if_no_promises()
  p <- pool_pair(workers = 2L)
  on.exit(pool_end(p))
  m <- mizu_map(p[["ctrl"]], 1:4, function(i) i * 2L, .chunks = 2L)
  o <- observe(promises::as.promise(m))
  pool_step(p, wk = p[["wks"]][[1L]])
  later::run_now() # one runner terminal, one pending: the scan defers
  expect_false(settled(o))
  pool_step(p, wk = p[["wks"]][[2L]])
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, as.list((1:4) * 2L))
})

test_that("a map promise's watchdog re-arms while workers stay live", {
  skip_if_no_promises()
  p <- pool_pair()
  on.exit(pool_end(p))
  m <- mizu_map(p[["ctrl"]], 1:4, function(i) i)
  o <- observe(promises::as.promise(m))
  # nothing stepped: the watchdog ticks once over live workers and re-arms
  expect_false(pump_until(function() settled(o), 1.5))
  while (pool_step(p) == 1L) {
    NULL
  }
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, as.list(1:4))
})

# Cross-process ----------------------------------------------------------------

test_that("a spawned echo peer drives a promise and its death rejects one", {
  skip_if_no_promises()
  skip_if_no_child_mizu()
  ch <- mizu_channel(echo_expr)
  on.exit(.Call(mizu:::mizu_channel_destroy, ch), add = TRUE)
  o <- observe(mizu_recv_async(ch))
  mizu_send(ch, 42L)
  expect_true(pump_until(function() settled(o)))
  expect_identical(o$value, 42L)
  # a pending promise rejects when the peer dies (the death watch chains on)
  o2 <- observe(mizu_recv_async(ch))
  pid <- .Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]]
  kill_hard(pid)
  expect_true(pump_until(function() settled(o2)))
  expect_s3_class(o2$error, "mizu_peer_gone")
})

test_that("a declined Python payload rejects the waiter, reaches the handler", {
  skip_if_no_promises()
  py <- skip_if_no_pymizu()
  # a Python HOST that sends before any identity read stages same-language:
  # a set crosses as a pickle, which the R read hook declines
  token_file <- tempfile()
  src_file <- tempfile(fileext = ".py")
  writeLines(
    c(
      "from pymizu import _pymizu as pm",
      "import sys",
      "h = pm._channel_new(16384, 256, 4194304, False, b'Spass')",
      "with open(sys.argv[1], 'w') as f:",
      "    f.write(h.token + '\\n')",
      "h.send({1, 2, 3})",
      "h.send('ok')",
      "h.send({4, 5})",
      "h.send('ok2')",
      "h.recv(30)",
      "h.destroy()"
    ),
    src_file
  )
  system2(py, c(src_file, shQuote(token_file)), wait = FALSE)
  expect_true(wait_for_file(token_file))
  ch <- .Call(mizu:::mizu_channel_attach, readLines(token_file), NULL)[[1L]]
  on.exit(.Call(mizu:::mizu_channel_destroy, ch), add = TRUE)
  # promise mode: the decline rejects the head waiter, the channel flows
  o <- observe(mizu_recv_async(ch))
  expect_true(pump_until(function() settled(o)))
  expect_s3_class(o$error, "mizu_error_python_payload")
  o2 <- observe(mizu_recv_async(ch))
  expect_true(pump_until(function() settled(o2)))
  expect_identical(o2$value, "ok")
  # handler mode: the decline is delivered as the condition, and the
  # handler continues with the next message
  got <- new.env(parent = emptyenv())
  got$xs <- list()
  mizu_recv_handler(ch, function(x) got$xs[[length(got$xs) + 1L]] <<- x)
  expect_true(pump_until(function() length(got$xs) == 2L))
  expect_s3_class(got$xs[[1L]], "mizu_error_python_payload")
  expect_identical(got$xs[[2L]], "ok2")
  mizu_recv_handler(ch, NULL)
})

test_that("a batch handler fed by a real peer receives every message across quanta", {
  skip_if_no_promises()
  skip_if_no_child_mizu()
  ch <- mizu_channel(echo_expr)
  on.exit(mizu_close(ch), add = TRUE)
  n <- mizu:::notify_quantum_n + 20L
  got <- new.env(parent = emptyenv())
  got$xs <- list()
  mizu_recv_handler(
    ch,
    function(xs) got$xs <- c(got$xs, xs),
    batch = TRUE
  )
  mizu_send_batch(ch, as.list(seq_len(n)))
  expect_true(pump_until(function() length(got$xs) == n, 30))
  expect_identical(unlist(got$xs), seq_len(n))
  mizu_recv_handler(ch, NULL)
})

test_that("a killed worker rejects its promise-armed tasks; survivors unaffected", {
  skip_if_no_promises()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  on.exit(mizu_pool_stop(p), add = TRUE)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  t1 <- mizu_submit(p, {
    Sys.sleep(3)
    1L
  })
  t2 <- mizu_submit(p, {
    Sys.sleep(3)
    2L
  })
  o1 <- observe(promises::as.promise(t1))
  o2 <- observe(promises::as.promise(t2))
  # wait for the claims: one in-flight task per worker, so the kill below
  # deterministically fails exactly one task (a worker holds one claim)
  expect_true(wait_until(
    mizu_pool_status(p)[["injection"]] == 0L &&
      mizu_pool_status(p)[["parked"]] == 0L
  ))
  d <- mizu_pool_dump(p)[["workers"]]
  victim <- as.integer(d[d[["status"]] == "live", "pid"][[1L]])
  kill_hard(victim)
  expect_true(pump_until(
    function() settled(o1) && settled(o2),
    30
  ))
  # exactly one task was claimed by the dead worker and rejects
  errors <- Filter(Negate(is.null), lapply(list(o1, o2), function(o) o$error))
  values <- Filter(Negate(is.null), lapply(list(o1, o2), function(o) o$value))
  expect_length(errors, 1L)
  expect_s3_class(errors[[1L]], "mizu_error_worker_died")
  expect_length(values, 1L)
  # the pool keeps serving on the survivor
  t3 <- mizu_submit(p, 3L)
  expect_identical(mizu_collect(t3, 10), 3L)
})

test_that("a promise-armed mizu_map() over real workers resolves the full gather", {
  skip_if_no_promises()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  on.exit(mizu_pool_stop(p), add = TRUE)
  m <- mizu_map(p, 1:16, function(i) i * 2L, .seed = 42L)
  o <- observe(promises::as.promise(m))
  expect_true(pump_until(function() settled(o), 30))
  expect_identical(
    o$value,
    mizu_collect(mizu_map(p, 1:16, function(i) i * 2L, .seed = 42L))
  )
})

test_that("a map promise rejects when every worker dies before any claim", {
  skip_if_no_promises()
  skip_if_no_child_mizu()
  p <- mizu_pool(n_workers = 2L)
  on.exit(mizu_pool_stop(p), add = TRUE)
  expect_true(wait_until(mizu_pool_status(p)[["parked"]] == 2L))
  lapply(1:2, function(i) {
    mizu_submit(p, {
      Sys.sleep(10)
      i
    })
  })
  m <- mizu_map(p, runif(100000), identity)
  expect_null(m[["st"]][["blob"]]) # region map: the cancel word rides the region
  o <- observe(promises::as.promise(m))
  d <- mizu_pool_dump(p)[["workers"]]
  for (pid in d[d[["status"]] == "live", "pid"]) {
    kill_hard(as.integer(pid))
  }
  expect_true(pump_until(function() settled(o), 30))
  expect_s3_class(o$error, "mizu_error_worker_died")
})

test_that("a hard-killed armed consumer's .ntfy socket file survives mizu_prune()", {
  skip_if_no_promises()
  skip_if_no_child_mizu()
  skip_on_os(c("linux", "windows"))
  ready <- tfile()
  expr <- substitute(
    {
      pr <- mizu_recv_async(ch)
      writeLines("armed", con = READY)
      Sys.sleep(30)
    },
    list(READY = ready)
  )
  ch <- mizu_channel(expr)
  on.exit(.Call(mizu:::mizu_channel_destroy, ch), add = TRUE)
  suffix <- .Call(mizu:::mizu_channel_suffix, ch)
  expect_true(wait_for_file(ready))
  pid <- .Call(mizu:::mizu_channel_stat, ch)[["peer_pid"]]
  kill_hard(pid)
  expect_true(wait_until(!pid_alive(pid)))
  ntfy <- list.files(
    .Call(mizu:::mizu_live_dir_call),
    sprintf("^mizu_%s[.]ntfy", suffix),
    full.names = TRUE
  )
  expect_length(ntfy, 1L)
  mizu_prune()
  expect_true(file.exists(ntfy))
  unlink(ntfy)
})

test_that("watch soak: many arm/disarm cycles leave no leaked fds", {
  skip_if_no_promises()
  p <- channel_pair()
  on.exit(channel_end(p))
  for (i in 1:20) {
    o <- observe(mizu_recv_async(p[["host"]]))
    mizu_send(p[["peer"]], i)
    expect_true(pump_until(function() settled(o)))
  }
  gc()
  skip_on_os(c("mac", "windows"))
  before <- fd_count()
  for (i in 1:20) {
    o <- observe(mizu_recv_async(p[["host"]]))
    mizu_send(p[["peer"]], i)
    expect_true(pump_until(function() settled(o)))
  }
  gc()
  expect_lte(fd_count(), before)
})
