# Cross-process pools: spawned workers driving the full submit -> execute ->
# collect path (the end-to-end SHM round-trip the phase must prove from R),
# the startup rendezvous and its walk-back, blocked collectors woken by
# publish, cross-process submitter attach, and orderly stop.

test_that("a spawned worker round-trips every payload kind", {
  skip_if_no_child_rei()
  p <- rei_pool()
  tasks <- list(
    rei_submit(p, x * 2L, x = 21L), # RAWVEC in and out
    rei_submit(p, c(l, list(b = 2)), l = list(a = 1)), # INLINE
    rei_submit(p, sum(v), v = runif(100000)), # SHM_RAW task
    rei_submit(p, seq_len(n) + 0, n = 100000L) # SHM_RAW result
  )
  expect_identical(rei_collect(tasks[[1L]], timeout = 30), 42L)
  expect_identical(rei_collect(tasks[[2L]], timeout = 30), list(a = 1, b = 2))
  expect_type(rei_collect(tasks[[3L]], timeout = 30), "double")
  expect_identical(
    rei_collect(tasks[[4L]], timeout = 30),
    as.double(seq_len(100000L))
  )
  st <- rei_pool_status(p)
  expect_identical(st[["workers"]], "live")
  expect_identical(unname(st[["tasks"]]), rep(0L, 5L))
  expect_true(rei_pool_stop(p, timeout = 10))
  expect_error(rei_submit(p, 1), "pool handle is closed")
})

test_that("an untransportable task error flattens instead of killing the worker", {
  skip_if_no_child_rei()
  p <- rei_pool()
  t <- rei_submit(p, {
    stop(structure(
      list(message = "typed", call = NULL, payload = new.env()),
      class = c("rei_test_error", "error", "condition")
    ))
  })
  err <- tryCatch(rei_collect(t, timeout = 30), error = identity)
  expect_s3_class(err, "rei_test_error")
  expect_identical(conditionMessage(err), "typed")
  expect_identical(err[["dropped_fields"]], "payload")
  # the publish cannot fail: the worker runs on
  expect_identical(rei_collect(rei_submit(p, 42L), timeout = 30), 42L)
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("submit captures expr and args through forwarding wrappers", {
  skip_if_no_child_rei()
  p <- rei_pool()
  g <- function(...) rei_submit(p, ...)
  h <- function(...) g(...)
  expect_identical(rei_collect(g(sum(x), x = 1:3), timeout = 30), 6L)
  expect_identical(rei_collect(h(sum(x), x = 1:3), timeout = 30), 6L)
  forced <- function(...) {
    list(...) # forces the dots: capture still yields the expression
    rei_submit(p, ...)
  }
  expect_identical(rei_collect(forced(1L + 1L, x = 1:3), timeout = 30), 2L)
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("submit capture matches substitute() non-chase and edge cases", {
  skip_if_no_child_rei()
  p <- rei_pool()
  w <- function(e) rei_submit(p, e) # base substitute() does not chase
  t <- w(1 + 2)
  expect_error(rei_collect(t, timeout = 30), "object 'e' not found")
  expect_error(
    # a missing expr crosses; the worker's eval raises
    rei_collect(rei_submit(p), timeout = 30),
    "argument is missing"
  )
  expect_error(rei_submit(p, sum(x), 1:3), "must be named")
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("collect_all round-trips a heterogeneous set across processes", {
  skip_if_no_child_rei()
  p <- rei_pool(2L)
  tasks <- list(
    a = rei_submit(p, x * 2L, x = 21L),
    b = rei_submit(p, "done"),
    c = rei_submit(p, sum(v), v = runif(100000))
  )
  res <- rei_collect_all(tasks, timeout = 30)
  expect_identical(names(res), c("a", "b", "c"))
  expect_identical(res[["a"]], 42L)
  expect_identical(res[["b"]], "done")
  expect_type(res[["c"]], "double")
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("a blocked collect is woken by the worker's publish", {
  skip_if_no_child_rei()
  p <- rei_pool()
  t <- rei_submit(p, {
    Sys.sleep(0.3)
    "woken"
  })
  # parked well before the result publishes; the directed unpark ends it
  expect_identical(rei_collect(t, timeout = 30), "woken")
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("a task error carries its condition across processes", {
  skip_if_no_child_rei()
  p <- rei_pool()
  t <- rei_submit(p, stop("worker-side ", x), x = "failure")
  err <- tryCatch(rei_collect(t, timeout = 30), error = identity)
  expect_s3_class(err, "error")
  expect_identical(conditionMessage(err), "worker-side failure")
  # the worker's loop re-enters after publishing the failure: the same
  # (only) worker serves the next task
  expect_identical(rei_collect(rei_submit(p, "alive"), timeout = 30), "alive")
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("a second process attaches as a submitter and collects", {
  skip_on_cran()
  skip_if_no_child_rei()
  p <- rei_pool()
  suffix <- .Call(rei:::rei_pool_suffix, p)
  f <- tfile()
  rei:::rei_spawn(sprintf(
    '
    q <- rei::rei_pool_attach("%s")
    t <- rei::rei_submit(q, x * 2L, x = 21L)
    writeLines(as.character(rei::rei_collect(t, timeout = 30)), %s)
  ',
    suffix,
    deparse(f)
  ))
  expect_true(wait_for_file(f, timeout = 30))
  expect_true(wait_until(identical(readLines(f), "42")))
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("two processes submit concurrently to one pool", {
  skip_on_cran() # host + attached submitter + 2 workers exceeds 2 cores
  skip_if_no_child_rei()
  p <- rei_pool(2L)
  suffix <- .Call(rei:::rei_pool_suffix, p)
  ready <- tfile()
  done <- tfile()
  rei:::rei_spawn(sprintf(
    '
    q <- rei::rei_pool_attach("%s")
    file.create(%s)
    r <- vapply(1:20, function(i)
      rei::rei_collect(rei::rei_submit(q, x + 1L, x = i), timeout = 30),
      integer(1))
    writeLines(as.character(sum(r)), %s)
  ',
    suffix,
    deparse(ready),
    deparse(done)
  ))
  # the handshake guarantees overlap: the host's round trips below run
  # while the attached submitter drives its own, each ring consumed by
  # both workers, results routed to each submitter's own slot partition
  expect_true(wait_for_file(ready, timeout = 30))
  r <- vapply(
    1:20,
    function(i) {
      rei_collect(rei_submit(p, x * 2L, x = i), timeout = 30)
    },
    integer(1)
  )
  expect_identical(r, (1:20) * 2L)
  expect_true(wait_for_file(done, timeout = 30))
  expect_true(wait_until(identical(readLines(done), "230")))
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("mori-shared task arguments map zero-copy in the worker", {
  skip_if_no_child_rei()
  skip_if_not_installed("mori")
  p <- rei_pool()
  x <- mori::share(runif(1000))
  t <- rei_submit(
    p,
    list(
      shared = mori::is_shared(x),
      name = mori::shared_name(x),
      total = sum(x)
    ),
    x = x
  )
  info <- rei_collect(t, timeout = 30)
  expect_true(info[["shared"]])
  expect_identical(info[["name"]], mori::shared_name(x))
  expect_equal(info[["total"]], sum(x))
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("the startup deadline walks the pool back", {
  err <- tryCatch(
    rei_pool(launcher = function(token, slot) NULL, startup_timeout = 0.5),
    error = identity
  )
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "failed to attach")
})

test_that("a custom launcher receives the token and slot", {
  skip_if_no_child_rei()
  seen <- NULL
  p <- rei_pool(launcher = function(token, slot) {
    seen <<- list(token = token, slot = slot)
    rei:::spawn_worker(token, slot)
  })
  expect_match(seen[["token"]], "^[0-9a-f]+_[0-9a-f]+$")
  expect_identical(seen[["slot"]], 0L)
  t <- rei_submit(p, "via launcher")
  expect_identical(rei_collect(t, timeout = 30), "via launcher")
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("stop cancels a pending task and the worker exits cleanly", {
  skip_if_no_child_rei()
  p <- rei_pool()
  t <- rei_submit(p, Sys.sleep(0.2))
  t2 <- rei_submit(p, "queued behind")
  # the worker is mid-sleep on t; t2 is still queued when stop broadcasts
  expect_true(rei_pool_stop(p, timeout = 10))
  expect_error(rei_collect(t2, timeout = 5), "pool handle is closed")
})

test_that("a second worker picks up tasks while the first is busy", {
  skip_on_cran()
  skip_if_no_child_rei()
  p <- rei_pool(n_workers = 2L)
  expect_identical(rei_pool_status(p)[["workers"]], c("live", "live"))
  slow <- rei_submit(p, {
    Sys.sleep(1)
    Sys.getpid()
  })
  # once slow is claimed, the quick tasks can only land on the other worker
  expect_true(wait_until(any(
    rei_pool_dump(p)[["workers"]][["in_flight"]] != -1L
  )))
  quick <- lapply(1:5, function(i) rei_submit(p, Sys.getpid()))
  pids <- vapply(quick, rei_collect, integer(1), timeout = 30)
  expect_identical(
    length(unique(c(pids, rei_collect(slow, timeout = 30)))),
    2L
  )
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("repeated submit/collect cycles park and wake without loss", {
  skip_on_cran()
  skip_if_no_child_rei()
  p <- rei_pool(n_workers = 2L)
  # an idle pool parks both workers; each cycle below is a fresh wake —
  # a lost wake in the handshake surfaces as a collect timeout
  expect_true(wait_until(rei_pool_status(p)[["parked"]] == 2L))
  for (i in 1:50) {
    t <- rei_submit(p, x + 1L, x = i)
    expect_identical(rei_collect(t, timeout = 10), i + 1L)
  }
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("nested fan-out runs with help mode and stealing live", {
  skip_on_cran()
  skip_if_no_child_rei()
  p <- rei_pool(n_workers = 2L)
  # the outer worker pushes eight subtasks and help-pops from its bottom
  # while the second worker steals from its top
  t <- rei_submit(p, {
    subs <- lapply(1:8, function(i) {
      rei_submit(
        pool,
        {
          Sys.sleep(0.05)
          i * 2L
        },
        i = i
      )
    })
    sum(vapply(subs, function(s) rei_collect(s, timeout = 30), integer(1)))
  })
  expect_identical(rei_collect(t, timeout = 30), 72L)
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("a trace-hook error takes the worker down as infrastructure", {
  skip_if_no_child_rei()
  errfile <- tfile()
  p <- rei_pool(launcher = rei_launcher(stderr = errfile))
  t <- rei_submit(
    p,
    rei_pool_trace(pool, function(event, id) {
      if (event == "done") stop("hook boom")
    })
  )
  # the install task's own publish fires the hook it installed: the result
  # is already out, then the error — outside any task eval — is fatal
  expect_null(rei_collect(t, timeout = 30))
  # on Windows the live worker holds its stderr file with no read sharing;
  # it becomes readable only once the dying worker releases the handle
  read_err <- function() {
    tryCatch(readLines(errfile, warn = FALSE), error = function(e) character())
  }
  expect_true(wait_until(
    any(grepl("rei worker error: hook boom", read_err())),
    timeout = 30
  ))
  # the dying worker released its slot on the way out
  expect_true(wait_until(
    rei_pool_status(p)[["workers"]] == "free",
    timeout = 30
  ))
  expect_true(rei_pool_stop(p, timeout = 10))
})

test_that("a full ring parks the submitter until a worker's pop wakes it", {
  skip_if_no_child_rei()
  p <- rei_pool(injection_cap = 4L, result_slots = 256L)
  # 32 submissions through a 4-slot ring: most block on full_waiters and
  # are woken directly by the consuming worker
  tasks <- lapply(1:32, function(i) {
    rei_submit(p, i * 2L, i = i, .timeout = 30)
  })
  vals <- vapply(tasks, rei_collect, integer(1), timeout = 30)
  expect_identical(vals, (1:32) * 2L)
  expect_true(rei_pool_stop(p, timeout = 10))
})
