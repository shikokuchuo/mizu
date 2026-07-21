# Per-process death listener: translates a watched pid's exit into a fired
# flag plus a directed unpark, at OS notification latency. The listener is
# only ever a wake trigger — the liveness lock is the verdict.

test_that("watching an already-dead pid fires immediately", {
  skip_on_os("windows")
  dead <- as.integer(system("echo $$", intern = TRUE))   # shell already exited
  w <- .Call(mov:::mov_death_watch_call, dead, NULL, 0L, FALSE)
  expect_true(wait_until(.Call(mov:::mov_death_fired_call, w)))
  .Call(mov:::mov_death_stop_call, w)
  expect_error(.Call(mov:::mov_death_fired_call, w), "stopped")
})

test_that("a live process's exit sets the fired flag", {
  skip_on_os("windows")
  pid <- as.integer(system("sleep 30 >/dev/null 2>&1 & echo $!", intern = TRUE))
  w <- .Call(mov:::mov_death_watch_call, pid, NULL, 0L, FALSE)
  expect_false(.Call(mov:::mov_death_fired_call, w))

  tools::pskill(pid)
  expect_true(wait_until(.Call(mov:::mov_death_fired_call, w)))
  .Call(mov:::mov_death_stop_call, w)
})

test_that("peer death unparks a parked waiter", {
  skip_on_os("windows")
  pid <- as.integer(system("sleep 30 >/dev/null 2>&1 & echo $!", intern = TRUE))
  xp <- .Call(mov:::mov_region_create, 4096)
  w <- .Call(mov:::mov_death_watch_call, pid, xp, 0L, TRUE)
  expect_false(.Call(mov:::mov_death_fired_call, w))

  # kill lands while parked; the listener converts it into a directed unpark
  system(sprintf("(sleep 0.3; kill %d) >/dev/null 2>&1", pid), wait = FALSE)
  t0 <- proc.time()[[3]]
  rc <- .Call(mov:::mov_park_call, xp, 0L, 30000L, TRUE)
  elapsed <- proc.time()[[3]] - t0
  expect_identical(rc, 0L)
  expect_lt(elapsed, 25)
  expect_true(.Call(mov:::mov_death_fired_call, w))
  .Call(mov:::mov_death_stop_call, w)
})

test_that("stopped watches are inert and handles single-shot", {
  skip_on_os("windows")
  pid <- as.integer(system("sleep 30 >/dev/null 2>&1 & echo $!", intern = TRUE))
  w <- .Call(mov:::mov_death_watch_call, pid, NULL, 0L, FALSE)
  .Call(mov:::mov_death_stop_call, w)
  expect_error(.Call(mov:::mov_death_fired_call, w), "stopped")
  tools::pskill(pid)
})
