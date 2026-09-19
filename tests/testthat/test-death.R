# Per-process death listener: translates a watched pid's exit into a fired
# flag plus a directed unpark, at OS notification latency. The listener is
# only ever a wake trigger — the liveness lock is the verdict.

test_that("watching an already-dead pid fires immediately", {
  skip_on_os("windows")
  dead <- as.integer(system("echo $$", intern = TRUE)) # shell already exited
  w <- .Call(mizu:::mizu_death_watch_call, dead, NULL, 0L, FALSE)
  expect_true(wait_until(.Call(mizu:::mizu_death_fired_call, w)))
  .Call(mizu:::mizu_death_stop_call, w)
  expect_error(.Call(mizu:::mizu_death_fired_call, w), "stopped")
})

test_that("death-watch stop validates its handle", {
  expect_error(.Call(mizu:::mizu_death_stop_call, 42), "not a death-watch")
  expect_error(.Call(mizu:::mizu_death_stop_call, NULL), "not a death-watch")
  xp <- .Call(mizu:::mizu_region_create, 4096)
  expect_error(.Call(mizu:::mizu_death_stop_call, xp), "not a death-watch")
})

test_that("a live process's exit sets the fired flag", {
  skip_on_os("windows")
  pid <- as.integer(system("sleep 30 >/dev/null 2>&1 & echo $!", intern = TRUE))
  w <- .Call(mizu:::mizu_death_watch_call, pid, NULL, 0L, FALSE)
  expect_false(.Call(mizu:::mizu_death_fired_call, w))

  tools::pskill(pid)
  expect_true(wait_until(.Call(mizu:::mizu_death_fired_call, w)))
  .Call(mizu:::mizu_death_stop_call, w)
})

test_that("peer death unparks a parked waiter", {
  skip_on_os("windows")
  pid <- as.integer(system("sleep 30 >/dev/null 2>&1 & echo $!", intern = TRUE))
  xp <- .Call(mizu:::mizu_region_create, 4096)
  w <- .Call(mizu:::mizu_death_watch_call, pid, xp, 0L, TRUE)
  expect_false(.Call(mizu:::mizu_death_fired_call, w))

  # kill lands while parked; the listener converts it into a directed unpark
  system(sprintf("(sleep 0.3; kill %d) >/dev/null 2>&1", pid), wait = FALSE)
  rc <- .Call(mizu:::mizu_park_call, xp, 0L, 30000L, TRUE)
  expect_identical(rc, 0L)
  expect_true(.Call(mizu:::mizu_death_fired_call, w))
  .Call(mizu:::mizu_death_stop_call, w)
})

test_that("stopped watches are inert and handles single-shot", {
  skip_on_os("windows")
  pid <- as.integer(system("sleep 30 >/dev/null 2>&1 & echo $!", intern = TRUE))
  w <- .Call(mizu:::mizu_death_watch_call, pid, NULL, 0L, FALSE)
  .Call(mizu:::mizu_death_stop_call, w)
  expect_error(.Call(mizu:::mizu_death_fired_call, w), "stopped")
  tools::pskill(pid)
})

test_that("death-watch entries reject foreign handles", {
  # mizu_death_stop_call has no handle_get guard of its own (it is the
  # finalizer direct) — only the fired probe validates
  expect_error(
    .Call(mizu:::mizu_death_fired_call, NULL),
    "not a death-watch handle"
  )
  p <- channel_pair()
  expect_error(
    .Call(mizu:::mizu_death_fired_call, p[["host"]]),
    "not a death-watch handle"
  )
  channel_end(p)
})
