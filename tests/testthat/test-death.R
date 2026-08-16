# Per-process death listener: translates a watched pid's exit into a fired
# flag plus a directed unpark, at OS notification latency. The listener is
# only ever a wake trigger — the liveness lock is the verdict.

test_that("watching an already-dead pid fires immediately", {
  skip_on_os("windows")
  dead <- as.integer(system("echo $$", intern = TRUE)) # shell already exited
  w <- .Call(sora:::sora_death_watch_call, dead, NULL, 0L, FALSE)
  expect_true(wait_until(.Call(sora:::sora_death_fired_call, w)))
  .Call(sora:::sora_death_stop_call, w)
  expect_error(.Call(sora:::sora_death_fired_call, w), "stopped")
})

test_that("a live process's exit sets the fired flag", {
  skip_on_os("windows")
  pid <- as.integer(system("sleep 30 >/dev/null 2>&1 & echo $!", intern = TRUE))
  w <- .Call(sora:::sora_death_watch_call, pid, NULL, 0L, FALSE)
  expect_false(.Call(sora:::sora_death_fired_call, w))

  tools::pskill(pid)
  expect_true(wait_until(.Call(sora:::sora_death_fired_call, w)))
  .Call(sora:::sora_death_stop_call, w)
})

test_that("peer death unparks a parked waiter", {
  skip_on_os("windows")
  pid <- as.integer(system("sleep 30 >/dev/null 2>&1 & echo $!", intern = TRUE))
  xp <- .Call(sora:::sora_region_create, 4096)
  w <- .Call(sora:::sora_death_watch_call, pid, xp, 0L, TRUE)
  expect_false(.Call(sora:::sora_death_fired_call, w))

  # kill lands while parked; the listener converts it into a directed unpark
  system(sprintf("(sleep 0.3; kill %d) >/dev/null 2>&1", pid), wait = FALSE)
  rc <- .Call(sora:::sora_park_call, xp, 0L, 30000L, TRUE)
  expect_identical(rc, 0L)
  expect_true(.Call(sora:::sora_death_fired_call, w))
  .Call(sora:::sora_death_stop_call, w)
})

test_that("stopped watches are inert and handles single-shot", {
  skip_on_os("windows")
  pid <- as.integer(system("sleep 30 >/dev/null 2>&1 & echo $!", intern = TRUE))
  w <- .Call(sora:::sora_death_watch_call, pid, NULL, 0L, FALSE)
  .Call(sora:::sora_death_stop_call, w)
  expect_error(.Call(sora:::sora_death_fired_call, w), "stopped")
  tools::pskill(pid)
})

test_that("death-watch entries reject foreign handles", {
  # sora_death_stop_call has no handle_get guard of its own (it is the
  # finalizer direct) — only the fired probe validates
  expect_error(
    .Call(sora:::sora_death_fired_call, NULL),
    "not a death-watch handle"
  )
  p <- channel_pair()
  expect_error(
    .Call(sora:::sora_death_fired_call, p[["host"]]),
    "not a death-watch handle"
  )
  channel_end(p)
})
