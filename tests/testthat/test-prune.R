# mizu_prune over the vendored reaper: reclaims /mizu_ regions orphaned by dead
# creators (a hard-killed process runs no finalizers).

test_that("mizu_prune returns NULL when there is nothing to reap", {
  mizu_prune() # clear leftovers of earlier crashed runs
  expect_null(mizu_prune())
})

test_that("mizu_prune reaps the orphans of a hard-killed process", {
  skip_on_os("windows") # Win32 mappings cannot outlive their creator
  skip_if_no_child_mizu()
  skip_if_no_reaper() # a zombie's PID is not free for the reaper
  f <- tfile()

  mizu:::mizu_spawn(sprintf(
    '
    library(mizu)
    xp <- .Call(mizu:::mizu_region_create, 4096)
    tmp <- paste0(%s, ".tmp")
    writeLines(.Call(mizu:::mizu_region_name, xp), tmp)
    file.rename(tmp, %s)
    Sys.sleep(0.2)                       # let the host observe the orphan
    tools::pskill(Sys.getpid(), tools::SIGKILL)
  ',
    deparse(f),
    deparse(f)
  ))

  expect_true(wait_for_file(f))
  nm <- readLines(f)

  # the orphan is openable regardless of whether the kill has landed yet
  ro <- .Call(mizu:::mizu_region_open, nm, FALSE)
  expect_gte(.Call(mizu:::mizu_region_size, ro), 4096)

  # after SIGKILL: the creator ran no finalizers, so only the reaper can
  # reclaim the name (poll: the kill has to land first)
  reaped <- character()
  expect_true(wait_until({
    reaped <- c(reaped, .Call(mizu:::mizu_prune_call))
    nm %in% reaped
  }))
  expect_error(.Call(mizu:::mizu_region_open, nm, FALSE), "cannot open")
})
