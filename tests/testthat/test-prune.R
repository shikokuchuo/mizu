# sora_prune over the vendored reaper: reclaims /sora_ regions orphaned by dead
# creators (a hard-killed process runs no finalizers).

test_that("sora_prune returns NULL when there is nothing to reap", {
  sora_prune()                      # clear leftovers of earlier crashed runs
  expect_null(sora_prune())
})

test_that("sora_prune reaps the orphans of a hard-killed process", {
  skip_on_os("windows")            # Win32 mappings cannot outlive their creator
  skip_if_no_child_sora()
  f <- tfile()

  sora:::sora_spawn(sprintf('
    library(sora)
    xp <- .Call(sora:::sora_region_create, 4096)
    tmp <- paste0(%s, ".tmp")
    writeLines(.Call(sora:::sora_region_name, xp), tmp)
    file.rename(tmp, %s)
    Sys.sleep(0.2)                       # let the host observe the orphan
    tools::pskill(Sys.getpid(), tools::SIGKILL)
  ', deparse(f), deparse(f)))

  expect_true(wait_for_file(f))
  nm <- readLines(f)

  # the orphan is openable regardless of whether the kill has landed yet
  ro <- .Call(sora:::sora_region_open, nm, FALSE)
  expect_gte(.Call(sora:::sora_region_size, ro), 4096)

  # after SIGKILL: the creator ran no finalizers, so only the reaper can
  # reclaim the name (poll: the kill has to land first)
  reaped <- character()
  expect_true(wait_until({
    reaped <- c(reaped, .Call(sora:::sora_prune_call))
    nm %in% reaped
  }))
  expect_error(.Call(sora:::sora_region_open, nm, FALSE), "cannot open")
})
