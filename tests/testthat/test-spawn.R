# Child-spawn helper: system2(Rscript, "-e", ...) with the host's .libPaths()
# propagated via R_LIBS — set, spawn, restore.

test_that("mov_spawn propagates the host library paths and restores R_LIBS", {
  f <- tfile()
  old <- Sys.getenv("R_LIBS", unset = NA)

  mov:::mov_spawn(sprintf('
    tmp <- paste0(%s, ".tmp")
    writeLines(strsplit(Sys.getenv("R_LIBS"), .Platform$path.sep)[[1]], tmp)
    file.rename(tmp, %s)
  ', deparse(f), deparse(f)))

  expect_identical(Sys.getenv("R_LIBS", unset = NA), old)   # restored
  expect_true(wait_for_file(f))
  child_libs <- normalizePath(readLines(f), mustWork = FALSE)
  host_libs <- normalizePath(.libPaths(), mustWork = FALSE)
  expect_true(all(host_libs %in% child_libs))
})

test_that("mov_spawn validates its input", {
  expect_error(mov:::mov_spawn(42))
  expect_error(mov:::mov_spawn(c("a", "b")))
  expect_error(mov:::mov_spawn(NA_character_))
})

test_that("spawn_peer admits only prefix-stripped region-name suffixes", {
  expect_error(mov:::spawn_peer("evil'; echo pwned"))
  expect_error(mov:::spawn_peer("/mov_1a2b_3c4d"))       # full name, not suffix
  expect_error(mov:::spawn_peer("1A2B_3C4D"))            # uppercase hex
})
