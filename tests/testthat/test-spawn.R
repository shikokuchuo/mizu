# Child-spawn helper: a static Rscript runner with the expression and the
# host's .libPaths() carried hex-encoded in argv — no host state mutated.

test_that("mizu_spawn propagates the host library paths via argv", {
  f <- tfile()
  old <- Sys.getenv("R_LIBS", unset = NA)

  mizu:::mizu_spawn(sprintf(
    '
    tmp <- paste0(%s, ".tmp")
    writeLines(.libPaths(), tmp)
    file.rename(tmp, %s)
  ',
    deparse(f),
    deparse(f)
  ))

  expect_identical(Sys.getenv("R_LIBS", unset = NA), old) # never touched
  expect_true(wait_for_file(f))
  child_libs <- normalizePath(readLines(f), mustWork = FALSE)
  host_libs <- normalizePath(.libPaths(), mustWork = FALSE)
  expect_true(all(host_libs %in% child_libs))
})

test_that("mizu_spawn runs a static script instead of Rscript -e", {
  f <- tfile()
  mizu:::mizu_spawn(sprintf("writeLines(commandArgs(FALSE), %s)", deparse(f)))
  expect_true(wait_for_file(f))
  args <- readLines(f)
  expect_false("-e" %in% args)
  expect_true(any(startsWith(args, "--file=")))
})

test_that("mizu_spawn round-trips expressions with quoting hazards", {
  f <- tfile()
  payload <- "a'b\"c\\d e\tf $PATH `id` %x% é"
  mizu:::mizu_spawn(sprintf("writeLines(%s, %s)", deparse(payload), deparse(f)))
  expect_true(wait_for_file(f))
  expect_identical(readLines(f, encoding = "UTF-8"), payload)
})

test_that("mizu_spawn validates its input", {
  expect_error(mizu:::mizu_spawn(42))
  expect_error(mizu:::mizu_spawn(c("a", "b")))
  expect_error(mizu:::mizu_spawn(NA_character_))
  expect_error(mizu:::mizu_spawn(""))
})

test_that("spawn_peer admits only prefix-stripped region-name suffixes", {
  expect_error(mizu:::spawn_peer("evil'; echo pwned"))
  expect_error(mizu:::spawn_peer("/mizu_1a2b_3c4d")) # full name, not suffix
  expect_error(mizu:::spawn_peer("1A2B_3C4D")) # uppercase hex
})

test_that("the child runner resolves to inst/ in a source layout", {
  root <- tempfile()
  dir.create(file.path(root, "inst", "scripts"), recursive = TRUE)
  file.create(file.path(root, "inst", "scripts", "mizu-child.R"))
  expect_identical(
    mizu:::mizu_child_script(root),
    file.path(root, "inst", "scripts", "mizu-child.R")
  )
  dir.create(file.path(root, "scripts"))
  file.create(file.path(root, "scripts", "mizu-child.R"))
  expect_identical(
    mizu:::mizu_child_script(root),
    file.path(root, "scripts", "mizu-child.R")
  )
})

test_that("mizu_py_launcher probes the interpreter for pymizu", {
  expect_snapshot(
    mizu_py_launcher(python = "/nonexistent/python3"),
    error = TRUE
  )
})

test_that("mizu_py_launcher needs a python3 on the PATH by default", {
  old <- Sys.getenv("PATH")
  Sys.setenv(PATH = tempdir())
  out <- tryCatch(mizu_py_launcher(), error = conditionMessage)
  Sys.setenv(PATH = old)
  expect_snapshot(out)
})

test_that("mizu_py_pool_launcher probes the interpreter for pymizu", {
  expect_snapshot(
    mizu_py_pool_launcher(python = "/nonexistent/python3"),
    error = TRUE
  )
})

test_that("mizu_py_pool_launcher needs a python3 on the PATH by default", {
  old <- Sys.getenv("PATH")
  Sys.setenv(PATH = tempdir())
  out <- tryCatch(mizu_py_pool_launcher(), error = conditionMessage)
  Sys.setenv(PATH = old)
  expect_snapshot(out)
})

test_that("spawn_worker validates the join token and slot", {
  expect_error(
    mizu:::spawn_worker("evil'; echo pwned", 0L),
    "malformed join token"
  )
  expect_error(mizu:::spawn_worker("1a2b_3c4d", -1L), "malformed join token")
})
