test_that("the golden corpus drives the R writer and builder", {
  skip_if_not_installed("bit64")
  cases <- ix_cases()
  corpus <- ix_corpus()

  for (row in seq_len(nrow(cases))) {
    id <- cases$id[[row]]
    kind <- cases$kind[[row]]
    langs <- cases$langs[[row]]
    value <- cases$value[[row]]
    note <- cases$note[[row]]
    if (!langs %in% c("all", "R")) next

    if (kind == "rt") {
      x <- ix_parse(value)
      expect_identical(ix_read(corpus[[id]]), x, info = id)
      expect_identical(ix_write(x), unname(corpus[[id]]), info = id)
    } else if (kind == "dec") {
      home <- sub("^.* => ", "", value)
      x <- ix_parse(home)
      expect_identical(ix_read(corpus[[id]]), x, info = id)
    } else if (kind == "enc") {
      x <- ix_parse(value)
      expect_identical(ix_write(x), unname(corpus[[id]]), info = id)
    } else if (kind == "read-err") {
      # cursor-level rows reject in the core cursor, builder-level in
      # the shape builder; both are errors here (the levels are split in
      # libmizu's C conformance test and this file's snapshot tests)
      expect_error(ix_read(corpus[[id]]), info = id)
    } else if (kind == "write-decline") {
      x <- ix_parse(value)
      expect_error(ix_write(x), info = id)
    }
  }
})

test_that("corpus read-err texts stay informative", {
  corpus <- ix_corpus()
  expect_snapshot(ix_read(corpus[["err-unknown-tag"]]), error = TRUE)
  expect_snapshot(ix_read(corpus[["err-version"]]), error = TRUE)
  expect_snapshot(ix_read(corpus[["err-task-kind"]]), error = TRUE)
  expect_snapshot(ix_read(corpus[["err-trailing"]]), error = TRUE)
  expect_snapshot(ix_read(corpus[["err-dup-key"]]), error = TRUE)
  expect_snapshot(ix_read(corpus[["err-frame-shape"]]), error = TRUE)
  expect_snapshot(ix_read(corpus[["err-attr-unknown"]]), error = TRUE)
  expect_snapshot(ix_read(corpus[["err-date-fractional"]]), error = TRUE)
})
