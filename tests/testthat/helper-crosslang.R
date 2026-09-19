# Cross-language tests need a python3 with pymizu and NumPy importable — the
# same guard the reference vignette's Python interop chunk evaluates under.
pymizu_ok <- local({
  val <- NULL
  function(py) {
    if (is.null(val)) {
      val <<- nzchar(py) &&
        system2(
          py,
          c("-c", shQuote("import pymizu, numpy")),
          stdout = FALSE,
          stderr = FALSE
        ) ==
          0L
    }
    val
  }
})

skip_if_no_pymizu <- function() {
  py <- Sys.which("python3")
  testthat::skip_if_not(
    pymizu_ok(py),
    "python3 with pymizu and numpy not available"
  )
  py
}

# The Python echo peer: the mirror of echo_expr.
py_echo <- "
import pymizu
while True:
    x = ch.recv(30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x)
"
