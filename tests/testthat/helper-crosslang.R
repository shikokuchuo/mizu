# Cross-language tests need a python3 with pyrei and NumPy importable — the
# same guard the reference vignette's Python interop chunk evaluates under.
pyrei_ok <- local({
  val <- NULL
  function(py) {
    if (is.null(val)) {
      val <<- nzchar(py) &&
        system2(
          py,
          c("-c", shQuote("import pyrei, numpy")),
          stdout = FALSE,
          stderr = FALSE
        ) ==
          0L
    }
    val
  }
})

skip_if_no_pyrei <- function() {
  py <- Sys.which("python3")
  testthat::skip_if_not(
    pyrei_ok(py),
    "python3 with pyrei and numpy not available"
  )
  py
}

# The Python echo peer: the mirror of echo_expr.
py_echo <- "
import pyrei
while True:
    x = ch.recv(30)
    if pyrei.is_sentinel(x):
        break
    ch.send(x)
"
