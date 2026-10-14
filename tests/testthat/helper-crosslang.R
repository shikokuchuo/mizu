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

# The Arrow exchange tests additionally need pyarrow in the peer.
pyarrow_ok <- local({
  val <- NULL
  function(py) {
    if (is.null(val)) {
      val <<- nzchar(py) &&
        system2(
          py,
          c("-c", shQuote("import pyarrow")),
          stdout = FALSE,
          stderr = FALSE
        ) ==
          0L
    }
    val
  }
})

skip_if_no_pyarrow <- function(py) {
  testthat::skip_if_not(pyarrow_ok(py), "pyarrow not available")
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

# The Python pool host: drives R workers through pymizu.r_pool_launcher and
# checks the nested-submit identity discipline — a nested help-collect must
# not clobber the outer task's foreign result policy (the value format, the
# neutral err stream, and the not-portable gate). Prints one line per case.
py_pool_nested_host <- r"--[
import pymizu

nested = (
    "t <- mizu::mizu_submit(mizu::mizu_current_pool(), x * 2, x = 21L)\n"
    "mizu::mizu_collect(t)\n"
)

with pymizu.Pool.create(1, launcher=pymizu.r_pool_launcher()) as p:
    t = p.submit(pymizu.call(source=nested + "list(a = 1, b = 'x')"))
    assert t.collect() == {"a": 1.0, "b": "x"}
    print("value ok", flush=True)
    try:
        p.submit(pymizu.call(source=nested + "stop('boom')")).collect()
    except pymizu.TaskError as e:
        assert e.remote_type == "simpleError", e.remote_type
    else:
        raise AssertionError("no TaskError from the error case")
    print("error ok", flush=True)
    try:
        p.submit(pymizu.call(source=nested + "function(x) x")).collect()
    except pymizu.TaskError as e:
        assert e.remote_type == "mizu_error_not_portable", e.remote_type
    else:
        raise AssertionError("no TaskError from the closure case")
    print("gate ok", flush=True)
]--"
