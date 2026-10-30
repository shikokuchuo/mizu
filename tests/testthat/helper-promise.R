# Async-test plumbing: the later/promises guard and the event-loop pump.
skip_if_no_promises <- function() {
  testthat::skip_if_not_installed("later")
  testthat::skip_if_not_installed("promises")
}

# Drive the loop until cond() holds or the deadline passes; returns the
# final cond() value so a timeout reads as FALSE/NULL, not an error.
pump_until <- function(cond, timeout = 10) {
  t0 <- Sys.time()
  repeat {
    later::run_now()
    if (isTRUE(cond())) {
      return(TRUE)
    }
    if (difftime(Sys.time(), t0, units = "secs") > timeout) {
      return(cond())
    }
    Sys.sleep(0.01)
  }
}

# A settling observer for one promise: value/error fields set from its
# then callbacks; id field for FIFO-order assertions. The callbacks are
# created in an empty-parented frame: devtools::test sources helpers into
# the package env, and a callback whose chain reaches package:mizu makes
# R_Serialize warn ("may not be available when loading") when a map
# descriptor carries a captured promise — the wire itself is unaffected.
observe <- function(p, id = NULL) {
  o <- new.env(parent = emptyenv())
  o$id <- id
  o$value <- NULL
  o$error <- NULL
  e <- new.env(parent = baseenv())
  e$o <- o
  promises::then(
    p,
    evalq(
      function(x) {
        o$value <- x
        invisible()
      },
      e
    ),
    evalq(
      function(err) {
        o$error <- err
        invisible()
      },
      e
    )
  )
  o
}

settled <- function(o) !is.null(o$value) || !is.null(o$error)

# Open fd count, for the watch soak (Linux /proc only).
fd_count <- function() length(list.files("/proc/self/fd"))
