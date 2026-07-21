# Forward-slashed tempfile safe to embed in child code via deparse()
tfile <- function() gsub("\\\\", "/", tempfile())

wait_for_file <- function(path, timeout = 10) {
  t0 <- Sys.time()
  while (!file.exists(path)) {
    if (difftime(Sys.time(), t0, units = "secs") > timeout) return(FALSE)
    Sys.sleep(0.05)
  }
  TRUE
}

# Re-evaluate `expr` in the caller's frame until truthy or the deadline passes
wait_until <- function(expr, timeout = 10) {
  q <- substitute(expr)
  env <- parent.frame()
  t0 <- Sys.time()
  repeat {
    if (isTRUE(eval(q, env))) return(TRUE)
    if (difftime(Sys.time(), t0, units = "secs") > timeout) return(FALSE)
    Sys.sleep(0.05)
  }
}

# Cross-process tests spawn fresh Rscript children that library(mov):
# available under R CMD check (mov is installed in the check library and
# mov_spawn propagates it via R_LIBS), but not under a bare load_all().
child_mov_ok <- local({
  val <- NULL
  function() {
    if (is.null(val)) {
      f <- tfile()
      mov:::mov_spawn(sprintf(
        'if (requireNamespace("mov", quietly = TRUE)) file.create(%s)',
        deparse(f)))
      val <<- wait_for_file(f)
      unlink(f)
    }
    val
  }
})

skip_if_no_child_mov <- function() {
  testthat::skip_if_not(child_mov_ok(), "mov not loadable from child processes")
}
