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

# Benchmarks run only where MOV_BENCH is set (see test-benchmark.R)
skip_unless_bench <- function() {
  testthat::skip_if_not(nzchar(Sys.getenv("MOV_BENCH")), "MOV_BENCH not set")
}

# In-process channel pair: both ends of one region attached from this
# process — the deterministic harness for ring mechanics, with no process
# management involved. The host end produces on the same ring the peer end
# consumes, exactly as across processes.
channel_pair <- function(capacity = 64L, slot_size = 256L, arena_size = 4096,
                         spin = FALSE) {
  host <- .Call(mov:::mov_channel_create, quote(NULL), capacity, slot_size,
                arena_size, tempdir(), spin)
  att <- .Call(mov:::mov_channel_attach, .Call(mov:::mov_channel_suffix, host))
  peer <- att[[1L]]
  .Call(mov:::mov_channel_ready_set, peer)
  stopifnot(.Call(mov:::mov_channel_ready_wait, host, 10))
  list(host = host, peer = peer)
}

# The canonical peer expression: echo everything until a sentinel arrives
echo_expr <- quote(
  repeat {
    x <- mov_recv(ch, timeout = 30)
    if (inherits(x, "mov_condition")) break
    mov_send(ch, x)
    mov_flush(ch)
  }
)
