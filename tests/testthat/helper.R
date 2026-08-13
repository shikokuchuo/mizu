# Forward-slashed tempfile safe to embed in child code via deparse()
tfile <- function() gsub("\\\\", "/", tempfile())

wait_for_file <- function(path, timeout = 10) {
  t0 <- Sys.time()
  while (!file.exists(path)) {
    if (difftime(Sys.time(), t0, units = "secs") > timeout) {
      return(FALSE)
    }
    Sys.sleep(0.05)
  }
  TRUE
}

# Hard-kill a process: no handlers, no finalizers. tools::SIGKILL is NA on
# Windows (the signal is undefined there), and pskill ignores the signal
# value anyway — any non-NA signal is TerminateProcess — so SIGTERM gives
# SIGKILL semantics on Windows.
kill_hard <- function(pid) {
  tools::pskill(
    pid,
    if (is.na(tools::SIGKILL)) {
      tools::SIGTERM
    } else {
      tools::SIGKILL
    }
  )
}

# Liveness probe for a raw pid. pskill(pid, 0) is the POSIX probe, but on
# Windows pskill ignores the signal value, so probing with 0 would
# terminate the process — ask tasklist instead.
pid_alive <- function(pid) {
  pid <- as.integer(pid)
  if (.Platform[["OS.type"]] == "windows") {
    out <- suppressWarnings(system2(
      "tasklist",
      c("/FI", sprintf('"PID eq %d"', pid), "/NH", "/FO", "CSV"),
      stdout = TRUE,
      stderr = FALSE
    ))
    return(any(grepl(sprintf('","%d","', pid), out, fixed = TRUE)))
  }
  isTRUE(tools::pskill(pid, 0L))
}

# Re-evaluate `expr` in the caller's frame until truthy or the deadline passes
wait_until <- function(expr, timeout = 10) {
  q <- substitute(expr)
  env <- parent.frame()
  t0 <- Sys.time()
  repeat {
    if (isTRUE(eval(q, env))) {
      return(TRUE)
    }
    if (difftime(Sys.time(), t0, units = "secs") > timeout) {
      return(FALSE)
    }
    Sys.sleep(0.05)
  }
}

# Cross-process tests spawn fresh Rscript children that library(kioto):
# available under R CMD check (kioto is installed in the check library and
# kio_spawn propagates its path via argv), but not under a bare load_all().
child_kioto_ok <- local({
  val <- NULL
  function() {
    if (is.null(val)) {
      f <- tfile()
      kioto:::kio_spawn(sprintf(
        'if (requireNamespace("kioto", quietly = TRUE)) file.create(%s)',
        deparse(f)
      ))
      val <<- wait_for_file(f)
      unlink(f)
    }
    val
  }
})

skip_if_no_child_kioto <- function() {
  testthat::skip_if_not(
    child_kioto_ok(),
    "kioto not loadable from child processes"
  )
}

# In-process channel pair: both ends of one region attached from this
# process — the deterministic harness for ring mechanics, with no process
# management involved. The host end produces on the same ring the peer end
# consumes, exactly as across processes.
channel_pair <- function(
  capacity = 64L,
  slot_size = 256L,
  arena_size = 4096,
  spin = FALSE
) {
  host <- .Call(
    kioto:::kio_channel_create,
    quote(NULL),
    capacity,
    slot_size,
    arena_size,
    spin
  )
  att <- .Call(
    kioto:::kio_channel_attach,
    .Call(kioto:::kio_channel_suffix, host)
  )
  peer <- att[[1L]]
  .Call(kioto:::kio_channel_ready_set, peer)
  if (!.Call(kioto:::kio_channel_ready_wait, host, 10)) {
    stop("channel peer not ready")
  }
  list(host = host, peer = peer)
}

# The canonical peer expression: echo everything until close or host death.
# An idle timeout is not terminal — under instrumentation (valgrind) the
# producer can exceed any idle bound — so only other sentinels end the loop.
echo_expr <- quote(
  repeat {
    x <- kio_recv(ch, timeout = 30)
    if (inherits(x, "kio_timeout")) {
      next
    }
    if (inherits(x, "kio_sentinel")) {
      break
    }
    kio_send(ch, x)
  }
)

# Orderly in-process channel teardown: the peer signals its close (flush +
# bit + wake), so both ends' close rendezvous succeeds.
channel_end <- function(p) {
  .Call(kioto:::kio_channel_close_signal, p[["peer"]])
  .Call(kioto:::kio_channel_close, p[["host"]], 5)
  .Call(kioto:::kio_channel_close, p[["peer"]], 5)
}

# In-process pool pair: controller plus one or more worker handles joined
# from this process — the deterministic harness for the injection /
# result-slot / stealing protocols, with no process management involved.
# Workers consume via single steps driven by the test; p$wk is the first
# worker, p$wks all of them.
pool_pair <- function(
  workers = 1L,
  max_submitters = 8L,
  injection_cap = 64L,
  per_worker_cap = 64L,
  result_slots = 64L,
  slot_size = 256L
) {
  ctrl <- .Call(
    kioto:::kio_pool_create,
    workers,
    max_submitters,
    injection_cap,
    per_worker_cap,
    result_slots,
    slot_size
  )
  suffix <- .Call(kioto:::kio_pool_suffix, ctrl)
  wks <- lapply(seq_len(workers) - 1L, function(slot) {
    wk <- .Call(kioto:::kio_pool_worker_join, suffix, slot)
    .Call(kioto:::kio_pool_set_eval, wk)
    wk
  })
  list(ctrl = ctrl, wk = wks[[1L]], wks = wks)
}

# One worker-loop iteration: 1 = executed a task, 0 = none, -1 = shutdown.
# A task error longjmps out of the step — the eval hot path arms no
# handler — so publish it as the task's ERR result, as worker_main does.
pool_step <- function(p, timeout = 0, wk = p[["wk"]]) {
  e <- tryCatch(
    return(.Call(kioto:::kio_pool_step, wk, timeout)),
    error = function(e) e
  )
  if (.Call(kioto:::kio_pool_run_outcome, wk, e) != 0L) {
    stop(e)
  }
  1L
}

# Test-only: move up to n queued injection entries onto the worker's own
# deque (the stand-in for Phase 3's nested submit)
pool_pull <- function(p, n, wk = p[["wk"]]) {
  .Call(kioto:::kio_pool_deque_pull, wk, n)
}

# Orderly in-process teardown: the workers leave (their slots free), then
# the controller destroys (broadcast + unlink + release).
pool_end <- function(p) {
  for (wk in p[["wks"]]) {
    .Call(kioto:::kio_pool_leave, wk)
  }
  .Call(kioto:::kio_pool_destroy, p[["ctrl"]])
}
