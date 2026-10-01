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
# terminate the process — ask tasklist instead. A zombie answers the POSIX
# probe but is dead: its PID stays taken until its parent or the system
# init reaps it, so the zombie state is checked explicitly.
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
  isTRUE(tools::pskill(pid, 0L)) && !pid_zombie(pid)
}

pid_zombie <- function(pid) {
  status <- sprintf("/proc/%d/status", pid)
  if (file.exists(status)) {
    # the process can be reaped between the exists probe and the read: a
    # failed read (a warning, then an error) means gone entirely, not zombie
    state <- tryCatch(
      suppressWarnings(readLines(status)),
      error = function(e) character()
    )
    return(any(grepl("^State:\\s+Z", state)))
  }
  stat <- suppressWarnings(system2(
    "ps",
    c("-o", "stat=", "-p", pid),
    stdout = TRUE,
    stderr = FALSE
  ))
  any(startsWith(trimws(stat), "Z"))
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

# Cross-process tests spawn fresh Rscript children that library(mizu):
# available under R CMD check (mizu is installed in the check library and
# mizu_spawn propagates its path via argv), but not under a bare load_all().
child_mizu_ok <- local({
  val <- NULL
  function() {
    if (is.null(val)) {
      f <- tfile()
      mizu:::mizu_spawn(sprintf(
        'if (requireNamespace("mizu", quietly = TRUE)) file.create(%s)',
        deparse(f)
      ))
      val <<- wait_for_file(f)
      unlink(f)
    }
    val
  }
})

skip_if_no_child_mizu <- function() {
  testthat::skip_if_not(
    child_mizu_ok(),
    "mizu not loadable from child processes"
  )
}

# The orphan-reaping tests need an init that reaps: R detaches background
# spawns, so a dead child's PID stays taken by a zombie until PID 1 reaps
# it — and the vendored reaper reclaims a region only once the creator's
# PID is free.
reaper_ok <- local({
  val <- NULL
  function() {
    if (is.null(val)) {
      f <- tfile()
      mizu:::mizu_spawn(sprintf(
        '
        tmp <- paste0(%s, ".tmp")
        writeLines(as.character(Sys.getpid()), tmp)
        file.rename(tmp, %s)
      ',
        deparse(f),
        deparse(f)
      ))
      # the probe child exits at once; its PID frees only if PID 1 reaps
      val <<- wait_for_file(f) &&
        wait_until(!isTRUE(tools::pskill(as.integer(readLines(f)), 0L)), 5)
      unlink(f)
    }
    val
  }
})

skip_if_no_reaper <- function() {
  testthat::skip_if_not(reaper_ok(), "PID 1 does not reap orphans")
}

# Fork is unavailable on Windows and blocked in Positron/RStudio sessions
# (their mcfork hook raises). Probe once with a trivial collected child.
fork_ok <- function() {
  ok <- tryCatch(
    parallel::mccollect(parallel::mcparallel(TRUE, silent = TRUE)),
    error = function(e) NULL
  )
  isTRUE(ok[[1L]])
}

skip_if_no_fork <- function() {
  testthat::skip_if_not(fork_ok(), "fork is unavailable in this session")
}

# In-process channel pair: both ends of one region attached from this
# process — the deterministic harness for ring mechanics, with no process
# management involved. The host end produces on the same ring the peer end
# consumes, exactly as across processes.
channel_pair <- function(
  capacity = 64L,
  slot_size = 256L,
  arena_size = 4096,
  spin = FALSE,
  ident = NULL
) {
  host <- .Call(
    mizu:::mizu_channel_create,
    quote(NULL),
    capacity,
    slot_size,
    arena_size,
    spin
  )
  att <- .Call(
    mizu:::mizu_channel_attach,
    .Call(mizu:::mizu_channel_suffix, host),
    ident
  )
  peer <- att[[1L]]
  .Call(mizu:::mizu_channel_ready_set, peer)
  if (!.Call(mizu:::mizu_channel_ready_wait, host, 10)) {
    stop("channel peer not ready")
  }
  list(host = host, peer = peer)
}

# The canonical peer expression: echo everything until close or host death.
# An idle timeout is not terminal — under instrumentation (valgrind) the
# producer can exceed any idle bound — so only other sentinels end the loop.
echo_expr <- quote(
  repeat {
    x <- mizu_recv(ch, timeout = 30)
    if (inherits(x, "mizu_timeout")) {
      next
    }
    if (inherits(x, "mizu_sentinel")) {
      break
    }
    mizu_send(ch, x)
  }
)

# Orderly in-process channel teardown: the peer signals its close (flush +
# bit + wake), so both ends' close rendezvous succeeds.
channel_end <- function(p) {
  .Call(mizu:::mizu_channel_close_signal, p[["peer"]])
  .Call(mizu:::mizu_channel_close, p[["host"]], 5)
  .Call(mizu:::mizu_channel_close, p[["peer"]], 5)
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
  slot_size = 512L,
  ident = NULL
) {
  ctrl <- .Call(
    mizu:::mizu_pool_create,
    workers,
    max_submitters,
    injection_cap,
    per_worker_cap,
    result_slots,
    slot_size
  )
  suffix <- .Call(mizu:::mizu_pool_suffix, ctrl)
  wks <- lapply(seq_len(workers) - 1L, function(slot) {
    wk <- .Call(mizu:::mizu_pool_worker_join, suffix, slot, ident)
    .Call(mizu:::mizu_pool_set_eval, wk)
    wk
  })
  list(ctrl = ctrl, wk = wks[[1L]], wks = wks)
}

# One worker-loop iteration: 1 = executed a task, 0 = none, -1 = shutdown.
# A task error longjmps out of the step — the eval hot path arms no
# handler — so publish it as the task's ERR result, as worker_main does.
pool_step <- function(p, timeout = 0, wk = p[["wk"]]) {
  e <- tryCatch(
    return(.Call(mizu:::mizu_pool_step, wk, timeout)),
    error = function(e) e
  )
  if (.Call(mizu:::mizu_pool_run_outcome, wk, e) != 0L) {
    stop(e)
  }
  1L
}

# Test-only: move up to n queued injection entries onto the worker's own
# deque (the stand-in for Phase 3's nested submit)
pool_pull <- function(p, n, wk = p[["wk"]]) {
  .Call(mizu:::mizu_pool_deque_pull, wk, n)
}

# Orderly in-process teardown: the workers leave (their slots free), then
# the controller destroys (broadcast + unlink + release).
pool_end <- function(p) {
  for (wk in p[["wks"]]) {
    .Call(mizu:::mizu_pool_leave, wk)
  }
  .Call(mizu:::mizu_pool_destroy, p[["ctrl"]])
}

# Byte-level access to the region behind a root view, for layout tests:
# view_peek(x) opens the region read-only and returns a reader of
# (offset, n) -> raw. i64v() decodes little-endian int64 words whose high
# halves are zero (layout counts and offsets are small); align64() is the
# layouts' section alignment.
view_peek <- function(x) {
  rg <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_zc_view_name, x),
    FALSE
  )
  function(offset, n) .Call(mizu:::mizu_peek, rg, offset, n)
}

i64v <- function(r) {
  w <- readBin(r, "integer", n = length(r) / 4, size = 4, endian = "little")
  stopifnot(all(w[c(FALSE, TRUE)] == 0L))
  as.numeric(w[c(TRUE, FALSE)])
}

align64 <- function(b) (b + 63) %/% 64 * 64

# A poke-crafted remote-leaf (tag 33) fixture for the MIZL remote-leaf
# reader: stage x (wrapped in a list by default) for region A and a
# one-element list for region B through one channel pair, with an RW
# mapping of B for poke_remote(), which rewrites B's directory entry 0 as
# a remote leaf — the identifier span over the orphaned local leaf bytes
# at offset 128 — so the consumer's lazy element extraction resolves it.
remote_leaf_pair <- function(x, wrap = TRUE) {
  p <- channel_pair(arena_size = 0)
  mizu_send(p[["host"]], if (wrap) list(x) else x)
  ya <- mizu_recv(p[["peer"]], 5)
  aname <- .Call(mizu:::mizu_zc_view_name, ya)
  mizu_send(p[["host"]], list(runif(40000)))
  yb <- mizu_recv(p[["peer"]], 5)
  rw <- .Call(
    mizu:::mizu_region_open,
    .Call(mizu:::mizu_zc_view_name, yb),
    TRUE
  )
  list(
    p = p,
    ya = ya,
    yb = yb,
    rw = rw,
    id = if (wrap) paste0(aname, "[1]") else aname
  )
}

i64raw <- function(x) {
  stopifnot(x >= 0 && x < 2^31)
  c(
    writeBin(as.integer(x), raw(), size = 4L, endian = "little"),
    as.raw(rep(0L, 4L))
  )
}

poke_remote <- function(
  fx,
  id = fx[["id"]],
  len,
  attrs = 0L,
  sxp = 33L,
  dsz = nchar(id, "bytes")
) {
  entry <- c(
    i64raw(128L),
    i64raw(dsz),
    writeBin(as.integer(sxp), raw(), size = 4L, endian = "little"),
    writeBin(as.integer(attrs), raw(), size = 4L, endian = "little"),
    i64raw(len)
  )
  .Call(mizu:::mizu_poke, fx[["rw"]], 64, entry)
  if (dsz > 0) {
    .Call(mizu:::mizu_poke, fx[["rw"]], 128, charToRaw(id))
  }
  invisible(fx)
}
