# Child-spawn helper: the single setup path for v1 channels. The host spawns
# the peer through a static Rscript runner — no control channel or descriptor
# passing.

rscript_path <- function() {
  file.path(
    R.home("bin"),
    if (.Platform[["OS.type"]] == "windows") "Rscript.exe" else "Rscript"
  )
}

to_hex <- function(x) paste(charToRaw(enc2utf8(x)), collapse = "")

# Launch a detached child R process evaluating `expr` (a length-1 character
# vector of R code). The expression and the host's .libPaths() ride argv
# hex-encoded: argv is copied into the child at creation, so concurrent
# launches share no state and the host environment is never touched, and
# pure [0-9a-f] has no quoting hazard on any platform. A static runner
# avoids Rscript -e, whose implementation writes a per-spawn command file.
# The runner lives at scripts/ once installed, inst/scripts/ in a source
# tree (a bare load_all)
rei_child_script <- function(root) {
  script <- file.path(root, "scripts", "rei-child.R")
  if (!file.exists(script)) {
    script <- file.path(root, "inst", "scripts", "rei-child.R")
  }
  script
}

rei_spawn <- function(expr, stdout = FALSE, stderr = FALSE) {
  if (
    !is.character(expr) || length(expr) != 1L || is.na(expr) || !nzchar(expr)
  ) {
    stop("rei: expr must be a non-empty string", call. = FALSE)
  }
  root <- getNamespaceInfo(asNamespace("rei"), "path")
  script <- rei_child_script(root)
  libs <- paste(.libPaths(), collapse = .Platform[["path.sep"]])
  system2(
    rscript_path(),
    c(shQuote(script), to_hex(expr), to_hex(libs)),
    wait = FALSE,
    stdout = stdout,
    stderr = stderr
  )
  invisible()
}

# Spawn the peer for a channel region. The join token — the region name's
# <pid hex>_<counter hex> tail after the platform prefix — is carried in the
# entry expression; the child prepends its own compiled-in prefix.
spawn_peer <- function(token, stdout = "", stderr = "") {
  if (!grepl("^[0-9a-f]+_[0-9a-f]+$", token)) {
    stop("rei: malformed join token", call. = FALSE)
  }
  rei_spawn(
    sprintf('rei:::peer_main("%s")', token),
    stdout = stdout,
    stderr = stderr
  )
}

# Spawn a pool worker: the join token and the host-assigned slot index
# travel as argv, under the same rules as spawn_peer.
spawn_worker <- function(token, slot, stdout = "", stderr = "") {
  if (!grepl("^[0-9a-f]+_[0-9a-f]+$", token) || slot < 0) {
    stop("rei: malformed join token or slot", call. = FALSE)
  }
  rei_spawn(
    sprintf('rei:::worker_main("%s",%dL)', token, as.integer(slot)),
    stdout = stdout,
    stderr = stderr
  )
}

#' Default Child Process Launcher
#'
#' Returns the launcher that [rei_channel()], [rei_pool()] and
#' [rei_spawn_workers()] use unless given a custom one. It spawns a
#' detached child R process through a static `Rscript` runner, with the
#' entry expression and the `.libPaths()` of the host hex-encoded in argv.
#'
#' @section Containers:
#' `system2(wait = FALSE)` detaches background children, so workers and
#' peers are adopted by PID 1 of the process namespace and reaped by that
#' init when they exit. In a container whose PID 1 does not reap (a plain
#' `docker run` without `--init`), exited children accumulate as zombie
#' PID-table entries. This is harmless to rei itself — death verdicts
#' come from the liveness lock, never the PID — but PID-probe supervision
#' misreads zombies as alive, and [rei_prune()] cannot reclaim a dead
#' process's regions while its PID stays taken.
#'
#' @param stdout,stderr forwarded to [system2()]. The default `""` sends
#'   the child output (for a channel peer, including its error epilogue)
#'   to the console of the host, `FALSE` discards it, and a file name
#'   collects it in that file.
#'
#'   On Windows, the child holds a redirection file without sharing for
#'   its lifetime. Only one live process can use a file at a time.
#'   Other processes cannot read the file while the child lives. A
#'   second spawn with the same file fails to launch. In a pool, this
#'   failure causes a startup timeout. A custom launcher that sets the
#'   file name from `slot` gives one log file per worker (see examples).
#'
#' @return A `function(token, slot)`. [rei_pool()] and
#'   [rei_spawn_workers()] call it with both arguments to spawn the worker
#'   for `slot`. [rei_channel()] calls it with `token` alone to spawn the
#'   peer.
#'
#' @examples
#' # One log file per worker on Windows.
#' launcher <- function(token, slot) {
#'   f <- if (missing(slot)) "peer.log" else sprintf("worker-%d.log", slot)
#'   rei_launcher(stderr = f)(token, slot)
#' }
#'
#' @export
rei_launcher <- function(stdout = "", stderr = "") {
  force(stdout)
  force(stderr)
  function(token, slot) {
    if (missing(slot)) {
      spawn_peer(token, stdout = stdout, stderr = stderr)
    } else {
      spawn_worker(token, slot, stdout = stdout, stderr = stderr)
    }
  }
}
