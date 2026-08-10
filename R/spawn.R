# Child-spawn helper: the single setup path for v1 channels. The host spawns
# the peer through a static Rscript runner — no control channel or descriptor
# passing.

rscript_path <- function() {
  file.path(
    R.home("bin"),
    if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript"
  )
}

to_hex <- function(x) paste(charToRaw(enc2utf8(x)), collapse = "")

# Launch a detached child R process evaluating `expr` (a length-1 character
# vector of R code). The expression and the host's .libPaths() ride argv
# hex-encoded: argv is copied into the child at creation, so concurrent
# launches share no state and the host environment is never touched, and
# pure [0-9a-f] has no quoting hazard on any platform. A static runner
# avoids Rscript -e, whose implementation writes a per-spawn command file.
kio_spawn <- function(expr, stdout = FALSE, stderr = FALSE) {
  stopifnot(is.character(expr), length(expr) == 1L, !is.na(expr), nzchar(expr))
  root <- getNamespaceInfo(asNamespace("kioto"), "path")
  script <- file.path(root, "scripts", "kio-child.R")
  if (!file.exists(script)) {
    script <- file.path(root, "inst", "scripts", "kio-child.R")
  }
  libs <- paste(.libPaths(), collapse = .Platform$path.sep)
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
  stopifnot(grepl("^[0-9a-f]+_[0-9a-f]+$", token))
  kio_spawn(
    sprintf('kioto:::peer_main("%s")', token),
    stdout = stdout,
    stderr = stderr
  )
}

# Spawn a pool worker: the join token and the host-assigned slot index
# travel as argv, under the same rules as spawn_peer.
spawn_worker <- function(token, slot, stdout = "", stderr = "") {
  stopifnot(grepl("^[0-9a-f]+_[0-9a-f]+$", token), slot >= 0)
  kio_spawn(
    sprintf('kioto:::worker_main("%s",%dL)', token, as.integer(slot)),
    stdout = stdout,
    stderr = stderr
  )
}

#' Default Child Process Launcher
#'
#' Returns the launcher that [kio_channel()], [kio_pool()] and
#' [kio_spawn_workers()] use unless given a custom one. It spawns a
#' detached child R process through a static `Rscript` runner, with the
#' entry expression and the `.libPaths()` of the host hex-encoded in argv.
#'
#' @param stdout,stderr forwarded to [system2()]. The default `""` sends
#'   the child output (for a channel peer, including its error epilogue)
#'   to the console of the host, `FALSE` discards it, and a file name
#'   collects it in that file.
#'
#' @return A `function(token, slot)`. [kio_pool()] and
#'   [kio_spawn_workers()] call it with both arguments to spawn the worker
#'   for `slot`. [kio_channel()] calls it with `token` alone to spawn the
#'   peer.
#'
#' @export
kio_launcher <- function(stdout = "", stderr = "") {
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
