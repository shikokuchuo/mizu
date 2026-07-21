# Child-spawn helper: the single setup path for v1 channels. The host spawns
# the peer directly via Rscript -e — the same launch mechanism mirai uses for
# daemons. No control channel, no descriptor passing.

rscript_path <- function() {
  file.path(R.home("bin"),
            if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript")
}

# Launch a detached child R process evaluating `expr` (a length-1 character
# vector of R code). The host's .libPaths() is propagated by setting R_LIBS
# around the spawn — set, spawn, restore; the child inherits its parent's
# environment on every platform — so renv and user-library setups resolve
# mov in the child.
mov_spawn <- function(expr, stdout = FALSE, stderr = FALSE) {
  stopifnot(is.character(expr), length(expr) == 1L, !is.na(expr))
  old <- Sys.getenv("R_LIBS", unset = NA)
  Sys.setenv(R_LIBS = paste(.libPaths(), collapse = .Platform$path.sep))
  on.exit(if (is.na(old)) Sys.unsetenv("R_LIBS") else Sys.setenv(R_LIBS = old))
  system2(rscript_path(), c("-e", shQuote(expr)),
          wait = FALSE, stdout = stdout, stderr = stderr)
  invisible()
}

# Spawn the peer for a channel region. Only the region name's suffix — the
# <pid hex>_<counter hex> tail after the platform prefix — crosses the
# process boundary: the child prepends its own compiled-in prefix, so the
# interpolated text is pure [0-9a-f_] with no quoting hazard on any platform.
# (The full Windows name cannot ride an -e string at all: "Local\mov_"
# contains "\m", an invalid escape in an R string literal.)
spawn_peer <- function(suffix, stdout = "", stderr = "") {
  stopifnot(grepl("^[0-9a-f]+_[0-9a-f]+$", suffix))
  mov_spawn(sprintf('mov:::peer_main("%s")', suffix),
            stdout = stdout, stderr = stderr)
}

# Spawn a pool worker: the region-name suffix and the host-assigned slot
# index travel as argv, under the same rules as spawn_peer.
spawn_worker <- function(suffix, slot, stdout = "", stderr = "") {
  stopifnot(grepl("^[0-9a-f]+_[0-9a-f]+$", suffix), slot >= 0)
  mov_spawn(sprintf('mov:::worker_main("%s",%dL)', suffix, as.integer(slot)),
            stdout = stdout, stderr = stderr)
}
