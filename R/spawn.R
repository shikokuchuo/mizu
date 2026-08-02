# Child-spawn helper: the single setup path for v1 channels. The host spawns
# the peer through a static Rscript runner — no control channel or descriptor
# passing.

rscript_path <- function() {
  file.path(R.home("bin"),
            if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript")
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
  if (!file.exists(script))
    script <- file.path(root, "inst", "scripts", "kio-child.R")
  libs <- paste(.libPaths(), collapse = .Platform$path.sep)
  system2(rscript_path(), c(shQuote(script), to_hex(expr), to_hex(libs)),
          wait = FALSE, stdout = stdout, stderr = stderr)
  invisible()
}

# Spawn the peer for a channel region. The region name's suffix — the <pid
# hex>_<counter hex> tail after the platform prefix — is carried in the entry
# expression; the child prepends its own compiled-in prefix.
spawn_peer <- function(suffix, stdout = "", stderr = "") {
  stopifnot(grepl("^[0-9a-f]+_[0-9a-f]+$", suffix))
  kio_spawn(sprintf('kioto:::peer_main("%s")', suffix),
            stdout = stdout, stderr = stderr)
}

# Spawn a pool worker: the region-name suffix and the host-assigned slot
# index travel as argv, under the same rules as spawn_peer.
spawn_worker <- function(suffix, slot, stdout = "", stderr = "") {
  stopifnot(grepl("^[0-9a-f]+_[0-9a-f]+$", suffix), slot >= 0)
  kio_spawn(sprintf('kioto:::worker_main("%s",%dL)', suffix, as.integer(slot)),
            stdout = stdout, stderr = stderr)
}
