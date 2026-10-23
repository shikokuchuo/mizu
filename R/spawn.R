# Child-spawn helper: the single setup path for v1 channels. The host spawns
# the peer through a static Rscript runner — no control channel or descriptor
# passing.

# The scalar-string gate of the constructor surface (each caller keeps its
# own contextual error message).
is_scalar_string <- function(x) {
  is.character(x) && length(x) == 1L && !is.na(x)
}

rscript_path <- function() {
  file.path(
    R.home("bin"),
    if (.Platform[["OS.type"]] == "windows") "Rscript.exe" else "Rscript"
  )
}

to_hex <- function(x) paste(charToRaw(enc2utf8(x)), collapse = "")

# Launch a detached child R process evaluating `expr` (a length-1 character
# vector of R code). The expression and the host's .libPaths() ride argv
# hex-encoded, one entry per element so a path containing the platform
# separator survives: argv is copied into the child at creation, so
# concurrent launches share no state and the host environment is never
# touched, and pure [0-9a-f] has no quoting hazard on any platform. A
# static runner avoids Rscript -e, whose implementation writes a per-spawn
# command file. The runner lives at scripts/ once installed, inst/scripts/
# in a source tree (a bare load_all)
mizu_child_script <- function(root) {
  script <- file.path(root, "scripts", "mizu-child.R")
  if (!file.exists(script)) {
    script <- file.path(root, "inst", "scripts", "mizu-child.R")
  }
  script
}

mizu_spawn <- function(expr, stdout = FALSE, stderr = FALSE) {
  if (!is_scalar_string(expr) || !nzchar(expr)) {
    stop("mizu: expr must be a non-empty string", call. = FALSE)
  }
  root <- getNamespaceInfo(asNamespace("mizu"), "path")
  script <- mizu_child_script(root)
  # system2 shQuotes the command path itself; only the args take quoting
  system2(
    rscript_path(),
    c(
      shQuote(script),
      to_hex(expr),
      vapply(.libPaths(), to_hex, character(1L), USE.NAMES = FALSE)
    ),
    wait = FALSE,
    stdout = stdout,
    stderr = stderr
  )
  invisible()
}

# The prologue of every spawned child (channel peer or pool worker): the
# runner evaluates the entry expression with the namespace loaded but not
# necessarily attached, and the allocator tuning applies to every child.
child_prologue <- function() {
  if (!any(search() == "package:mizu")) {
    attachNamespace("mizu")
  }
  .Call(mizu_tune_malloc)
  invisible()
}

# The join token is the region name's <pid hex>_<counter hex> tail.
valid_token <- function(token) {
  grepl("^[0-9a-f]+_[0-9a-f]+$", token)
}

# Spawn the peer for a channel region. The join token — the region name's
# <pid hex>_<counter hex> tail after the platform prefix — is carried in the
# entry expression; the child prepends its own compiled-in prefix.
spawn_peer <- function(token, stdout = "", stderr = "") {
  if (!valid_token(token)) {
    stop("mizu: malformed join token", call. = FALSE)
  }
  mizu_spawn(
    sprintf('mizu:::peer_main("%s")', token),
    stdout = stdout,
    stderr = stderr
  )
}

# Spawn a pool worker: the join token and the host-assigned slot index
# travel as argv, under the same rules as spawn_peer.
spawn_worker <- function(token, slot, stdout = "", stderr = "") {
  if (!valid_token(token) || slot < 0) {
    stop("mizu: malformed join token or slot", call. = FALSE)
  }
  mizu_spawn(
    sprintf('mizu:::worker_main("%s",%dL)', token, as.integer(slot)),
    stdout = stdout,
    stderr = stderr
  )
}

#' Default Child Process Launcher
#'
#' Returns the launcher that [mizu_channel()], [mizu_pool()] and
#' [mizu_spawn_workers()] use unless given a custom one. It spawns a
#' detached child R process through a static `Rscript` runner, with the
#' entry expression and the `.libPaths()` of the host hex-encoded in argv.
#'
#' @section Containers:
#' `system2(wait = FALSE)` detaches background children, so workers and
#' peers are adopted by PID 1 of the process namespace and reaped by that
#' init when they exit. In a container whose PID 1 does not reap (a plain
#' `docker run` without `--init`), exited children accumulate as zombie
#' PID-table entries. This is harmless to mizu itself — death verdicts
#' come from the liveness lock, never the PID — but PID-probe supervision
#' misreads zombies as alive, and [mizu_prune()] cannot reclaim a dead
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
#' @return A `function(token, slot)`. [mizu_pool()] and
#'   [mizu_spawn_workers()] call it with both arguments to spawn the worker
#'   for `slot`. [mizu_channel()] calls it with `token` alone to spawn the
#'   peer.
#'
#' @examples
#' # One log file per worker on Windows.
#' launcher <- function(token, slot) {
#'   f <- if (missing(slot)) "peer.log" else sprintf("worker-%d.log", slot)
#'   mizu_launcher(stderr = f)(token, slot)
#' }
#'
#' @export
mizu_launcher <- function(stdout = "", stderr = "") {
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

# Feature probe for a pymizu entry module: an importable pymizu.child (the
# source-drop path) or pymizu.worker. Successful probes are cached per
# interpreter and module, keyed by the package's init file: a hit
# revalidates with one file.exists, so an uninstalled or replaced
# environment re-probes (and the launcher errors early) instead of
# failing only at spawn with a startup timeout. A failed probe is FALSE
# with a `detail` attribute (the exit status, the interpreter's stderr
# tail) for the launcher's error: the probe runs in this process's
# environment, which is the part a shell cannot reproduce.
pymizu_prober <- local({
  cache <- new.env(parent = emptyenv())
  function(python, module) {
    key <- paste(python, module)
    hit <- cache[[key]]
    if (!is.null(hit) && file.exists(hit)) {
      return(TRUE)
    }
    # the search path goes to stderr ahead of the import, so a failure
    # shows where the interpreter looked; stderr lands in a file (TRUE
    # would merge it into the stdout read below). Errors only, so an
    # interrupt propagates rather than reading as a failed probe; the
    # non-zero-status warning is muffled
    err <- tempfile("mizu-probe-")
    on.exit(unlink(err))
    res <- tryCatch(
      suppressWarnings(
        system2(
          python,
          c(
            "-c",
            shQuote(paste0(
              "import sys; print(sys.path, file=sys.stderr); ",
              sprintf("import pymizu.%s, pymizu; ", module),
              "print(pymizu.__file__)"
            ))
          ),
          stdout = TRUE,
          stderr = err,
          timeout = 60
        )
      ),
      error = function(e) {
        structure(character(0), reason = conditionMessage(e))
      }
    )
    st <- attr(res, "status")
    file <- if (length(res) >= 1L) res[[length(res)]] else ""
    if ((is.null(st) || st == 0L) && nzchar(file) && file.exists(file)) {
      cache[[key]] <- file
      return(TRUE)
    }
    lines <- if (file.exists(err)) readLines(err, warn = FALSE) else character()
    detail <- c(
      attr(res, "reason"),
      if (identical(st, 124L)) "timed out after 60 s",
      if (!is.null(st)) sprintf("exit status %d", st),
      if (nzchar(file) && !file.exists(file)) {
        sprintf("reported '%s', which does not exist", file)
      },
      lines[seq.int(to = length(lines), length.out = min(length(lines), 8L))]
    )
    structure(FALSE, detail = paste(detail, collapse = "\n"))
  }
})

# The shared preflight of the python launchers: resolve the interpreter
# (defaulting to python3 on the PATH) and probe it for the pymizu entry
# module, so a missing interpreter or package raises before anything exists.
# `caller` names the exported launcher in the error messages.
py_resolve <- function(python, module, caller) {
  if (is.null(python)) {
    python <- unname(Sys.which("python3"))
    if (!nzchar(python)) {
      stop(
        sprintf(
          "mizu: %s() needs python3 on the PATH (or pass python)",
          caller
        ),
        call. = FALSE
      )
    }
  } else {
    resolved <- unname(Sys.which(python))
    if (!nzchar(resolved)) {
      stop(
        sprintf(
          "mizu: %s() cannot find the Python interpreter '%s'",
          caller,
          python
        ),
        call. = FALSE
      )
    }
    python <- resolved
  }
  ok <- pymizu_prober(python, module)
  if (!ok) {
    stop(
      sprintf(
        "mizu: %s() needs the Python package 'pymizu' installed for %s\n%s",
        caller,
        python,
        attr(ok, "detail")
      ),
      call. = FALSE
    )
  }
  python
}

# The spawned process itself: `python -m <module> token [slot]`. The pool
# verb calls the launcher with both arguments; the channel verb with token
# alone (the same convention as mizu_launcher()).
py_module_launcher <- function(python, module, stdout, stderr) {
  force(stdout)
  force(stderr)
  function(token, slot) {
    args <- c("-m", module, token)
    if (!missing(slot)) {
      args <- c(args, slot)
    }
    system2(python, args, wait = FALSE, stdout = stdout, stderr = stderr)
  }
}

#' Python Channel Peer Launcher
#'
#' Returns a launcher for [mizu_channel()] that spawns the peer as a
#' Python process running `python -m pymizu.child`, the peer entry of
#' [pymizu](https://github.com/shikokuchuo/pymizu), the Python binding of
#' the same shared-memory core. The mirror of pymizu's `r_launcher()`,
#' which spawns an R peer from a Python host.
#'
#' The interpreter is probed for pymizu when the launcher is created, so a
#' missing interpreter or package raises here, before the channel exists.
#' The payload rules for a non-R peer apply: only vectors and strings
#' cross (see [mizu_send()]).
#'
#' @param python path to the Python interpreter. The default looks up
#'   `python3` on the `PATH`.
#' @param stdout,stderr forwarded to [system2()] for the peer process, as
#'   in [mizu_launcher()].
#'
#' @return A `function(token)` that spawns the peer process, for the
#'   `launcher` argument of [mizu_channel()].
#'
#' @examples
#' \dontrun{
#' ch <- mizu_channel(
#'   "
#' import pymizu
#' while True:
#'     x = ch.recv(30)
#'     if pymizu.is_sentinel(x):
#'         break
#'     ch.send(x * 2)
#' ",
#'   launcher = mizu_py_launcher()
#' )
#' mizu_send(ch, c(1.5, 2.5, 3.5))
#' mizu_recv(ch, timeout = 5)
#' mizu_close(ch)
#' }
#'
#' @export
mizu_py_launcher <- function(python = NULL, stdout = "", stderr = "") {
  python <- py_resolve(python, "child", "mizu_py_launcher")
  py_module_launcher(python, "pymizu.child", stdout, stderr)
}

#' Python Pool Worker Launcher
#'
#' Returns a launcher for [mizu_pool()] that spawns each worker as a
#' Python process running `python -m pymizu.worker`, the worker entry of
#' [pymizu](https://github.com/shikokuchuo/pymizu), the Python binding of
#' the same shared-memory core. The mirror of pymizu's
#' `r_pool_launcher()`, which spawns R workers from a Python host.
#'
#' The first worker's join records the workers' language in the pool, so
#' the launcher carries no language attribute: a pool of Python workers
#' takes [mizu_call()] specifications through [mizu_submit_call()], and a
#' native [mizu_submit()] errors locally naming the spec verb. A launcher
#' that spawns the wrong language fails at join, not at the first task.
#'
#' The interpreter is probed for pymizu when the launcher is created, so a
#' missing interpreter or package raises here, before any pool exists.
#'
#' @inheritParams mizu_py_launcher
#'
#' @return A `function(token, slot)` that spawns one worker process, for
#'   the `launcher` argument of [mizu_pool()] and [mizu_spawn_workers()].
#'
#' @examples
#' \dontrun{
#' p <- mizu_pool(2, launcher = mizu_py_pool_launcher())
#' t <- mizu_submit_call(p, mizu_call("numpy.mean", runif(100)))
#' mizu_collect(t)
#' mizu_pool_stop(p)
#' }
#'
#' @export
mizu_py_pool_launcher <- function(python = NULL, stdout = "", stderr = "") {
  python <- py_resolve(python, "worker", "mizu_py_pool_launcher")
  py_module_launcher(python, "pymizu.worker", stdout, stderr)
}
