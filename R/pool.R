#' Create a Task Pool and Spawn Its Workers
#'
#' Creates a shared-memory task pool — per-submitter injection rings feeding
#' worker processes, with results published through a slot pool — and spawns
#' its worker processes. Submission is an SHM ring write plus at most one
#' directed wake: no dispatcher process is in the loop. Each worker owns a
#' work-stealing deque; idle workers steal from busy peers and consume the
#' injection rings, with a fairness tick bounding external-submission
#' latency on a saturated pool.
#'
#' The pool's lifetime is bound to the creating process, which holds
#' submitter slot 0 of the returned handle: use it directly with
#' [mov_submit()] and [mov_collect()]. Other processes join as submitters
#' via [mov_pool_attach()]. Dropping the handle (or exiting R) shuts the
#' pool down as [mov_pool_stop()] would, without the wait.
#'
#' Payload contents interoperate transparently with mori: a `mori::share()`d
#' object anywhere inside a task's arguments or its result serializes to its
#' short identifier wire form via mori's own hooks and maps zero-copy on the
#' other side.
#'
#' @param n_workers number of worker processes to spawn, at most
#'   `max_workers`.
#' @param max_workers worker registry capacity (at most 64).
#' @param max_submitters submitter registry capacity (at most 64). Each
#'   submitter owns its own injection ring and an equal share of
#'   `result_slots`.
#' @param injection_cap entries per submitter injection ring; a power of two.
#' @param per_worker_cap entries per worker work-stealing deque; a power of
#'   two.
#' @param result_slots total result slots, partitioned equally across
#'   submitter slots; rounded up to a multiple of `max_submitters`. Bounds
#'   each submitter's outstanding (uncollected) tasks.
#' @param slot_size bytes per queue entry and result slot; a power of two
#'   between 64 and 2^20. Task payloads that serialize past the inline
#'   budget travel via a fresh region per payload, so pools dispatching
#'   closures or multi-argument tasks should prefer `512L`.
#' @param launcher `NULL` for the default launcher (`system2(Rscript, ...)`
#'   with the host's `.libPaths()` propagated via `R_LIBS`), or a
#'   `function(suffix, slot)` that arranges for an R process to eventually
#'   call `mov:::worker_main(suffix, slot)`.
#' @param stdout,stderr forwarded to [system2()] by the default launcher;
#'   the default `""` sends worker output to the host's console. Ignored
#'   when `launcher` is supplied.
#' @param liveness_dir directory for the pool's liveness lock files;
#'   recorded in the region so every participant uses the same files.
#'   Defaults to [tempdir()].
#' @param startup_timeout seconds to wait for all workers to join before
#'   giving up and destroying the pool.
#'
#' @return A pool handle (class `"mov_pool"`) holding submitter slot 0.
#'   Handles are process-private and do not survive `fork()`.
#'
#' @examples
#' \dontrun{
#' p <- mov_pool()
#' t <- mov_submit(p, x + y, x = 1, y = 2)
#' mov_collect(t)
#' mov_pool_stop(p)
#' }
#'
#' @export
mov_pool <- function(n_workers = 1L, max_workers = n_workers,
                     max_submitters = 8L, injection_cap = 1024L,
                     per_worker_cap = 1024L, result_slots = 4096L,
                     slot_size = 256L, launcher = NULL, stdout = "",
                     stderr = "", liveness_dir = tempdir(),
                     startup_timeout = 10) {
  n_workers <- as.integer(n_workers)
  if (is.na(n_workers) || n_workers < 1L)
    stop("mov: n_workers must be at least 1", call. = FALSE)
  if (n_workers > as.integer(max_workers))
    stop("mov: n_workers exceeds max_workers", call. = FALSE)
  p <- .Call(mov_pool_create, max_workers, max_submitters, injection_cap,
             per_worker_cap, result_slots, slot_size, liveness_dir)
  suffix <- .Call(mov_pool_suffix, p)
  for (slot in seq_len(n_workers) - 1L) {
    if (is.null(launcher))
      spawn_worker(suffix, slot, stdout = stdout, stderr = stderr)
    else
      launcher(suffix, slot)
  }
  if (!.Call(mov_pool_ready_wait, p, n_workers, startup_timeout)) {
    .Call(mov_pool_destroy, p)
    stop("mov: workers failed to attach within ", format(startup_timeout),
         " seconds", call. = FALSE)
  }
  p
}

#' Attach to a Pool as a Submitter
#'
#' Joins an existing pool from another process, claiming a free submitter
#' slot with its own injection ring and result-slot subrange. The pool's
#' name travels out-of-band (it is `mov_pool_status(p)$name` on the
#' creator).
#'
#' @param name the pool's region name (or its suffix — the part after the
#'   platform prefix).
#'
#' @return A pool handle (class `"mov_pool"`) holding a submitter slot.
#'
#' @export
mov_pool_attach <- function(name) {
  stopifnot(is.character(name), length(name) == 1L, !is.na(name))
  .Call(mov_pool_attach_call, sub("^.*mov_", "", name))
}

#' Submit a Task and Collect Its Result
#'
#' `mov_submit()` captures `expr` unevaluated, serializes it with its named
#' arguments into the submitter's own injection ring (payloads past the
#' inline budget travel via a fresh region), and returns a task handle
#' immediately. A worker evaluates the expression in a fresh environment
#' containing the arguments as bindings. `mov_collect()` blocks until the
#' result is published, then returns the task's value — or raises the
#' task's error condition, re-signalled in the collecting process.
#'
#' Submission blocks only when the submitter's own injection ring is full —
#' back-pressure is per-submitter — and errors on `.timeout` expiry rather
#' than stalling. Collection returns the `mov_timeout` sentinel (class
#' `c("mov_timeout", "mov_condition")`) if no result arrives within
#' `timeout`. A task whose handle was cancelled (or whose pool was stopped)
#' raises an error on collect.
#'
#' A handle can be collected exactly once: the result slot is released to
#' the pool as the value is returned. Dropping an uncollected handle to the
#' garbage collector cancels a still-queued task and discards a published
#' result.
#'
#' @param pool a pool handle from [mov_pool()] or [mov_pool_attach()].
#' @param expr an expression, captured unevaluated. It sees only the
#'   arguments in `...` (plus the worker's global environment); packages
#'   must be loaded by the expression itself.
#' @param ... named values bound in the evaluation environment. Values are
#'   serialized — `mori::share()`d objects reduce to identifiers and map
#'   zero-copy on the worker.
#' @param .timeout seconds to wait for injection-ring space before erroring;
#'   `Inf` (the default) waits indefinitely. Ctrl-C remains responsive.
#' @param task a task handle from `mov_submit()`.
#' @param timeout seconds to wait for the result before returning the
#'   `mov_timeout` sentinel; `Inf` (the default) waits indefinitely, `0`
#'   polls.
#'
#' @return `mov_submit()` returns a task handle (class `"mov_task"`).
#'   `mov_collect()` returns the task's value, or the `mov_timeout`
#'   sentinel.
#'
#' @examples
#' \dontrun{
#' p <- mov_pool()
#' t <- mov_submit(p, sum(x), x = runif(10))
#' mov_collect(t, timeout = 30)
#' mov_pool_stop(p)
#' }
#'
#' @export
mov_submit <- function(pool, expr, ..., .timeout = Inf) {
  args <- list(...)
  if (length(args) && (is.null(names(args)) || !all(nzchar(names(args)))))
    stop("mov: all task arguments must be named", call. = FALSE)
  .Call(mov_pool_submit, pool, list(substitute(expr), args), .timeout)
}

#' @rdname mov_submit
#' @export
mov_collect <- function(task, timeout = Inf)
  .Call(mov_pool_collect, task, timeout)

#' Cancel a Task
#'
#' Advisory and discard-only, never preemptive: a task still queued is
#' skipped by the worker; a task already executing runs to completion and
#' its result is dropped. Collecting a cancelled handle raises an error.
#'
#' @inheritParams mov_submit
#'
#' @return Invisibly, `TRUE` if this call cancelled the task, `FALSE` if it
#'   was too late — the task completed, or was already cancelled.
#'
#' @export
mov_cancel <- function(task) invisible(.Call(mov_pool_cancel, task))

#' Stop a Pool
#'
#' Broadcasts shutdown, wakes every parked participant, cancels all pending
#' tasks (blocked collectors raise "task cancelled or pool stopped"), waits
#' up to `timeout` seconds for workers to exit cleanly, and unlinks the
#' region and liveness files. The handle is dead afterwards; stopping it
#' again is a no-op. Only the creating process can stop a pool.
#'
#' @inheritParams mov_submit
#' @param timeout seconds to wait for workers' clean exit.
#'
#' @return Invisibly, `TRUE` if all workers exited within the timeout,
#'   `FALSE` otherwise (with a warning; workers still exit on their own).
#'
#' @export
mov_pool_stop <- function(pool, timeout = 5) {
  ok <- .Call(mov_pool_stop_call, pool, timeout)
  if (!ok)
    warning("mov: pool stop timed out waiting for workers; they exit on ",
            "their own once they observe shutdown", call. = FALSE)
  invisible(ok)
}

#' Inspect a Pool
#'
#' A read-only snapshot of the pool region: registry states, parked-worker
#' count, queued injection entries, and result-slot occupancy.
#'
#' @inheritParams mov_submit
#'
#' @return A list with elements `name`, `role`, `max_workers`,
#'   `max_submitters`, `injection_cap`, `result_slots`, `slot_size`,
#'   `workers` (per-slot states), `parked`, `submitters` (per-slot states),
#'   `injection` (entries queued and unclaimed), `tasks` (result slots by
#'   state: pending / ok / err / cancel), `deque` (per-worker deque
#'   depths), and `shutdown`.
#'
#' @export
mov_pool_status <- function(pool) {
  st <- .Call(mov_pool_status_call, pool)
  st$workers <- c("free", "claiming", "live", "leaving",
                  "reaping")[st$workers + 1L]
  st$submitters <- c("free", "live", "reaping")[st$submitters + 1L]
  names(st$tasks) <- c("pending", "ok", "err", "cancel")
  st
}

# Worker entry point: invoked as `Rscript -e 'mov:::worker_main("<suffix>",
# <slot>)'` by the launcher. Rebuilds the region name from the compiled-in
# prefix plus the argv suffix, attaches writable, validates the header,
# claims its host-assigned slot (liveness lock before status CAS), points
# its death listener at the owner, and unparks the creator on reaching
# LIVE. The loop then lives in mov_pool_step: one claim in tier order
# (fairness tick, own deque, steal, injection) per task, parked indefinitely
# when idle, returning negative on shutdown or owner death.
worker_main <- function(suffix, slot) {
  if (!"package:mov" %in% search()) attachNamespace("mov")
  h <- .Call(mov_pool_worker_join, suffix, slot)
  status <- 0L
  tryCatch(
    repeat {
      if (.Call(mov_pool_step, h, 3600, worker_eval) < 0L) break
    },
    error = function(e) {
      cat("mov worker error: ", conditionMessage(e), "\n", sep = "",
          file = stderr())
      status <<- 1L
    }
  )
  .Call(mov_pool_leave, h)
  quit(save = "no", status = status)
}

# Evaluates one task payload — list(expr, named args) — in a fresh
# environment over the worker's global environment. User errors are caught
# and published as ERR results; anything escaping this function is
# infrastructure failure and takes the worker down.
worker_eval <- function(payload) {
  env <- list2env(payload[[2L]], parent = globalenv())
  tryCatch(list(TRUE, eval(payload[[1L]], env)),
           error = function(e) list(FALSE, e))
}
