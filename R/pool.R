#' Create a Task Pool and Spawn Its Workers
#'
#' Creates a shared-memory task pool — per-submitter injection rings feeding
#' worker processes, with results published through result slots — and spawns
#' its worker processes. Submission is an SHM ring write plus at most one
#' directed wake: no dispatcher process is in the loop. Each worker owns a
#' work-stealing deque; idle workers steal from busy peers and consume the
#' injection rings, with a fairness tick bounding external-submission
#' latency on a saturated pool.
#'
#' The pool's lifetime is bound to the creating process, which holds
#' submitter slot 0 of the returned handle: use it directly with
#' [kio_submit()] and [kio_collect()]. Other processes join as submitters
#' via [kio_pool_attach()]. Dropping the handle (or exiting R) shuts the
#' pool down as [kio_pool_stop()] would, without the wait.
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
#'   between 128 and 2^20. Payloads (task or result) that serialize past
#'   the inline budget travel via a fresh region per payload — an
#'   order-of-magnitude latency cliff, surfaced per submitter as
#'   [kio_pool_stats()]`$submitters$spills`. The default `512L` keeps
#'   typical expression-plus-arguments tasks inline; pools moving only
#'   scalar payloads can drop to `256L`.
#' @param launcher `NULL` for the default launcher (`system2(Rscript, ...)`
#'   with the host's `.libPaths()` propagated via `R_LIBS`), or a
#'   `function(suffix, slot)` that arranges for an R process to eventually
#'   call `kioto:::worker_main(suffix, slot)`.
#' @param stdout,stderr forwarded to [system2()] by the default launcher;
#'   the default `""` sends worker output to the host's console. Ignored
#'   when `launcher` is supplied.
#' @param liveness_dir directory for the pool's liveness lock files;
#'   recorded in the region so every participant uses the same files.
#'   Defaults to [tempdir()].
#' @param startup_timeout seconds to wait for all workers to join before
#'   giving up, destroying the pool, and raising `kio_error_startup` (see
#'   [kio_error]).
#'
#' @return A pool handle (class `"kio_pool"`) holding submitter slot 0.
#'   Handles are process-private and do not survive `fork()`.
#'
#' @examples
#' \dontrun{
#' p <- kio_pool()
#' t <- kio_submit(p, x + y, x = 1, y = 2)
#' kio_collect(t)
#' kio_pool_stop(p)
#' }
#'
#' @export
kio_pool <- function(n_workers = 1L, max_workers = n_workers,
                     max_submitters = 8L, injection_cap = 1024L,
                     per_worker_cap = 1024L, result_slots = 4096L,
                     slot_size = 512L, launcher = NULL, stdout = "",
                     stderr = "", liveness_dir = tempdir(),
                     startup_timeout = 30) {
  n_workers <- as.integer(n_workers)
  if (is.na(n_workers) || n_workers < 1L)
    stop("kioto: n_workers must be at least 1", call. = FALSE)
  if (n_workers > as.integer(max_workers))
    stop("kioto: n_workers exceeds max_workers", call. = FALSE)
  p <- .Call(kio_pool_create, max_workers, max_submitters, injection_cap,
             per_worker_cap, result_slots, slot_size, liveness_dir)
  suffix <- .Call(kio_pool_suffix, p)
  for (slot in seq_len(n_workers) - 1L) {
    if (is.null(launcher))
      spawn_worker(suffix, slot, stdout = stdout, stderr = stderr)
    else
      launcher(suffix, slot)
  }
  if (!.Call(kio_pool_ready_wait, p, seq_len(n_workers) - 1L,
             startup_timeout)) {
    .Call(kio_pool_destroy, p)
    stop_kio("kio_error_startup",
             paste0("kioto: workers failed to attach within ",
                    format(startup_timeout), " seconds"))
  }
  p
}

#' Grow or Shrink a Pool's Worker Set
#'
#' `kio_spawn_workers()` spawns additional workers into free registry
#' slots, up to the pool's `max_workers`, and waits for them to join.
#' `kio_retire_worker()` asks one worker to exit cleanly: the request is
#' non-blocking and never preemptive — the worker observes it between
#' tasks, releases its slot, and any work still queued on its deque is
#' consumed in place by the remaining workers. A retired worker's process
#' may linger briefly as a lifetime anchor for results it produced that
#' have not yet been collected.
#'
#' Slots free up when workers retire, exit at shutdown, or die and are
#' reaped, so a pool can cycle workers within its registry capacity for
#' its whole lifetime. Only the creating process can resize a pool.
#'
#' @inheritParams kio_submit
#' @inheritParams kio_pool
#' @param n number of workers to spawn.
#' @param slot the worker's slot index (0-based, as reported by
#'   [kio_pool_dump()]).
#'
#' @return `kio_spawn_workers()` invisibly returns the slot indices
#'   spawned into. `kio_retire_worker()` invisibly returns `NULL`.
#'
#' @export
kio_spawn_workers <- function(pool, n = 1L, launcher = NULL, stdout = "",
                              stderr = "", startup_timeout = 30) {
  n <- as.integer(n)
  if (is.na(n) || n < 1L)
    stop("kioto: n must be at least 1", call. = FALSE)
  free <- which(kio_pool_status(pool)$workers == "free") - 1L
  if (length(free) < n)
    stop("kioto: not enough free worker slots (", length(free), " free)",
         call. = FALSE)
  slots <- free[seq_len(n)]
  suffix <- .Call(kio_pool_suffix, pool)
  for (slot in slots) {
    if (is.null(launcher))
      spawn_worker(suffix, slot, stdout = stdout, stderr = stderr)
    else
      launcher(suffix, slot)
  }
  if (!.Call(kio_pool_ready_wait, pool, as.integer(slots), startup_timeout))
    stop_kio("kio_error_startup",
             paste0("kioto: workers failed to attach within ",
                    format(startup_timeout), " seconds"))
  invisible(as.integer(slots))
}

#' @rdname kio_spawn_workers
#' @export
kio_retire_worker <- function(pool, slot)
  invisible(.Call(kio_pool_retire, pool, as.integer(slot)))

#' Attach to a Pool as a Submitter
#'
#' Joins an existing pool from another process, claiming a free submitter
#' slot with its own injection ring and result-slot subrange. The pool's
#' name travels out-of-band (it is `kio_pool_status(p)$name` on the
#' creator).
#'
#' @param name the pool's region name (or its suffix — the part after the
#'   platform prefix).
#'
#' @return A pool handle (class `"kio_pool"`) holding a submitter slot.
#'
#' @export
kio_pool_attach <- function(name) {
  stopifnot(is.character(name), length(name) == 1L, !is.na(name))
  .Call(kio_pool_attach_call, sub("^.*kio_", "", name))
}

#' Submit a Task and Collect Its Result
#'
#' `kio_submit()` captures `expr` unevaluated, serializes it with its named
#' arguments into the submitter's own injection ring (payloads past the
#' inline budget travel via a fresh region), and returns a task handle
#' immediately. A worker evaluates the expression in a fresh environment
#' containing the arguments as bindings. `kio_collect()` blocks until the
#' result is published, then returns the task's value — or raises the
#' task's error condition, re-signalled in the collecting process.
#'
#' Submission blocks only when the submitter's own injection ring is full —
#' back-pressure is per-submitter — and raises `kio_error_submit_timeout`
#' on `.timeout` expiry rather than stalling. Collection returns the
#' `kio_timeout` sentinel (class `c("kio_timeout", "kio_sentinel")`) if no
#' result arrives within `timeout`. A task whose handle was cancelled (or
#' whose pool was stopped) raises `kio_error_cancelled` on collect; a task
#' whose executing worker died raises `kio_error_worker_died`, carrying the
#' worker's slot and pid (see [kio_error]): worker death is detected at OS
#' notification latency (a kernel-released lock is the verdict — no
#' heartbeats, no polling) and fails exactly the tasks the dead worker had
#' claimed, while work still queued on its deque is consumed by the
#' surviving workers.
#'
#' @section Outcomes:
#' Timeout on collect is a normal outcome and is returned; every
#' exceptional outcome is raised, as a classed condition (see [kio_error]):
#'
#' | outcome | surfaced as | class |
#' |---|---|---|
#' | result published | the value, returned | — |
#' | no result within `timeout` | sentinel, returned | `c("kio_timeout", "kio_sentinel")` |
#' | ring full past `.timeout` | raised by `kio_submit()` | `kio_error_submit_timeout` |
#' | result slots exhausted | raised by `kio_submit()` | `kio_error_slots_exhausted` |
#' | pool stopped, or owner died | raised by `kio_submit()` | `kio_error_stopped` |
#' | task raised an error | re-signalled on collect | the task's own condition classes |
#' | cancelled, or pool stopped | raised on collect | `kio_error_cancelled` |
#' | executing worker died | raised on collect | `kio_error_worker_died` |
#'
#' Task expressions see their evaluating worker's own handle as `pool`
#' (beneath the arguments in `...`), so tasks can submit nested subtasks:
#' `kio_submit(pool, ...)` inside a task pushes onto the worker's own
#' work-stealing deque — no ring, no wait; a full deque runs the subtask
#' inline instead. A worker blocked in `kio_collect()` on a nested handle
#' helps rather than sleeps: it executes work from its own deque (and
#' steals from peers) until the awaited result publishes, so nested
#' fan-outs run at fork/join cost and never deadlock the pool. Nested
#' submission claims a submitter slot for the worker on first use.
#'
#' A handle can be collected exactly once: the result slot is released to
#' the pool as the value is returned. Dropping an uncollected handle to the
#' garbage collector cancels a still-queued task and discards a published
#' result.
#'
#' @param pool a pool handle from [kio_pool()] or [kio_pool_attach()] — or,
#'   inside a task, the worker's own handle bound as `pool`.
#' @param expr an expression, captured unevaluated — unlike [kio_channel()],
#'   which requires its expression pre-quoted. It sees only the
#'   arguments in `...` (plus the worker's global environment); packages
#'   must be loaded by the expression itself.
#' @param ... named values bound in the evaluation environment. Values are
#'   serialized — `mori::share()`d objects reduce to identifiers and map
#'   zero-copy on the worker.
#' @param .timeout seconds to wait for injection-ring space before erroring;
#'   `Inf` (the default) waits indefinitely. Ctrl-C remains responsive.
#' @param task a task handle from `kio_submit()`.
#' @param timeout seconds to wait for the result before returning the
#'   `kio_timeout` sentinel; `Inf` (the default) waits indefinitely, `0`
#'   polls.
#'
#' @return `kio_submit()` returns a task handle (class `"kio_task"`).
#'   `kio_collect()` returns the task's value, or the `kio_timeout`
#'   sentinel.
#'
#' @examples
#' \dontrun{
#' p <- kio_pool()
#' t <- kio_submit(p, sum(x), x = runif(10))
#' kio_collect(t, timeout = 30)
#' kio_pool_stop(p)
#' }
#'
#' @export
kio_submit <- function(pool, expr, ..., .timeout = Inf) {
  args <- list(...)
  if (length(args) && (is.null(names(args)) || !all(nzchar(names(args)))))
    stop("kioto: all task arguments must be named", call. = FALSE)
  .Call(kio_pool_submit, pool, list(substitute(expr), args), .timeout)
}

#' @rdname kio_submit
#' @export
kio_collect <- function(task, timeout = Inf)
  .Call(kio_pool_collect, task, timeout)

#' Cancel a Task
#'
#' Advisory and discard-only, never preemptive: a task still queued is
#' skipped by the worker; a task already executing runs to completion and
#' its result is dropped. Collecting a cancelled handle raises
#' `kio_error_cancelled` (see [kio_error]).
#'
#' @inheritParams kio_submit
#'
#' @return Invisibly, `TRUE` if this call cancelled the task, `FALSE` if it
#'   was too late — the task completed, or was already cancelled.
#'
#' @export
kio_cancel <- function(task) invisible(.Call(kio_pool_cancel, task))

#' Stop a Pool
#'
#' Broadcasts shutdown, wakes every parked participant, cancels all pending
#' tasks (blocked collectors raise `kio_error_cancelled`), waits
#' up to `timeout` seconds for workers to exit cleanly, and unlinks the
#' region and liveness files. The handle is dead afterwards; stopping it
#' again is a no-op. Only the creating process can stop a pool.
#'
#' @inheritParams kio_submit
#' @param timeout seconds to wait for workers' clean exit.
#'
#' @return Invisibly, `TRUE` if all workers exited within the timeout,
#'   `FALSE` otherwise (with a warning; workers still exit on their own).
#'
#' @export
kio_pool_stop <- function(pool, timeout = 5) {
  ok <- .Call(kio_pool_stop_call, pool, timeout)
  if (!ok)
    warning("kioto: pool stop timed out waiting for workers; they exit on ",
            "their own once they observe shutdown", call. = FALSE)
  invisible(ok)
}

#' Inspect a Pool
#'
#' A read-only snapshot of the pool region: registry states, parked-worker
#' count, queued injection entries, and result-slot occupancy.
#'
#' @inheritParams kio_submit
#'
#' @return A list with elements `name`, `role`, `max_workers`,
#'   `max_submitters`, `injection_cap`, `result_slots`, `slot_size`,
#'   `workers` (per-slot states), `parked`, `submitters` (per-slot states),
#'   `injection` (entries queued and unclaimed), `tasks` (result slots by
#'   state: pending / ok / err / cancel / died), `deque` (per-worker deque
#'   depths), and `shutdown`.
#'
#' @export
kio_pool_status <- function(pool) {
  st <- .Call(kio_pool_status_call, pool)
  st$workers <- c("free", "claiming", "live", "leaving",
                  "reaping")[st$workers + 1L]
  st$submitters <- c("free", "live", "reaping")[st$submitters + 1L]
  names(st$tasks) <- c("pending", "ok", "err", "cancel", "died")
  st
}

#' Dump a Pool's Distributed State
#'
#' A read-only debugging snapshot of the entire pool region, one level
#' deeper than [kio_pool_status()]: per-slot registry detail, the park /
#' ready / back-pressure masks unpacked per slot, and every occupied result
#' slot. State is distributed across processes and execution is
#' non-deterministic, so this is the first tool to reach for when a pool
#' hangs. The scan takes no locks and can race in-flight transitions;
#' each field is a consistent single read, rows need not be mutually
#' consistent.
#'
#' @inheritParams kio_submit
#'
#' @return A list with elements `name`, `shutdown`, `workers` (data frame:
#'   slot, status, pid, park_state, parked, deque `top` / `bottom`, and the
#'   in-flight result slot), `submitters` (data frame: slot, status, pid,
#'   result-slot subrange, queued injection entries, ready and
#'   full-waiter mask bits), and `tasks` (data frame of occupied result
#'   slots: slot, status, sequence, executing worker, parked waiter).
#'
#' @export
kio_pool_dump <- function(pool) {
  d <- .Call(kio_pool_dump_call, pool)
  w <- d$workers
  w$status <- c("free", "claiming", "live", "leaving",
                "reaping")[w$status + 1L]
  w$park_state <- c("running", "idle", "parked", "waking")[w$park_state + 1L]
  d$workers <- data.frame(slot = seq_along(w$status) - 1L, w)
  s <- d$submitters
  s$status <- c("free", "live", "reaping")[s$status + 1L]
  d$submitters <- data.frame(slot = seq_along(s$status) - 1L, s)
  tk <- lapply(d$tasks, `[`, !is.na(d$tasks$slot))
  tk$status <- c("free", "pending", "ok", "err", "cancel",
                 "died")[tk$status + 1L]
  d$tasks <- data.frame(tk)
  d
}

#' Cumulative Pool Counters
#'
#' Per-worker and per-submitter counters accumulated since each
#' participant joined, complementing the point-in-time snapshots of
#' [kio_pool_status()] and [kio_pool_dump()]. Nothing here costs the hot
#' paths anything: submitter counts are the injection rings' own monotonic
#' positions (submission writes nothing extra; the spill counter is bumped
#' only on the spill path itself, which a fresh region per payload already
#' dominates), and worker counters are kept process-locally and mirrored
#' into the region only when a worker parks, leaves, or passes its fairness
#' tick — so under continuous load a worker's row can lag by up to 61
#' claims, and is exact whenever that worker is parked, retired, or the
#' pool is quiescent.
#'
#' @inheritParams kio_submit
#'
#' @return A list of two data frames. `workers`: one row per worker slot
#'   with `status`, `pid`, `tasks` (task evaluations run, help-mode and
#'   nested inline execution included), `steals` (entries claimed from
#'   peers' deques), `injections` (entries claimed from injection rings),
#'   `parks` (kernel parks in the worker loop), `helps` (claims executed
#'   while blocked in a nested collect), and the current `deque` depth.
#'   `submitters`: one row per submitter slot with `status`, `pid`,
#'   `injected` (entries ever published to its injection ring), `claimed`
#'   (entries workers have taken from it), `spills` (payloads past the
#'   inline budget that traveled via a fresh region each — task payloads at
#'   submit and result payloads at publish, both attributed to the task's
#'   submitter; nonzero means `slot_size` is undersized for the traffic),
#'   and `queued` (`injected - claimed`). Counters reset when a slot is
#'   reused by a new joiner.
#'
#' @export
kio_pool_stats <- function(pool) {
  st <- .Call(kio_pool_stats_call, pool)
  w <- st$workers
  w$status <- c("free", "claiming", "live", "leaving",
                "reaping")[w$status + 1L]
  st$workers <- data.frame(slot = seq_along(w$status) - 1L, w)
  s <- st$submitters
  s$status <- c("free", "live", "reaping")[s$status + 1L]
  s$queued <- s$injected - s$claimed
  st$submitters <- data.frame(slot = seq_along(s$status) - 1L, s)
  st
}

#' Trace Task Lifecycle Events
#'
#' Registers a hook on a pool handle, called as `fn(event, id)` at each
#' task lifecycle event this process observes: `"submit"` when a task is
#' committed, and — on worker handles — `"start"` before a task's
#' evaluation, `"done"` / `"error"` when its result publishes, or `"drop"`
#' when a claimed task is discarded (cancelled before or during execution,
#' or its out-of-line payload died with its enqueuer). `id` identifies the
#' task as `"<submitter slot>:<counter>"`, stable across processes, so
#' logs from both sides of a pool can be correlated.
#'
#' Registration is per-handle and per-process. A submitter tracing its own
#' handle sees only `"submit"`; execution events happen on the workers. To
#' trace a worker, install the hook from a task, on the worker's own
#' handle bound as `pool`: `kio_submit(p, kio_pool_trace(pool, fn))`.
#' The disabled hook costs one pointer check per event site, and no event
#' sites exist on the channel hot path. An error raised by the hook
#' propagates as an infrastructure failure at its site — on a worker it
#' takes the worker down (unlike a task's own error, which publishes as
#' that task's ERR result).
#'
#' @inheritParams kio_submit
#' @param fn a `function(event, id)`, or `NULL` to remove a registered
#'   hook.
#'
#' @return Invisibly, `NULL`.
#'
#' @export
kio_pool_trace <- function(pool, fn = NULL)
  invisible(.Call(kio_pool_set_trace, pool, fn))

# Worker entry point: invoked as `Rscript -e 'kioto:::worker_main("<suffix>",
# <slot>)'` by the launcher. Rebuilds the region name from the compiled-in
# prefix plus the argv suffix, attaches writable, validates the header,
# claims its host-assigned slot (liveness lock before status CAS), points
# its death listener at the owner, and unparks the creator on reaching
# LIVE. The loop then lives in kio_pool_step: one claim in tier order
# (fairness tick, own deque, steal, injection) per task, parked indefinitely
# when idle, returning negative on shutdown or owner death. The eval hot
# path arms no error handler: a task error longjmps out of the step and
# kio_pool_fail_inflight publishes the caught condition as that task's ERR
# result — FALSE marks an error from outside any task eval, which is
# infrastructure failure and takes the worker down.
worker_main <- function(suffix, slot) {
  if (!"package:kioto" %in% search()) attachNamespace("kioto")
  h <- .Call(kio_pool_worker_join, suffix, slot)
  .Call(kio_pool_set_eval, h)
  status <- 0L
  rc <- -1L
  repeat {
    e <- tryCatch({
      repeat {
        rc <- .Call(kio_pool_step, h, 3600)
        if (rc < 0L) break
      }
      NULL
    }, error = function(e) e)
    if (is.null(e)) break
    if (!.Call(kio_pool_fail_inflight, h, e)) {
      cat("kioto worker error: ", conditionMessage(e), "\n", sep = "",
          file = stderr())
      status <- 1L
      break
    }
  }
  .Call(kio_pool_leave, h)
  # a retired worker (-2) lingers as a lifetime anchor for its uncollected
  # results: plain bounded sleeps, since no unpark can reach a released
  # slot; shutdown or owner death ends the linger
  if (rc == -2L)
    while (!.Call(kio_pool_lame_duck, h)) Sys.sleep(1)
  quit(save = "no", status = status)
}
