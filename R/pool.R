#' Create a Task Pool and Spawn Its Workers
#'
#' Creates a shared-memory task pool and spawns its worker processes.
#' Per-submitter injection rings feed the workers, and results are
#' published through result slots. A submission is one SHM ring write plus
#' at most one directed wake: no dispatcher process is in the loop. Each
#' worker owns a work-stealing deque. Idle workers steal from busy peers
#' and consume the injection rings. A fairness tick bounds the latency of
#' external submissions on a saturated pool.
#'
#' The lifetime of the pool is bound to the creating process, which holds
#' submitter slot 0 of the returned handle. Use this handle directly with
#' [rei_submit()] and [rei_collect()]. Other processes join as submitters
#' through [rei_pool_attach()]. Dropping the handle (or exiting R) shuts
#' the pool down as [rei_pool_stop()] does, but without the wait.
#'
#' Payload contents interoperate transparently with mori. A `mori::share()`d
#' object anywhere inside the arguments of a task or its result serializes
#' to its short identifier wire form through the mori hooks. It maps
#' zero-copy on the other side.
#'
#' The liveness lock files of the pool (the death-detection verdict) live
#' in a per-platform directory chosen at create time. This is `/dev/shm` on
#' Linux, and the per-user temporary directory on macOS and Windows. The
#' chosen path is recorded in the region, so every participant uses the
#' same files. The environment variable `REI_LIVENESS_DIR`, read in the
#' creating process, overrides the default.
#'
#' @param n_workers number of worker processes to spawn, at most
#'   `max_workers`.
#' @param max_workers worker registry capacity (at most 64).
#' @param max_submitters submitter registry capacity (at most 64). Each
#'   submitter owns its own injection ring and an equal share of
#'   `result_slots`.
#' @param injection_cap entries per submitter injection ring. A power of
#'   two.
#' @param per_worker_cap entries per worker work-stealing deque. A power of
#'   two.
#' @param result_slots total result slots, partitioned equally across the
#'   submitter slots and rounded up to a multiple of `max_submitters`. This
#'   bounds the outstanding (uncollected) tasks of each submitter.
#' @param slot_size bytes per queue entry and result slot. A power of two
#'   between 128 and 2^20. A payload (task or result) that serializes past
#'   the inline budget travels in a fresh region per payload. This is an
#'   order-of-magnitude latency cliff, surfaced per submitter as
#'   [rei_pool_stats()]`$submitters$spills`. The default `512L` keeps
#'   typical expression-plus-arguments tasks inline. Pools that move only
#'   scalar payloads can drop to `256L`.
#' @param launcher a `function(token, slot)` that arranges for an R process
#'   to call `rei:::worker_main(token, slot)`. The default
#'   [rei_launcher()] spawns `Rscript` and propagates the `.libPaths()` of
#'   the host. Its `stdout` and `stderr` arguments direct the worker
#'   output. A custom launcher must arrange the library paths itself.
#' @param startup_timeout seconds to wait for all workers to join. On
#'   expiry, rei destroys the pool and raises `rei_error_startup` (see
#'   [rei_error]).
#'
#' @return A pool handle (class `"rei_pool"`) holding submitter slot 0.
#'   Handles are process-private and do not survive `fork()`.
#'
#' @examples
#' p <- rei_pool()
#' t <- rei_submit(p, x + y, x = 1, y = 2)
#' rei_collect(t)
#' rei_pool_stop(p)
#'
#' @export
rei_pool <- function(
  n_workers = 1L,
  max_workers = n_workers,
  max_submitters = 8L,
  injection_cap = 1024L,
  per_worker_cap = 1024L,
  result_slots = 4096L,
  slot_size = 512L,
  launcher = rei_launcher(),
  startup_timeout = 30
) {
  n_workers <- as.integer(n_workers)
  if (is.na(n_workers) || n_workers < 1L) {
    stop("rei: n_workers must be at least 1", call. = FALSE)
  }
  if (n_workers > as.integer(max_workers)) {
    stop("rei: n_workers exceeds max_workers", call. = FALSE)
  }
  p <- .Call(
    rei_pool_create,
    max_workers,
    max_submitters,
    injection_cap,
    per_worker_cap,
    result_slots,
    slot_size
  )
  token <- .Call(rei_pool_suffix, p)
  for (slot in seq_len(n_workers) - 1L) {
    launcher(token, slot)
  }
  if (
    !.Call(rei_pool_ready_wait, p, seq_len(n_workers) - 1L, startup_timeout)
  ) {
    .Call(rei_pool_destroy, p)
    stop_rei(
      "rei_error_startup",
      paste0(
        "rei: workers failed to attach within ",
        format(startup_timeout),
        " seconds"
      )
    )
  }
  p
}

#' Grow or Shrink the Worker Set of a Pool
#'
#' `rei_spawn_workers()` spawns additional workers into free registry
#' slots, up to the `max_workers` of the pool, and waits for them to join.
#' `rei_retire_worker()` asks one worker to exit cleanly. The request is
#' non-blocking and never preemptive. The worker observes it between tasks
#' and releases its slot. The remaining workers consume in place any work
#' still queued on its deque. The process of a retired worker can linger
#' briefly as a lifetime anchor for the results it produced that are not
#' yet collected.
#'
#' Slots free up when workers retire, exit at shutdown, or die and are
#' reaped. So a pool can cycle workers within its registry capacity for
#' its whole lifetime. Only the creating process can resize a pool.
#'
#' @inheritParams rei_submit
#' @inheritParams rei_pool
#' @param n number of workers to spawn.
#' @param slot the slot index of the worker (0-based, as reported by
#'   [rei_pool_dump()]).
#'
#' @return `rei_spawn_workers()` invisibly returns the slot indices
#'   spawned into. `rei_retire_worker()` invisibly returns `NULL`.
#'
#' @examplesIf interactive()
#' p <- rei_pool(n_workers = 1L, max_workers = 2L)
#' rei_retire_worker(p, 0L)
#' rei_spawn_workers(p)
#' rei_pool_stop(p)
#'
#' @export
rei_spawn_workers <- function(
  pool,
  n = 1L,
  launcher = rei_launcher(),
  startup_timeout = 30
) {
  n <- as.integer(n)
  if (is.na(n) || n < 1L) {
    stop("rei: n must be at least 1", call. = FALSE)
  }
  free <- which(rei_pool_status(pool)[["workers"]] == "free") - 1L
  if (length(free) < n) {
    stop(
      "rei: not enough free worker slots (",
      length(free),
      " free)",
      call. = FALSE
    )
  }
  slots <- free[seq_len(n)]
  token <- .Call(rei_pool_suffix, pool)
  for (slot in slots) {
    launcher(token, slot)
  }
  if (!.Call(rei_pool_ready_wait, pool, as.integer(slots), startup_timeout)) {
    stop_rei(
      "rei_error_startup",
      paste0(
        "rei: workers failed to attach within ",
        format(startup_timeout),
        " seconds"
      )
    )
  }
  invisible(as.integer(slots))
}

#' @rdname rei_spawn_workers
#' @export
rei_retire_worker <- function(pool, slot) {
  invisible(.Call(rei_pool_retire, pool, as.integer(slot)))
}

#' Attach to a Pool as a Submitter
#'
#' Joins an existing pool from another process. Claims a free submitter
#' slot with its own injection ring and result-slot subrange. The name of
#' the pool travels out of band: it is `rei_pool_status(p)$name` on the
#' creator.
#'
#' @param name the region name of the pool, or its suffix (the part after
#'   the platform prefix).
#'
#' @return A pool handle (class `"rei_pool"`) holding a submitter slot.
#'
#' @examples
#' p <- rei_pool()
#' name <- rei_pool_status(p)[["name"]]
#' # `name` travels out of band to the joining process, which runs:
#' # pa <- rei_pool_attach(name)
#' # rei_collect(rei_submit(pa, runif(3)))
#' rei_pool_stop(p)
#'
#' @export
rei_pool_attach <- function(name) {
  if (!is.character(name) || length(name) != 1L || is.na(name)) {
    stop("rei: name must be a character string", call. = FALSE)
  }
  .Call(rei_pool_attach_call, sub(".*?([0-9a-f]+_[0-9a-f]+)$", "\\1", name))
}

#' Submit a Task and Collect Its Result
#'
#' `rei_submit()` captures `expr` unevaluated, serializes it with its
#' named arguments into the injection ring of the submitter, and returns a
#' task handle immediately. Payloads past the inline budget travel in a
#' fresh region. A worker evaluates the expression in a fresh environment
#' that contains the arguments as bindings. `rei_collect()` blocks until
#' the result is published, then returns the value of the task. If the
#' task raised an error, `rei_collect()` signals that condition again in
#' the collecting process.
#'
#' Submission blocks only when the injection ring of the submitter is
#' full: back-pressure is per-submitter. On `.timeout` expiry, submission
#' raises `rei_error_submit_timeout` instead of stalling. If no result
#' arrives within `timeout`, collection returns the `rei_timeout` sentinel
#' (class `c("rei_timeout", "rei_sentinel")`). Collecting a task whose
#' handle was cancelled (or whose pool was stopped) raises
#' `rei_error_cancelled`. Collecting a task whose executing worker died
#' raises `rei_error_worker_died`, carrying the slot and pid of the worker
#' (see [rei_error]). Worker death is detected at OS notification latency:
#' a kernel-released lock is the verdict, with no heartbeats and no
#' polling. The death fails exactly the tasks that the dead worker
#' claimed, and the surviving workers consume the work still queued on its
#' deque.
#'
#' @section Outcomes:
#' A timeout on collect is a normal outcome and is returned. Every
#' exceptional outcome is raised as a classed condition (see [rei_error]):
#'
#' | outcome | surfaced as | class |
#' |---|---|---|
#' | result published | the value, returned | — |
#' | no result within `timeout` | sentinel, returned | `c("rei_timeout", "rei_sentinel")` |
#' | ring full past `.timeout` | raised by `rei_submit()` | `rei_error_submit_timeout` |
#' | result slots exhausted | raised by `rei_submit()` | `rei_error_slots_exhausted` |
#' | pool stopped, or owner died | raised by `rei_submit()` | `rei_error_stopped` |
#' | task raised an error | re-signalled on collect | the condition classes of the task itself |
#' | cancelled, or pool stopped | raised on collect | `rei_error_cancelled` |
#' | executing worker died | raised on collect | `rei_error_worker_died` |
#'
#' A re-signalled task error is a transport condition: it carries the
#' original classes, `message`, `call`, and every named field the
#' payload codec can carry within the result slot's inline budget, with
#' anything untransportable dropped and named in a `dropped_fields`
#' field. See the Task error transport section of [rei_error].
#'
#' A task expression sees the handle of its evaluating worker as `pool`
#' (beneath the arguments in `...`), so a task can submit nested subtasks.
#' `rei_submit(pool, ...)` inside a task pushes onto the work-stealing
#' deque of the worker itself: no ring, no wait. A full deque runs the
#' subtask inline instead. A worker blocked in `rei_collect()` on a nested
#' handle helps instead of sleeping. It executes work from its own deque
#' (and steals from peers) until the awaited result is published. So
#' nested fan-outs run at fork/join cost and never deadlock the pool. A
#' nested submission claims a submitter slot for the worker on first use.
#'
#' A handle can be collected exactly once: the result slot is released to
#' the pool as the value is returned. If an uncollected handle goes to the
#' garbage collector, a still-queued task is cancelled and a published
#' result is discarded. To wait on several handles at once,
#' [rei_collect_any()] reports the first terminal task and
#' [rei_collect_all()] returns every result in input order.
#'
#' @param pool a pool handle from [rei_pool()] or [rei_pool_attach()], or —
#'   inside a task — the own handle of the worker, bound as `pool`.
#' @param expr an expression, captured unevaluated. This differs from
#'   [rei_channel()], which requires its expression pre-quoted. The
#'   expression sees only the arguments in `...` and the global environment
#'   of the worker. The expression itself must load any packages it needs.
#' @param ... named values bound in the evaluation environment. The values
#'   are serialized. `mori::share()`d objects reduce to identifiers and map
#'   zero-copy on the worker.
#' @param .timeout seconds to wait for injection-ring space before the
#'   call errors. `Inf` (the default) waits indefinitely. Ctrl-C stays
#'   responsive.
#' @param task a task handle from `rei_submit()`.
#' @param timeout seconds to wait for the result before the call returns
#'   the `rei_timeout` sentinel. `Inf` (the default) waits indefinitely,
#'   and `0` polls.
#'
#' @return `rei_submit()` returns a task handle (class `"rei_task"`).
#'   `rei_collect()` returns the value of the task, or the `rei_timeout`
#'   sentinel.
#'
#' @examples
#' p <- rei_pool()
#' t <- rei_submit(p, sum(x), x = runif(10))
#' rei_collect(t, timeout = 30)
#' rei_pool_stop(p)
#'
#' @export
rei_submit <- function(pool, expr, ..., .timeout = Inf) {
  .Call(rei_pool_submit_expr, pool, substitute(expr), list(...), .timeout, 0L)
}

#' @rdname rei_submit
#' @export
rei_collect <- function(task, timeout = Inf) {
  .Call(rei_pool_collect, task, timeout)
}

#' Submit a Batch of Tasks
#'
#' `rei_submit_batch()` submits one task per element of `exprs` in a
#' single `.Call`: one R boundary crossing and one wake-up sweep per
#' batch instead of per task. At target rates the call boundary is a
#' first-order cost, so a burst submitted this way reaches the workers
#' sooner than the same burst looped through [rei_submit()]. Pair with
#' [rei_collect_all()] to batch the collection side too.
#'
#' Each task's wire payload is the same `list(expr, args)` as
#' [rei_submit()]'s, with the `...` arguments shared by every task in
#' the batch. Unlike `rei_submit()`, expressions are not captured:
#' the elements of `exprs` are pre-quoted (or plain values, which
#' evaluate to themselves).
#'
#' Submission semantics per task are [rei_submit()]'s, with one
#' difference: if the injection ring fills past `.timeout` mid-batch,
#' the call returns the handles accepted so far instead of raising
#' `rei_error_submit_timeout`. Fatal outcomes (pool stopped, result
#' slots exhausted) still raise; tasks already submitted stay valid and
#' collectible.
#'
#' @inheritParams rei_submit
#' @param exprs a list of expressions, one per task. Quote them
#'   yourself: elements of a list cannot be captured unevaluated.
#'
#' @return A list of task handles (class `"rei_task"`), one per
#'   accepted task — shorter than `exprs` when the ring filled past
#'   `.timeout` mid-batch.
#'
#' @examples
#' p <- rei_pool()
#' ts <- rei_submit_batch(p, list(quote(1 + 1), quote(2 + 2)))
#' rei_collect_all(ts, timeout = 30)
#' rei_pool_stop(p)
#'
#' @export
rei_submit_batch <- function(pool, exprs, ..., .timeout = Inf) {
  .Call(rei_pool_submit_batch, pool, exprs, list(...), .timeout, 0L)
}

#' Collect the First Available Result From Several Tasks
#'
#' `rei_collect_any()` waits on several task handles at once and returns
#' as soon as any of them reaches a terminal state — result published,
#' error raised, cancelled, or its worker died. Among handles already
#' terminal, the earliest in `tasks` is reported. The wait parks on the
#' submitter's single parker: any publishing worker wakes it directly,
#' with no polling. For the whole set at once, [rei_collect_all()]
#' waits until every task is terminal and returns all results in input
#' order.
#'
#' @section Outcomes:
#' As for [rei_collect()], but attributed to a handle by position:
#'
#' | outcome | surfaced as | class |
#' |---|---|---|
#' | result published | `list(index, value)`, returned | — |
#' | nothing terminal within `timeout` | sentinel, returned | `c("rei_timeout", "rei_sentinel")` |
#' | task raised an error | re-signalled with an `index` field | the condition classes of the task itself |
#' | cancelled, or pool stopped | raised with an `index` field | `rei_error_cancelled` |
#' | executing worker died | raised with an `index` field | `rei_error_worker_died` |
#'
#' The `index` field of a raised condition is the 1-based position of the
#' task in `tasks` (conditions are lists, so the field travels in place).
#' The reported handle is consumed; the remaining handles stay valid and
#' collectible.
#'
#' @param tasks a non-empty list of task handles from [rei_submit()] on
#'   the same pool handle.
#' @inheritParams rei_submit
#'
#' @return For a published result, `list(index = i, value = v)`: the
#'   1-based position of the task in `tasks` and its value. Otherwise the
#'   `rei_timeout` sentinel.
#'
#' @examplesIf interactive()
#' p <- rei_pool(2L)
#' slow <- rei_submit(p, { Sys.sleep(0.5); "slow" })
#' fast <- rei_submit(p, "fast")
#' # completion order, not submission order
#' rei_collect_any(list(slow, fast), timeout = 30)
#' rei_collect(slow, timeout = 30)
#' rei_pool_stop(p)
#'
#' @export
rei_collect_any <- function(tasks, timeout = Inf) {
  res <- .Call(rei_pool_collect_any, tasks, timeout)
  if (inherits(res, "rei_caught")) {
    cond <- res[[1L]]
    if (is.list(cond)) {
      cond$index <- attr(res, "index")
    }
    stop(cond)
  }
  res
}

#' Collect the Results of Several Tasks, in Order
#'
#' `rei_collect_all()` waits until every task in `tasks` reaches a
#' terminal state and returns all results in input order — the batch
#' counterpart of [rei_collect()] for fire-then-collect patterns, with
#' one R call boundary for the whole set instead of one per task. The
#' wait parks on the submitter's single parker: any publishing worker
#' wakes it directly, with no polling.
#'
#' For homogeneous element-wise work, [rei_map()] remains the right
#' answer (it batches submission and staging, not just collection).
#' `rei_collect_all()` is for heterogeneous handle sets — different
#' expressions and arguments — which is what the per-task API is for.
#'
#' @section Outcomes:
#' As for [rei_collect()], attributed to a handle by position as in
#' [rei_collect_any()]:
#'
#' | outcome | surfaced as | class |
#' |---|---|---|
#' | all results published | list of values, returned | — |
#' | not all terminal within `timeout` | sentinel, returned | `c("rei_timeout", "rei_sentinel")` |
#' | a task raised an error | re-signalled with an `index` field | the condition classes of the task itself |
#' | a task cancelled, or pool stopped | raised with an `index` field | `rei_error_cancelled` |
#' | an executing worker died | raised with an `index` field | `rei_error_worker_died` |
#'
#' The `index` field of a raised condition is the 1-based position in
#' `tasks` of the first such task. Handles up to and including the
#' reported one are consumed; the remaining handles stay valid and
#' collectible. A timeout consumes nothing: every handle stays valid and
#' collectible.
#'
#' @param tasks a non-empty list of task handles from [rei_submit()] on
#'   the same pool handle.
#' @inheritParams rei_submit
#'
#' @return A plain list of the task values in the order of `tasks`; the
#'   names of `tasks` carry over. On timeout, the `rei_timeout`
#'   sentinel.
#'
#' @examplesIf interactive()
#' p <- rei_pool(2L)
#' ts <- list(
#'   total = rei_submit(p, sum(x), x = runif(10)),
#'   label = rei_submit(p, "done")
#' )
#' rei_collect_all(ts, timeout = 30)
#' rei_pool_stop(p)
#'
#' @export
rei_collect_all <- function(tasks, timeout = Inf) {
  res <- .Call(rei_pool_collect_all, tasks, timeout)
  if (inherits(res, "rei_caught")) {
    cond <- res[[1L]]
    if (is.list(cond)) {
      cond$index <- attr(res, "index")
    }
    stop(cond)
  }
  res
}

#' Cancel a Task
#'
#' Advisory and discard-only, never preemptive. The worker skips a task
#' that is still queued. A task already executing runs to completion, and
#' its result is dropped. Collecting a cancelled handle raises
#' `rei_error_cancelled` (see [rei_error]).
#'
#' @inheritParams rei_submit
#'
#' @return Invisibly, `TRUE` if this call cancelled the task. `FALSE` if
#'   the call was too late: the task completed, was already cancelled, or
#'   its pool is gone.
#'
#' @examples
#' p <- rei_pool()
#' t <- rei_submit(p, runif(1))
#' rei_cancel(t)
#' rei_pool_stop(p)
#'
#' @export
rei_cancel <- function(task) invisible(.Call(rei_pool_cancel, task))

#' Stop a Pool
#'
#' Broadcasts shutdown, wakes every parked participant, and cancels all
#' pending tasks (blocked collectors raise `rei_error_cancelled`). Then
#' waits up to `timeout` seconds for the workers to exit cleanly, and
#' unlinks the region and the liveness files. The handle is dead
#' afterwards, and stopping it again is a no-op. Only the creating process
#' can stop a pool.
#'
#' @inheritParams rei_submit
#' @param timeout seconds to wait for the clean exit of the workers.
#'
#' @return Invisibly, `TRUE` if all workers exited within the timeout.
#'   `FALSE` otherwise, with a warning (the workers still exit on their
#'   own).
#'
#' @examples
#' p <- rei_pool()
#' t <- rei_submit(p, 1 + 1)
#' rei_collect(t)
#' rei_pool_stop(p)
#'
#' @export
rei_pool_stop <- function(pool, timeout = 5) {
  ok <- .Call(rei_pool_stop_call, pool, timeout)
  if (!ok) {
    warning(
      "rei: pool stop timed out waiting for workers; they exit on ",
      "their own once they observe shutdown",
      call. = FALSE
    )
  }
  invisible(ok)
}

#' Inspect a Pool
#'
#' A read-only snapshot of the pool region: registry states, parked-worker
#' count, queued injection entries, and result-slot occupancy.
#'
#' @inheritParams rei_submit
#'
#' @return A list with elements `name`, `role`, `max_workers`,
#'   `max_submitters`, `injection_cap`, `result_slots`, `slot_size`,
#'   `workers` (per-slot states), `parked`, `submitters` (per-slot states),
#'   `injection` (entries queued and unclaimed), `tasks` (result slots by
#'   state: pending / ok / err / cancel / died), `deque` (per-worker deque
#'   depths), and `shutdown`.
#'
#' @examples
#' p <- rei_pool()
#' rei_pool_status(p)
#' rei_pool_stop(p)
#'
#' @export
rei_pool_status <- function(pool) {
  st <- .Call(rei_pool_status_call, pool)
  st[["workers"]] <- c("free", "claiming", "live", "leaving", "reaping")[
    st[["workers"]] + 1L
  ]
  st[["submitters"]] <- c("free", "live", "reaping")[st[["submitters"]] + 1L]
  names(st[["tasks"]]) <- c("pending", "ok", "err", "cancel", "died")
  st
}

#' Dump the Distributed State of a Pool
#'
#' A read-only debugging snapshot of the entire pool region, one level
#' deeper than [rei_pool_status()]. It shows per-slot registry detail, the
#' park, ready, and back-pressure masks unpacked per slot, and every
#' occupied result slot. State is distributed across processes and
#' execution is non-deterministic, so reach for this tool first when a
#' pool hangs. The scan takes no locks and can race in-flight transitions.
#' Each field is a consistent single read. The rows need not be mutually
#' consistent.
#'
#' @inheritParams rei_submit
#'
#' @return A list with elements `name`, `shutdown`, `workers` (data frame:
#'   slot, status, pid, park_state, parked, deque `top` and `bottom`, and
#'   the in-flight result slot), `submitters` (data frame: slot, status,
#'   pid, result-slot subrange, queued injection entries, ready and
#'   full-waiter mask bits), `tasks` (data frame of occupied result slots:
#'   slot, status, sequence, executing worker, parked waiter), `help`, and
#'   `local`. `help` is the help-wanted doorbell: `TRUE` while injection
#'   entries are queued with every worker busy, awaiting pickup by a map
#'   runner at its next batch transition or a worker helping out of a
#'   nested [rei_collect()] (the `rehome` event of [rei_pool_trace()]).
#'   `local` is the process-private machinery of this handle: the
#'   occupancy of the producer free list (`fl_entries`, `fl_bytes`), its
#'   reuse count (`fl_hits`), the `open_hits` and `open_misses` of the
#'   consumer mapping cache, and `collect_parks` (how often a collect on
#'   this handle parked waiting for a result).
#'
#' @examples
#' p <- rei_pool()
#' t <- rei_submit(p, runif(1))
#' rei_pool_dump(p)
#' rei_collect(t)
#' rei_pool_stop(p)
#'
#' @export
rei_pool_dump <- function(pool) {
  d <- .Call(rei_pool_dump_call, pool)
  w <- d[["workers"]]
  w[["status"]] <- c("free", "claiming", "live", "leaving", "reaping")[
    w[["status"]] + 1L
  ]
  w[["park_state"]] <- c("running", "idle", "parked", "waking")[
    w[["park_state"]] + 1L
  ]
  d[["workers"]] <- data.frame(slot = seq_along(w[["status"]]) - 1L, w)
  s <- d[["submitters"]]
  s[["status"]] <- c("free", "live", "reaping")[s[["status"]] + 1L]
  d[["submitters"]] <- data.frame(slot = seq_along(s[["status"]]) - 1L, s)
  tk <- lapply(d[["tasks"]], `[`, !is.na(d[["tasks"]][["slot"]]))
  tk[["status"]] <- c("free", "pending", "ok", "err", "cancel", "died")[
    tk[["status"]] + 1L
  ]
  d[["tasks"]] <- data.frame(tk)
  d
}

#' Cumulative Pool Counters
#'
#' Per-worker and per-submitter counters, accumulated since each
#' participant joined. They complement the point-in-time snapshots of
#' [rei_pool_status()] and [rei_pool_dump()]. Nothing here costs the hot
#' paths anything. The submitter counts are the monotonic positions of the
#' injection rings themselves: submission writes nothing extra, and the
#' spill counter moves only on the spill path, which a fresh region per
#' payload already dominates. The worker counters are kept process-locally
#' and mirrored into the region only when a worker parks, leaves, or
#' passes its fairness tick. So under continuous load, the row of a worker
#' can lag by up to 61 claims. The row is exact whenever that worker is
#' parked or retired, or the pool is quiescent.
#'
#' @inheritParams rei_submit
#'
#' @return A list of two data frames. `workers`: one row per worker slot,
#'   with `status`, `pid`, `tasks` (task evaluations run, help-mode and
#'   nested inline execution included), `steals` (entries claimed from the
#'   deques of peers), `injections` (entries claimed from injection
#'   rings), `parks` (kernel parks in the worker loop), `helps` (claims
#'   executed while blocked in a nested collect), and the current `deque`
#'   depth. `submitters`: one row per submitter slot, with `status`,
#'   `pid`, `injected` (entries ever published to its injection ring),
#'   `claimed` (entries the workers took from it), `spills` (payloads past
#'   the inline budget that traveled in their own region — task payloads
#'   at submit and result payloads at publish, both attributed to the
#'   submitter of the task. A nonzero value means `slot_size` is
#'   undersized for the traffic), `spill_reuse` (the subset of `spills`
#'   that recycled a retired region from the free list of the producer
#'   instead of creating one), and `queued` (`injected - claimed`).
#'   Steady-state spill traffic approaches `spills`, so
#'   `spills - spill_reuse` is the region-churn rate. Counters reset when
#'   a new joiner reuses a slot.
#'
#' @examples
#' p <- rei_pool()
#' t <- rei_submit(p, runif(5))
#' rei_collect(t)
#' rei_pool_stats(p)
#' rei_pool_stop(p)
#'
#' @export
rei_pool_stats <- function(pool) {
  st <- .Call(rei_pool_stats_call, pool)
  w <- st[["workers"]]
  w[["status"]] <- c("free", "claiming", "live", "leaving", "reaping")[
    w[["status"]] + 1L
  ]
  st[["workers"]] <- data.frame(slot = seq_along(w[["status"]]) - 1L, w)
  s <- st[["submitters"]]
  s[["status"]] <- c("free", "live", "reaping")[s[["status"]] + 1L]
  s[["queued"]] <- s[["injected"]] - s[["claimed"]]
  st[["submitters"]] <- data.frame(slot = seq_along(s[["status"]]) - 1L, s)
  st
}

#' Trace Task Lifecycle Events
#'
#' Registers a hook on a pool handle. The hook is called as
#' `fn(event, id)` at each task lifecycle event that this process
#' observes:
#'
#' * `"submit"` when a task is committed.
#' * On worker handles: `"start"` before the evaluation of a task, and
#'   `"done"` or `"error"` when its result is published.
#' * `"drop"` when a claimed task is discarded (cancelled before or during
#'   execution, or its out-of-line payload died with its enqueuer).
#' * `"rehome"` when a doorbell help beat claims a map runner. The helper
#'   moves it onto its own deque — where idle peers can steal it — instead
#'   of executing it nested.
#'
#' `id` identifies the task as `"<submitter slot>:<counter>"`. This id is
#' stable across processes, so logs from both sides of a pool can be
#' correlated.
#'
#' Registration is per-handle and per-process. A submitter that traces its
#' own handle sees only `"submit"`. Execution events happen on the
#' workers. To trace a worker, install the hook from a task, on the own
#' handle of the worker bound as `pool`:
#' `rei_submit(p, rei_pool_trace(pool, fn))`. The disabled hook costs one
#' pointer check per event site, and no event sites exist on the channel
#' hot path. An error raised by the hook propagates as an infrastructure
#' failure at its site. On a worker, it takes the worker down. This
#' differs from the own error of a task, which is published as the ERR
#' result of that task.
#'
#' @inheritParams rei_submit
#' @param fn a `function(event, id)`, or `NULL` to remove a registered
#'   hook.
#'
#' @return Invisibly, `NULL`.
#'
#' @examples
#' p <- rei_pool()
#' rei_pool_trace(p, function(event, id) cat(event, id, "\n"))
#' t <- rei_submit(p, 1 + 1)
#' rei_collect(t)
#' rei_pool_trace(p)
#' rei_pool_stop(p)
#'
#' @export
rei_pool_trace <- function(pool, fn = NULL) {
  invisible(.Call(rei_pool_set_trace, pool, fn))
}

# Worker entry point: invoked through the Rscript child runner by the launcher.
# Rebuilds the region name from the compiled-in prefix plus the token (the
# name's suffix), attaches writable, validates the header,
# claims its host-assigned slot (liveness lock before status CAS), points
# its death listener at the owner, and unparks the creator on reaching
# LIVE. The loop then lives in rei_pool_run: claims in tier order
# (fairness tick, own deque, steal, injection), parked indefinitely when
# idle, returning only on shutdown, owner death, or retire — the per-task
# R round-trip is replaced by an interrupt check and a deadline recompute
# in C. rei_pool_step remains for the test harness's single-stepping. The
# eval hot path arms no error handler: a task error longjmps out of the
# run, and rei_pool_run_outcome dispatches on what the run produced —
# an exit code passes through to end the loop, a caught condition is
# published as that task's ERR result, and 1 marks an error from outside
# any task eval, which is infrastructure failure and takes the worker
# down.
worker_main <- function(token, slot) {
  if (!any(search() == "package:rei")) {
    attachNamespace("rei")
  }
  .Call(rei_tune_malloc)
  h <- .Call(rei_pool_worker_join, token, slot)
  .Call(rei_pool_set_eval, h)
  status <- 0L
  rc <- -1L
  repeat {
    e <- tryCatch(.Call(rei_pool_run, h, 3600), error = function(e) e)
    rc <- .Call(rei_pool_run_outcome, h, e)
    if (rc < 0L) {
      break
    }
    if (rc > 0L) {
      cat(
        "rei worker error: ",
        conditionMessage(e),
        "\n",
        sep = "",
        file = stderr()
      )
      status <- 1L
      break
    }
  }
  .Call(rei_pool_leave, h)
  # a retired worker (-2) lingers as a lifetime anchor for its uncollected
  # results: plain bounded sleeps, since no unpark can reach a released
  # slot; shutdown or owner death ends the linger
  if (rc == -2L) {
    while (!.Call(rei_pool_lame_duck, h)) {
      Sys.sleep(1)
    }
  }
  quit(save = "no", status = status)
}
