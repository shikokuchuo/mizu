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
#' [mizu_submit()] and [mizu_collect()]. Other processes join as submitters
#' through [mizu_pool_attach()]. Dropping the handle (or exiting R) shuts
#' the pool down as [mizu_pool_stop()] does, but without the wait.
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
#' same files. The environment variable `MIZU_LIVENESS_DIR`, read in the
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
#'   [mizu_pool_stats()]`$submitters$spills`. The default `512L` keeps
#'   typical expression-plus-arguments tasks inline. Pools that move only
#'   scalar payloads can drop to `256L`.
#' @param launcher a `function(token, slot)` that arranges for an R process
#'   to call `mizu:::worker_main(token, slot)`. The default
#'   [mizu_launcher()] spawns `Rscript` and propagates the `.libPaths()` of
#'   the host. Its `stdout` and `stderr` arguments direct the worker
#'   output. A custom launcher must arrange the library paths itself.
#' @param startup_timeout seconds to wait for all workers to join. On
#'   expiry, mizu destroys the pool and raises `mizu_error_startup` (see
#'   [mizu_error]).
#'
#' @return A pool handle (class `"mizu_pool"`) holding submitter slot 0.
#'   Handles are process-private and do not survive `fork()`.
#'
#' @examples
#' p <- mizu_pool()
#' t <- mizu_submit(p, x + y, x = 1, y = 2)
#' mizu_collect(t)
#' mizu_pool_stop(p)
#'
#' @export
mizu_pool <- function(
  n_workers = 1L,
  max_workers = n_workers,
  max_submitters = 8L,
  injection_cap = 1024L,
  per_worker_cap = 1024L,
  result_slots = 4096L,
  slot_size = 512L,
  launcher = mizu_launcher(),
  startup_timeout = 30
) {
  n_workers <- as.integer(n_workers)
  if (is.na(n_workers) || n_workers < 1L) {
    stop("mizu: n_workers must be at least 1", call. = FALSE)
  }
  if (n_workers > as.integer(max_workers)) {
    stop("mizu: n_workers exceeds max_workers", call. = FALSE)
  }
  p <- .Call(
    mizu_pool_create,
    max_workers,
    max_submitters,
    injection_cap,
    per_worker_cap,
    result_slots,
    slot_size
  )
  tryCatch(
    launch_workers(p, seq_len(n_workers) - 1L, launcher, startup_timeout),
    # a pool that never came up is destroyed, not left to its finalizer
    mizu_error_startup = function(e) {
      .Call(mizu_pool_destroy, p)
      stop(e)
    }
  )
  p
}

# Spawn the workers for `slots` through the launcher and await their join:
# the one launch path shared by mizu_pool (initial set) and
# mizu_spawn_workers (growth). Raises mizu_error_startup on timeout.
launch_workers <- function(pool, slots, launcher, startup_timeout) {
  token <- .Call(mizu_pool_suffix, pool)
  for (slot in slots) {
    launcher(token, slot)
  }
  if (!.Call(mizu_pool_ready_wait, pool, as.integer(slots), startup_timeout)) {
    stop_startup("workers", startup_timeout)
  }
  invisible(as.integer(slots))
}

#' Grow or Shrink the Worker Set of a Pool
#'
#' `mizu_spawn_workers()` spawns additional workers into free registry
#' slots, up to the `max_workers` of the pool, and waits for them to join.
#' `mizu_retire_worker()` asks one worker to exit cleanly. The request is
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
#' @inheritParams mizu_pool_stop
#' @inheritParams mizu_pool
#' @param n number of workers to spawn.
#' @param slot the slot index of the worker (0-based, as reported by
#'   [mizu_pool_dump()]).
#'
#' @return `mizu_spawn_workers()` invisibly returns the slot indices
#'   spawned into. `mizu_retire_worker()` invisibly returns `NULL`.
#'
#' @examplesIf interactive()
#' p <- mizu_pool(n_workers = 1L, max_workers = 2L)
#' mizu_retire_worker(p, 0L)
#' mizu_spawn_workers(p)
#' mizu_pool_stop(p)
#'
#' @export
mizu_spawn_workers <- function(
  pool,
  n = 1L,
  launcher = mizu_launcher(),
  startup_timeout = 30
) {
  n <- as.integer(n)
  if (is.na(n) || n < 1L) {
    stop("mizu: n must be at least 1", call. = FALSE)
  }
  free <- which(mizu_pool_status(pool)[["workers"]] == "free") - 1L
  if (length(free) < n) {
    stop(
      "mizu: not enough free worker slots (",
      length(free),
      " free)",
      call. = FALSE
    )
  }
  launch_workers(pool, free[seq_len(n)], launcher, startup_timeout)
}

#' @rdname mizu_spawn_workers
#' @export
mizu_retire_worker <- function(pool, slot) {
  invisible(.Call(mizu_pool_retire, pool, as.integer(slot)))
}

#' Attach to a Pool as a Submitter
#'
#' Joins an existing pool from another process. Claims a free submitter
#' slot with its own injection ring and result-slot subrange. The name of
#' the pool travels out of band: it is `mizu_pool_status(p)$name` on the
#' creator.
#'
#' @param name the region name of the pool, or its suffix (the part after
#'   the platform prefix).
#'
#' @return A pool handle (class `"mizu_pool"`) holding a submitter slot.
#'
#' @examples
#' p <- mizu_pool()
#' name <- mizu_pool_status(p)[["name"]]
#' # `name` travels out of band to the joining process, which runs:
#' # pa <- mizu_pool_attach(name)
#' # mizu_collect(mizu_submit(pa, runif(3)))
#' mizu_pool_stop(p)
#'
#' @export
mizu_pool_attach <- function(name) {
  if (!is.character(name) || length(name) != 1L || is.na(name)) {
    stop("mizu: name must be a character string", call. = FALSE)
  }
  .Call(mizu_pool_attach_call, sub(".*?([0-9a-f]+_[0-9a-f]+)$", "\\1", name))
}

#' Submit a Task and Collect Its Result
#'
#' `mizu_submit()` captures `.expr` unevaluated, serializes it with its
#' named arguments into the injection ring of the submitter, and returns a
#' task handle immediately. Payloads past the inline budget travel in a
#' fresh region. A worker evaluates the expression in a fresh environment
#' that contains the arguments as bindings. `mizu_collect()` blocks until
#' the result is published, then returns the value of the task. If the
#' task raised an error, `mizu_collect()` signals that condition again in
#' the collecting process.
#'
#' Submission blocks only when the injection ring of the submitter is
#' full: back-pressure is per-submitter. On `.timeout` expiry, submission
#' raises `mizu_error_submit_timeout` instead of stalling. If no result
#' arrives within `timeout`, collection returns the `mizu_timeout` sentinel
#' (class `c("mizu_timeout", "mizu_sentinel")`). Collecting a task whose
#' handle was cancelled (or whose pool was stopped) raises
#' `mizu_error_cancelled`. Collecting a task whose executing worker died
#' raises `mizu_error_worker_died`, carrying the slot and pid of the worker
#' (see [mizu_error]). Worker death is detected at OS notification latency:
#' a kernel-released lock is the verdict, with no heartbeats and no
#' polling. The death fails exactly the tasks that the dead worker
#' claimed, and the surviving workers consume the work still queued on its
#' deque.
#'
#' @section Outcomes:
#' A timeout on collect is a normal outcome and is returned. Every
#' exceptional outcome is raised as a classed condition (see [mizu_error]):
#'
#' | outcome | surfaced as | class |
#' |---|---|---|
#' | result published | the value, returned | — |
#' | no result within `timeout` | sentinel, returned | `c("mizu_timeout", "mizu_sentinel")` |
#' | ring full past `.timeout` | raised by `mizu_submit()` | `mizu_error_submit_timeout` |
#' | result slots exhausted | raised by `mizu_submit()` | `mizu_error_slots_exhausted` |
#' | pool stopped, or owner died | raised by `mizu_submit()` | `mizu_error_stopped` |
#' | task raised an error | re-signalled on collect | the condition classes of the task itself |
#' | cancelled, or pool stopped | raised on collect | `mizu_error_cancelled` |
#' | executing worker died | raised on collect | `mizu_error_worker_died` |
#'
#' A re-signalled task error is a transport condition: it carries the
#' original classes, `message`, `call`, and every named field the
#' payload codec can carry within the result slot's inline budget, with
#' anything untransportable dropped and named in a `dropped_fields`
#' field. See the Task error transport section of [mizu_error].
#'
#' Inside a task, [mizu_current_pool()] returns the handle of the
#' evaluating worker, so a task can submit nested subtasks.
#' `mizu_submit(mizu_current_pool(), ...)` inside a task pushes onto the
#' work-stealing deque of the worker itself: no ring, no wait. A full
#' deque runs the subtask inline instead. A worker blocked in
#' `mizu_collect()` on a nested handle helps instead of sleeping. It
#' executes work from its own deque (and steals from peers) until the
#' awaited result is published. So nested fan-outs run at fork/join cost
#' and never deadlock the pool. A nested submission claims a submitter
#' slot for the worker on first use.
#'
#' A handle can be collected exactly once: the result slot is released to
#' the pool as the value is returned. If an uncollected handle goes to the
#' garbage collector, a still-queued task is cancelled and a published
#' result is discarded. To wait on several handles at once,
#' [mizu_collect_any()] reports the first terminal task and
#' [mizu_collect_all()] returns every result in input order.
#'
#' @param .pool a pool handle from [mizu_pool()] or [mizu_pool_attach()];
#'   inside a task, the evaluating worker's own handle from
#'   [mizu_current_pool()].
#' @param .expr an expression, captured unevaluated. This differs from
#'   [mizu_channel()], which requires its expression pre-quoted. The
#'   expression sees only the arguments in `...` and the global environment
#'   of the worker. The expression itself must load any packages it needs.
#' @param ... named values bound in the evaluation environment. The values
#'   are serialized. `mori::share()`d objects reduce to identifiers and map
#'   zero-copy on the worker. The formals ahead of `...` are dot-prefixed,
#'   so a name you pass through `...` can never collide with them.
#' @param .timeout seconds to wait for injection-ring space before the
#'   call raises `mizu_error_submit_timeout`. Submission blocks only
#'   when the ring is full (back-pressure) and returns immediately
#'   otherwise. `Inf` (the default) waits indefinitely; `0` does not wait.
#' @param task a task handle from `mizu_submit()`.
#' @param timeout seconds to wait for the result before the call returns
#'   the `mizu_timeout` sentinel. `Inf` (the default) waits indefinitely;
#'   `0` does not wait.
#'
#' @return `mizu_submit()` returns a task handle (class `"mizu_task"`).
#'   `mizu_collect()` returns the value of the task, or the `mizu_timeout`
#'   sentinel.
#'
#' @examples
#' p <- mizu_pool()
#' t <- mizu_submit(p, sum(x), x = runif(10))
#' mizu_collect(t, timeout = 30)
#' mizu_pool_stop(p)
#'
#' @export
mizu_submit <- function(.pool, .expr, ..., .timeout = Inf) {
  .Call(
    mizu_pool_submit_expr,
    .pool,
    substitute(.expr),
    list(...),
    .timeout,
    0L
  )
}

#' @rdname mizu_submit
#' @export
mizu_collect <- function(task, timeout = Inf) {
  .Call(mizu_pool_collect, task, timeout)
}

#' The Evaluating Worker's Own Pool Handle
#'
#' Inside a pool task, `mizu_current_pool()` returns the pool handle of
#' the worker evaluating the task — the handle to pass to [mizu_submit()]
#' for nested submission (subtasks push onto the worker's own
#' work-stealing deque, and a worker blocked collecting them helps instead
#' of sleeping). Outside a task, it returns `NULL`.
#'
#' The handle is runtime-owned: the pool sets it around each task
#' evaluation, so unlike a variable binding it cannot be shadowed by a
#' task argument or a local assignment.
#'
#' @return A pool handle (class `"mizu_pool"`) inside a pool task;
#'   otherwise `NULL`.
#'
#' @examples
#' p <- mizu_pool()
#' t <- mizu_submit(p, {
#'   s <- mizu_submit(mizu_current_pool(), x * 2L, x = x)
#'   mizu_collect(s, timeout = 30)
#' }, x = 21L)
#' mizu_collect(t, timeout = 30)
#' mizu_pool_stop(p)
#'
#' @export
mizu_current_pool <- function() {
  .Call(mizu_current_pool_call)
}

#' Submit a Batch of Tasks
#'
#' `mizu_submit_batch()` submits one task per element of `.exprs` in a
#' single `.Call`: one R boundary crossing and one wake-up sweep per
#' batch instead of per task. At target rates the call boundary is a
#' first-order cost, so a burst submitted this way reaches the workers
#' sooner than the same burst looped through [mizu_submit()]. Pair with
#' [mizu_collect_all()] to batch the collection side too.
#'
#' Each task's wire payload is the same `list(.expr, args)` as
#' [mizu_submit()]'s, with the `...` arguments shared by every task in
#' the batch. Unlike `mizu_submit()`, expressions are not captured:
#' the elements of `.exprs` are pre-quoted (or plain values, which
#' evaluate to themselves).
#'
#' Submission semantics per task are [mizu_submit()]'s, with one
#' difference: if the injection ring fills past `.timeout` mid-batch,
#' the call returns the handles accepted so far instead of raising
#' `mizu_error_submit_timeout`. Fatal outcomes (pool stopped, result
#' slots exhausted) still raise; tasks already submitted stay valid and
#' collectible.
#'
#' @inheritParams mizu_submit
#' @param .exprs a list of expressions, one per task. Quote them
#'   yourself: elements of a list cannot be captured unevaluated.
#'
#' @return A list of task handles (class `"mizu_task"`), one per
#'   accepted task — shorter than `.exprs` when the ring filled past
#'   `.timeout` mid-batch.
#'
#' @examples
#' p <- mizu_pool()
#' ts <- mizu_submit_batch(p, list(quote(1 + 1), quote(2 + 2)))
#' mizu_collect_all(ts, timeout = 30)
#' mizu_pool_stop(p)
#'
#' @export
mizu_submit_batch <- function(.pool, .exprs, ..., .timeout = Inf) {
  .Call(mizu_pool_submit_batch, .pool, .exprs, list(...), .timeout, 0L)
}

#' Collect the First Available Result From Several Tasks
#'
#' `mizu_collect_any()` waits on several task handles at once and returns
#' as soon as any of them reaches a terminal state — result published,
#' error raised, cancelled, or its worker died. Among handles already
#' terminal, the earliest in `tasks` is reported. The wait parks on the
#' submitter's single parker: any publishing worker wakes it directly,
#' with no polling. For the whole set at once, [mizu_collect_all()]
#' waits until every task is terminal and returns all results in input
#' order.
#'
#' @section Outcomes:
#' As for [mizu_collect()], but attributed to a handle by position:
#'
#' | outcome | surfaced as | class |
#' |---|---|---|
#' | result published | `list(index, value)`, returned | — |
#' | nothing terminal within `timeout` | sentinel, returned | `c("mizu_timeout", "mizu_sentinel")` |
#' | task raised an error | re-signalled with an `index` field | the condition classes of the task itself |
#' | cancelled, or pool stopped | raised with an `index` field | `mizu_error_cancelled` |
#' | executing worker died | raised with an `index` field | `mizu_error_worker_died` |
#'
#' The `index` field of a raised condition is the 1-based position of the
#' task in `tasks` (conditions are lists, so the field travels in place).
#' The reported handle is consumed; the remaining handles stay valid and
#' collectible.
#'
#' @param tasks a non-empty list of task handles from [mizu_submit()] on
#'   the same pool handle.
#' @param timeout seconds to wait for a task to reach a terminal state
#'   before the call returns the `mizu_timeout` sentinel. `Inf` (the
#'   default) waits indefinitely; `0` does not wait.
#'
#' @return For a published result, `list(index = i, value = v)`: the
#'   1-based position of the task in `tasks` and its value. Otherwise the
#'   `mizu_timeout` sentinel.
#'
#' @examplesIf interactive()
#' p <- mizu_pool(2L)
#' slow <- mizu_submit(p, { Sys.sleep(0.5); "slow" })
#' fast <- mizu_submit(p, "fast")
#' # completion order, not submission order
#' mizu_collect_any(list(slow, fast), timeout = 30)
#' mizu_collect(slow, timeout = 30)
#' mizu_pool_stop(p)
#'
#' @export
mizu_collect_any <- function(tasks, timeout = Inf) {
  .Call(mizu_pool_collect_any, tasks, timeout)
}

#' Collect the Results of Several Tasks, in Order
#'
#' `mizu_collect_all()` waits until every task in `tasks` reaches a
#' terminal state and returns all results in input order — the batch
#' counterpart of [mizu_collect()] for fire-then-collect patterns, with
#' one R call boundary for the whole set instead of one per task. The
#' wait parks on the submitter's single parker: any publishing worker
#' wakes it directly, with no polling.
#'
#' For homogeneous element-wise work, [mizu_map()] remains the right
#' answer (it batches submission and staging, not just collection).
#' `mizu_collect_all()` is for heterogeneous handle sets — different
#' expressions and arguments — which is what the per-task API is for.
#'
#' @section Outcomes:
#' As for [mizu_collect()], attributed to a handle by position as in
#' [mizu_collect_any()]:
#'
#' | outcome | surfaced as | class |
#' |---|---|---|
#' | all results published | list of values, returned | — |
#' | not all terminal within `timeout` | sentinel, returned | `c("mizu_timeout", "mizu_sentinel")` |
#' | a task raised an error | re-signalled with an `index` field | the condition classes of the task itself |
#' | a task cancelled, or pool stopped | raised with an `index` field | `mizu_error_cancelled` |
#' | an executing worker died | raised with an `index` field | `mizu_error_worker_died` |
#'
#' The `index` field of a raised condition is the 1-based position in
#' `tasks` of the first such task. Only the reported handle is consumed;
#' every other handle — the results ahead of it included — stays valid
#' and collectible. A timeout consumes nothing: every handle stays valid
#' and collectible.
#'
#' @param tasks a non-empty list of task handles from [mizu_submit()] on
#'   the same pool handle.
#' @param timeout seconds to wait for every task to reach a terminal state
#'   before the call returns the `mizu_timeout` sentinel. `Inf` (the
#'   default) waits indefinitely; `0` does not wait.
#'
#' @return A plain list of the task values in the order of `tasks`; the
#'   names of `tasks` carry over. On timeout, the `mizu_timeout`
#'   sentinel.
#'
#' @examplesIf interactive()
#' p <- mizu_pool(2L)
#' ts <- list(
#'   total = mizu_submit(p, sum(x), x = runif(10)),
#'   label = mizu_submit(p, "done")
#' )
#' mizu_collect_all(ts, timeout = 30)
#' mizu_pool_stop(p)
#'
#' @export
mizu_collect_all <- function(tasks, timeout = Inf) {
  .Call(mizu_pool_collect_all, tasks, timeout)
}

#' Cancel a Task
#'
#' Advisory and discard-only, never preemptive. The worker skips a task
#' that is still queued. A task already executing runs to completion, and
#' its result is dropped. Collecting a cancelled handle raises
#' `mizu_error_cancelled` (see [mizu_error]).
#'
#' @param task a task handle from `mizu_submit()`.
#'
#' @return Invisibly, `TRUE` if this call cancelled the task. `FALSE` if
#'   the call was too late: the task completed, was already cancelled, or
#'   its pool is gone.
#'
#' @examples
#' p <- mizu_pool()
#' t <- mizu_submit(p, runif(1))
#' mizu_cancel(t)
#' mizu_pool_stop(p)
#'
#' @export
mizu_cancel <- function(task) invisible(.Call(mizu_pool_cancel, task))

#' Stop a Pool
#'
#' Broadcasts shutdown, wakes every parked participant, and cancels all
#' pending tasks (blocked collectors raise `mizu_error_cancelled`). Then
#' waits up to `timeout` seconds for the workers to exit cleanly, and
#' unlinks the region and the liveness files. The handle is dead
#' afterwards, and stopping it again is a no-op. Only the creating process
#' can stop a pool.
#'
#' @param pool a pool handle from [mizu_pool()] or [mizu_pool_attach()];
#'   inside a task, the evaluating worker's own handle from
#'   [mizu_current_pool()].
#' @param timeout seconds to wait for the clean exit of the workers.
#'
#' @return Invisibly, `TRUE` if all workers exited within the timeout.
#'   `FALSE` otherwise, with a warning (the workers still exit on their
#'   own).
#'
#' @examples
#' p <- mizu_pool()
#' t <- mizu_submit(p, 1 + 1)
#' mizu_collect(t)
#' mizu_pool_stop(p)
#'
#' @export
mizu_pool_stop <- function(pool, timeout = 5) {
  ok <- .Call(mizu_pool_stop_call, pool, timeout)
  if (!ok) {
    warning(
      "mizu: pool stop timed out waiting for workers; they exit on ",
      "their own once they observe shutdown",
      call. = FALSE
    )
  }
  invisible(ok)
}

# The core registry enums, decoded to labels for the inspection verbs. One
# source of truth: status, dump and stats index these with the raw region
# codes + 1, so a core-side enum change touches exactly these lines.
worker_states <- c("free", "claiming", "live", "leaving", "reaping")
submitter_states <- c("free", "live", "reaping")
slot_states <- c("free", "pending", "ok", "err", "cancel", "died")
park_states <- c("running", "idle", "parked", "waking")

# Recode a C registry enum vector onto its labels (C enums are 0-based).
recode_states <- function(x, states) {
  states[x + 1L]
}

#' Inspect a Pool
#'
#' A read-only snapshot of the pool region: registry states, parked-worker
#' count, queued injection entries, and result-slot occupancy.
#'
#' @inheritParams mizu_pool_stop
#'
#' @return A list with elements `name`, `role`, `max_workers`,
#'   `max_submitters`, `injection_cap`, `result_slots`, `slot_size`,
#'   `workers` (per-slot states), `parked`, `submitters` (per-slot states),
#'   `injection` (entries queued and unclaimed), `tasks` (result slots by
#'   state: pending / ok / err / cancel / died), `deque` (per-worker deque
#'   depths), and `shutdown`.
#'
#' @examples
#' p <- mizu_pool()
#' mizu_pool_status(p)
#' mizu_pool_stop(p)
#'
#' @export
mizu_pool_status <- function(pool) {
  st <- .Call(mizu_pool_status_call, pool)
  st[["workers"]] <- recode_states(st[["workers"]], worker_states)
  st[["submitters"]] <- recode_states(st[["submitters"]], submitter_states)
  names(st[["tasks"]]) <- slot_states[-1L]
  st
}

#' Dump the Distributed State of a Pool
#'
#' A read-only debugging snapshot of the entire pool region, one level
#' deeper than [mizu_pool_status()]. It shows per-slot registry detail, the
#' park, ready, and back-pressure masks unpacked per slot, and every
#' occupied result slot. State is distributed across processes and
#' execution is non-deterministic, so reach for this tool first when a
#' pool hangs. The scan takes no locks and can race in-flight transitions.
#' Each field is a consistent single read. The rows need not be mutually
#' consistent.
#'
#' @inheritParams mizu_pool_stop
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
#'   nested [mizu_collect()] (the `rehome` event of [mizu_pool_trace()]).
#'   `local` is the process-private machinery of this handle: the
#'   occupancy of the producer free list (`fl_entries`, `fl_bytes`), its
#'   reuse count (`fl_hits`), the `open_hits` and `open_misses` of the
#'   consumer mapping cache, and `collect_parks` (how often a collect on
#'   this handle parked waiting for a result).
#'
#' @examples
#' p <- mizu_pool()
#' t <- mizu_submit(p, runif(1))
#' mizu_pool_dump(p)
#' mizu_collect(t)
#' mizu_pool_stop(p)
#'
#' @export
mizu_pool_dump <- function(pool) {
  d <- .Call(mizu_pool_dump_call, pool)
  lang <- d[["language"]]
  d[["language"]] <- if (is.null(lang)) {
    "none"
  } else if (lang >= 1L && lang <= 3L) {
    c("bytes", "R", "Python")[lang]
  } else {
    "unknown"
  }
  w <- d[["workers"]]
  w[["status"]] <- recode_states(w[["status"]], worker_states)
  w[["park_state"]] <- recode_states(w[["park_state"]], park_states)
  d[["workers"]] <- data.frame(slot = seq_along(w[["status"]]) - 1L, w)
  s <- d[["submitters"]]
  s[["status"]] <- recode_states(s[["status"]], submitter_states)
  d[["submitters"]] <- data.frame(slot = seq_along(s[["status"]]) - 1L, s)
  tk <- lapply(d[["tasks"]], `[`, !is.na(d[["tasks"]][["slot"]]))
  tk[["status"]] <- recode_states(tk[["status"]], slot_states)
  d[["tasks"]] <- data.frame(tk)
  d
}

#' Cumulative Pool Counters
#'
#' Per-worker and per-submitter counters, accumulated since each
#' participant joined. They complement the point-in-time snapshots of
#' [mizu_pool_status()] and [mizu_pool_dump()]. Nothing here costs the hot
#' paths anything. The submitter counts are the monotonic positions of the
#' injection rings themselves: submission writes nothing extra, and the
#' spill counter moves only on the spill path, which a fresh region per
#' payload already dominates. The worker counters are kept process-locally
#' and mirrored into the region only when a worker parks, leaves, or
#' passes its fairness tick. So under continuous load, the row of a worker
#' can lag by up to 61 claims. The row is exact whenever that worker is
#' parked or retired, or the pool is quiescent.
#'
#' @inheritParams mizu_pool_stop
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
#' p <- mizu_pool()
#' t <- mizu_submit(p, runif(5))
#' mizu_collect(t)
#' mizu_pool_stats(p)
#' mizu_pool_stop(p)
#'
#' @export
mizu_pool_stats <- function(pool) {
  st <- .Call(mizu_pool_stats_call, pool)
  w <- st[["workers"]]
  w[["status"]] <- recode_states(w[["status"]], worker_states)
  st[["workers"]] <- data.frame(slot = seq_along(w[["status"]]) - 1L, w)
  s <- st[["submitters"]]
  s[["status"]] <- recode_states(s[["status"]], submitter_states)
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
#' workers. To trace a worker, install the hook from a task on the
#' worker's own handle:
#' `mizu_submit(p, mizu_pool_trace(mizu_current_pool(), fn))`. The
#' disabled hook costs one pointer check per event site, and no event
#' sites exist on the channel
#' hot path. An error raised by the hook propagates as an infrastructure
#' failure at its site. On a worker, it takes the worker down. This
#' differs from the own error of a task, which is published as the ERR
#' result of that task.
#'
#' @inheritParams mizu_pool_stop
#' @param fn a `function(event, id)`, or `NULL` to remove a registered
#'   hook.
#'
#' @return Invisibly, `NULL`.
#'
#' @examples
#' p <- mizu_pool()
#' mizu_pool_trace(p, function(event, id) cat(event, id, "\n"))
#' t <- mizu_submit(p, 1 + 1)
#' mizu_collect(t)
#' mizu_pool_trace(p)
#' mizu_pool_stop(p)
#'
#' @export
mizu_pool_trace <- function(pool, fn = NULL) {
  invisible(.Call(mizu_pool_set_trace, pool, fn))
}

# Worker entry point: invoked through the Rscript child runner by the launcher.
# Rebuilds the region name from the compiled-in prefix plus the token (the
# name's suffix), attaches writable, validates the header,
# claims its host-assigned slot (liveness lock before status CAS), points
# its death listener at the owner, and unparks the creator on reaching
# LIVE. The loop then lives in mizu_pool_run: claims in tier order
# (fairness tick, own deque, steal, injection), parked indefinitely when
# idle, returning only on shutdown, owner death, or retire — the per-task
# R round-trip is replaced by an interrupt check and a deadline recompute
# in C. mizu_pool_step remains for the test harness's single-stepping. The
# eval hot path arms no error handler: a task error longjmps out of the
# run, and mizu_pool_run_outcome dispatches on what the run produced —
# an exit code passes through to end the loop, a caught condition is
# published as that task's ERR result, and 1 marks an error from outside
# any task eval, which is infrastructure failure and takes the worker
# down.
worker_main <- function(token, slot) {
  child_prologue()
  h <- .Call(mizu_pool_worker_join, token, slot, NULL)
  .Call(mizu_pool_set_eval, h)
  status <- 0L
  rc <- -1L
  repeat {
    e <- tryCatch(.Call(mizu_pool_run, h, 3600), error = function(e) e)
    rc <- .Call(mizu_pool_run_outcome, h, e)
    if (rc < 0L) {
      break
    }
    if (rc > 0L) {
      cat(
        "mizu worker error: ",
        conditionMessage(e),
        "\n",
        sep = "",
        file = stderr()
      )
      status <- 1L
      break
    }
  }
  .Call(mizu_pool_leave, h)
  # a retired worker (-2) lingers as a lifetime anchor for its uncollected
  # results: plain bounded sleeps, since no unpark can reach a released
  # slot; shutdown or owner death ends the linger
  if (rc == -2L) {
    while (!.Call(mizu_pool_lame_duck, h)) {
      Sys.sleep(1)
    }
  }
  quit(save = "no", status = status)
}
