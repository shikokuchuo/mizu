#' Error Conditions
#'
#' Terminal failures that a caller can act on programmatically are raised
#' as classed conditions. Each inherits `"mizu_error"` (alongside `"error"`
#' and `"condition"`), with a subclass that names the failure. Handlers
#' dispatch with `tryCatch(..., mizu_error_worker_died = ...)` or test with
#' [inherits()] instead of matching message text. Messages are not API and
#' can be reworded. The class vectors and fields below are API.
#'
#' The subclasses, where they are raised, and the structured fields they
#' carry as condition elements:
#'
#' * `mizu_error_submit_timeout` — [mizu_submit()] on `.timeout` expiry with
#'   the injection ring of the submitter still full.
#' * `mizu_error_slots_exhausted` — [mizu_submit()] and [mizu_map()] when
#'   every result slot in the subrange of the submitter is already
#'   outstanding. Collect or cancel before resubmitting.
#' * `mizu_error_stopped` — [mizu_submit()], [mizu_map()] and
#'   [mizu_pool_attach()] against a pool that was stopped or whose owner
#'   process died.
#' * `mizu_error_cancelled` — [mizu_collect()] on a task that was cancelled
#'   or whose pool was stopped.
#' * `mizu_error_worker_died` — [mizu_collect()] on a task whose executing
#'   worker died. Fields `slot` (worker registry slot, 0-based as in
#'   [mizu_pool_dump()]) and `pid`: the claimant record of the result slot,
#'   read at collect time. This is informational, racy against slot reuse
#'   exactly as [mizu_pool_dump()] is, and `NA` where no claim was
#'   recorded. [mizu_map()] signals this class again with the lost elements
#'   as an additional `elements` field: a two-column matrix of inclusive
#'   `lo, hi` ranges, runner-granular and conservative (see the Errors
#'   section of [mizu_map()]).
#' * `mizu_error_startup` — [mizu_channel()], [mizu_pool()] and
#'   [mizu_spawn_workers()] when a child process fails to attach within
#'   `startup_timeout`.
#' * `mizu_error_shm` — shared-memory region create or open failure
#'   anywhere on the surface. Field `bytes`: the requested size of a
#'   region that was not created, `NA` when a region was not opened.
#' * `mizu_error_python_payload` — [mizu_recv()] or [mizu_recv_batch()]
#'   on a channel message written in a language-private stream (a pymizu
#'   codec or pickle payload) that this reader cannot interpret. The
#'   portable interchange subset crosses; the declined message is
#'   consumed, so the channel keeps flowing.
#' * `mizu_error_not_portable` — [mizu_send()] on a channel whose peer is
#'   a foreign-language process, of a value outside the portable
#'   interchange subset (a named atomic vector, an environment, a
#'   closure, an S4 object, an ordered factor, a data frame subclass,
#'   and the like). Fields `path` (where in the value the walk
#'   declined), `reason`, and `remedy` (a one-line rewrite where one
#'   exists, else `character(0)`).
#'
#' Errors of misuse (unnamed task arguments, out-of-range slots, operations
#' on a closed handle) stay plain errors: the classed hierarchy covers the
#' outcomes that a running system produces, not programming mistakes.
#'
#' Raised `mizu_error` conditions are distinct from sentinels (class
#' `mizu_sentinel`, returned by [mizu_send()], [mizu_recv()] and
#' [mizu_collect()]). A sentinel is an ordinary return value that tags a
#' terminal state on the hot path, not a signalled condition. See
#' [mizu_is_sentinel()].
#'
#' Which discipline applies follows the shape of the call. The verbs that
#' move payloads and wait with a bound — [mizu_send()], [mizu_recv()],
#' [mizu_collect()], [mizu_map()] — return sentinels for transport states.
#' These are: not yet (`mizu_timeout`), not now (`mizu_full`), stream over
#' (`mizu_closed`, `mizu_peer_gone`). Their caller is a loop, and these are
#' its normal outcomes. Conditions are raised where a request failed for
#' good. Constructors and [mizu_submit()], whose return is a handle the
#' next line uses, raise on every failure. [mizu_collect()] raises when the
#' value can never arrive: the own error of the task re-signalled,
#' `mizu_error_cancelled`, `mizu_error_worker_died`. A sentinel invites the
#' next iteration of the loop. A condition means stop and deal with it.
#'
#' @section Task error transport:
#' A task's own error, re-signalled by [mizu_collect()] (and
#' [mizu_collect_any()] / [mizu_collect_all()]), is a transport condition
#' built on the worker at publish time. The caught condition itself never
#' crosses, so a condition that cannot be serialized can never kill the
#' worker. The transport condition carries the original classes, the raw
#' `message` field (custom `conditionMessage()` methods are bypassed;
#' the message is truncated past its share of the result slot's inline
#' budget), `call`, and every named field the compact payload codec can
#' carry within that budget, in the priority order message, `call`,
#' fields, then `dropped_fields`. Fields the codec cannot carry
#' (environments, closures, S4 objects, ALTREP vectors, external
#' pointers) and fields past the remaining budget are dropped and named
#' in a `dropped_fields` field, absent when nothing was dropped. Only
#' the named elements of a condition are considered: an unnamed element
#' cannot be named in `dropped_fields` and is dropped silently.
#'
#' @name mizu_error
#' @aliases mizu_error_submit_timeout mizu_error_slots_exhausted mizu_error_stopped mizu_error_cancelled mizu_error_worker_died mizu_error_startup mizu_error_shm mizu_error_python_payload
NULL

# Raise a classed mizu error — class c(subclass, "mizu_error", "error",
# "condition"), structured fields in `...` — matching the C-side mizu_stop
# (condition.c).
stop_mizu <- function(subclass, message, ...) {
  stop(errorCondition(message, ..., class = c(subclass, "mizu_error")))
}

#' Test for a mizu Sentinel
#'
#' Identity comparison against the four interned sentinel singletons —
#' `mizu_full`, `mizu_timeout`, `mizu_closed`, `mizu_peer_gone` — that the
#' verbs of mizu return to tag terminal states. `inherits(x,
#' "mizu_sentinel")` tests the class alone, which any payload can carry.
#' This includes a genuine sentinel forwarded over a channel, which arrives
#' as an ordinary copy. `mizu_is_sentinel()` is provenance: `TRUE` only for
#' the exact objects that the own calls of mizu return in this process.
#' So code that relays untrusted values can distinguish its terminal states
#' from look-alike payloads.
#'
#' Sentinels are ordinary values, not R conditions: nothing is signalled,
#' and condition handlers never see them.
#'
#' @param x any R object.
#'
#' @return `TRUE` or `FALSE`.
#'
#' @examples
#' mizu_is_sentinel(42)
#' # class alone does not make a sentinel:
#' mizu_is_sentinel(structure("x", class = c("mizu_timeout", "mizu_sentinel")))
#'
#' @export
mizu_is_sentinel <- function(x) .Call(mizu_sentinel_check, x)
