#' Error Conditions
#'
#' Terminal failures that a caller can act on programmatically are raised
#' as classed conditions. Each inherits `"sora_error"` (alongside `"error"`
#' and `"condition"`), with a subclass that names the failure. Handlers
#' dispatch with `tryCatch(..., sora_error_worker_died = ...)` or test with
#' [inherits()] instead of matching message text. Messages are not API and
#' can be reworded. The class vectors and fields below are API.
#'
#' The subclasses, where they are raised, and the structured fields they
#' carry as condition elements:
#'
#' * `sora_error_submit_timeout` — [sora_submit()] on `.timeout` expiry with
#'   the injection ring of the submitter still full.
#' * `sora_error_slots_exhausted` — [sora_submit()] and [sora_map()] when
#'   every result slot in the subrange of the submitter is already
#'   outstanding. Collect or cancel before resubmitting.
#' * `sora_error_stopped` — [sora_submit()], [sora_map()] and
#'   [sora_pool_attach()] against a pool that was stopped or whose owner
#'   process died.
#' * `sora_error_cancelled` — [sora_collect()] on a task that was cancelled
#'   or whose pool was stopped.
#' * `sora_error_worker_died` — [sora_collect()] on a task whose executing
#'   worker died. Fields `slot` (worker registry slot, 0-based as in
#'   [sora_pool_dump()]) and `pid`: the claimant record of the result slot,
#'   read at collect time. This is informational, racy against slot reuse
#'   exactly as [sora_pool_dump()] is, and `NA` where no claim was
#'   recorded. [sora_map()] signals this class again with the lost elements
#'   as an additional `elements` field: a two-column matrix of inclusive
#'   `lo, hi` ranges, runner-granular and conservative (see the Errors
#'   section of [sora_map()]).
#' * `sora_error_startup` — [sora_channel()], [sora_pool()] and
#'   [sora_spawn_workers()] when a child process fails to attach within
#'   `startup_timeout`.
#' * `sora_error_shm` — shared-memory region create or open failure
#'   anywhere on the surface. Field `bytes`: the requested size of a
#'   region that was not created, `NA` when a region was not opened.
#'
#' Errors of misuse (unnamed task arguments, out-of-range slots, operations
#' on a closed handle) stay plain errors: the classed hierarchy covers the
#' outcomes that a running system produces, not programming mistakes.
#'
#' Raised `sora_error` conditions are distinct from sentinels (class
#' `sora_sentinel`, returned by [sora_send()], [sora_recv()] and
#' [sora_collect()]). A sentinel is an ordinary return value that tags a
#' terminal state on the hot path, not a signalled condition. See
#' [sora_is_sentinel()].
#'
#' Which discipline applies follows the shape of the call. The verbs that
#' move payloads and wait with a bound — [sora_send()], [sora_recv()],
#' [sora_collect()], [sora_map()] — return sentinels for transport states.
#' These are: not yet (`sora_timeout`), not now (`sora_full`), stream over
#' (`sora_closed`, `sora_peer_gone`). Their caller is a loop, and these are
#' its normal outcomes. Conditions are raised where a request failed for
#' good. Constructors and [sora_submit()], whose return is a handle the
#' next line uses, raise on every failure. [sora_collect()] raises when the
#' value can never arrive: the own error of the task re-signalled,
#' `sora_error_cancelled`, `sora_error_worker_died`. A sentinel invites the
#' next iteration of the loop. A condition means stop and deal with it.
#'
#' @section Task error transport:
#' A task's own error, re-signalled by [sora_collect()] (and
#' [sora_collect_any()] / [sora_collect_all()]), is a transport condition
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
#' @name sora_error
#' @aliases sora_error_submit_timeout sora_error_slots_exhausted sora_error_stopped sora_error_cancelled sora_error_worker_died sora_error_startup sora_error_shm
NULL

# Raise a classed sora error — class c(subclass, "sora_error", "error",
# "condition"), structured fields in `...` — matching the C-side sora_stop
# (condition.c).
stop_sora <- function(subclass, message, ...) {
  stop(errorCondition(message, ..., class = c(subclass, "sora_error")))
}

#' Test for a sora Sentinel
#'
#' Identity comparison against the four interned sentinel singletons —
#' `sora_full`, `sora_timeout`, `sora_closed`, `sora_peer_gone` — that the
#' verbs of sora return to tag terminal states. `inherits(x,
#' "sora_sentinel")` tests the class alone, which any payload can carry.
#' This includes a genuine sentinel forwarded over a channel, which arrives
#' as an ordinary copy. `sora_is_sentinel()` is provenance: `TRUE` only for
#' the exact objects that the own calls of sora return in this process.
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
#' sora_is_sentinel(42)
#' # class alone does not make a sentinel:
#' sora_is_sentinel(structure("x", class = c("sora_timeout", "sora_sentinel")))
#'
#' @export
sora_is_sentinel <- function(x) .Call(sora_sentinel_check, x)
