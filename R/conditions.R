#' Error Conditions
#'
#' Terminal failures a caller can act on programmatically are raised as
#' classed conditions: each inherits `"kio_error"` (alongside `"error"` and
#' `"condition"`) with a subclass naming the failure, so handlers dispatch
#' with `tryCatch(..., kio_error_worker_died = ...)` or test with
#' [inherits()] instead of matching message text. Messages are not API and
#' may be reworded; the class vectors and fields below are.
#'
#' The subclasses, where they are raised, and the structured fields they
#' carry as condition elements:
#'
#' * `kio_error_submit_timeout` — [kio_submit()] on `.timeout` expiry with
#'   the submitter's injection ring still full.
#' * `kio_error_slots_exhausted` — [kio_submit()] and [kio_map()] when
#'   every result slot in the submitter's subrange is already outstanding;
#'   collect or cancel before resubmitting.
#' * `kio_error_stopped` — [kio_submit()], [kio_map()] and
#'   [kio_pool_attach()] against a pool that has been stopped or whose
#'   owner process died.
#' * `kio_error_cancelled` — [kio_collect()] on a task that was cancelled
#'   or whose pool was stopped.
#' * `kio_error_worker_died` — [kio_collect()] on a task whose executing
#'   worker died. Fields `slot` (worker registry slot, 0-based as in
#'   [kio_pool_dump()]) and `pid` — the result slot's claimant record read
#'   at collect time: informational, racy against slot reuse exactly as
#'   [kio_pool_dump()] is, and `NA` where no claim was recorded.
#'   [kio_map()] re-signals this class with the failed chunk's element
#'   range as an additional `elements` field (`c(lo, hi)`).
#' * `kio_error_startup` — [kio_channel()], [kio_pool()] and
#'   [kio_spawn_workers()] when a child process fails to attach within
#'   `startup_timeout`.
#' * `kio_error_shm` — shared-memory region create / open failure anywhere
#'   on the surface. Field `bytes`: the requested size of a region that
#'   could not be created, `NA` when a region could not be opened.
#'
#' Errors of misuse (unnamed task arguments, out-of-range slots, operations
#' on a closed handle) remain plain errors: the classed hierarchy covers
#' the outcomes a running system produces, not programming mistakes.
#'
#' Raised `kio_error` conditions are distinct from sentinels (class
#' `kio_sentinel`, returned by [kio_send()], [kio_recv()] and
#' [kio_collect()]): a sentinel is an ordinary return value tagging a
#' terminal state on the hot path, not a signalled condition — see
#' [kio_is_sentinel()].
#'
#' Which discipline applies follows the shape of the call. Verbs that move
#' payloads and bounded waits — [kio_send()], [kio_recv()],
#' [kio_collect()], [kio_map()] — return sentinels for transport states:
#' not yet (`kio_timeout`), not now (`kio_full`), stream over
#' (`kio_closed`, `kio_peer_gone`). Their caller is a loop, and these are
#' its normal outcomes. Conditions are raised where a request has failed
#' for good: constructors and [kio_submit()], whose return is a handle the
#' next line uses, raise on every failure; [kio_collect()] raises when the
#' value can never arrive — the task's own error re-signalled,
#' `kio_error_cancelled`, `kio_error_worker_died`. A sentinel invites the
#' loop's next iteration; a condition means stop and deal with it.
#'
#' @name kio_error
#' @aliases kio_error_submit_timeout kio_error_slots_exhausted kio_error_stopped kio_error_cancelled kio_error_worker_died kio_error_startup kio_error_shm
NULL

# Raise a classed kioto error — class c(subclass, "kio_error", "error",
# "condition"), structured fields in `...` — matching the C-side kio_stop
# (condition.c).
stop_kio <- function(subclass, message, ...)
  stop(errorCondition(message, ..., class = c(subclass, "kio_error")))

#' Test for a kioto Sentinel
#'
#' Identity comparison against the four interned sentinel singletons —
#' `kio_full`, `kio_timeout`, `kio_closed`, `kio_peer_gone` — that kioto's
#' verbs return to tag terminal states. `inherits(x, "kio_sentinel")` tests
#' the class alone, which any payload can carry — including a genuine
#' sentinel forwarded over a channel, which arrives as an ordinary copy.
#' `kio_is_sentinel()` is provenance: `TRUE` only for the exact objects
#' kioto's own calls return in this process, so code relaying untrusted
#' values can distinguish its terminal states from look-alike payloads.
#'
#' Sentinels are ordinary values, not R conditions: nothing is signalled,
#' and condition handlers never see them.
#'
#' @param x any R object.
#'
#' @return `TRUE` or `FALSE`.
#'
#' @examples
#' kio_is_sentinel(42)
#' # class alone does not make a sentinel:
#' kio_is_sentinel(structure("x", class = c("kio_timeout", "kio_sentinel")))
#'
#' @export
kio_is_sentinel <- function(x) .Call(kio_sentinel_check, x)
