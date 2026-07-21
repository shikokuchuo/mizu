#' Remove Orphaned Shared Memory Regions
#'
#' Remove mov shared memory regions left behind by processes that no longer
#' exist (e.g. after a crash). Regions belonging to running processes are
#' never touched, and mori regions are invisible to mov (and vice versa) —
#' the two packages keep disjoint namespaces.
#'
#' A crashed process cannot clean up after itself, and a new process that
#' happens to reuse its PID cannot reap its orphans either (it reads its own
#' PID as alive) — run `mov_prune()` while the PID is free, before reuse.
#'
#' @return Invisibly, a character vector of the region names removed, or
#'   `NULL` if none were. On platforms whose shared memory namespace cannot
#'   be enumerated (Windows, where orphans cannot exist), always `NULL`.
#'
#' @examples
#' mov_prune()
#'
#' @export
mov_prune <- function() invisible(.Call(mov_prune_call))
