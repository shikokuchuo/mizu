#' Print Methods for kioto Objects
#'
#' One-line summaries. A channel prints its region name, side, and
#' conversation state: `open`, `closed` once either side has signalled
#' close, or `peer gone` — [kio_alive()]'s verdict, probed at print. A
#' pool prints its region name, this handle's role, live workers out of
#' registry capacity, and pending (uncompleted) tasks. A task handle
#' prints its state, probed without consuming the result: `pending`,
#' `ok`, `err`, `cancel`, or `died` in the result-slot vocabulary of
#' [kio_pool_status()], `collected` once the result has been taken, or
#' `dropped` when its pool is gone. Sentinels print as their class. The
#' handle methods never error and never touch the rings — a handle whose
#' resources have been released (a closed channel, a stopped pool)
#' prints as closed — so auto-printing is always safe.
#'
#' @param x the object.
#' @param ... ignored.
#'
#' @return `x`, invisibly.
#'
#' @export
print.kio_channel <- function(x, ...) {
  st <- tryCatch(.Call(kio_channel_stat, x), error = function(e) NULL)
  cat(if (is.null(st)) "<kio_channel: closed>\n"
      else sprintf("<kio_channel %s: %s, %s>\n", st$name, st$side,
                   if (st$closed) "closed"
                   else if (kio_alive(x)) "open" else "peer gone"))
  invisible(x)
}

#' @rdname print.kio_channel
#' @export
print.kio_pool <- function(x, ...) {
  st <- tryCatch(kio_pool_status(x), error = function(e) NULL)
  cat(if (is.null(st)) "<kio_pool: closed>\n"
      else sprintf("<kio_pool %s: %s, %d/%d workers live, %d pending%s>\n",
                   st$name, st$role, sum(st$workers == "live"),
                   st$max_workers, st$tasks[["pending"]],
                   if (st$shutdown) ", shutdown" else ""))
  invisible(x)
}

#' @rdname print.kio_channel
#' @export
print.kio_task <- function(x, ...) {
  cat(sprintf("<kio_task: %s>\n", .Call(kio_pool_task_state, x)))
  invisible(x)
}

#' @rdname print.kio_channel
#' @export
print.kio_map_prepared <- function(x, ...) {
  cat(sprintf("<kio_map_prepared: %.0f elements, %s>\n", length(x$x),
              if (is.null(x$st)) "stale (next run restages)"
              else if (is.null(x$st$blob)) x$st$name else "inline blob"))
  invisible(x)
}

#' @rdname print.kio_channel
#' @export
print.kio_sentinel <- function(x, ...) {
  cat(sprintf("<%s>\n", class(x)[1L]))
  invisible(x)
}
