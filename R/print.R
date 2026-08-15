#' Print Methods for sora Objects
#'
#' One-line summaries. A channel prints its region name, side, and
#' conversation state: `open`, `closed` once either side signalled close,
#' or `peer gone` — the verdict of [sora_alive()], probed at print. A pool
#' prints its region name, the role of this handle, live workers out of
#' registry capacity, and pending (uncompleted) tasks. A task handle
#' prints its state, probed without consuming the result. The state is
#' `pending`, `ok`, `err`, `cancel`, or `died` in the result-slot
#' vocabulary of [sora_pool_status()]. It is `collected` once the result is
#' taken, or `dropped` when its pool is gone. Sentinels print as their
#' class. The handle methods never error and never touch the rings. A
#' handle whose resources are released (a closed channel, a stopped pool)
#' prints as closed. So auto-printing is always safe.
#'
#' @param x the object.
#' @param ... ignored.
#'
#' @return `x`, invisibly.
#'
#' @export
print.sora_channel <- function(x, ...) {
  st <- tryCatch(.Call(sora_channel_stat, x), error = function(e) NULL)
  cat(
    if (is.null(st)) {
      "<sora_channel: closed>\n"
    } else {
      sprintf(
        "<sora_channel %s: %s, %s>\n",
        st[["name"]],
        st[["side"]],
        if (st[["closed"]]) {
          "closed"
        } else if (sora_alive(x)) {
          "open"
        } else {
          "peer gone"
        }
      )
    }
  )
  invisible(x)
}

#' @rdname print.sora_channel
#' @export
print.sora_pool <- function(x, ...) {
  st <- tryCatch(sora_pool_status(x), error = function(e) NULL)
  cat(
    if (is.null(st)) {
      "<sora_pool: closed>\n"
    } else {
      sprintf(
        "<sora_pool %s: %s, %d/%d workers live, %d pending%s>\n",
        st[["name"]],
        st[["role"]],
        sum(st[["workers"]] == "live"),
        st[["max_workers"]],
        st[["tasks"]][["pending"]],
        if (st[["shutdown"]]) ", shutdown" else ""
      )
    }
  )
  invisible(x)
}

#' @rdname print.sora_channel
#' @export
print.sora_task <- function(x, ...) {
  cat(sprintf("<sora_task: %s>\n", .Call(sora_pool_task_state, x)))
  invisible(x)
}

#' @rdname print.sora_channel
#' @export
print.sora_map_prepared <- function(x, ...) {
  cat(sprintf(
    "<sora_map_prepared: %.0f elements, %s>\n",
    length(x[["x"]]),
    if (is.null(x[["st"]])) {
      "stale (next run restages)"
    } else if (is.null(x[["st"]][["blob"]])) {
      x[["st"]][["name"]]
    } else {
      "inline blob"
    }
  ))
  invisible(x)
}

#' @rdname print.sora_channel
#' @export
print.sora_sentinel <- function(x, ...) {
  cat(sprintf("<%s>\n", class(x)[1L]))
  invisible(x)
}
