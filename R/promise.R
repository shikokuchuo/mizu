# Event-driven promises and handlers: the notify endpoints of the vendored
# core, surfaced through later and promises. A channel side or a pool
# submitter arms its endpoint; a producer signals it at the same fenced
# checkpoint where it tests the parked announce, so a pending consumer is
# woken without polling. The core's watch fires a pure-C callback on its
# listener thread, which schedules the drain on the main thread through
# later's execLaterNative2 (src/notify_r.c). Every drain is bounded per
# callback (notify_quantum_n items or ~1 ms, whichever first) and
# re-scheduled with later(secs = 0) while more remains — a saturated ring
# cannot starve the event loop. Resolution is per-burst / per-collection,
# never per-item beyond the small-count forms: the handler's batch form
# delivers one list per drain pass, and the collective verbs resolve
# exactly one promise per collection.

# The drain quantum: at most this many messages delivered / waiters
# settled, or this many seconds of resolution work, per later callback.
# The seconds bound is the real guard for promise resolution (tens of
# microseconds each); messages are cheap enough that the items bound
# only bites ahead of it, and it sizes the batch handler's burst read.
notify_quantum_n <- 2048L
notify_quantum_secs <- 0.001

# the requireNamespace probes memoize (they ran twice per async verb
# call); the env's fields mutate where a namespace binding could not
notify_pkgs <- new.env(parent = emptyenv())

notify_requires <- function() {
  if (!isTRUE(notify_pkgs[["later"]])) {
    if (!requireNamespace("later", quietly = TRUE)) {
      stop(
        "mizu: the async verbs require the 'later' package to be installed",
        call. = FALSE
      )
    }
    notify_pkgs[["later"]] <- TRUE
  }
  if (!isTRUE(notify_pkgs[["promises"]])) {
    if (!requireNamespace("promises", quietly = TRUE)) {
      stop(
        "mizu: the async verbs require the 'promises' package to be installed",
        call. = FALSE
      )
    }
    notify_pkgs[["promises"]] <- TRUE
  }
  invisible()
}

notify_requires_later <- function() {
  if (!isTRUE(notify_pkgs[["later"]])) {
    if (!requireNamespace("later", quietly = TRUE)) {
      stop(
        "mizu: mizu_recv_handler() requires the 'later' package to be installed",
        call. = FALSE
      )
    }
    notify_pkgs[["later"]] <- TRUE
  }
  invisible()
}

# Cancel a waiter's later() timer, if one is armed. later() returns its
# own cancel function; cancelling a spent registration is a no-op.
cancel_timer <- function(w) {
  timer <- w[["timer"]]
  if (!is.null(timer)) {
    w[["timer"]] <- NULL
    timer()
  }
  invisible()
}

# Channel state ---------------------------------------------------------------

# The per-channel notify state env (prot slot [3] of the handle): mode
# NULL | "promise" | "handler" (the two consumer forms are exclusive),
# the pending-promise FIFO, the handler and its batch flag, and the
# .drain closure the C bridge calls. The FIFO is an env linked list
# (fifo_head / fifo_tail, w[["next"]] links): O(1) push and pop — a list
# paid a vector copy per op either end, quadratic per burst at depth.
notify_chan_state <- function(ch) {
  st <- .Call(mizu_chan_notify_peek, ch)
  if (is.null(st)) {
    st <- new.env(parent = emptyenv())
    st[["ch"]] <- ch
    st[["mode"]] <- NULL
    st[["fifo_head"]] <- NULL
    st[["fifo_tail"]] <- NULL
    st[["batch"]] <- FALSE
    st[[".drain"]] <- function() chan_drain(st)
    .Call(mizu_chan_notify_set, ch, st)
  }
  st
}

chan_disarm <- function(state) {
  state[["mode"]] <- NULL
  state[["fifo_head"]] <- NULL
  state[["fifo_tail"]] <- NULL
  state[["fn"]] <- NULL
  .Call(mizu_chan_notify_disarm, state[["ch"]])
  invisible()
}

# Push one waiter (envs carry their resolve/reject; w[["next"]] links).
chan_fifo_push <- function(state, w) {
  w[["next"]] <- NULL
  if (is.null(state[["fifo_head"]])) {
    state[["fifo_head"]] <- w
  } else {
    state[["fifo_tail"]][["next"]] <- w
  }
  state[["fifo_tail"]] <- w
  invisible()
}

# Pop the head waiter, or NULL when empty.
chan_fifo_pop <- function(state) {
  w <- state[["fifo_head"]]
  if (is.null(w)) {
    return(NULL)
  }
  state[["fifo_head"]] <- w[["next"]]
  if (is.null(state[["fifo_head"]])) {
    state[["fifo_tail"]] <- NULL
  }
  w[["next"]] <- NULL
  w
}

# Remove a waiter from the FIFO by identity (its timer fired, or it
# settled); disarm when the FIFO runs empty.
chan_fifo_remove <- function(state, w) {
  prev <- NULL
  cur <- state[["fifo_head"]]
  while (!is.null(cur)) {
    if (identical(cur, w)) {
      nxt <- cur[["next"]]
      if (is.null(prev)) {
        state[["fifo_head"]] <- nxt
      } else {
        prev[["next"]] <- nxt
      }
      if (identical(state[["fifo_tail"]], cur)) {
        state[["fifo_tail"]] <- prev
      }
      cur[["next"]] <- NULL
      break
    }
    prev <- cur
    cur <- cur[["next"]]
  }
  if (is.null(state[["fifo_head"]]) && identical(state[["mode"]], "promise")) {
    chan_disarm(state)
  }
  invisible()
}

# The terminal delivery shared by the drain and the mizu_close flush: the
# handler gets the condition as its final call; every pending promise
# rejects with it. Then the consumer disarms.
chan_terminal <- function(state, cond) {
  if (identical(state[["mode"]], "handler")) {
    fn <- state[["fn"]]
    chan_disarm(state)
    fn(cond)
  } else {
    w <- state[["fifo_head"]]
    chan_disarm(state)
    while (!is.null(w)) {
      cancel_timer(w)
      w[["reject"]](cond)
      w <- w[["next"]]
    }
  }
  invisible()
}

# One handler call with the error discipline: an erroring handler is
# removed (the consumer disarmed), then the error propagates out of the
# drain — later surfaces it at the loop's run site.
chan_call_fn <- function(state, arg) {
  tryCatch(
    state[["fn"]](arg),
    error = function(e) {
      chan_disarm(state)
      stop(e)
    }
  )
}

# The main-thread channel drain: resolve waiters / deliver to the handler
# out of the ring, bounded per callback. Ends disarmed (nothing pending),
# armed-and-sleeping (the watch fires on the next burst), or re-scheduled
# (the quantum is spent and work remains).
chan_drain <- function(state) {
  # a timer can disarm between the watch's signal and this callback: the
  # mode is stable within a pass (callbacks never fire mid-loop), but it
  # may be gone at entry — read it (and the batch flag) once, not per item
  mode <- state[["mode"]]
  if (is.null(mode)) {
    return(invisible())
  }
  batch <- isTRUE(state[["batch"]])
  t0 <- mono_time()
  items <- 0L
  repeat {
    # the FIFO can empty mid-pass with messages still in the ring: disarm
    # ahead of consuming anything with nobody waiting (the next arm's
    # availability report picks those messages up)
    if (identical(mode, "promise") && is.null(state[["fifo_head"]])) {
      chan_disarm(state)
      return(invisible())
    }
    x <- if (identical(mode, "handler") && batch) {
      tryCatch(
        .Call(mizu_channel_recv_batch, state[["ch"]], notify_quantum_n, 0),
        mizu_error_python_payload = identity
      )
    } else {
      tryCatch(
        .Call(mizu_channel_recv, state[["ch"]], 0),
        mizu_error_python_payload = identity
      )
    }
    if (mizu_is_sentinel(x)) {
      if (inherits(x, "mizu_timeout")) {
        # ring empty: re-arm and re-check (the loop head owns the
        # nothing-pending disarm; no waiter can leave the FIFO mid-pass)
        avail <- .Call(mizu_chan_notify_arm, state[["ch"]], state)
        if (!avail) {
          return(invisible())
        }
      } else {
        cond <- if (inherits(x, "mizu_closed")) {
          cond_closed()
        } else {
          cond_peer_gone()
        }
        chan_terminal(state, cond)
        return(invisible())
      }
    } else if (inherits(x, "mizu_error_python_payload")) {
      # the foreign decline: consumed, the channel keeps flowing — reject
      # the head waiter / deliver to the handler as the condition
      items <- items + 1L
      if (identical(mode, "handler")) {
        chan_call_fn(state, x)
      } else {
        w <- chan_fifo_pop(state)
        cancel_timer(w)
        w[["reject"]](x)
      }
    } else {
      items <- items +
        if (identical(mode, "handler") && batch) {
          chan_call_fn(state, x)
          length(x)
        } else if (identical(mode, "handler")) {
          chan_call_fn(state, x)
          1L
        } else {
          w <- chan_fifo_pop(state)
          cancel_timer(w)
          w[["resolve"]](x)
          1L
        }
    }
    # the clock read strides (per item it was a .Call per message): the
    # quantum's seconds bound is advisory
    if (
      items >= notify_quantum_n ||
        (items %% 64L == 0L && mono_time() - t0 >= notify_quantum_secs)
    ) {
      later::later(state[[".drain"]], 0)
      return(invisible())
    }
  }
}

#' Receive Channel Messages as a Promise or a Stream
#'
#' `mizu_recv_async()` returns immediately with a
#' [promises::promise()] — it never blocks — resolving with the next
#' received payload: the identical R object a blocking [mizu_recv()]
#' would have returned, zero-copy ALTREP views included. Chain with
#' [promises::then()] or the promise pipe `%...>%`.
#'
#' The promise rejects on `mizu_closed`, `mizu_peer_gone`, and on
#' `timeout` expiry — as classed *error* conditions carrying the same
#' class names as the sentinels, so `inherits(x, "mizu_closed")` works on
#' either form. Close and peer death reach the drain through the core's
#' terminal wakes, so the rejection is prompt, never timeout-driven.
#'
#' Several outstanding calls on one channel resolve in FIFO call order,
#' one message each. Arming is loss-free: messages already in the ring
#' resolve from the arm's synchronous drain (delivered asynchronously at
#' top level, per promises discipline); only genuinely pending entries
#' ride the watch.
#'
#' `mizu_recv_handler()` is the stream form: it installs `fn` on the
#' channel and each burst of messages is delivered to it from the event
#' loop — no promise is allocated per message. With `batch = TRUE` each
#' drained burst is delivered as one list in a single `fn(list)` call
#' (the throughput form: one R call per burst); with `batch = FALSE`,
#' `fn(value)` is called per message. Transport terminals are delivered
#' to `fn` as the classed error conditions (`mizu_closed`,
#' `mizu_peer_gone`), after which the handler is removed. A declined
#' foreign payload (see [mizu_send()]) is delivered as its
#' `mizu_error_python_payload` condition and the handler continues. An
#' error raised by `fn` removes the handler and propagates at the event
#' loop. `mizu_recv_handler(ch, NULL)` removes the handler.
#'
#' A channel holds at most one of the two consumer forms at a time:
#' installing a handler with promises pending, or requesting a promise
#' with a handler installed, is an error. Mixing a blocking
#' [mizu_recv()] with pending promises consumes in arrival order — the
#' blocking call may take a message a promise was waiting on.
#'
#' Every main-thread drain is bounded per event-loop quantum and
#' re-scheduled, so a saturated ring starves neither Shiny nor
#' [later::run_now()].
#'
#' Requires the \pkg{later} package (and \pkg{promises} for
#' `mizu_recv_async()`), both of which must be installed.
#'
#' @param ch a channel handle from [mizu_channel()] (or the `ch` binding
#'   inside a peer expression). Arming a `spin` channel errors: its
#'   publish path skips the wake checkpoint by design.
#' @param timeout seconds after which the promise rejects with the
#'   `mizu_timeout` classed error. `Inf` (the default) waits
#'   indefinitely.
#' @param fn a `function(x)` called with each received message (or, with
#'   `batch = TRUE`, each burst as one list), and finally with a
#'   terminal condition. `NULL` removes the handler.
#' @param batch deliver each drained burst to `fn` as one list in a
#'   single call, instead of one call per message.
#' @param x for the `as.promise` method: a channel handle, treated as
#'   `mizu_recv_async(x)`.
#'
#' @return `mizu_recv_async()`: a [promises::promise()] resolving with
#'   the next message. `mizu_recv_handler()`: the channel handle,
#'   invisibly.
#'
#' @examplesIf interactive() && requireNamespace("promises", quietly = TRUE)
#' library(promises)
#' ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch))))
#' mizu_recv_async(ch) %...>% print()
#' mizu_send(ch, 42L)
#' later::run_now()
#' mizu_close(ch)
#'
#' @export
mizu_recv_async <- function(ch, timeout = Inf) {
  notify_requires()
  if (!is.numeric(timeout) || length(timeout) != 1L || is.na(timeout)) {
    stop("mizu: timeout must be a number of seconds, not NA", call. = FALSE)
  }
  state <- notify_chan_state(ch)
  if (identical(state[["mode"]], "handler")) {
    stop(
      "mizu: a handler is installed on this channel \u2014 mizu_recv_async() ",
      "and mizu_recv_handler() are mutually exclusive",
      call. = FALSE
    )
  }
  state[["mode"]] <- "promise"
  # the arm runs ahead of the promise action: promise() converts an error
  # raised inside the action into a rejection, but arming failures (a spin
  # channel, a dead peer) are call-site errors. A signal landing between
  # here and the registration below schedules a drain that runs after the
  # action (FIFO), so nothing is missed.
  avail <- .Call(mizu_chan_notify_arm, ch, state)
  promises::promise(function(resolve, reject) {
    w <- new.env(parent = emptyenv())
    w[["resolve"]] <- resolve
    w[["reject"]] <- reject
    chan_fifo_push(state, w)
    if (is.finite(timeout)) {
      w[["timer"]] <- later::later(
        function() {
          chan_fifo_remove(state, w)
          reject(cond_timeout())
        },
        timeout
      )
    }
    if (avail) {
      chan_drain(state)
    }
  })
}

#' @rdname mizu_recv_async
#' @export
mizu_recv_handler <- function(ch, fn, batch = FALSE) {
  if (is.null(fn)) {
    state <- .Call(mizu_chan_notify_peek, ch)
    if (!is.null(state) && identical(state[["mode"]], "handler")) {
      chan_disarm(state)
    }
    return(invisible(ch))
  }
  notify_requires_later()
  if (!is.function(fn)) {
    stop("mizu: fn must be a function or NULL", call. = FALSE)
  }
  state <- notify_chan_state(ch)
  if (identical(state[["mode"]], "promise")) {
    stop(
      "mizu: promises are pending on this channel \u2014 mizu_recv_async() ",
      "and mizu_recv_handler() are mutually exclusive",
      call. = FALSE
    )
  }
  state[["mode"]] <- "handler"
  state[["fn"]] <- fn
  state[["batch"]] <- isTRUE(batch)
  avail <- .Call(mizu_chan_notify_arm, ch, state)
  if (avail) {
    chan_drain(state)
  }
  invisible(ch)
}

#' @rdname mizu_recv_async
#' @exportS3Method promises::as.promise
as.promise.mizu_channel <- function(x) mizu_recv_async(x)

#' @rdname mizu_recv_async
#' @exportS3Method promises::is.promising
is.promising.mizu_channel <- function(x) TRUE

# The mizu_close() flush (called after the close rendezvous): reject every
# pending promise and deliver the terminal to a handler — the handle's
# core-side teardown has already stopped the watch, so this is the last
# delivery. Total on a released handle (the state env outlives it in the
# prot chain).
notify_chan_flush_closed <- function(ch) {
  st <- .Call(mizu_chan_notify_peek, ch)
  if (!is.null(st) && !is.null(st[["mode"]])) {
    chan_terminal(st, cond_closed())
  }
  invisible()
}

# Pool state ------------------------------------------------------------------

# The per-pool-submitter notify state env (prot slot [6]): the waiter
# registry and the .drain closure. One coalescing notify word serves
# every outstanding waiter of the submitter — each enrolled task carries
# the NOTIFY announce on its own result slot, and a publish to any of
# them signals the endpoint once per burst. The registry is the same env
# linked list as the channel FIFO: enrollment pushes at the tail, the
# drain unlinks settled waiters, both O(1).
notify_pool_state <- function(pool) {
  st <- .Call(mizu_pool_notify_peek, pool)
  if (is.null(st)) {
    st <- new.env(parent = emptyenv())
    st[["pool"]] <- pool
    st[["w_head"]] <- NULL
    st[["w_tail"]] <- NULL
    st[[".drain"]] <- function() pool_drain(st)
    .Call(mizu_pool_notify_set, pool, st)
  }
  st
}

pool_disarm <- function(state) {
  .Call(mizu_pool_notify_disarm, state[["pool"]])
  invisible()
}

pool_waiter_push <- function(state, w) {
  w[["next"]] <- NULL
  if (is.null(state[["w_head"]])) {
    state[["w_head"]] <- w
  } else {
    state[["w_tail"]][["next"]] <- w
  }
  state[["w_tail"]] <- w
  invisible()
}

# Unlink a waiter by identity (a timeout fires); FALSE when absent.
pool_waiter_remove <- function(state, w) {
  prev <- NULL
  cur <- state[["w_head"]]
  while (!is.null(cur)) {
    if (identical(cur, w)) {
      nxt <- cur[["next"]]
      if (is.null(prev)) {
        state[["w_head"]] <- nxt
      } else {
        prev[["next"]] <- nxt
      }
      if (identical(state[["w_tail"]], cur)) {
        state[["w_tail"]] <- prev
      }
      cur[["next"]] <- NULL
      return(TRUE)
    }
    prev <- cur
    cur <- cur[["next"]]
  }
  FALSE
}

# The completion probe per waiter: the C verdicts (atomic loads only, no
# per-task vector) — per task for the single form, any/all for the
# collectives, and the terminal-or-abandoned runner verdicts for a map.
waiter_done <- function(w) {
  switch(
    w[["kind"]],
    task = .Call(mizu_pool_waiter_terminal, w[["tasks"]], 0L),
    any = .Call(mizu_pool_waiter_terminal, w[["tasks"]], 1L),
    all = .Call(mizu_pool_waiter_terminal, w[["tasks"]], 2L),
    map = {
      hs <- w[["map"]][["st"]][["handles"]]
      hs <- hs[!vapply(hs, is.null, TRUE)]
      !length(hs) ||
        .Call(mizu_map_runners_terminal, w[["map"]][["st"]][["wrap"]], hs)
    },
    stop("mizu: unknown waiter kind", call. = FALSE)
  )
}

# Resolve or reject a settled waiter. The announce withdraw runs ahead of
# the collect for the collective forms (the losers return to
# blocking-collectible; a racing publisher at worst sends one absorbed
# signal). The collects run non-blocking (the scan proved terminal), and
# a raised condition becomes the rejection — the same classed condition
# the sync verb raises, index fields included.
settle_waiter <- function(state, w) {
  cancel_timer(w)
  kind <- w[["kind"]]
  if (kind == "task") {
    v <- tryCatch(
      .Call(mizu_pool_collect, w[["tasks"]][[1L]], 0),
      error = identity
    )
  } else if (kind == "any") {
    .Call(mizu_pool_task_notify, w[["tasks"]], FALSE)
    v <- tryCatch(
      .Call(mizu_pool_collect_any, w[["tasks"]], 0),
      error = identity
    )
  } else if (kind == "all") {
    .Call(mizu_pool_task_notify, w[["tasks"]], FALSE)
    v <- tryCatch(
      .Call(mizu_pool_collect_all, w[["tasks"]], 0),
      error = identity
    )
  } else {
    # map: withdraw the runner announces, then the ordinary map gather —
    # every runner terminal or abandoned, so map_collect never parks
    m <- w[["map"]]
    st <- m[["st"]]
    hs <- st[["handles"]]
    hs <- hs[!vapply(hs, is.null, TRUE)]
    if (length(hs)) {
      .Call(mizu_pool_task_notify, hs, FALSE)
    }
    m[["done"]] <- TRUE
    v <- if (isTRUE(m[["cancelled"]])) {
      map_cond_cancelled()
    } else {
      tryCatch(
        map_collect(st, Inf, m[["collect"]], doomed0 = w[["doomed"]]),
        error = identity
      )
    }
    if (!inherits(v, "error")) {
      pm <- m[["pm"]]
      if (!is.null(pm) && !isTRUE(st[["consumed"]])) {
        pm[["st"]] <- st
      }
    }
  }
  if (inherits(v, "error")) {
    w[["reject"]](v)
  } else {
    w[["resolve"]](v)
  }
  invisible()
}

# The main-thread pool drain: one C verdict per waiter per pass, settling
# the completed, bounded per callback. The edge-triggered submitter word
# is cleared by the publish that woke the drain, so a pass that ends with
# a non-empty pending set re-arms and re-checks — the announce/re-check
# transplant: either the scan sees a publish or its publisher sees the
# word. Disarms only when the registry empties.
pool_drain <- function(state) {
  t0 <- mono_time()
  items <- 0L
  repeat {
    prev <- NULL
    w <- state[["w_head"]]
    while (!is.null(w)) {
      nxt <- w[["next"]]
      if (!waiter_done(w)) {
        prev <- w
        w <- nxt
        next
      }
      if (is.null(prev)) {
        state[["w_head"]] <- nxt
      } else {
        prev[["next"]] <- nxt
      }
      if (identical(state[["w_tail"]], w)) {
        state[["w_tail"]] <- prev
      }
      w[["next"]] <- NULL
      settle_waiter(state, w)
      items <- items + 1L
      # the clock read strides (per settle it was a .Call each)
      if (
        items >= notify_quantum_n ||
          (items %% 64L == 0L && mono_time() - t0 >= notify_quantum_secs)
      ) {
        later::later(state[[".drain"]], 0)
        return(invisible())
      }
      w <- nxt
    }
    if (is.null(state[["w_head"]])) {
      pool_disarm(state)
      return(invisible())
    }
    .Call(mizu_pool_notify_arm, state[["pool"]], state)
    # the re-check: a publish that landed between the last scan and the
    # re-arm signals nothing (it saw the cleared word) — scan once more
    if (!pool_any_done(state)) {
      return(invisible())
    }
  }
}

pool_any_done <- function(state) {
  w <- state[["w_head"]]
  while (!is.null(w)) {
    if (waiter_done(w)) {
      return(TRUE)
    }
    w <- w[["next"]]
  }
  FALSE
}

# The enrollment conflict check shared by every async form: a task
# already claimed by a pending waiter refuses another consumer, naming
# it. Returns the matching waiter when `want` is the waiter's own kind
# (as.promise's cached-promise lookup).
notify_find_waiter <- function(state, task) {
  w <- state[["w_head"]]
  while (!is.null(w)) {
    for (t in w[["tasks"]]) {
      if (identical(t, task)) {
        return(w)
      }
    }
    w <- w[["next"]]
  }
  NULL
}

notify_check_unenrolled <- function(state, tasks, verb) {
  if (is.null(state[["w_head"]])) {
    return(invisible())
  }
  for (task in tasks) {
    w <- notify_find_waiter(state, task)
    if (!is.null(w)) {
      stop(
        sprintf(
          "mizu: %s refuses a task with a pending consumer (%s) \u2014 ",
          verb,
          w[["label"]]
        ),
        "one consumer per result",
        call. = FALSE
      )
    }
  }
  invisible()
}

# The collect verbs' refusal (called with the task or each task of a
# blocking collective): a promise-armed or collection-enrolled task
# refuses a blocking collect, so one writer owns each result slot's
# announce. The flag is the enrollment itself — it clears at settle.
notify_check_collectable <- function(task) {
  st <- .Call(mizu_task_notify_state, task)
  if (is.null(st) || is.null(st[["w_head"]])) {
    return(invisible())
  }
  w <- notify_find_waiter(st, task)
  if (!is.null(w)) {
    stop(
      sprintf(
        "mizu: task has a pending consumer (%s) and refuses a blocking ",
        w[["label"]]
      ),
      "collect \u2014 one consumer per result",
      call. = FALSE
    )
  }
  invisible()
}

# Enroll one waiter. The announce and the arm run OUTSIDE the promise
# action (pool_announce_arm at the call sites): promise() converts an
# action error into a rejection, but a stale task or a submitter-less
# handle is a call-site error. The registration and the re-check scan
# ride the action: the scan settles whatever was already published, so
# arming is loss-free, and a signal landing between arm and registration
# schedules a drain that runs after it (FIFO).
pool_announce_arm <- function(state, tasks) {
  .Call(mizu_pool_task_notify, tasks, TRUE)
  .Call(mizu_pool_notify_arm, state[["pool"]], state)
  invisible()
}

pool_enroll <- function(state, w) {
  pool_waiter_push(state, w)
  pool_drain(state)
  invisible()
}

#' Collect Pool Results as Promises
#'
#' `mizu_collect_all_async()` and `mizu_collect_any_async()` are the
#' async counterparts of [mizu_collect_all()] and [mizu_collect_any()]:
#' each returns immediately with exactly **one** [promises::promise()]
#' per collection — no per-task promise is ever allocated. The arm
#' announces each task on its own result slot; on each wake a C terminal
#' scan notes newly-terminal handles, and when the collection completes
#' the sync verb's gather runs once and resolves the promise.
#'
#' Semantics mirror the sync verbs. Collect-all resolves in input order
#' with names carried; the first erroring handle by position rejects
#' with `index` (only that handle consumed, the rest collectible).
#' Collect-any resolves `list(index, value)` for the first terminal
#' task, a raised outcome rejecting with the `index` field. A
#' `mizu_collect_all_async()` timeout rejects with the `mizu_timeout`
#' classed error and consumes nothing. When a collection settles —
#' resolve, reject, or timeout — every enrolled handle's announce is
#' withdrawn: the handles the settle leaves unconsumed return to
#' blocking-collectible.
#'
#' A task with a promise attached — or enrolled in a pending collection —
#' refuses [mizu_collect()], [mizu_collect_any()], [mizu_collect_all()],
#' and a second enrollment, with an error naming the pending consumer.
#' [mizu_cancel()] stays allowed: the cancel publishes, and the drain
#' rejects with `mizu_error_cancelled`.
#'
#' Requires the \pkg{later} and \pkg{promises} packages.
#'
#' @param tasks a non-empty list of task handles from [mizu_submit()] on
#'   the same pool handle.
#' @param timeout seconds after which the promise rejects with the
#'   `mizu_timeout` classed error; nothing is consumed. `Inf` (the
#'   default) waits indefinitely.
#' @param x for the `as.promise` methods: a task handle, or a map handle
#'   from [mizu_map()] / [mizu_map_run()].
#'
#' @return A [promises::promise()]: collect-all resolves a plain list of
#'   the task values in the order of `tasks` (names carried); collect-any
#'   resolves `list(index, value)`.
#'
#' @examplesIf interactive() && requireNamespace("promises", quietly = TRUE)
#' library(promises)
#' p <- mizu_pool(2L)
#' ts <- list(
#'   total = mizu_submit(p, sum(x), x = runif(10)),
#'   label = mizu_submit(p, "done")
#' )
#' mizu_collect_all_async(ts) %...>% print()
#' later::run_now()
#' mizu_pool_stop(p)
#'
#' @export
mizu_collect_all_async <- function(tasks, timeout = Inf) {
  notify_requires()
  if (!is.numeric(timeout) || length(timeout) != 1L || is.na(timeout)) {
    stop("mizu: timeout must be a number of seconds, not NA", call. = FALSE)
  }
  if (!is.list(tasks) || !length(tasks)) {
    stop("mizu: tasks must be a non-empty list of task handles", call. = FALSE)
  }
  pool <- .Call(mizu_task_pool, tasks[[1L]])
  state <- notify_pool_state(pool)
  notify_check_unenrolled(state, tasks, "mizu_collect_all_async()")
  pool_announce_arm(state, tasks)
  w <- new.env(parent = emptyenv())
  w[["kind"]] <- "all"
  w[["tasks"]] <- tasks
  w[["label"]] <- "mizu_collect_all_async()"
  promises::promise(function(resolve, reject) {
    w[["resolve"]] <- resolve
    w[["reject"]] <- reject
    if (is.finite(timeout)) {
      w[["timer"]] <- later::later(
        function() {
          if (!pool_waiter_remove(state, w)) {
            return(invisible())
          }
          .Call(mizu_pool_task_notify, tasks, FALSE)
          if (is.null(state[["w_head"]])) {
            pool_disarm(state)
          }
          reject(cond_timeout())
        },
        timeout
      )
    }
    pool_enroll(state, w)
  })
}

#' @rdname mizu_collect_all_async
#' @export
mizu_collect_any_async <- function(tasks) {
  notify_requires()
  if (!is.list(tasks) || !length(tasks)) {
    stop("mizu: tasks must be a non-empty list of task handles", call. = FALSE)
  }
  pool <- .Call(mizu_task_pool, tasks[[1L]])
  state <- notify_pool_state(pool)
  notify_check_unenrolled(state, tasks, "mizu_collect_any_async()")
  pool_announce_arm(state, tasks)
  w <- new.env(parent = emptyenv())
  w[["kind"]] <- "any"
  w[["tasks"]] <- tasks
  w[["label"]] <- "mizu_collect_any_async()"
  promises::promise(function(resolve, reject) {
    w[["resolve"]] <- resolve
    w[["reject"]] <- reject
    pool_enroll(state, w)
  })
}

#' @rdname mizu_collect_all_async
#' @exportS3Method promises::as.promise
as.promise.mizu_task <- function(x) {
  notify_requires()
  pool <- .Call(mizu_task_pool, x)
  state <- notify_pool_state(pool)
  existing <- notify_find_waiter(state, x)
  if (!is.null(existing)) {
    if (existing[["kind"]] == "task") {
      return(existing[["promise"]])
    }
    stop(
      sprintf(
        "mizu: task is enrolled in a pending %s and refuses a second ",
        existing[["label"]]
      ),
      "promise \u2014 one consumer per result",
      call. = FALSE
    )
  }
  tstate <- .Call(mizu_pool_task_state, x)
  if (tstate == "collected" || tstate == "dropped") {
    stop("mizu: task handle is already collected", call. = FALSE)
  }
  pool_announce_arm(state, list(x))
  w <- new.env(parent = emptyenv())
  w[["kind"]] <- "task"
  w[["tasks"]] <- list(x)
  w[["label"]] <- "as.promise()"
  promise <- promises::promise(function(resolve, reject) {
    w[["resolve"]] <- resolve
    w[["reject"]] <- reject
    pool_enroll(state, w)
  })
  w[["promise"]] <- promise
  promise
}

#' @rdname mizu_collect_all_async
#' @exportS3Method promises::is.promising
is.promising.mizu_task <- function(x) TRUE

#' @rdname mizu_collect_all_async
#' @exportS3Method promises::as.promise
as.promise.mizu_map <- function(x) {
  notify_requires()
  if (!is.null(x[["promise"]])) {
    return(x[["promise"]])
  }
  if (isTRUE(x[["done"]])) {
    stop("mizu: map handle is already collected", call. = FALSE)
  }
  st <- x[["st"]]
  if (isTRUE(st[["stream"]])) {
    stop(
      "mizu: a streaming map handle cannot be promise-armed \u2014 collect it ",
      "with mizu_collect()",
      call. = FALSE
    )
  }
  if (isTRUE(st[["empty"]])) {
    # no enrollment: nothing runs, nothing publishes — resolve at once
    x[["done"]] <- TRUE
    x[["promised"]] <- TRUE
    promise <- promises::promise(function(resolve, reject) {
      resolve(map_empty_result(st))
    })
    x[["promise"]] <- promise
    return(promise)
  }
  pool <- x[["pool"]]
  state <- notify_pool_state(pool)
  w <- new.env(parent = emptyenv())
  w[["kind"]] <- "map"
  w[["map"]] <- x
  w[["tasks"]] <- st[["handles"]]
  w[["label"]] <- "a map promise"
  w[["doomed"]] <- FALSE
  # the death verdict no publish can carry: every worker live at submit
  # gone from the registry with the runners unclaimed. No signal fires
  # for it, so — as the sync collect's bounded registry poll — a 1 s
  # watchdog checks while the map pends, then takes the doomed path.
  arm_watchdog <- function() {
    w[["timer"]] <- later::later(
      function() {
        if (
          !is.null(st[["live0"]]) &&
            st[["live0"]] > 0L &&
            map_workers_gone(st[["pool"]])
        ) {
          w[["doomed"]] <- TRUE
          if (!is.null(st[["wrap"]])) {
            .Call(mizu_map_cancel_set, st[["wrap"]])
          }
          pool_drain(state)
        } else {
          arm_watchdog()
        }
      },
      1
    )
    invisible()
  }
  w[["timer"]] <- NULL
  x[["promised"]] <- TRUE
  pool_announce_arm(state, st[["handles"]])
  promise <- promises::promise(function(resolve, reject) {
    w[["resolve"]] <- resolve
    w[["reject"]] <- reject
    arm_watchdog()
    pool_enroll(state, w)
  })
  w[["promise"]] <- promise
  x[["promise"]] <- promise
  promise
}

#' @rdname mizu_collect_all_async
#' @exportS3Method promises::is.promising
is.promising.mizu_map <- function(x) TRUE
