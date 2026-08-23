#' Create a Channel and Spawn Its Peer
#'
#' Creates a shared-memory SPSC channel — one lock-free ring per direction —
#' and spawns a child R process connected to the other end. The channel is
#' two-way after the spawn: the returned handle produces on the host-to-peer
#' ring and consumes the peer-to-host ring. Setup is one-sided: only a short
#' join token (the suffix of the region name) crosses the process boundary,
#' as a command-line argument.
#'
#' `expr` is a quoted expression, not a closure. It captures nothing, and
#' unlike [rei_submit()] rei does not capture it for you: pass it
#' pre-quoted. The peer evaluates it in a fresh environment whose parent is
#' the global environment of the child. `ch` (the peer-side channel handle)
#' is the only binding that rei provides. Data crosses the ring, and the
#' expression itself loads any packages it needs. When the expression
#' returns or errors, the peer signals an orderly close and exits. An error
#' message goes to the stderr of the child.
#'
#' Payload contents interoperate transparently with mori. A `mori::share()`d
#' object anywhere inside a payload serializes to its short identifier wire
#' form through the mori hooks, and maps zero-copy on the other side.
#'
#' The two liveness lock files of the channel (the death-detection verdict)
#' live in a per-platform directory chosen at create time. This is
#' `/dev/shm` on Linux, and the per-user temporary directory on macOS and
#' Windows. The chosen path is recorded in the region, so both sides use
#' the same files. The environment variable `REI_LIVENESS_DIR`, read in
#' the creating process, overrides the default.
#'
#' @param expr a quoted expression (for example `quote({ ... })`), evaluated
#'   in the peer process with `ch` bound to the peer-side channel handle.
#' @param capacity slots per ring. A power of two between 2 and 2^24.
#' @param slot_size bytes per slot. A power of two between 64 and 2^20. The
#'   inline payload budget is `slot_size - 16`. Payloads that serialize
#'   larger spill to the arena, and past it to a fresh region.
#' @param arena_size spill-arena bytes per direction. A multiple of 64, or 0
#'   to disable (every spill then creates a region). This bounds the
#'   in-flight spill. An undersized arena degrades to region-create
#'   fallbacks, never to errors.
#' @param spin opts the channel into pure-spin waiting: consumers never park
#'   and producers skip the wake check on publish. Use this only when the
#'   consumer never yields. If a spin-mode consumer parks, the producer
#'   never wakes it.
#' @param launcher a `function(token)` that arranges for an R process to
#'   call `rei:::peer_main(token)`. The default [rei_launcher()] spawns
#'   `Rscript` and propagates the `.libPaths()` of the host. Its `stdout`
#'   and `stderr` arguments direct the peer output, including the error
#'   epilogue. A custom launcher must arrange the library paths itself.
#' @param startup_timeout seconds to wait for the peer to attach and signal
#'   ready. On expiry, rei releases the channel and raises
#'   `rei_error_startup` (see [rei_error]).
#'
#' @return A channel handle (class `"rei_channel"`). Handles are
#'   process-private and do not survive `fork()`.
#'
#' @examples
#' ch <- rei_channel(quote(
#'   repeat {
#'     x <- rei_recv(ch, timeout = 30)
#'     if (inherits(x, "rei_sentinel")) break
#'     rei_send(ch, x)
#'   }
#' ))
#' rei_send(ch, 42L)
#' rei_recv(ch, timeout = 5)
#' rei_close(ch)
#'
#' @export
rei_channel <- function(
  expr,
  capacity = 16384L,
  slot_size = 256L,
  arena_size = 4194304,
  spin = FALSE,
  launcher = rei_launcher(),
  startup_timeout = 30
) {
  if (!is.language(expr)) {
    stop(
      "rei: expr must be a quoted expression (wrap it in quote())",
      call. = FALSE
    )
  }
  ch <- .Call(rei_channel_create, expr, capacity, slot_size, arena_size, spin)
  token <- .Call(rei_channel_suffix, ch)
  launcher(token)
  if (!.Call(rei_channel_ready_wait, ch, startup_timeout)) {
    .Call(rei_channel_destroy, ch)
    stop_rei(
      "rei_error_startup",
      paste0(
        "rei: child failed to attach within ",
        format(startup_timeout),
        " seconds"
      )
    )
  }
  ch
}

#' Send and Receive over a Channel
#'
#' `rei_send()` publishes a message to the peer. The message is visible the
#' moment the call returns, with no separate flush step. `rei_recv()`
#' returns the next message, and waits up to `timeout` seconds for it.
#'
#' Sends never block for ring space. Receives surface every terminal state
#' as a class-tagged sentinel, not an error. Dispatch with
#' `inherits(x, "rei_sentinel")`, or on the specific classes:
#'
#' * `rei_full` — the ring is full (send). Back off until the peer drains,
#'   or drop the message.
#' * `rei_timeout` — no message arrived within `timeout` (recv).
#' * `rei_closed` — the other side closed the channel. A receive drains all
#'   published messages before it reports this.
#' * `rei_peer_gone` — the peer died without closing. The verdict comes
#'   from the kernel-released liveness lock, at OS death-notification
#'   latency. A receive drains first here too: the published messages of a
#'   dead peer are complete and valid. Sticky once returned.
#'
#' `NULL` is a legal payload. Sentinels are ordinary values, identifiable
#' by class alone, and never signalled conditions. [rei_is_sentinel()]
#' checks identity where payloads are untrusted.
#' `NULL` crosses as an immediate: no serialization and no receive-side
#' allocation. Length-1 character vectors that fit the inline budget cross
#' with a single byte copy, encoding mark preserved. Attribute-free
#' non-ALTREP atomic vectors ride a serialization-free fast path with a
#' byte-identical round-trip — inline within the budget, and past it as
#' bare bytes in the arena or a spill region (no serialize, no parse).
#' Other plain values — attributed vectors, strings, lists, calls — cross
#' as a compact binary stream written and read without R's serializer.
#' Anything else is R-serialized. Mori-shared objects reduce to identifier
#' wire forms through the mori hooks.
#'
#' @param ch a channel handle from [rei_channel()] (or the `ch` binding
#'   inside a peer expression).
#' @param x the payload: any R object.
#' @param timeout seconds to wait before the call returns the `rei_timeout`
#'   sentinel. `Inf` (the default) waits indefinitely, and `0` polls.
#'   Ctrl-C stays responsive during the wait.
#'
#' @return `rei_send()` returns `TRUE` (invisibly) on success, or a
#'   sentinel otherwise. `rei_recv()` returns the received payload or a
#'   sentinel.
#'
#' @examples
#' ch <- rei_channel(quote(rei_send(ch, rei_recv(ch))))
#' rei_send(ch, list(1, "a"))
#' rei_recv(ch, timeout = 5)
#' rei_close(ch)
#'
#' @export
rei_send <- function(ch, x) invisible(.Call(rei_channel_send, ch, x))

#' @rdname rei_send
#' @export
rei_recv <- function(ch, timeout = Inf) .Call(rei_channel_recv, ch, timeout)

#' Batched Send and Receive
#'
#' At target rates the R call boundary is a first-order cost.
#' `rei_send_batch()` moves a list of payloads in a single `.Call` and
#' publishes them to the peer in one batched tail store.
#' `rei_recv_batch()` drains up to `n` messages in a single park cycle and
#' a single batched head publication.
#'
#' @inheritParams rei_send
#' @param xs a list of payloads.
#' @param n maximum number of messages to return.
#'
#' @return `rei_send_batch()` returns the number of messages accepted. This
#'   is less than `length(xs)` when the ring filled or the channel closed
#'   midway. Send the next element with [rei_send()] to learn which.
#'   `rei_recv_batch()` waits for the first message like [rei_recv()] and
#'   returns its sentinels on timeout, close, or peer death. It then
#'   returns a list of 1 to `n` already-published messages without waiting
#'   further.
#'
#' @examples
#' ch <- rei_channel(quote(rei_send_batch(ch, rei_recv_batch(ch, 3L))))
#' rei_send_batch(ch, list(1, 2, 3))
#' rei_recv_batch(ch, 3L, timeout = 5)
#' rei_close(ch)
#'
#' @export
rei_send_batch <- function(ch, xs) .Call(rei_channel_send_batch, ch, xs)

#' @rdname rei_send_batch
#' @export
rei_recv_batch <- function(ch, n = 256L, timeout = Inf) {
  .Call(rei_channel_recv_batch, ch, n, timeout)
}

#' Close a Channel
#'
#' Orderly shutdown. Signals close to the peer, then waits up to `timeout`
#' seconds for the close of the peer (or its death). This rendezvous makes
#' it safe to release the sent-payload pins: the peer sets its bit only
#' after it finishes draining. On rendezvous, all resources are released
#' and the region name is unlinked. The handle is dead afterwards, and
#' closing it again is a no-op. On timeout the handle stays usable, and the
#' resources release at garbage collection, which runs the same rendezvous
#' check again.
#'
#' After either side signals close, sends on both sides return the
#' `rei_closed` sentinel, and receives drain the remaining messages before
#' they return the same.
#'
#' @inheritParams rei_send
#' @param timeout seconds to wait for the close of the peer.
#'
#' @return Invisibly, `TRUE` on rendezvous, `FALSE` on timeout (with a
#'   warning).
#'
#' @examples
#' ch <- rei_channel(quote(rei_send(ch, "done")))
#' rei_recv(ch, timeout = 5)
#' rei_close(ch)
#'
#' @export
rei_close <- function(ch, timeout = 5) {
  ok <- .Call(rei_channel_close, ch, timeout)
  if (!ok) {
    warning(
      "rei: close timed out waiting for the peer; resources release ",
      "when the handle is garbage collected",
      call. = FALSE
    )
  }
  invisible(ok)
}

#' Probe Peer Liveness
#'
#' An explicit probe for supervisors. Reports whether the peer process
#' holds its liveness lock, in about 1 microsecond with no waiting.
#' [rei_recv()] and [rei_send()] surface peer death automatically as
#' `rei_peer_gone`. Use this probe to ask without touching the rings. A
#' peer that closed the channel but still runs reads as alive.
#'
#' @inheritParams rei_send
#'
#' @return `TRUE` while the peer process is alive, `FALSE` after it dies.
#'   At that point the survivor unlinked the names of the channel.
#'
#' @examples
#' ch <- rei_channel(quote(rei_recv(ch)))
#' rei_alive(ch)
#' rei_close(ch)
#'
#' @export
rei_alive <- function(ch) .Call(rei_channel_alive, ch)

# Peer entry point: invoked through the Rscript child runner by the launcher.
# Rebuilds the region name from the compiled-in prefix plus the token (the
# name's suffix), attaches writable, validates the preamble, takes its liveness
# lock, points its death listener at the host, and materializes the staged
# expression *before* signalling ready — the host's rei_channel frame holds
# the expression (and every region its identifiers name) alive exactly until
# then. The epilogue is the peer half of the close protocol.
peer_main <- function(token) {
  if (!"package:rei" %in% search()) {
    attachNamespace("rei")
  }
  att <- .Call(rei_channel_attach, token)
  ch <- att[[1L]]
  expr <- att[[2L]]
  .Call(rei_channel_ready_set, ch)
  env <- new.env(parent = globalenv())
  env[["ch"]] <- ch
  status <- 0L
  tryCatch(
    eval(expr, envir = env),
    error = function(e) {
      cat(
        "rei peer error: ",
        conditionMessage(e),
        "\n",
        sep = "",
        file = stderr()
      )
      status <<- 1L
    },
    interrupt = function(e) status <<- 2L
  )
  .Call(rei_channel_close_signal, ch)
  quit(save = "no", status = status)
}
