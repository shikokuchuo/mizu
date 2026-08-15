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
#' unlike [sora_submit()] sora does not capture it for you: pass it
#' pre-quoted. The peer evaluates it in a fresh environment whose parent is
#' the global environment of the child. `ch` (the peer-side channel handle)
#' is the only binding that sora provides. Data crosses the ring, and the
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
#' the same files. The environment variable `SORA_LIVENESS_DIR`, read in
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
#'   call `sora:::peer_main(token)`. The default [sora_launcher()] spawns
#'   `Rscript` and propagates the `.libPaths()` of the host. Its `stdout`
#'   and `stderr` arguments direct the peer output, including the error
#'   epilogue. A custom launcher must arrange the library paths itself.
#' @param startup_timeout seconds to wait for the peer to attach and signal
#'   ready. On expiry, sora releases the channel and raises
#'   `sora_error_startup` (see [sora_error]).
#'
#' @return A channel handle (class `"sora_channel"`). Handles are
#'   process-private and do not survive `fork()`.
#'
#' @examples
#' ch <- sora_channel(quote(
#'   repeat {
#'     x <- sora_recv(ch, timeout = 30)
#'     if (inherits(x, "sora_sentinel")) break
#'     sora_send(ch, x)
#'   }
#' ))
#' sora_send(ch, 42L)
#' sora_recv(ch, timeout = 5)
#' sora_close(ch)
#'
#' @export
sora_channel <- function(
  expr,
  capacity = 16384L,
  slot_size = 256L,
  arena_size = 4194304,
  spin = FALSE,
  launcher = sora_launcher(),
  startup_timeout = 30
) {
  if (!is.language(expr)) {
    stop(
      "sora: expr must be a quoted expression (wrap it in quote())",
      call. = FALSE
    )
  }
  ch <- .Call(sora_channel_create, expr, capacity, slot_size, arena_size, spin)
  token <- .Call(sora_channel_suffix, ch)
  launcher(token)
  if (!.Call(sora_channel_ready_wait, ch, startup_timeout)) {
    .Call(sora_channel_destroy, ch)
    stop_sora(
      "sora_error_startup",
      paste0(
        "sora: child failed to attach within ",
        format(startup_timeout),
        " seconds"
      )
    )
  }
  ch
}

#' Send and Receive over a Channel
#'
#' `sora_send()` publishes a message to the peer. The message is visible the
#' moment the call returns, with no separate flush step. `sora_recv()`
#' returns the next message, and waits up to `timeout` seconds for it.
#'
#' Sends never block for ring space. Receives surface every terminal state
#' as a class-tagged sentinel, not an error. Dispatch with
#' `inherits(x, "sora_sentinel")`, or on the specific classes:
#'
#' * `sora_full` — the ring is full (send). Back off until the peer drains,
#'   or drop the message.
#' * `sora_timeout` — no message arrived within `timeout` (recv).
#' * `sora_closed` — the other side closed the channel. A receive drains all
#'   published messages before it reports this.
#' * `sora_peer_gone` — the peer died without closing. The verdict comes
#'   from the kernel-released liveness lock, at OS death-notification
#'   latency. A receive drains first here too: the published messages of a
#'   dead peer are complete and valid. Sticky once returned.
#'
#' `NULL` is a legal payload. Sentinels are ordinary values, identifiable
#' by class alone, and never signalled conditions. [sora_is_sentinel()]
#' checks identity where payloads are untrusted.
#' Attribute-free non-ALTREP atomic vectors that fit the inline budget ride
#' a serialization-free fast path with a byte-identical round-trip.
#' Anything else is R-serialized. Mori-shared objects reduce to identifier
#' wire forms through the mori hooks.
#'
#' @param ch a channel handle from [sora_channel()] (or the `ch` binding
#'   inside a peer expression).
#' @param x the payload: any R object.
#' @param timeout seconds to wait before the call returns the `sora_timeout`
#'   sentinel. `Inf` (the default) waits indefinitely, and `0` polls.
#'   Ctrl-C stays responsive during the wait.
#'
#' @return `sora_send()` returns `TRUE` (invisibly) on success, or a
#'   sentinel otherwise. `sora_recv()` returns the received payload or a
#'   sentinel.
#'
#' @examples
#' ch <- sora_channel(quote(sora_send(ch, sora_recv(ch))))
#' sora_send(ch, list(1, "a"))
#' sora_recv(ch, timeout = 5)
#' sora_close(ch)
#'
#' @export
sora_send <- function(ch, x) invisible(.Call(sora_channel_send, ch, x))

#' @rdname sora_send
#' @export
sora_recv <- function(ch, timeout = Inf) .Call(sora_channel_recv, ch, timeout)

#' Batched Send and Receive
#'
#' At target rates the R call boundary is a first-order cost.
#' `sora_send_batch()` moves a list of payloads in a single `.Call` and
#' publishes them to the peer in one batched tail store.
#' `sora_recv_batch()` drains up to `n` messages in a single park cycle and
#' a single batched head publication.
#'
#' @inheritParams sora_send
#' @param xs a list of payloads.
#' @param n maximum number of messages to return.
#'
#' @return `sora_send_batch()` returns the number of messages accepted. This
#'   is less than `length(xs)` when the ring filled or the channel closed
#'   midway. Send the next element with [sora_send()] to learn which.
#'   `sora_recv_batch()` waits for the first message like [sora_recv()] and
#'   returns its sentinels on timeout, close, or peer death. It then
#'   returns a list of 1 to `n` already-published messages without waiting
#'   further.
#'
#' @examples
#' ch <- sora_channel(quote(sora_send_batch(ch, sora_recv_batch(ch, 3L))))
#' sora_send_batch(ch, list(1, 2, 3))
#' sora_recv_batch(ch, 3L, timeout = 5)
#' sora_close(ch)
#'
#' @export
sora_send_batch <- function(ch, xs) .Call(sora_channel_send_batch, ch, xs)

#' @rdname sora_send_batch
#' @export
sora_recv_batch <- function(ch, n = 256L, timeout = Inf) {
  .Call(sora_channel_recv_batch, ch, n, timeout)
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
#' `sora_closed` sentinel, and receives drain the remaining messages before
#' they return the same.
#'
#' @inheritParams sora_send
#' @param timeout seconds to wait for the close of the peer.
#'
#' @return Invisibly, `TRUE` on rendezvous, `FALSE` on timeout (with a
#'   warning).
#'
#' @examples
#' ch <- sora_channel(quote(sora_send(ch, "done")))
#' sora_recv(ch, timeout = 5)
#' sora_close(ch)
#'
#' @export
sora_close <- function(ch, timeout = 5) {
  ok <- .Call(sora_channel_close, ch, timeout)
  if (!ok) {
    warning(
      "sora: close timed out waiting for the peer; resources release ",
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
#' [sora_recv()] and [sora_send()] surface peer death automatically as
#' `sora_peer_gone`. Use this probe to ask without touching the rings. A
#' peer that closed the channel but still runs reads as alive.
#'
#' @inheritParams sora_send
#'
#' @return `TRUE` while the peer process is alive, `FALSE` after it dies.
#'   At that point the survivor unlinked the names of the channel.
#'
#' @examples
#' ch <- sora_channel(quote(sora_recv(ch)))
#' sora_alive(ch)
#' sora_close(ch)
#'
#' @export
sora_alive <- function(ch) .Call(sora_channel_alive, ch)

# Peer entry point: invoked through the Rscript child runner by the launcher.
# Rebuilds the region name from the compiled-in prefix plus the token (the
# name's suffix), attaches writable, validates the preamble, takes its liveness
# lock, points its death listener at the host, and materializes the staged
# expression *before* signalling ready — the host's sora_channel frame holds
# the expression (and every region its identifiers name) alive exactly until
# then. The epilogue is the peer half of the close protocol.
peer_main <- function(token) {
  if (!"package:sora" %in% search()) {
    attachNamespace("sora")
  }
  att <- .Call(sora_channel_attach, token)
  ch <- att[[1L]]
  expr <- att[[2L]]
  .Call(sora_channel_ready_set, ch)
  env <- new.env(parent = globalenv())
  env[["ch"]] <- ch
  status <- 0L
  tryCatch(
    eval(expr, envir = env),
    error = function(e) {
      cat(
        "sora peer error: ",
        conditionMessage(e),
        "\n",
        sep = "",
        file = stderr()
      )
      status <<- 1L
    },
    interrupt = function(e) status <<- 2L
  )
  .Call(sora_channel_close_signal, ch)
  quit(save = "no", status = status)
}
