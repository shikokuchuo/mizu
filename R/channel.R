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
#' unlike [mizu_submit()] mizu does not capture it for you: pass it
#' pre-quoted. The peer evaluates it in a fresh environment whose parent is
#' the global environment of the child. `ch` (the peer-side channel handle)
#' is the only binding that mizu provides. Data crosses the ring, and the
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
#' the same files. The environment variable `MIZU_LIVENESS_DIR`, read in
#' the creating process, overrides the default.
#'
#' @param expr a quoted expression (for example `quote({ ... })`), evaluated
#'   in the peer process with `ch` bound to the peer-side channel handle.
#'   A single character string is instead UTF-8 source text in the peer's
#'   language, for a non-R peer spawned by a custom `launcher` (an R peer
#'   parses and evaluates it as R source).
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
#' @param launcher a `function(token)` that spawns the peer process. For an
#'   R peer, it arranges for a process to call `mizu:::peer_main(token)`. The
#'   default [mizu_launcher()] spawns `Rscript` and propagates the
#'   `.libPaths()` of the host. Its `stdout` and `stderr` arguments direct
#'   the peer output, including the error epilogue. A custom launcher must
#'   arrange the library paths itself. For a peer in another language (a
#'   source-string `expr`), it spawns a program that attaches with `token`
#'   and speaks the wire protocol, such as `python3 -m pymizu.child` for a
#'   Python peer — [mizu_py_launcher()] is the ready-made launcher for
#'   that case.
#' @param startup_timeout seconds to wait for the peer to attach and signal
#'   ready. On expiry, mizu releases the channel and raises
#'   `mizu_error_startup` (see [mizu_error]).
#'
#' @return A channel handle (class `"mizu_channel"`). Handles are
#'   process-private and do not survive `fork()`.
#'
#' @examples
#' ch <- mizu_channel(quote(
#'   repeat {
#'     x <- mizu_recv(ch, timeout = 30)
#'     if (inherits(x, "mizu_sentinel")) break
#'     mizu_send(ch, x)
#'   }
#' ))
#' mizu_send(ch, 42L)
#' mizu_recv(ch, timeout = 5)
#' mizu_close(ch)
#'
#' @export
mizu_channel <- function(
  expr,
  capacity = 16384L,
  slot_size = 256L,
  arena_size = 4194304,
  spin = FALSE,
  launcher = mizu_launcher(),
  startup_timeout = 30
) {
  if (
    !is.language(expr) &&
      !(is.character(expr) && length(expr) == 1L && !is.na(expr))
  ) {
    stop(
      "mizu: expr must be a quoted expression (wrap it in quote()) or a ",
      "single source string (for a peer in another language)",
      call. = FALSE
    )
  }
  ch <- .Call(mizu_channel_create, expr, capacity, slot_size, arena_size, spin)
  token <- .Call(mizu_channel_suffix, ch)
  launcher(token)
  if (!.Call(mizu_channel_ready_wait, ch, startup_timeout)) {
    .Call(mizu_channel_destroy, ch)
    stop_mizu(
      "mizu_error_startup",
      paste0(
        "mizu: child failed to attach within ",
        format(startup_timeout),
        " seconds"
      )
    )
  }
  ch
}

#' Send and Receive over a Channel
#'
#' `mizu_send()` publishes a message to the peer. The message is visible the
#' moment the call returns, with no separate flush step. `mizu_recv()`
#' returns the next message, and waits up to `timeout` seconds for it.
#'
#' Sends never block for ring space. Receives surface every terminal state
#' as a class-tagged sentinel, not an error. Dispatch with
#' `inherits(x, "mizu_sentinel")`, or on the specific classes:
#'
#' * `mizu_full` — the ring is full (send). Back off until the peer drains,
#'   or drop the message.
#' * `mizu_timeout` — no message arrived within `timeout` (recv).
#' * `mizu_closed` — the other side closed the channel. A receive drains all
#'   published messages before it reports this.
#' * `mizu_peer_gone` — the peer died without closing. The verdict comes
#'   from the kernel-released liveness lock, at OS death-notification
#'   latency. A receive drains first here too: the published messages of a
#'   dead peer are complete and valid. Sticky once returned.
#'
#' `NULL` is a legal payload. Sentinels are ordinary values, identifiable
#' by class alone, and never signalled conditions. [mizu_is_sentinel()]
#' checks identity where payloads are untrusted.
#' `NULL` crosses as an immediate: no serialization and no receive-side
#' allocation. Length-1 character vectors that fit the inline budget cross
#' with a single byte copy, encoding mark preserved. Attribute-free
#' non-ALTREP atomic vectors — and `integer64` vectors whose only
#' attribute is the class — ride a serialization-free fast path with a
#' byte-identical round-trip — inline within the budget, and past it as
#' bare bytes in the arena or a spill region (no serialize, no parse).
#' Other plain values — attributed vectors, strings, lists, calls — cross
#' as a compact binary stream written and read without R's serializer.
#' Anything else is R-serialized. Mori-shared objects reduce to identifier
#' wire forms through the mori hooks.
#'
#' Cross-language, a numpy `int64` array (or an Arrow one) from a Python
#' peer lands as an `integer64` vector — bit64's exact layout, constructed
#' without bit64: with bit64 loaded the result is fully functional, and
#' without it the bits round-trip but print as raw doubles. The missing
#' sentinel is shared both directions: a genuine `INT64_MIN` reads as
#' `NA_integer64_` in R, and an `NA_integer64_` arrives as `INT64_MIN` in
#' Python.
#'
#' From a non-R peer, only vectors and strings are legal payloads. Anything
#' else (a pymizu codec stream or a pickle) is declined: the receive raises
#' a classed `mizu_error_python_payload` error (see [mizu_error]). The
#' declined message is consumed, so the channel keeps flowing.
#'
#' @param ch a channel handle from [mizu_channel()] (or the `ch` binding
#'   inside a peer expression).
#' @param x the payload: any R object.
#' @param timeout seconds to wait for a message before returning the
#'   `mizu_timeout` sentinel. `Inf` (the default) waits indefinitely;
#'   `0` does not wait.
#'
#' @return `mizu_send()` returns `TRUE` (invisibly) on success, or a
#'   sentinel otherwise. `mizu_recv()` returns the received payload or a
#'   sentinel.
#'
#' @examples
#' ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch))))
#' mizu_send(ch, list(1, "a"))
#' mizu_recv(ch, timeout = 5)
#' mizu_close(ch)
#'
#' @export
mizu_send <- function(ch, x) invisible(.Call(mizu_channel_send, ch, x))

#' @rdname mizu_send
#' @export
mizu_recv <- function(ch, timeout = Inf) .Call(mizu_channel_recv, ch, timeout)

#' Batched Send and Receive
#'
#' At target rates the R call boundary is a first-order cost.
#' `mizu_send_batch()` moves a list of payloads in a single `.Call` and
#' publishes them to the peer in one batched tail store.
#' `mizu_recv_batch()` drains up to `n` messages in a single park cycle and
#' a single batched head publication.
#'
#' @inheritParams mizu_send
#' @param xs a list of payloads.
#' @param n maximum number of messages to return.
#'
#' @return `mizu_send_batch()` returns the number of messages accepted. This
#'   is less than `length(xs)` when the ring filled or the channel closed
#'   midway. Send the next element with [mizu_send()] to learn which.
#'   `mizu_recv_batch()` waits for the first message like [mizu_recv()] and
#'   returns its sentinels on timeout, close, or peer death. It then
#'   returns a list of 1 to `n` already-published messages without waiting
#'   further. A batch that reaches a message it cannot read (a foreign
#'   Python payload) returns what it read before it, and the failure
#'   surfaces on the next receive, which raises like [mizu_recv()] — as
#'   does the batch itself when the first message is the one declined.
#'
#' @examples
#' ch <- mizu_channel(quote(mizu_send_batch(ch, mizu_recv_batch(ch, 3L))))
#' mizu_send_batch(ch, list(1, 2, 3))
#' mizu_recv_batch(ch, 3L, timeout = 5)
#' mizu_close(ch)
#'
#' @export
mizu_send_batch <- function(ch, xs) .Call(mizu_channel_send_batch, ch, xs)

#' @rdname mizu_send_batch
#' @export
mizu_recv_batch <- function(ch, n = 256L, timeout = Inf) {
  .Call(mizu_channel_recv_batch, ch, n, timeout)
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
#' `mizu_closed` sentinel, and receives drain the remaining messages before
#' they return the same.
#'
#' @inheritParams mizu_send
#' @param timeout seconds to wait for the close of the peer.
#'
#' @return Invisibly, `TRUE` on rendezvous, `FALSE` on timeout (with a
#'   warning).
#'
#' @examples
#' ch <- mizu_channel(quote(mizu_send(ch, "done")))
#' mizu_recv(ch, timeout = 5)
#' mizu_close(ch)
#'
#' @export
mizu_close <- function(ch, timeout = 5) {
  ok <- .Call(mizu_channel_close, ch, timeout)
  if (!ok) {
    warning(
      "mizu: close timed out waiting for the peer; resources release ",
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
#' [mizu_recv()] and [mizu_send()] surface peer death automatically as
#' `mizu_peer_gone`. Use this probe to ask without touching the rings. A
#' peer that closed the channel but still runs reads as alive.
#'
#' @inheritParams mizu_send
#'
#' @return `TRUE` while the peer process is alive, `FALSE` after it dies.
#'   At that point the survivor unlinked the names of the channel.
#'
#' @examples
#' ch <- mizu_channel(quote(mizu_recv(ch)))
#' mizu_alive(ch)
#' mizu_close(ch)
#'
#' @export
mizu_alive <- function(ch) .Call(mizu_channel_alive, ch)

# Peer entry point: invoked through the Rscript child runner by the launcher.
# Rebuilds the region name from the compiled-in prefix plus the token (the
# name's suffix), attaches writable, validates the preamble, takes its liveness
# lock, points its death listener at the host, and materializes the staged
# expression *before* signalling ready — the host's mizu_channel frame holds
# the expression (and every region its identifiers name) alive exactly until
# then. The epilogue is the peer half of the close protocol.
peer_main <- function(token) {
  if (!any(search() == "package:mizu")) {
    attachNamespace("mizu")
  }
  .Call(mizu_tune_malloc)
  att <- .Call(mizu_channel_attach, token)
  ch <- att[[1L]]
  expr <- att[[2L]]
  .Call(mizu_channel_ready_set, ch)
  env <- new.env(parent = globalenv())
  env[["ch"]] <- ch
  status <- 0L
  tryCatch(
    # a character drop is UTF-8 source text (MIZU_DROP_SOURCE); parse errors
    # are peer errors, an orderly close like an eval error
    eval(if (is.character(expr)) parse(text = expr) else expr, envir = env),
    error = function(e) {
      cat(
        "mizu peer error: ",
        conditionMessage(e),
        "\n",
        sep = "",
        file = stderr()
      )
      status <<- 1L
    },
    interrupt = function(e) status <<- 2L
  )
  .Call(mizu_channel_close_signal, ch)
  quit(save = "no", status = status)
}
