#' Create a Channel and Spawn Its Peer
#'
#' Creates a shared-memory SPSC channel — one lock-free ring per direction —
#' and spawns a child R process connected to its other end. The channel is
#' bidirectional after spawn: the returned handle produces on the host-to-peer
#' ring and consumes the peer-to-host ring. Setup is one-sided: only a short
#' join token (the region name's suffix) crosses the process boundary, as a
#' command-line argument.
#'
#' `expr` is a quoted expression, not a closure — it captures nothing, and
#' unlike [kio_submit()] it is not captured for you: pass it pre-quoted. The
#' peer evaluates it in a fresh environment whose parent is the child's
#' global environment, with `ch` (the peer-side channel handle) as the only
#' binding kioto provides; data crosses the ring, and packages are loaded by
#' the expression itself. When the expression returns (or errors — the
#' condition message is written to the child's stderr), the peer signals an
#' orderly close and exits.
#'
#' Payload contents interoperate transparently with mori: a `mori::share()`d
#' object anywhere inside a payload serializes to its short identifier wire
#' form via mori's own hooks and maps zero-copy on the other side.
#'
#' The channel's two liveness lock files (the death-detection verdict) are
#' created in a per-platform directory resolved at create time: `/dev/shm`
#' on Linux, the per-user temporary directory on macOS and Windows. The
#' resolved path is recorded in the region so both sides use the same
#' files. The environment variable `KIOTO_LIVENESS_DIR`, read in the
#' creating process, overrides the default.
#'
#' @param expr a quoted expression (e.g. `quote({ ... })`) evaluated in the
#'   peer process with `ch` bound to the peer-side channel handle.
#' @param capacity slots per ring; a power of two between 2 and 2^24.
#' @param slot_size bytes per slot; a power of two between 64 and 2^20. The
#'   inline payload budget is `slot_size - 16`; payloads that serialize
#'   larger spill to the arena, and past it to a fresh region.
#' @param arena_size spill-arena bytes per direction; a multiple of 64, or 0
#'   to disable (every spill then creates a region). Bounds in-flight spill;
#'   undersizing degrades to region-create fallbacks, never to errors.
#' @param spin opt the channel into pure-spin waiting: consumers never park
#'   and producers skip the wake check on publish. Only for callers whose
#'   consumer never yields — if a spin-mode consumer did park, the producer
#'   would never wake it.
#' @param launcher a `function(token)` that arranges for an R process to
#'   eventually call `kioto:::peer_main(token)`. The default
#'   [kio_launcher()] spawns `Rscript` with the host's `.libPaths()`
#'   propagated (its `stdout`/`stderr` arguments direct peer output,
#'   including its error epilogue); a custom launcher must arrange library
#'   paths itself.
#' @param startup_timeout seconds to wait for the peer to attach and signal
#'   ready before giving up, releasing the channel, and raising
#'   `kio_error_startup` (see [kio_error]).
#'
#' @return A channel handle (class `"kio_channel"`). Handles are
#'   process-private and do not survive `fork()`.
#'
#' @examples
#' \dontrun{
#' ch <- kio_channel(quote(
#'   repeat {
#'     x <- kio_recv(ch, timeout = 30)
#'     if (inherits(x, "kio_sentinel")) break
#'     kio_send(ch, x)
#'   }
#' ))
#' kio_send(ch, 42L)
#' kio_recv(ch, timeout = 5)
#' kio_close(ch)
#' }
#'
#' @export
kio_channel <- function(expr, capacity = 16384L, slot_size = 256L,
                        arena_size = 4194304, spin = FALSE,
                        launcher = kio_launcher(), startup_timeout = 30) {
  if (!is.language(expr))
    stop("kioto: expr must be a quoted expression (wrap it in quote())",
         call. = FALSE)
  ch <- .Call(kio_channel_create, expr, capacity, slot_size, arena_size,
              spin)
  token <- .Call(kio_channel_suffix, ch)
  launcher(token)
  if (!.Call(kio_channel_ready_wait, ch, startup_timeout)) {
    .Call(kio_channel_destroy, ch)
    stop_kio("kio_error_startup",
             paste0("kioto: child failed to attach within ",
                    format(startup_timeout), " seconds"))
  }
  ch
}

#' Send and Receive over a Channel
#'
#' `kio_send()` publishes a message to the peer — visible the moment the
#' call returns, with no separate flush step. `kio_recv()` returns the next
#' message, blocking up to `timeout` seconds.
#'
#' Sends never block for ring space and receives surface every terminal
#' state as a class-tagged sentinel rather than an error (dispatch with
#' `inherits(x, "kio_sentinel")`, or on the specific classes):
#'
#' * `kio_full` — the ring is full (send); back off until the peer drains,
#'   or drop.
#' * `kio_timeout` — no message within `timeout` (recv).
#' * `kio_closed` — the other side closed the channel. recv drains all
#'   published messages before reporting this.
#' * `kio_peer_gone` — the peer died without closing (verdict from the
#'   kernel-released liveness lock, detection at OS death-notification
#'   latency). recv likewise drains first: a dead peer's published messages
#'   are complete and valid. Sticky once returned.
#'
#' `NULL` is a legal payload; sentinels are identifiable by class alone —
#' ordinary values, never signalled conditions ([kio_is_sentinel()] checks
#' identity where payloads are untrusted).
#' Attribute-free non-ALTREP atomic vectors that fit the inline budget ride
#' a serialization-free fast path with a byte-identical round-trip; anything
#' else is R-serialized (mori-shared objects reduce to identifier wire forms
#' via mori's hooks).
#'
#' @param ch a channel handle from [kio_channel()] (or the `ch` binding
#'   inside a peer expression).
#' @param x the payload: any R object.
#' @param timeout seconds to wait before returning the `kio_timeout`
#'   sentinel; `Inf` (the default) waits indefinitely, `0` polls. Ctrl-C
#'   remains responsive while waiting.
#'
#' @return `kio_send()` returns `TRUE` (invisibly) on success, else a
#'   sentinel. `kio_recv()` returns the received payload or a sentinel.
#'
#' @examples
#' \dontrun{
#' ch <- kio_channel(quote(kio_send(ch, kio_recv(ch))))
#' kio_send(ch, list(1, "a"))
#' kio_recv(ch, timeout = 5)
#' kio_close(ch)
#' }
#'
#' @export
kio_send <- function(ch, x) invisible(.Call(kio_channel_send, ch, x))

#' @rdname kio_send
#' @export
kio_recv <- function(ch, timeout = Inf) .Call(kio_channel_recv, ch, timeout)

#' Batched Send and Receive
#'
#' At target rates the R call boundary is a first-order cost.
#' `kio_send_batch()` moves a list of payloads under a single `.Call`,
#' publishing them to the peer in one batched tail store; `kio_recv_batch()`
#' drains up to `n` messages under a single park cycle and a single batched
#' head publication.
#'
#' @inheritParams kio_send
#' @param xs a list of payloads.
#' @param n maximum number of messages to return.
#'
#' @return `kio_send_batch()` returns the number of messages accepted: less
#'   than `length(xs)` when the ring filled or the channel closed midway
#'   (send the next element with [kio_send()] to learn which).
#'   `kio_recv_batch()` waits for the first message like [kio_recv()] —
#'   returning its sentinels on timeout, close, or peer death — then
#'   returns a list of between 1 and `n` already-published messages without
#'   waiting further.
#'
#' @export
kio_send_batch <- function(ch, xs) .Call(kio_channel_send_batch, ch, xs)

#' @rdname kio_send_batch
#' @export
kio_recv_batch <- function(ch, n = 256L, timeout = Inf)
  .Call(kio_channel_recv_batch, ch, n, timeout)

#' Close a Channel
#'
#' Orderly shutdown: signals close to the peer, and
#' waits up to `timeout` seconds for the peer's own close (or its death) —
#' the rendezvous that makes releasing sent-payload pins safe, since the
#' peer sets its bit only after it has finished draining. On rendezvous all
#' resources are released and the region name unlinked; the handle is dead
#' afterwards (closing it again is a no-op). On timeout the handle stays
#' usable and resources release when it is garbage collected, re-running the
#' same rendezvous check.
#'
#' Once either side has signalled close, sends on both sides return the
#' `kio_closed` sentinel and receives drain remaining messages before doing
#' the same.
#'
#' @inheritParams kio_send
#' @param timeout seconds to wait for the peer's close.
#'
#' @return Invisibly, `TRUE` on rendezvous, `FALSE` on timeout (with a
#'   warning).
#'
#' @export
kio_close <- function(ch, timeout = 5) {
  ok <- .Call(kio_channel_close, ch, timeout)
  if (!ok)
    warning("kioto: close timed out waiting for the peer; resources release ",
            "when the handle is garbage collected", call. = FALSE)
  invisible(ok)
}

#' Probe Peer Liveness
#'
#' An explicit probe for supervisors: reports whether the peer process holds
#' its liveness lock (`~1` microsecond, no waiting). [kio_recv()] and
#' [kio_send()] surface peer death automatically as `kio_peer_gone`; this is
#' for callers that want to ask without touching the rings. A peer that has
#' closed the channel but is still running reads as alive.
#'
#' @inheritParams kio_send
#'
#' @return `TRUE` while the peer process is alive, `FALSE` once it has died
#'   (at which point the survivor has unlinked the channel's names).
#'
#' @export
kio_alive <- function(ch) .Call(kio_channel_alive, ch)

# Peer entry point: invoked through the Rscript child runner by the launcher.
# Rebuilds the region name from the compiled-in prefix plus the token (the
# name's suffix), attaches writable, validates the preamble, takes its liveness
# lock, points its death listener at the host, and materializes the staged
# expression *before* signalling ready — the host's kio_channel frame holds
# the expression (and every region its identifiers name) alive exactly until
# then. The epilogue is the peer half of the close protocol.
peer_main <- function(token) {
  if (!"package:kioto" %in% search()) attachNamespace("kioto")
  att <- .Call(kio_channel_attach, token)
  ch <- att[[1L]]
  expr <- att[[2L]]
  .Call(kio_channel_ready_set, ch)
  env <- new.env(parent = globalenv())
  env[["ch"]] <- ch
  status <- 0L
  tryCatch(
    eval(expr, envir = env),
    error = function(e) {
      cat("kioto peer error: ", conditionMessage(e), "\n", sep = "",
          file = stderr())
      status <<- 1L
    },
    interrupt = function(e) status <<- 2L
  )
  .Call(kio_channel_close_signal, ch)
  quit(save = "no", status = status)
}
