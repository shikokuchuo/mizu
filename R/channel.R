#' Create a Channel and Spawn Its Peer
#'
#' Creates a shared-memory SPSC channel — one lock-free ring per direction —
#' and spawns a child R process connected to its other end. The channel is
#' bidirectional after spawn: the returned handle produces on the host-to-peer
#' ring and consumes the peer-to-host ring. Setup is one-sided: only the
#' region name's suffix crosses the process boundary, as a command-line
#' argument.
#'
#' `expr` is a quoted expression, not a closure — it captures nothing. The
#' peer evaluates it in a fresh environment whose parent is the child's
#' global environment, with `ch` (the peer-side channel handle) as the only
#' binding mov provides; data crosses the ring, and packages are loaded by
#' the expression itself. When the expression returns (or errors — the
#' condition message is written to the child's stderr), the peer signals an
#' orderly close and exits.
#'
#' Payload contents interoperate transparently with mori: a `mori::share()`d
#' object anywhere inside a payload serializes to its short identifier wire
#' form via mori's own hooks and maps zero-copy on the other side.
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
#' @param launcher `NULL` for the default launcher (`system2(Rscript, ...)`
#'   with the host's `.libPaths()` propagated via `R_LIBS`), or a
#'   `function(suffix)` that arranges for an R process to eventually call
#'   `mov:::peer_main(suffix)` — such a launcher must arrange library paths
#'   itself.
#' @param stdout,stderr forwarded to [system2()] by the default launcher;
#'   the default `""` sends peer output (including its error epilogue) to
#'   the host's console. Ignored when `launcher` is supplied.
#' @param liveness_dir directory for the channel's two liveness lock files;
#'   recorded in the region so both sides use the same files. Defaults to
#'   [tempdir()].
#' @param startup_timeout seconds to wait for the peer to attach and signal
#'   ready before giving up and releasing the channel.
#'
#' @return A channel handle (class `"mov_channel"`). Handles are
#'   process-private and do not survive `fork()`.
#'
#' @examples
#' \dontrun{
#' ch <- mov_channel(quote(
#'   repeat {
#'     x <- mov_recv(ch, timeout = 30)
#'     if (inherits(x, "mov_condition")) break
#'     mov_send(ch, x)
#'     mov_flush(ch)
#'   }
#' ))
#' mov_send(ch, 42L)
#' mov_flush(ch)
#' mov_recv(ch, timeout = 5)
#' mov_close(ch)
#' }
#'
#' @export
mov_channel <- function(expr, capacity = 16384L, slot_size = 256L,
                        arena_size = 4194304, spin = FALSE, launcher = NULL,
                        stdout = "", stderr = "", liveness_dir = tempdir(),
                        startup_timeout = 5) {
  ch <- .Call(mov_channel_create, expr, capacity, slot_size, arena_size,
              liveness_dir, spin)
  suffix <- .Call(mov_channel_suffix, ch)
  if (is.null(launcher))
    spawn_peer(suffix, stdout = stdout, stderr = stderr)
  else
    launcher(suffix)
  if (!.Call(mov_channel_ready_wait, ch, startup_timeout)) {
    .Call(mov_channel_destroy, ch)
    stop("mov: child failed to attach within ", format(startup_timeout),
         " seconds", call. = FALSE)
  }
  ch
}

#' Send and Receive over a Channel
#'
#' `mov_send()` stages a message at the producer-local tail; it becomes
#' visible to the peer only on `mov_flush()`. There is no auto-flush: flush
#' after every send for minimum latency, or every N sends for throughput —
#' single-message latency is a property of caller code, not ring internals.
#' `mov_recv()` returns the next message, blocking up to `timeout` seconds.
#'
#' Sends never block for ring space and receives surface every terminal
#' state as a class-tagged sentinel rather than an error (dispatch with
#' `inherits(x, "mov_condition")`, or on the specific classes):
#'
#' * `mov_full` — the ring is full (send); back off, flush, or drop.
#' * `mov_timeout` — no message within `timeout` (recv).
#' * `mov_closed` — the other side closed the channel. recv drains all
#'   published messages before reporting this.
#' * `mov_peer_gone` — the peer died without closing (verdict from the
#'   kernel-released liveness lock, detection at OS death-notification
#'   latency). recv likewise drains first: a dead peer's published messages
#'   are complete and valid. Sticky once returned.
#'
#' `NULL` is a legal payload; sentinels are identifiable by class alone.
#' Attribute-free non-ALTREP atomic vectors that fit the inline budget ride
#' a serialization-free fast path with a byte-identical round-trip; anything
#' else is R-serialized (mori-shared objects reduce to identifier wire forms
#' via mori's hooks).
#'
#' @param ch a channel handle from [mov_channel()] (or the `ch` binding
#'   inside a peer expression).
#' @param x the payload: any R object.
#' @param timeout seconds to wait before returning the `mov_timeout`
#'   sentinel; `Inf` (the default) waits indefinitely, `0` polls. Ctrl-C
#'   remains responsive while waiting.
#'
#' @return `mov_send()` returns `TRUE` (invisibly) on success, else a
#'   sentinel. `mov_flush()` returns `NULL` invisibly. `mov_recv()` returns
#'   the received payload or a sentinel.
#'
#' @examples
#' \dontrun{
#' ch <- mov_channel(quote(mov_send(ch, mov_recv(ch)) && mov_flush(ch)))
#' mov_send(ch, list(1, "a"))
#' mov_flush(ch)
#' mov_recv(ch, timeout = 5)
#' mov_close(ch)
#' }
#'
#' @export
mov_send <- function(ch, x) invisible(.Call(mov_channel_send, ch, x))

#' @rdname mov_send
#' @export
mov_flush <- function(ch) invisible(.Call(mov_channel_flush, ch))

#' @rdname mov_send
#' @export
mov_recv <- function(ch, timeout = Inf) .Call(mov_channel_recv, ch, timeout)

#' Batched Send and Receive
#'
#' At target rates the R call boundary is a first-order cost.
#' `mov_send_batch()` stages a list of payloads and flushes once under a
#' single `.Call`; `mov_recv_batch()` drains up to `n` messages under a
#' single park cycle and a single batched head publication.
#'
#' @inheritParams mov_send
#' @param xs a list of payloads.
#' @param n maximum number of messages to return.
#'
#' @return `mov_send_batch()` returns the number of messages accepted: less
#'   than `length(xs)` when the ring filled or the channel closed midway
#'   (send the next element with [mov_send()] to learn which).
#'   `mov_recv_batch()` waits for the first message like [mov_recv()] —
#'   returning its sentinels on timeout, close, or peer death — then
#'   returns a list of between 1 and `n` already-published messages without
#'   waiting further.
#'
#' @export
mov_send_batch <- function(ch, xs) .Call(mov_channel_send_batch, ch, xs)

#' @rdname mov_send_batch
#' @export
mov_recv_batch <- function(ch, n = 256L, timeout = Inf)
  .Call(mov_channel_recv_batch, ch, n, timeout)

#' Close a Channel
#'
#' Orderly shutdown: flushes staged writes, signals close to the peer, and
#' waits up to `timeout` seconds for the peer's own close (or its death) —
#' the rendezvous that makes releasing sent-payload pins safe, since the
#' peer sets its bit only after it has finished draining. On rendezvous all
#' resources are released and the region name unlinked; the handle is dead
#' afterwards (closing it again is a no-op). On timeout the handle stays
#' usable and resources release when it is garbage collected, re-running the
#' same rendezvous check.
#'
#' Once either side has signalled close, sends on both sides return the
#' `mov_closed` sentinel and receives drain remaining messages before doing
#' the same.
#'
#' @inheritParams mov_send
#' @param timeout seconds to wait for the peer's close.
#'
#' @return Invisibly, `TRUE` on rendezvous, `FALSE` on timeout (with a
#'   warning).
#'
#' @export
mov_close <- function(ch, timeout = 5) {
  ok <- .Call(mov_channel_close, ch, timeout)
  if (!ok)
    warning("mov: close timed out waiting for the peer; resources release ",
            "when the handle is garbage collected", call. = FALSE)
  invisible(ok)
}

#' Probe Peer Liveness
#'
#' An explicit probe for supervisors: reports whether the peer process holds
#' its liveness lock (`~1` microsecond, no waiting). [mov_recv()] and
#' [mov_send()] surface peer death automatically as `mov_peer_gone`; this is
#' for callers that want to ask without touching the rings. A peer that has
#' closed the channel but is still running reads as alive.
#'
#' @inheritParams mov_send
#'
#' @return `TRUE` while the peer process is alive, `FALSE` once it has died
#'   (at which point the survivor has unlinked the channel's names).
#'
#' @export
mov_alive <- function(ch) .Call(mov_channel_alive, ch)

# Peer entry point: invoked as `Rscript -e 'mov:::peer_main("<suffix>")'` by
# the launcher. Rebuilds the region name from the compiled-in prefix plus the
# argv suffix, attaches writable, validates the preamble, takes its liveness
# lock, points its death listener at the host, and materializes the staged
# expression *before* signalling ready — the host's mov_channel frame holds
# the expression (and every region its identifiers name) alive exactly until
# then. The epilogue is the peer half of the close protocol.
peer_main <- function(suffix) {
  if (!"package:mov" %in% search()) attachNamespace("mov")
  att <- .Call(mov_channel_attach, suffix)
  ch <- att[[1L]]
  expr <- att[[2L]]
  .Call(mov_channel_ready_set, ch)
  env <- new.env(parent = globalenv())
  env[["ch"]] <- ch
  status <- 0L
  tryCatch(
    eval(expr, envir = env),
    error = function(e) {
      cat("mov peer error: ", conditionMessage(e), "\n", sep = "",
          file = stderr())
      status <<- 1L
    },
    interrupt = function(e) status <<- 2L
  )
  .Call(mov_channel_close_signal, ch)
  quit(save = "no", status = status)
}
