# Create a Channel and Spawn Its Peer

Creates a shared-memory SPSC channel — one lock-free ring per direction
— and spawns a child R process connected to the other end. The channel
is two-way after the spawn: the returned handle produces on the
host-to-peer ring and consumes the peer-to-host ring. Setup is
one-sided: only a short join token (the suffix of the region name)
crosses the process boundary, as a command-line argument.

## Usage

``` r
mizu_channel(
  expr,
  capacity = 16384L,
  slot_size = 256L,
  arena_size = 4194304,
  spin = FALSE,
  launcher = mizu_launcher(),
  startup_timeout = 30
)
```

## Arguments

- expr:

  a quoted expression (for example `quote({ ... })`), evaluated in the
  peer process with `ch` bound to the peer-side channel handle. A single
  character string is instead UTF-8 source text in the peer's language,
  for a non-R peer spawned by a custom `launcher` (an R peer parses and
  evaluates it as R source).

- capacity:

  slots per ring. A power of two between 2 and 2^24.

- slot_size:

  bytes per slot. A power of two between 64 and 2^20. The inline payload
  budget is `slot_size - 16`. Payloads that serialize larger spill to
  the arena, and past it to a fresh region.

- arena_size:

  spill-arena bytes per direction. A multiple of 64, or 0 to disable
  (every spill then creates a region). This bounds the in-flight spill.
  An undersized arena degrades to region-create fallbacks, never to
  errors.

- spin:

  opts the channel into pure-spin waiting: consumers never park and
  producers skip the wake check on publish. Use this only when the
  consumer never yields. If a spin-mode consumer parks, the producer
  never wakes it.

- launcher:

  a `function(token)` that spawns the peer process. For an R peer, it
  arranges for a process to call `mizu:::peer_main(token)`. The default
  [`mizu_launcher()`](https://shikokuchuo.github.io/mizu/reference/mizu_launcher.md)
  spawns `Rscript` and propagates the
  [`.libPaths()`](https://rdrr.io/r/base/libPaths.html) of the host. Its
  `stdout` and `stderr` arguments direct the peer output, including the
  error epilogue. A custom launcher must arrange the library paths
  itself. For a peer in another language (a source-string `expr`), it
  spawns a program that attaches with `token` and speaks the wire
  protocol, such as `python3 -m pymizu.child` for a Python peer —
  [`mizu_py_launcher()`](https://shikokuchuo.github.io/mizu/reference/mizu_py_launcher.md)
  is the ready-made launcher for that case.

- startup_timeout:

  seconds to wait for the peer to attach and signal ready. On expiry,
  mizu releases the channel and raises `mizu_error_startup` (see
  [mizu_error](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)).

## Value

A channel handle (class `"mizu_channel"`). Handles are process-private
and do not survive `fork()`.

## Details

`expr` is a quoted expression, not a closure. It captures nothing, and
unlike
[`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
mizu does not capture it for you: pass it pre-quoted. The peer evaluates
it in a fresh environment whose parent is the global environment of the
child. `ch` (the peer-side channel handle) is the only binding that mizu
provides. Data crosses the ring, and the expression itself loads any
packages it needs. When the expression returns or errors, the peer
signals an orderly close and exits. An error message goes to the stderr
of the child.

Payload contents interoperate transparently with mori. A
[`mori::share()`](https://rdrr.io/pkg/mori/man/share.html)d object
anywhere inside a payload serializes to its short identifier wire form
through the mori hooks, and maps zero-copy on the other side.

The two liveness lock files of the channel (the death-detection verdict)
live in a per-platform directory chosen at create time. This is
`/dev/shm` on Linux, and the per-user temporary directory on macOS and
Windows. The chosen path is recorded in the region, so both sides use
the same files. The environment variable `MIZU_LIVENESS_DIR`, read in
the creating process, overrides the default.

## Examples

``` r
ch <- mizu_channel(quote(
  repeat {
    x <- mizu_recv(ch, timeout = 30)
    if (inherits(x, "mizu_sentinel")) break
    mizu_send(ch, x)
  }
))
mizu_send(ch, 42L)
mizu_recv(ch, timeout = 5)
#> [1] 42
mizu_close(ch)
```
