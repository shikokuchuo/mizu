# Channels

The main function is
[`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md).
It creates a two-way shared-memory channel — one lock-free ring per
direction — and spawns a child R process connected to the other end. The
child evaluates a quoted expression with `ch` bound to its side of the
channel. All data crosses the rings, not the process boundary.

``` r

library(mizu)

ch <- mizu_channel(quote(
  repeat {
    x <- mizu_recv(ch, timeout = 30)
    if (inherits(x, "mizu_sentinel")) break
    mizu_send(ch, x * 2)
  }
))

mizu_send(ch, 21)
mizu_recv(ch, timeout = 5)
#> [1] 42
```

[`mizu_send()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)
publishes a message to the peer, visible the moment the call returns.
[`mizu_recv()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)
returns the next message.
[`mizu_send_batch()`](https://shikokuchuo.net/mizu/reference/mizu_send_batch.md)
and
[`mizu_recv_batch()`](https://shikokuchuo.net/mizu/reference/mizu_send_batch.md)
move a whole list of messages in one call. Use them at rates where the
per-call overhead of R itself starts to matter.

Outcomes that end a conversation — ring full, timeout, orderly close,
peer death — come back as class-tagged sentinel values, not errors. A
receive loop tests for them with `inherits(x, "mizu_sentinel")`, or with
the specific classes `mizu_full`, `mizu_timeout`, `mizu_closed`,
`mizu_peer_gone`.
[`mizu_is_sentinel()`](https://shikokuchuo.net/mizu/reference/mizu_is_sentinel.md)
is the stricter test: it returns `TRUE` only for the exact sentinel
objects mizu returned. A class test alone cannot tell a terminal state
from a look-alike payload — even a genuine sentinel forwarded as a
message arrives as an ordinary copy. No call needs an error handler. If
the process at the other end dies, receives first drain the messages
that it already published, then report `mizu_peer_gone`.
`mizu_alive(ch)` reports at any time whether the peer still runs,
without touching the rings.

[`mizu_close()`](https://shikokuchuo.net/mizu/reference/mizu_close.md)
performs an orderly shutdown. It waits for the peer to finish reading
its messages before it releases the shared resources:

``` r

mizu_close(ch)
```

If the peer’s code raises an error, the error crosses as data: a receive
returns a `mizu_error_remote` condition object rather than signalling
it.
[`mizu_is_remote_error()`](https://shikokuchuo.net/mizu/reference/mizu_is_remote_error.md)
tests for one,
[`conditionMessage()`](https://rdrr.io/r/base/conditions.html) leads
with the remote type, and
[`mizu_raise()`](https://shikokuchuo.net/mizu/reference/mizu_is_remote_error.md)
signals it in your own session:

``` r

ch <- mizu_channel(quote(stop("boom")))
x <- mizu_recv(ch, timeout = 5)
mizu_is_remote_error(x)
#> [1] TRUE
conditionMessage(x)
#> [1] "simpleError: boom"
mizu_close(ch)
```

The peer also prints an error epilogue to its standard error, which
lands on your console by default. The `launcher` argument customizes how
the peer is spawned: its default
[`mizu_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_launcher.md)
takes `stdout` / `stderr`, so
`launcher = mizu_launcher(stderr = "peer.log")` collects the epilogue in
a file instead — the same launcher spawns the R workers of
[`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md).

## Promises and handlers

For event-driven consumers — a Shiny app, an `httpuv` server, any
program organized around an event loop — the channel waits on a notify
endpoint instead of parking a thread. When the peer publishes, the
endpoint wakes the event loop, and the message is delivered on the main
thread through `later`. There is no polling and no extra thread.
Requires the `later` package, and `promises` for the promise forms.

[`mizu_recv_async()`](https://shikokuchuo.net/mizu/reference/mizu_recv_async.md)
returns a promise (never blocks) that resolves with the next message —
the identical object a blocking
[`mizu_recv()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)
would have returned — and rejects on close, peer death, or its own
`timeout`, as classed error conditions carrying the sentinel class
names.
[`mizu_recv_handler()`](https://shikokuchuo.net/mizu/reference/mizu_recv_async.md)
is the stream form: it installs a function and each burst is delivered
to it from the event loop, with `batch = TRUE` collecting the burst into
one list per call. Resolution is per burst, never per item: a promise
costs tens of microseconds of R work against about a microsecond of
transport, so high volumes belong on the batch handler. Every
main-thread drain is bounded per event-loop turn and re-scheduled, so a
saturated ring cannot starve the loop.

``` r

library(promises)
ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch))))
mizu_recv_async(ch) %...>% print()
mizu_send(ch, 42L)
for (i in 1:4) later::run_now(1) # pump the loop until the resolution lands
#> [1] 42
mizu_close(ch)
```

A channel holds at most one of the two consumer forms at a time:
installing a handler with promises pending, or requesting a promise with
a handler installed, is an error.
