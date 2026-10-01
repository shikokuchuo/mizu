# Channels

The essential function is
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
is the stricter test — identity against the four interned singletons: a
class test alone cannot tell a terminal state from a look-alike payload,
and a genuine sentinel forwarded as a message arrives as an ordinary
copy. No call needs an error handler. If the process at the other end
dies, receives first drain the messages that it already published, then
report `mizu_peer_gone`. `mizu_alive(ch)` reports at any time whether
the peer still runs, without touching the rings.

[`mizu_close()`](https://shikokuchuo.net/mizu/reference/mizu_close.md)
performs an orderly shutdown. It waits for the peer to finish draining
before it releases the shared resources:

``` r

mizu_close(ch)
```

If the code of the peer raises, the error crosses as data: a receive
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
lands on your console by default. The `launcher` argument customizes the
spawn: its default
[`mizu_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_launcher.md)
takes `stdout` / `stderr`, so
`launcher = mizu_launcher(stderr = "peer.log")` collects the epilogue in
a file instead — the same factory launches the R workers of
[`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md).
