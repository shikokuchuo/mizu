# Receive Channel Messages as a Promise or a Stream

`mizu_recv_async()` returns immediately with a
[`promises::promise()`](https://rstudio.github.io/promises/reference/promise.html)
— it never blocks — resolving with the next received payload: the
identical R object a blocking
[`mizu_recv()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)
would have returned, zero-copy ALTREP views included. Chain with
[`promises::then()`](https://rstudio.github.io/promises/reference/then.html)
or the promise pipe `%...>%`.

## Usage

``` r
mizu_recv_async(ch, timeout = Inf)

mizu_recv_handler(ch, fn, batch = FALSE)

# S3 method for class 'mizu_channel'
as.promise(x)

# S3 method for class 'mizu_channel'
is.promising(x)
```

## Arguments

- ch:

  a channel handle from
  [`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md)
  (or the `ch` binding inside a peer expression). Arming a `spin`
  channel errors: its publish path skips the wake checkpoint by design.

- timeout:

  seconds after which the promise rejects with the `mizu_timeout`
  classed error. `Inf` (the default) waits indefinitely.

- fn:

  a `function(x)` called with each received message (or, with
  `batch = TRUE`, each burst as one list), and finally with a terminal
  condition. `NULL` removes the handler.

- batch:

  deliver each drained burst to `fn` as one list in a single call,
  instead of one call per message.

- x:

  for the `as.promise` method: a channel handle, treated as
  `mizu_recv_async(x)`.

## Value

`mizu_recv_async()`: a
[`promises::promise()`](https://rstudio.github.io/promises/reference/promise.html)
resolving with the next message. `mizu_recv_handler()`: the channel
handle, invisibly.

## Details

The promise rejects on `mizu_closed`, `mizu_peer_gone`, and on `timeout`
expiry — as classed *error* conditions carrying the same class names as
the sentinels, so `inherits(x, "mizu_closed")` works on either form.
Close and peer death reach the drain through the core's terminal wakes,
so the rejection is prompt, never timeout-driven.

Several outstanding calls on one channel resolve in FIFO call order, one
message each. Arming is loss-free: messages already in the ring resolve
from the arm's synchronous drain (delivered asynchronously at top level,
per promises discipline); only genuinely pending entries ride the watch.

`mizu_recv_handler()` is the stream form: it installs `fn` on the
channel and each burst of messages is delivered to it from the event
loop — no promise is allocated per message. With `batch = TRUE` each
drained burst is delivered as one list in a single `fn(list)` call (the
throughput form: one R call per burst); with `batch = FALSE`,
`fn(value)` is called per message. Transport terminals are delivered to
`fn` as the classed error conditions (`mizu_closed`, `mizu_peer_gone`),
after which the handler is removed. A declined foreign payload (see
[`mizu_send()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)) is
delivered as its `mizu_error_python_payload` condition and the handler
continues. An error raised by `fn` removes the handler and propagates at
the event loop. `mizu_recv_handler(ch, NULL)` removes the handler.

A channel holds at most one of the two consumer forms at a time:
installing a handler with promises pending, or requesting a promise with
a handler installed, is an error. Mixing a blocking
[`mizu_recv()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)
with pending promises consumes in arrival order — the blocking call may
take a message a promise was waiting on.

Every main-thread drain is bounded per event-loop quantum and
re-scheduled, so a saturated ring starves neither Shiny nor
[`later::run_now()`](https://later.r-lib.org/reference/run_now.html).

Requires the later package (and promises for `mizu_recv_async()`), both
of which must be installed.

## Examples

``` r
if (FALSE) { # interactive() && requireNamespace("promises", quietly = TRUE)
library(promises)
ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch))))
mizu_recv_async(ch) %...>% print()
mizu_send(ch, 42L)
later::run_now()
mizu_close(ch)
}
```
