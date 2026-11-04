# Receive Channel Messages as a Promise or a Stream

`mizu_recv_async()` returns a
[`promises::promise()`](https://rstudio.github.io/promises/reference/promise.html)
immediately, without blocking. The promise resolves with the next
received payload, the same R object a blocking
[`mizu_recv()`](https://shikokuchuo.net/mizu/dev/reference/mizu_send.md)
would have returned, zero-copy ALTREP views included. Chain it with
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
  [`mizu_channel()`](https://shikokuchuo.net/mizu/dev/reference/mizu_channel.md)
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

The promise rejects on `mizu_closed`, `mizu_peer_gone`, and `timeout`
expiry with classed *error* conditions that carry the same class names
as the sentinels, so `inherits(x, "mizu_closed")` works on either form.
Close and peer death wake the event loop directly, so the rejection is
prompt rather than waiting for the timeout.

Several outstanding calls on one channel resolve in FIFO call order, one
message each. No message is lost: messages already in the ring resolve
immediately (still asynchronously, as promises require), and only
messages yet to arrive wait on the event.

`mizu_recv_handler()` is the stream form. It installs `fn` on the
channel, and the event loop delivers each burst of messages to it
without allocating a promise per message. With `batch = TRUE`, each
drained burst is delivered as one list in a single `fn(list)` call,
which is the high-throughput form. With `batch = FALSE`, `fn(value)` is
called once per message. Close and peer death are delivered to `fn` as
the classed error conditions (`mizu_closed`, `mizu_peer_gone`), after
which the handler is removed. A declined foreign payload (see
[`mizu_send()`](https://shikokuchuo.net/mizu/dev/reference/mizu_send.md))
is delivered as its `mizu_error_python_payload` condition and the
handler continues. An error raised by `fn` removes the handler and
propagates at the event loop. `mizu_recv_handler(ch, NULL)` removes the
handler.

A channel message belongs to whoever takes it first. A channel can have
only one of the two consumer forms at a time: installing a handler while
promises are pending, or requesting a promise while a handler is
installed, is an error. A blocking
[`mizu_recv()`](https://shikokuchuo.net/mizu/dev/reference/mizu_send.md)
mixed with either consumer form consumes in arrival order, so the
blocking call may take a message that a promise or the handler was
waiting on. A close or peer death that it observes is observed by the
consumer too, because both are channel state, re-read on every receive
and never consumed. (A task result instead belongs to exactly one
consumer; see
[`mizu_collect_all_async()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_all_async.md).)

Every main-thread drain is bounded per event-loop turn and re-scheduled,
so a saturated ring starves neither Shiny nor
[`later::run_now()`](https://later.r-lib.org/reference/run_now.html).

Requires the later package, and promises for `mizu_recv_async()`.

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
