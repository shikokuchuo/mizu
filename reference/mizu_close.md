# Close a Channel

Orderly shutdown. Signals close to the peer, then waits up to `timeout`
seconds for the close of the peer (or its death). This rendezvous makes
it safe to release the sent-payload pins: the peer sets its bit only
after it finishes draining. On rendezvous, all resources are released
and the region name is unlinked. The handle is dead afterwards, and
closing it again is a no-op. On timeout the handle stays usable, and the
resources release at garbage collection, which runs the same rendezvous
check again.

## Usage

``` r
mizu_close(ch, timeout = 5)
```

## Arguments

- ch:

  a channel handle from
  [`mizu_channel()`](https://shikokuchuo.github.io/mizu/reference/mizu_channel.md)
  (or the `ch` binding inside a peer expression).

- timeout:

  seconds to wait for the close of the peer.

## Value

Invisibly, `TRUE` on rendezvous, `FALSE` on timeout (with a warning).

## Details

After either side signals close, sends on both sides return the
`mizu_closed` sentinel, and receives drain the remaining messages before
they return the same.

## Examples

``` r
ch <- mizu_channel(quote(mizu_send(ch, "done")))
mizu_recv(ch, timeout = 5)
#> [1] "done"
mizu_close(ch)
```
