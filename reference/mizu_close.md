# Close a Channel

Orderly shutdown. Signals close to the peer, then waits up to `timeout`
seconds for the peer's close (or its death). Waiting for the peer is
what makes it safe to release the resources held by sent payloads: the
peer confirms only after it has finished reading its messages. On that
confirmation, all resources are released and the region name is deleted.
The handle is dead afterwards, and closing it again is a no-op. On
timeout the handle stays usable, and the resources release at garbage
collection, which runs the same check again.

## Usage

``` r
mizu_close(ch, timeout = 5)
```

## Arguments

- ch:

  a channel handle from
  [`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md)
  (or the `ch` binding inside a peer expression).

- timeout:

  seconds to wait for the close of the peer.

## Value

Invisibly, `TRUE` on a clean mutual close, `FALSE` on timeout (with a
warning).

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
