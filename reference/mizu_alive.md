# Check Whether the Channel Peer Is Alive

Reports whether the peer process still holds its liveness lock, in about
a microsecond, without touching the rings.
[`mizu_recv()`](https://shikokuchuo.net/mizu/reference/mizu_send.md) and
[`mizu_send()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)
report peer death automatically as `mizu_peer_gone`. This function asks
directly. A peer that closed the channel but still runs counts as alive.

## Usage

``` r
mizu_alive(ch)
```

## Arguments

- ch:

  a channel handle from
  [`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md)
  (or the `ch` binding inside a peer expression).

## Value

`TRUE` while the peer process is alive, `FALSE` after it dies. By then
the survivor has deleted the channel's shared memory.

## Examples

``` r
ch <- mizu_channel(quote(mizu_recv(ch)))
mizu_alive(ch)
#> [1] TRUE
mizu_close(ch)
```
