# Probe Peer Liveness

An explicit probe for supervisors. Reports whether the peer process
holds its liveness lock, in about 1 microsecond with no waiting.
[`mizu_recv()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
and
[`mizu_send()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
surface peer death automatically as `mizu_peer_gone`. Use this probe to
ask without touching the rings. A peer that closed the channel but still
runs reads as alive.

## Usage

``` r
mizu_alive(ch)
```

## Arguments

- ch:

  a channel handle from
  [`mizu_channel()`](https://shikokuchuo.github.io/mizu/reference/mizu_channel.md)
  (or the `ch` binding inside a peer expression).

## Value

`TRUE` while the peer process is alive, `FALSE` after it dies. At that
point the survivor unlinked the names of the channel.

## Examples

``` r
ch <- mizu_channel(quote(mizu_recv(ch)))
mizu_alive(ch)
#> [1] TRUE
mizu_close(ch)
```
