# Batched Send and Receive

At target rates the R call boundary is a first-order cost.
`mizu_send_batch()` moves a list of payloads in a single `.Call` and
publishes them to the peer in one batched tail store.
`mizu_recv_batch()` drains up to `n` messages in a single park cycle and
a single batched head publication.

## Usage

``` r
mizu_send_batch(ch, xs)

mizu_recv_batch(ch, n = 256L, timeout = Inf)
```

## Arguments

- ch:

  a channel handle from
  [`mizu_channel()`](https://shikokuchuo.github.io/mizu/reference/mizu_channel.md)
  (or the `ch` binding inside a peer expression).

- xs:

  a list of payloads.

- n:

  maximum number of messages to return.

- timeout:

  seconds to wait for a message before returning the `mizu_timeout`
  sentinel. `Inf` (the default) waits indefinitely; `0` does not wait.

## Value

`mizu_send_batch()` returns the number of messages accepted. This is
less than `length(xs)` when the ring filled or the channel closed
midway. Send the next element with
[`mizu_send()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
to learn which. `mizu_recv_batch()` waits for the first message like
[`mizu_recv()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
and returns its sentinels on timeout, close, or peer death. It then
returns a list of 1 to `n` already-published messages without waiting
further. A batch that reaches a message it cannot read (a foreign Python
payload) returns what it read before it, and the failure surfaces on the
next receive, which raises like
[`mizu_recv()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
— as does the batch itself when the first message is the one declined.

## Examples

``` r
ch <- mizu_channel(quote(mizu_send_batch(ch, mizu_recv_batch(ch, 3L))))
mizu_send_batch(ch, list(1, 2, 3))
#> [1] 3
mizu_recv_batch(ch, 3L, timeout = 5)
#> [[1]]
#> [1] 1
#> 
#> [[2]]
#> [1] 2
#> 
#> [[3]]
#> [1] 3
#> 
mizu_close(ch)
```
