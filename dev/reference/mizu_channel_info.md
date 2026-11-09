# Inspect a Channel

A read-only snapshot of the channel region and the handle-local
counters: ring geometry and watermarks, the free-list and open caches,
and the notify signals sent to an armed async consumer.

## Usage

``` r
mizu_channel_info(ch)
```

## Arguments

- ch:

  a channel handle from
  [`mizu_channel()`](https://shikokuchuo.net/mizu/dev/reference/mizu_channel.md)
  (or the `ch` binding inside a peer expression).

## Value

A list with elements `name` (the platform region name, kept for
debugging), `side`, `capacity`, `slot_size`, `arena_size`, `inline_max`,
`spin`, `ready`, `closed`, `peer_pid`, `tx_sent`, `tx_published`,
`tx_consumed`, `rx_consumed`, `rx_published`, `fl_entries`, `fl_hits`,
`open_hits`, `open_misses`, `ledger_entries`, `zc_open_hits`,
`zc_open_misses`, and `notify_sends`.

## Examples

``` r
ch <- mizu_channel(quote(mizu_recv(ch)))
mizu_channel_info(ch)
#> $name
#> [1] "/mizu_1a5c_30477db0"
#> 
#> $side
#> [1] "host"
#> 
#> $capacity
#> [1] 16384
#> 
#> $slot_size
#> [1] 256
#> 
#> $arena_size
#> [1] 4194304
#> 
#> $inline_max
#> [1] 240
#> 
#> $spin
#> [1] FALSE
#> 
#> $ready
#> [1] TRUE
#> 
#> $closed
#> [1] 0
#> 
#> $peer_pid
#> [1] 7597
#> 
#> $tx_sent
#> [1] 0
#> 
#> $tx_published
#> [1] 0
#> 
#> $tx_consumed
#> [1] 0
#> 
#> $rx_consumed
#> [1] 0
#> 
#> $rx_published
#> [1] 0
#> 
#> $fl_entries
#> [1] 0
#> 
#> $fl_hits
#> [1] 0
#> 
#> $open_hits
#> [1] 0
#> 
#> $open_misses
#> [1] 0
#> 
#> $ledger_entries
#> [1] 0
#> 
#> $zc_open_hits
#> [1] 0
#> 
#> $zc_open_misses
#> [1] 0
#> 
#> $notify_sends
#> [1] 0
#> 
mizu_close(ch)
```
