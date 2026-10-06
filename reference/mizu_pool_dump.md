# Dump the Distributed State of a Pool

A read-only debugging snapshot of the entire pool region, one level
deeper than
[`mizu_pool_status()`](https://shikokuchuo.net/mizu/reference/mizu_pool_status.md).
It shows per-slot registry detail, the park, ready, and back-pressure
masks unpacked per slot, and every occupied result slot. State is
distributed across processes and execution is non-deterministic, so
reach for this tool first when a pool hangs. The scan takes no locks and
can race in-flight transitions. Each field is a consistent single read.
The rows need not be mutually consistent.

## Usage

``` r
mizu_pool_dump(pool)
```

## Arguments

- pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md).

## Value

A list with elements `name`, `shutdown`, `workers` (data frame: slot,
status, pid, park_state, parked, deque `top` and `bottom`, and the
in-flight result slot), `submitters` (data frame: slot, status, pid,
result-slot subrange, queued injection entries, ready and full-waiter
mask bits), `tasks` (data frame of occupied result slots: slot, status,
sequence, executing worker, parked waiter), `help`, and `local`. `help`
is the help-wanted doorbell: `TRUE` while injection entries are queued
with every worker busy, awaiting pickup by a map runner at its next
batch transition or a worker helping out of a nested
[`mizu_collect()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
(the `rehome` event of
[`mizu_pool_trace()`](https://shikokuchuo.net/mizu/reference/mizu_pool_trace.md)).
`local` is the process-private machinery of this handle: the occupancy
of the producer free list (`fl_entries`, `fl_bytes`), its reuse count
(`fl_hits`), the `open_hits` and `open_misses` of the consumer mapping
cache, and `collect_parks` (how often a collect on this handle parked
waiting for a result).

## Examples

``` r
p <- mizu_pool()
t <- mizu_submit(p, runif(1))
mizu_pool_dump(p)
#> $name
#> [1] "/mizu_1a00_d012152a"
#> 
#> $shutdown
#> [1] FALSE
#> 
#> $workers
#>   slot status  pid park_state parked top bottom in_flight
#> 1    0   live 7549     parked   TRUE   0      0        -1
#> 
#> $submitters
#>   slot status  pid rs_start rs_count queued ready full_waiter
#> 1    0   live 6656        0      512      0 FALSE       FALSE
#> 2    1   free    0        0        0      0 FALSE       FALSE
#> 3    2   free    0        0        0      0 FALSE       FALSE
#> 4    3   free    0        0        0      0 FALSE       FALSE
#> 5    4   free    0        0        0      0 FALSE       FALSE
#> 6    5   free    0        0        0      0 FALSE       FALSE
#> 7    6   free    0        0        0      0 FALSE       FALSE
#> 8    7   free    0        0        0      0 FALSE       FALSE
#> 
#> $tasks
#>   slot status sequence worker waiter
#> 1    0     ok        1      0     -1
#> 
#> $local
#> $local$fl_entries
#> [1] 0
#> 
#> $local$fl_bytes
#> [1] 0
#> 
#> $local$fl_hits
#> [1] 0
#> 
#> $local$open_hits
#> [1] 0
#> 
#> $local$open_misses
#> [1] 0
#> 
#> $local$collect_parks
#> [1] 0
#> 
#> 
#> $help
#> [1] FALSE
#> 
#> $language
#> [1] "R"
#> 
#> $capabilities
#> [1] 31
#> 
mizu_collect(t)
#> [1] 0.2471427
mizu_pool_stop(p)
```
