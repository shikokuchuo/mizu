# Introduction to mizu

mizu provides parallel computation and data exchange between R processes
on the same machine: lock-free channels and work-stealing task pools
over POSIX shared memory (Linux, macOS) or Win32 file mappings
(Windows).

A channel is a two-way message link between an R session and a helper
process that it spawns. A pool is a set of worker processes that divide
submitted tasks among themselves.

In both, one process writes data and the other reads it in place — never
copied through a socket, pipe, or file.

``` r

library(mizu)

p <- mizu_pool(n_workers = 4L)
t <- mizu_submit(p, sum(x) + y, x = 1:10, y = 100)
mizu_collect(t)
#> [1] 155
mizu_map(p, 1:5, \(i) i * 2L, .template = integer(1))
#> [1]  2  4  6  8 10
mizu_pool_stop(p)
```

## The articles

- [Channels](https://shikokuchuo.net/mizu/articles/channels.md) —
  two-way message links between R processes: send and receive, batch
  verbs, sentinel values, remote errors.
- [Task pools](https://shikokuchuo.net/mizu/articles/pools.md) —
  work-stealing worker pools: submit and collect, nested tasks, waiting
  on several tasks, sizing and observing a pool.
- [Parallel map](https://shikokuchuo.net/mizu/articles/map.md) —
  [`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md):
  staged-once mapping over a pool, templates, reproducible random
  numbers, prepared maps.
- [Python interop](https://shikokuchuo.net/mizu/articles/interop.md) —
  channels and pools shared with Python processes through pymizu,
  including the cross-language map.
- [Benchmarks](https://shikokuchuo.net/mizu/articles/benchmarks.md) —
  head-to-head measurements against mirai.
- [Operations](https://shikokuchuo.net/mizu/articles/operations.md) —
  sizing `/dev/shm` on Linux, the Linux memory allocator, and crash
  semantics.
