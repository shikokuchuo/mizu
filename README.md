
<!-- README.md is generated from README.Rmd. Please edit that file -->

# rei れい

<!-- badges: start -->

[![R-CMD-check](https://github.com/shikokuchuo/rei/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/shikokuchuo/rei/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

      ________
     /\       \
    /  \  rei \
    \  /       /
     \/_______/

Parallel computation and data exchange between R processes on the same machine.
Lock-free channels and work-stealing task pools over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).
A channel is a two-way message link between an R session and a helper process that it spawns.
A pool is a set of worker processes that divide submitted tasks among themselves.
In both, one process writes data and the other reads it in place — never copied through a socket, pipe, or file.
The hot path stays in user space: single-producer single-consumer rings with batched publication, spin-then-park waiting, and event-driven peer-death detection.

R evaluates code on a single thread, so parallelism in R means multiple processes.
rei makes the communication between these processes cheap enough that you can divide work at granularities usually reserved for threads.
rei complements [mirai](https://mirai.r-lib.org), the general solution for parallel and distributed computing in R.
Payload contents interoperate transparently with [mori](https://shikokuchuo.net/mori/) shared objects.

## Installation

Install the development version from GitHub:

``` r
pak::pak("shikokuchuo/rei")
```

## Channels

The essential function is `rei_channel()`.
It creates a two-way shared-memory channel — one lock-free ring per direction — and spawns a child R process connected to the other end.
The child evaluates a quoted expression with `ch` bound to its side of the channel.
All data crosses the rings, not the process boundary.

``` r
library(rei)

ch <- rei_channel(quote(
  repeat {
    x <- rei_recv(ch, timeout = 30)
    if (inherits(x, "rei_sentinel")) break
    rei_send(ch, x * 2)
  }
))

rei_send(ch, 21)
rei_recv(ch, timeout = 5)
#> [1] 42
rei_close(ch)
```

## Task pools

`rei_pool()` spawns a pool of worker processes with work-stealing deques and no dispatcher in the loop.
`rei_submit()` captures an expression together with the values it needs and returns a task handle immediately.
`rei_collect()` waits for the result of that task:

``` r
p <- rei_pool(n_workers = 4L)

t <- rei_submit(p, sum(x) + y, x = 1:10, y = 100)
rei_collect(t)
#> [1] 155

rei_pool_stop(p)
```

## Reference vignette

`vignette("reference", package = "rei")` covers the full surface: batch operations and sentinel values, nested tasks and `rei_map()`, growing, attaching to, and observing a running pool, benchmarks against mirai, sizing `/dev/shm` on Linux, and crash semantics.
