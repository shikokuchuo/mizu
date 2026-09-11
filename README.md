
<!-- README.md is generated from README.Rmd. Please edit that file -->

# rei れい

<!-- badges: start -->

[![R-CMD-check](https://github.com/shikokuchuo/rei/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/shikokuchuo/rei/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

      ________
     /\       \
    /  \  rei  \
    \  /  れい  /
     \/_______/

rei is the R binding to [librei](https://github.com/shikokuchuo/librei), a C library for lock-free shared-memory IPC.

Parallel computation and data exchange between R processes: channels and work-stealing task pools over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).

A channel is a two-way message link between an R session and a helper process that it spawns.
A pool is a set of worker processes that divide submitted tasks among themselves.
In both, one process writes data and the other reads it in place — never copied through a socket, pipe, or file.

The hot path stays in user space: single-producer single-consumer rings with batched publication, spin-then-park waiting, and event-driven peer-death detection.

R evaluates code on a single thread, so parallelism in R means multiple processes.
rei makes the communication between these processes cheap enough that you can divide work at granularities usually reserved for threads.

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

## Parallel map

`rei_map()` maps a function over a vector or list on the pool and returns the results in input order.
The function and data cross into shared memory once; a handful of chunk tasks divide the elements, and each worker self-schedules element ranges off a shared cursor.

``` r
p <- rei_pool(n_workers = 4L)

rei_map(p, 1:5, \(i) i * 2L, .template = integer(1))
#> [1]  2  4  6  8 10

rei_pool_stop(p)
```

## Benchmarks

Headline numbers against mirai (M4 Pro, from `dev/bench/rei-mirai-bench.R`):

| Benchmark | rei | mirai | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.8 µs | 100.5 µs | 126x |
| Pipelined throughput, 1 worker | 833,000 tasks/s | 9,700 tasks/s | 86x |
| 8 MB vector round trip | 400 µs | 18.4 ms | 46x |
| Parallel map of 2,000 ~10 µs tasks, 4 workers | 5.0 ms | 218 ms | 44x |

## Python interop

A channel peer can be a Python process that runs [pyrei](https://github.com/shikokuchuo/pyrei), the Python binding of the same core.
Pass the peer program as a source string (instead of a quoted expression) and set the launcher to `rei_py_launcher()`:

``` r
ch <- rei_channel(
  "
import pyrei
while True:
    x = ch.recv(30)
    if pyrei.is_sentinel(x):
        break
    ch.send(x * 2)
",
  launcher = rei_py_launcher()
)

rei_send(ch, c(1.5, 2.5, 3.5)) # arrives in Python as a float64 NumPy array
rei_recv(ch, timeout = 5) # echoes back as a numeric vector
#> [1] 3 5 7
rei_close(ch)
```

`rei_py_launcher()` needs a `python3` with pyrei installed.
A launcher is one function that takes the join token and spawns the peer process; for a different spawn method, write your own.
The pyrei README shows the reverse direction: a Python host that spawns an R peer with `pyrei.r_launcher()`.

## Reference vignette

`vignette("reference", package = "rei")` covers the full surface: batch operations and sentinel values, nested tasks and `rei_map()`, growing, attaching to, and observing a running pool, Python interop, benchmarks against mirai, sizing `/dev/shm` on Linux, tuning the Linux memory allocator, and crash semantics.

------------------------------------------------------------------------

Please note that this project is released with a [Contributor Code of Conduct](https://github.com/shikokuchuo/rei/blob/main/.github/CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.
