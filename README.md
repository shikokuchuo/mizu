
<!-- README.md is generated from README.Rmd. Please edit that file -->

# mizu 水

<!-- badges: start -->

[![R-CMD-check](https://github.com/shikokuchuo/mizu/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/shikokuchuo/mizu/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

      ________
     /\       \
    /  \  mizu \
    \  /   水  /
     \/_______/

mizu makes communication between R processes cheap enough to divide work at granularities usually reserved for threads.

Channels and work-stealing task pools run over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).

A channel is a two-way message link between an R session and a helper process that it spawns.
A pool is a set of worker processes that divide submitted tasks among themselves.
In both, one process writes data and the other reads it in place — never copied through a socket, pipe, or file.

The hot path stays in user space: single-producer single-consumer rings with batched publication, spin-then-park waiting, and event-driven peer-death detection.

mizu is built on [libmizu](https://github.com/shikokuchuo/libmizu), a C library for lock-free shared-memory IPC.
[pymizu](https://github.com/shikokuchuo/pymizu) binds the same core for Python, so R and Python can talk to each other.

Pre-release.
The API is not stable and may change at any time before a release.

## Installation

Install the development version from GitHub:

``` r
pak::pak("shikokuchuo/mizu")
```

## Channels

The essential function is `mizu_channel()`.
It creates a two-way shared-memory channel — one lock-free ring per direction — and spawns a child R process connected to the other end.
The child evaluates a quoted expression with `ch` bound to its side of the channel.
All data crosses the rings, not the process boundary.

``` r
library(mizu)

ch <- mizu_channel(quote(
  repeat {
    x <- mizu_recv(ch, timeout = 30)
    if (inherits(x, "mizu_sentinel")) break
    mizu_send(ch, x * 2)
  }
))

mizu_send(ch, 21)
mizu_recv(ch, timeout = 5)
#> [1] 42
mizu_close(ch)
```

## Task pools

`mizu_pool()` spawns a pool of worker processes with work-stealing deques and no dispatcher in the loop.
`mizu_submit()` captures an expression together with the values it needs and returns a task handle immediately.
`mizu_collect()` waits for the result of that task:

``` r
p <- mizu_pool(n_workers = 4L)

t <- mizu_submit(p, sum(x) + y, x = 1:10, y = 100)
mizu_collect(t)
#> [1] 155

mizu_pool_stop(p)
```

## Parallel map

`mizu_map()` maps a function over a vector or list on the pool and returns the results in input order.
The function and data cross into shared memory once; a handful of chunk tasks divide the elements, and each worker self-schedules element ranges off a shared cursor.

``` r
p <- mizu_pool(n_workers = 4L)

mizu_map(p, 1:5, \(i) i * 2L, .template = integer(1))
#> [1]  2  4  6  8 10

mizu_pool_stop(p)
```

## Benchmarks

Communication overhead against mirai on one machine (M4 Pro, from `dev/bench/mizu-mirai-bench.R`):

| Benchmark | mizu | mirai | Speedup |
|----|----|----|----|
| Trivial task round trip | 0.8 µs | 100.5 µs | 126x |
| Pipelined throughput, 1 worker | 833,000 tasks/s | 9,700 tasks/s | 86x |
| 8 MB vector round trip | 400 µs | 18.4 ms | 46x |
| Parallel map of 2,000 ~10 µs tasks, 4 workers | 5.0 ms | 218 ms | 44x |

These rows measure communication overhead, the cost mizu is built to remove.
mirai also covers workers on remote machines and HPC clusters, which shared memory cannot reach.

## Python interop

A channel peer can be a Python process that runs [pymizu](https://github.com/shikokuchuo/pymizu), the Python binding of the same core.
Pass the peer program as a source string (instead of a quoted expression) and set the launcher to `mizu_py_launcher()`:

``` r
ch <- mizu_channel(
  "
import pymizu
while True:
    x = ch.recv(30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x * 2)
",
  launcher = mizu_py_launcher()
)

mizu_send(ch, c(1.5, 2.5, 3.5)) # arrives in Python as a float64 NumPy array
mizu_recv(ch, timeout = 5) # echoes back as a numeric vector
mizu_close(ch)
```

`mizu_py_launcher()` needs a `python3` with pymizu installed.
A launcher is one function that takes the join token and spawns the peer process; for a different spawn method, write your own.
The pymizu README shows the reverse direction: a Python host that spawns an R peer with `pymizu.r_launcher()`.

## Reference vignette

`vignette("reference", package = "mizu")` covers the full surface: batch operations and sentinel values, nested tasks and `mizu_map()`, growing, attaching to, and observing a running pool, Python interop, benchmarks against mirai, sizing `/dev/shm` on Linux, tuning the Linux memory allocator, and crash semantics.

------------------------------------------------------------------------

Please note that this project is released with a [Contributor Code of Conduct](https://github.com/shikokuchuo/mizu/blob/main/.github/CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.
