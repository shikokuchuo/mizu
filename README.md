
<!-- README.md is generated from README.Rmd. Please edit that file -->

# mizu 水

<!-- badges: start -->

[![R-CMD-check](https://github.com/shikokuchuo/mizu/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/shikokuchuo/mizu/actions/workflows/R-CMD-check.yaml)
[![Codecov test coverage](https://codecov.io/gh/shikokuchuo/mizu/graph/badge.svg)](https://app.codecov.io/gh/shikokuchuo/mizu)
[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE.md)
<!-- badges: end -->

      ________
     /\       \
    /  \  mizu \
    \  /   水  /
     \/_______/

mizu makes communication between R processes cheap enough to divide work at granularities usually reserved for threads.

Shared memory has always been the fastest IPC transport.
A socket round trip costs four system calls and four copies of the data; in shared memory, one process reads the bytes the other wrote — the kernel never touches the data.
mizu handles the synchronization, waiting, and peer crashes for you, so a task round trip drops from around 100 µs over sockets to under a microsecond — two orders of magnitude (see [Benchmarks](#benchmarks)).

Channels and work-stealing task pools run over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).
A channel is a two-way message link between an R session and a helper process that it spawns.
A pool is a set of worker processes that divide submitted tasks among themselves.

The hot path stays in user space: single-producer single-consumer rings with batched publication, spin-then-park waiting, and event-driven peer-death detection.

mizu is built on [libmizu](https://github.com/shikokuchuo/libmizu), a C library for lock-free shared-memory IPC.
[pymizu](https://github.com/shikokuchuo/pymizu) binds the same core for Python, so R and Python can talk to each other.

- Zero-copy vectors and data frames — received as ALTREP views over the shared pages, never unserialized
- Reproducible parallel randomness (`.seed`), invariant for any worker count or steal order
- Sentinels, not errors, on hot paths; worker crashes detected at OS latency
- R 4.3+ on Linux, macOS, and Windows

> **Pre-release.** The API is not stable and may change at any time before a release.

## Installation

Install the development version from GitHub:

``` r
pak::pak("shikokuchuo/mizu")
```

Requires R 4.3 or later on a 64-bit platform (Linux: kernel 5.3 or later).

## Quickstart

``` r
library(mizu)

p <- mizu_pool(n_workers = 4L)
t <- mizu_submit(p, sum(x) + y, x = 1:10, y = 100)
mizu_collect(t)
#> [1] 155
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

A task error re-raises on collect as a classed condition with its message, call, and fields intact; a dead worker’s claimed tasks fail the same way, detected at OS notification latency with no heartbeats or polling.

## Parallel map

`mizu_map()` maps a function over a vector or list on the pool and returns the results in input order.
The function and data cross into shared memory once; a handful of chunk tasks divide the elements, and each worker self-schedules element ranges off a shared cursor.

``` r
p <- mizu_pool(n_workers = 4L)

mizu_map(p, 1:5, \(i) i * 2L, .template = integer(1))
#> [1]  2  4  6  8 10

mizu_pool_stop(p)
```

## Python interop

Pool workers can be Python processes that run [pymizu](https://github.com/shikokuchuo/pymizu), the Python binding of the same core.
`mizu_py_pool_launcher()` spawns them and `mizu_call()` describes the task — here R’s built-in `mtcars` summarized by polars:

``` r
p <- mizu_pool(4L, launcher = mizu_py_pool_launcher())

t <- mizu_submit_call(
  p,
  mizu_call(
    .source = "import polars as pl
pl.DataFrame(x).group_by('cyl').agg(pl.col('mpg').mean()).sort('cyl')",
    x = mtcars
  )
)
mizu_collect(t)
#>   cyl      mpg
#> 1   4 26.66364
#> 2   6 19.74286
#> 3   8 15.10000

mizu_pool_stop(p)
```

A numeric vector arrives in Python as a NumPy array and a data frame as a `pymizu.Frame`; a NumPy array, or a polars, pyarrow, or pandas frame, arrives back as a vector or data.frame.
For a channel peer, pass the peer program as a source string (instead of a quoted expression) and set the launcher to `mizu_py_launcher()`.
The launchers need a `python3` with pymizu installed.
A launcher is one function that takes the join token and spawns the peer process; for a different spawn method, write your own.
The pymizu README shows the reverse direction: a Python host that drives R workers with `pymizu.r_pool_launcher()`.

## Vignettes

`vignette("mizu", package = "mizu")` is the overview hub, with topic vignettes on benchmarks against mirai, channels, task pools, the parallel map, Python interop, and operations (sizing `/dev/shm` on Linux, tuning the Linux memory allocator, and crash semantics).
The [pkgdown site](https://shikokuchuo.net/mizu/) has the function reference and the rendered vignettes.

------------------------------------------------------------------------

Please note that this project is released with a [Contributor Code of Conduct](https://github.com/shikokuchuo/mizu/blob/main/.github/CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.
