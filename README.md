
<!-- README.md is generated from README.Rmd. Please edit that file -->

# mov

<!-- badges: start -->

[![R-CMD-check](https://github.com/shikokuchuo/mov/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/shikokuchuo/mov/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

      ________
     /\ m     \
    /  \ o     \
    \  /  v    /
     \/_______/

High-rate, small-message transport between R processes on the same
machine over POSIX shared memory (Linux, macOS) or Win32 file mappings
(Windows). The hot path stays entirely in user space: lock-free
single-producer single-consumer rings with batched publication, hybrid
spin-then-park waiting, and event-driven peer-death detection.

mov is a complement to [mirai](https://mirai.r-lib.org) for workloads
measurably bound by per-message socket overhead. Payload contents
interoperate transparently with [mori](https://github.com/r-lib/mori)
shared objects.

## Installation

Install the development version from GitHub:

``` r
pak::pak("shikokuchuo/mov")
```

mov requires 64-bit R, and on Linux a kernel \>= 5.3.

## Quick start

The essential function is `mov_channel()`: it creates a bidirectional
shared-memory channel — one lock-free ring per direction — and spawns a
child R process connected to its other end. The child evaluates a quoted
expression with `ch` bound to its side of the channel; all data crosses
the rings, not the process boundary.

``` r
library(mov)

ch <- mov_channel(quote(
  repeat {
    x <- mov_recv(ch, timeout = 30)
    if (inherits(x, "mov_condition")) break
    mov_send(ch, x * 2)
    mov_flush(ch)
  }
))

mov_send(ch, 21)
mov_flush(ch)
mov_recv(ch, timeout = 5)
#> [1] 42
```

`mov_send()` stages a message; `mov_flush()` publishes staged messages
to the peer — flush after every send for minimum latency, or every N
sends for throughput. Terminal states (ring full, timeout, orderly
close, peer death) are returned as class-tagged sentinels rather than
errors, so the hot loop stays branch-cheap; `mov_send_batch()` and
`mov_recv_batch()` amortize the R call boundary itself.

`mov_close()` performs an orderly shutdown, rendezvousing with the peer
before releasing shared resources:

``` r
mov_close(ch)
```

## Task pool

Built on the same transport, `mov_pool()` spawns a pool of worker
processes with work-stealing deques and no dispatcher in the loop:
submission is a shared-memory ring write plus at most one directed wake.

``` r
p <- mov_pool(n_workers = 4L)

t <- mov_submit(p, sum(x) + y, x = runif(10), y = 100)
mov_collect(t)
#> [1] 105.5046
```

Task expressions see their evaluating worker’s own handle as `pool`, so
tasks can fan out nested subtasks. A nested submit pushes straight onto
the worker’s own work-stealing deque — no ring, no wake — and a worker
blocked collecting a nested result executes other work instead of
sleeping, so divide-and-conquer runs at fork/join cost and never
deadlocks the pool:

``` r
t <- mov_submit(
  p,
  {
    subtasks <- lapply(parts, function(part) mov_submit(pool, sum(x), x = part))
    do.call(sum, lapply(subtasks, mov_collect))
  },
  parts = split(1:1000, rep(1:4, each = 250))
)
mov_collect(t)
#> [1] 500500

mov_pool_stop(p)
```

Worker death is detected at OS notification latency — a kernel-released
liveness lock is the verdict, with no heartbeats and no polling — and
fails exactly the tasks the dead worker had claimed. If a process
crashes without cleaning up, `mov_prune()` removes the shared-memory
regions it left behind.
