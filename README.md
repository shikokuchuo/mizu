
<!-- README.md is generated from README.Rmd. Please edit that file -->

# kioto

<!-- badges: start -->

[![R-CMD-check](https://github.com/shikokuchuo/kioto/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/shikokuchuo/kioto/actions/workflows/R-CMD-check.yaml)
<!-- badges: end -->

      ________
     /\ k     \
    /  \ i o   \
    \  /   t o /
     \/_______/

Parallel computation and data exchange between R processes on the same machine: lock-free channels and work-stealing task pools over POSIX shared memory (Linux, macOS) or Win32 file mappings (Windows).
A channel is a two-way message link between an R session and a helper process it spawns; a pool is a set of worker processes that divide submitted tasks among themselves.
In both, data written by one process is read in place by the other — never copied through a socket, pipe, or file — and the hot path stays entirely in user space: single-producer single-consumer rings with batched publication, hybrid spin-then-park waiting, and event-driven peer-death detection.

R evaluates code on a single thread, so parallelism in R means multiple processes.
kioto makes the communication between them cheap enough that work can be divided at granularities usually reserved for threads.
It is a complement to [mirai](https://mirai.r-lib.org), the general solution for parallel and distributed computing in R; payload contents interoperate transparently with [mori](https://github.com/r-lib/mori) shared objects.

## Installation

Install the development version from GitHub:

``` r
pak::pak("shikokuchuo/kioto")
```

## Channels

The essential function is `kio_channel()`: it creates a bidirectional shared-memory channel — one lock-free ring per direction — and spawns a child R process connected to its other end.
The child evaluates a quoted expression with `ch` bound to its side of the channel; all data crosses the rings, not the process boundary.

``` r
library(kioto)

ch <- kio_channel(quote(
  repeat {
    x <- kio_recv(ch, timeout = 30)
    if (inherits(x, "kio_sentinel")) break
    kio_send(ch, x * 2)
  }
))

kio_send(ch, 21)
kio_recv(ch, timeout = 5)
#> [1] 42
```

`kio_send()` publishes a message to the peer, visible the moment the call returns; `kio_recv()` returns the next one.
`kio_send_batch()` and `kio_recv_batch()` move a whole list of messages under a single call, for rates at which the per-call overhead of R itself starts to matter.

Outcomes that end a conversation — ring full, timeout, orderly close, peer death — are returned as class-tagged sentinel values rather than thrown as errors, so a receive loop tests for them with `inherits(x, "kio_sentinel")` (or on the specific classes `kio_full`, `kio_timeout`, `kio_closed`, `kio_peer_gone`) instead of wrapping every call in error handlers.
If the process at the other end dies, receives first drain the messages it had already published, then report `kio_peer_gone`; `kio_alive(ch)` asks whether the peer is still running at any time, without touching the rings.

`kio_close()` performs an orderly shutdown, waiting for the peer to finish draining before shared resources are released:

``` r
kio_close(ch)
```

## Task pools

Built on the same transport, `kio_pool()` spawns a pool of worker processes with work-stealing deques and no dispatcher in the loop: a submitted task goes straight from the submitting process into shared memory, where a worker claims it — and idle workers steal from busy ones, so load balances itself.
`kio_submit()` captures an expression together with the values it needs and returns a task handle immediately, leaving your session free to continue; `kio_collect()` waits for that task’s result:

``` r
p <- kio_pool(n_workers = 4L)

t <- kio_submit(p, sum(x) + y, x = 1:10, y = 100)
kio_collect(t)
#> [1] 155
```

An error raised inside a task is captured and re-signalled in your session when you collect it.
`kio_cancel(t)` withdraws a task: one still queued is skipped, while one already running completes and its result is discarded — cancellation never interrupts executing code.

Inside a task, the evaluating worker’s own handle is available as `pool`, so a task can split itself into subtasks.
A nested submit pushes straight onto the worker’s own work-stealing deque — no ring, no wake — and a worker waiting on a nested result executes other work instead of sleeping, so divide-and-conquer runs at fork/join cost and never deadlocks the pool:

``` r
t <- kio_submit(
  p,
  {
    subtasks <- lapply(parts, \(part) kio_submit(pool, sum(x), x = part))
    do.call(sum, lapply(subtasks, kio_collect))
  },
  parts = split(1:1000, rep(1:4, each = 250))
)
kio_collect(t)
#> [1] 500500
```

## Parallel map

`kio_map()` maps a function over a vector or list on the pool, returning results in input order.
It is not a loop over `kio_submit()`: the function, its constant arguments, and the data are staged once in shared memory, a handful of chunk tasks divide the elements, and each worker sets up the map at most once — so the per-element cost approaches `lapply()`’s while the work spreads across workers and balances itself through stealing.

``` r
kio_map(p, 1:5, \(i) i * 2L)
#> [[1]]
#> [1] 2
#> 
#> [[2]]
#> [1] 4
#> 
#> [[3]]
#> [1] 6
#> 
#> [[4]]
#> [1] 8
#> 
#> [[5]]
#> [1] 10
```

A `.template` (in the style of `vapply()`’s `FUN.VALUE`) returns an atomic vector or matrix instead of a list, with results written straight into shared memory — moving cross-process exactly once, unserialized:

``` r
kio_map(p, seq.int(-5, 5), abs, .template = numeric(1))
#>  [1] 5 4 3 2 1 0 1 2 3 4 5
```

Random numbers drawn inside the function are not reproducible by default — and cost nothing extra.
Passing `.seed` gives every element its own L’Ecuyer-CMRG stream, so results are identical for any chunking, worker count, or steal order:

``` r
identical(kio_map(p, 1:4, \(i) rnorm(i), .seed = 123L),
          kio_map(p, 1:4, \(i) rnorm(i), .seed = 123L, .chunks = 4L))
#> [1] TRUE
```

## Sizing, sharing and watching a pool

A pool can grow and shrink while it runs.
Retirement is graceful: the worker finishes what it is doing, and anything still queued to it is consumed by the remaining workers.
Other R processes can join a running pool as submitters — the pool’s name is the only thing that needs to be communicated to them:

``` r
kio_spawn_workers(p, n = 2L)     # two more workers join the pool
kio_retire_worker(p, slot = 0L)  # worker 0 exits after its current task

# in another R process — submit and collect exactly as the creator does:
p2 <- kio_pool_attach(name)      # name: kio_pool_status(p)$name on the creator
```

Four read-only tools observe a running pool without disturbing it:

``` r
kio_pool_status(p)  # snapshot: worker states, queued tasks, result slots
kio_pool_stats(p)   # cumulative counters: tasks run, steals, parks per worker
kio_pool_dump(p)    # every slot in full detail — the first tool when a pool hangs
kio_pool_trace(p, \(event, id) message(event, " ", id))  # task lifecycle hook
```

When you are done, `kio_pool_stop()` cancels pending tasks, waits for the workers to exit cleanly, and releases the shared region:

``` r
kio_pool_stop(p)
```

## When a process dies

Death of any participant is detected at OS notification latency, with no heartbeats and no polling: every process holds a lock that the kernel releases the instant it exits — for any reason — and that release is the verdict.
A dead worker fails exactly the tasks it had claimed (collecting them raises an error), while work still queued to it is consumed by the surviving workers; on a channel, the survivor sees `kio_peer_gone`.
The split is deliberate and holds across the whole surface: transport states — not yet, not now, stream over — return as sentinel values for the receiving loop to handle, while a request that can never be satisfied — a task’s own error, a cancelled task, a dead worker, a child that failed to start — raises a classed condition (see `?kio_error`).

A crashed process cannot clean up after itself: `kio_prune()` removes the shared-memory regions left behind by processes that no longer exist.
Regions belonging to running processes are never touched.

## Functions at a glance

| Function | Purpose |
|----|----|
| **Channels** |  |
| `kio_channel()` | create a channel and spawn the peer process at its other end |
| `kio_send()` | send a message to the peer |
| `kio_recv()` | receive the next message, waiting up to a timeout |
| `kio_send_batch()` / `kio_recv_batch()` | move many messages in one call |
| `kio_alive()` | is the peer process still running? |
| `kio_close()` | orderly shutdown of a channel |
| **Task pools** |  |
| `kio_pool()` | create a pool and spawn its workers |
| `kio_submit()` | send an expression to the pool; returns a task handle immediately |
| `kio_collect()` | wait for and return a task’s result |
| `kio_cancel()` | withdraw a task (never interrupts one already running) |
| `kio_map()` | map a function over a vector on the pool, staged once, in input order |
| `kio_pool_attach()` | join an existing pool as a submitter, from another process |
| `kio_spawn_workers()` / `kio_retire_worker()` | grow / shrink the worker set while the pool runs |
| `kio_pool_status()` / `kio_pool_stats()` / `kio_pool_dump()` | observe a pool: snapshot, cumulative counters, full diagnostic dump |
| `kio_pool_trace()` | register a hook called at each task lifecycle event |
| `kio_pool_stop()` | shut the pool down |
| **Housekeeping** |  |
| `kio_is_sentinel()` | is this one of kioto’s own sentinel values? (identity, not class) |
| `kio_prune()` | remove shared-memory regions orphaned by crashed processes |
