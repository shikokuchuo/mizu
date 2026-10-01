# Create a Task Pool and Spawn Its Workers

Creates a shared-memory task pool and spawns its worker processes.
Per-submitter injection rings feed the workers, and results are
published through result slots. A submission is one SHM ring write plus
at most one directed wake: no dispatcher process is in the loop. Each
worker owns a work-stealing deque. Idle workers steal from busy peers
and consume the injection rings. A fairness tick bounds the latency of
external submissions on a saturated pool.

## Usage

``` r
mizu_pool(
  n_workers = 1L,
  max_workers = n_workers,
  max_submitters = 8L,
  injection_cap = 1024L,
  per_worker_cap = 1024L,
  result_slots = 4096L,
  slot_size = 512L,
  launcher = mizu_launcher(),
  startup_timeout = 30
)
```

## Arguments

- n_workers:

  number of worker processes to spawn, at most `max_workers`.

- max_workers:

  worker registry capacity (at most 64).

- max_submitters:

  submitter registry capacity (at most 64). Each submitter owns its own
  injection ring and an equal share of `result_slots`.

- injection_cap:

  entries per submitter injection ring. A power of two.

- per_worker_cap:

  entries per worker work-stealing deque. A power of two.

- result_slots:

  total result slots, partitioned equally across the submitter slots and
  rounded up to a multiple of `max_submitters`. This bounds the
  outstanding (uncollected) tasks of each submitter.

- slot_size:

  bytes per queue entry and result slot. A power of two between 128 and
  2^20. A payload (task or result) that serializes past the inline
  budget travels in a fresh region per payload. This is an
  order-of-magnitude latency cliff, surfaced per submitter as
  [`mizu_pool_stats()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_stats.md)`$submitters$spills`.
  The default `512L` keeps typical expression-plus-arguments tasks
  inline. Pools that move only scalar payloads can drop to `256L`.

- launcher:

  a `function(token, slot)` that arranges for an R process to call
  `mizu:::worker_main(token, slot)`. The default
  [`mizu_launcher()`](https://shikokuchuo.github.io/mizu/reference/mizu_launcher.md)
  spawns `Rscript` and propagates the
  [`.libPaths()`](https://rdrr.io/r/base/libPaths.html) of the host. Its
  `stdout` and `stderr` arguments direct the worker output. A custom
  launcher must arrange the library paths itself.

- startup_timeout:

  seconds to wait for all workers to join. On expiry, mizu destroys the
  pool and raises `mizu_error_startup` (see
  [mizu_error](https://shikokuchuo.github.io/mizu/reference/mizu_error.md)).

## Value

A pool handle (class `"mizu_pool"`) holding submitter slot 0. Handles
are process-private and do not survive `fork()`.

## Details

The lifetime of the pool is bound to the creating process, which holds
submitter slot 0 of the returned handle. Use this handle directly with
[`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
and
[`mizu_collect()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md).
Other processes join as submitters through
[`mizu_pool_attach()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_attach.md).
Dropping the handle (or exiting R) shuts the pool down as
[`mizu_pool_stop()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_stop.md)
does, but without the wait.

Payload contents interoperate transparently with mori. A
[`mori::share()`](https://rdrr.io/pkg/mori/man/share.html)d object
anywhere inside the arguments of a task or its result serializes to its
short identifier wire form through the mori hooks. It maps zero-copy on
the other side.

The liveness lock files of the pool (the death-detection verdict) live
in a per-platform directory chosen at create time. This is `/dev/shm` on
Linux, and the per-user temporary directory on macOS and Windows. The
chosen path is recorded in the region, so every participant uses the
same files. The environment variable `MIZU_LIVENESS_DIR`, read in the
creating process, overrides the default.

## Examples

``` r
p <- mizu_pool()
t <- mizu_submit(p, x + y, x = 1, y = 2)
mizu_collect(t)
#> [1] 3
mizu_pool_stop(p)
```
