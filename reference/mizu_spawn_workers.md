# Grow or Shrink the Worker Set of a Pool

`mizu_spawn_workers()` spawns additional workers into free registry
slots, up to the `max_workers` of the pool, and waits for them to join.
`mizu_retire_worker()` asks one worker to exit cleanly. The request is
non-blocking and never preemptive. The worker observes it between tasks
and releases its slot. The remaining workers consume in place any work
still queued on its deque. The process of a retired worker can linger
briefly as a lifetime anchor for the results it produced that are not
yet collected.

## Usage

``` r
mizu_spawn_workers(
  pool,
  n = 1L,
  launcher = mizu_launcher(),
  startup_timeout = 30
)

mizu_retire_worker(pool, slot)
```

## Arguments

- pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_current_pool.md).

- n:

  number of workers to spawn.

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

- slot:

  the slot index of the worker (0-based, as reported by
  [`mizu_pool_dump()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_dump.md)).

## Value

`mizu_spawn_workers()` invisibly returns the slot indices spawned into.
`mizu_retire_worker()` invisibly returns `NULL`.

## Details

Slots free up when workers retire, exit at shutdown, or die and are
reaped. So a pool can cycle workers within its registry capacity for its
whole lifetime. Only the creating process can resize a pool.

## Examples

``` r
if (FALSE) { # interactive()
p <- mizu_pool(n_workers = 1L, max_workers = 2L)
mizu_retire_worker(p, 0L)
mizu_spawn_workers(p)
mizu_pool_stop(p)
}
```
