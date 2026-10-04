# Cumulative Pool Counters

Per-worker and per-submitter counters, accumulated since each
participant joined. They complement the point-in-time snapshots of
[`mizu_pool_status()`](https://shikokuchuo.net/mizu/reference/mizu_pool_status.md)
and
[`mizu_pool_dump()`](https://shikokuchuo.net/mizu/reference/mizu_pool_dump.md).
Nothing here costs the hot paths anything. The submitter counts are the
monotonic positions of the injection rings themselves: submission writes
nothing extra, and the spill counter moves only on the spill path, which
a fresh region per payload already dominates. The worker counters are
kept process-locally and mirrored into the region only when a worker
parks, leaves, or passes its fairness tick. So under continuous load,
the row of a worker can lag by up to 61 claims. The row is exact
whenever that worker is parked or retired, or the pool is quiescent.

## Usage

``` r
mizu_pool_stats(pool)
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

A list of two data frames. `workers`: one row per worker slot, with
`status`, `pid`, `tasks` (task evaluations run, help-mode and nested
inline execution included), `steals` (entries claimed from the deques of
peers), `injections` (entries claimed from injection rings), `parks`
(kernel parks in the worker loop), `helps` (claims executed while
blocked in a nested collect), and the current `deque` depth.
`submitters`: one row per submitter slot, with `status`, `pid`,
`injected` (entries ever published to its injection ring), `claimed`
(entries the workers took from it), `spills` (payloads past the inline
budget that traveled in their own region — task payloads at submit and
result payloads at publish, both attributed to the submitter of the
task. A nonzero value means `slot_size` is undersized for the traffic),
`spill_reuse` (the subset of `spills` that recycled a retired region
from the free list of the producer instead of creating one), and
`queued` (`injected - claimed`). Steady-state spill traffic approaches
`spills`, so `spills - spill_reuse` is the region-churn rate. Counters
reset when a new joiner reuses a slot.

## Examples

``` r
p <- mizu_pool()
t <- mizu_submit(p, runif(5))
mizu_collect(t)
#> [1] 0.4875422 0.8822684 0.8142058 0.7425027 0.1328077
mizu_pool_stats(p)
#> $workers
#>   slot status  pid tasks steals injections parks helps deque
#> 1    0   live 7825     1      0          1     1     0     0
#> 
#> $submitters
#>   slot status  pid injected claimed spills spill_reuse queued
#> 1    0   live 6932        1       1      0           0      0
#> 2    1   free    0        0       0      0           0      0
#> 3    2   free    0        0       0      0           0      0
#> 4    3   free    0        0       0      0           0      0
#> 5    4   free    0        0       0      0           0      0
#> 6    5   free    0        0       0      0           0      0
#> 7    6   free    0        0       0      0           0      0
#> 8    7   free    0        0       0      0           0      0
#> 
mizu_pool_stop(p)
```
