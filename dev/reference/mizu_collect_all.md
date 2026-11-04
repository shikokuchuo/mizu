# Collect the Results of Several Tasks, in Order

`mizu_collect_all()` waits until every task in `tasks` reaches a
terminal state and returns all results in input order. It is the batch
counterpart of
[`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md),
with one R call for the whole set instead of one per task. The wait
sleeps until a publishing worker wakes it directly, with no polling.

## Usage

``` r
mizu_collect_all(tasks, timeout = Inf)
```

## Arguments

- tasks:

  a non-empty list of task handles from
  [`mizu_submit()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
  on the same pool handle.

- timeout:

  seconds to wait for every task to reach a terminal state before the
  call returns the `mizu_timeout` sentinel. `Inf` (the default) waits
  indefinitely; `0` does not wait.

## Value

A plain list of the task values in the order of `tasks`; the names of
`tasks` carry over. On timeout, the `mizu_timeout` sentinel.

## Details

For homogeneous element-wise work, use
[`mizu_map()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map.md),
which batches submission and staging as well as collection.
`mizu_collect_all()` is for sets of tasks with different expressions and
arguments, which is what the per-task API is for.

## Outcomes

As for
[`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md),
attributed to a handle by position as in
[`mizu_collect_any()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_any.md):

|  |  |  |
|----|----|----|
| outcome | surfaced as | class |
| all results published | list of values, returned | — |
| not all terminal within `timeout` | sentinel, returned | `c("mizu_timeout", "mizu_sentinel")` |
| a task raised an error | re-signalled with an `index` field | the condition classes of the task itself |
| a task cancelled, or pool stopped | raised with an `index` field | `mizu_error_cancelled` |
| an executing worker died | raised with an `index` field | `mizu_error_worker_died` |

The `index` field of a raised condition is the 1-based position in
`tasks` of the first such task. Only the reported handle is consumed.
Every other handle, including those ahead of it, stays valid and
collectible. A timeout consumes nothing, so every handle stays valid and
collectible.

## Examples

``` r
if (FALSE) { # interactive()
p <- mizu_pool(2L)
ts <- list(
  total = mizu_submit(p, sum(x), x = runif(10)),
  label = mizu_submit(p, "done")
)
mizu_collect_all(ts, timeout = 30)
mizu_pool_stop(p)
}
```
