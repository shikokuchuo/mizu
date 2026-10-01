# Submit a Batch of Tasks

`mizu_submit_batch()` submits one task per element of `exprs` in a
single `.Call`: one R boundary crossing and one wake-up sweep per batch
instead of per task. At target rates the call boundary is a first-order
cost, so a burst submitted this way reaches the workers sooner than the
same burst looped through
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md).
Pair with
[`mizu_collect_all()`](https://shikokuchuo.net/mizu/reference/mizu_collect_all.md)
to batch the collection side too.

## Usage

``` r
mizu_submit_batch(pool, exprs, ..., .timeout = Inf)
```

## Arguments

- pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md).

- exprs:

  a list of expressions, one per task. Quote them yourself: elements of
  a list cannot be captured unevaluated.

- ...:

  named values bound in the evaluation environment. The values are
  serialized.
  [`mori::share()`](https://rdrr.io/pkg/mori/man/share.html)d objects
  reduce to identifiers and map zero-copy on the worker.

- .timeout:

  seconds to wait for injection-ring space before the call raises
  `mizu_error_submit_timeout`. Submission blocks only when the ring is
  full (back-pressure) and returns immediately otherwise. `Inf` (the
  default) waits indefinitely; `0` does not wait.

## Value

A list of task handles (class `"mizu_task"`), one per accepted task —
shorter than `exprs` when the ring filled past `.timeout` mid-batch.

## Details

Each task's wire payload is the same `list(expr, args)` as
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)'s,
with the `...` arguments shared by every task in the batch. Unlike
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md),
expressions are not captured: the elements of `exprs` are pre-quoted (or
plain values, which evaluate to themselves).

Submission semantics per task are
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)'s,
with one difference: if the injection ring fills past `.timeout`
mid-batch, the call returns the handles accepted so far instead of
raising `mizu_error_submit_timeout`. Fatal outcomes (pool stopped,
result slots exhausted) still raise; tasks already submitted stay valid
and collectible.

## Examples

``` r
p <- mizu_pool()
ts <- mizu_submit_batch(p, list(quote(1 + 1), quote(2 + 2)))
mizu_collect_all(ts, timeout = 30)
#> [[1]]
#> [1] 2
#> 
#> [[2]]
#> [1] 4
#> 
mizu_pool_stop(p)
```
