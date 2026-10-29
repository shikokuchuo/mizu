# Collect Pool Results as Promises

`mizu_collect_all_async()` and `mizu_collect_any_async()` are the async
counterparts of
[`mizu_collect_all()`](https://shikokuchuo.net/mizu/reference/mizu_collect_all.md)
and
[`mizu_collect_any()`](https://shikokuchuo.net/mizu/reference/mizu_collect_any.md):
each returns immediately with exactly **one**
[`promises::promise()`](https://rstudio.github.io/promises/reference/promise.html)
per collection — no per-task promise is ever allocated. The arm
announces each task on its own result slot; on each wake a C terminal
scan notes newly-terminal handles, and when the collection completes the
sync verb's gather runs once and resolves the promise.

## Usage

``` r
mizu_collect_all_async(tasks, timeout = Inf)

mizu_collect_any_async(tasks, timeout = Inf)

# S3 method for class 'mizu_task'
as.promise(x)

# S3 method for class 'mizu_task'
is.promising(x)

# S3 method for class 'mizu_map'
as.promise(x)

# S3 method for class 'mizu_map'
is.promising(x)
```

## Arguments

- tasks:

  a non-empty list of task handles from
  [`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
  on the same pool handle.

- timeout:

  seconds after which the promise rejects with the `mizu_timeout`
  classed error; nothing is consumed. `Inf` (the default) waits
  indefinitely.

- x:

  for the `as.promise` methods: a task handle, or a map handle from
  [`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md) /
  [`mizu_map_run()`](https://shikokuchuo.net/mizu/reference/mizu_map_prepare.md).

## Value

A
[`promises::promise()`](https://rstudio.github.io/promises/reference/promise.html):
collect-all resolves a plain list of the task values in the order of
`tasks` (names carried); collect-any resolves `list(index, value)`.

## Details

Semantics mirror the sync verbs. Collect-all resolves in input order
with names carried; the first erroring handle by position rejects with
`index` (only that handle consumed, the rest collectible). Collect-any
resolves `list(index, value)` for the first terminal task, a raised
outcome rejecting with the `index` field. A timeout rejects with the
`mizu_timeout` classed error and consumes nothing. When a collection
settles — resolve, reject, or timeout — every enrolled handle's announce
is withdrawn: the handles the settle leaves unconsumed return to
blocking-collectible.

A task with a promise attached — or enrolled in a pending collection —
refuses
[`mizu_collect()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md),
[`mizu_collect_any()`](https://shikokuchuo.net/mizu/reference/mizu_collect_any.md),
[`mizu_collect_all()`](https://shikokuchuo.net/mizu/reference/mizu_collect_all.md),
and a second enrollment, with an error naming the pending consumer.
[`mizu_cancel()`](https://shikokuchuo.net/mizu/reference/mizu_cancel.md)
stays allowed: the cancel publishes, and the drain rejects with
`mizu_error_cancelled`.

Requires the later and promises packages.

## Examples

``` r
if (FALSE) { # interactive() && requireNamespace("promises", quietly = TRUE)
library(promises)
p <- mizu_pool(2L)
ts <- list(
  total = mizu_submit(p, sum(x), x = runif(10)),
  label = mizu_submit(p, "done")
)
mizu_collect_all_async(ts) %...>% print()
later::run_now()
mizu_pool_stop(p)
}
```
