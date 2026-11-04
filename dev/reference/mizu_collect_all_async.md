# Collect Pool Results as Promises

`mizu_collect_all_async()` and `mizu_collect_any_async()` are the async
counterparts of
[`mizu_collect_all()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_all.md)
and
[`mizu_collect_any()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_any.md).
Each returns immediately with one
[`promises::promise()`](https://rstudio.github.io/promises/reference/promise.html)
for the whole collection, and no per-task promise is allocated. On each
wake the pool checks for newly finished tasks, and when the collection
completes, the result is assembled once as in the blocking version and
the promise resolves.

## Usage

``` r
mizu_collect_all_async(tasks, timeout = Inf)

mizu_collect_any_async(tasks, timeout = Inf)

mizu_collect_async(task, timeout = Inf)

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
  [`mizu_submit()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
  on the same pool handle.

- timeout:

  seconds after which the promise rejects with the `mizu_timeout`
  classed error; nothing is consumed. `Inf` (the default) waits
  indefinitely.

- task:

  a task handle from
  [`mizu_submit()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md).

- x:

  for the `as.promise` methods: a task handle, or a map handle from
  [`mizu_map()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map.md)
  /
  [`mizu_map_run()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map_prepare.md).

## Value

A
[`promises::promise()`](https://rstudio.github.io/promises/reference/promise.html):
`mizu_collect_async()` resolves the task's value; collect-all resolves a
plain list of the task values in the order of `tasks` (names carried);
collect-any resolves `list(index, value)`.

## Details

`mizu_collect_async()` is the single-task form, the async counterpart of
[`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md).
It returns immediately with a
[`promises::promise()`](https://rstudio.github.io/promises/reference/promise.html)
that resolves with the task's value and rejects with the same classed
condition a blocking collect would raise. There is one promise per task.
While the task is pending the promise is cached, so a repeated call, or
[`as.promise()`](https://rstudio.github.io/promises/reference/is.promise.html)
on the handle (which
[`promises::then()`](https://rstudio.github.io/promises/reference/then.html)
calls), returns the same promise, and `timeout` applies only to the call
that created it. Once settled the task is collected, and a further call
errors (one consumer per result, below). A `timeout` rejects with the
`mizu_timeout` classed error and consumes nothing, so the task can be
collected with a blocking call again.

The behavior mirrors the blocking functions. Collect-all resolves in
input order with names carried over, and the first erroring handle by
position rejects with `index` (only that handle is consumed, the rest
stay collectible). Collect-any resolves `list(index, value)` for the
first terminal task, and a raised outcome rejects with the `index`
field. A timeout rejects with the `mizu_timeout` classed error and
consumes nothing. When a collection finishes, whether resolved,
rejected, or timed out, every handle in it is released, and handles
whose results were not consumed can be collected with a blocking call
again.

A task result belongs to exactly one consumer. A task with a promise
attached, or one that is part of a pending collection, refuses
[`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md),
[`mizu_collect_any()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_any.md)
and
[`mizu_collect_all()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_all.md)
with an error naming the pending consumer, and a task enrolled in a
collection likewise refuses a promise.
[`mizu_cancel()`](https://shikokuchuo.net/mizu/dev/reference/mizu_cancel.md)
is still allowed and is the destructive option: the cancel publishes,
the result is gone for every consumer, and the pending promise rejects
with `mizu_error_cancelled`. A `timeout` is the non-destructive
counterpart. It only detaches the promise, and the task keeps running
with its result still collectible. (A channel message instead belongs to
whoever takes it first; see
[`mizu_recv_async()`](https://shikokuchuo.net/mizu/dev/reference/mizu_recv_async.md).)

Requires the later and promises packages.

## Examples

``` r
if (FALSE) { # interactive() && requireNamespace("promises", quietly = TRUE)
library(promises)
p <- mizu_pool(2L)
t <- mizu_submit(p, sum(runif(10)))
mizu_collect_async(t, timeout = 5) %...>% print()
ts <- list(
  total = mizu_submit(p, sum(x), x = runif(10)),
  label = mizu_submit(p, "done")
)
mizu_collect_all_async(ts) %...>% print()
later::run_now()
mizu_pool_stop(p)
}
```
