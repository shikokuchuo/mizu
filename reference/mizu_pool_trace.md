# Trace Task Lifecycle Events

Registers a hook on a pool handle. The hook is called as `fn(event, id)`
at each task lifecycle event that this process observes:

## Usage

``` r
mizu_pool_trace(pool, fn = NULL)
```

## Arguments

- pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md).

- fn:

  a `function(event, id)`, or `NULL` to remove a registered hook.

## Value

Invisibly, `NULL`.

## Details

- `"submit"` when a task is committed.

- On worker handles: `"start"` before the evaluation of a task, and
  `"done"` or `"error"` when its result is published.

- `"drop"` when a claimed task is discarded (cancelled before or during
  execution, or its out-of-line payload died with its enqueuer).

- `"rehome"` when a doorbell help beat claims a map runner. The helper
  moves it onto its own deque — where idle peers can steal it — instead
  of executing it nested.

`id` identifies the task as `"<submitter slot>:<counter>"`. This id is
stable across processes, so logs from both sides of a pool can be
correlated.

Registration is per-handle and per-process. A submitter that traces its
own handle sees only `"submit"`. Execution events happen on the workers.
To trace a worker, install the hook from a task on the worker's own
handle: `mizu_submit(p, mizu_pool_trace(mizu_current_pool(), fn))`. The
disabled hook costs one pointer check per event site, and no event sites
exist on the channel hot path. An error raised by the hook propagates as
an infrastructure failure at its site. On a worker, it takes the worker
down. This differs from the own error of a task, which is published as
the ERR result of that task.

## Examples

``` r
p <- mizu_pool()
mizu_pool_trace(p, function(event, id) cat(event, id, "\n"))
t <- mizu_submit(p, 1 + 1)
#> submit 0:1 
mizu_collect(t)
#> [1] 2
mizu_pool_trace(p)
mizu_pool_stop(p)
```
