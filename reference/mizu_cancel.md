# Cancel a Task or a Map

Advisory and discard-only, never preemptive. The worker skips a task
that is still queued. A task already executing runs to completion, and
its result is dropped. Collecting a cancelled handle raises
`mizu_error_cancelled` (see
[mizu_error](https://shikokuchuo.net/mizu/reference/mizu_error.md)).

## Usage

``` r
mizu_cancel(task)
```

## Arguments

- task:

  a task handle from
  [`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md),
  or a map handle from
  [`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md) /
  [`mizu_map_run()`](https://shikokuchuo.net/mizu/reference/mizu_map_prepare.md).

## Value

Invisibly, `TRUE` if this call cancelled the task. `FALSE` if the call
was too late: the task completed, was already cancelled, or its pool is
gone.

## Details

On a map handle from
[`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md) /
[`mizu_map_run()`](https://shikokuchuo.net/mizu/reference/mizu_map_prepare.md),
the shared cancel word of the map is set: executing runners stop within
about one batch of their next transition, and every outstanding runner
task is cancelled as above. A blocking
[`mizu_collect()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
on the handle raises `mizu_error_cancelled`; a pending promise rejects
with it.

## Examples

``` r
p <- mizu_pool()
t <- mizu_submit(p, runif(1))
mizu_cancel(t)
mizu_pool_stop(p)
```
