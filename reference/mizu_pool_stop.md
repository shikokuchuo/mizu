# Stop a Pool

Broadcasts shutdown, wakes every parked participant, and cancels all
pending tasks (blocked collectors raise `mizu_error_cancelled`). Then
waits up to `timeout` seconds for the workers to exit cleanly, and
unlinks the region and the liveness files. The handle is dead
afterwards, and stopping it again is a no-op. Only the creating process
can stop a pool.

## Usage

``` r
mizu_pool_stop(pool, timeout = 5)
```

## Arguments

- pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_current_pool.md).

- timeout:

  seconds to wait for the clean exit of the workers.

## Value

Invisibly, `TRUE` if all workers exited within the timeout. `FALSE`
otherwise, with a warning (the workers still exit on their own).

## Examples

``` r
p <- mizu_pool()
t <- mizu_submit(p, 1 + 1)
mizu_collect(t)
#> [1] 2
mizu_pool_stop(p)
```
