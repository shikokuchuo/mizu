# The Evaluating Worker's Own Pool Handle

Inside a pool task, `mizu_current_pool()` returns the pool handle of the
worker evaluating the task — the handle to pass to
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
for nested submission (subtasks push onto the worker's own work-stealing
deque, and a worker blocked collecting them helps instead of sleeping).
Outside a task, it returns `NULL`.

## Usage

``` r
mizu_current_pool()
```

## Value

A pool handle (class `"mizu_pool"`) inside a pool task; otherwise
`NULL`.

## Details

The handle is runtime-owned: the pool sets it around each task
evaluation, so unlike a variable binding it cannot be shadowed by a task
argument or a local assignment.

## Examples

``` r
p <- mizu_pool()
t <- mizu_submit(p, {
  s <- mizu_submit(mizu_current_pool(), x * 2L, x = x)
  mizu_collect(s, timeout = 30)
}, x = 21L)
mizu_collect(t, timeout = 30)
#> [1] 42
mizu_pool_stop(p)
```
