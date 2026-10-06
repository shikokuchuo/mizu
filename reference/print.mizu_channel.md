# Print Methods for mizu Objects

One-line summaries. A channel prints its region name, side, and
conversation state: `open`, `closed` once either side signalled close,
or `peer gone` — the verdict of
[`mizu_alive()`](https://shikokuchuo.net/mizu/reference/mizu_alive.md),
probed at print. A pool prints its region name, the role of this handle,
live workers out of registry capacity, and pending (uncompleted) tasks.
A task handle prints its state, probed without consuming the result. The
state is `pending`, `ok`, `err`, `cancel`, or `died` in the result-slot
vocabulary of
[`mizu_pool_status()`](https://shikokuchuo.net/mizu/reference/mizu_pool_status.md).
It is `collected` once the result is taken, or `dropped` when its pool
is gone. Sentinels print as their class. The handle methods never error
and never touch the rings. A handle whose resources are released (a
closed channel, a stopped pool) prints as closed. So auto-printing is
always safe.

## Usage

``` r
# S3 method for class 'mizu_channel'
print(x, ...)

# S3 method for class 'mizu_pool'
print(x, ...)

# S3 method for class 'mizu_task'
print(x, ...)

# S3 method for class 'mizu_map_prepared'
print(x, ...)

# S3 method for class 'mizu_sentinel'
print(x, ...)

# S3 method for class 'mizu_error_remote'
print(x, ...)
```

## Arguments

- x:

  the object.

- ...:

  ignored.

## Value

`x`, invisibly.

## Examples

``` r
p <- mizu_pool()
p
#> <mizu_pool /mizu_1add_bb71b417: controller, 1/1 workers live, 0 pending>
t <- mizu_submit(p, 1 + 1)
t
#> <mizu_task: ok>
mizu_collect(t)
#> [1] 2
mizu_pool_stop(p)
p
#> <mizu_pool: closed>
```
