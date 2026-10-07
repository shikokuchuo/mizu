# The Process-Wide Default Pool

`mizu_default_pool()` returns the pool registered as this process's
default, or `NULL` when none is set. `mizu_set_default_pool()` sets the
default — or clears it, with `NULL` — and invisibly returns the previous
one, so callers can save and restore. `mizu_with_pool()` evaluates
`expr` with `pool` as the default and restores the previous default on
exit, including on error. `mizu_local_pool()` sets the default until the
calling frame exits (the withr `local_*` pattern).

## Usage

``` r
mizu_default_pool()

mizu_set_default_pool(pool)

mizu_with_pool(pool, expr)

mizu_local_pool(pool, frame = parent.frame())
```

## Arguments

- pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md),
  or `NULL` to clear the default.

- expr:

  an expression to evaluate with `pool` as the default.

- frame:

  the frame at whose exit the previous default is restored; defaults to
  the caller of `mizu_local_pool()`.

## Value

`mizu_default_pool()`: the default pool handle, or `NULL`.
`mizu_set_default_pool()` and `mizu_local_pool()`: the previous default
(a pool handle or `NULL`), invisibly. `mizu_with_pool()`: the value of
`expr`.

## Details

The registry anchors the handle: a pool set as the default stays alive
even after its variable is removed, until the default is cleared or
replaced. Handles from
[`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md)
are valid defaults; ownership and teardown stay with the pool's creator.

Setting a default checks the type only: a stopped pool is accepted
(liveness is transient; a probe would prove nothing about use time) and
fails later with the usual stopped-pool errors. The default is
process-global state — after a `fork()`, a child process reads it as
unset.

## Package authors

A function that takes an optional pool resolves it in this order: an
explicit `pool` argument, then
[`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md)
inside a task (the evaluating worker's own pool, for nested submission),
then `mizu_default_pool()`, then the caller's own fallback — sequential
evaluation or an error.
[`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md)
is runtime-owned by the pool around each task evaluation and cannot be
shadowed; the default pool is user-set state and never overrides it.

## Examples

``` r
p <- mizu_pool()
old <- mizu_set_default_pool(p)

# the package-author resolution idiom
f <- function(x, pool = mizu_default_pool()) {
  if (is.null(pool)) {
    stop("no pool: pass one, or set a default", call. = FALSE)
  }
  mizu_collect(mizu_submit(pool, x + 1L, x = x))
}
f(1L)
#> [1] 2

# scoped use: the previous default returns on exit
mizu_with_pool(NULL, mizu_default_pool())
#> NULL
mizu_default_pool()
#> <mizu_pool /mizu_1abf_be94cde8: controller, 1/1 workers live, 0 pending>

mizu_set_default_pool(old)
mizu_pool_stop(p)
```
