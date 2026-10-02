# Parallel map

[`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md) maps
a function over a vector or list on the pool and returns the results in
input order. It is not a loop over
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md).
The function, its constant arguments, and the data are staged once in
shared memory. A handful of chunk tasks divide the elements, and each
worker sets up the map at most once. The per-element cost approaches the
cost of [`lapply()`](https://rdrr.io/r/base/lapply.html), while the work
spreads across workers and balances itself through stealing.

``` r

library(mizu)

p <- mizu_pool(n_workers = 4L)
mizu_map(p, 1:5, \(i) i * 2L)
#> [[1]]
#> [1] 2
#> 
#> [[2]]
#> [1] 4
#> 
#> [[3]]
#> [1] 6
#> 
#> [[4]]
#> [1] 8
#> 
#> [[5]]
#> [1] 10
```

## Templates

A `.template` (in the style of the `FUN.VALUE` argument of
[`vapply()`](https://rdrr.io/r/base/lapply.html)) returns an atomic
vector or matrix instead of a list. The workers write the results
straight into shared memory: each result crosses the process boundary
exactly once, unserialized:

``` r

mizu_map(p, seq.int(-5, 5), abs, .template = numeric(1))
#>  [1] 5 4 3 2 1 0 1 2 3 4 5
```

With `.collect = "view"`, the result is the shared output area itself,
wrapped as a read-only ALTREP view: the gather copy is skipped, and the
region’s teardown defers to the view’s garbage collection.

An `integer64` template (bit64’s layout, constructed without bit64) is
also supported. int64 joins no widening lattice, so results must be
exact `integer64` of the template’s length.

## Reproducible randomness

Random numbers drawn inside the function are not reproducible by
default, and cost nothing extra. Pass `.seed` to give every element its
own L’Ecuyer-CMRG stream. Then the results are identical for any
chunking, worker count, or steal order:

``` r

identical(mizu_map(p, 1:4, \(i) rnorm(i), .seed = 123L),
          mizu_map(p, 1:4, \(i) rnorm(i), .seed = 123L, .chunks = 4L))
#> [1] TRUE
```

## Prepared maps

[`mizu_map_prepare()`](https://shikokuchuo.net/mizu/reference/mizu_map_prepare.md)
stages a map — the function, its constant arguments, and the data —
without running it: the serialization and the region create are paid
once. Each
[`mizu_map_run()`](https://shikokuchuo.net/mizu/reference/mizu_map_prepare.md)
then costs only task submission and collection, re-arms in O(1), and
reuses the cached map context of each worker. Repeated stochastic
simulation is the headline use: `.seed` rides each run rather than the
region, so the random streams vary per run for free:

``` r

pm <- mizu_map_prepare(p, 1:1000, \(i, draws) mean(rnorm(draws)) * i, draws = 100L)
runs <- lapply(1:4, \(s) mizu_map_run(pm, .seed = s))
vapply(runs, \(r) r[[1L]], numeric(1))
#> [1] 0.13109678 0.08153219 0.17986914 0.17683176
```

`mizu_map_run(pm, .x = x2)` replaces the staged data: an atomic vector
of the same type and length swaps in place at memcpy cost; any other
replacement restages transparently. A run collected with
`.collect = "view"` hands its region to the view, so the next run
restages into a fresh one. The handle pins the staged data and the
region for its lifetime; both release at garbage collection.

``` r

mizu_pool_stop(p)
```
