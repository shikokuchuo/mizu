# Parallel map

[`mizu_map()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map.md)
maps a function over a vector or list on the pool and returns a map
handle immediately, just as
[`mizu_submit()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
returns a task.
[`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
on the handle waits and returns the results in input order.
[`as.promise()`](https://rstudio.github.io/promises/reference/is.promise.html)
(or [`then()`](https://rstudio.github.io/promises/reference/then.html)
directly) collects through the event loop instead; see [Promises and
handlers](https://shikokuchuo.net/mizu/dev/articles/pools.html#promises-and-handlers).
A map is not a loop over
[`mizu_submit()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md).
The function, its constant arguments, and the data are staged once in
shared memory. A few tasks divide the elements between the workers, and
each worker sets up the map at most once. The per-element cost
approaches the cost of [`lapply()`](https://rdrr.io/r/base/lapply.html),
while the work spreads across workers and balances itself through
stealing.

``` r

library(mizu)

p <- mizu_pool(n_workers = 4L)
m <- mizu_map(p, 1:5, \(i) i * 2L)
mizu_collect(m)
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

A `.template`, like the `FUN.VALUE` argument of
[`vapply()`](https://rdrr.io/r/base/lapply.html), gives you an atomic
vector or matrix instead of a list. The workers write their results
directly into shared memory, so each result crosses the process boundary
once, without serialization:

``` r

m <- mizu_map(p, seq.int(-5, 5), abs, .template = numeric(1))
mizu_collect(m)
#>  [1] 5 4 3 2 1 0 1 2 3 4 5
```

With `.collect = "view"`, the result is the shared output area itself,
wrapped as a read-only ALTREP view. There is no final copy, and the
region stays alive until the view is garbage collected.

An `integer64` template (bit64’s layout, constructed without bit64) is
also supported. Results must then be `integer64` of the template’s
length, since an int64 result never widens to double.

## Reproducible randomness

By default, random numbers drawn inside the function are not
reproducible, and cost nothing extra. Pass `.seed` to give every element
its own L’Ecuyer-CMRG stream. The results are then identical for any
chunking, worker count, or steal order:

``` r

m1 <- mizu_map(p, 1:4, \(i) rnorm(i), .seed = 123L)
m2 <- mizu_map(p, 1:4, \(i) rnorm(i), .seed = 123L, .n_chunks = 4L)
identical(mizu_collect(m1), mizu_collect(m2))
#> [1] TRUE
```

## Prepared maps

[`mizu_map_prepare()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map_prepare.md)
stages a map — creating the shared-memory region and writing the
function, its constant arguments, and the data into it — without running
it, so that cost is paid once. Each
[`mizu_map_run()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map_prepare.md)
then costs only task submission and collection, and reuses the map
context cached on each worker. Repeated stochastic simulation is the
main use. `.seed` is passed per run rather than staged with the region,
so each run can use different random streams at no extra cost:

``` r

pm <- mizu_map_prepare(p, 1:1000, \(i, draws) mean(rnorm(draws)) * i, draws = 100L)
runs <- lapply(1:4, \(s) {
  m <- mizu_map_run(pm, .seed = s)
  mizu_collect(m)
})
vapply(runs, `[[`, numeric(1), 1L)
#> [1] 0.13109678 0.08153219 0.17986914 0.17683176
```

`mizu_map_run(pm, .x = x2)` replaces the staged data. An atomic vector
of the same type and length is swapped in place with one copy, and any
other replacement restages the map automatically. A run collected with
`.collect = "view"` hands its region to the view, so the next run
restages into a fresh one. The handle keeps the staged data and the
region alive for its lifetime, and both are released at garbage
collection.

## Nested maps

Maps nest: `mizu_map(mizu_current_pool(), ...)` inside a task expression
uses the handle of the worker running the task. The runner submissions
push onto that worker’s own deque, and while the collect waits it
executes its own runners as idle peers steal the rest, so a nested map
cannot deadlock the pool. With a `.template`, the nested map collects as
an atomic vector that the task can reduce directly, with no list to
[`unlist()`](https://rdrr.io/r/base/unlist.html):

``` r

t <- mizu_submit(
  p,
  {
    m <- mizu_map(mizu_current_pool(), parts, sum, .template = numeric(1))
    sum(mizu_collect(m))
  },
  parts = split(as.numeric(1:100000), rep(1:10, each = 1e4))
)
mizu_collect(t)
#> [1] 5000050000
```

The first nested map on a worker claims a submitter slot. With the
default `max_submitters = 8`, one of which the controller holds, at most
7 workers can nest at the same time. Raise `max_submitters` for wider
nested fan-outs.

``` r

mizu_pool_stop(p)
```
