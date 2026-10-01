# Prepared Maps: Stage Once, Run Many

`mizu_map_prepare()` stages a map — `f`, the constant arguments in
`...`, and `x` — serialized once into a shared map region, without
running it. It returns a prepared-map handle. Each `mizu_map_run()` then
costs only task submission and collection: no serialization and no
region create. Because the region (and its name) stays alive across
runs, workers that ran a previous run reuse their cached map context
instead of re-attaching. Repeated stochastic simulation is the headline
use. `mizu_map_run(pm, .seed = i)` varies the RNG streams per run for
free: the seed state rides the runner payloads, not the region.

## Usage

``` r
mizu_map_prepare(pool, x, f, ..., .template = NULL, .chunks = NULL)

mizu_map_run(pm, x = NULL, .seed = NULL, .timeout = Inf, .collect = "value")
```

## Arguments

- pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md).

- x:

  a vector (atomic or list) to map over. Anything else is coerced with
  [`as.list()`](https://rdrr.io/r/base/list.html), as
  [`lapply()`](https://rdrr.io/r/base/lapply.html) does.

- f:

  a function (or, as
  [`match.fun()`](https://rdrr.io/r/base/match.fun.html) accepts, its
  name) applied as `f(x[[i]], ...)`. Serialized once with its enclosing
  environment. Keep that environment small, as with any cross-process
  map. A
  [`mizu_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
  specification maps over a pool of any worker language — see the
  Cross-language maps section.

- ...:

  further constant arguments to `f`, staged once.

- .template:

  `NULL` for a list result, or a
  [`vapply()`](https://rdrr.io/r/base/lapply.html)-style `FUN.VALUE`: an
  atomic vector template that each result must match.

- .chunks:

  the morsel count of the map (its scheduling granularity), or `NULL`
  for the default. See the Granularity section.

- pm:

  a prepared-map handle from `mizu_map_prepare()`.

- .seed:

  `NULL` (default: no RNG guarantees, no cost), or a numeric vector of
  length 1 or 2 that derives reproducible per-element RNG streams:
  `seed`, or `c(seed, offset)` to shift every element's stream by
  `offset` positions. See the Reproducible RNG section.

- .timeout:

  seconds after which the map gives up, cancels its outstanding work,
  and returns the `mizu_timeout` sentinel (class
  `c("mizu_timeout", "mizu_sentinel")`). `Inf` (the default) waits
  indefinitely. One deadline covers submission and collection.

- .collect:

  `"value"` (the default) gathers template results into an owning R
  vector. `"view"` instead returns an ALTREP view over the map's shared
  output area — no gather copy — for pipelines that immediately reduce.
  Requires an atomic `.template`. The view is copy-on-write: the first
  write materializes a private copy. It keeps the map region alive until
  released, and re-sending it through a channel or pool crosses as a
  full copy, not by reference.

## Value

`mizu_map_prepare()`: a prepared-map handle. `mizu_map_run()`: exactly
what [`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md)
returns for the staged map — a list, a templated atomic vector, or the
`mizu_timeout` sentinel.

## Details

Between runs, the shared scheduling state of the region is re-armed in
O(1). The cursor and cancel word clear, and the run generation embedded
in every claim word advances. A straggler task from a previous run can
never issue against the cursor of the new run. After an unclean run — a
`.timeout` expiry, an error in `f`, a worker death — the handle is
marked stale. The next `mizu_map_run()` restages into a fresh region
transparently (the old one unlinks at garbage collection under any
stragglers). A run collected with `.collect = "view"` restages likewise:
the returned view pins its region, so the next run stages fresh rather
than re-arming pages a held view still reads. A map small enough to ride
entirely inline keeps its staged blob on the handle instead: runs
resubmit it, still skipping the serialization.

The prepared handle pins the staged `x` (for transparent restaging) and
the map region for its lifetime. Both release at garbage collection when
the handle is dropped. The chunking geometry is fixed at prepare time.
The runner count adapts to the live workers at each run.

## Replacing x between runs

`mizu_map_run(pm, x = x2)` runs over a replacement `x`. When both the
staged and the replacement `x` are bare-byte eligible (atomic,
non-ALTREP, no attributes beyond names) with identical type and length,
the swap is in place. The new bytes are copied over the `x` section of
the region. This runs the iterate-over-same-shape loop (optimizer steps,
simulation sweeps) at memcpy cost, skipping the region create and the
re-attach of every worker. Any other change of `x` — a different shape
or type, a list, a map staged inline — restages transparently on the
next run.

## Examples

``` r
p <- mizu_pool()
pm <- mizu_map_prepare(p, 1:1000, function(i, draws) {
  mean(rnorm(draws)) * i
}, draws = 100L)
runs <- lapply(1:50, function(s) mizu_map_run(pm, .seed = s))
mizu_pool_stop(p)
```
