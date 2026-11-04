# Parallel Map Over a Pool

Maps `.f` over the elements of `.x` on a pool and returns a map handle
immediately, just as
[`mizu_submit()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
returns a task.
[`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
on the handle waits for completion and returns the results in input
order: a list by default, or an atomic vector (or matrix) in the style
of [`vapply()`](https://rdrr.io/r/base/lapply.html) when `.template` is
given.
[`mizu_cancel()`](https://shikokuchuo.net/mizu/dev/reference/mizu_cancel.md)
stops it.
[`as.promise()`](https://rstudio.github.io/promises/reference/is.promise.html)
(or [`then()`](https://rstudio.github.io/promises/reference/then.html)
directly) collects it through the event loop instead; see
[`mizu_collect_all_async()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_all_async.md).

## Usage

``` r
mizu_map(
  .pool,
  .x,
  .f,
  ...,
  .template = NULL,
  .n_chunks = NULL,
  .seed = NULL,
  .timeout = Inf,
  .stream = FALSE,
  .collect = "copy"
)
```

## Arguments

- .pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.net/mizu/dev/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.net/mizu/dev/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.net/mizu/dev/reference/mizu_current_pool.md).

- .x:

  a vector (atomic or list) to map over. Anything else is coerced with
  [`as.list()`](https://rdrr.io/r/base/list.html), as
  [`lapply()`](https://rdrr.io/r/base/lapply.html) does.

- .f:

  a function (or, as
  [`match.fun()`](https://rdrr.io/r/base/match.fun.html) accepts, its
  name) applied as `.f(.x[[i]], ...)`. Serialized once with its
  enclosing environment. Keep that environment small, as with any
  cross-process map. A
  [`mizu_call()`](https://shikokuchuo.net/mizu/dev/reference/mizu_call.md)
  specification maps over a pool of any worker language; see the
  Cross-language maps section.

- ...:

  further constant arguments to `.f`, staged once. As in
  [`mizu_submit()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md),
  the formals ahead of `...` are dot-prefixed, so an argument name
  passed here never collides with them.

- .template:

  `NULL` for a list result, or a
  [`vapply()`](https://rdrr.io/r/base/lapply.html)-style `FUN.VALUE`: an
  atomic vector template that each result must match.

- .n_chunks:

  the morsel count of the map (its scheduling granularity), or `NULL`
  for the default. See the Granularity section.

- .seed:

  `NULL` (default: no RNG guarantees, no cost), or a numeric vector of
  length 1 or 2 that derives reproducible per-element RNG streams:
  `seed`, or `c(seed, offset)` to shift every element's stream by
  `offset` positions. See the Reproducible RNG section.

- .timeout:

  seconds after which submission gives up, cancels the tasks already in,
  and returns the `mizu_timeout` sentinel (class
  `c("mizu_timeout", "mizu_sentinel")`). `Inf` (the default) waits
  indefinitely. Submission blocks only while an injection ring is full;
  the collection deadline is
  [`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)'s
  `timeout`.

- .stream:

  `TRUE` streams slices of `.x` to workers under a sliding window
  instead of staging `.x` into shared memory wholesale, bounding
  shared-memory use by the window times the slice size. See the
  Streaming maps section. A streaming map handle collects with
  [`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
  only (it cannot be promise-armed).

- .collect:

  `"copy"` (the default) gathers template results into an owning R
  vector at
  [`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md).
  `"view"` instead returns an ALTREP view over the map's shared output
  area, with no gather copy, for pipelines that reduce it immediately.
  Requires an atomic `.template`. The view is copy-on-write: the first
  write materializes a private copy. It keeps the map region alive until
  released, and re-sending it through a channel or pool crosses as a
  full copy, not by reference. The choice is stored on the handle.

## Value

A map handle (class `"mizu_map"`).
[`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
on it returns a list of the results of `.f` in the order of `.x`, with
`names(.x)` reapplied. With `.template`, an atomic vector of type
`typeof(.template)`, or an `m * length(.x)` matrix when
`length(.template) > 1`. This is an owning vector, or a copy-on-write
view over the shared output area with `.collect = "view"`. On `.timeout`
expiry mid-submission, `mizu_map()` itself returns the `mizu_timeout`
sentinel, and
[`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
returns it on its own `timeout` expiry. A length-0 `.x` returns a handle
whose collect yields the empty result, as
[`lapply()`](https://rdrr.io/r/base/lapply.html) does, and creates no
region or tasks.

## Details

Unlike a loop of per-element
[`mizu_submit()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
calls, one `mizu_map()` call serializes `.f`, the constant arguments in
`...`, and `.x` once. It submits one *runner* task per live worker, and
each worker sets up that map context at most once. The remaining
per-element cost is one R function call, as in
[`lapply()`](https://rdrr.io/r/base/lapply.html). Runners schedule
themselves: they claim contiguous batches of elements from a shared
cursor in the map region and size each batch adaptively toward a fixed
time target. A trivial `.f` runs in large batches with near-zero
scheduling overhead, while an expensive or skewed `.f` settles on fine
claims that keep the workers balanced. An atomic, non-ALTREP `.x` with
no attributes beyond names travels as bare bytes: the workers slice
their batches directly from shared memory without deserializing `.x`,
and no worker holds more than a batch of it at a time.

Runners are ordinary pool tasks, stolen and balanced like any other
work. Worker death is detected and reported (see Errors), and every pool
invariant applies unchanged. Between batches, a runner also answers a
pool-wide doorbell. When another submitter's task arrives while every
worker is busy inside a map, one runner picks it up at its next batch
boundary, so pickup latency is bounded and independent of `length(.x)`.
A map runner picked up this way is not executed on the spot. The helper
moves it onto its own deque, where the next free worker steals it and
joins that map.

## Granularity

Elements are claimed in *morsels*: contiguous ranges of
`max(1, min(n %/% (workers * 256), 256))` elements. The morsel is the
granularity floor for cancellation, help, and loss reporting. Morsels go
to runners in adaptively sized batches of consecutive morsels.
`.n_chunks` overrides the morsel count (giving
`min(length(.x), .n_chunks)` morsels). There is no per-morsel shared
state, so `.n_chunks = length(x)` costs no extra memory and suits
extremely skewed element times. A map submitted while no worker is live
queues a single runner in the injection ring and runs when a worker
joins. If no worker has ever joined the pool, `mizu_map()` errors
instead, as
[`mizu_submit_call()`](https://shikokuchuo.net/mizu/dev/reference/mizu_call.md)
does. If the submitter's result slots are all occupied by outstanding
tasks, `mizu_map()` errors immediately, before it stages anything.

## Templates

`.template` works like
[`vapply()`](https://rdrr.io/r/base/lapply.html)'s `FUN.VALUE`: every
result must match its type and length exactly, or coerce upward (logical
-\> integer -\> double -\> complex, checked on the workers per element).
A classed `integer64` template (bit64's layout) is also supported.
Results must then be `integer64` of the template's length, since an
int64 result never widens to double, and the gathered vector or view
keeps the class. Results are written directly into a shared output area
and gathered in one copy, so no result is serialized. With
`.collect = "view"`, the gather copy is skipped as well, and the result
is a copy-on-write view over the shared output area itself, for
pipelines that reduce it immediately. A template of length `m > 1`
gathers an `m * length(.x)` matrix with the names of the template as row
names, as [`vapply()`](https://rdrr.io/r/base/lapply.html) does.
Character templates are assembled through the generic result path
instead, so their type checks surface at assembly rather than per
element on the workers. Large results of a consistent shape belong on
the template path. A runner's generic results accumulate on the worker
and publish once, so `.template` both caps worker memory and moves the
values across processes without serialization. For readers of
[`mizu_pool_stats()`](https://shikokuchuo.net/mizu/dev/reference/mizu_pool_stats.md):
a map with large *generic* results publishes them through the ordinary
result path, and can add a few result-slot `spills` per call even when
the pool's `slot_size` is right for its usual traffic.

## Errors, timeout, and cleanup

An error raised by `.f` is signalled again in the caller as the original
condition, with a `mizu_map_index` field naming the failing element (the
lowest index among the elements that ran). Failure is fail-fast: the
erroring runner sets the map's shared cancel word before publishing its
error, so every other runner stops within about one batch instead of
finishing the remaining elements. If a worker dies mid-map, the map
raises `mizu_error_worker_died` (see
[mizu_error](https://shikokuchuo.net/mizu/dev/reference/mizu_error.md))
with the lost elements in a `lost` field, a two-column matrix of
inclusive `lo, hi` ranges. Loss is reported conservatively, per runner.
A dead runner's results publish only at the end, so everything it
completed is reported as lost along with what it was executing, and a
lost element is never reported as completed. On `.timeout` expiry,
during submit or collect, the outstanding work is cancelled and the
`mizu_timeout` sentinel is returned, not raised. Executing runners
observe cancellation within about one batch (one element where `.f` is
expensive), independent of `length(.x)`. The slot of a published but
uncollected result is released only when the dropped handle's finalizer
runs at the next garbage collection, and the map's staging region is
deleted then too. A later map absorbs these temporarily occupied slots
by using fewer runners.

## Reproducible RNG

By default, nothing is guaranteed about random draws inside `.f`. The
workers seed lazily and independently, and the fast path pays nothing
for the option. `.seed` gives every element a reproducible L'Ecuyer-CMRG
stream: element `i` runs under the stream `i` jumps of 2^127 steps from
the base state that `set.seed(.seed, "L'Ecuyer-CMRG")` installs. The
caller's own `.Random.seed` is not touched, and the RNG state of each
worker is saved and restored around its batches. Because the streams are
per-element, the results are identical for any `.n_chunks` value, batch
sizing, worker count, or steal order. With `c(seed, offset)`, element
`i` runs under the stream of element `i + offset` of an unoffset run, so
a map split across runs or processes reproduces the streams of one
uninterrupted run.

## Nested maps

`mizu_map(mizu_current_pool(), ...)` inside a task expression uses the
handle of the worker running it. Runner submissions push onto the
worker's own deque. The blocked collect executes its own runners while
idle peers steal the rest, so nested parallelism has little overhead.
The first nested map on a worker claims a submitter slot, so with the
default `max_submitters = 8` (one held by the controller) at most 7
workers can nest at the same time. Raise `max_submitters` for wider
nested fan-outs.

## Cross-language maps

`.f` may be a
[`mizu_call()`](https://shikokuchuo.net/mizu/dev/reference/mizu_call.md)
specification instead of a function. This is how you map over a pool
whose workers are not R, such as one spawned with
[`mizu_py_launcher()`](https://shikokuchuo.net/mizu/dev/reference/mizu_py_launcher.md).
A spec `.f` always stages a shared map region: the descriptor crosses in
the interchange format, and each runner task carries a region reference
that any worker language reads. The map element fills the spec's first
positional argument (name kind) or binds as `x` (source kind), and the
spec's own constant arguments travel with it, so `...` must be empty
with a spec `.f`. Constants and elements must be portable values (the
interchange subset documented in
[`mizu_send()`](https://shikokuchuo.net/mizu/dev/reference/mizu_send.md)),
and a non-portable one raises `mizu_error_not_portable` at stage time.
`.template` and `.collect` work unchanged, with the results gathered (or
view-wrapped) by the submitter's own language, and a per-element error
crosses with its element index. `.seed` crosses as the language-neutral
`(seed, offset)` pair and each worker language derives its own
per-element streams. Batching and steal-order invariance holds within a
worker language, but the draws are not identical across languages. A
spec map runs on a same-language pool too, subject to the same
portability rules.

## Streaming maps

With `.stream = TRUE`, the map does not stage the whole of `.x` into
shared memory. It streams slices of `.x` to workers as they take work,
and the return value is unchanged. Fixed slices of `.x` ride ordinary
chunk tasks under a sliding submit/collect window of at most
`min(.n_chunks, 2 * live workers, free result slots)` outstanding tasks,
so shared-memory use is bounded by the window times the slice size
instead of the size of `.x`. With the default chunk count that is
roughly `(2 * workers) / .n_chunks` of the serialized `.x`. The chunk
count defaults to `min(length(.x), 32 * live workers)`, and `.n_chunks`
overrides it (`.n_chunks = length(.x)` is the mirai-style extreme of one
element per task). Everything else (result order, `.template` and
`.collect`, `.seed` invariance, the error behavior) is the same as for
the non-streaming map. Fail-fast latency grows from about one adaptive
morsel batch to about one chunk, because the outstanding work at any
time is bounded by the window, so the bound moves with `.n_chunks`.
Fixed chunks have two costs: the adaptive batch sizing of the morsel
scheduler is lost (raise `.n_chunks` to mitigate skew), and slices cross
serialized, so each chunk pays an ordinary submit's serialization
instead of the non-streaming map's serialization-free slicing. A
streaming map with R workers shares one description of the map with
every worker, and each worker reads that description at most once. A
[`mizu_call()`](https://shikokuchuo.net/mizu/dev/reference/mizu_call.md)
spec as `.f` streams too, over workers of any language. Each chunk in
the window then stages a small shared region of its own (the slice plus
the task that runs on it, one runner task per chunk), removed as soon as
the chunk completes. Shared-memory use stays bounded by the window plus
a small per-worker cache (up to 8 recent slices per worker) rather than
by the size of `.x`. Each chunk also writes its results separately, so
they cannot combine into one view: `.collect = "view"` errors on a
streaming spec map, as does preparing one with
[`mizu_map_prepare()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map_prepare.md).

## Very large x

The serialized runner wrapper needs a little over 200 bytes of entry
inline budget, so pools created with `slot_size = 256L` (224-byte
budget) fit it. The exception is `.seed`: its 6-word RNG state pushes
the wrapper to about 270 bytes. Seeded maps on such pools work but spill
a region per runner, so keep the default `slot_size` on pools meant for
seeded maps. For a very large `.x`, two paths avoid materializing it on
every worker. If the shared-memory use is acceptable, share it first: a
zero-copy view received from a channel or a pool result, or a
[`mori::share()`](https://rdrr.io/pkg/mori/man/share.html)d vector,
reduces to its 30-byte identifier inside the staged descriptor, and
workers read elements directly off the shared pages with OS demand
paging, so no worker copies any part of `.x` (`mizu_map()` itself never
calls mori). If `.x` must not be staged wholesale at all,
`.stream = TRUE` keeps only a window of slices in shared memory; see the
Streaming maps section.

As in [`lapply()`](https://rdrr.io/r/base/lapply.html), `.x` is indexed
with `[[` on the workers after an
[`as.list()`](https://rdrr.io/r/base/list.html) coercion of anything
that is not a plain vector, so a data.frame maps over its columns and a
factor over its elements.

## Examples

``` r
p <- mizu_pool()
m <- mizu_map(p, 1:10, function(i) i * 2L)
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
#> 
#> [[6]]
#> [1] 12
#> 
#> [[7]]
#> [1] 14
#> 
#> [[8]]
#> [1] 16
#> 
#> [[9]]
#> [1] 18
#> 
#> [[10]]
#> [1] 20
#> 

m <- mizu_map(p, rnorm(1e5), abs, .template = numeric(1))
v <- mizu_collect(m)

m <- mizu_map(p, 1:4, function(i) rnorm(2), .seed = 42L)
mizu_collect(m)
#> [[1]]
#> [1]  1.11932846 -0.07617141
#> 
#> [[2]]
#> [1] -0.2084809 -1.0341493
#> 
#> [[3]]
#> [1] 0.001100034 1.763058291
#> 
#> [[4]]
#> [1]  0.2262605 -0.4827515
#> 
mizu_pool_stop(p)
```
