# Parallel Map Over a Pool

Maps `.f` over the elements of `.x` on a pool and returns the results in
input order. The result is a list by default, or an atomic vector (or
matrix) with [`vapply()`](https://rdrr.io/r/base/lapply.html) semantics
when `.template` is given. Unlike mapping with per-element
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
calls, one `mizu_map()` call serializes `.f`, the constant arguments in
`...`, and `.x` exactly once. It submits one *runner* task per live
worker, and each worker materializes that map context at most once. The
per-element residual cost is one R closure call, as in
[`lapply()`](https://rdrr.io/r/base/lapply.html). Runners self-schedule:
they claim contiguous element batches off a shared cursor in the map
region and size each batch adaptively toward a fixed time target. A
trivial `.f` runs in large batches at near-zero scheduling overhead. An
expensive or skewed `.f` self-limits to fine claims that keep the
workers balanced. An atomic, non-ALTREP `.x` with no attributes beyond
names also travels as bare bytes. The workers slice their batches
straight from shared memory without deserializing `.x`, and no worker
ever materializes more than a batch of it.

## Usage

``` r
mizu_map(
  .pool,
  .x,
  .f,
  ...,
  .template = NULL,
  .chunks = NULL,
  .seed = NULL,
  .timeout = Inf,
  .collect = "value"
)
```

## Arguments

- .pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md).

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
  [`mizu_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
  specification maps over a pool of any worker language — see the
  Cross-language maps section.

- ...:

  further constant arguments to `.f`, staged once. As in
  [`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md),
  the formals ahead of `...` are dot-prefixed, so an argument name
  passed here never collides with them.

- .template:

  `NULL` for a list result, or a
  [`vapply()`](https://rdrr.io/r/base/lapply.html)-style `FUN.VALUE`: an
  atomic vector template that each result must match.

- .chunks:

  the morsel count of the map (its scheduling granularity), or `NULL`
  for the default. See the Granularity section.

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

A list of the results of `.f` in the order of `.x`, with `names(.x)`
reapplied. With `.template`, an atomic vector of type
`typeof(.template)` (an `m * length(.x)` matrix when
`length(.template) > 1`) — an owning vector, or a copy-on-write view
over the shared output area with `.collect = "view"`. On `.timeout`
expiry, the `mizu_timeout` sentinel.

## Details

Runners are ordinary pool tasks: they are stolen and balanced like any
other work. Worker death is detected and reported (see Errors), and
every pool invariant applies unchanged. Between batches, a runner also
answers a pool-wide doorbell. When the task of another submitter arrives
with every worker busy inside a map, one runner picks it up at its next
batch boundary. Foreign-task pickup latency is time-bounded and
independent of `length(.x)`. A runner claimed off the bell is not
executed there: a runner is the join ticket of a map. The helper moves
it onto its own deque instead, where the next free worker steals it and
joins that map.

## Granularity

Elements are claimed in *morsels*: contiguous ranges of
`max(1, min(n %/% (workers * 256), 256))` elements. The morsel is the
granularity floor for cancellation, help, and loss reporting. Morsels go
to runners in adaptively sized batches of consecutive morsels. `.chunks`
overrides the morsel count outright (`min(length(.x), .chunks)`
morsels). With no per-morsel shared state, `.chunks = length(x)` is
admissible at zero memory cost for pathological imbalance. A map
submitted while no worker is live queues a single runner in the
injection ring and runs when a worker joins. If no worker has ever
joined the pool, `mizu_map()` errors instead, as
[`mizu_submit_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
does. If the result-slot subrange of the submitter is fully occupied by
outstanding tasks, `mizu_map()` errors immediately, before it stages
anything.

## Templates

`.template` gives [`vapply()`](https://rdrr.io/r/base/lapply.html)
semantics: every result must match its type and length exactly, or
coerce upward (logical -\> integer -\> double -\> complex, checked on
the workers per element). A classed `integer64` template (bit64's
layout) is also supported: results must be `integer64` of the template's
length — int64 joins no widening lattice — and the gathered vector or
view keeps the class. Results are written directly into a shared output
area and gathered in one copy: zero result serializations. With
`.collect = "view"`, the gather copy is skipped as well: the result is a
copy-on-write view over the shared output area itself, for pipelines
that immediately reduce. A template of length `m > 1` gathers an
`m * length(.x)` matrix, with the names of the template as row names, as
[`vapply()`](https://rdrr.io/r/base/lapply.html) does. Character
templates are assembled through the generic result path instead. Their
type checks then surface at assembly, not per element on the workers.
Big shape-regular results belong on the template path: the generic
results of a runner accumulate on the worker and publish once, so
`.template` both caps worker memory and moves the values cross-process
without serialization. Note for readers of
[`mizu_pool_stats()`](https://shikokuchuo.net/mizu/reference/mizu_pool_stats.md):
a map with large *generic* results publishes them through the ordinary
result framing. Such a map can add a few result-slot `spills` per call
even when the `slot_size` of the pool is right for its usual traffic.

## Errors, timeout, and cleanup

An error raised by `.f` signals again in the caller as the original
condition. A `mizu_map_index` field names the failing element: the first
by element index among the elements that ran. Failure is fail-fast. The
erroring runner sets the shared cancel word of the map before it
publishes. So every peer stops within about one batch instead of
draining the remaining elements. If a worker dies mid-map, the map
raises `mizu_error_worker_died` (see
[mizu_error](https://shikokuchuo.net/mizu/reference/mizu_error.md)),
carrying the lost elements as an `elements` field: a two-column matrix
of inclusive `lo, hi` ranges. Loss is reported runner-granular and
conservatively. The results of a dead runner publish only at the end, so
everything it completed is reported lost alongside what it was
executing, never the reverse. On `.timeout` expiry, mid-submit or
mid-collect, the outstanding work is cancelled and the `mizu_timeout`
sentinel is returned, never raised. Executing runners observe
cancellation within about one batch (one element where `.f` is
expensive), independent of `length(.x)`. The slot of a
published-uncollected result is released only when the finalizer of the
dropped handle runs at the next garbage collection. The staging region
of the map is likewise unlinked at GC. A subsequent map absorbs this
transient occupancy by clamping its runner count.

## Reproducible RNG

By default nothing is guaranteed about random draws inside `.f`: the
workers seed lazily and independently, and the fast path pays nothing
for the option. `.seed` opts into reproducible per-element L'Ecuyer-CMRG
streams: element `i` runs under the stream `i` jumps of 2^127 steps from
the base state that `set.seed(.seed, "L'Ecuyer-CMRG")` installs. The own
`.Random.seed` of the caller is not touched, and the RNG state of each
worker is saved and restored around its batches. Because the streams are
per-element, the results are identical for any `.chunks` value, batch
sizing, worker count, or steal order. With `c(seed, offset)`, element
`i` runs under the stream of element `i + offset` of an unoffset run, so
a map split across runs or processes reproduces the streams of one
uninterrupted run.

## Nested maps

`mizu_map(mizu_current_pool(), ...)` inside a task expression uses the
evaluating worker's own handle. Runner submissions push onto the own
deque of the worker. The blocked collect executes its own runners while
idle peers steal the rest: fork/join-shaped recursive parallelism at
deque cost. The first nested map of a worker claims a submitter slot. So
at the default `max_submitters = 8` (one held by the controller) at most
7 workers can nest concurrently. Raise `max_submitters` for wider nested
fan-outs.

## Cross-language maps

`.f` may be a
[`mizu_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
specification instead of a function — the way to map over a foreign pool
(one spawned with
[`mizu_py_pool_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_py_pool_launcher.md),
or any pool whose workers are not R). A spec `.f` always stages a shared
map region: the descriptor crosses in the interchange format and each
runner task carries a region reference any worker language reads. The
map element fills the spec's first positional argument (name kind) or
binds as `x` (source kind), and the spec's own constant arguments ride
with it — so `...` must be empty with a spec `.f`. Constants and
elements must be portable values (the interchange subset documented in
[`mizu_send()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)); a
non-portable one raises `mizu_error_not_portable` at stage time.
`.template` and `.collect` work unchanged — the output area is
wire-typed slots gathered (or view-wrapped) by the submitter's own
binding — and a per-element error crosses with its element index.
`.seed` carries as the language-neutral `(seed, offset)` pair and each
worker language derives its own per-element streams: batching- and
steal-order invariance holds within a worker language, but the draws are
not identical across languages. A spec map runs on a same-language pool
too, subject to the same portability rules.

## Very large x

The serialized runner wrapper needs a little over 200 bytes of entry
inline budget, so pools created with `slot_size = 256L` (224-byte
budget) fit it. The exception is `.seed`: its 6-word RNG state pushes
the wrapper to about 270 bytes. Seeded maps on such pools work but spill
a region per runner, so keep the default `slot_size` on pools meant for
seeded maps. For a very large `.x`, sharing it first is the recommended
path: a zero-copy view received from a channel or a pool result, or a
[`mori::share()`](https://rdrr.io/pkg/mori/man/share.html)d vector,
reduces to its ~30-byte identifier inside the staged descriptor, and
workers read elements straight off the shared pages with OS demand
paging — no worker copies any part of `.x` (`mizu_map` itself never
calls mori).

As in [`lapply()`](https://rdrr.io/r/base/lapply.html), `.x` is indexed
with `[[` on the workers after an
[`as.list()`](https://rdrr.io/r/base/list.html) coercion of anything
that is not a plain vector. So a data.frame maps over its columns, and a
factor over its elements.

## Examples

``` r
p <- mizu_pool()
mizu_map(p, 1:10, function(i) i * 2L)
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
v <- mizu_map(p, rnorm(1e5), abs, .template = numeric(1))
mizu_map(p, 1:4, function(i) rnorm(2), .seed = 42L)
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
