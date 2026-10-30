# mizu_map: parallel map over a pool. One call stages f / `...` / x once
# (a single descriptor stream in one fresh map region — or entirely inline
# in chunk tasks when it fits the entry budget), then submits one *runner*
# task per live worker: runners self-schedule element ranges off a shared
# cursor in the map region (morsel-driven scheduling), one adaptive-sized
# batch per mizu_map_next transition, and publish their batch histories as
# their single ordinary result. Workers materialize the map context at
# most once each (cached on the worker handle, prot[5]); a RAWVEC-eligible
# x is sliced per batch straight from the mapping, never deserialized; the
# template path writes results into a shared output area so n results move
# cross-process unserialized. The region-less blob path keeps fixed
# chunks: the morsel state needs the region, and at those sizes chunk
# overhead is already negligible. The stages below are composable so the
# deterministic harness can interleave pool_step() between submit and
# collect — mizu_map merely composes them.

# Worker-side function references, built once at install time. String form
# keeps the own-namespace ::: out of the code tree (both are internal, and
# the call must resolve on a worker that has only loaded the namespace).
# Arguments ride as positional literals embedded in the call itself, with
# an empty task-args list: vectors self-evaluate, and skipping the
# named-argument bindings keeps the serialized wrapper inside a
# slot_size = 256 pool's 224-byte entry inline budget (the runner's pool
# is self-resolved: mizu_current_pool() as its first body line).
map_chunk_ref <- str2lang("mizu:::map_chunk")
map_runner_ref <- str2lang("mizu:::map_runner")
map_stream_chunk_ref <- str2lang("mizu:::map_stream_chunk")

map_payload <- function(st, r) {
  seeded <- !is.null(st[["seed_state"]])
  expr <- as.call(c(
    list(map_chunk_ref, r, st[["blob"]]),
    if (seeded) list(st[["seed_state"]])
  ))
  list(expr, list())
}

# One runner task's wire payload: map_runner(n, a[, s]) — a packs
# c(ordinal, generation) as doubles, with a third element flagging the
# template path (one vector, not three scalars: the difference between
# fitting a slot_size = 256 entry budget and not).
runner_payload <- function(st, r) {
  seeded <- !is.null(st[["seed_state"]])
  expr <- as.call(c(
    list(
      map_runner_ref,
      st[["name"]],
      c(r, st[["gen"]], if (st[["direct"]]) 1)
    ),
    if (seeded) list(st[["seed_state"]])
  ))
  list(expr, list())
}

map_template_types <- c("logical", "integer", "double", "complex", "raw")

# Morsel geometry: target ~256 morsels per runner, clamped to a constant
# grain (the morsel cap) so the responsiveness floor — cancellation, help,
# lost-set granularity — stays n-independent. Hard-coded, no user knob;
# the gate sweep freezes the value ({256, 1024} candidates).
map_morsel_cap <- 256
map_morsels_per_runner <- 256

# The pool's own monotonic clock. The one map deadline is computed against
# it once and threaded through submit and collect absolute — the _try
# entries convert to a remaining budget at entry — so R-side expiry checks
# and C-side waits read a single timescale, with no per-call allocation.
mono_time <- function() .Call(mizu_now_call)

#' Parallel Map Over a Pool
#'
#' Maps `.f` over the elements of `.x` on a pool and returns a map handle
#' immediately — as [mizu_submit()] returns a task. [mizu_collect()] on
#' the handle waits for completion and returns the results in input
#' order: a list by default, or an atomic vector (or matrix) with
#' [vapply()]-style results when `.template` is given. [mizu_cancel()]
#' stops it. `as.promise()` (or `then()` directly) collects it through
#' the event loop instead — see [mizu_collect_all_async()]. Unlike
#' mapping with per-element [mizu_submit()] calls, one `mizu_map()` call
#' serializes `.f`, the constant arguments in `...`, and `.x` exactly once.
#' It submits one *runner* task per live worker, and each worker
#' materializes that map context at most once. The remaining per-element
#' cost is one R function call, as in [lapply()]. Runners self-schedule:
#' they claim contiguous element batches off a shared cursor in the map
#' region and size each batch adaptively toward a fixed time target. A
#' trivial `.f` runs in large batches at near-zero scheduling overhead. An
#' expensive or skewed `.f` self-limits to fine claims that keep the
#' workers balanced. An atomic, non-ALTREP `.x` with no attributes beyond
#' names also travels as bare bytes. The workers slice their batches
#' straight from shared memory without deserializing `.x`, and no worker
#' ever materializes more than a batch of it.
#'
#' Runners are ordinary pool tasks: they are stolen and balanced like any
#' other work. Worker death is detected and reported (see Errors), and
#' every pool invariant applies unchanged. Between batches, a runner also
#' answers a pool-wide doorbell: when another submitter's task arrives
#' while every worker is busy inside a map, one runner picks it up at
#' its next batch boundary. Pickup latency is bounded and independent of
#' `length(.x)`. A map runner picked up this way is not executed on the
#' spot: the helper moves it onto its own deque instead, where the next
#' free worker steals it and joins that map.
#'
#' @section Granularity:
#' Elements are claimed in *morsels*: contiguous ranges of
#' `max(1, min(n %/% (workers * 256), 256))` elements. The morsel is the
#' granularity floor for cancellation, help, and loss reporting. Morsels
#' go to runners in adaptively sized batches of consecutive morsels.
#' `.chunks` overrides the morsel count outright (`min(length(.x), .chunks)`
#' morsels). With no per-morsel shared state, `.chunks = length(x)` is
#' admissible at zero memory cost for extremely skewed element times. A
#' map submitted while no worker is live queues a single runner in the
#' injection ring and runs when a worker joins. If no worker has ever
#' joined the pool, `mizu_map()` errors instead, as `mizu_submit_call()`
#' does. If the result-slot subrange of the submitter is fully occupied
#' by outstanding tasks, `mizu_map()` errors immediately, before it
#' stages anything.
#'
#' @section Templates:
#' `.template` is [vapply()]'s `FUN.VALUE`: every result must match its
#' type and length exactly, or coerce upward (logical -> integer ->
#' double -> complex, checked on the workers per element). A classed
#' `integer64` template (bit64's layout) is also supported: results must
#' be `integer64` of the template's length — an int64 result never
#' widens to double — and the gathered vector or view keeps the class.
#' Results are written directly into a shared output area and gathered
#' in one copy: zero result serializations. With `.collect = "view"`,
#' the gather copy is skipped as well: the result is a copy-on-write
#' view over the shared output area itself, for pipelines that
#' immediately reduce. A template of length `m > 1` gathers an
#' `m * length(.x)` matrix, with the names of the template as row names, as
#' `vapply()` does. Character templates are assembled through the generic
#' result path instead. Their type checks then surface at assembly, not
#' per element on the workers. Big results of a consistent shape belong
#' on the template path: a runner's generic results accumulate on the
#' worker and publish once, so `.template` both caps worker memory and
#' moves the values across processes without serialization. Note for
#' readers of [mizu_pool_stats()]: a map with large *generic* results
#' publishes them through the ordinary result path. Such a map can add a
#' few result-slot `spills` per call even when the `slot_size` of the
#' pool is right for its usual traffic.
#'
#' @section Errors, timeout, and cleanup:
#' An error raised by `.f` signals again in the caller as the original
#' condition. A `mizu_map_index` field names the failing element: the first
#' by element index among the elements that ran. Failure is fail-fast: the
#' erroring runner sets the map's shared cancel word before publishing
#' its error, so every other runner stops within about one batch instead
#' of finishing the remaining elements. If a worker dies mid-map, the map
#' raises `mizu_error_worker_died` (see [mizu_error]), carrying the lost
#' elements as an `elements` field: a two-column matrix of inclusive
#' `lo, hi` ranges. Loss is reported conservatively, per runner: a dead
#' runner's results publish only at the end, so everything it completed
#' counts as lost alongside what it was executing, never the reverse. On
#' `.timeout` expiry, mid-submit or mid-collect, the outstanding work is
#' cancelled and the `mizu_timeout` sentinel is returned, never raised.
#' Executing runners observe cancellation within about one batch (one
#' element where `.f` is expensive), independent of `length(.x)`. The
#' slot of a published-uncollected result is released only when the
#' dropped handle's finalizer runs at the next garbage collection. The
#' map's staging region is likewise deleted at garbage collection. A
#' later map absorbs these temporarily occupied slots by using fewer
#' runners.
#'
#' @section Reproducible RNG:
#' By default nothing is guaranteed about random draws inside `.f`: the
#' workers seed lazily and independently, and the fast path pays nothing
#' for the option. `.seed` opts into reproducible per-element
#' L'Ecuyer-CMRG streams: element `i` runs under the stream `i` jumps of
#' 2^127 steps from the base state that `set.seed(.seed, "L'Ecuyer-CMRG")`
#' installs. The caller's own `.Random.seed` is not touched, and the
#' RNG state of each worker is saved and restored around its batches.
#' Because the streams are per-element, the results are identical for any
#' `.chunks` value, batch sizing, worker count, or steal order. With
#' `c(seed, offset)`, element `i` runs under the stream of element
#' `i + offset` of an unoffset run, so a map split across runs or
#' processes reproduces the streams of one uninterrupted run.
#'
#' @section Nested maps:
#' `mizu_map(mizu_current_pool(), ...)` inside a task expression uses the
#' handle of the worker running it. Runner submissions push onto the
#' worker's own deque. The blocked collect executes its own
#' runners while idle peers steal the rest: nested parallelism at minimal
#' overhead. The first nested map of a worker claims a
#' submitter slot. So at the default `max_submitters = 8` (one held by the
#' controller) at most 7 workers can nest concurrently. Raise
#' `max_submitters` for wider nested fan-outs.
#'
#' @section Cross-language maps:
#' `.f` may be a [mizu_call()] specification instead of a function — the
#' way to map over a foreign pool (one spawned with
#' [mizu_py_launcher()], or any pool whose workers are not R). A spec
#' `.f` always stages a shared map region: the descriptor crosses in the
#' interchange format and each runner task carries a region reference any
#' worker language reads. The map element fills the spec's first
#' positional argument (name kind) or binds as `x` (source kind), and the
#' spec's own constant arguments ride with it — so `...` must be empty
#' with a spec `.f`. Constants and elements must be portable values (the
#' interchange subset documented in [mizu_send()]); a non-portable one
#' raises `mizu_error_not_portable` at stage time. `.template` and
#' `.collect` work unchanged — the results are gathered (or
#' view-wrapped) by the submitter's own language — and a
#' per-element error crosses with its element index. `.seed` carries as
#' the language-neutral `(seed, offset)` pair and each worker language
#' derives its own per-element streams: batching- and steal-order
#' invariance holds within a worker language, but the draws are not
#' identical across languages. A spec map runs on a same-language pool
#' too, subject to the same portability rules.
#'
#' @section Streaming maps:
#' With `.stream = TRUE`, the map never stages the whole of `.x` into
#' shared memory: it streams slices of `.x` to workers as they take work;
#' the return value is unchanged. Fixed x-slices ride ordinary chunk tasks
#' under a sliding submit/collect window of at most
#' `min(.chunks, 2 * live workers, free result slots)` outstanding tasks,
#' so shared-memory use is bounded by `window x slice` instead of the
#' size of `.x` — with the default chunk count that is roughly
#' `(2 * workers) / .chunks` of the serialized `.x`. The chunk count
#' defaults to `min(length(.x), 32 * live workers)` and `.chunks`
#' overrides it outright (`.chunks = length(.x)` is the mirai-style
#' extreme of one element per task). Everything else — result order,
#' `.template` and `.collect`, `.seed` invariance, the error behavior —
#' is exactly the non-streaming map's. Fail-fast latency grows from
#' about one adaptive morsel batch to about one chunk: the outstanding
#' work at any time is bounded by the window, so the bound moves with
#' `.chunks`. Two trade-offs come with fixed chunks: the adaptive batch
#' sizing of the morsel scheduler is lost (to mitigate skew, raise
#' `.chunks`), and slices cross serialized, so each chunk pays an
#' ordinary submit's serialization instead of the non-streaming map's
#' serialization-free slicing. A streaming map with R
#' workers shares one description of the map with every worker, and each
#' worker reads that description at most once. A [mizu_call()] spec as
#' `.f` streams too, over workers of any language. Each chunk in the
#' window then stages a small shared region of its own (the slice plus
#' the task that runs on it, one runner task per chunk), removed as soon
#' as the chunk completes. Shared-memory use stays bounded by the window
#' plus a small per-worker cache (up to 8 recent slices per worker)
#' rather than by the size of `.x`. Each chunk also writes its results
#' separately, so they cannot combine into one view:
#' `.collect = "view"` errors on a streaming spec map, as does preparing
#' one with [mizu_map_prepare()].
#'
#' @section Very large x:
#' The serialized runner wrapper needs a little over 200 bytes of entry
#' inline budget, so pools created with `slot_size = 256L` (224-byte
#' budget) fit it. The exception is `.seed`: its 6-word RNG state pushes
#' the wrapper to about 270 bytes. Seeded maps on such pools work but
#' spill a region per runner, so keep the default `slot_size` on pools
#' meant for seeded maps. For a very large `.x`, two paths avoid
#' materializing it per worker. If the shared-memory use is acceptable,
#' share it first: a zero-copy view received from a channel or a pool
#' result, or a `mori::share()`d vector, reduces to its ~30-byte
#' identifier inside the staged descriptor, and workers read elements
#' straight off the shared pages with OS demand paging — no worker copies
#' any part of `.x` (`mizu_map` itself never calls mori). If `.x` must
#' not be staged wholesale at all, `.stream = TRUE` keeps only a window
#' of slices in shared memory — see the Streaming maps section.
#'
#' As in [lapply()], `.x` is indexed with `[[` on the workers after an
#' `as.list()` coercion of anything that is not a plain vector. So a
#' data.frame maps over its columns, and a factor over its elements.
#'
#' @inheritParams mizu_submit
#' @param .x a vector (atomic or list) to map over. Anything else is
#'   coerced with `as.list()`, as [lapply()] does.
#' @param .f a function (or, as [match.fun()] accepts, its name) applied as
#'   `.f(.x[[i]], ...)`. Serialized once with its enclosing environment.
#'   Keep that environment small, as with any cross-process map. A
#'   [mizu_call()] specification maps over a pool of any worker language —
#'   see the Cross-language maps section.
#' @param ... further constant arguments to `.f`, staged once. As in
#'   [mizu_submit()], the formals ahead of `...` are dot-prefixed, so an
#'   argument name passed here never collides with them.
#' @param .template `NULL` for a list result, or a [vapply()]-style
#'   `FUN.VALUE`: an atomic vector template that each result must match.
#' @param .chunks the morsel count of the map (its scheduling
#'   granularity), or `NULL` for the default. See the Granularity section.
#' @param .seed `NULL` (default: no RNG guarantees, no cost), or a numeric
#'   vector of length 1 or 2 that derives reproducible per-element RNG
#'   streams: `seed`, or `c(seed, offset)` to shift every element's
#'   stream by `offset` positions. See the Reproducible RNG section.
#' @param .timeout seconds after which submission gives up, cancels the
#'   tasks already in, and returns the `mizu_timeout` sentinel (class
#'   `c("mizu_timeout", "mizu_sentinel")`). `Inf` (the default) waits
#'   indefinitely. Submission blocks only while an injection ring is full;
#'   the collection deadline is [mizu_collect()]'s `timeout`.
#' @param .stream `TRUE` streams slices of `.x` to workers under a
#'   sliding window instead of staging `.x` into shared memory wholesale,
#'   bounding shared-memory use by the window times the slice size.
#'   See the Streaming maps section. A streaming map handle collects
#'   with [mizu_collect()] only (it cannot be promise-armed).
#' @param .collect `"value"` (the default) gathers template results into
#'   an owning R vector at [mizu_collect()]. `"view"` instead returns an
#'   ALTREP view over the map's shared output area — no gather copy — for
#'   pipelines that immediately reduce. Requires an atomic `.template`.
#'   The view is copy-on-write: the first write materializes a private
#'   copy. It keeps the map region alive until released, and re-sending
#'   it through a channel or pool crosses as a full copy, not by
#'   reference. The choice is stored on the handle.
#'
#' @return A map handle (class `"mizu_map"`). [mizu_collect()] on it
#'   returns a list of the results of `.f` in the order of `.x`, with
#'   `names(.x)` reapplied. With `.template`, an atomic vector of type
#'   `typeof(.template)` (an `m * length(.x)` matrix when
#'   `length(.template) > 1`) — an owning vector, or a copy-on-write view
#'   over the shared output area with `.collect = "view"`. On `.timeout`
#'   expiry mid-submission, `mizu_map()` itself returns the
#'   `mizu_timeout` sentinel; [mizu_collect()] returns it on its own
#'   `timeout` expiry. A length-0 `.x` returns a handle whose collect
#'   yields the empty result, as [lapply()] — no region or tasks are
#'   created.
#'
#' @examples
#' p <- mizu_pool()
#' m <- mizu_map(p, 1:10, function(i) i * 2L)
#' mizu_collect(m)
#'
#' m <- mizu_map(p, rnorm(1e5), abs, .template = numeric(1))
#' v <- mizu_collect(m)
#'
#' m <- mizu_map(p, 1:4, function(i) rnorm(2), .seed = 42L)
#' mizu_collect(m)
#' mizu_pool_stop(p)
#'
#' @export
mizu_map <- function(
  .pool,
  .x,
  .f,
  ...,
  .template = NULL,
  .chunks = NULL,
  .seed = NULL,
  .timeout = Inf,
  .stream = FALSE,
  .collect = "value"
) {
  ident <- map_check_native(.pool, .f)
  dots <- list(...)
  .f <- map_check_f(.f, dots)
  map_template_check(.template)
  map_collect_check(.collect, .template)
  if (.stream && inherits(.f, "mizu_call") && identical(.collect, "view")) {
    stop(
      "mizu: .collect = \"view\" cannot compose a streaming spec map \u2014 ",
      "per-chunk output areas cannot compose into one view",
      call. = FALSE
    )
  }
  if (length(.x) == 0L) {
    return(map_empty_handle(.pool, .x, .template, .collect))
  }
  st <- if (.stream) {
    map_stage_stream(.pool, .x, .f, dots, .template, .chunks, .seed, ident)
  } else {
    map_stage(.pool, .x, .f, dots, .template, .chunks, .seed, ident)
  }
  map_submit_handle(.pool, st, .timeout, .collect)
}

# The length-0 map: a handle like any other (the return type never
# varies), whose collect yields the lapply/vapply empty result. No
# region, no tasks, no finalizer — nothing to cancel.
map_empty_handle <- function(pool, x, template, collect) {
  st <- new.env(parent = emptyenv())
  st[["empty"]] <- TRUE
  st[["stream"]] <- FALSE
  st[["n"]] <- 0L
  st[["nms"]] <- names(x)
  st[["template"]] <- template
  st[["blob"]] <- NULL
  st[["name"]] <- "no region (empty map)"
  m <- new.env(parent = emptyenv())
  m[["st"]] <- st
  m[["pool"]] <- pool
  m[["collect"]] <- collect
  m[["pm"]] <- NULL
  m[["done"]] <- FALSE
  m[["promised"]] <- FALSE
  m[["cancelled"]] <- FALSE
  class(m) <- "mizu_map"
  m
}

map_empty_result <- function(st) {
  map_empty(structure(list(), names = st[["nms"]]), st[["template"]])
}

# The one submit path shared by mizu_map (stage + submit on an anonymous
# state) and mizu_map_run (a prepared state, re-armed by the caller):
# submit the tasks against the submission deadline — the backstop cancels
# the tasks already in when submission raises or the deadline expires —
# then return the handle. The handle owns the map from here: blocking
# collection through mizu_collect(), event-driven through as.promise(),
# mizu_cancel() to stop it, and a GC finalizer cancels a dropped
# uncollected map, so no run of .f is wasted.
map_submit_handle <- function(pool, st, timeout, collect = "value") {
  # as mizu_r_timeout_arg: an NA/NaN would read as an indefinite wait
  if (!is.numeric(timeout) || length(timeout) != 1L || is.na(timeout)) {
    stop("mizu: .timeout must be a number of seconds, not NA", call. = FALSE)
  }
  deadline <- if (is.finite(timeout)) mono_time() + timeout else Inf
  ok <- FALSE
  on.exit(if (!ok) map_cancel(st))
  if (isTRUE(st[["stream"]])) {
    map_submit_stream(pool, st, deadline)
  } else {
    map_submit(pool, st, deadline)
  }
  if (st[["timed_out"]]) {
    return(.Call(mizu_map_timeout_call))
  }
  m <- new.env(parent = emptyenv())
  m[["st"]] <- st
  m[["pool"]] <- pool
  m[["collect"]] <- collect
  m[["pm"]] <- NULL
  m[["done"]] <- FALSE
  m[["promised"]] <- FALSE
  m[["cancelled"]] <- FALSE
  class(m) <- "mizu_map"
  # the finalizer mirrors the sync backstop for a dropped UNCOLLECTED
  # handle: the cancel word stops executing runners within ~a batch, and
  # the C entries no-op on closed handles. It must not fire after a
  # collect — a prepared handle's state lives on for the next run (the
  # closure receives the handle at call time, so nothing pins it).
  reg.finalizer(
    m,
    function(e) {
      if (!isTRUE(e[["done"]])) {
        map_cancel(e[["st"]])
      }
    },
    onexit = TRUE
  )
  ok <- TRUE
  m
}

# The cancelled-map failure, raised by mizu_collect() and the promise
# settle alike when the cancel word was set externally before any runner
# published (mizu_cancel(), or a pool stop cancelling the queued runners).
map_cond_cancelled <- function() {
  errorCondition(
    "mizu: map cancelled or pool stopped",
    class = c("mizu_error_cancelled", "mizu_error")
  )
}

# The blocking collect of a map handle (mizu_collect's map case): the
# existing map_collect machinery one verb deeper — the gather (or the
# "view" wrap), element-indexed errors, the lost-set scan — against the
# collect deadline, with the sync backstop armed for interrupts. One
# collection per handle, as for tasks.
map_collect_handle <- function(m, timeout) {
  if (isTRUE(m[["done"]])) {
    stop("mizu: map handle is already collected", call. = FALSE)
  }
  if (isTRUE(m[["promised"]])) {
    stop(
      "mizu: map handle has a pending promise and refuses a blocking ",
      "collect \u2014 one consumer per result",
      call. = FALSE
    )
  }
  if (!is.numeric(timeout) || length(timeout) != 1L || is.na(timeout)) {
    stop("mizu: timeout must be a number of seconds, not NA", call. = FALSE)
  }
  if (isTRUE(m[["cancelled"]])) {
    stop(map_cond_cancelled())
  }
  m[["done"]] <- TRUE
  st <- m[["st"]]
  if (isTRUE(st[["empty"]])) {
    return(map_empty_result(st))
  }
  deadline <- if (is.finite(timeout)) mono_time() + timeout else Inf
  on.exit(map_cancel(st))
  r <- if (isTRUE(st[["stream"]])) {
    map_collect_stream(st, deadline, m[["collect"]])
  } else {
    map_collect(st, deadline, m[["collect"]])
  }
  # a clean value collect hands the state back to a prepared handle (a
  # view collect consumed its region); timeout and error leaves it stale
  pm <- m[["pm"]]
  if (
    !is.null(pm) && !inherits(r, "mizu_timeout") && !isTRUE(st[["consumed"]])
  ) {
    pm[["st"]] <- st
  }
  r
}

#' Prepared Maps: Stage Once, Run Many
#'
#' `mizu_map_prepare()` stages a map — `.f`, the constant arguments in
#' `...`, and `.x` — serialized once into a shared map region, without
#' running it. It returns a prepared-map handle. Each `mizu_map_run()`
#' then costs only task submission and collection: no serialization and
#' no region create. Because the region (and its name) stays alive across
#' runs, workers that ran a previous run reuse their cached map context
#' instead of re-attaching. Repeated stochastic simulation is the headline
#' use. `mizu_map_run(pm, .seed = i)` varies the RNG streams per run for
#' free: the seed state rides the runner payloads, not the region.
#'
#' Between runs, the region's shared scheduling state resets in O(1). The
#' cursor and cancel word clear, and the run generation embedded in every
#' claim word advances. A leftover task from a previous run can never
#' interfere with the new run. After an unclean run — a
#' `.timeout` expiry, an error in `.f`, a worker death — the handle is
#' marked stale. The next `mizu_map_run()` restages into a fresh region
#' automatically (the old one is deleted at garbage collection, once any
#' leftover tasks are done with it). A run collected with
#' `.collect = "view"` restages likewise: the returned view keeps its
#' region alive, so the next run stages fresh rather than reusing pages
#' a held view still reads. A map small enough to ride entirely inline
#' keeps its staged blob on the handle instead: runs resubmit it, still
#' skipping the serialization.
#'
#' The prepared handle keeps the staged `.x` (for automatic restaging)
#' and the map region alive for its lifetime. Both release at garbage
#' collection when the handle is dropped. The number of chunks is fixed
#' at prepare time. The runner count adapts to the live workers at each
#' run.
#'
#' @inheritParams mizu_map
#'
#' @return `mizu_map_prepare()`: a prepared-map handle. `mizu_map_run()`:
#'   exactly what [mizu_map()] returns for the staged map — a map handle
#'   whose [mizu_collect()] returns a list, a templated atomic vector,
#'   or the `mizu_timeout` sentinel.
#'
#' @examples
#' p <- mizu_pool()
#' pm <- mizu_map_prepare(p, 1:1000, function(i, draws) {
#'   mean(rnorm(draws)) * i
#' }, draws = 100L)
#' runs <- lapply(1:50, function(s) {
#'   m <- mizu_map_run(pm, .seed = s)
#'   mizu_collect(m)
#' })
#' mizu_pool_stop(p)
#'
#' @export
mizu_map_prepare <- function(
  .pool,
  .x,
  .f,
  ...,
  .template = NULL,
  .chunks = NULL,
  .stream = FALSE
) {
  ident <- map_check_native(.pool, .f)
  dots <- list(...)
  .f <- map_check_f(.f, dots)
  map_template_check(.template)
  if (.stream && inherits(.f, "mizu_call")) {
    stop(
      "mizu: a prepared map with a mizu_call() spec as '.f' cannot stream ",
      "\u2014 a spec stream stages its chunk regions at run time, so there ",
      "is nothing to prepare",
      call. = FALSE
    )
  }
  pm <- new.env(parent = emptyenv())
  pm[["pool"]] <- .pool
  pm[["x"]] <- .x
  pm[["f"]] <- .f
  pm[["dots"]] <- dots
  pm[["template"]] <- .template
  pm[["chunks"]] <- .chunks
  pm[["stream"]] <- .stream
  if (length(.x) > 0L) {
    pm[["st"]] <- if (.stream) {
      map_stage_stream(.pool, .x, .f, dots, .template, .chunks, ident = ident)
    } else {
      map_stage(.pool, .x, .f, dots, .template, .chunks, ident = ident)
    }
  }
  class(pm) <- "mizu_map_prepared"
  pm
}

#' @section Replacing x between runs:
#' `mizu_map_run(pm, .x = x2)` runs over a replacement `.x`. When both the
#' staged and the replacement `.x` are bare-byte eligible (atomic,
#' non-ALTREP, no attributes beyond names) with identical type and length,
#' the swap is in place. The new bytes are copied over the `.x` section of
#' the region. This runs the iterate-over-same-shape loop (optimizer
#' steps, simulation sweeps) at the cost of one copy, skipping the region
#' create and the re-attach of every worker. Any other change of `.x` — a
#' different shape or type, a list, a map staged inline — restages
#' automatically on the next run. A streaming prepared map keeps `.x`
#' in the submitting process, so a replacement `.x` of any shape
#' re-slices on the next run — only a length change under `.template`
#' restages (the output area is sized for the staged length).
#'
#' @rdname mizu_map_prepare
#' @param pm a prepared-map handle from [mizu_map_prepare()].
#' @export
mizu_map_run <- function(
  pm,
  .x = NULL,
  .seed = NULL,
  .timeout = Inf,
  .collect = "value"
) {
  if (!inherits(pm, "mizu_map_prepared")) {
    stop("mizu: not a prepared-map handle", call. = FALSE)
  }
  map_collect_check(.collect, pm[["template"]])
  stream <- isTRUE(pm[["stream"]])
  if (!is.null(.x)) {
    if (stream) map_swap_x_stream(pm, .x) else map_swap_x(pm, .x)
  }
  if (length(pm[["x"]]) == 0L) {
    return(map_empty_handle(
      pm[["pool"]],
      pm[["x"]],
      pm[["template"]],
      .collect
    ))
  }
  st <- pm[["st"]]
  if (is.null(st)) {
    # unclean previous run (or a prior restage failure): stage afresh — a
    # straggler against the old region dies at its exhausted cursor or
    # stale-generation claim word, and the region unlinks at GC
    st <- if (stream) {
      map_stage_stream(
        pm[["pool"]],
        pm[["x"]],
        pm[["f"]],
        pm[["dots"]],
        pm[["template"]],
        pm[["chunks"]],
        .seed
      )
    } else {
      map_stage(
        pm[["pool"]],
        pm[["x"]],
        pm[["f"]],
        pm[["dots"]],
        pm[["template"]],
        pm[["chunks"]],
        .seed
      )
    }
  } else {
    # a rearm failure (transient slot exhaustion) raises before the reset
    # touches anything, so the staged state stays good for a retry; from
    # the reset on, pessimism rules — restage unless the run ends clean
    if (stream) {
      map_rearm_stream(pm[["pool"]], st, .seed)
    } else {
      map_rearm(pm[["pool"]], st, .seed)
    }
    pm[["st"]] <- NULL
  }
  m <- map_submit_handle(pm[["pool"]], st, .timeout, .collect)
  if (inherits(m, "mizu_timeout")) {
    # the backstop cancelled the run; the state is dead — restage next run
    return(m)
  }
  # the prepared back-ref: a clean collect restores the state for re-arm
  # (a view collect consumes the region, so the next run restages)
  m[["pm"]] <- pm
  m
}

# The integer64 gates: the bare form — the class is the entire attribute
# set — is the stage-side rule (the wire tag carries the class, consumed
# at stage and re-applied per slice); the class form the assemble-side one
# (a gathered vector keeps no attributes but its class, and typeof shared
# with double means typeof alone cannot gate).
map_is_int64_bare <- function(x) {
  identical(attributes(x), list(class = "integer64"))
}

map_is_int64_class <- function(x) {
  typeof(x) == "double" && identical(class(x), "integer64")
}

# lapply's coercion rule, so [[ on the workers sees what lapply's would.
# The exception is class-only integer64: it stays a vector and rides the
# raw x section, its class re-applied to each slice (and to each element
# at batch time) — list-coercing it would forfeit the bare-bytes section
map_coerce_x <- function(x) {
  if ((!is.vector(x) || is.object(x)) && !map_is_int64_bare(x)) {
    x <- as.list(x)
  }
  x
}

# The x-section attribute gate, mirroring the C raw gate: bare, names-only
# (names stay submitter-side for assembly), or class-only integer64 (the
# wire tag carries the class — consumed at stage, re-applied per slice).
map_x_attrs_ok <- function(x) {
  attrs <- attributes(x)
  is.null(attrs) || identical(names(attrs), "names") || map_is_int64_bare(x)
}

# The x-section wire type: class-only integer64 stages as int64 (int64 and
# double share typeof "double", so typeof alone cannot gate a swap).
map_wire_type <- function(x) {
  if (typeof(x) == "double" && map_is_int64_bare(x)) "integer64" else typeof(x)
}

# Replace a prepared map's x: an in-place memcpy over the region's x
# section when the staged and replacement x are both bare-byte eligible
# with identical wire type and length (a RAWVEC x is sliced from the
# mapping per batch, never cached worker-side, so the swap is invisible to
# the workers) — anything else drops the staged state, and the next run
# restages with the new x (descriptor-carried x IS cached worker-side,
# so shape or type changes must re-key the region).
map_swap_x <- function(pm, x) {
  st <- pm[["st"]]
  swappable <- !is.null(st) &&
    is.null(st[["blob"]]) &&
    isTRUE(st[["xraw"]]) &&
    map_wire_type(x) == map_wire_type(pm[["x"]]) &&
    length(x) == length(pm[["x"]]) &&
    .Call(mizu_map_eligible, x) >= 0 &&
    map_x_attrs_ok(x)
  pm[["x"]] <- x
  if (swappable) {
    .Call(mizu_map_swap_x, st[["wrap"]], x)
    st[["nms"]] <- names(x)
  } else {
    pm[["st"]] <- NULL
  }
  invisible(pm)
}

# Replace a prepared streaming map's x: no swap gate — the region holds
# only f and dots, so any shape works by re-slicing, and only the chunk
# geometry recomputes when n changes. The exception is the template path:
# its output area sizes off the staged n, so a length change restages.
map_swap_x_stream <- function(pm, x) {
  st <- pm[["st"]]
  x <- map_coerce_x(x)
  pm[["x"]] <- x
  if (is.null(st)) {
    return(invisible(pm))
  }
  n <- length(x)
  if (st[["direct"]] && n != st[["n"]]) {
    pm[["st"]] <- NULL
    return(invisible(pm))
  }
  st[["x"]] <- x
  st[["nms"]] <- names(x)
  if (n != st[["n"]]) {
    st[["n"]] <- n
    map_stream_geometry(st, .Call(mizu_pool_map_caps, pm[["pool"]]))
  }
  invisible(pm)
}

# The wire form of .seed — the language-neutral (seed, offset) pair as
# doubles, NULL when unseeded. One validation path shared by
# map_seed_state (the native CMRG derivation) and a spec map's kind-2
# runner fields (the pair itself rides the stream; the worker's own
# language derives from it).
map_seed_pair <- function(seed) {
  if (is.null(seed)) {
    return(NULL)
  }
  n <- length(seed)
  if (!is.numeric(seed) || n == 0L || n > 2L) {
    stop(
      "mizu: .seed must be a numeric vector of length 1 or 2",
      call. = FALSE
    )
  }
  s <- seed[1L]
  if (!is.finite(s) || s != floor(s) || abs(s) > .Machine$integer.max) {
    stop("mizu: .seed[1] must be an integer", call. = FALSE)
  }
  if (n == 1L) {
    return(c(s, 0))
  }
  offset <- seed[2L]
  if (
    !is.finite(offset) || offset < 0 || offset != floor(offset) || offset > 2^53
  ) {
    stop(
      "mizu: .seed[2] (stream offset) must be a non-negative integer no larger than 2^53",
      call. = FALSE
    )
  }
  c(s, offset)
}

# The worker-side RNG guard shared by map_chunk and map_runner: captures
# the worker's own .Random.seed (possibly absent — workers seed lazily) and
# the RNG kinds, and returns the nullary restore the caller registers with
# on.exit, so a seeded batch leaves the worker's RNG state exactly as it
# found it. The kinds ride along: f's first draw off the installed
# L'Ecuyer-CMRG state switches the process-global generator, and restoring
# .Random.seed alone leaves the next unseeded draw randomizing under CMRG.
# The full kind triple is captured, not just kind[1]: the .Random.seed
# restore re-decodes normal.kind from the seed word only when a seed exists
# — on a lazily seeded worker an f that switched normal.kind or sample.kind
# would otherwise leak into the worker's next task.
rng_save <- function() {
  os <- get0(".Random.seed", envir = globalenv(), inherits = FALSE)
  rk <- RNGkind()
  function() {
    if (!identical(RNGkind(), rk)) {
      RNGkind(rk[[1L]], rk[[2L]], rk[[3L]])
    }
    if (is.null(os)) {
      if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
        rm(".Random.seed", envir = globalenv())
      }
    } else {
      assign(".Random.seed", os, envir = globalenv())
    }
  }
}

# One validation path for .seed, shared by map_stage and map_rearm: NULL
# stays NULL (unseeded), a scalar derives the base CMRG state, and a
# length-2 vector pre-seeks that base by seed[2] stream jumps — exact by
# jump composition (A^a A^b = A^(a+b) over the jump matrices), so element
# i runs under the stream of element i + seed[2] of an unoffset run, and
# c(s, 0) is identical to s. Runs once per map, never per element.
map_seed_state <- function(seed) {
  pair <- map_seed_pair(seed)
  if (is.null(pair)) {
    return(NULL)
  }
  base <- .Call(mizu_map_rng_base, pair[[1L]])
  if (pair[[2L]] == 0) {
    return(base)
  }
  # mizu_map_rng_seek truncates k to uint64_t: Inf and huge offsets error
  # on the C side ("invalid stream index")
  .Call(mizu_map_rng_seek, base, pair[[2L]])
}

# One runner per live worker (floored at 1 so a workerless map still
# queues), clamped by the submitter's free result slots and the injection
# ring — and by the morsel count where that is known
map_runner_count <- function(caps, nm = Inf) {
  as.integer(min(nm, max(1L, caps[[1L]]), caps[[2L]], caps[[3L]]))
}

# The slots-exhausted failure, raised at stage and rearm alike: before
# any region exists, so the map leaves nothing behind.
stop_slots_exhausted <- function() {
  stop_mizu(
    "mizu_error_slots_exhausted",
    paste0(
      "mizu: result slots exhausted \u2014 collect or cancel ",
      "outstanding tasks first"
    )
  )
}

# The map caps probe with the slots-exhausted guard: free_rs counts FREE
# slots in this submitter's own subrange (claiming a worker's submitter
# slot on nested first use); zero errors here, before any region exists.
map_caps <- function(pool) {
  caps <- .Call(mizu_pool_map_caps, pool)
  if (caps[[2L]] == 0L) {
    stop_slots_exhausted()
  }
  caps
}

# The worker-death failure, raised from either collect path with the lost
# element ranges attached (blob path: the chunk range; region path: the
# lost-set scan's arithmetic).
stop_worker_died <- function(slot, pid, elts) {
  stop_mizu(
    "mizu_error_worker_died",
    sprintf(
      "mizu: worker died while executing map elements %s",
      map_ranges_label(elts)
    ),
    slot = slot,
    pid = pid,
    elements = elts
  )
}

# Re-arm a staged map state for another run: per-run seed state (it rides
# the runner payloads, never the region), a fresh runner count against
# the live workers, and — on the region path — the O(1) shared-state
# reset whose bumped generation fences every straggler from the run
# before. The staged morsel geometry is inherited: batching absorbs
# worker-count drift between runs.
map_rearm <- function(pool, st, seed) {
  if (isTRUE(st[["spec"]])) {
    st[["seed_pair"]] <- map_seed_pair(seed)
  } else {
    st[["seed_state"]] <- map_seed_state(seed)
  }
  if (is.null(st[["blob"]])) {
    caps <- map_caps(pool)
    st[["gen"]] <- .Call(mizu_map_reset, st[["wrap"]])
    st[["R"]] <- map_runner_count(caps, st[["nm"]])
    st[["live0"]] <- caps[[1L]]
    st[["handles"]] <- vector("list", st[["R"]])
  } else {
    st[["handles"]] <- vector("list", st[["C"]])
  }
  st[["timed_out"]] <- FALSE
  invisible(st)
}

# Re-arm a staged streaming map for another run: per-run seed state, a
# fresh window against the live workers, and the O(1) reset that clears
# the shared state (streaming reads none of it — the cancel word included
# — but the reset keeps the region indistinguishable from a fresh one).
# The chunk geometry is inherited from staging or the last swap.
map_rearm_stream <- function(pool, st, seed) {
  st[["seed_state"]] <- map_seed_state(seed)
  caps <- map_caps(pool)
  .Call(mizu_map_reset, st[["wrap"]])
  map_stream_window(st, caps)
  st[["handles"]] <- vector("list", st[["C"]])
  st[["timed_out"]] <- FALSE
  invisible(st)
}

# The map guard at the entry points: a pool no worker has ever joined
# fails fast for any f (plain mizu_submit() queues by design, but a fused
# submit+collect would park in collect forever). On a foreign pool a
# native f fails fast too (its runner tasks are same-language private
# frames that would otherwise each fail remotely, one error per runner);
# a spec f takes the cross-language path on any pool, needing the pool
# word already set: the descriptor's target byte stages once, at stage
# time. Language byte 2 is R. Returns the pool word, which never resets
# once set — the spec paths downstream take it rather than reading again.
map_check_native <- function(pool, f) {
  ident <- .Call(mizu_pool_ident, pool)
  if (is.null(ident)) {
    stop("mizu: no worker has joined this pool", call. = FALSE)
  }
  if (inherits(f, "mizu_call") || ident[[1L]] == 2L) {
    return(ident)
  }
  stop(
    "mizu: this pool's workers are not R - mizu_map() needs a mizu_call() ",
    "spec as '.f' on a foreign pool",
    call. = FALSE
  )
}

# The '.f' gate shared by the map entry points: a mizu_call() spec crosses
# as-is (its constants ride the spec, so '...' must be empty), anything
# else resolves as an ordinary function.
map_check_f <- function(f, dots) {
  if (inherits(f, "mizu_call")) {
    if (length(dots)) {
      stop(
        "mizu: constant arguments ride the mizu_call() spec \u2014 '...' must ",
        "be empty with a spec '.f'",
        call. = FALSE
      )
    }
    return(f)
  }
  match.fun(f)
}

map_collect_check <- function(collect, template) {
  if (identical(collect, "value")) {
    return(invisible())
  }
  if (!identical(collect, "view")) {
    stop("mizu: .collect must be \"value\" or \"view\"", call. = FALSE)
  }
  if (is.null(template) || !typeof(template) %in% map_template_types) {
    stop(
      "mizu: .collect = \"view\" requires an atomic .template ",
      "(logical, integer, double, complex or raw)",
      call. = FALSE
    )
  }
  invisible()
}

map_template_check <- function(template) {
  if (is.null(template)) {
    return(invisible())
  }
  if (
    !typeof(template) %in% c(map_template_types, "character") ||
      length(template) == 0L
  ) {
    stop(
      "mizu: .template must be a logical, integer, double, complex, ",
      "raw or character vector of positive length",
      call. = FALSE
    )
  }
  invisible()
}

# The n == 0 result, before any region or task exists: what lapply /
# vapply return for empty input.
map_empty <- function(x, template) {
  if (is.null(template)) {
    out <- vector("list", 0L)
    names(out) <- names(x)
    return(out)
  }
  out <- vector(typeof(template), 0L)
  if (map_is_int64_class(template)) {
    class(out) <- "integer64"
  }
  m <- length(template)
  if (m > 1L) {
    dim(out) <- c(m, 0L)
    tn <- names(template)
    if (!is.null(tn)) dimnames(out) <- list(tn, NULL)
  }
  out
}

strip_srcref <- function(f) .Call(mizu_strip_srcref, f)

# The .chunks gate, identical at every stage entry.
map_check_chunks <- function(chunks) {
  if (is.null(chunks)) {
    return(NULL)
  }
  chunks <- as.numeric(chunks)
  if (length(chunks) != 1L || is.na(chunks) || chunks < 1) {
    stop("mizu: .chunks must be a positive number", call. = FALSE)
  }
  chunks
}

# The balanced-range split of n into st[["C"]] chunks — sizes differing by
# at most one, the leading chunks one element longer — filling the 1-based
# inclusive lo/hi ranges (the blob path and the streaming geometry).
map_set_ranges <- function(st, n) {
  C <- st[["C"]]
  size <- n %/% C
  sizes <- rep.int(size, C)
  extra <- n %% C
  if (extra > 0) {
    sizes[seq_len(extra)] <- size + 1
  }
  st[["hi"]] <- cumsum(sizes)
  st[["lo"]] <- st[["hi"]] - sizes + 1
  invisible(st)
}

# Stage one map call: the RAWVEC gate, the region-less probe, morsel
# geometry, and the region create — in that order, so slot exhaustion
# errors before anything exists. Returns the mutable map state the other
# stages share; mizu_map's frame holds it (and with it the region's
# producer wrap) for the map's duration.
map_stage <- function(
  pool,
  x,
  f,
  dots,
  template = NULL,
  chunks = NULL,
  seed = NULL,
  ident = NULL
) {
  spec <- inherits(f, "mizu_call")
  # srcrefs would serialize each closure's source (and its srcfile
  # environment) into the descriptor: stripping them keeps staged sizes
  # deterministic across keep.source settings — often the difference
  # between the region-less path and a region
  if (typeof(f) == "closure") {
    f <- strip_srcref(f)
  }
  x <- map_coerce_x(x)
  n <- length(x)
  map_template_check(template)
  direct <- !is.null(template) && typeof(template) %in% map_template_types

  st <- new.env(parent = emptyenv())
  st[["n"]] <- n
  st[["nms"]] <- names(x)
  st[["template"]] <- template
  st[["direct"]] <- direct
  st[["spec"]] <- spec
  # a spec map on foreign workers collects the workers' own runner shape
  # (pymizu's element-range histories), not this binding's morsel pairs
  st[["pymap"]] <- FALSE
  st[["seed_state"]] <- if (spec) NULL else map_seed_state(seed)
  st[["seed_pair"]] <- if (spec) map_seed_pair(seed) else NULL

  caps <- map_caps(pool)
  chunks <- map_check_chunks(chunks)

  # the raw x-section gate: type / ALTREP / S4 only — the as.list
  # coercion above owns the attribute rule
  xlen <- .Call(mizu_map_eligible, x)
  st[["xraw"]] <- xlen >= 0

  # Region-less probe (generic maps only — the template path needs the
  # region's output area, and a spec map always stages a region: the
  # inline chunk tasks are same-language private frames a foreign worker
  # cannot run): does the full chunk payload, wrapper plus the single
  # descriptor stream as an ordinary argument, pass the bounded check
  # against the entry inline budget? The bounded pass doubles as the
  # descriptor count when the region is needed after all. Skipped when a
  # RAWVEC x alone already exceeds the budget, so a huge x is never
  # serialized just to learn it does not fit.
  inline_entry <- caps[[4L]]
  desc_len <- NULL
  if (!spec && is.null(template) && !(st[["xraw"]] && xlen > inline_entry)) {
    bl <- .Call(mizu_bounded_call, list(f, dots, x), inline_entry)
    desc_len <- bl[[1L]]
    if (!is.null(bl[[2L]])) {
      st[["blob"]] <- bl[[2L]]
      if (
        is.null(.Call(
          mizu_bounded_call,
          map_payload(st, c(1, 1)),
          inline_entry
        )[[2L]])
      ) {
        st[["blob"]] <- NULL
      }
    }
  }

  if (!is.null(st[["blob"]])) {
    # region-less blob path: today's fixed chunks — the morsel state needs
    # the region, and at these sizes chunk overhead is already negligible
    C <- min(
      n,
      if (is.null(chunks)) 8 * caps[[1L]] else chunks,
      caps[[2L]],
      caps[[3L]]
    )
    st[["C"]] <- max(1L, as.integer(C))
    map_set_ranges(st, n)
    st[["handles"]] <- vector("list", st[["C"]])
  } else {
    # region path: x rides its own bare-byte section when the gate admits
    # it (the descriptor then carries only f and dots — cheap to count),
    # else folded into the one descriptor stream, whose exact size the
    # probe already counted. .chunks overrides the morsel count outright,
    # bypassing the cap (no per-morsel storage exists, so .chunks = n is
    # admissible at zero memory cost); the default targets
    # ~map_morsels_per_runner morsels per runner under the constant grain.
    st[["ms"]] <- if (is.null(chunks)) {
      runners <- map_runner_count(caps)
      max(1, min(n %/% (runners * map_morsels_per_runner), map_morsel_cap))
    } else {
      ceiling(n / min(n, floor(chunks)))
    }
    st[["nm"]] <- ceiling(n / st[["ms"]])
    st[["gen"]] <- 0
    if (spec) {
      # the cross-language path: one 'I' descriptor (the f spec nested as
      # a task tag, the list-x bare or nil when the raw section carries
      # it) and kind-2 runner tasks, on any pool language. A non-raw
      # atomic x (ALTREP, attributed) coerces to the element list — the
      # same values the native loop's [[ would hand f
      # the pool word: read at the entry points; the restage paths fetch
      # it here
      ident <- if (is.null(ident)) .Call(mizu_pool_ident, pool) else ident
      st[["pymap"]] <- ident[[1L]] != 2L
      desc <- .Call(
        mizu_interop_map_desc_call,
        f,
        if (st[["xraw"]]) {
          NULL
        } else if (.Call(mizu_zc_view_check, x)) {
          # a view x crosses as one ref leaf (F1's D6): the workers read
          # the resolved view off the shared pages, never an element walk
          x
        } else {
          as.list(x)
        },
        ident[[1L]]
      )
      sr <- .Call(
        mizu_map_stage,
        desc,
        if (st[["xraw"]]) x,
        NULL,
        n,
        if (direct) template,
        st[["ms"]]
      )
    } else {
      desc <- if (st[["xraw"]]) list(f, dots) else list(f, dots, x)
      sr <- .Call(
        mizu_map_stage,
        desc,
        if (st[["xraw"]]) x,
        if (!st[["xraw"]]) desc_len,
        n,
        if (direct) template,
        st[["ms"]]
      )
    }
    st[["name"]] <- sr[[1L]]
    st[["wrap"]] <- sr[[2L]]
    st[["R"]] <- map_runner_count(caps, st[["nm"]])
    st[["live0"]] <- caps[[1L]]
    st[["handles"]] <- vector("list", st[["R"]])
  }
  st[["timed_out"]] <- FALSE
  st
}

# The sliding window: min(C, 2 x live workers, free result slots) — one
# chunk executing per worker plus one covering the collect -> refill ->
# claim gap, floored at 1 (zero free slots already errored at stage).
map_stream_window <- function(st, caps) {
  st[["W"]] <- max(
    1L,
    as.integer(min(st[["C"]], 2 * max(1L, caps[[1L]]), caps[[2L]]))
  )
  invisible(st)
}

# Streaming chunk geometry: C fixed chunks over n — min(n, 32 x live
# workers) by default, .chunks overriding outright, bounded only by n (the
# window paces outstanding work, so no slot/ring clamp, unlike the blob
# path; .chunks = n is the mirai extreme of one element per task). lo/hi
# via map_set_ranges, shared with the blob path.
map_stream_geometry <- function(st, caps) {
  n <- st[["n"]]
  C <- min(n, if (is.null(st[["chunks"]])) 32 * caps[[1L]] else st[["chunks"]])
  st[["C"]] <- max(1L, as.integer(C))
  map_set_ranges(st, n)
  map_stream_window(st, caps)
}

# Stage a streaming map: x stays submitter-side and feeds fixed slices
# through ordinary chunk tasks under a sliding submit/collect window, so
# SHM residency is bounded by window x slice instead of sizeof(x). The
# region is descriptor-only — f + dots, plus the template output area
# (sized off the explicit n, independent of x) — through the existing
# mizu_map_stage with x = NULL: one code path and the worker's ctx cache.
# Its morsel geometry is inert (no streaming path claims, cursors, or
# reads the cancel word; fail-fast rides task-level cancellation over the
# bounded window), so one morsel keeps the header minimal. A spec f
# cannot ride same-language chunk tasks, so each in-flight chunk stages
# its own small map region ('I' descriptor + one-morsel geometry) with
# exactly one kind-2 runner (ordinal 0, generation 0), closed at the
# chunk's own turn — parallelism and the residency bound are unchanged.
map_stage_stream <- function(
  pool,
  x,
  f,
  dots,
  template = NULL,
  chunks = NULL,
  seed = NULL,
  ident = NULL
) {
  spec <- inherits(f, "mizu_call")
  if (typeof(f) == "closure") {
    f <- strip_srcref(f)
  }
  # lapply's coercion rule, as map_stage (the class-only integer64
  # exception included: it stays a vector, its slices re-classed)
  x <- map_coerce_x(x)
  n <- length(x)
  map_template_check(template)
  direct <- !is.null(template) && typeof(template) %in% map_template_types

  st <- new.env(parent = emptyenv())
  st[["stream"]] <- TRUE
  st[["spec"]] <- spec
  st[["n"]] <- n
  st[["nms"]] <- names(x)
  st[["x"]] <- x
  st[["template"]] <- template
  st[["direct"]] <- direct
  st[["m"]] <- if (direct) length(template)

  caps <- map_caps(pool)
  chunks <- map_check_chunks(chunks)
  st[["chunks"]] <- chunks
  map_stream_geometry(st, caps)

  if (spec) {
    # the cross-language stream: no single region — one per in-flight
    # chunk, staged at refill time. The chunk's raw x section takes
    # exactly map_stage's verdicts (a bare, non-ALTREP atomic x — an
    # ALTREP or attributed x coerces to the element list, the same
    # values the non-streaming spec map hands f), so the 'I' descriptor
    # builds once with x nil; a list x's slice rides its own descriptor
    xlen <- .Call(mizu_map_eligible, x)
    xraw <- xlen >= 0
    if (!xraw) {
      x <- as.list(x)
      st[["x"]] <- x
    }
    ident <- if (is.null(ident)) .Call(mizu_pool_ident, pool) else ident
    st[["pymap"]] <- ident[[1L]] != 2L
    st[["xraw"]] <- xraw
    st[["f"]] <- f
    st[["lang"]] <- ident[[1L]]
    st[["desc"]] <- if (xraw) {
      .Call(mizu_interop_map_desc_call, f, NULL, ident[[1L]])
    }
    st[["seed_pair"]] <- map_seed_pair(seed)
    # the refill composes each chunk's offset as sp[[2]] + lo - 1 in
    # doubles — exact only to 2^53, where the native path's jump
    # composition never rounds; decline rather than diverge
    sp <- st[["seed_pair"]]
    if (!is.null(sp) && sp[[2L]] > 2^53 - (n - 1)) {
      stop(
        "mizu: .seed[2] + length(.x) must stay within 2^53, the exact ",
        "seed offset range",
        call. = FALSE
      )
    }
    st[["wraps"]] <- vector("list", st[["C"]])
    st[["handles"]] <- vector("list", st[["C"]])
    st[["timed_out"]] <- FALSE
    return(st)
  }
  st[["seed_state"]] <- map_seed_state(seed)

  sr <- .Call(
    mizu_map_stage,
    list(f, dots),
    NULL,
    NULL,
    n,
    if (direct) template,
    n
  )
  st[["name"]] <- sr[[1L]]
  st[["wrap"]] <- sr[[2L]]
  st[["handles"]] <- vector("list", st[["C"]])
  st[["timed_out"]] <- FALSE
  st
}

# One streaming chunk task's wire payload: map_stream_chunk(n, r, x[, s])
# — `n` names the descriptor region, `r` packs c(lo, hi) as doubles, `x`
# is the submitter-side slice embedded as a call literal (the blob path's
# embedding pattern): it crosses via the ordinary staging tiers and is
# GC-freed after submit, so only W serialized slices are ever resident.
# `s` is the 6-word RNG base state when seeded.
map_stream_payload <- function(st, k) {
  seeded <- !is.null(st[["seed_state"]])
  expr <- as.call(c(
    list(
      map_stream_chunk_ref,
      st[["name"]],
      c(st[["lo"]][[k]], st[["hi"]][[k]]),
      .Call(mizu_map_xslice, st[["x"]], st[["lo"]][[k]], st[["hi"]][[k]])
    ),
    if (seeded) list(st[["seed_state"]])
  ))
  list(expr, list())
}

# The streaming run, decomposed so the pool_pair() harness can interleave
# pool_step() between the window's turns, exactly as the blob path's
# stages: prime submits up to W chunk tasks (unflagged, like blob chunks),
# each turn collects one completion and refills one chunk, and
# map_submit_stream / map_collect_stream compose them against the map's
# one deadline. Expiry marks the state timed out and surfaces as the
# sentinel; the callers' on.exit backstop cancels the outstanding window.

# Refill the window up to W (or C). FALSE marks the state timed out — the
# deadline expired before or inside a ring-space wait. The pre-check ahead
# of each payload build mirrors map_submit's: a nested submit never waits
# on ring space, so an expired deadline must be caught here. The spec
# variant stages the chunk's region and submits its one kind-2 runner
# (ordinal 0, generation 0), the seed pair offset by the chunk base.
map_stream_refill <- function(pool, st, deadline) {
  C <- st[["C"]]
  W <- min(st[["W"]], C)
  next_k <- st[["next_k"]]
  oi <- st[["oi"]]
  spec <- isTRUE(st[["spec"]])
  while (next_k <= C && length(oi) < W) {
    if (mono_time() >= deadline) {
      st[["timed_out"]] <- TRUE
      return(FALSE)
    }
    h <- if (spec) {
      lo <- st[["lo"]][[next_k]]
      hi <- st[["hi"]][[next_k]]
      n_k <- hi - lo + 1
      desc <- if (st[["xraw"]]) {
        st[["desc"]]
      } else {
        .Call(
          mizu_interop_map_desc_call,
          st[["f"]],
          st[["x"]][lo:hi],
          st[["lang"]]
        )
      }
      sr <- .Call(
        mizu_map_stage,
        desc,
        if (st[["xraw"]]) .Call(mizu_map_xslice, st[["x"]], lo, hi),
        NULL,
        n_k,
        if (st[["direct"]]) st[["template"]],
        n_k
      )
      st[["wraps"]][[next_k]] <- sr[[2L]]
      sp <- st[["seed_pair"]]
      .Call(
        mizu_pool_submit_map_runner,
        pool,
        list(
          sr[[1L]],
          0,
          if (is.null(sp)) NULL else c(sp[[1L]], sp[[2L]] + lo - 1)
        ),
        deadline
      )
    } else {
      .Call(
        mizu_pool_submit_try,
        pool,
        map_stream_payload(st, next_k),
        deadline,
        0L
      )
    }
    if (inherits(h, "mizu_timeout")) {
      st[["timed_out"]] <- TRUE
      return(FALSE)
    }
    st[["handles"]][[next_k]] <- h
    oi <- c(oi, next_k)
    next_k <- next_k + 1L
  }
  st[["next_k"]] <- next_k
  st[["oi"]] <- oi
  TRUE
}

# Prime the run: the generic-results accumulator and the outstanding set
# are per-run state, then the first window fills. A spec stream's
# template path accumulates the flat n x m vector each chunk's gather
# splices into at its turn.
map_stream_prime <- function(pool, st, deadline) {
  st[["out"]] <- if (st[["direct"]]) {
    if (isTRUE(st[["spec"]])) {
      vector(typeof(st[["template"]]), st[["n"]] * st[["m"]])
    }
  } else {
    vector("list", st[["n"]])
  }
  st[["oi"]] <- integer(0L)
  st[["next_k"]] <- 1L
  map_stream_refill(pool, st, deadline)
}

# The fail path of a streaming turn, NORET: cancel the outstanding window,
# then drain the siblings non-blockingly. A blind collect of a
# just-cancelled task races the worker's cancel consume (its publish or
# pre-eval skip frees the slot, which a collect reads as already
# collected), so each sibling is probed first and only a stable terminal
# outcome (ok / err / died) is collected — pending and cancelled siblings
# carry no outcome and are ignored. A worker death reports its chunk's
# range (the blob-path shape), drained deaths adding theirs; otherwise the
# minimum-element-index error among those observed raises — first by
# element index among the elements that ran, the set that ran already
# depending on completion order. An error carrying no element index is not
# f's (pool stop, infrastructure) and stays fatal, as the runner path's.
# The spec arm (spec = TRUE) reads every error through map_err_index
# offset by its chunk's base — `v` arrives from collect_any with "index"
# the window position and "mizu_map_index" the chunk-local element; the
# drained siblings collect per handle (no stamp), so their chunk is known.
# Each observed error's fields normalize to the global element index
# before the minimum raises.
map_stream_fail <- function(st, v, k, spec) {
  died <- NULL
  elts <- NULL
  errs <- list()
  index_of <- function(e) e[["mizu_map_index"]]
  globalize <- function(e, kk) e
  if (spec) {
    index_of <- map_err_index
    globalize <- function(e, kk) {
      i <- map_err_index(e)
      gi <- i + st[["lo"]][[kk]] - 1
      if (is.integer(i) && gi <= .Machine$integer.max) {
        gi <- as.integer(gi)
      }
      e[["mizu_map_index"]] <- gi
      if (!is.null(e[["index"]])) {
        e[["index"]] <- gi
      }
      e
    }
  }
  if (inherits(v, "mizu_error_worker_died")) {
    died <- v
    elts <- rbind(elts, c(lo = st[["lo"]][[k]], hi = st[["hi"]][[k]]))
  } else if (!is.null(v[["mizu_map_index"]])) {
    # the element-index test is mizu_map_index only: collect_any stamps
    # every raised outcome with "index" the window position, and
    # map_err_index's fallback would fabricate an element index for an
    # infrastructure error — those take the stop arm below
    errs <- list(globalize(v, k))
  } else {
    stop(v)
  }
  oi <- st[["oi"]]
  hl <- st[["handles"]][oi]
  map_cancel(st)
  now <- mono_time()
  for (i in seq_along(oi)) {
    state <- .Call(mizu_pool_task_state, hl[[i]])
    if (state != "ok" && state != "err" && state != "died") {
      next
    }
    w <- .Call(mizu_pool_collect_try, hl[[i]], now)
    if (!is.object(w) || !inherits(w, "mizu_caught")) {
      next
    }
    w <- w[[1L]]
    if (inherits(w, "mizu_error_worker_died")) {
      # the first death reports slot/pid; every death adds its range
      if (is.null(died)) {
        died <- w
      }
      elts <- rbind(
        elts,
        c(lo = st[["lo"]][[oi[[i]]]], hi = st[["hi"]][[oi[[i]]]])
      )
    } else if (!is.null(index_of(w))) {
      errs[[length(errs) + 1L]] <- globalize(w, oi[[i]])
    } else if (!inherits(w, "mizu_error_cancelled")) {
      stop(w)
    }
  }
  if (!is.null(died)) {
    stop_worker_died(died[["slot"]], died[["pid"]], elts)
  }
  idx <- vapply(errs, function(e) as.numeric(e[["mizu_map_index"]]), 0)
  stop(errs[[which.min(idx)]])
}

# One streaming turn: collect the next completed chunk (parking on
# collect_any against the remaining deadline), splice generic results into
# place (template results are already in the region's output area; the
# chunk's NULL result is drained and dropped), and refill. Errors take
# the fail path above; deadline expiry marks the state timed out. The spec
# turn splices by worker language (foreign element ranges through
# mizu_map_splice_ix, R morsel pairs through mizu_map_splice chunk-locally),
# gathers the template path's per-chunk output area into the flat
# accumulator, and closes the chunk's region at its own turn.
map_stream_turn <- function(pool, st, deadline) {
  oi <- st[["oi"]]
  remaining <- deadline - mono_time()
  v <- if (remaining > 0) {
    tryCatch(
      .Call(mizu_pool_collect_any, st[["handles"]][oi], remaining),
      error = identity
    )
  } else {
    .Call(mizu_map_timeout_call)
  }
  if (inherits(v, "mizu_timeout")) {
    st[["timed_out"]] <- TRUE
    return(invisible())
  }
  # an infrastructure error from collect_any (pool stopped, owner dead)
  # carries no position stamp — fail with it, not a subscript error
  if (is.null(v[["index"]])) {
    stop(v)
  }
  pos <- v[["index"]]
  k <- oi[[pos]]
  st[["oi"]] <- oi[-pos]
  st[["handles"]][k] <- list(NULL)
  spec <- isTRUE(st[["spec"]])
  if (inherits(v, "condition")) {
    map_stream_fail(st, v, k, spec)
  }
  if (spec) {
    lo <- st[["lo"]][[k]]
    hi <- st[["hi"]][[k]]
    wrap <- st[["wraps"]][[k]]
    st[["wraps"]][k] <- list(NULL)
    if (st[["direct"]]) {
      # rows [lo, hi] of the flat n x m accumulator, one memcpy splice —
      # an R-level `[<-` allocates the seq.int index and dispatches per chunk
      .Call(
        mizu_map_asplice,
        st[["out"]],
        (lo - 1) * st[["m"]] + 1,
        .Call(mizu_map_gather, wrap)
      )
    } else if (st[["pymap"]]) {
      .Call(mizu_map_splice_ix, st[["out"]], list(v[["value"]]), lo - 1)
    } else {
      local <- vector("list", hi - lo + 1)
      .Call(mizu_map_splice, local, list(v[["value"]]), hi - lo + 1)
      .Call(mizu_map_vsplice, st[["out"]], lo, local)
    }
    .Call(mizu_map_close, wrap)
  } else if (!st[["direct"]]) {
    # the splice is one C pass (the region path's assembly discipline): an
    # R-level `[<-` would dispatch per element, per chunk
    .Call(mizu_map_vsplice, st[["out"]], st[["lo"]][[k]], v[["value"]])
  }
  map_stream_refill(pool, st, deadline)
  invisible()
}

# The handle-first split of the streaming run: map_submit_stream primes
# the window at mizu_map() / mizu_map_run() (work starts at the call),
# map_collect_stream drives the turns at mizu_collect(). The on.exit
# backstop of each caller cancels the outstanding window on unwind.
map_submit_stream <- function(pool, st, deadline) {
  st[["pool"]] <- pool
  map_stream_prime(pool, st, deadline)
  invisible(st)
}

map_collect_stream <- function(st, deadline, collect = "value") {
  while (length(st[["oi"]]) && !st[["timed_out"]]) {
    map_stream_turn(st[["pool"]], st, deadline)
  }
  if (st[["timed_out"]]) {
    return(.Call(mizu_map_timeout_call))
  }
  map_assemble(st, st[["out"]], collect)
}

# Submit the map's tasks — R runner tasks on the region path, C chunk
# tasks on the blob path — through the ordinary submit path, against the
# map's one deadline. A deadline expiring mid-submit (before a task, or
# inside a ring-space wait) cancels the tasks already in and marks the
# state timed out — the caller returns the sentinel. Runners are ordinary
# tasks — stolen, balanced, reaped like any other work — except flagged
# MIZU_ENTRY_RUNNER on the wire, so a doorbell help beat re-homes one onto
# the helper's own deque instead of executing a join ticket nested. Chunk
# tasks stay unflagged: bounded work, no cursor to drain.
map_submit <- function(pool, st, deadline = Inf) {
  blob <- !is.null(st[["blob"]])
  spec <- isTRUE(st[["spec"]])
  st[["pool"]] <- pool
  # armed across the loop: a timed-out return or a fatal submit error
  # (stopped, slots exhausted) longjmping through cancels the tasks
  # already in; disarmed once every task is submitted
  on.exit(map_cancel(st))
  for (k in seq_along(st[["handles"]])) {
    # pre-check, not just the C entry's: a nested (worker-side) submit
    # never waits on ring space, so an expired deadline must be caught
    # here, before the payload is built
    if (mono_time() >= deadline) {
      st[["timed_out"]] <- TRUE
      return(invisible(st))
    }
    h <- if (spec) {
      # the kind-2 runner task: region name, the ordinal and generation
      # packed in one i64 (ordinal the high 32 bits), the (seed, offset)
      # pair when seeded — staged as the 'I' runner stream
      .Call(
        mizu_pool_submit_map_runner,
        pool,
        list(
          st[["name"]],
          (k - 1) * 2^32 + st[["gen"]],
          st[["seed_pair"]]
        ),
        deadline
      )
    } else {
      payload <- if (blob) {
        map_payload(st, c(st[["lo"]][[k]], st[["hi"]][[k]]))
      } else {
        runner_payload(st, k - 1L)
      }
      .Call(
        mizu_pool_submit_try,
        pool,
        payload,
        deadline,
        if (blob) 0L else 1L
      )
    }
    if (inherits(h, "mizu_timeout")) {
      st[["timed_out"]] <- TRUE
      return(invisible(st))
    }
    st[["handles"]][[k]] <- h
  }
  on.exit()
  invisible(st)
}

map_ranges_label <- function(elts) {
  if (nrow(elts) == 0L) {
    return("(none)")
  }
  paste(sprintf("%.0f-%.0f", elts[, 1L], elts[, 2L]), collapse = ", ")
}

# A collected runner error's element index: this binding's runners
# annotate the condition itself (mizu_map_index); a foreign runner's
# error crosses as a mizu_error_remote, whose index field carries it.
map_err_index <- function(e) {
  i <- e[["mizu_map_index"]]
  if (is.null(i)) {
    i <- e[["index"]]
  }
  i
}

# The pymap-shape splice (a spec map on foreign workers) runs in C:
# mizu_map_splice_ix handles their runners' published element ranges
# (0-based half-open), as mizu_map_splice does this binding's (morsel
# start, morsel count) pairs.

# The death lost-set scan needs this binding's morsel-pair histories:
# convert the foreign runners' element ranges back (a batch is whole
# morsels, the final grant possibly partial, so the count rounds up).
map_hist_ix <- function(runs, ms) {
  lapply(runs, function(run) {
    hist <- run[[1L]]
    list(
      vapply(
        hist,
        function(rg) floor(as.numeric(rg[[1L]]) / ms),
        numeric(1L)
      ),
      vapply(
        hist,
        function(rg) {
          ceiling((as.numeric(rg[[2L]]) - as.numeric(rg[[1L]])) / ms)
        },
        numeric(1L)
      )
    )
  })
}

# The pending loop's registry-read death verdict: the workers live at
# submit are all gone from the registry and no shutdown was broadcast (a
# clean stop surfaces through the handles themselves). "claiming" counts
# as live — a joining worker is about to claim; "leaving" and "reaping"
# do not, as neither takes new work.
map_workers_gone <- function(pool) {
  pst <- mizu_pool_status(pool)
  !pst[["shutdown"]] && !any(pst[["workers"]] %in% c("live", "claiming"))
}

# Collect the map's tasks, then assemble: value lists spliced into place
# by element position with names(x) reapplied, or one gather from the
# region's output area on the template path. Splicing by position and
# minimum-element-index error selection are both collection-order
# independent, so the region path's deferred order changes no semantics.
# doomed0 seeds the doomed flag for the promise settle: its watchdog
# reads the registry verdict off-loop (the drain never reaches the
# park-slice branch that would set it here).
map_collect <- function(
  st,
  deadline = Inf,
  collect = "value",
  doomed0 = FALSE
) {
  out <- if (!st[["direct"]]) vector("list", st[["n"]])
  if (!is.null(st[["blob"]])) {
    for (k in seq_len(st[["C"]])) {
      # terminal outcomes come back mizu_caught-boxed (only C boxes, so an
      # OK result that is itself a condition stays bare) — no handler frame
      v <- .Call(mizu_pool_collect_try, st[["handles"]][[k]], deadline)
      # terminal outcomes are all classed; a bare value skips every check
      if (is.object(v)) {
        if (inherits(v, "mizu_caught")) {
          v <- v[[1L]]
          map_cancel(st)
          if (inherits(v, "mizu_error_worker_died")) {
            elts <- cbind(lo = st[["lo"]][[k]], hi = st[["hi"]][[k]])
            stop_worker_died(v[["slot"]], v[["pid"]], elts)
          }
          stop(v)
        }
        if (inherits(v, "mizu_timeout")) {
          map_cancel(st)
          return(v)
        }
      }
      st[["handles"]][k] <- list(NULL)
      # the splice is one C pass, as the streaming path's (an R-level
      # `[<-` would dispatch per element, per chunk)
      .Call(mizu_map_vsplice, out, st[["lo"]][[k]], v)
    }
  } else {
    # Region path: collect the R runner handles under the exhausted-runner
    # trim, delivered through a deferred collection order. A runner
    # carries no work of its own, so once the cursor exhausts a
    # still-queued runner is dead weight — but parking unboundedly on its
    # handle would wait for a busy peer to claim and no-op it, making a
    # short map's completion hostage to an unrelated foreign task. So:
    # each pass abandons what the armed trigger allows, collects handles
    # whose CLAIM word reads RUNNING (immediate when already published;
    # bounded by the map's own work when executing), and defers IDLE ones
    # — the trigger stays armed while we wait. Only when every uncollected
    # handle defers does collect park, in bounded slices, re-scanning on
    # each return.
    errs <- list()
    died <- NULL
    runs <- list()
    # the death exit's state: a verdict that never surfaces on a handle
    # (the workers all died with the pending runners unclaimed — a queued
    # runner's slot has no reaper) is read off the registry instead,
    # polled at most once a second while the loop makes no progress
    doomed <- doomed0
    last_live_scan <- -Inf
    # consume one runner handle: FALSE when the park slice (or the map
    # deadline) expired with the slot still pending
    consume <- function(k, deadline) {
      v <- .Call(mizu_pool_collect_try, st[["handles"]][[k]], deadline)
      # terminal outcomes are all classed; a bare value skips every check
      if (is.object(v)) {
        if (inherits(v, "mizu_timeout")) {
          return(FALSE)
        }
        if (inherits(v, "mizu_caught")) {
          v <- v[[1L]]
          # fail fast: peers stop within ~a batch (idempotent — an erroring
          # runner already stored this before its ERR publish)
          .Call(mizu_map_cancel_set, st[["wrap"]])
          if (inherits(v, "mizu_error_worker_died")) {
            if (is.null(died)) died <<- v
          } else if (!is.null(map_err_index(v))) {
            errs[[length(errs) + 1L]] <<- v
            # the erroring runner's completed batches still count against
            # the lost set; only its uncompleted batch reports lost
            if (!is.null(v[["mizu_map_hist"]])) {
              runs[[length(runs) + 1L]] <<- v[["mizu_map_hist"]]
            }
          } else {
            map_cancel(st)
            stop(v)
          }
          st[["handles"]][k] <- list(NULL)
          return(TRUE)
        }
      }
      st[["handles"]][k] <- list(NULL)
      # one list carries every published runner result: the death lost-set
      # scan reads the batch histories, the clean path's C assembly
      # splices the batch values
      runs[[length(runs) + 1L]] <<- v
      TRUE
    }
    pending <- seq_len(st[["R"]])
    while (length(pending)) {
      progress <- FALSE
      still <- integer(0L)
      for (k in pending) {
        if (mono_time() >= deadline) {
          map_cancel(st)
          return(.Call(mizu_map_timeout_call))
        }
        # the verdict is the C morsel-state code: 2 abandoned, 1 running,
        # 0 idle
        verdict <- .Call(mizu_map_abandon, st[["wrap"]], k - 1L)
        done <- if (verdict == 2L) {
          # abandoned: never started and never will — cancel and drop; a
          # claim that lands anyway loses its first-call CAS and publishes
          # empty
          h <- st[["handles"]][[k]]
          if (!is.null(h)) {
            .Call(mizu_pool_cancel, h)
          }
          st[["handles"]][k] <- list(NULL)
          TRUE
        } else if (verdict == 1L) {
          consume(k, deadline)
        } else {
          FALSE # idle: defer, the trim trigger unarmed
        }
        if (done) {
          progress <- TRUE
        } else {
          still <- c(still, k)
        }
      }
      pending <- still
      if (length(pending) && !progress) {
        # every uncollected handle defers (nothing claimed yet:
        # pre-first-claim, or every worker pinned): park on one in bounded
        # slices, so a claim landing on a different handle — or exhaustion
        # reached while parked — is picked up within a slice
        now <- mono_time()
        if (now >= deadline) {
          map_cancel(st)
          return(.Call(mizu_map_timeout_call))
        }
        # with the workers live at submit all gone (and no shutdown
        # broadcast, which surfaces through the handles), nothing will
        # ever claim the pending runners: arm the trim so they abandon,
        # and let the drain raise the documented worker_died below. A map
        # submitted with no live worker keeps waiting — one may join
        if (
          st[["live0"]] > 0L &&
            now - last_live_scan >= 1 &&
            {
              last_live_scan <- now
              map_workers_gone(st[["pool"]])
            }
        ) {
          .Call(mizu_map_cancel_set, st[["wrap"]])
          doomed <- TRUE
        }
        if (consume(pending[[1L]], min(deadline, now + 0.05))) {
          pending <- pending[-1L]
        }
      }
    }
    if (
      is.null(died) &&
        !doomed &&
        !length(runs) &&
        !length(errs) &&
        .Call(mizu_map_cancel_get, st[["wrap"]])
    ) {
      # every runner abandoned with nothing published AND the cancel word
      # set: an external cancel (mizu_cancel(), or a pool stop) landed
      # before any claim — cursor exhaustion alone cannot produce this
      # (its finisher published or wrote the output area)
      map_cancel(st)
      stop(map_cond_cancelled())
    }
    if (!is.null(died) || doomed) {
      map_cancel(st)
      # the lost set is arithmetic over the collected histories, in C:
      # issued = [0, cursor), lost = issued minus their union
      elts <- .Call(
        mizu_map_lost,
        st[["wrap"]],
        if (st[["pymap"]]) map_hist_ix(runs, st[["ms"]]) else runs
      )
      # the registry-read verdict carries no claimant record: NA slot/pid,
      # as the field documentation allows
      stop_worker_died(
        if (!is.null(died)) died[["slot"]] else NA_integer_,
        if (!is.null(died)) died[["pid"]] else NA,
        elts
      )
    }
    if (length(errs)) {
      map_cancel(st)
      # first by element index among the runners that ran — the set that
      # ran already depended on steal order; the fail-fast store only
      # shrinks it sooner
      idx <- vapply(errs, function(e) as.numeric(map_err_index(e)), 0)
      stop(errs[[which.min(idx)]])
    }
    if (!st[["direct"]]) {
      if (st[["pymap"]]) {
        # generic assembly off the foreign runners' element-range shape —
        # the same C pass over the other history shape
        .Call(mizu_map_splice_ix, out, runs, 0)
      } else {
        # one C pass splices every runner's batch value lists into out by
        # element position
        .Call(mizu_map_splice, out, runs, st[["ms"]])
      }
    }
  }
  map_assemble(st, out, collect)
}

# The shared assembly tail of map_collect and the streaming loop: the
# generic path's named list, or the template paths' one gather memcpy from
# the output area — or, for a character template (generic chunk results),
# vapply's own checks. A "view" collect instead wraps the output area as
# an ALTREP view (no gather copy), its names / dim applied in C — the R
# setters would duplicate the view and the default ALTREP duplicate
# materializes. The view pins the region, so the state is marked consumed
# and a prepared re-run restages rather than overwriting it
map_assemble <- function(st, out, collect = "value") {
  if (!st[["direct"]] && is.null(st[["template"]])) {
    names(out) <- st[["nms"]]
    return(out)
  }
  if (st[["direct"]] && identical(collect, "view")) {
    st[["consumed"]] <- TRUE
    return(.Call(
      mizu_map_gather_view,
      st[["wrap"]],
      st[["nms"]],
      names(st[["template"]])
    ))
  }
  res <- if (st[["direct"]]) {
    if (isTRUE(st[["spec"]]) && isTRUE(st[["stream"]])) {
      # a spec stream accumulated the flat vector a chunk gather at a
      # time; the memcpy splice carries no attributes, so a classed
      # integer64 gather's class is re-applied here
      if (map_is_int64_class(st[["template"]])) {
        class(out) <- "integer64"
      }
      out
    } else {
      .Call(mizu_map_gather, st[["wrap"]])
    }
  } else {
    vapply(out, identity, st[["template"]], USE.NAMES = FALSE)
  }
  m <- length(st[["template"]])
  if (m > 1L) {
    dim(res) <- c(m, st[["n"]])
    tn <- names(st[["template"]])
    if (!is.null(tn) || !is.null(st[["nms"]])) {
      dimnames(res) <- list(tn, st[["nms"]])
    }
  } else {
    names(res) <- st[["nms"]]
  }
  res
}

# Cancel the map: set the region's cancel word (executing runners stop
# within ~a batch of their next transition), then cancel every outstanding
# (uncollected) task — the PENDING -> CANCEL arm; an executing task's
# publish CAS discards — and drop the handle references, so slot release
# completes at the next GC. A spec stream's per-chunk regions get the
# same: the cancel word per outstanding chunk, the task cancels, then the
# close. Total: it runs from on.exit while an error (or pool shutdown)
# unwinds — the C entries no-op on closed handles.
map_cancel <- function(st) {
  if (!is.null(st[["wrap"]])) {
    .Call(mizu_map_cancel_set, st[["wrap"]])
  }
  if (!is.null(st[["wraps"]])) {
    for (w in st[["wraps"]]) {
      if (!is.null(w)) .Call(mizu_map_cancel_set, w)
    }
  }
  for (h in st[["handles"]]) {
    if (!is.null(h)) .Call(mizu_pool_cancel, h)
  }
  st[["handles"]] <- vector("list", length(st[["handles"]]))
  if (!is.null(st[["wraps"]])) {
    for (w in st[["wraps"]]) {
      if (!is.null(w)) .Call(mizu_map_close, w)
    }
    st[["wraps"]] <- vector("list", length(st[["wraps"]]))
  }
  invisible()
}

# Worker-side blob-path chunk evaluator, riding each chunk task as
# mizu:::map_chunk(r, b[, s]): `r` packs c(lo, hi) as doubles, `b` is the
# inline descriptor blob — the sizes this path admits make a per-chunk
# unserialize negligible, so there is no cache — and `s` the 6-word RNG
# base state when seeded.
map_chunk <- function(r, b, s = NULL) {
  lo <- r[[1L]]
  hi <- r[[2L]]
  d <- .Call(mizu_unserialize_call, b)
  sr <- NULL
  if (!is.null(s)) {
    # per-element streams: seek to element lo's stream in O(log lo), then
    # one jump per element inside the batch loop
    restore_rng <- rng_save()
    on.exit(restore_rng())
    sr <- .Call(mizu_map_rng_seek, s, lo)
  }
  # the element loop is one .Call (mizu_map_batch builds the f call once
  # and swaps only the element cell per iteration, lapply's discipline);
  # one tryCatch per chunk, not per element, annotates an escaping error
  # with the failing element's index — stamped into eic by the loop
  # before each eval — a plain longjmp would leave "first by element
  # index" with nothing to report
  eic <- numeric(1L)
  tryCatch(
    .Call(
      mizu_map_batch,
      NULL,
      d[[1L]],
      d[[2L]],
      d[[3L]],
      lo - 1,
      lo,
      hi,
      sr,
      eic,
      environment()
    ),
    error = function(e) {
      if (eic[[1L]] >= 1) {
        e[["mizu_map_index"]] <- eic[[1L]]
      }
      stop(e)
    }
  )
}

# Worker-side streaming chunk evaluator, riding each chunk task as
# mizu:::map_stream_chunk(n, r, x[, s]): `n` names the descriptor-only map
# region (attached at most once per worker per map through the map_ctx
# cache), `r` packs c(lo, hi) as doubles, `x` is the chunk's slice, and
# `s` the 6-word RNG base state when seeded. The batch loop is the
# existing mizu_map_batch over the slice: template results write straight
# into the region's output area, generic results publish as the chunk
# task's ordinary result. The per-element RNG seek (O(log lo)) keeps
# streams invariant across chunk count, window size, and completion
# order.
map_stream_chunk <- function(n, r, x, s = NULL) {
  lo <- r[[1L]]
  hi <- r[[2L]]
  ctx <- map_ctx(mizu_current_pool(), n)
  sr <- NULL
  if (!is.null(s)) {
    restore_rng <- rng_save()
    on.exit(restore_rng())
    sr <- .Call(mizu_map_rng_seek, s, lo)
  }
  # one tryCatch per chunk, as map_chunk's: eic is the loop's in-flight
  # element index, annotating an escaping error with the failing element
  eic <- numeric(1L)
  tryCatch(
    .Call(
      mizu_map_batch,
      if (ctx[["tmpl"]]) ctx[["xp"]],
      ctx[["f"]],
      ctx[["dots"]],
      x,
      0,
      lo,
      hi,
      sr,
      eic,
      environment()
    ),
    error = function(e) {
      if (eic[[1L]] >= 1) {
        e[["mizu_map_index"]] <- eic[[1L]]
      }
      stop(e)
    }
  )
}

# Worker-side morsel runner, riding each runner task as
# mizu:::map_runner(n, a[, s]): `n` names the map region, `a`
# packs the runner's ordinal into the CLAIM array, the run generation its
# payload carries, and the template flag. The whole batch transition is
# one .Call: mizu_map_next claims the runner's CLAIM lane on its first
# call (NULL when the trim won, or when a prepared reset re-armed the
# word — the runner exits without issuing), then per transition checks
# the cancel word and the pool signals, sizes and issues the next batch
# off the shared cursor, and returns its element range, x slice and help
# flag. Each completed batch is appended to a local history only after
# evaluation — a batch interrupted by an error is never recorded
# (correctly lost), one completed just before a cancel-triggered NULL is
# (never falsely lost) — and the history rides the runner's single
# ordinary result publish: list(batch starts, batch sizes, per-batch
# value lists | NULL on the template path). Template histories merge
# contiguous batches (no values to keep aligned), so they usually
# collapse to one range and stay inside any slot's inline budget.
map_runner <- function(n, a, s = NULL) {
  pool <- mizu_current_pool()
  r <- a[[1L]]
  g <- a[[2L]]
  tmpl <- length(a) > 2L
  ctx <- map_ctx(pool, n)
  xp <- ctx[["xp"]]
  f <- ctx[["f"]]
  dots <- ctx[["dots"]]
  sig <- .Call(mizu_pool_signals, pool)
  hm <- numeric(8)
  hk <- numeric(8)
  vals <- if (!tmpl) vector("list", 8L)
  nb <- 0L
  if (!is.null(s)) {
    restore_rng <- rng_save()
    on.exit(restore_rng())
  }
  tryCatch(
    repeat {
      nx <- .Call(mizu_map_next, xp, r, g, sig, NULL, NULL)
      if (is.null(nx)) {
        break
      }
      # doorbell: one foreign injection task between batches hands
      # concurrent submitters their chunk-boundary interleave back. A
      # seeded batch leaves its per-element stream installed, so the beat
      # runs under the worker's own RNG — restored first, then re-captured
      # (the beat's draws advance it)
      if (nx[[6L]]) {
        if (!is.null(s)) {
          restore_rng()
          .Call(mizu_pool_help_once, pool)
          restore_rng <- rng_save()
        } else {
          .Call(mizu_pool_help_once, pool)
        }
      }
      lo <- nx[[3L]]
      hi <- nx[[4L]]
      xs <- nx[[5L]]
      base <- if (is.null(xs)) {
        xs <- ctx[["x"]]
        lo - 1
      } else {
        0
      }
      # per-element streams stay per-element: one O(log lo) seek per
      # batch, one jump per element inside the batch loop — results
      # invariant across morsel size, batch sizing, runner count, and
      # issue order
      sr <- if (!is.null(s)) .Call(mizu_map_rng_seek, s, lo)
      # the element loop itself is one .Call per batch: mizu_map_batch
      # builds the f call once and swaps only the element cell per
      # iteration (lapply's discipline), with template writes and RNG
      # installs inline. eic is the loop's in-flight element index, so
      # the one tryCatch per batch annotates an escaping error with the
      # failing element before the outer handler re-signals it
      eic <- numeric(1L)
      out <- tryCatch(
        .Call(
          mizu_map_batch,
          xp,
          f,
          dots,
          xs,
          base,
          lo,
          hi,
          sr,
          eic,
          environment()
        ),
        error = function(e) {
          if (eic[[1L]] >= 1) {
            e[["mizu_map_index"]] <- eic[[1L]]
          }
          stop(e)
        }
      )
      if (tmpl && nb > 0L && hm[[nb]] + hk[[nb]] == nx[[1L]]) {
        hk[[nb]] <- hk[[nb]] + nx[[2L]]
      } else {
        nb <- nb + 1L
        if (nb > length(hm)) {
          length(hm) <- 2L * length(hm)
          length(hk) <- 2L * length(hk)
          if (!tmpl) length(vals) <- 2L * length(vals)
        }
        hm[[nb]] <- nx[[1L]]
        hk[[nb]] <- nx[[2L]]
      }
      if (!tmpl) vals[[nb]] <- out
    },
    error = function(e) {
      # an unannotated error is not f's (a transition or help failure):
      # leave it so collect treats it as fatal. An annotated one carries
      # the completed batches, so a death lost-set scan counts only this
      # runner's uncompleted batch
      if (!is.null(e[["mizu_map_index"]])) {
        e[["mizu_map_hist"]] <- list(hm[seq_len(nb)], hk[seq_len(nb)])
      }
      # the fail-fast store, ahead of the ERR publish: peers observe it
      # within ~a batch instead of draining the cursor first
      .Call(mizu_map_cancel_set, xp)
      stop(e)
    }
  )
  list(hm[seq_len(nb)], hk[seq_len(nb)], if (!tmpl) vals[seq_len(nb)])
}

# The worker-local map-context cache, keyed by region name in a small
# environment on the worker handle (created lazily, cleared whole by the
# idle sweep — see pool_idle_sweep). Plain bounded cache: past 8 resident
# contexts, clear everything — eviction only drops the cache reference (an
# in-flight runner's own ctx keeps its mapping alive), and more than a
# handful of maps interleaving on one worker is pathological and costs one
# re-attach. A miss attaches the region — writable always: every runner
# CASes the shared morsel state, not just the template path's output area
# — no-populate, validates the header, and unserializes the one
# descriptor stream: at most once per worker per map.
map_ctx <- function(pool, name) {
  cache <- .Call(mizu_pool_map_cache, pool)
  ctx <- get0(name, envir = cache, inherits = FALSE)
  if (is.null(ctx)) {
    if (length(ls(cache)) >= 8L) {
      rm(list = ls(cache), envir = cache)
    }
    xp <- .Call(mizu_map_open, name, TRUE)
    d <- .Call(mizu_map_desc, xp)
    if (inherits(d, "mizu_map_ix")) {
      # the cross-language form (Phase 5): the f spec decoded once per
      # worker — name kind: the resolved function and its constant
      # arguments, the element prepended per call; source kind: a closure
      # over the argument namespace, the element its formal x and the
      # positional constants its "..."
      task <- d[[2L]]
      f <- task[[1L]]
      dots <- if (d[[1L]] == 0L) as.list(task[-1L]) else task[[2L]]
      ctx <- list(
        f = f,
        dots = dots,
        x = d[[3L]],
        xp = xp,
        tmpl = .Call(mizu_map_is_template, xp)
      )
    } else {
      ctx <- list(
        f = d[[1L]],
        dots = d[[2L]],
        x = if (length(d) >= 3L) d[[3L]],
        xp = xp,
        tmpl = .Call(mizu_map_is_template, xp)
      )
    }
    assign(name, ctx, envir = cache)
  }
  ctx
}

# Worker-side kind-2 runner entry (the cross-language map): the exec hook
# hands the decoded runner stream's fields here — n the region name, g
# the ordinal and generation packed in one i64 (the ordinal the high 32
# bits), sd the (seed, offset) pair when seeded. The runner is always
# same-language as the worker: unpack and run the native loop, deriving
# this language's own seed state from the neutral pair.
map_runner_ix <- function(n, g, sd) {
  s <- if (!is.null(sd)) map_seed_state(sd)
  ctx <- map_ctx(mizu_current_pool(), n)
  map_runner(n, c(g %/% 2^32, g %% 2^32, if (ctx[["tmpl"]]) 1), s)
}
