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

# One blob-path chunk task's wire payload: map_chunk(r, b[, s]).
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
#' Maps `.f` over the elements of `.x` on a pool and returns the results in
#' input order. The result is a list by default, or an atomic vector (or
#' matrix) with [vapply()] semantics when `.template` is given. Unlike
#' mapping with per-element [mizu_submit()] calls, one `mizu_map()` call
#' serializes `.f`, the constant arguments in `...`, and `.x` exactly once.
#' It submits one *runner* task per live worker, and each worker
#' materializes that map context at most once. The per-element residual
#' cost is one R closure call, as in [lapply()]. Runners self-schedule:
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
#' answers a pool-wide doorbell. When the task of another submitter
#' arrives with every worker busy inside a map, one runner picks it up at
#' its next batch boundary. Foreign-task pickup latency is time-bounded
#' and independent of `length(.x)`. A runner claimed off the bell is not
#' executed there: a runner is the join ticket of a map. The helper moves
#' it onto its own deque instead, where the next free worker steals it and
#' joins that map.
#'
#' @section Granularity:
#' Elements are claimed in *morsels*: contiguous ranges of
#' `max(1, min(n %/% (workers * 256), 256))` elements. The morsel is the
#' granularity floor for cancellation, help, and loss reporting. Morsels
#' go to runners in adaptively sized batches of consecutive morsels.
#' `.chunks` overrides the morsel count outright (`min(length(.x), .chunks)`
#' morsels). With no per-morsel shared state, `.chunks = length(x)` is
#' admissible at zero memory cost for pathological imbalance. A map
#' submitted while no worker is live queues a single runner in the
#' injection ring and runs when a worker joins. If no worker has ever
#' joined the pool, `mizu_map()` errors instead, as `mizu_submit_call()`
#' does. If the result-slot subrange of the submitter is fully occupied
#' by outstanding tasks, `mizu_map()` errors immediately, before it
#' stages anything.
#'
#' @section Templates:
#' `.template` gives `vapply()` semantics: every result must match its
#' type and length exactly, or coerce upward (logical -> integer ->
#' double -> complex, checked on the workers per element). A classed
#' `integer64` template (bit64's layout) is also supported: results must
#' be `integer64` of the template's length — int64 joins no widening
#' lattice — and the gathered vector or view keeps the class. Results are
#' written directly into a shared output area and gathered in one copy:
#' zero result serializations. With `.collect = "view"`, the gather copy
#' is skipped as well: the result is a copy-on-write view over the shared
#' output area itself, for pipelines that immediately reduce. A template
#' of length `m > 1` gathers an
#' `m * length(.x)` matrix, with the names of the template as row names, as
#' `vapply()` does. Character templates are assembled through the generic
#' result path instead. Their type checks then surface at assembly, not
#' per element on the workers. Big shape-regular results belong on the
#' template path: the generic results of a runner accumulate on the worker
#' and publish once, so `.template` both caps worker memory and moves the
#' values cross-process without serialization. Note for readers of
#' [mizu_pool_stats()]: a map with large *generic* results publishes them
#' through the ordinary result framing. Such a map can add a few
#' result-slot `spills` per call even when the `slot_size` of the pool is
#' right for its usual traffic.
#'
#' @section Errors, timeout, and cleanup:
#' An error raised by `.f` signals again in the caller as the original
#' condition. A `mizu_map_index` field names the failing element: the first
#' by element index among the elements that ran. Failure is fail-fast. The
#' erroring runner sets the shared cancel word of the map before it
#' publishes. So every peer stops within about one batch instead of
#' draining the remaining elements. If a worker dies mid-map, the map
#' raises `mizu_error_worker_died` (see [mizu_error]), carrying the lost
#' elements as an `elements` field: a two-column matrix of inclusive
#' `lo, hi` ranges. Loss is reported runner-granular and conservatively.
#' The results of a dead runner publish only at the end, so everything it
#' completed is reported lost alongside what it was executing, never the
#' reverse. On `.timeout` expiry, mid-submit or mid-collect, the
#' outstanding work is cancelled and the `mizu_timeout` sentinel is
#' returned, never raised. Executing runners observe cancellation within
#' about one batch (one element where `.f` is expensive), independent of
#' `length(.x)`. The slot of a published-uncollected result is released
#' only when the finalizer of the dropped handle runs at the next garbage
#' collection. The staging region of the map is likewise unlinked at GC. A
#' subsequent map absorbs this transient occupancy by clamping its runner
#' count.
#'
#' @section Reproducible RNG:
#' By default nothing is guaranteed about random draws inside `.f`: the
#' workers seed lazily and independently, and the fast path pays nothing
#' for the option. `.seed` opts into reproducible per-element
#' L'Ecuyer-CMRG streams: element `i` runs under the stream `i` jumps of
#' 2^127 steps from the base state that `set.seed(.seed, "L'Ecuyer-CMRG")`
#' installs. The own `.Random.seed` of the caller is not touched, and the
#' RNG state of each worker is saved and restored around its batches.
#' Because the streams are per-element, the results are identical for any
#' `.chunks` value, batch sizing, worker count, or steal order. With
#' `c(seed, offset)`, element `i` runs under the stream of element
#' `i + offset` of an unoffset run, so a map split across runs or
#' processes reproduces the streams of one uninterrupted run.
#'
#' @section Nested maps:
#' `mizu_map(mizu_current_pool(), ...)` inside a task expression uses the
#' evaluating worker's own handle. Runner submissions push onto the own
#' deque of the worker. The blocked collect executes its own
#' runners while idle peers steal the rest: fork/join-shaped recursive
#' parallelism at deque cost. The first nested map of a worker claims a
#' submitter slot. So at the default `max_submitters = 8` (one held by the
#' controller) at most 7 workers can nest concurrently. Raise
#' `max_submitters` for wider nested fan-outs.
#'
#' @section Cross-language maps:
#' `.f` may be a [mizu_call()] specification instead of a function — the
#' way to map over a foreign pool (one spawned with
#' [mizu_py_pool_launcher()], or any pool whose workers are not R). A spec
#' `.f` always stages a shared map region: the descriptor crosses in the
#' interchange format and each runner task carries a region reference any
#' worker language reads. The map element fills the spec's first
#' positional argument (name kind) or binds as `x` (source kind), and the
#' spec's own constant arguments ride with it — so `...` must be empty
#' with a spec `.f`. Constants and elements must be portable values (the
#' interchange subset documented in [mizu_send()]); a non-portable one
#' raises `mizu_error_not_portable` at stage time. `.template` and
#' `.collect` work unchanged — the output area is wire-typed slots
#' gathered (or view-wrapped) by the submitter's own binding — and a
#' per-element error crosses with its element index. `.seed` carries as
#' the language-neutral `(seed, offset)` pair and each worker language
#' derives its own per-element streams: batching- and steal-order
#' invariance holds within a worker language, but the draws are not
#' identical across languages. A spec map runs on a same-language pool
#' too, subject to the same portability rules.
#'
#' @section Very large x:
#' The serialized runner wrapper needs a little over 200 bytes of entry
#' inline budget, so pools created with `slot_size = 256L` (224-byte
#' budget) fit it. The exception is `.seed`: its 6-word RNG state pushes
#' the wrapper to about 270 bytes. Seeded maps on such pools work but
#' spill a region per runner, so keep the default `slot_size` on pools
#' meant for seeded maps. For a very large `.x`, sharing it first is the
#' recommended path: a zero-copy view received from a channel or a pool
#' result, or a `mori::share()`d vector, reduces to its ~30-byte
#' identifier inside the staged descriptor, and workers read elements
#' straight off the shared pages with OS demand paging — no worker copies
#' any part of `.x` (`mizu_map` itself never calls mori).
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
#' @param .timeout seconds after which the map gives up, cancels its
#'   outstanding work, and returns the `mizu_timeout` sentinel (class
#'   `c("mizu_timeout", "mizu_sentinel")`). `Inf` (the default) waits
#'   indefinitely. One deadline covers submission and collection.
#' @param .collect `"value"` (the default) gathers template results into
#'   an owning R vector. `"view"` instead returns an ALTREP view over the
#'   map's shared output area — no gather copy — for pipelines that
#'   immediately reduce. Requires an atomic `.template`. The view is
#'   copy-on-write: the first write materializes a private copy. It keeps
#'   the map region alive until released, and re-sending it through a
#'   channel or pool crosses as a full copy, not by reference.
#'
#' @return A list of the results of `.f` in the order of `.x`, with
#'   `names(.x)` reapplied. With `.template`, an atomic vector of type
#'   `typeof(.template)` (an `m * length(.x)` matrix when
#'   `length(.template) > 1`) — an owning vector, or a copy-on-write view
#'   over the shared output area with `.collect = "view"`. On `.timeout`
#'   expiry, the `mizu_timeout` sentinel.
#'
#' @examples
#' p <- mizu_pool()
#' mizu_map(p, 1:10, function(i) i * 2L)
#' v <- mizu_map(p, rnorm(1e5), abs, .template = numeric(1))
#' mizu_map(p, 1:4, function(i) rnorm(2), .seed = 42L)
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
  .collect = "value"
) {
  map_check_native(.pool, .f)
  spec <- inherits(.f, "mizu_call")
  dots <- list(...)
  if (spec) {
    if (length(dots)) {
      stop(
        "mizu: constant arguments ride the mizu_call() spec \u2014 '...' must ",
        "be empty with a spec '.f'",
        call. = FALSE
      )
    }
  } else {
    .f <- match.fun(.f)
  }
  map_template_check(.template)
  map_collect_check(.collect, .template)
  if (length(.x) == 0L) {
    return(map_empty(.x, .template))
  }
  st <- map_stage(.pool, .x, .f, dots, .template, .chunks, .seed)
  map_run(.pool, st, .timeout, .collect)
}

# The one run path shared by mizu_map (stage + run on an anonymous state)
# and mizu_map_run (a prepared state, re-armed by the caller): submit the
# tasks, collect against the single deadline, and keep the interrupt
# backstop armed — Ctrl-C in submit or collect cancels every outstanding
# task and drops the references; after a clean collect all handles are
# consumed and the backstop is a no-op.
map_run <- function(pool, st, timeout, collect = "value") {
  deadline <- if (is.finite(timeout)) mono_time() + timeout else Inf
  on.exit(map_cancel(st))
  map_submit(pool, st, deadline)
  if (st[["timed_out"]]) {
    return(.Call(mizu_map_timeout_call))
  }
  map_collect(st, deadline, collect)
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
#' Between runs, the shared scheduling state of the region is re-armed in
#' O(1). The cursor and cancel word clear, and the run generation embedded
#' in every claim word advances. A straggler task from a previous run can
#' never issue against the cursor of the new run. After an unclean run — a
#' `.timeout` expiry, an error in `.f`, a worker death — the handle is
#' marked stale. The next `mizu_map_run()` restages into a fresh region
#' transparently (the old one unlinks at garbage collection under any
#' stragglers). A run collected with `.collect = "view"` restages
#' likewise: the returned view pins its region, so the next run stages
#' fresh rather than re-arming pages a held view still reads. A map small
#' enough to ride entirely inline keeps its staged blob on the handle
#' instead: runs resubmit it, still skipping the serialization.
#'
#' The prepared handle pins the staged `.x` (for transparent restaging) and
#' the map region for its lifetime. Both release at garbage collection
#' when the handle is dropped. The chunking geometry is fixed at prepare
#' time. The runner count adapts to the live workers at each run.
#'
#' @inheritParams mizu_map
#'
#' @return `mizu_map_prepare()`: a prepared-map handle. `mizu_map_run()`:
#'   exactly what [mizu_map()] returns for the staged map — a list, a
#'   templated atomic vector, or the `mizu_timeout` sentinel.
#'
#' @examples
#' p <- mizu_pool()
#' pm <- mizu_map_prepare(p, 1:1000, function(i, draws) {
#'   mean(rnorm(draws)) * i
#' }, draws = 100L)
#' runs <- lapply(1:50, function(s) mizu_map_run(pm, .seed = s))
#' mizu_pool_stop(p)
#'
#' @export
mizu_map_prepare <- function(
  .pool,
  .x,
  .f,
  ...,
  .template = NULL,
  .chunks = NULL
) {
  map_check_native(.pool, .f)
  spec <- inherits(.f, "mizu_call")
  dots <- list(...)
  if (spec) {
    if (length(dots)) {
      stop(
        "mizu: constant arguments ride the mizu_call() spec \u2014 '...' must ",
        "be empty with a spec '.f'",
        call. = FALSE
      )
    }
  } else {
    .f <- match.fun(.f)
  }
  map_template_check(.template)
  pm <- new.env(parent = emptyenv())
  pm[["pool"]] <- .pool
  pm[["x"]] <- .x
  pm[["f"]] <- .f
  pm[["dots"]] <- dots
  pm[["template"]] <- .template
  pm[["chunks"]] <- .chunks
  if (length(.x) > 0L) {
    pm[["st"]] <- map_stage(.pool, .x, .f, dots, .template, .chunks)
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
#' steps, simulation sweeps) at memcpy cost, skipping the region create
#' and the re-attach of every worker. Any other change of `.x` — a
#' different shape or type, a list, a map staged inline — restages
#' transparently on the next run.
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
  if (!is.null(.x)) {
    map_swap_x(pm, .x)
  }
  if (length(pm[["x"]]) == 0L) {
    return(map_empty(pm[["x"]], pm[["template"]]))
  }
  st <- pm[["st"]]
  if (is.null(st)) {
    # unclean previous run (or a prior restage failure): stage afresh — a
    # straggler against the old region dies at its exhausted cursor or
    # stale-generation claim word, and the region unlinks at GC
    st <- map_stage(
      pm[["pool"]],
      pm[["x"]],
      pm[["f"]],
      pm[["dots"]],
      pm[["template"]],
      pm[["chunks"]],
      .seed
    )
  } else {
    # a rearm failure (transient slot exhaustion) raises before the reset
    # touches anything, so the staged state stays good for a retry; from
    # the reset on, pessimism rules — restage unless the run ends clean
    map_rearm(pm[["pool"]], st, .seed)
    pm[["st"]] <- NULL
  }
  r <- map_run(pm[["pool"]], st, .timeout, .collect)
  # a view collect consumes the region (the returned view pins it), so a
  # prepared handle restages on its next run instead of re-arming pages a
  # held view still reads
  if (!inherits(r, "mizu_timeout") && !isTRUE(st[["consumed"]])) {
    pm[["st"]] <- st
  }
  r
}

# The x-section attribute gate, mirroring the C raw gate: bare, names-only
# (names stay submitter-side for assembly), or class-only integer64 (the
# wire tag carries the class — consumed at stage, re-applied per slice).
map_x_attrs_ok <- function(x) {
  attrs <- attributes(x)
  is.null(attrs) ||
    identical(names(attrs), "names") ||
    identical(attrs, list(class = "integer64"))
}

# The x-section wire type: class-only integer64 stages as int64 (int64 and
# double share typeof "double", so typeof alone cannot gate a swap).
map_wire_type <- function(x) {
  if (
    typeof(x) == "double" && identical(attributes(x), list(class = "integer64"))
  ) {
    "integer64"
  } else {
    typeof(x)
  }
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
  if (!is.finite(s) || abs(s) > .Machine$integer.max) {
    stop("mizu: .seed[1] must be an integer", call. = FALSE)
  }
  if (n == 1L) {
    return(c(s, 0))
  }
  offset <- seed[2L]
  if (is.na(offset) || offset < 0 || offset != floor(offset)) {
    stop(
      "mizu: .seed[2] (stream offset) must be a non-negative integer",
      call. = FALSE
    )
  }
  c(s, offset)
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
    caps <- .Call(mizu_pool_map_caps, pool)
    if (caps[[2L]] == 0L) {
      stop_mizu(
        "mizu_error_slots_exhausted",
        paste0(
          "mizu: result slots exhausted \u2014 collect or ",
          "cancel outstanding tasks first"
        )
      )
    }
    st[["gen"]] <- .Call(mizu_map_reset, st[["wrap"]])
    st[["R"]] <- map_runner_count(caps, st[["nm"]])
    st[["handles"]] <- vector("list", st[["R"]])
  } else {
    st[["handles"]] <- vector("list", st[["C"]])
  }
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
# time. Language byte 2 is R.
map_check_native <- function(pool, f) {
  ident <- .Call(mizu_pool_ident, pool)
  if (is.null(ident)) {
    stop("mizu: no worker has joined this pool", call. = FALSE)
  }
  if (inherits(f, "mizu_call") || ident[[1L]] == 2L) {
    return(invisible())
  }
  stop(
    "mizu: this pool's workers are not R - mizu_map() needs a mizu_call() ",
    "spec as '.f' on a foreign pool",
    call. = FALSE
  )
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
  if (typeof(template) == "double" && identical(class(template), "integer64")) {
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
  seed = NULL
) {
  spec <- inherits(f, "mizu_call")
  # srcrefs would serialize each closure's source (and its srcfile
  # environment) into the descriptor: stripping them keeps staged sizes
  # deterministic across keep.source settings — often the difference
  # between the region-less path and a region
  if (typeof(f) == "closure") {
    f <- strip_srcref(f)
  }
  # lapply's coercion rule, so [[ on the workers sees what lapply's would.
  # The exception is class-only integer64: it stays a vector and rides the
  # raw x section, its class re-applied to each slice (and to each element
  # at batch time) — list-coercing it would forfeit the bare-bytes section
  if (
    (!is.vector(x) || is.object(x)) &&
      !identical(attributes(x), list(class = "integer64"))
  ) {
    x <- as.list(x)
  }
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

  # free_rs counts FREE slots in this submitter's own subrange (claiming a
  # worker's submitter slot on nested first use); zero errors here, before
  # any region is created
  caps <- .Call(mizu_pool_map_caps, pool)
  if (caps[[2L]] == 0L) {
    stop_mizu(
      "mizu_error_slots_exhausted",
      paste0(
        "mizu: result slots exhausted \u2014 collect or cancel ",
        "outstanding tasks first"
      )
    )
  }
  if (!is.null(chunks)) {
    chunks <- as.numeric(chunks)
    if (length(chunks) != 1L || is.na(chunks) || chunks < 1) {
      stop("mizu: .chunks must be a positive number", call. = FALSE)
    }
  }

  # the names-tolerant RAWVEC gate: C checks type / ALTREP / S4, the
  # attribute condition (none, names only, or class-only integer64) is
  # cheaper here
  xlen <- .Call(mizu_map_eligible, x)
  if (xlen >= 0 && !map_x_attrs_ok(x)) {
    xlen <- -1
  }
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
    size <- n %/% st[["C"]]
    sizes <- rep.int(size, st[["C"]])
    extra <- n %% st[["C"]]
    if (extra > 0) {
      sizes[seq_len(extra)] <- size + 1
    }
    st[["hi"]] <- cumsum(sizes)
    st[["lo"]] <- st[["hi"]] - sizes + 1
    st[["handles"]] <- vector("list", st[["C"]])
  } else {
    # region path: x rides its own bare-byte section when the gate admits
    # it (the descriptor then carries only f and dots — cheap to count),
    # else folded into the one descriptor stream, whose exact size the
    # probe already counted. .chunks overrides the morsel count outright,
    # bypassing the cap (no per-morsel storage exists, so .chunks = n is
    # admissible at zero memory cost); the default targets
    # ~map_morsels_per_runner morsels per runner under the constant grain.
    runners <- map_runner_count(caps)
    st[["ms"]] <- if (is.null(chunks)) {
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
      ident <- .Call(mizu_pool_ident, pool)
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
    st[["handles"]] <- vector("list", st[["R"]])
  }
  st[["timed_out"]] <- FALSE
  st
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

# The pymap-shape splice (a spec map on foreign workers): their runners
# publish batch histories as element ranges (0-based half-open), not this
# binding's (morsel start, morsel count) pairs — collect splices by
# position either way. The template path publishes no values.
map_splice_ix <- function(out, runs) {
  for (run in runs) {
    vals <- run[[2L]]
    if (is.null(vals)) {
      next
    }
    hist <- run[[1L]]
    for (j in seq_along(hist)) {
      rg <- hist[[j]]
      out[(as.numeric(rg[[1L]]) + 1):as.numeric(rg[[2L]])] <- vals[[j]]
    }
  }
  out
}

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

# Collect the map's tasks, then assemble: value lists spliced into place
# by element position with names(x) reapplied, or one gather from the
# region's output area on the template path. Splicing by position and
# minimum-element-index error selection are both collection-order
# independent, so the region path's deferred order changes no semantics.
map_collect <- function(st, deadline = Inf, collect = "value") {
  out <- if (!st[["direct"]]) vector("list", st[["n"]])
  if (!is.null(st[["blob"]])) {
    # blob path: in-order chunk collection, as ever
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
            stop_mizu(
              "mizu_error_worker_died",
              sprintf(
                "mizu: worker died while executing map elements %s",
                map_ranges_label(elts)
              ),
              slot = v[["slot"]],
              pid = v[["pid"]],
              elements = elts
            )
          }
          stop(v)
        }
        if (inherits(v, "mizu_timeout")) {
          map_cancel(st)
          return(v)
        }
      }
      st[["handles"]][k] <- list(NULL)
      out[seq.int(st[["lo"]][[k]], st[["hi"]][[k]])] <- v
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
        if (consume(pending[[1L]], min(deadline, now + 0.05))) {
          pending <- pending[-1L]
        }
      }
    }
    if (!is.null(died)) {
      map_cancel(st)
      # the lost set is arithmetic over the collected histories, in C:
      # issued = [0, cursor), lost = issued minus their union
      elts <- .Call(
        mizu_map_lost,
        st[["wrap"]],
        if (st[["pymap"]]) map_hist_ix(runs, st[["ms"]]) else runs
      )
      stop_mizu(
        "mizu_error_worker_died",
        sprintf(
          "mizu: worker died while executing map elements %s",
          map_ranges_label(elts)
        ),
        slot = died[["slot"]],
        pid = died[["pid"]],
        elements = elts
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
        # generic assembly off the foreign runners' element-range shape
        out <- map_splice_ix(out, runs)
      } else {
        # one C pass splices every runner's batch value lists into out by
        # element position
        .Call(mizu_map_splice, out, runs, st[["ms"]])
      }
    }
  }
  if (!st[["direct"]] && is.null(st[["template"]])) {
    names(out) <- st[["nms"]]
    return(out)
  }
  # template assembly: one memcpy from the output area — or, for a
  # character template (generic chunk results), vapply's own checks. A
  # "view" collect instead wraps the output area as an ALTREP view (no
  # gather copy), its names / dim applied in C — the R setters would
  # duplicate the view and the default ALTREP duplicate materializes. The
  # view pins the region, so the state is marked consumed and a prepared
  # re-run restages rather than overwriting it
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
    .Call(mizu_map_gather, st[["wrap"]])
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
# completes at the next GC. Total: it runs from on.exit while an error
# (or pool shutdown) unwinds — both C entries no-op on closed handles.
map_cancel <- function(st) {
  if (!is.null(st[["wrap"]])) {
    .Call(mizu_map_cancel_set, st[["wrap"]])
  }
  for (h in st[["handles"]]) {
    if (!is.null(h)) .Call(mizu_pool_cancel, h)
  }
  st[["handles"]] <- vector("list", length(st[["handles"]]))
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
    # one jump per element inside the batch loop; the worker's own RNG
    # state (possibly absent — workers seed lazily) is restored either way
    os <- get0(".Random.seed", envir = globalenv(), inherits = FALSE)
    on.exit(
      if (is.null(os)) {
        if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
          rm(".Random.seed", envir = globalenv())
        }
      } else {
        assign(".Random.seed", os, envir = globalenv())
      }
    )
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
    os <- get0(".Random.seed", envir = globalenv(), inherits = FALSE)
    on.exit(
      if (is.null(os)) {
        if (exists(".Random.seed", envir = globalenv(), inherits = FALSE)) {
          rm(".Random.seed", envir = globalenv())
        }
      } else {
        assign(".Random.seed", os, envir = globalenv())
      }
    )
  }
  tryCatch(
    repeat {
      nx <- .Call(mizu_map_next, xp, r, g, sig, NULL, NULL)
      if (is.null(nx)) {
        break
      }
      # doorbell: one foreign injection task between batches hands
      # concurrent submitters their chunk-boundary interleave back
      if (nx[[6L]]) {
        .Call(mizu_pool_help_once, pool)
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
