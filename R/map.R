# kio_map: parallel map over a pool. One call stages f / `...` / x once
# (a single descriptor stream in one fresh map region — or entirely inline
# in chunk tasks when it fits the entry budget), then submits one *runner*
# task per live worker: runners self-schedule element ranges off a shared
# cursor in the map region (morsel-driven scheduling), one adaptive-sized
# batch per kio_map_next transition, and publish their batch histories as
# their single ordinary result. Workers materialize the map context at
# most once each (cached on the worker handle, prot[5]); a RAWVEC-eligible
# x is sliced per batch straight from the mapping, never deserialized; the
# template path writes results into a shared output area so n results move
# cross-process unserialized. The region-less blob path keeps fixed
# chunks: the morsel state needs the region, and at those sizes chunk
# overhead is already negligible. The stages below are composable so the
# deterministic harness can interleave pool_step() between submit and
# collect — kio_map merely composes them.

# Worker-side function references, built once at install time. String form
# keeps the own-namespace ::: out of the code tree (both are internal, and
# the call must resolve on a worker that has only loaded the namespace).
# Arguments ride as positional literals embedded in the call itself, with
# an empty task-args list: vectors self-evaluate, and skipping the
# named-argument bindings keeps the serialized wrapper inside a
# slot_size = 256 pool's 224-byte entry inline budget (`pool` stays a
# symbol — it must resolve to the evaluating worker's own handle).
map_chunk_ref <- str2lang("kioto:::map_chunk")
map_runner_ref <- str2lang("kioto:::map_runner")

# One blob-path chunk task's wire payload: map_chunk(pool, r, b[, s]).
map_payload <- function(st, r) {
  seeded <- !is.null(st$seed_state)
  expr <- as.call(c(list(map_chunk_ref, quote(pool), r, st$blob),
                    if (seeded) list(st$seed_state)))
  list(expr, list())
}

# One runner task's wire payload: map_runner(pool, n, a[, s]) — a packs
# c(ordinal, generation) as doubles, with a third element flagging the
# template path (one vector, not three scalars: the difference between
# fitting a slot_size = 256 entry budget and not).
runner_payload <- function(st, r) {
  seeded <- !is.null(st$seed_state)
  expr <- as.call(c(list(map_runner_ref, quote(pool), st$name,
                         c(r, st$gen, if (st$direct) 1)),
                    if (seeded) list(st$seed_state)))
  list(expr, list())
}

map_template_types <- c("logical", "integer", "double", "complex", "raw")

# Morsel geometry: target ~256 morsels per runner, clamped to a constant
# grain (the morsel cap) so the responsiveness floor — cancellation, help,
# lost-set granularity — stays n-independent. Hard-coded, no user knob;
# the gate sweep freezes the value ({256, 1024} candidates).
map_morsel_cap <- 256
map_morsels_per_runner <- 256

# Monotonic-enough clock for the one deadline that threads through both
# submit-side ring-space waits and the collect loop.
mono_time <- function() proc.time()[[3L]]

#' Parallel Map Over a Pool
#'
#' Maps `f` over the elements of `x` on a pool, returning results in input
#' order — a list by default, or an atomic vector (or matrix) with
#' [vapply()] semantics when `.template` is given. Unlike mapping with
#' per-element [kio_submit()] calls, one `kio_map()` call serializes `f`,
#' the constant arguments in `...`, and `x` exactly once, submits one
#' *runner* task per live worker, and each worker materializes that map
#' context at most once — so the per-element residual cost is one R
#' closure call, as in [lapply()]. Runners self-schedule: they claim
#' contiguous element batches off a shared cursor in the map region,
#' sizing each batch adaptively toward a fixed time target, so trivial
#' `f` runs in large batches at near-zero scheduling overhead while
#' expensive or skewed `f` self-limits to fine claims that keep the
#' workers balanced. An atomic, non-ALTREP `x` with no attributes beyond
#' names additionally travels as bare bytes: workers slice their batches
#' straight from shared memory without deserializing `x`, and no worker
#' ever materializes more than a batch of it.
#'
#' Runners are ordinary pool tasks: they are stolen and balanced like any
#' other work, worker death is detected and reported (see Errors), and
#' every pool invariant applies unchanged. Between batches a runner also
#' answers a pool-wide doorbell: when another submitter's task arrives
#' with every worker busy inside a map, one runner picks it up at its next
#' batch boundary — foreign-task pickup latency is time-bounded and
#' independent of `length(x)`.
#'
#' @section Granularity:
#' Elements are claimed in *morsels* — contiguous ranges of
#' `max(1, min(n %/% (workers * 256), 256))` elements, the granularity
#' floor for cancellation, help, and loss reporting — and issued to
#' runners in adaptively sized batches of consecutive morsels. `.chunks`
#' overrides the morsel count outright (`min(length(x), .chunks)`
#' morsels): with no per-morsel shared state, `.chunks = length(x)` is
#' admissible at zero memory cost for pathological imbalance. A map
#' submitted while no worker is live queues a single runner in the
#' injection ring and runs when a worker joins. If the submitter's
#' result-slot subrange is fully occupied by outstanding tasks,
#' `kio_map()` errors immediately, before staging anything.
#'
#' @section Templates:
#' `.template` gives `vapply()` semantics: every result must match its
#' type and length exactly, or coerce upward (logical -> integer -> double
#' -> complex; checked on the workers, per element). Results are written
#' directly into a shared output area and gathered in one copy — zero
#' result serializations. A template of length `m > 1` gathers an
#' `m * length(x)` matrix, with the template's names as row names, as
#' `vapply()`. Character templates are assembled through the generic
#' result path instead (their type checks then surface at assembly, not
#' per element on the workers). Big shape-regular results belong on the
#' template path: a runner's generic results accumulate on the worker and
#' publish once, so `.template` both caps worker memory and moves the
#' values cross-process without serialization. Note for
#' [kio_pool_stats()] readers: a map whose *generic* results are large
#' publishes them through the ordinary result framing, so such maps can
#' add a few result-slot `spills` per call without the pool's `slot_size`
#' being undersized for its usual traffic.
#'
#' @section Errors, timeout, and cleanup:
#' An error raised by `f` re-signals in the caller as the original
#' condition with a `kio_map_index` field naming the failing element —
#' the first by element index among the elements that ran. Failure is
#' fail-fast: the erroring runner sets the map's shared cancel word
#' before publishing, so every peer stops within about one batch instead
#' of draining the remaining elements. If a worker dies mid-map, the map
#' raises `kio_error_worker_died` (see [kio_error]) carrying the lost
#' elements as an `elements` field — a two-column matrix of inclusive
#' `lo, hi` ranges. Loss is reported runner-granular and conservatively:
#' a dead runner's results publish only at the end, so everything it had
#' completed is reported lost alongside what it was executing — never
#' the reverse. On `.timeout` expiry — mid-submit or mid-collect —
#' outstanding work is cancelled and the `kio_timeout` sentinel is
#' returned, never raised. Executing runners observe cancellation within
#' about one batch (one element where `f` is expensive), independent of
#' `length(x)`; a published-uncollected result's slot is released only
#' when the dropped handle's finalizer runs at the next garbage
#' collection, and the map's staging region is likewise unlinked at GC —
#' transient occupancy a subsequent map absorbs by clamping its runner
#' count.
#'
#' @section Reproducible RNG:
#' By default nothing is guaranteed about random draws inside `f`: workers
#' seed lazily and independently, and the fast path pays nothing for the
#' option. `.seed` opts into reproducible per-element L'Ecuyer-CMRG
#' streams: element `i` runs under the stream `i` jumps of 2^127 steps
#' from the base state that `set.seed(.seed, "L'Ecuyer-CMRG")` would
#' install (the caller's own `.Random.seed` is not touched, and each
#' worker's RNG state is saved and restored around its batches). Because
#' streams are per-element, results are identical for any `.chunks` value,
#' batch sizing, worker count, or steal order.
#'
#' @section Nested maps:
#' `kio_map(pool, ...)` inside a task expression uses the evaluating
#' worker's own handle (bound as `pool`): runner submissions push onto the
#' worker's own deque and the blocked collect executes its own runners
#' while idle peers steal the rest — fork/join-shaped recursive
#' parallelism at deque cost. A worker's first nested map claims a
#' submitter slot, so at the default `max_submitters = 8` (one held by
#' the controller) at most 7 workers can nest concurrently; raise
#' `max_submitters` for wider nested fan-outs.
#'
#' @section Very large x:
#' The serialized runner wrapper needs a little over 200 bytes of entry
#' inline budget, so pools created with `slot_size = 256L` (224-byte
#' budget) fit it — except when `.seed` is given, whose 6-word RNG state
#' pushes the wrapper to ~250 bytes: seeded maps on such pools work but
#' spill a region per runner, so keep the default `slot_size` on pools
#' meant for seeded maps. For a very large `x`, `mori::share()` is the
#' recommended path when mori is available: a shared `x` reduces to its
#' ~30-byte identifier inside the staged descriptor and maps zero-copy on
#' each worker with OS demand paging (`kio_map` itself never calls mori).
#'
#' As `lapply()`, `x` is indexed with `[[` on the workers after an
#' `as.list()` coercion of anything that is not a plain vector — so a
#' data.frame maps over its columns, and a factor over its elements.
#'
#' @inheritParams kio_submit
#' @param x a vector (atomic or list) to map over; anything else is
#'   coerced with `as.list()`, as [lapply()] does.
#' @param f a function (or, as [match.fun()] accepts, its name) applied as
#'   `f(x[[i]], ...)`. Serialized once with its enclosing environment —
#'   keep that environment small, as with any cross-process map.
#' @param ... further constant arguments to `f`, staged once.
#' @param .template `NULL` for a list result, or a [vapply()]-style
#'   `FUN.VALUE`: an atomic vector template each result must match.
#' @param .chunks the map's morsel count (its scheduling granularity), or
#'   `NULL` for the default; see the Granularity section.
#' @param .seed `NULL` (default: no RNG guarantees, no cost), or a scalar
#'   integer deriving reproducible per-element RNG streams; see the
#'   Reproducible RNG section.
#' @param .timeout seconds after which the map gives up, cancels its
#'   outstanding work, and returns the `kio_timeout` sentinel (class
#'   `c("kio_timeout", "kio_sentinel")`); `Inf` (the default) waits
#'   indefinitely. One deadline covers submission and collection.
#'
#' @return A list of `f`'s results in the order of `x`, with `names(x)`
#'   reapplied — or, with `.template`, an atomic vector of type
#'   `typeof(.template)` (an `m * length(x)` matrix when
#'   `length(.template) > 1`). On `.timeout` expiry, the `kio_timeout`
#'   sentinel.
#'
#' @examples
#' \dontrun{
#' p <- kio_pool(n_workers = 4L)
#' kio_map(p, 1:10, function(i) i * 2L)
#' kio_map(p, rnorm(1e6), abs, .template = numeric(1))
#' kio_map(p, 1:4, function(i) rnorm(2), .seed = 42L)
#' kio_pool_stop(p)
#' }
#'
#' @importFrom utils removeSource
#' @export
kio_map <- function(pool, x, f, ..., .template = NULL, .chunks = NULL,
                    .seed = NULL, .timeout = Inf) {
  f <- match.fun(f)
  map_template_check(.template)
  if (length(x) == 0L) return(map_empty(x, .template))
  st <- map_stage(pool, x, f, list(...), .template, .chunks, .seed)
  map_run(pool, st, .timeout)
}

# The one run path shared by kio_map (stage + run on an anonymous state)
# and kio_map_run (a prepared state, re-armed by the caller): submit the
# tasks, collect against the single deadline, and keep the interrupt
# backstop armed — Ctrl-C in submit or collect cancels every outstanding
# task and drops the references; after a clean collect all handles are
# consumed and the backstop is a no-op.
map_run <- function(pool, st, timeout) {
  deadline <- if (is.finite(timeout)) mono_time() + timeout else Inf
  on.exit(map_cancel(st))
  map_submit(pool, st, deadline)
  if (st$timed_out) return(.Call(kio_map_timeout_call))
  map_collect(pool, st, deadline)
}

#' Prepared Maps: Stage Once, Run Many
#'
#' `kio_map_prepare()` stages a map — `f`, the constant arguments in
#' `...`, and `x`, serialized once into a shared map region — without
#' running it, and returns a prepared-map handle. Each `kio_map_run()`
#' then costs only task submission and collection: no serialization, no
#' region create, and — because the region (and its name) stays alive
#' across runs — workers that ran a previous run reuse their cached map
#' context instead of re-attaching. Repeated stochastic simulation is the
#' headline use: `kio_map_run(pm, .seed = i)` varies the RNG streams per
#' run for free, since seed state rides the runner payloads, not the
#' region.
#'
#' Between runs the region's shared scheduling state is re-armed in O(1):
#' the cursor and cancel word clear, and the run generation embedded in
#' every claim word advances — a straggler task from a previous run can
#' never issue against the new run's cursor. After an unclean run — a
#' `.timeout` expiry, an error in `f`, a worker death — the handle is
#' marked stale and the next `kio_map_run()` restages into a fresh region
#' transparently (the old one unlinks at garbage collection under any
#' stragglers). A map small enough to ride entirely inline keeps its
#' staged blob on the handle instead: runs resubmit it, still skipping
#' the serialization.
#'
#' The prepared handle pins the staged `x` (for transparent restaging)
#' and the map region for its lifetime; both release at garbage
#' collection when the handle is dropped. Chunking geometry is fixed at
#' prepare time; the runner count adapts to the live workers at each run.
#'
#' @inheritParams kio_map
#'
#' @return `kio_map_prepare()`: a prepared-map handle. `kio_map_run()`:
#'   exactly what [kio_map()] returns for the staged map — a list, a
#'   templated atomic vector, or the `kio_timeout` sentinel.
#'
#' @examples
#' \dontrun{
#' p <- kio_pool(n_workers = 4L)
#' pm <- kio_map_prepare(p, 1:1000, function(i, draws) {
#'   mean(rnorm(draws)) * i
#' }, draws = 100L)
#' runs <- lapply(1:50, function(s) kio_map_run(pm, .seed = s))
#' kio_pool_stop(p)
#' }
#'
#' @export
kio_map_prepare <- function(pool, x, f, ..., .template = NULL,
                            .chunks = NULL) {
  f <- match.fun(f)
  map_template_check(.template)
  pm <- new.env(parent = emptyenv())
  pm$pool <- pool
  pm$x <- x
  pm$f <- f
  pm$dots <- list(...)
  pm$template <- .template
  pm$chunks <- .chunks
  if (length(x) > 0L)
    pm$st <- map_stage(pool, x, f, pm$dots, .template, .chunks)
  class(pm) <- "kio_map_prepared"
  pm
}

#' @section Replacing x between runs:
#' `kio_map_run(pm, x = x2)` runs over a replacement `x`. When both the
#' staged and replacement `x` are bare-byte eligible (atomic, non-ALTREP,
#' no attributes beyond names) with identical type and length, the new
#' bytes are copied in place over the region's `x` section — the
#' iterate-over-same-shape loop (optimizer steps, simulation sweeps) at
#' memcpy cost, skipping the region create and every worker's re-attach.
#' Any other change of `x` — a different shape or type, a list, a map
#' staged inline — restages transparently on the next run.
#'
#' @rdname kio_map_prepare
#' @param pm a prepared-map handle from [kio_map_prepare()].
#' @export
kio_map_run <- function(pm, x = NULL, .seed = NULL, .timeout = Inf) {
  if (!inherits(pm, "kio_map_prepared"))
    stop("kioto: not a prepared-map handle", call. = FALSE)
  if (!is.null(x)) map_swap_x(pm, x)
  if (length(pm$x) == 0L) return(map_empty(pm$x, pm$template))
  st <- pm$st
  if (is.null(st)) {
    # unclean previous run (or a prior restage failure): stage afresh — a
    # straggler against the old region dies at its exhausted cursor or
    # stale-generation claim word, and the region unlinks at GC
    st <- map_stage(pm$pool, pm$x, pm$f, pm$dots, pm$template, pm$chunks,
                    .seed)
  } else {
    # a rearm failure (transient slot exhaustion) raises before the reset
    # touches anything, so the staged state stays good for a retry; from
    # the reset on, pessimism rules — restage unless the run ends clean
    map_rearm(pm$pool, st, .seed)
    pm$st <- NULL
  }
  r <- map_run(pm$pool, st, .timeout)
  if (!inherits(r, "kio_timeout")) pm$st <- st
  r
}

# Replace a prepared map's x: an in-place memcpy over the region's x
# section when the staged and replacement x are both bare-byte eligible
# with identical type and length (a RAWVEC x is sliced from the mapping
# per batch, never cached worker-side, so the swap is invisible to the
# workers) — anything else drops the staged state, and the next run
# restages with the new x (descriptor-carried x IS cached worker-side,
# so shape or type changes must re-key the region).
map_swap_x <- function(pm, x) {
  st <- pm$st
  swappable <- !is.null(st) && is.null(st$blob) && isTRUE(st$xraw) &&
    typeof(x) == typeof(pm$x) && length(x) == length(pm$x) &&
    .Call(kio_map_eligible, x) >= 0 &&
    (is.null(attributes(x)) ||
       identical(names(attributes(x)), "names"))
  pm$x <- x
  if (swappable) {
    .Call(kio_map_swap_x, st$wrap, x)
    st$nms <- names(x)
  } else {
    pm$st <- NULL
  }
  invisible(pm)
}

# Re-arm a staged map state for another run: per-run seed state (it rides
# the runner payloads, never the region), a fresh runner count against
# the live workers, and — on the region path — the O(1) shared-state
# reset whose bumped generation fences every straggler from the run
# before. The staged morsel geometry is inherited: batching absorbs
# worker-count drift between runs.
map_rearm <- function(pool, st, seed) {
  st$seed_state <- if (!is.null(seed)) {
    seed <- suppressWarnings(as.integer(seed))
    if (length(seed) != 1L || is.na(seed))
      stop("kioto: .seed must be a scalar integer", call. = FALSE)
    .Call(kio_map_rng_base, seed)
  }
  if (is.null(st$blob)) {
    caps <- .Call(kio_pool_map_caps, pool)
    if (caps[[2L]] == 0L)
      stop_kio("kio_error_slots_exhausted",
               paste0("kioto: result slots exhausted \u2014 collect or ",
                      "cancel outstanding tasks first"))
    st$gen <- .Call(kio_map_reset, st$wrap)
    st$R <- as.integer(min(st$nm, max(1L, caps[[1L]]), caps[[2L]],
                           caps[[3L]]))
    st$handles <- vector("list", st$R)
  } else {
    st$handles <- vector("list", st$C)
  }
  st$timed_out <- FALSE
  invisible(st)
}

map_template_check <- function(template) {
  if (is.null(template)) return(invisible())
  if (!typeof(template) %in% c(map_template_types, "character") ||
      length(template) == 0L)
    stop("kioto: .template must be a logical, integer, double, complex, ",
         "raw or character vector of positive length", call. = FALSE)
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
  m <- length(template)
  if (m > 1L) {
    dim(out) <- c(m, 0L)
    tn <- names(template)
    if (!is.null(tn)) dimnames(out) <- list(tn, NULL)
  }
  out
}

# Stage one map call: the RAWVEC gate, the region-less probe, morsel
# geometry, and the region create — in that order, so slot exhaustion
# errors before anything exists. Returns the mutable map state the other
# stages share; kio_map's frame holds it (and with it the region's
# producer wrap) for the map's duration.
map_stage <- function(pool, x, f, dots, template = NULL, chunks = NULL,
                      seed = NULL) {
  # srcrefs would serialize each closure's source (and its srcfile
  # environment) into the descriptor: stripping them keeps staged sizes
  # deterministic across keep.source settings — often the difference
  # between the region-less path and a region
  if (typeof(f) == "closure") f <- removeSource(f)
  # lapply's coercion rule, so [[ on the workers sees what lapply's would
  if (!is.vector(x) || is.object(x)) x <- as.list(x)
  n <- length(x)
  map_template_check(template)
  direct <- !is.null(template) && typeof(template) %in% map_template_types

  st <- new.env(parent = emptyenv())
  st$n <- n
  st$nms <- names(x)
  st$template <- template
  st$direct <- direct
  st$seed_state <- if (!is.null(seed)) {
    seed <- suppressWarnings(as.integer(seed))
    if (length(seed) != 1L || is.na(seed))
      stop("kioto: .seed must be a scalar integer", call. = FALSE)
    .Call(kio_map_rng_base, seed)
  }

  # free_rs counts FREE slots in this submitter's own subrange (claiming a
  # worker's submitter slot on nested first use); zero errors here, before
  # any region is created
  caps <- .Call(kio_pool_map_caps, pool)
  if (caps[[2L]] == 0L)
    stop_kio("kio_error_slots_exhausted",
             paste0("kioto: result slots exhausted \u2014 collect or cancel ",
                    "outstanding tasks first"))
  if (!is.null(chunks)) {
    chunks <- as.numeric(chunks)
    if (length(chunks) != 1L || is.na(chunks) || chunks < 1)
      stop("kioto: .chunks must be a positive number", call. = FALSE)
  }

  # the names-tolerant RAWVEC gate: C checks type / ALTREP / S4, the
  # attribute condition (none, or names only) is cheaper here
  xlen <- .Call(kio_map_eligible, x)
  if (xlen >= 0 && !is.null(attributes(x)) &&
      !identical(names(attributes(x)), "names"))
    xlen <- -1
  st$xraw <- xlen >= 0

  # Region-less probe (generic maps only — the template path needs the
  # region's output area): does the full chunk payload, wrapper plus the
  # single descriptor stream as an ordinary argument, pass the bounded
  # check against the entry inline budget? The bounded pass doubles as the
  # descriptor count when the region is needed after all. Skipped when a
  # RAWVEC x alone already exceeds the budget, so a huge x is never
  # serialized just to learn it does not fit.
  inline_entry <- caps[[4L]]
  desc_len <- NULL
  if (is.null(template) && !(st$xraw && xlen > inline_entry)) {
    bl <- .Call(kio_bounded_call, list(f, dots, x), inline_entry)
    desc_len <- bl[[1L]]
    if (!is.null(bl[[2L]])) {
      st$blob <- bl[[2L]]
      if (is.null(.Call(kio_bounded_call, map_payload(st, c(1, 1)),
                        inline_entry)[[2L]]))
        st$blob <- NULL
    }
  }

  if (!is.null(st$blob)) {
    # region-less blob path: today's fixed chunks — the morsel state needs
    # the region, and at these sizes chunk overhead is already negligible
    C <- min(n, if (is.null(chunks)) 8 * caps[[1L]] else chunks,
             caps[[2L]], caps[[3L]])
    st$C <- max(1L, as.integer(C))
    size <- n %/% st$C
    sizes <- rep.int(size, st$C)
    extra <- n %% st$C
    if (extra > 0) sizes[seq_len(extra)] <- size + 1
    st$hi <- cumsum(sizes)
    st$lo <- st$hi - sizes + 1
    st$handles <- vector("list", st$C)
  } else {
    # region path: x rides its own bare-byte section when the gate admits
    # it (the descriptor then carries only f and dots — cheap to count),
    # else folded into the one descriptor stream, whose exact size the
    # probe already counted. .chunks overrides the morsel count outright,
    # bypassing the cap (no per-morsel storage exists, so .chunks = n is
    # admissible at zero memory cost); the default targets
    # ~map_morsels_per_runner morsels per runner under the constant grain.
    runners <- min(max(1L, caps[[1L]]), caps[[2L]], caps[[3L]])
    st$ms <- if (is.null(chunks)) {
      max(1, min(n %/% (runners * map_morsels_per_runner), map_morsel_cap))
    } else {
      ceiling(n / min(n, floor(chunks)))
    }
    st$nm <- ceiling(n / st$ms)
    st$gen <- 0
    desc <- if (st$xraw) list(f, dots) else list(f, dots, x)
    sr <- .Call(kio_map_stage, desc, if (st$xraw) x, if (!st$xraw) desc_len,
                n, if (direct) template, st$ms)
    st$name <- sr[[1L]]
    st$wrap <- sr[[2L]]
    # one runner per live worker (floored at 1 so a workerless map still
    # queues), clamped by the free slots and the ring as chunks were
    st$R <- as.integer(min(st$nm, max(1L, caps[[1L]]), caps[[2L]],
                           caps[[3L]]))
    st$handles <- vector("list", st$R)
  }
  st$timed_out <- FALSE
  st
}

# Submit the map's tasks — R runner tasks on the region path, C chunk
# tasks on the blob path — through the ordinary submit path, against the
# map's one deadline. A deadline expiring mid-submit (before a task, or
# inside a ring-space wait) cancels the tasks already in and marks the
# state timed out — the caller returns the sentinel. Runners are ordinary
# tasks: stolen, balanced, reaped, and helped like any other work.
map_submit <- function(pool, st, deadline = Inf) {
  blob <- !is.null(st$blob)
  for (k in seq_along(st$handles)) {
    rem <- deadline - mono_time()
    if (rem <= 0) {
      st$timed_out <- TRUE
      map_cancel(st)
      return(invisible(st))
    }
    payload <- if (blob) map_payload(st, c(st$lo[[k]], st$hi[[k]]))
               else runner_payload(st, k - 1L)
    h <- tryCatch(.Call(kio_pool_submit, pool, payload, rem),
                  error = identity)
    if (inherits(h, "error")) {
      map_cancel(st)
      if (inherits(h, "kio_error_submit_timeout")) {
        st$timed_out <- TRUE
        return(invisible(st))
      }
      stop(h)
    }
    st$handles[[k]] <- h
  }
  invisible(st)
}

map_ranges_label <- function(elts) {
  if (nrow(elts) == 0L) return("(none)")
  paste(sprintf("%.0f-%.0f", elts[, 1L], elts[, 2L]), collapse = ", ")
}

# Collect the map's tasks, then assemble: value lists spliced into place
# by element position with names(x) reapplied, or one gather from the
# region's output area on the template path. Splicing by position and
# minimum-element-index error selection are both collection-order
# independent, so the region path's deferred order changes no semantics.
map_collect <- function(pool, st, deadline = Inf) {
  out <- if (!st$direct) vector("list", st$n)
  if (!is.null(st$blob)) {
    # blob path: in-order chunk collection, as ever
    for (k in seq_len(st$C)) {
      rem <- deadline - mono_time()
      if (rem <= 0) {
        map_cancel(st)
        return(.Call(kio_map_timeout_call))
      }
      # list-wrap so an OK result is never mistaken for a raised condition
      # (f may legitimately return one)
      v <- tryCatch(list(.Call(kio_pool_collect, st$handles[[k]], rem)),
                    error = identity)
      if (inherits(v, "condition")) {
        map_cancel(st)
        if (inherits(v, "kio_error_worker_died")) {
          elts <- cbind(lo = st$lo[[k]], hi = st$hi[[k]])
          stop_kio("kio_error_worker_died",
                   sprintf("kioto: worker died while executing map elements %s",
                           map_ranges_label(elts)),
                   slot = v$slot, pid = v$pid, elements = elts)
        }
        stop(v)
      }
      v <- v[[1L]]
      if (inherits(v, "kio_timeout")) {
        map_cancel(st)
        return(v)
      }
      st$handles[k] <- list(NULL)
      out[seq.int(st$lo[[k]], st$hi[[k]])] <- v
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
    ms <- st$ms
    errs <- list()
    died <- NULL
    hists <- list()
    # consume one runner handle: FALSE when the park slice (or the map
    # deadline) expired with the slot still pending
    consume <- function(k, timeout) {
      v <- tryCatch(list(.Call(kio_pool_collect, st$handles[[k]], timeout)),
                    error = identity)
      if (!inherits(v, "condition") && inherits(v[[1L]], "kio_timeout"))
        return(FALSE)
      st$handles[k] <- list(NULL)
      if (inherits(v, "condition")) {
        # fail fast: peers stop within ~a batch (idempotent — an erroring
        # runner already stored this before its ERR publish)
        tryCatch(.Call(kio_map_cancel_set, st$wrap),
                 error = function(e) NULL)
        if (inherits(v, "kio_error_worker_died")) {
          if (is.null(died)) died <<- v
        } else if (!is.null(v$kio_map_index)) {
          errs[[length(errs) + 1L]] <<- v
          # the erroring runner's completed batches still count against
          # the lost set; only its uncompleted batch reports lost
          if (!is.null(v$kio_map_hist))
            hists[[length(hists) + 1L]] <<- v$kio_map_hist
        } else {
          map_cancel(st)
          stop(v)
        }
        return(TRUE)
      }
      v <- v[[1L]]
      hists[[length(hists) + 1L]] <<- v[1:2]
      if (!st$direct)
        for (b in seq_along(v[[3L]])) {
          lo <- v[[1L]][[b]] * ms + 1
          hi <- min(st$n, (v[[1L]][[b]] + v[[2L]][[b]]) * ms)
          out[seq.int(lo, hi)] <<- v[[3L]][[b]]
        }
      TRUE
    }
    pending <- seq_len(st$R)
    while (length(pending)) {
      progress <- FALSE
      for (k in pending) {
        rem <- deadline - mono_time()
        if (rem <= 0) {
          map_cancel(st)
          return(.Call(kio_map_timeout_call))
        }
        verdict <- .Call(kio_map_abandon, st$wrap, k - 1L)
        done <- if (verdict == "abandoned") {
          # never started and never will: cancel and drop — a claim that
          # lands anyway loses its first-call CAS and publishes empty
          h <- st$handles[[k]]
          if (!is.null(h)) tryCatch(.Call(kio_pool_cancel, h),
                                    error = function(e) NULL)
          st$handles[k] <- list(NULL)
          TRUE
        } else if (verdict == "running") {
          consume(k, rem)
        } else {
          FALSE                     # idle: defer, the trim trigger unarmed
        }
        if (done) {
          pending <- setdiff(pending, k)
          progress <- TRUE
        }
      }
      if (length(pending) && !progress) {
        # every uncollected handle defers (nothing claimed yet:
        # pre-first-claim, or every worker pinned): park on one in bounded
        # slices, so a claim landing on a different handle — or exhaustion
        # reached while parked — is picked up within a slice
        rem <- deadline - mono_time()
        if (rem <= 0) {
          map_cancel(st)
          return(.Call(kio_map_timeout_call))
        }
        if (consume(pending[[1L]], min(rem, 0.05)))
          pending <- setdiff(pending, pending[[1L]])
      }
    }
    if (!is.null(died)) {
      map_cancel(st)
      # the lost set is arithmetic over the collected results: issued =
      # [0, cursor), lost = issued minus the union of collected histories.
      # A batch in no history was issued but never completed (its claimant
      # died, or f errored mid-batch); a dead runner's whole history lands
      # here too — it publishes only at exhaustion — and durably written
      # template elements report conservatively as lost, never wrong.
      cur <- .Call(kio_map_info, st$wrap)$cursor
      # as.numeric: empty hists (every runner that ran died) must stay a
      # zero-length vector — unlist(list()) is NULL and order(NULL) errors
      hm <- as.numeric(unlist(lapply(hists, `[[`, 1L)))
      hk <- as.numeric(unlist(lapply(hists, `[[`, 2L)))
      o <- order(hm)
      hm <- hm[o]
      hk <- hk[o]
      lo <- numeric(0)
      hi <- numeric(0)
      at <- 0
      for (b in seq_along(hm)) {
        if (hm[[b]] > at) {
          lo <- c(lo, at)
          hi <- c(hi, hm[[b]] - 1)
        }
        at <- hm[[b]] + hk[[b]]
      }
      if (at < cur) {
        lo <- c(lo, at)
        hi <- c(hi, cur - 1)
      }
      elts <- cbind(lo = lo * ms + 1, hi = pmin(st$n, (hi + 1) * ms))
      stop_kio("kio_error_worker_died",
               sprintf("kioto: worker died while executing map elements %s",
                       map_ranges_label(elts)),
               slot = died$slot, pid = died$pid, elements = elts)
    }
    if (length(errs)) {
      map_cancel(st)
      # first by element index among the runners that ran — the set that
      # ran already depended on steal order; the fail-fast store only
      # shrinks it sooner
      idx <- vapply(errs, function(e) as.numeric(e$kio_map_index), 0)
      stop(errs[[which.min(idx)]])
    }
  }
  if (!st$direct && is.null(st$template)) {
    names(out) <- st$nms
    return(out)
  }
  # template assembly: one memcpy from the output area — or, for a
  # character template (generic chunk results), vapply's own checks
  res <- if (st$direct) .Call(kio_map_gather, st$wrap)
         else vapply(out, identity, st$template, USE.NAMES = FALSE)
  m <- length(st$template)
  if (m > 1L) {
    dim(res) <- c(m, st$n)
    tn <- names(st$template)
    if (!is.null(tn) || !is.null(st$nms)) dimnames(res) <- list(tn, st$nms)
  } else {
    names(res) <- st$nms
  }
  res
}

# Cancel the map: set the region's cancel word (executing runners stop
# within ~a batch of their next transition), then cancel every outstanding
# (uncollected) task — the PENDING -> CANCEL arm; an executing task's
# publish CAS discards — and drop the handle references, so slot release
# completes at the next GC. Total: it must be safe from on.exit while an
# error (or pool shutdown) unwinds.
map_cancel <- function(st) {
  if (!is.null(st$wrap))
    tryCatch(.Call(kio_map_cancel_set, st$wrap), error = function(e) NULL)
  for (h in st$handles)
    if (!is.null(h)) tryCatch(.Call(kio_pool_cancel, h),
                              error = function(e) NULL)
  st$handles <- vector("list", length(st$handles))
  invisible()
}

# Worker-side blob-path chunk evaluator, riding each chunk task as
# kioto:::map_chunk(pool, r, b[, s]): `pool` resolves to the evaluating
# worker's own handle via the base-env binding (as nested submit), `r`
# packs c(lo, hi) as doubles, `b` is the inline descriptor blob — the
# sizes this path admits make a per-chunk unserialize negligible, so
# there is no cache — and `s` the 6-word RNG base state when seeded.
map_chunk <- function(pool, r, b, s = NULL) {
  lo <- r[[1L]]
  hi <- r[[2L]]
  d <- .Call(kio_unserialize_call, b)
  f <- d[[1L]]
  dots <- d[[2L]]
  xs <- d[[3L]]
  len <- hi - lo + 1
  out <- vector("list", len)
  if (!is.null(s)) {
    # per-element streams: seek to element lo's stream in O(log lo), then
    # one jump per element; the worker's own RNG state (possibly absent —
    # workers seed lazily) is restored either way
    os <- get0(".Random.seed", envir = globalenv(), inherits = FALSE)
    on.exit(
      if (is.null(os)) {
        if (exists(".Random.seed", envir = globalenv(), inherits = FALSE))
          rm(".Random.seed", envir = globalenv())
      } else {
        assign(".Random.seed", os, envir = globalenv())
      }
    )
    sr <- .Call(kio_map_rng_seek, s, lo)
  }
  # one tryCatch per chunk, not per element: it annotates an escaping
  # error with the failing element's index before re-signalling — a plain
  # longjmp would leave "first by element index" with nothing to report —
  # and costs nothing off the error path
  ei <- lo
  tryCatch(
    for (i in seq_len(len)) {
      ei <- lo + i - 1
      if (!is.null(s)) sr <- .Call(kio_map_rng_install, sr)
      out[i] <- list(do.call(f, c(list(xs[[lo - 1 + i]]), dots)))
    },
    error = function(e) {
      e$kio_map_index <- ei
      stop(e)
    }
  )
  out
}

# Worker-side morsel runner, riding each runner task as
# kioto:::map_runner(pool, n, a[, s]): `n` names the map region, `a`
# packs the runner's ordinal into the CLAIM array, the run generation its
# payload carries, and the template flag. The whole batch transition is
# one .Call: kio_map_next claims the runner's CLAIM lane on its first
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
map_runner <- function(pool, n, a, s = NULL) {
  r <- a[[1L]]
  g <- a[[2L]]
  tmpl <- length(a) > 2L
  ctx <- map_ctx(pool, n)
  xp <- ctx$xp
  f <- ctx$f
  dots <- ctx$dots
  sig <- .Call(kio_pool_signals, pool)
  hm <- numeric(8)
  hk <- numeric(8)
  vals <- if (!tmpl) vector("list", 8L)
  nb <- 0L
  if (!is.null(s)) {
    os <- get0(".Random.seed", envir = globalenv(), inherits = FALSE)
    on.exit(
      if (is.null(os)) {
        if (exists(".Random.seed", envir = globalenv(), inherits = FALSE))
          rm(".Random.seed", envir = globalenv())
      } else {
        assign(".Random.seed", os, envir = globalenv())
      }
    )
  }
  ei <- NA_real_
  tryCatch(
    repeat {
      nx <- .Call(kio_map_next, xp, r, g, sig, NULL, NULL)
      if (is.null(nx)) break
      # doorbell: one foreign injection task between batches hands
      # concurrent submitters their chunk-boundary interleave back
      if (nx[[6L]]) .Call(kio_pool_help_once, pool)
      lo <- nx[[3L]]
      hi <- nx[[4L]]
      len <- hi - lo + 1
      xs <- nx[[5L]]
      base <- if (is.null(xs)) {
        xs <- ctx$x
        lo - 1
      } else 0
      # per-element streams stay per-element: one O(log lo) seek per
      # batch, one jump per element — results invariant across morsel
      # size, batch sizing, runner count, and issue order
      if (!is.null(s)) sr <- .Call(kio_map_rng_seek, s, lo)
      out <- if (!tmpl) vector("list", len)
      for (i in seq_len(len)) {
        ei <- lo + i - 1
        if (!is.null(s)) sr <- .Call(kio_map_rng_install, sr)
        v <- do.call(f, c(list(xs[[base + i]]), dots))
        if (tmpl) .Call(kio_map_write, xp, ei, v)
        else out[i] <- list(v)
      }
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
      # an NA index means the error is not f's (a transition or help
      # failure): leave it unannotated so collect treats it as fatal
      if (!is.na(ei)) {
        e$kio_map_index <- ei
        # completed batches ride the condition, so a death lost-set scan
        # counts only this runner's uncompleted batch
        e$kio_map_hist <- list(hm[seq_len(nb)], hk[seq_len(nb)])
      }
      # the fail-fast store, ahead of the ERR publish: peers observe it
      # within ~a batch instead of draining the cursor first
      tryCatch(.Call(kio_map_cancel_set, xp), error = function(e2) NULL)
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
  cache <- .Call(kio_pool_map_cache, pool)
  ctx <- get0(name, envir = cache, inherits = FALSE)
  if (is.null(ctx)) {
    if (length(ls(cache)) >= 8L) rm(list = ls(cache), envir = cache)
    xp <- .Call(kio_map_open, name, TRUE)
    d <- .Call(kio_map_desc, xp)
    ctx <- list(f = d[[1L]], dots = d[[2L]],
                x = if (length(d) >= 3L) d[[3L]], xp = xp)
    assign(name, ctx, envir = cache)
  }
  ctx
}
