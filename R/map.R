# kio_map: parallel map over a pool. One call stages f / `...` / x once
# (a single descriptor stream in one fresh map region — or entirely inline
# in the chunk tasks when it fits the entry budget), submits C fixed-size
# chunk tasks through the ordinary submit path, and collects in chunk
# order. Workers materialize the map context at most once each (cached on
# the worker handle, prot[5]); a RAWVEC-eligible x is sliced per chunk
# straight from the mapping, never deserialized; the template path writes
# results into a shared output area so n results move cross-process
# unserialized. The stages below are composable so the deterministic
# harness can interleave pool_step() between submit and collect — kio_map
# merely composes them.

# The chunk-call function reference, built once at install time. String
# form keeps the own-namespace ::: out of the code tree (map_chunk is
# internal, and the call must resolve on a worker that has only loaded the
# namespace). Chunk arguments ride as positional literals embedded in the
# call itself, with an empty task-args list: vectors self-evaluate, and
# skipping the named-argument bindings keeps the serialized wrapper inside
# a slot_size = 256 pool's 224-byte entry inline budget (`pool` stays a
# symbol — it must resolve to the evaluating worker's own handle).
map_chunk_ref <- str2lang("kioto:::map_chunk")

# One chunk task's wire payload: map_chunk(pool, n, r[, b][, s]) — b's NULL
# placeholder appears only when s follows it.
map_payload <- function(st, r) {
  seeded <- !is.null(st$seed_state)
  expr <- as.call(c(list(map_chunk_ref, quote(pool), st$name, r),
                    if (!is.null(st$blob)) list(st$blob)
                    else if (seeded) list(NULL),
                    if (seeded) list(st$seed_state)))
  list(expr, list())
}

map_template_types <- c("logical", "integer", "double", "complex", "raw")

# Monotonic-enough clock for the one deadline that threads through both
# submit-side ring-space waits and the collect loop.
mono_time <- function() proc.time()[[3L]]

#' Parallel Map Over a Pool
#'
#' Maps `f` over the elements of `x` on a pool, returning results in input
#' order — a list by default, or an atomic vector (or matrix) with
#' [vapply()] semantics when `.template` is given. Unlike mapping with
#' per-element [kio_submit()] calls, one `kio_map()` call serializes `f`,
#' the constant arguments in `...`, and `x` exactly once, submits a small
#' number of fixed-size chunk tasks (roughly 8 per live worker), and each
#' worker materializes that map context at most once — so the per-element
#' residual cost is one R closure call, as in [lapply()]. An atomic,
#' non-ALTREP `x` with no attributes beyond names additionally travels as
#' bare bytes: workers slice their chunks straight from shared memory
#' without deserializing `x`, and no worker ever materializes more than a
#' chunk of it.
#'
#' Chunks are ordinary pool tasks: they are stolen and balanced like any
#' other work, worker death fails only the affected chunks' elements, and
#' every pool invariant applies unchanged.
#'
#' @section Chunking:
#' By default `[1, length(x)]` splits into
#' `min(length(x), 8 * live_workers, free_result_slots, injection_cap)`
#' contiguous chunks (at least 1) — enough granularity to absorb imbalanced
#' `f` while amortizing per-chunk overhead. `.chunks` overrides the count
#' (up to `length(x)` for pathological imbalance) but is clamped silently
#' by the same free-result-slot and injection-ring caps. A map submitted
#' while no worker is live queues in the injection ring as a single chunk
#' and runs when a worker joins. If the submitter's result-slot subrange
#' is fully occupied by outstanding tasks, `kio_map()` errors immediately,
#' before staging anything.
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
#' per element on the workers). Note for [kio_pool_stats()] readers: a
#' map whose *generic* chunk results are large publishes them through the
#' ordinary result framing, so such maps can add a handful of result-slot
#' `spills` per call without the pool's `slot_size` being undersized for
#' its usual traffic.
#'
#' @section Errors, timeout, and cleanup:
#' Chunks are collected in input order. An error raised by `f`
#' re-signals in the caller as the original condition with a
#' `kio_map_index` field naming the failing element — the first by element
#' index among the chunks that ran. Remaining chunks are cancelled
#' (advisory: queued chunks are dropped; an executing chunk finishes and
#' its result is discarded). The trade-off of in-order collection is that
#' an instant failure in a later chunk is not observed while collect
#' blocks on an earlier slow chunk. If a worker dies mid-chunk, the map
#' errors with that chunk's element range. On `.timeout` expiry —
#' mid-submit or mid-collect — outstanding chunks are cancelled and the
#' `kio_timeout` sentinel is returned, never raised. Cancellation is
#' immediate, but a published-uncollected chunk result's slot is released
#' only when the dropped handle's finalizer runs at the next garbage
#' collection, and the map's staging region is likewise unlinked at GC —
#' transient occupancy a subsequent map absorbs by clamping its chunk
#' count.
#'
#' @section Reproducible RNG:
#' By default nothing is guaranteed about random draws inside `f`: workers
#' seed lazily and independently, and the fast path pays nothing for the
#' option. `.seed` opts into reproducible per-element L'Ecuyer-CMRG
#' streams: element `i` runs under the stream `i` jumps of 2^127 steps
#' from the base state that `set.seed(.seed, "L'Ecuyer-CMRG")` would
#' install (the caller's own `.Random.seed` is not touched, and each
#' worker's RNG state is saved and restored around its chunks). Because
#' streams are per-element, results are identical for any `.chunks` value,
#' worker count, or steal order.
#'
#' @section Nested maps:
#' `kio_map(pool, ...)` inside a task expression uses the evaluating
#' worker's own handle (bound as `pool`): chunk submissions push onto the
#' worker's own deque and the blocked collect executes its own chunks
#' while idle peers steal the rest — fork/join-shaped recursive
#' parallelism at deque cost. A worker's first nested map claims a
#' submitter slot, so at the default `max_submitters = 8` (one held by
#' the controller) at most 7 workers can nest concurrently; raise
#' `max_submitters` for wider nested fan-outs.
#'
#' @section Very large x:
#' The serialized chunk wrapper needs a little over 200 bytes of entry
#' inline budget, so pools created with `slot_size = 256L` (224-byte
#' budget) fit it — except when `.seed` is given, whose 6-word RNG state
#' pushes the wrapper to ~250 bytes: seeded maps on such pools work but
#' spill a region per chunk, so keep the default `slot_size` on pools
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
#' @param .chunks number of chunk tasks to split the map into, or `NULL`
#'   for the default; see the Chunking section.
#' @param .seed `NULL` (default: no RNG guarantees, no cost), or a scalar
#'   integer deriving reproducible per-element RNG streams; see the
#'   Reproducible RNG section.
#' @param .timeout seconds after which the map gives up, cancels its
#'   outstanding chunks, and returns the `kio_timeout` sentinel (class
#'   `c("kio_timeout", "kio_condition")`); `Inf` (the default) waits
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
  deadline <- if (is.finite(.timeout)) mono_time() + .timeout else Inf
  # the interrupt backstop (Ctrl-C in submit or collect): cancel every
  # outstanding chunk and drop the references. After a clean collect all
  # handles are consumed and this is a no-op.
  on.exit(map_cancel(st))
  map_submit(pool, st, deadline)
  if (st$timed_out) return(.Call(kio_map_timeout_call))
  map_collect(pool, st, deadline)
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

# Stage one map call: chunk-count formula, the RAWVEC gate, the region-less
# probe, and the region create — in that order, so slot exhaustion errors
# before anything exists. Returns the mutable map state the other stages
# share; kio_map's frame holds it (and with it the region's producer wrap)
# for the map's duration.
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
    stop("kioto: result slots exhausted \u2014 collect or cancel ",
         "outstanding tasks first", call. = FALSE)
  C <- if (is.null(chunks)) {
    min(n, 8 * caps[[1L]], caps[[2L]], caps[[3L]])
  } else {
    chunks <- as.numeric(chunks)
    if (length(chunks) != 1L || is.na(chunks) || chunks < 1)
      stop("kioto: .chunks must be a positive number", call. = FALSE)
    min(n, chunks, caps[[2L]], caps[[3L]])
  }
  st$C <- max(1L, as.integer(C))
  size <- n %/% st$C
  sizes <- rep.int(size, st$C)
  extra <- n %% st$C
  if (extra > 0) sizes[seq_len(extra)] <- size + 1
  st$hi <- cumsum(sizes)
  st$lo <- st$hi - sizes + 1

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

  if (is.null(st$blob)) {
    # region path: x rides its own bare-byte section when the gate admits
    # it (the descriptor then carries only f and dots — cheap to count),
    # else folded into the one descriptor stream, whose exact size the
    # probe already counted
    desc <- if (st$xraw) list(f, dots) else list(f, dots, x)
    sr <- .Call(kio_map_stage, desc, if (st$xraw) x, if (!st$xraw) desc_len,
                n, if (direct) template)
    st$name <- sr[[1L]]
    st$wrap <- sr[[2L]]
  }
  st$handles <- vector("list", st$C)
  st$timed_out <- FALSE
  st
}

# Submit the C chunk tasks through the ordinary submit path, against the
# map's one deadline. A deadline expiring mid-submit (before a chunk, or
# inside a ring-space wait) cancels the chunks already in and marks the
# state timed out — the caller returns the sentinel.
map_submit <- function(pool, st, deadline = Inf) {
  tmpl <- if (st$direct) 1
  for (k in seq_len(st$C)) {
    rem <- deadline - mono_time()
    if (rem <= 0) {
      st$timed_out <- TRUE
      map_cancel(st)
      return(invisible(st))
    }
    payload <- map_payload(st, c(st$lo[[k]], st$hi[[k]], tmpl))
    h <- tryCatch(.Call(kio_pool_submit, pool, payload, rem),
                  error = identity)
    if (inherits(h, "error")) {
      map_cancel(st)
      if (grepl("submission timed out", conditionMessage(h), fixed = TRUE)) {
        st$timed_out <- TRUE
        return(invisible(st))
      }
      stop(h)
    }
    st$handles[[k]] <- h
  }
  invisible(st)
}

# Collect in chunk order — order preservation and deterministic
# first-by-element-index error selection both fall out of collection order
# — then assemble: chunk lists spliced into place with names(x) reapplied,
# or one gather from the region's output area on the template path.
map_collect <- function(pool, st, deadline = Inf) {
  out <- if (!st$direct) vector("list", st$n)
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
      if (identical(conditionMessage(v),
                    "kioto: worker died while executing this task"))
        stop(sprintf("kioto: worker died while executing map elements %.0f-%.0f",
                     st$lo[[k]], st$hi[[k]]), call. = FALSE)
      stop(v)
    }
    v <- v[[1L]]
    if (inherits(v, "kio_timeout")) {
      map_cancel(st)
      return(v)
    }
    st$handles[k] <- list(NULL)
    if (!st$direct) out[seq.int(st$lo[[k]], st$hi[[k]])] <- v
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

# Cancel every outstanding (uncollected) chunk — the PENDING -> CANCEL arm;
# executing chunks finish and their publish CAS discards — and drop the
# handle references, so slot release completes at the next GC. Total: it
# must be safe from on.exit while an error (or pool shutdown) unwinds.
map_cancel <- function(st) {
  for (h in st$handles)
    if (!is.null(h)) tryCatch(.Call(kio_pool_cancel, h),
                              error = function(e) NULL)
  st$handles <- vector("list", st$C)
  invisible()
}

# Worker-side chunk evaluator, riding each chunk task as
# kioto:::map_chunk(pool, n, r[, b][, s]): `pool` resolves to the
# evaluating worker's own handle via the base-env binding (as nested
# submit), `n` names the map region (NULL when the descriptor blob `b`
# rides inline instead), `r` packs c(lo, hi) as doubles — a third element
# flags the template path, whose region must be attached writable — and
# `s` is the 6-word RNG base state when the map is seeded.
map_chunk <- function(pool, n, r, b = NULL, s = NULL) {
  lo <- r[[1L]]
  hi <- r[[2L]]
  tmpl <- length(r) > 2L
  if (is.null(n)) {
    # region-less: the sizes this path admits make a per-chunk
    # unserialize negligible, so there is no cache
    d <- .Call(kio_unserialize_call, b)
    xs <- d[[3L]]
    xp <- NULL
    base <- lo - 1
  } else {
    ctx <- map_ctx(pool, n, tmpl)
    d <- ctx
    xp <- ctx$xp
    if (is.null(ctx$x)) {
      # RAWVEC x: slice [lo, hi] straight from the mapping — per chunk,
      # so this worker never holds more than a chunk of a huge x
      xs <- .Call(kio_map_slice, xp, lo, hi)
      base <- 0
    } else {
      xs <- ctx$x
      base <- lo - 1
    }
  }
  f <- d[[1L]]
  dots <- d[[2L]]
  len <- hi - lo + 1
  out <- if (tmpl) NULL else vector("list", len)
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
      v <- do.call(f, c(list(xs[[base + i]]), dots))
      if (tmpl) .Call(kio_map_write, xp, ei, v)
      else out[i] <- list(v)
    },
    error = function(e) {
      e$kio_map_index <- ei
      stop(e)
    }
  )
  out
}

# The worker-local map-context cache, keyed by region name in a small
# environment on the worker handle (created lazily, cleared whole by the
# idle sweep — see pool_idle_sweep). Plain bounded cache: past 8 resident
# contexts, clear everything — eviction only drops the cache reference (an
# in-flight chunk's own ctx keeps its mapping alive), and more than a
# handful of maps interleaving on one worker is pathological and costs one
# re-attach. A miss attaches the region (read-only consumer open on the
# generic path; writable, no-populate, for a template's output area),
# validates the header, and unserializes the one descriptor stream —
# at most once per worker per map.
map_ctx <- function(pool, name, writable) {
  cache <- .Call(kio_pool_map_cache, pool)
  ctx <- get0(name, envir = cache, inherits = FALSE)
  if (is.null(ctx)) {
    if (length(ls(cache)) >= 8L) rm(list = ls(cache), envir = cache)
    xp <- .Call(kio_map_open, name, writable)
    d <- .Call(kio_map_desc, xp)
    ctx <- list(f = d[[1L]], dots = d[[2L]],
                x = if (length(d) >= 3L) d[[3L]], xp = xp)
    assign(name, ctx, envir = cache)
  }
  ctx
}
