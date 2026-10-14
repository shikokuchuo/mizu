# The Arrow send path's R half (called from src/arrow.c): nanoarrow is the
# hub, called at R level only. arrow_batches() pulls a send candidate into
# the ABI structs the core writer reads; the bails convert the held state
# back to a value. Neither bail ever re-streams x: a released stream errors
# on a re-read, a consumed-but-unreleased one yields zero batches, and a
# re-streamed RecordBatchReader likewise (nanoarrow 0.9.0 behaviour) — the
# bails hold the pulled batches or the unconsumed stream.

# The requireNamespace gate + the cached namespace lookup.
nano <- new.env(parent = emptyenv())

arrow_ns <- function() {
  ns <- nano$ns
  if (is.null(ns)) {
    if (!requireNamespace("nanoarrow", quietly = TRUE)) {
      stop(
        "the 'nanoarrow' package is required to send Arrow data",
        call. = FALSE
      )
    }
    ns <- asNamespace("nanoarrow")
    nano$ns <- ns
  }
  ns
}

# Pull x into the schema + owned batch structs the core writer reads. A
# non-struct schema returns the unconsumed stream for the arrow_to_value()
# bail; a struct schema collects and releases. Sending a stream consumes
# it: a streaming RecordBatchReader is fully materialized at send, source
# bytes and region resident at once (inherent to the one-region design).
arrow_batches <- function(x) {
  ns <- arrow_ns()
  stream <- ns$as_nanoarrow_array_stream(x)
  schema <- stream$get_schema()
  if (!identical(schema$format, "+s")) {
    return(list(schema = schema, batches = NULL, stream = stream))
  }
  batches <- ns$collect_array_stream(stream)
  stream$release()
  list(schema = schema, batches = batches, stream = NULL)
}

# The fidelity-keeping ptype: nanoarrow's inference widens int64 to double
# and resolves a dictionary to its values; with bit64 available an int64
# array (or each int64 column of a struct) keeps its exact integer64 form.
# A string dictionary takes the factor() ptype only with `factors` (the
# bare path) — the struct bail rebuilds them, a zero-batch export carrying
# no dictionary for the ptype to key on.
arrow_ptype <- function(schema, factors) {
  to <- arrow_ns()$infer_nanoarrow_ptype(schema)
  dict_str <- function(sch) {
    !is.null(sch$dictionary) && sch$dictionary$format %in% c("u", "U", "vu")
  }
  int64 <- requireNamespace("bit64", quietly = TRUE)
  if (factors && dict_str(schema)) {
    return(factor())
  }
  if (int64 && identical(schema$format, "l")) {
    return(bit64::integer64())
  }
  if (identical(schema$format, "+s")) {
    ch <- schema$children
    for (i in seq_along(ch)) {
      if (factors && dict_str(ch[[i]])) {
        to[[i]] <- factor()
      } else if (int64 && identical(ch[[i]]$format, "l")) {
        to[[i]] <- bit64::integer64()
      }
    }
  }
  to
}

# The non-struct bail: convert on the unconsumed stream — a vector. An
# ordered dictionary keeps its class (the core writer's decline never sees
# a bare array): nanoarrow's conversion rejects the ordered flag outright,
# so it converts flat off a rebuilt schema and re-keys on the dictionary.
arrow_to_value <- function(stream) {
  ns <- arrow_ns()
  schema <- stream$get_schema()
  if (is.null(schema$dictionary) || bitwAnd(schema$flags, 1L) == 0L) {
    return(ns$convert_array_stream(stream, to = arrow_ptype(schema, TRUE)))
  }
  batches <- ns$collect_array_stream(stream)
  stream$release()
  d <- batches[[1L]]$dictionary
  lvls <- if (is.null(d)) character(0) else ns$convert_array(d)
  idx <- switch(
    schema$format,
    c = ns$na_int8(),
    C = ns$na_uint8(),
    s = ns$na_int16(),
    S = ns$na_uint16(),
    i = ns$na_int32(),
    I = ns$na_uint32(),
    l = ns$na_int64(),
    L = ns$na_uint64()
  )
  clean <- ns$na_dictionary(schema$dictionary, idx, ordered = FALSE)
  value <- ns$convert_array_stream(ns$basic_array_stream(
    batches,
    schema = clean
  ))
  out <- factor(value, levels = lvls)
  class(out) <- c("ordered", "factor")
  out
}

# The post-collect bails: the held batches stream back as a struct — a
# data.frame. String dictionary columns convert flat, then rebuild as the
# factor they encode off batch 0's dictionary (a zero-batch export carries
# none — the stream then holds no levels at all).
arrow_batches_to_value <- function(schema, batches) {
  ns <- arrow_ns()
  out <- ns$convert_array_stream(
    ns$basic_array_stream(batches, schema = schema),
    to = arrow_ptype(schema, FALSE)
  )
  ch <- schema$children
  for (i in seq_along(ch)) {
    if (!is.null(ch[[i]]$dictionary)) {
      d <- if (length(batches)) batches[[1L]]$children[[i]]$dictionary
      lvls <- if (is.null(d)) character(0) else ns$convert_array(d)
      out[[i]] <- factor(out[[i]], levels = lvls)
    }
  }
  out
}

#' Convert a Received Frame to an Arrow Stream
#'
#' `mizu_as_arrow()` converts a data frame — typically one received over a
#' channel or collected from a pool — into a struct-typed
#' `nanoarrow_array_stream`, ready for
#' `nanoarrow::convert_array_stream()`, `arrow::as_arrow_table()`, or any
#' other Arrow C Data Interface consumer.
#'
#' Frames arrive over channels and pools alike: an Arrow tabular sent
#' top-level stages as one frame region, and an Arrow tabular nested in a
#' payload (a list element, a task argument, the spliced task expression
#' itself) crosses by reference as a frame leaf — the received object
#' holds a view in place of the table. Collect a pool result and the
#' view is what you get; convert explicitly with this function. One
#' caveat on sends: the conversion counts (a uint64 past 2^53, an int32
#' or int64 minimum reading as `NA`) warn at send over a channel, but at
#' a pool publish they fire on the worker and are not observable by the
#' submitter.
#'
#' Columns still backed by their received shared-memory pages export
#' zero-copy: the stream's buffers point straight into the shared region
#' for attribute-free double, integer, `integer64`, and character views
#' (character as `large_string`). This covers any region-backed column —
#' the frame's own leaves, remote leaves referencing another region, and
#' root views assembled into a data frame — with one counted loan per
#' distinct region; a region whose producer exited degrades only its own
#' columns to the copy path. Everything else — factors, Date /
#' POSIXct / difftime columns, logical and raw vectors, and materialized
#' or never-shared columns — converts per column through
#' `nanoarrow::as_nanoarrow_array()`. The shared region stays mapped
#' until the stream and every array produced from it are fully released.
#'
#' @param x a data frame.
#'
#' @return A struct-typed `nanoarrow_array_stream`.
#'
#' @examplesIf requireNamespace("nanoarrow", quietly = TRUE) && requireNamespace("arrow", quietly = TRUE)
#' ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch))))
#' mizu_send(ch, arrow::arrow_table(data.frame(x = runif(1e5), s = "a")))
#' df <- mizu_recv(ch, timeout = 30)
#' stream <- mizu_as_arrow(df)
#' arrow::as_arrow_table(stream)
#' mizu_close(ch)
#'
#' @export
mizu_as_arrow <- function(x) {
  if (!is.data.frame(x)) {
    stop("mizu: expected a data frame", call. = FALSE)
  }
  ns <- arrow_ns()
  n <- length(x)
  arrays <- vector("list", n)
  for (i in seq_len(n)) {
    arrays[[i]] <- ns$nanoarrow_allocate_array()
  }
  # one fused call: the per-column borrow decision and the zero-copy fill;
  # kinds[i] == 0L marks every column taking the nanoarrow copy path
  kinds <- .Call(mizu_arrow_export, x, arrays)
  schemas <- vector("list", n)
  borrowed <- which(kinds != 0L)
  for (i in borrowed) {
    schemas[[i]] <- switch(
      kinds[i],
      ns$na_double(),
      ns$na_int32(),
      ns$na_int64(),
      ns$na_large_string(),
      ns$na_uint64()
    )
    ns$nanoarrow_array_set_schema(arrays[[i]], schemas[[i]])
  }
  for (i in setdiff(seq_len(n), borrowed)) {
    arrays[[i]] <- tryCatch(
      ns$as_nanoarrow_array(x[[i]]),
      error = function(e) {
        stop(
          sprintf(
            "mizu: column '%s' has no Arrow conversion: %s",
            names(x)[i],
            conditionMessage(e)
          ),
          call. = FALSE
        )
      }
    )
    schemas[[i]] <- ns$infer_nanoarrow_schema(x[[i]])
  }
  names(schemas) <- names(x)
  struct_sch <- ns$na_struct(schemas)
  parent <- ns$nanoarrow_array_init(struct_sch)
  parent <- ns$nanoarrow_array_modify(
    parent,
    list(children = arrays, length = nrow(x)),
    validate = FALSE
  )
  # the modify rebuilds the parent's schema with unnamed children; restore
  # the named one, or basic_array_stream's identity check misses and the
  # parent re-converts (a copy, and a broken one for borrowed columns)
  ns$nanoarrow_array_set_schema(parent, struct_sch, validate = FALSE)
  ns$basic_array_stream(list(parent), schema = struct_sch, validate = FALSE)
}
