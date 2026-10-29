# The Arrow send path's R half (called from src/arrow.c): nanoarrow is the
# hub. The pull (mizu_arrow_pull) keeps the one R call
# (as_nanoarrow_array_stream — arrow's own S3 export) and reads the schema
# and batches off the stream extptr in C; the bails below convert the held
# state back to a value. Neither bail ever re-streams x: a released stream
# errors on a re-read, a consumed-but-unreleased one yields zero batches,
# and a re-streamed RecordBatchReader likewise (nanoarrow 0.9.0 behaviour)
# — the bails hold the pulled batches or the unconsumed stream.

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
  if (int64 && is.null(schema$dictionary) && identical(schema$format, "l")) {
    return(bit64::integer64())
  }
  if (identical(schema$format, "+s")) {
    ch <- schema$children
    for (i in seq_along(ch)) {
      if (factors && dict_str(ch[[i]])) {
        to[[i]] <- factor()
      } else if (
        int64 && is.null(ch[[i]]$dictionary) && identical(ch[[i]]$format, "l")
      ) {
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
  # the union of per-batch dictionaries, first-seen order: Arrow permits
  # dictionary deltas, and batch 0's set alone maps unseen levels to NA
  lvls <- character(0)
  for (b in batches) {
    d <- b$dictionary
    if (!is.null(d)) {
      lvls <- union(lvls, ns$convert_array(d))
    }
  }
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
# data.frame. The C pull moves bare batch structs into fresh extptrs (the
# ABI move), so each takes its schema association here. String dictionary
# columns convert flat, then rebuild as the factor they encode off the
# union of the per-batch dictionaries (Arrow permits dictionary deltas;
# a zero-batch export carries none — the stream then holds no levels at
# all). nanoarrow 0.9.0's validation rejects ARROW_FLAG_DICTIONARY_ORDERED
# outright (its "only relevant for dictionaries" error, on a dictionary),
# so an ordered dictionary's bit strips for the conversion and the rebuilt
# factor re-marks ordered — fidelity kept when a *different* column
# declines the frame.
arrow_batches_to_value <- function(schema, batches) {
  ns <- arrow_ns()
  ch <- schema$children
  ord <- vapply(
    ch,
    function(s) !is.null(s$dictionary) && bitwAnd(s$flags, 1L) != 0L,
    logical(1)
  )
  for (i in which(ord)) {
    schema$children[[i]]$flags <- bitwAnd(schema$children[[i]]$flags, -2L)
  }
  for (i in seq_along(batches)) {
    ns$nanoarrow_array_set_schema(batches[[i]], schema, validate = FALSE)
  }
  out <- ns$convert_array_stream(
    ns$basic_array_stream(batches, schema = schema),
    to = arrow_ptype(schema, FALSE)
  )
  for (i in seq_along(ch)) {
    if (!is.null(ch[[i]]$dictionary)) {
      lvls <- character(0)
      for (b in batches) {
        d <- b$children[[i]]$dictionary
        if (!is.null(d)) {
          lvls <- union(lvls, ns$convert_array(d))
        }
      }
      out[[i]] <- factor(out[[i]], levels = lvls)
      if (ord[[i]]) {
        class(out[[i]]) <- c("ordered", "factor")
      }
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
#' Frames arrive over channels and pools alike: Arrow data sent at top
#' level stages as one frame region, and Arrow data nested inside a
#' payload (a list element, a task argument, the task expression itself)
#' crosses by reference — the received object holds a view in place of
#' the table. Collect a pool result and the view is what you get.
#' Convert it explicitly with this function. One caveat on sends: the
#' conversion warnings (a uint64 past 2^63 - 1 becoming `NA`, an int32
#' or int64 minimum reading as `NA`) fire at send over a channel, but at
#' a pool publish they fire on the worker and the submitter never sees
#' them.
#'
#' Columns still backed by their received shared-memory pages export
#' zero-copy: the stream's buffers point straight into the shared region
#' for attribute-free double, integer, `integer64`, and character views
#' (character as `large_string`). This covers any column backed by a
#' shared region — the frame's own columns, columns referencing another
#' region, and received views assembled into a data frame — with one
#' reference held per region. A region whose producer has exited affects
#' only its own columns, which fall back to copying. Everything else —
#' factors, Date / POSIXct / difftime columns, logical and raw vectors,
#' and materialized or never-shared columns — converts per column
#' through `nanoarrow::as_nanoarrow_array()`. The shared region stays
#' mapped until the stream and every array produced from it are fully
#' released.
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
    col <- x[[i]]
    ord <- is.ordered(col)
    if (ord) {
      # nanoarrow 0.9.0 cannot export an ordered factor (its validation
      # rejects ARROW_FLAG_DICTIONARY_ORDERED): export the plain factor
      # and stamp the bit on the schema by hand — validate = FALSE at the
      # stream build keeps the flagged schema out of the validator
      col <- factor(col, ordered = FALSE)
    }
    arrays[[i]] <- tryCatch(
      ns$as_nanoarrow_array(col),
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
    schemas[[i]] <- ns$infer_nanoarrow_schema(col)
    if (ord) {
      schemas[[i]]$flags <- bitwOr(schemas[[i]]$flags, 1L)
    }
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
