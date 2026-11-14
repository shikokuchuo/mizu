# The Arrow send path's R half (called from src/arrow.c): nanoarrow is the
# hub. The pull (mizu_arrow_pull) keeps the one R call
# (as_nanoarrow_array_stream — arrow's own S3 export) and reads the schema
# and batches off the stream extptr in C. Struct streams the core can frame
# write straight off those batches; only non-struct streams and unsupported
# nested tables convert through the bails below. Neither bail re-streams x: a
# released stream errors on a re-read, a consumed-but-unreleased one yields
# zero batches,
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

# The unsupported-struct bail: the held batches stream back as a struct — a
# data.frame. The C pull moves bare batch structs into fresh extptrs (the
# ABI move), so each takes its schema association here. String dictionary
# columns convert flat, then rebuild as the factor they encode off the
# union of the per-batch dictionaries (Arrow permits dictionary deltas;
# a zero-batch export carries none — the stream then holds no levels at
# all). nanoarrow 0.9.0's validation rejects ARROW_FLAG_DICTIONARY_ORDERED
# outright (its "only relevant for dictionaries" error, on a dictionary),
# so an ordered dictionary's bit strips for the conversion and the rebuilt
# factor re-marks ordered — fidelity kept when a column declines the frame.
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

#' Convert a Received Frame or Vector to an Arrow Stream
#'
#' `mizu_as_arrow()` converts a data frame or an atomic vector, typically one
#' received over a channel or collected from a pool or a map, into a
#' `nanoarrow_array_stream`, ready for `nanoarrow::convert_array_stream()`,
#' `arrow::as_arrow_table()`, or any other Arrow C Data Interface consumer.
#'
#' Frames arrive over channels and pools alike. Top-level Arrow data past the
#' zero-copy floor stages as one frame region; smaller data and region-stage
#' declines arrive as an ordinary data frame. One nested table (a list element,
#' a task argument, or the task expression itself) can cross by reference, with
#' the others arriving as data frames. This function accepts either form. One
#' caveat on sends: the conversion warnings (a uint64 past 2^63 - 1 becoming
#' `NA`, an int32 or int64 minimum reading as `NA`) fire at send over a channel,
#' but at a pool publish they fire on the worker and the submitter never sees
#' them.
#'
#' Columns still backed by their received shared-memory pages export zero-copy:
#' the stream's buffers point directly into the shared region for attribute-free
#' double, integer, `integer64`, raw, and character views (raw as `uint8`,
#' character as `large_string`). This covers any column backed by a shared
#' region (the frame's own columns, columns referencing another region, received
#' views assembled into a data frame, and the output of a [mizu_map()] collected
#' with `.collect = "view"`), with one reference held per region. A region whose
#' producer has exited affects only its own columns, which fall back to copying.
#' Everything else (factors, Date, POSIXct and difftime columns, logical
#' vectors, and materialized or never-shared columns) is copied, per column
#' through `nanoarrow::as_nanoarrow_array()`. The copy gives the same Arrow type
#' and values as the zero-copy export of the same data: a character column is
#' always a `large_string`, and in a double column `NA` is a null while `NaN`
#' stays a value. The shared region stays mapped until the stream and every
#' array produced from it are released.
#'
#' An atomic vector exports as a single array, under the same rules as a
#' column. Arrow arrays carry no attributes, so names, `dim` and `dimnames` are
#' dropped: a matrix or array exports flat, in column-major order. For a map
#' output with a length-`m` template that is `m` consecutive values per element
#' of `.x`. Use this form for a named or matrix map output, which is not
#' zero-copy as a data frame column (a matrix column there is copied as a
#' fixed-size list with one entry per row, and its `NaN` values become nulls).
#'
#' The zero-copy columns also travel back by reference. When the Arrow data is
#' sent on (over a channel, or returned as a pool result) with such a column
#' unchanged, the send references the original shared region for that column
#' and writes only the columns that are new or modified. Dropping, reordering
#' and renaming columns, and re-batching rows, keep a column unchanged; slicing,
#' filtering and converting it do not. This applies to columns that arrived as
#' the direct elements of a received data frame or list, not to a received bare
#' vector placed in a data frame, which is copied as before.
#'
#' @param x a data frame or an atomic vector.
#'
#' @return A `nanoarrow_array_stream`: struct-typed with one batch for a data
#'   frame, and of the vector's own Arrow type with one array for a vector.
#'
#' @examplesIf requireNamespace("nanoarrow", quietly = TRUE) && requireNamespace("arrow", quietly = TRUE)
#' ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch))))
#' mizu_send(ch, arrow::arrow_table(data.frame(x = runif(1e5), s = "a")))
#' df <- mizu_recv(ch, timeout = 30)
#' stream <- mizu_as_arrow(df)
#' arrow::as_arrow_table(stream)
#' mizu_close(ch)
#'
#' # a bare vector exports as a single array
#' mizu_as_arrow(c(1.5, NA, NaN))$get_next()
#'
#' @export
mizu_as_arrow <- function(x) {
  bare <- !is.data.frame(x)
  if (bare && (is.null(x) || !is.atomic(x))) {
    stop("mizu: expected a data frame or an atomic vector", call. = FALSE)
  }
  ns <- arrow_ns()
  cols <- if (bare) list(x) else x
  n <- length(cols)
  arrays <- vector("list", n)
  for (i in seq_len(n)) {
    arrays[[i]] <- ns$nanoarrow_allocate_array()
  }
  # one fused call: the per-column decision and the fill of every column
  # mizu builds itself (the zero-copy borrows and the double copy);
  # kinds[i] == 0L marks a column taking the nanoarrow copy path. The bare
  # form's attribute gate is split: the names here (an Arrow array carries
  # none of the admitted ones), the class in C
  kinds <- if (bare && !all(names(attributes(x)) %in% arrow_bare_attrs)) {
    0L
  } else {
    .Call(mizu_arrow_export, cols, arrays, bare)
  }
  schemas <- vector("list", n)
  filled <- which(kinds != 0L)
  for (i in filled) {
    schemas[[i]] <- switch(
      kinds[i],
      ns$na_double(),
      ns$na_int32(),
      ns$na_int64(),
      ns$na_large_string(),
      ns$na_uint64(),
      ns$na_uint8(),
      ns$na_double()
    )
    ns$nanoarrow_array_set_schema(arrays[[i]], schemas[[i]])
  }
  for (i in setdiff(seq_len(n), filled)) {
    col <- cols[[i]]
    if (bare && !is.null(dim(col))) {
      # the bare form is flat: left on, dim gives nanoarrow's row-wise
      # fixed-size-list form where a view of the same matrix borrows flat
      dim(col) <- NULL
    }
    conv <- arrow_copy(ns, col, if (!bare) names(x)[i])
    arrays[[i]] <- conv$array
    schemas[[i]] <- conv$schema
  }
  if (bare) {
    array <- arrays[[1L]]
    ns$nanoarrow_array_set_schema(array, schemas[[1L]], validate = FALSE)
    return(ns$basic_array_stream(
      list(array),
      schema = schemas[[1L]],
      validate = FALSE
    ))
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

arrow_bare_attrs <- c("names", "dim", "dimnames", "class")

# The nanoarrow copy conversion, shared by the column loop and the bare
# form (`name` NULL): list(array, schema). An unclassed character vector
# converts as large_string, the type a string view borrows as, so the
# schema does not depend on whether the column is still a view.
arrow_copy <- function(ns, col, name) {
  ord <- is.ordered(col)
  if (ord) {
    # nanoarrow 0.9.0 cannot export an ordered factor (its validation
    # rejects ARROW_FLAG_DICTIONARY_ORDERED): export the plain factor
    # and stamp the bit on the schema by hand — validate = FALSE at the
    # stream build keeps the flagged schema out of the validator
    col <- factor(col, ordered = FALSE)
  }
  large <- is.character(col) && !is.object(col) && is.null(dim(col))
  array <- tryCatch(
    if (large) {
      ns$as_nanoarrow_array(col, schema = ns$na_large_string())
    } else {
      ns$as_nanoarrow_array(col)
    },
    error = function(e) {
      stop(
        sprintf(
          "mizu: %s has no Arrow conversion: %s",
          if (is.null(name)) "the vector" else sprintf("column '%s'", name),
          conditionMessage(e)
        ),
        call. = FALSE
      )
    }
  )
  schema <- if (large) {
    ns$na_large_string()
  } else {
    ns$infer_nanoarrow_schema(col)
  }
  if (ord) {
    schema$flags <- bitwOr(schema$flags, 1L)
  }
  list(array = array, schema = schema)
}
