# arrow_table() conversions queue one parallel task per plain-vector
# column on libarrow's eternal CPU thread pool; the launched workers and
# the task queue are not freed at process exit, which --use-valgrind
# reports as definite leaks. Disable arrow threading for the test
# session — the frame paths are agnostic to how a producer converts —
# and propagate it to spawned children through R_TESTS: options are
# session-local, but every Rscript child sources that file at startup
# (the check's own R_TESTS, when set, is chained first).
options(arrow.use_threads = FALSE)
rt <- Sys.getenv("R_TESTS")
startup <- tempfile(fileext = ".R")
writeLines(
  c(
    if (nzchar(rt) && file.exists(rt)) sprintf("source(%s)", deparse(rt)),
    "options(arrow.use_threads = FALSE)"
  ),
  startup
)
Sys.setenv(R_TESTS = startup)

is_view <- function(x) .Call(mizu:::mizu_zc_view_check, x)

# The fused decide-and-fill export, driven directly: x's pre-allocated
# schema-less arrays filled in place off the located regions, the per-column
# kinds returned (0 = the nanoarrow copy path, 7 = the double copy). `bare`
# takes list(vector) under the bare form's relaxed attribute gate.
arrow_export <- function(x, bare = FALSE) {
  arrays <- lapply(
    seq_along(x),
    function(i) nanoarrow::nanoarrow_allocate_array()
  )
  list(
    kinds = .Call(mizu:::mizu_arrow_export, x, arrays, bare),
    arrays = arrays
  )
}

# A `.collect = "view"` map result off the in-process pool harness.
map_view <- function(p, x, f, template) {
  st <- mizu:::map_stage(p[["ctrl"]], x, f, list(), template)
  mizu:::map_submit(p[["ctrl"]], st)
  while (pool_step(p) == 1L) {
    NULL
  }
  mizu:::map_collect(st, mizu:::mono_time() + 30, "view")
}

# The first array of an export stream, read back with its schema format.
arrow_read <- function(stream) {
  format <- stream$get_schema()$format
  array <- stream$get_next()
  list(
    format = format,
    length = array$length,
    nulls = array$null_count,
    value = nanoarrow::convert_array(array)
  )
}

# A one-column frame stream assembled from independently sliced batches.
arrow_one_col_batches <- function(schema, arrays, name = "v") {
  sch <- nanoarrow::na_struct(setNames(list(schema), name))
  parents <- lapply(arrays, function(array) {
    parent <- nanoarrow::nanoarrow_array_init(sch)
    parent <- nanoarrow::nanoarrow_array_modify(
      parent,
      list(children = list(array), length = array$length),
      validate = FALSE
    )
    nanoarrow::nanoarrow_array_set_schema(parent, sch, validate = FALSE)
    parent
  })
  nanoarrow::basic_array_stream(parents, schema = sch, validate = FALSE)
}

# Read a filled borrowed array back as an R vector.
array_value <- function(array, schema) {
  nanoarrow::nanoarrow_array_set_schema(array, schema)
  nanoarrow::convert_array(array)
}

# The mapped-type matrix, sized so the frame stages as a region (its span
# past the 32 KiB floor). uint8 stays null-free: a null-bearing one
# declines (R raw has no NA). float16/float32 columns hold values exact at
# their width. want is the R-level expectation the receive is compared
# against.
arrow_matrix <- function(n = 6000L) {
  i <- seq_len(n)
  want <- data.frame(
    lgl = rep(c(TRUE, FALSE, NA), length.out = n),
    i8 = rep(c(1L, NA, -3L), length.out = n),
    i16 = rep(c(300L, NA, -300L), length.out = n),
    u8 = as.raw(rep(c(0L, 1L, 255L), length.out = n)),
    u16 = rep(c(0L, NA, 65535L), length.out = n),
    i32 = rep(c(1L, NA, -300000L), length.out = n),
    u32 = as.numeric(rep(c(0, NA, 4294967295), length.out = n)),
    f16 = rep(c(0.5, NA, -2.5), length.out = n),
    f32 = rep(c(0.5, NA, -2.75), length.out = n),
    f64 = i + 0.5,
    str = rep(c("alpha", NA, "héllo ✓"), length.out = n),
    lstr = rep(c("beta", NA, "γ"), length.out = n),
    fct = factor(
      rep(c("a", "b", NA), length.out = n),
      levels = c("a", "b", "zzz")
    ),
    dat = as.Date("2024-01-01") + i %% 100,
    tms = as.POSIXct("2024-01-01 00:00:00.5", tz = "UTC") + i %% 1000,
    dur = as.difftime(rep(c(1, NA, 300), length.out = n), units = "secs")
  )
  tab <- arrow::arrow_table(
    lgl = want$lgl,
    i8 = arrow::Array$create(want$i8)$cast(arrow::int8()),
    i16 = arrow::Array$create(want$i16)$cast(arrow::int16()),
    u8 = arrow::Array$create(as.integer(want$u8))$cast(arrow::uint8()),
    u16 = arrow::Array$create(want$u16)$cast(arrow::uint16()),
    i32 = want$i32,
    u32 = arrow::Array$create(want$u32)$cast(arrow::uint32()),
    f16 = arrow::Array$create(want$f16)$cast(arrow::float16()),
    f32 = arrow::Array$create(want$f32)$cast(arrow::float32()),
    f64 = want$f64,
    str = want$str,
    lstr = arrow::Array$create(want$lstr)$cast(arrow::large_utf8()),
    fct = want$fct,
    dat = want$dat,
    tms = want$tms,
    dur = want$dur
  )
  list(tab = tab, want = want)
}

# A one-column frame stream assembled by hand (validate = FALSE throughout):
# the schema/batch decline arms need frames no real producer emits.
arrow_one_col_stream <- function(schema, array, name = "v") {
  sch <- nanoarrow::na_struct(setNames(list(schema), name))
  parent <- nanoarrow::nanoarrow_array_init(sch)
  parent <- nanoarrow::nanoarrow_array_modify(
    parent,
    list(children = list(array), length = array$length),
    validate = FALSE
  )
  nanoarrow::nanoarrow_array_set_schema(parent, sch, validate = FALSE)
  nanoarrow::basic_array_stream(list(parent), schema = sch, validate = FALSE)
}

grab_warnings <- function(expr) {
  ws <- character()
  withCallingHandlers(
    expr,
    warning = function(w) {
      ws <<- c(ws, conditionMessage(w))
      invokeRestart("muffleWarning")
    }
  )
  ws
}

view_name <- function(x) .Call(mizu:::mizu_zc_view_name, x)

# The region behind each column of a received frame: a remote leaf names
# the region it references, a local leaf the frame's own ("" off a view).
col_regions <- function(df) {
  one <- function(col) {
    id <- view_name(col)
    if (length(id)) sub("\\[.*$", "", id) else ""
  }
  vapply(df, one, "", USE.NAMES = FALSE)
}

# The provenance fixtures: a frame past the region floor (i doubles as
# valid 0-based dictionary indices), its receive as views over a channel
# pair, and the return leg of an Arrow consumer's hand-back.
prov_frame <- function(n = 6000L) {
  df <- data.frame(
    x = runif(n),
    i = rep(0:2, length.out = n),
    s = rep(c("alpha", "beta", NA), length.out = n)
  )
  df$x[2L] <- NA
  df
}

prov_recv <- function(p, x) {
  mizu_send(p[["host"]], x)
  mizu_recv(p[["peer"]], 5)
}

prov_back <- function(p, x) {
  mizu_send(p[["peer"]], x)
  mizu_recv(p[["host"]], 5)
}

# The data address of every buffer of every column of a struct array.
buffer_addrs <- function(array) {
  info <- getFromNamespace("nanoarrow_buffer_info", "nanoarrow")
  lapply(array$children, function(child) {
    vapply(
      child$buffers,
      function(b) nanoarrow::nanoarrow_pointer_addr_chr(info(b)$data),
      ""
    )
  })
}
