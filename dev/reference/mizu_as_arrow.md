# Convert a Received Frame or Vector to an Arrow Stream

`mizu_as_arrow()` converts a data frame or an atomic vector, typically
one received over a channel or collected from a pool or a map, into a
`nanoarrow_array_stream`, ready for
[`nanoarrow::convert_array_stream()`](https://arrow.apache.org/nanoarrow/latest/r/reference/convert_array_stream.html),
[`arrow::as_arrow_table()`](https://arrow.apache.org/docs/r/reference/as_arrow_table.html),
or any other Arrow C Data Interface consumer.

## Usage

``` r
mizu_as_arrow(x)
```

## Arguments

- x:

  a data frame or an atomic vector.

## Value

A `nanoarrow_array_stream`: struct-typed with one batch for a data
frame, and of the vector's own Arrow type with one array for a vector.

## Details

Frames arrive over channels and pools alike. Top-level Arrow data past
the zero-copy floor stages as one frame region; smaller data and
region-stage declines arrive as an ordinary data frame. One nested table
(a list element, a task argument, or the task expression itself) can
cross by reference, with the others arriving as data frames. This
function accepts either form. One caveat on sends: the conversion
warnings (a uint64 past 2^63 - 1 becoming `NA`, an int32 or int64
minimum reading as `NA`) fire at send over a channel, but at a pool
publish they fire on the worker and the submitter never sees them.

Columns still backed by their received shared-memory pages export
zero-copy: the stream's buffers point directly into the shared region
for attribute-free double, integer, `integer64`, raw, and character
views (raw as `uint8`, character as `large_string`). This covers any
column backed by a shared region (the frame's own columns, columns
referencing another region, received views assembled into a data frame,
and the output of a
[`mizu_map()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map.md)
collected with `.collect = "view"`), with one reference held per region.
A region whose producer has exited affects only its own columns, which
fall back to copying. Everything else (factors, Date, POSIXct and
difftime columns, logical vectors, and materialized or never-shared
columns) is copied, per column through
[`nanoarrow::as_nanoarrow_array()`](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array.html).
The copy gives the same Arrow type and values as the zero-copy export of
the same data: a character column is always a `large_string`, and in a
double column `NA` is a null while `NaN` stays a value. The shared
region stays mapped until the stream and every array produced from it
are released.

An atomic vector exports as a single array, under the same rules as a
column. Arrow arrays carry no attributes, so names, `dim` and `dimnames`
are dropped: a matrix or array exports flat, in column-major order. For
a map output with a length-`m` template that is `m` consecutive values
per element of `.x`. Use this form for a named or matrix map output,
which is not zero-copy as a data frame column (a matrix column there is
copied as a fixed-size list with one entry per row, and its `NaN` values
become nulls).

The zero-copy columns also travel back by reference. When the Arrow data
is sent on (over a channel, or returned as a pool result) with such a
column unchanged, the send references the original shared region for
that column and writes only the columns that are new or modified.
Dropping, reordering and renaming columns, and re-batching rows, keep a
column unchanged; slicing, filtering and converting it do not. This
applies to columns that arrived as the direct elements of a received
data frame or list, not to a received bare vector placed in a data
frame, which is copied as before.

## Examples

``` r
ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch))))
mizu_send(ch, arrow::arrow_table(data.frame(x = runif(1e5), s = "a")))
df <- mizu_recv(ch, timeout = 30)
stream <- mizu_as_arrow(df)
arrow::as_arrow_table(stream)
#> Table
#> 100000 rows x 2 columns
#> $x <double>
#> $s <large_string>
mizu_close(ch)

# a bare vector exports as a single array
mizu_as_arrow(c(1.5, NA, NaN))$get_next()
#> <nanoarrow_array double[3]>
#>  $ length    : int 3
#>  $ null_count: int 1
#>  $ offset    : int 0
#>  $ buffers   :List of 2
#>   ..$ :<nanoarrow_buffer validity<bool>[8][1 b]> `TRUE FALSE TRUE T...`
#>   ..$ :<nanoarrow_buffer data<double>[3][24 b]> `1.5 NA NaN`
#>  $ dictionary: NULL
#>  $ children  : list()
```
