# Convert a Received Frame to an Arrow Stream

`mizu_as_arrow()` converts a data frame, typically one received over a
channel or collected from a pool, into a struct-typed
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

  a data frame.

## Value

A struct-typed `nanoarrow_array_stream`.

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
for attribute-free double, integer, `integer64`, and character views
(character as `large_string`). This covers any column backed by a shared
region (the frame's own columns, columns referencing another region, and
received views assembled into a data frame), with one reference held per
region. A region whose producer has exited affects only its own columns,
which fall back to copying. Everything else (factors, Date, POSIXct and
difftime columns, logical and raw vectors, and materialized or
never-shared columns) converts per column through
[`nanoarrow::as_nanoarrow_array()`](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array.html).
The shared region stays mapped until the stream and every array produced
from it are released.

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
```
