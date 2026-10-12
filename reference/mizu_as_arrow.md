# Convert a Received Frame to an Arrow Stream

`mizu_as_arrow()` converts a data frame — typically one received over a
channel — into a struct-typed `nanoarrow_array_stream`, ready for
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

Columns still backed by their received shared-memory pages export
zero-copy: the stream's buffers point straight into the shared region
for attribute-free double, integer, `integer64`, and character views
(character as `large_string`). This covers any region-backed column —
the frame's own leaves, remote leaves referencing another region, and
root views assembled into a data frame — with one counted loan per
distinct region; a region whose producer exited degrades only its own
columns to the copy path. Everything else — factors, Date / POSIXct /
difftime columns, logical and raw vectors, and materialized or
never-shared columns — converts per column through
[`nanoarrow::as_nanoarrow_array()`](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array.html).
The shared region stays mapped until the stream and every array produced
from it are fully released.

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
