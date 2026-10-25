# Python interop

The process at the other end of a channel does not have to be R.
[pymizu](https://github.com/shikokuchuo/pymizu) is the Python
counterpart of mizu, built on the same shared-memory core. A channel
connects the two languages as directly as two R sessions. Pass the peer
program as a source string instead of a quoted expression, and set the
launcher to
[`mizu_py_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_py_launcher.md).
The launcher checks for the interpreter and pymizu before the channel is
created, then spawns `python3 -m pymizu.child`. In this example, a
Python process doubles the Old Faithful eruption durations. (The Python
examples in this vignette run when a `python3` with pymizu and NumPy
installed is on the `PATH`.)

``` r

library(mizu)

ch <- mizu_channel(
  "
import pymizu
while True:
    x = ch.recv(30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x * 2)
",
  launcher = mizu_py_launcher()
)

mizu_send(ch, faithful$eruptions)
head(mizu_recv(ch, timeout = 5))
#> [1] 7.200 3.600 6.666 4.566 9.066 5.766
mizu_close(ch)
```

## Large data crosses without copying

Past a size floor, the data itself never moves. mizu writes it to shared
memory once, and both sides read it in place: no serialization, no
parsing, no further copies. A large value you receive arrives as an
ALTREP view over the shared pages. This is the same zero-copy mechanism
you get from another R process. A large value you send arrives in Python
backed by those same pages. The peer below echoes back whatever it
receives. This channel stays open for the next few examples:

``` r

ch <- mizu_channel(
  "
import pymizu
while True:
    x = ch.recv(30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x)
",
  launcher = mizu_py_launcher()
)

big <- rep(faithful$waiting, 5000) # 1.36 million waiting times, ten megabytes
mizu_send(ch, big)
back <- mizu_recv(ch, timeout = 5)
identical(back, big)
#> [1] TRUE
```

Apart from that one write into the shared region, nothing moved at all.
Python’s echo came back as a reference with zero payload bytes. If you
send a received view onward, it crosses the same way: as the region’s
name alone. So does an unmodified data frame echoed back. The zero-copy
tier covers every large layout: a flat vector, a matrix or array of any
memory order, a character vector, or a nested Python tree of lists,
dicts, and arrays. A large data frame crosses as one shared region in
both directions. The floor is low: 256 KB for a flat vector on a
channel, and 32 KB everywhere else.[^1]

## What crosses

R’s built-in datasets supply the proof: each form below comes back from
Python [`identical()`](https://rdrr.io/r/base/identical.html) to what
you sent, down to values, shape, class, and labels.

``` r

echo <- function(x) {
  mizu_send(ch, x)
  mizu_recv(ch, timeout = 5)
}

identical(echo(mtcars), mtcars)              # a data frame
#> [1] TRUE
identical(echo(iris), iris)                  # a factor column stays a factor
#> [1] TRUE
identical(echo(state.x77), state.x77)        # a matrix keeps its dimnames
#> [1] TRUE
identical(echo(HairEyeColor), HairEyeColor)  # a table keeps class and axis labels
#> [1] TRUE
identical(echo(islands), islands)            # a named vector keeps its names
#> [1] TRUE

days <- as.Date(paste(1973, airquality$Month, airquality$Day, sep = "-"))
identical(echo(days), days)                  # a Date keeps its meaning
#> [1] TRUE
```

Python sees the natural equivalent throughout: a NumPy array for a
vector, a dict for a named list, a `datetime64` array for a `Date`, a
`pymizu.Frame` for a data frame.

## What doesn’t cross

Some values have no portable home on the other side.
[`mizu_send()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)
declines these with a `mizu_error_not_portable` that points at the
offending value. The channel stays usable. Among the built-in datasets,
`esoph`, `CO2`, and `ChickWeight` do not cross: each has an
ordered-factor column, a type with no NumPy equivalent.

``` r

tryCatch(mizu_send(ch, esoph), error = conditionMessage)
#> [1] "mizu: value is not portable to the peer (an ordered factor has no portable home at x$agegp)"
mizu_close(ch)
```

[`mizu_send()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)
also declines a `POSIXlt` time, a character matrix, an S4 object, a
data.table, and a named character vector or named factor (names ride on
the plain atomic vectors only). From the Python side, the declined types
are nested, decimal, `date64`, `time`, and `interval` Arrow columns.

## Data frames and Arrow

A data frame crosses as a `pymizu.Frame`, one `pl.from_arrow(f)` or
`pa.table(f)` away from polars or pyarrow. A polars, pyarrow, or pandas
frame arrives back as a `data.frame`. A tibble’s class and extra
attributes cross unchanged. An unmodified round trip is
[`identical()`](https://rdrr.io/r/base/identical.html): a tibble comes
back a tibble.

Any Arrow producer stages directly from its buffers: an `arrow` Table or
RecordBatchReader, a struct-typed `nanoarrow_array_stream`,
`DBI::dbGetQueryArrow()`, duckdb, or adbc. Staging is one layout write
into the shared region, not a conversion to R values first.
[`mizu_as_arrow()`](https://shikokuchuo.net/mizu/reference/mizu_as_arrow.md)
converts a received frame back to Arrow. Columns without attributes
(double, integer, `integer64`, character) convert without copying:

``` r

ch <- mizu_channel(quote(mizu_send(ch, mizu_recv(ch))))
mizu_send(ch, arrow::arrow_table(data.frame(
  eruptions = rep(faithful$eruptions, 500),
  waiting = rep(faithful$waiting, 500)
)))

df <- mizu_recv(ch, timeout = 30)
arrow::as_arrow_table(mizu_as_arrow(df))
#> Table
#> 136000 rows x 2 columns
#> $eruptions <double>
#> $waiting <double>

# a subset keeps the same backing pages
arrow::as_arrow_table(mizu_as_arrow(df["waiting"]))
#> Table
#> 136000 rows x 1 columns
#> $waiting <double>
mizu_close(ch)
```

Sending a stream consumes it: a RecordBatchReader is fully materialized
at send. The fidelity is frame-level, not Arrow-schema-exact: a
dictionary returns as a factor, and unsigned integers widen. The [dtype
matrix](https://shikokuchuo.net/pymizu/user-guide/interop.html#the-dtype-matrix)
on the pymizu site has the exact per-dtype correspondence.

## Mixed-language pools

Channels move values. Pools move computation, and a pool’s workers can
be Python processes too.
[`mizu_py_pool_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_py_pool_launcher.md)
spawns the workers.
[`mizu_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
builds a language-neutral task: a qualified `"mod.fn"` name or a
`.source =` string, plus constant arguments.
[`mizu_submit_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
submits it. Unnamed arguments become Python’s positional arguments, and
named ones become its keyword arguments:

``` r

p <- mizu_pool(2L, launcher = mizu_py_pool_launcher())

t <- mizu_submit_call(p, mizu_call("numpy.quantile", faithful$eruptions, q = c(0.25, 0.5, 0.75)))
mizu_collect(t)
#> [1] 2.16275 4.00000 4.45425
quantile(faithful$eruptions, c(0.25, 0.5, 0.75)) # R's own answer, for comparison
#>     25%     50%     75% 
#> 2.16275 4.00000 4.45425

s <- mizu_submit_call(p, mizu_call(.source = "y = x * 2\ny + 1", x = 20))
mizu_collect(s)
#> [1] 41
```

A `.source =` task runs in a fresh namespace with the named arguments
bound as names. The trailing expression’s value is the result. A plain
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
on a Python pool fails locally, and the error points to
[`mizu_submit_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md).
Task errors cross as `mizu_error_remote` conditions that keep the remote
type. Arguments follow the same rules as channel payloads, zero-copy
included. A large array crosses as one shared view, and a view already
in shared memory crosses as its name alone. The reverse direction
mirrors this: `pymizu.Pool.create()` with `pymizu.r_pool_launcher()`,
and `Pool.submit()` with `pymizu.call()` specs.

## The cross-language map

To map a Python function over an R vector, pass a
[`mizu_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
spec as
[`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md)’s
`.f`. Each element of `.x` becomes the spec’s first positional argument,
or binds as `x` for a `.source =` spec. `.template`,
`.collect = "view"`, and `.seed` work as they do on a same-language map:

``` r

mizu_map(p, faithful$waiting[1:6], mizu_call("numpy.log"), .template = numeric(1))
#> [1] 4.369448 3.988984 4.304065 4.127134 4.442651 4.007333
mizu_pool_stop(p)
```

[^1]: Below the floor a value crosses as one quick copy. At any size,
    these types cross: atomic vectors (logical, integer, double,
    complex, character, raw, and `integer64`, bit64’s exact layout
    constructed without bit64), strings (`NA_character_` reads as Python
    `None`, and `None` arrives as `NULL`), lists (a named list becomes a
    dict), and `Date`, `POSIXct`, and `difftime`. A few round trips
    change the container but never the values. A standalone factor
    returns as character strings, a length-1 `NA` returns as `NULL`, and
    a logical vector with missing values returns as integer. A `POSIXct`
    timezone becomes `"UTC"`, and `difftime` units become seconds. The
    bit patterns `INT32_MIN` and `INT64_MIN` collide with R’s own `NA`
    sentinel and read as `NA`. A `uint64` past 2^53 crosses as `NA` with
    a warning at send.
