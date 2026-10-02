# Python interop

The channel speaks a language-agnostic wire protocol, so the peer at the
other end does not have to be R.
[pymizu](https://github.com/shikokuchuo/pymizu) is the Python binding of
the same shared-memory core, and a channel connects the two directly.
Pass the peer program as a source string (instead of a quoted
expression) and set the launcher to
[`mizu_py_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_py_launcher.md),
which spawns `python3 -m pymizu.child` — probing for the interpreter and
pymizu before the channel is created. Here a Python echo loop, mirroring
the R one in
[Channels](https://shikokuchuo.net/mizu/articles/channels.md) (runs when
a `python3` with pymizu and NumPy installed is on the `PATH`):

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

mizu_send(ch, c(1.5, 2.5, 3.5))
mizu_recv(ch, timeout = 5)
#> [1] 3 5 7
mizu_close(ch)
```

Atomic vectors cross without serialization: an R `numeric`, `integer`,
or `raw` vector arrives in Python as a NumPy `float64`, `int32`, or
`uint8` array, and the same dtypes travel back the other way. An
`integer64` vector crosses as a NumPy `int64` array both ways — bit64’s
exact layout, constructed without bit64 — with `NA` at `INT64_MIN` in
each direction. A large vector arrives as a zero-copy read-only view
over the shared pages — the same ALTREP mechanism as between R
processes, surfaced on the Python side as a NumPy array that maps the
region. Strings cross as strings in both directions; `NA_character_`
reads as `None` in Python, and `None` comes back as `NULL`. A large
Python `list[str | None]` arrives as a zero-copy character-vector view
over the shared pages — `None` as `NA_character_`, an empty string as
`""` — one layout write, no copy or parse; below the zero-copy floor the
list crosses by value.

Scalars, lists, and dicts with string keys cross both ways as R’s own
logical/integer/double/complex/character/list values. A `Date` arrives
as `datetime64[D]`, a `POSIXct` as naive `datetime64[us]`, a factor as
`list[str | None]` (a dictionary-encoded column inside a frame), and a
matrix as a Fortran-order array — the data never transposes. A large
Fortran-order numpy array arrives as a zero-copy matrix view over the
shared pages — one layout write, no copy or parse; C-order and strided
arrays still cross value-exact by copy. A `difftime`, `POSIXlt`, a named
vector, and S4 objects stay on R’s side: sending one on a Python channel
raises at
[`mizu_send()`](https://shikokuchuo.net/mizu/reference/mizu_send.md).

A data frame crosses as a `pymizu.Frame` — column dict on one side,
`pl.from_arrow(f)` / `pa.table(f)` on the way into polars or pyarrow —
and a polars, pyarrow, or pandas frame arrives back as a `data.frame`.
Past a size floor the frame travels as one shared-memory region both
directions: no copy, no parse, numeric columns read in place. An
unmodified R → polars → R round trip moves zero payload bytes at all —
the return hop is a reference to the region R itself staged. The full
contract, with the NA-sentinel and widening rules per dtype, is the
dtype matrix in the pymizu README; a value outside the portable subset
raises at send time on either side, naming the path and reason.

## Mixed-language pools

Channels move values; pools move computation — a pool’s workers can be
Python processes, driven through a neutral task specification.
[`mizu_py_pool_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_py_pool_launcher.md)
spawns them (`python3 -m pymizu.worker`, probed like the channel
launcher),
[`mizu_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
describes the task — a qualified `"mod.fn"` name or a `.source =`
string, plus the constant arguments — and
[`mizu_submit_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
stages it:

``` r

p <- mizu_pool(2L, launcher = mizu_py_pool_launcher())
t <- mizu_submit_call(p, mizu_call("numpy.quantile", runif(100), q = c(0.25, 0.5, 0.75)))
mizu_collect(t)
#> [1] 0.3387805 0.5704322 0.7699057
s <- mizu_submit_call(p, mizu_call(.source = "y = x * 2\ny + 1", x = 20))
mizu_collect(s)
#> [1] 41
mizu_pool_stop(p)
```

Unnamed arguments map to the positional list and named ones to the named
dict, matching Python’s `*args, **kwargs`; a `.source =` task evaluates
in a fresh namespace with the named arguments bound as names and the
result as the trailing expression’s value. The workers’ language comes
from the pool itself — a plain
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
on a foreign pool errors locally naming
[`mizu_submit_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md),
a bare unqualified name errors at submit, and a non-portable argument
raises `mizu_error_not_portable` at submit. Task errors cross as
`mizu_error_remote` conditions with the remote type preserved, and a
result without a portable home fails the task with one naming the type.
Large arguments cross by reference rather than by copy: a fresh array
past the zero-copy floor stages one layout write into a shared region
and the worker reads a view over it, and an argument that is already a
shared view (received from a channel, a pool result, or a
[`mori::share()`](https://rdrr.io/pkg/mori/man/share.html)d vector)
crosses as its identifier alone — a received view re-sent as an argument
moves zero payload bytes. A pool whose workers predate the ref reader
declines such arguments locally at submit, naming the remedy. The
reverse direction mirrors: `pymizu.Pool.create()` with
`pymizu.r_pool_launcher()`, and `Pool.submit()` with `pymizu.call()`
specs.

## The cross-language map

A spec is also
[`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md)’s
`.f` — the cross-language map. The element fills the spec’s first
positional slot (name kind) or binds as `x` (source kind), the spec’s
own constants ride with it (so `...` stays empty), and `.template`,
`.collect = "view"`, and `.seed` all work — the seed carrying as a
neutral pair each worker language derives its own streams from, so
invariance holds per worker language rather than identical draws across
them:

``` r

p <- mizu_pool(2L, launcher = mizu_py_pool_launcher())
mizu_map(p, runif(6), mizu_call("numpy.quantile", q = c(0.25, 0.75)))
#> [[1]]
#> [1] 0.4734116 0.4734116
#> 
#> [[2]]
#> [1] 0.673698 0.673698
#> 
#> [[3]]
#> [1] 0.2863747 0.2863747
#> 
#> [[4]]
#> [1] 0.86397 0.86397
#> 
#> [[5]]
#> [1] 0.4032393 0.4032393
#> 
#> [[6]]
#> [1] 0.1486028 0.1486028
mizu_map(p, runif(6), mizu_call("numpy.log"), .template = numeric(1))
#> [1] -0.1312512 -2.0955744 -1.2864743 -2.6057958 -0.6011432 -0.2042145
mizu_pool_stop(p)
```
