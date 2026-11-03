# Call a Function in Another Language

`mizu_call()` builds a task specification: a qualified name or a source
string in the pool workers' language, plus the constant arguments.
`mizu_submit_call()` stages it as a neutral task stream that any worker
language reads. This is how you drive a pool of workers in another
language: spawn the workers with
[`mizu_py_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_py_launcher.md)
and submit specs to them.

## Usage

``` r
mizu_call(.name = NULL, ..., .source = NULL)

mizu_submit_call(pool, spec, .timeout = Inf)
```

## Arguments

- .name:

  \[character(1)\] a qualified function name: `"pkg::fn"` (or
  `"pkg:::fn"`) for R workers, `"mod.fn"` for Python workers. The
  qualifier is required because the name resolves in the worker's own
  namespace, which is the runner module rather than the submitter's
  global environment.

- ...:

  the constant arguments. Unnamed arguments map to the positional
  argument list and named ones to the named argument dict, matching
  Python's `*args` and `**kwargs`. Arguments must be portable values
  (the interchange subset documented in
  [`mizu_send()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)).
  A non-portable argument raises `mizu_error_not_portable` at submit.

- .source:

  \[character(1)\] source code in the workers' language. Evaluated in a
  fresh namespace (parented on the global environment in R, a fresh dict
  over `__builtins__` in Python) with the named arguments bound as names
  and positional arguments bound as `..1`, `..2`, ... The result is the
  trailing expression's value, or `NULL` when the source ends with a
  statement.

- pool:

  a pool handle (see
  [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)).

- spec:

  a `"mizu_call"` specification.

- .timeout:

  maximum seconds to wait for submission capacity.

## Value

`mizu_call()`: a `"mizu_call"` specification (a classed list).
`mizu_submit_call()`: a task handle (class `"mizu_task"`), exactly as
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
returns.

## Details

The spec describes a call, not a value. The language is not named at the
call site: `mizu_submit_call()` reads the workers' language from the
pool itself (for attached submitters too), and a bare unqualified name
errors at submit rather than when the spec is built.

Results and errors cross in the submitter's own formats. A task error
arrives as a `mizu_error_remote` condition (see
[`mizu_is_remote_error()`](https://shikokuchuo.net/mizu/reference/mizu_is_remote_error.md)),
and a result that is not portable fails the task with a condition naming
the value's type. On a same-language pool, `mizu_submit_call()` keeps
the full same-language error format.

Large arguments cross to foreign workers by reference rather than by
copy. A fresh value past the zero-copy floor is written once into a
shared region, and the worker reads a view over it. An argument that is
already a shared view (received from a channel, a pool result, or a
[`mori::share()`](https://rdrr.io/pkg/mori/man/share.html)d vector)
crosses as its identifier alone, with no payload bytes. On a pool whose
workers are too old to read a reference, such a task fails at submit
with an error naming the fix.

## Examples

``` r
p <- mizu_pool()
# same-language pools take specs too
t <- mizu_submit_call(p, mizu_call("stats::quantile", runif(100)))
mizu_collect(t)
#>         0%        25%        50%        75%       100% 
#> 0.03167256 0.28760158 0.54435834 0.70977269 0.98961159 
mizu_pool_stop(p)

if (FALSE) { # interactive()
p <- mizu_pool(2, launcher = mizu_py_launcher())
t <- mizu_submit_call(p, mizu_call("numpy.mean", runif(100)))
mizu_collect(t)
mizu_pool_stop(p)
}
```
