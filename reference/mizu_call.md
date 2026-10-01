# Call a Function in Another Language: the Task Specification

`mizu_call()` builds a task specification: a qualified name or a source
string in the pool workers' language, plus the constant arguments.
`mizu_submit_call()` stages it as a neutral task stream any worker
language reads. This is how a foreign pool is driven: spawn the workers
with
[`mizu_py_pool_launcher()`](https://shikokuchuo.github.io/mizu/reference/mizu_py_pool_launcher.md)
and submit specs to them.

## Usage

``` r
mizu_call(name = NULL, ..., source = NULL)

mizu_submit_call(pool, spec, .timeout = Inf)
```

## Arguments

- name:

  \[character(1)\] a qualified function name: `"pkg::fn"` (or
  `"pkg:::fn"`) for R workers, `"mod.fn"` for Python workers. The
  qualifier is required: a worker's global namespace is the runner
  module, never the submitter's, so the name resolves through the
  worker's own namespace machinery.

- ...:

  the constant arguments. Unnamed arguments map to the positional
  argument list and named ones to the named argument dict, matching
  Python's `*args` and `**kwargs`. Arguments must be portable values
  (the interchange subset documented in
  [`mizu_send()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)):
  a non-portable argument raises `mizu_error_not_portable` at submit,
  never a fallback.

- source:

  \[character(1)\] source code in the workers' language. Evaluated in a
  fresh namespace (parented on the global environment in R, a fresh dict
  over `__builtins__` in Python) with the named arguments bound as names
  and positional arguments bound as `..1`, `..2`, ... The result is the
  trailing expression's value, or `NULL` when the source ends with a
  statement.

- pool:

  a pool handle (see
  [`mizu_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool.md)).

- spec:

  a `"mizu_call"` specification.

- .timeout:

  maximum seconds to wait for submission capacity.

## Value

`mizu_call()`: a `"mizu_call"` specification (a classed list).
`mizu_submit_call()`: a task handle (class `"mizu_task"`), exactly as
[`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
returns.

## Details

The spec describes a call; it is not a value. The language never appears
at the call site: `mizu_submit_call()` resolves the workers' language
from the pool itself, for attached submitters too, and a bare
(unqualified) name errors at submit, not at spec construction.

Results and errors cross in the submitter's own formats: a task error
arrives as a `mizu_error_remote` condition (see
[`mizu_is_remote_error()`](https://shikokuchuo.github.io/mizu/reference/mizu_is_remote_error.md)),
and a result that has no portable home fails the task with one naming
the value's type. On a same-language pool the spec verb keeps the rich
private error format.

## Examples

``` r
p <- mizu_pool()
# same-language pools take specs too
t <- mizu_submit_call(p, mizu_call("stats::quantile", runif(100)))
mizu_collect(t)
#>          0%         25%         50%         75%        100% 
#> 0.007399441 0.256357024 0.467888899 0.690372972 0.997069135 
mizu_pool_stop(p)

if (FALSE) { # \dontrun{
p <- mizu_pool(2, launcher = mizu_py_pool_launcher())
t <- mizu_submit_call(p, mizu_call("numpy.mean", runif(100)))
mizu_collect(t)
mizu_pool_stop(p)
} # }
```
