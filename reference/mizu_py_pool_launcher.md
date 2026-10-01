# Python Pool Worker Launcher

Returns a launcher for
[`mizu_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool.md)
that spawns each worker as a Python process running
`python -m pymizu.worker`, the worker entry of
[pymizu](https://github.com/shikokuchuo/pymizu), the Python binding of
the same shared-memory core. The mirror of pymizu's `r_pool_launcher()`,
which spawns R workers from a Python host.

## Usage

``` r
mizu_py_pool_launcher(python = NULL, stdout = "", stderr = "")
```

## Arguments

- python:

  path to the Python interpreter. The default looks up `python3` on the
  `PATH`.

- stdout, stderr:

  forwarded to [`system2()`](https://rdrr.io/r/base/system2.html) for
  the peer process, as in
  [`mizu_launcher()`](https://shikokuchuo.github.io/mizu/reference/mizu_launcher.md).

## Value

A `function(token, slot)` that spawns one worker process, for the
`launcher` argument of
[`mizu_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool.md)
and
[`mizu_spawn_workers()`](https://shikokuchuo.github.io/mizu/reference/mizu_spawn_workers.md).

## Details

The first worker's join records the workers' language in the pool, so
the launcher carries no language attribute: a pool of Python workers
takes
[`mizu_call()`](https://shikokuchuo.github.io/mizu/reference/mizu_call.md)
specifications through
[`mizu_submit_call()`](https://shikokuchuo.github.io/mizu/reference/mizu_call.md),
and a native
[`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
errors locally naming the spec verb. A launcher that spawns the wrong
language fails at join, not at the first task.

The interpreter is probed for pymizu when the launcher is created, so a
missing interpreter or package raises here, before any pool exists.

## Examples

``` r
if (FALSE) { # \dontrun{
p <- mizu_pool(2, launcher = mizu_py_pool_launcher())
t <- mizu_submit_call(p, mizu_call("numpy.mean", runif(100)))
mizu_collect(t)
mizu_pool_stop(p)
} # }
```
