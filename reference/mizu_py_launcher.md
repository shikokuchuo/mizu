# Python Launcher

Returns a launcher that spawns Python processes running
[pymizu](https://github.com/shikokuchuo/pymizu), the Python binding of
the same shared-memory core: `python -m pymizu.child` for the channel
peer of
[`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md),
`python -m pymizu.worker` for the pool workers of
[`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md) and
[`mizu_spawn_workers()`](https://shikokuchuo.net/mizu/reference/mizu_spawn_workers.md).
The mirror of pymizu's `r_launcher()`, which spawns R processes from a
Python host.

## Usage

``` r
mizu_py_launcher(python = NULL, stdout = "", stderr = "")
```

## Arguments

- python:

  path to the Python interpreter. The default looks up `python3` on the
  `PATH`.

- stdout, stderr:

  forwarded to [`system2()`](https://rdrr.io/r/base/system2.html) for
  the spawned process, as in
  [`mizu_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_launcher.md).

## Value

A `function(token, slot)`.
[`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md)
calls it with `token` alone to spawn the peer.
[`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md) and
[`mizu_spawn_workers()`](https://shikokuchuo.net/mizu/reference/mizu_spawn_workers.md)
call it with both arguments to spawn the worker for `slot`.

## Details

The first worker's join records the workers' language in the pool, so
the launcher carries no language attribute: a pool of Python workers
takes
[`mizu_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
specifications through
[`mizu_submit_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md),
and a plain
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
fails at once with an error naming
[`mizu_submit_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md).
A launcher that spawns the wrong language fails at join, not at the
first task.

The interpreter is probed for pymizu when the launcher is created, so a
missing interpreter or package raises here, before anything exists. The
payload rules for a non-R channel peer apply: only vectors and strings
cross (see
[`mizu_send()`](https://shikokuchuo.net/mizu/reference/mizu_send.md)).

## Examples

``` r
if (FALSE) { # \dontrun{
ch <- mizu_channel(
  r"(
import pymizu
while True:
    x = ch.recv(30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x * 2)
)",
  launcher = mizu_py_launcher()
)
mizu_send(ch, c(1.5, 2.5, 3.5))
mizu_recv(ch, timeout = 5)
mizu_close(ch)

p <- mizu_pool(2, launcher = mizu_py_launcher())
t <- mizu_submit_call(p, mizu_call("numpy.mean", runif(100)))
mizu_collect(t)
mizu_pool_stop(p)
} # }
```
