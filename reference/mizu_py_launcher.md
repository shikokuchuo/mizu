# Python Channel Peer Launcher

Returns a launcher for
[`mizu_channel()`](https://shikokuchuo.github.io/mizu/reference/mizu_channel.md)
that spawns the peer as a Python process running
`python -m pymizu.child`, the peer entry of
[pymizu](https://github.com/shikokuchuo/pymizu), the Python binding of
the same shared-memory core. The mirror of pymizu's `r_launcher()`,
which spawns an R peer from a Python host.

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
  the peer process, as in
  [`mizu_launcher()`](https://shikokuchuo.github.io/mizu/reference/mizu_launcher.md).

## Value

A `function(token)` that spawns the peer process, for the `launcher`
argument of
[`mizu_channel()`](https://shikokuchuo.github.io/mizu/reference/mizu_channel.md).

## Details

The interpreter is probed for pymizu when the launcher is created, so a
missing interpreter or package raises here, before the channel exists.
The payload rules for a non-R peer apply: only vectors and strings cross
(see
[`mizu_send()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)).

## Examples

``` r
if (FALSE) { # \dontrun{
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
mizu_close(ch)
} # }
```
