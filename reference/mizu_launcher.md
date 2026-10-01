# Default Child Process Launcher

Returns the launcher that
[`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md),
[`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md) and
[`mizu_spawn_workers()`](https://shikokuchuo.net/mizu/reference/mizu_spawn_workers.md)
use unless given a custom one. It spawns a detached child R process
through a static `Rscript` runner, with the entry expression and the
[`.libPaths()`](https://rdrr.io/r/base/libPaths.html) of the host
hex-encoded in argv.

## Usage

``` r
mizu_launcher(stdout = "", stderr = "")
```

## Arguments

- stdout, stderr:

  forwarded to [`system2()`](https://rdrr.io/r/base/system2.html). The
  default `""` sends the child output (for a channel peer, including its
  error epilogue) to the console of the host, `FALSE` discards it, and a
  file name collects it in that file.

  On Windows, the child holds a redirection file without sharing for its
  lifetime. Only one live process can use a file at a time. Other
  processes cannot read the file while the child lives. A second spawn
  with the same file fails to launch. In a pool, this failure causes a
  startup timeout. A custom launcher that sets the file name from `slot`
  gives one log file per worker (see examples).

## Value

A `function(token, slot)`.
[`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md) and
[`mizu_spawn_workers()`](https://shikokuchuo.net/mizu/reference/mizu_spawn_workers.md)
call it with both arguments to spawn the worker for `slot`.
[`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md)
calls it with `token` alone to spawn the peer.

## Containers

`system2(wait = FALSE)` detaches background children, so workers and
peers are adopted by PID 1 of the process namespace and reaped by that
init when they exit. In a container whose PID 1 does not reap (a plain
`docker run` without `--init`), exited children accumulate as zombie
PID-table entries. This is harmless to mizu itself — death verdicts come
from the liveness lock, never the PID — but PID-probe supervision
misreads zombies as alive, and
[`mizu_prune()`](https://shikokuchuo.net/mizu/reference/mizu_prune.md)
cannot reclaim a dead process's regions while its PID stays taken.

## Examples

``` r
# One log file per worker on Windows.
launcher <- function(token, slot) {
  f <- if (missing(slot)) "peer.log" else sprintf("worker-%d.log", slot)
  mizu_launcher(stderr = f)(token, slot)
}
```
