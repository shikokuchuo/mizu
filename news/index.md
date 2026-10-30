# Changelog

## mizu (development version)

Initial release.

- [`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md):
  lock-free SPSC channels between R processes — or between R and a
  Python peer — over POSIX shared memory or Win32 file mappings, with
  batching, serialization-free raw tiers, sentinel outcomes for a full
  ring, timeout, close or peer death, and peer death delivered as an OS
  event.
- [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md):
  work-stealing task pools over spawned workers —
  [`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
  /
  [`mizu_collect()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md),
  batch submission,
  [`mizu_collect_any()`](https://shikokuchuo.net/mizu/reference/mizu_collect_any.md)
  /
  [`mizu_collect_all()`](https://shikokuchuo.net/mizu/reference/mizu_collect_all.md),
  worker retirement and respawn, nested submission from inside a task,
  and classed remote errors transported with their fields intact.
- [`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md): a
  parallel map over one shared staging region — adaptive morsel
  scheduling, streaming and prepared maps, `.template` output areas with
  a zero-copy `.collect = "view"`, and reproducible per-element random
  streams (`.seed`).
- Zero-copy vectors and data frames — received as ALTREP views over the
  shared pages, and a view sent on again crosses by reference.
- Zero-copy Arrow interchange — ‘arrow’ tables and record batches travel
  as one shared-memory frame, converting back with
  [`mizu_as_arrow()`](https://shikokuchuo.net/mizu/reference/mizu_as_arrow.md)
  without copying.
- Python interop — channels and pools mix with Python processes running
  pymizu:
  [`mizu_py_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_py_launcher.md)
  /
  [`mizu_py_pool_launcher()`](https://shikokuchuo.net/mizu/reference/mizu_py_pool_launcher.md)
  spawn peers and workers, and
  [`mizu_call()`](https://shikokuchuo.net/mizu/reference/mizu_call.md)
  task specs drive them, data frames crossing with full type fidelity.
- Event-driven async —
  [`mizu_recv_async()`](https://shikokuchuo.net/mizu/reference/mizu_recv_async.md),
  [`mizu_recv_handler()`](https://shikokuchuo.net/mizu/reference/mizu_recv_async.md),
  and
  [`as.promise()`](https://rstudio.github.io/promises/reference/is.promise.html)
  methods for channels, tasks, and maps over later and promises, ready
  for Shiny.
- Housekeeping —
  [`mizu_prune()`](https://shikokuchuo.net/mizu/reference/mizu_prune.md)
  reclaims shared memory orphaned by dead processes;
  [`mizu_pool_dump()`](https://shikokuchuo.net/mizu/reference/mizu_pool_dump.md)
  introspects a running or hung pool.

## mizu 0.0.1

CRAN release: 2026-09-30

- Initial CRAN release.
