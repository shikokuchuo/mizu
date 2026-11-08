# Changelog

## mizu (development version)

- Fixes
  [`mizu_send()`](https://shikokuchuo.net/mizu/dev/reference/mizu_send.md)
  of an Arrow table failing with “attempt to apply non-function” once a
  [`mizu_map()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map.md)
  result had been collected in the session
  ([\#19](https://github.com/shikokuchuo/mizu/issues/19)).

## mizu 0.1.1

- Faster sends of ALTREP atomic vectors to a Python peer.

## mizu 0.1.0

CRAN release: 2026-11-04

Initial release.

- [`mizu_channel()`](https://shikokuchuo.net/mizu/dev/reference/mizu_channel.md):
  lock-free SPSC channels between R processes — or between R and a
  Python peer — over POSIX shared memory or Win32 file mappings, with
  sentinel outcomes for a full ring, timeout, close, or peer death.
- [`mizu_pool()`](https://shikokuchuo.net/mizu/dev/reference/mizu_pool.md):
  work-stealing task pools over spawned workers —
  [`mizu_submit()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md)
  /
  [`mizu_collect()`](https://shikokuchuo.net/mizu/dev/reference/mizu_submit.md),
  batch submission,
  [`mizu_collect_any()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_any.md)
  /
  [`mizu_collect_all()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_all.md),
  worker retirement and respawn, nested submission, and classed remote
  errors.
- [`mizu_map()`](https://shikokuchuo.net/mizu/dev/reference/mizu_map.md):
  a parallel map — streaming and prepared maps, `.template` output areas
  with a zero-copy `.collect = "view"`, and reproducible per-element
  random streams (`.seed`).
- Zero-copy vectors and data frames — received as ALTREP views, and a
  view sent on again crosses by reference.
- Zero-copy Arrow interchange — ‘arrow’ tables and record batches travel
  without copying, converting back with
  [`mizu_as_arrow()`](https://shikokuchuo.net/mizu/dev/reference/mizu_as_arrow.md).
- Python interop — channels and pools mix with Python processes running
  pymizu:
  [`mizu_py_launcher()`](https://shikokuchuo.net/mizu/dev/reference/mizu_py_launcher.md)
  spawns peers and workers,
  [`mizu_call()`](https://shikokuchuo.net/mizu/dev/reference/mizu_call.md)
  task specs drive them, and data frames cross with full type fidelity.
- Event-driven async —
  [`mizu_recv_async()`](https://shikokuchuo.net/mizu/dev/reference/mizu_recv_async.md),
  [`mizu_recv_handler()`](https://shikokuchuo.net/mizu/dev/reference/mizu_recv_async.md),
  [`mizu_collect_async()`](https://shikokuchuo.net/mizu/dev/reference/mizu_collect_all_async.md),
  and
  [`as.promise()`](https://rstudio.github.io/promises/reference/is.promise.html)
  methods for channels, tasks, and maps over later and promises, ready
  for Shiny.
- Agent skill — an agent skill for AI coding assistants ships in the
  installed package at `skills/r-mizu`, picked up automatically by `btw`
  and similar tools.
- Housekeeping —
  [`mizu_prune()`](https://shikokuchuo.net/mizu/dev/reference/mizu_prune.md)
  reclaims shared memory orphaned by dead processes;
  [`mizu_pool_dump()`](https://shikokuchuo.net/mizu/dev/reference/mizu_pool_dump.md)
  introspects a running or hung pool.
