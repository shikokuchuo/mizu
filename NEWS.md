# mizu (development version)

Initial release.

* `mizu_channel()`: lock-free SPSC channels between R processes — or
  between R and a Python peer — over POSIX shared memory or Win32 file
  mappings, with batching, serialization-free raw tiers, sentinel
  outcomes for a full ring, timeout, close or peer death, and peer death
  delivered as an OS event.
* `mizu_pool()`: work-stealing task pools over spawned workers —
  `mizu_submit()` / `mizu_collect()`, batch submission,
  `mizu_collect_any()` / `mizu_collect_all()`, worker retirement and
  respawn, nested submission from inside a task, and classed remote
  errors transported with their fields intact.
* `mizu_map()`: a parallel map over one shared staging region —
  adaptive morsel scheduling, streaming and prepared maps, `.template`
  output areas with a zero-copy `.collect = "view"`, and reproducible
  per-element random streams (`.seed`).
* Zero-copy vectors and data frames — received as ALTREP views over the
  shared pages, and a view sent on again crosses by reference.
* Zero-copy Arrow interchange — 'arrow' tables and record batches
  travel as one shared-memory frame, converting back with
  `mizu_as_arrow()` without copying.
* Python interop — channels and pools mix with Python processes running
  pymizu: `mizu_py_launcher()` spawns peers and workers, and
  `mizu_call()` task specs drive them, data frames crossing with full
  type fidelity.
* Event-driven async — `mizu_recv_async()`, `mizu_recv_handler()`, and
  `as.promise()` methods for channels, tasks, and maps over later and
  promises, ready for Shiny.
* Housekeeping — `mizu_prune()` reclaims shared memory orphaned by dead
  processes; `mizu_pool_dump()` introspects a running or hung pool.

# mizu 0.0.1

* Initial CRAN release.
