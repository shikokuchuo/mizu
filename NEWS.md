# mizu (development version)

* Faster re-sending of Arrow data obtained from `mizu_as_arrow()`: columns left unchanged are no longer copied.
* `mizu_submit_batch()` failing mid-batch now attaches the handles of the tasks already submitted to the raised condition as `accepted`; they run and stay collectible.
* Faster `mizu_map()` collection and lower shared-memory use for a (non-prepared) map collected by value.
* Faster `mizu_map()` on small and medium inputs.
* Idle workers now release the shared memory of finished maps promptly.
* Streaming `mizu_map()` with a `mizu_call()` spec no longer retains completed chunk regions on R workers.
* Native `mizu_map()` character-template results are validated and flattened per worker batch; a type or length mismatch now fails fast with its element index. Spec maps retain submitter-side validation.
* `mizu_cancel()` on a collected map handle no longer disrupts a later run of the same prepared map.
* New `mizu_recv_batch_async()`: the promise counterpart of `mizu_recv_batch()` — one promise resolves with a list of up to `n` messages, the high-throughput async receive.
* Fixed pool shutdown often waiting an extra 50 ms after its workers had exited cleanly.
* Fixes `mizu_send()` of an Arrow table failing with "attempt to apply non-function" once a `mizu_map()` result had been collected in the session (#19).
* Faster message delivery for the async channel verbs (`mizu_recv_async()`, `mizu_recv_batch_async()`, `mizu_recv_handler()`).
* Faster sends of large payloads and of Arrow tables with missing values or factor columns, and faster first reads of large zero-copy views on Linux.
* Faster by-value sends of Arrow tables, including nested tables and sends that cannot use a shared-memory frame region.
* Faster resolution of pool task promises with `mizu_collect_async()` and `as.promise()`, particularly when many tasks are awaiting collection.

# mizu 0.1.1

* Faster sends of ALTREP atomic vectors to a Python peer.

# mizu 0.1.0

Initial release.

* `mizu_channel()`: lock-free SPSC channels between R processes — or between R and a Python peer — over POSIX shared memory or Win32 file mappings, with sentinel outcomes for a full ring, timeout, close, or peer death.
* `mizu_pool()`: work-stealing task pools over spawned workers — `mizu_submit()` / `mizu_collect()`, batch submission, `mizu_collect_any()` / `mizu_collect_all()`, worker retirement and respawn, nested submission, and classed remote errors.
* `mizu_map()`: a parallel map — streaming and prepared maps, `.template` output areas with a zero-copy `.collect = "view"`, and reproducible per-element random streams (`.seed`).
* Zero-copy vectors and data frames — received as ALTREP views, and a view sent on again crosses by reference.
* Zero-copy Arrow interchange — 'arrow' tables and record batches travel without copying, converting back with `mizu_as_arrow()`.
* Python interop — channels and pools mix with Python processes running pymizu: `mizu_py_launcher()` spawns peers and workers, `mizu_call()` task specs drive them, and data frames cross with full type fidelity.
* Event-driven async — `mizu_recv_async()`, `mizu_recv_handler()`, `mizu_collect_async()`, and `as.promise()` methods for channels, tasks, and maps over later and promises, ready for Shiny.
* Agent skill — an agent skill for AI coding assistants ships in the installed package at `skills/r-mizu`, picked up automatically by `btw` and similar tools.
* Housekeeping — `mizu_prune()` reclaims shared memory orphaned by dead processes; `mizu_pool_dump()` introspects a running or hung pool.
