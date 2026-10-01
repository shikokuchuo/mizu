# Error Conditions

Terminal failures that a caller can act on programmatically are raised
as classed conditions. Each inherits `"mizu_error"` (alongside `"error"`
and `"condition"`), with a subclass that names the failure. Handlers
dispatch with `tryCatch(..., mizu_error_worker_died = ...)` or test with
[`inherits()`](https://rdrr.io/r/base/class.html) instead of matching
message text. Messages are not API and can be reworded. The class
vectors and fields below are API.

## Details

The subclasses, where they are raised, and the structured fields they
carry as condition elements:

- `mizu_error_submit_timeout` —
  [`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
  on `.timeout` expiry with the injection ring of the submitter still
  full.

- `mizu_error_slots_exhausted` —
  [`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
  and
  [`mizu_map()`](https://shikokuchuo.github.io/mizu/reference/mizu_map.md)
  when every result slot in the subrange of the submitter is already
  outstanding. Collect or cancel before resubmitting.

- `mizu_error_stopped` —
  [`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md),
  [`mizu_map()`](https://shikokuchuo.github.io/mizu/reference/mizu_map.md)
  and
  [`mizu_pool_attach()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_attach.md)
  against a pool that was stopped or whose owner process died.

- `mizu_error_cancelled` —
  [`mizu_collect()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
  on a task that was cancelled or whose pool was stopped.

- `mizu_error_worker_died` —
  [`mizu_collect()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
  on a task whose executing worker died. Fields `slot` (worker registry
  slot, 0-based as in
  [`mizu_pool_dump()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_dump.md))
  and `pid`: the claimant record of the result slot, read at collect
  time. This is informational, racy against slot reuse exactly as
  [`mizu_pool_dump()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool_dump.md)
  is, and `NA` where no claim was recorded.
  [`mizu_map()`](https://shikokuchuo.github.io/mizu/reference/mizu_map.md)
  signals this class again with the lost elements as an additional
  `elements` field: a two-column matrix of inclusive `lo, hi` ranges,
  runner-granular and conservative (see the Errors section of
  [`mizu_map()`](https://shikokuchuo.github.io/mizu/reference/mizu_map.md)).

- `mizu_error_startup` —
  [`mizu_channel()`](https://shikokuchuo.github.io/mizu/reference/mizu_channel.md),
  [`mizu_pool()`](https://shikokuchuo.github.io/mizu/reference/mizu_pool.md)
  and
  [`mizu_spawn_workers()`](https://shikokuchuo.github.io/mizu/reference/mizu_spawn_workers.md)
  when a child process fails to attach within `startup_timeout`.

- `mizu_error_shm` — shared-memory region create or open failure
  anywhere on the surface. Field `bytes`: the requested size of a region
  that was not created, `NA` when a region was not opened.

- `mizu_error_python_payload` —
  [`mizu_recv()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
  or
  [`mizu_recv_batch()`](https://shikokuchuo.github.io/mizu/reference/mizu_send_batch.md)
  on a channel message written in a language-private stream (a pymizu
  codec or pickle payload) that this reader cannot interpret. The
  portable interchange subset crosses; the declined message is consumed,
  so the channel keeps flowing.

- `mizu_error_not_portable` —
  [`mizu_send()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
  on a channel whose peer is a foreign-language process, of a value
  outside the portable interchange subset (a named atomic vector, an
  environment, a closure, an S4 object, an ordered factor, a data frame
  subclass, and the like). Fields `path` (where in the value the walk
  declined), `reason`, and `remedy` (a one-line rewrite where one
  exists, else `character(0)`).

- `mizu_error_remote` — never raised by mizu itself: the value a channel
  receive returns for a remote error stream, most commonly a peer
  process whose expression errored (the peer shims send one before exit,
  on every channel). Fields `remote_type` (the most-specific class of
  the original error), `message`, `detail` (the call or traceback text),
  and `index` (the 1-based element index) when the remote error carries
  one. Test with
  [`mizu_is_remote_error()`](https://shikokuchuo.github.io/mizu/reference/mizu_is_remote_error.md);
  raise with
  [`mizu_raise()`](https://shikokuchuo.github.io/mizu/reference/mizu_is_remote_error.md).

Errors of misuse (unnamed task arguments, out-of-range slots, operations
on a closed handle) stay plain errors: the classed hierarchy covers the
outcomes that a running system produces, not programming mistakes.

Raised `mizu_error` conditions are distinct from sentinels (class
`mizu_sentinel`, returned by
[`mizu_send()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md),
[`mizu_recv()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md)
and
[`mizu_collect()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)).
A sentinel is an ordinary return value that tags a terminal state on the
hot path, not a signalled condition. See
[`mizu_is_sentinel()`](https://shikokuchuo.github.io/mizu/reference/mizu_is_sentinel.md).

Which discipline applies follows the shape of the call. The verbs that
move payloads and wait with a bound —
[`mizu_send()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md),
[`mizu_recv()`](https://shikokuchuo.github.io/mizu/reference/mizu_send.md),
[`mizu_collect()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md),
[`mizu_map()`](https://shikokuchuo.github.io/mizu/reference/mizu_map.md)
— return sentinels for transport states. These are: not yet
(`mizu_timeout`), not now (`mizu_full`), stream over (`mizu_closed`,
`mizu_peer_gone`). Their caller is a loop, and these are its normal
outcomes. Conditions are raised where a request failed for good.
Constructors and
[`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md),
whose return is a handle the next line uses, raise on every failure.
[`mizu_collect()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
raises when the value can never arrive: the own error of the task
re-signalled, `mizu_error_cancelled`, `mizu_error_worker_died`. A
sentinel invites the next iteration of the loop. A condition means stop
and deal with it.

## Task error transport

A task's own error, re-signalled by
[`mizu_collect()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
(and
[`mizu_collect_any()`](https://shikokuchuo.github.io/mizu/reference/mizu_collect_any.md)
/
[`mizu_collect_all()`](https://shikokuchuo.github.io/mizu/reference/mizu_collect_all.md)),
is a transport condition built on the worker at publish time. The caught
condition itself never crosses, so a condition that cannot be serialized
can never kill the worker. The transport condition carries the original
classes, the raw `message` field (custom
[`conditionMessage()`](https://rdrr.io/r/base/conditions.html) methods
are bypassed; the message is truncated past its share of the result
slot's inline budget), `call`, and every named field the compact payload
codec can carry within that budget, in the priority order message,
`call`, fields, then `dropped_fields`. Fields the codec cannot carry
(environments, closures, S4 objects, ALTREP vectors, external pointers)
and fields past the remaining budget are dropped and named in a
`dropped_fields` field, absent when nothing was dropped. Only the named
elements of a condition are considered: an unnamed element cannot be
named in `dropped_fields` and is dropped silently.
