# Submit a Task and Collect Its Result

`mizu_submit()` captures `expr` unevaluated, serializes it with its
named arguments into the injection ring of the submitter, and returns a
task handle immediately. Payloads past the inline budget travel in a
fresh region. A worker evaluates the expression in a fresh environment
that contains the arguments as bindings. `mizu_collect()` blocks until
the result is published, then returns the value of the task. If the task
raised an error, `mizu_collect()` signals that condition again in the
collecting process.

## Usage

``` r
mizu_submit(pool, expr, ..., .timeout = Inf)

mizu_collect(task, timeout = Inf)
```

## Arguments

- pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md).

- expr:

  an expression, captured unevaluated. This differs from
  [`mizu_channel()`](https://shikokuchuo.net/mizu/reference/mizu_channel.md),
  which requires its expression pre-quoted. The expression sees only the
  arguments in `...` and the global environment of the worker. The
  expression itself must load any packages it needs.

- ...:

  named values bound in the evaluation environment. The values are
  serialized.
  [`mori::share()`](https://rdrr.io/pkg/mori/man/share.html)d objects
  reduce to identifiers and map zero-copy on the worker.

- .timeout:

  seconds to wait for injection-ring space before the call raises
  `mizu_error_submit_timeout`. Submission blocks only when the ring is
  full (back-pressure) and returns immediately otherwise. `Inf` (the
  default) waits indefinitely; `0` does not wait.

- task:

  a task handle from `mizu_submit()`.

- timeout:

  seconds to wait for the result before the call returns the
  `mizu_timeout` sentinel. `Inf` (the default) waits indefinitely; `0`
  does not wait.

## Value

`mizu_submit()` returns a task handle (class `"mizu_task"`).
`mizu_collect()` returns the value of the task, or the `mizu_timeout`
sentinel.

## Details

Submission blocks only when the injection ring of the submitter is full:
back-pressure is per-submitter. On `.timeout` expiry, submission raises
`mizu_error_submit_timeout` instead of stalling. If no result arrives
within `timeout`, collection returns the `mizu_timeout` sentinel (class
`c("mizu_timeout", "mizu_sentinel")`). Collecting a task whose handle
was cancelled (or whose pool was stopped) raises `mizu_error_cancelled`.
Collecting a task whose executing worker died raises
`mizu_error_worker_died`, carrying the slot and pid of the worker (see
[mizu_error](https://shikokuchuo.net/mizu/reference/mizu_error.md)).
Worker death is detected at OS notification latency: a kernel-released
lock is the verdict, with no heartbeats and no polling. The death fails
exactly the tasks that the dead worker claimed, and the surviving
workers consume the work still queued on its deque.

## Outcomes

A timeout on collect is a normal outcome and is returned. Every
exceptional outcome is raised as a classed condition (see
[mizu_error](https://shikokuchuo.net/mizu/reference/mizu_error.md)):

|  |  |  |
|----|----|----|
| outcome | surfaced as | class |
| result published | the value, returned | — |
| no result within `timeout` | sentinel, returned | `c("mizu_timeout", "mizu_sentinel")` |
| ring full past `.timeout` | raised by `mizu_submit()` | `mizu_error_submit_timeout` |
| result slots exhausted | raised by `mizu_submit()` | `mizu_error_slots_exhausted` |
| pool stopped, or owner died | raised by `mizu_submit()` | `mizu_error_stopped` |
| task raised an error | re-signalled on collect | the condition classes of the task itself |
| cancelled, or pool stopped | raised on collect | `mizu_error_cancelled` |
| executing worker died | raised on collect | `mizu_error_worker_died` |

A re-signalled task error is a transport condition: it carries the
original classes, `message`, `call`, and every named field the payload
codec can carry within the result slot's inline budget, with anything
untransportable dropped and named in a `dropped_fields` field. See the
Task error transport section of
[mizu_error](https://shikokuchuo.net/mizu/reference/mizu_error.md).

Inside a task,
[`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md)
returns the handle of the evaluating worker, so a task can submit nested
subtasks. `mizu_submit(mizu_current_pool(), ...)` inside a task pushes
onto the work-stealing deque of the worker itself: no ring, no wait. A
full deque runs the subtask inline instead. A worker blocked in
`mizu_collect()` on a nested handle helps instead of sleeping. It
executes work from its own deque (and steals from peers) until the
awaited result is published. So nested fan-outs run at fork/join cost
and never deadlock the pool. A nested submission claims a submitter slot
for the worker on first use.

A handle can be collected exactly once: the result slot is released to
the pool as the value is returned. If an uncollected handle goes to the
garbage collector, a still-queued task is cancelled and a published
result is discarded. To wait on several handles at once,
[`mizu_collect_any()`](https://shikokuchuo.net/mizu/reference/mizu_collect_any.md)
reports the first terminal task and
[`mizu_collect_all()`](https://shikokuchuo.net/mizu/reference/mizu_collect_all.md)
returns every result in input order.

## Examples

``` r
p <- mizu_pool()
t <- mizu_submit(p, sum(x), x = runif(10))
mizu_collect(t, timeout = 30)
#> [1] 5.357433
mizu_pool_stop(p)
```
