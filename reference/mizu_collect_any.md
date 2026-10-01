# Collect the First Available Result From Several Tasks

`mizu_collect_any()` waits on several task handles at once and returns
as soon as any of them reaches a terminal state — result published,
error raised, cancelled, or its worker died. Among handles already
terminal, the earliest in `tasks` is reported. The wait parks on the
submitter's single parker: any publishing worker wakes it directly, with
no polling. For the whole set at once,
[`mizu_collect_all()`](https://shikokuchuo.github.io/mizu/reference/mizu_collect_all.md)
waits until every task is terminal and returns all results in input
order.

## Usage

``` r
mizu_collect_any(tasks, timeout = Inf)
```

## Arguments

- tasks:

  a non-empty list of task handles from
  [`mizu_submit()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md)
  on the same pool handle.

- timeout:

  seconds to wait for injection-ring space before the call raises
  `mizu_error_submit_timeout`. Submission blocks only when the ring is
  full (back-pressure) and returns immediately otherwise. `Inf` (the
  default) waits indefinitely; `0` does not wait.

## Value

For a published result, `list(index = i, value = v)`: the 1-based
position of the task in `tasks` and its value. Otherwise the
`mizu_timeout` sentinel.

## Outcomes

As for
[`mizu_collect()`](https://shikokuchuo.github.io/mizu/reference/mizu_submit.md),
but attributed to a handle by position:

|  |  |  |
|----|----|----|
| outcome | surfaced as | class |
| result published | `list(index, value)`, returned | — |
| nothing terminal within `timeout` | sentinel, returned | `c("mizu_timeout", "mizu_sentinel")` |
| task raised an error | re-signalled with an `index` field | the condition classes of the task itself |
| cancelled, or pool stopped | raised with an `index` field | `mizu_error_cancelled` |
| executing worker died | raised with an `index` field | `mizu_error_worker_died` |

The `index` field of a raised condition is the 1-based position of the
task in `tasks` (conditions are lists, so the field travels in place).
The reported handle is consumed; the remaining handles stay valid and
collectible.

## Examples

``` r
if (FALSE) { # interactive()
p <- mizu_pool(2L)
slow <- mizu_submit(p, { Sys.sleep(0.5); "slow" })
fast <- mizu_submit(p, "fast")
# completion order, not submission order
mizu_collect_any(list(slow, fast), timeout = 30)
mizu_collect(slow, timeout = 30)
mizu_pool_stop(p)
}
```
