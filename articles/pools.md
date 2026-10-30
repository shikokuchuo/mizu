# Task pools

[`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)
builds on the same shared-memory transport as a channel. It spawns a
pool of worker processes with work-stealing deques and no dispatcher in
the loop. A submitted task goes straight from the submitting process
into shared memory, where a worker claims it. Idle workers steal from
busy ones, so the load balances itself.
[`mizu_submit()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
captures an expression together with the values it needs and returns a
task handle immediately. Your session stays free to continue.
[`mizu_collect()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
waits for the result of that task:

``` r

library(mizu)

p <- mizu_pool(n_workers = 4L)

t <- mizu_submit(p, sum(x) + y, x = 1:10, y = 100)
mizu_collect(t)
#> [1] 155
```

If a task raises an error, the pool captures it and signals it again in
your session when you collect the result. `mizu_cancel(t)` withdraws a
task. A task still queued is skipped. A task already running completes,
and its result is discarded. Cancellation never interrupts executing
code.

## Nested tasks

Inside a task,
[`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md)
returns the handle of the worker running it, so a task can split itself
into subtasks. A nested submit pushes straight onto that worker’s own
work-stealing deque — no ring, no wake. A worker waiting on a nested
result executes other work instead of sleeping. Divide-and-conquer runs
at minimal overhead and never deadlocks the pool:

``` r

t <- mizu_submit(
  p,
  {
    subtasks <- lapply(parts, \(part) mizu_submit(mizu_current_pool(), sum(x), x = part))
    do.call(sum, lapply(subtasks, mizu_collect))
  },
  parts = split(1:1000, rep(1:4, each = 250))
)
mizu_collect(t)
#> [1] 500500
```

[`mizu_map()`](https://shikokuchuo.net/mizu/reference/mizu_map.md) nests
the same way: a task can map over its own pool with
`mizu_map(mizu_current_pool(), ...)` — see [Nested
maps](https://shikokuchuo.net/mizu/articles/map.html#nested-maps).

## A default pool for package code

Package code can read a process-wide default pool with
[`mizu_default_pool()`](https://shikokuchuo.net/mizu/reference/mizu_default_pool.md).
[`mizu_set_default_pool()`](https://shikokuchuo.net/mizu/reference/mizu_default_pool.md)
sets it — `NULL` clears it — and invisibly returns the previous default,
so callers can save and restore. The registry keeps the handle alive: a
pool set as the default stays alive even after its variable is removed.
[`mizu_with_pool()`](https://shikokuchuo.net/mizu/reference/mizu_default_pool.md)
scopes the default to an expression,
[`mizu_local_pool()`](https://shikokuchuo.net/mizu/reference/mizu_default_pool.md)
to the calling frame. Both restore the previous default on exit.

Resolve an optional pool in this order: an explicit `pool` argument,
[`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md)
inside a task,
[`mizu_default_pool()`](https://shikokuchuo.net/mizu/reference/mizu_default_pool.md),
then your own fallback — sequential evaluation or an error. The default
never overrides
[`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md):
inside a task the worker’s own handle wins, so nested submission is
never shadowed. Setting a default checks the type only — a stopped pool
is accepted and fails at use time — and handles from
[`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md)
are valid defaults.

``` r

run <- function(x, pool = mizu_default_pool()) {
  if (is.null(pool)) {
    stop("no pool: pass one, or set a default", call. = FALSE)
  }
  mizu_collect(mizu_submit(pool, x * 2L, x = x))
}

old <- mizu_set_default_pool(p)
run(21L)
#> [1] 42
mizu_set_default_pool(old)
```

## Waiting on several tasks

[`mizu_collect_any()`](https://shikokuchuo.net/mizu/reference/mizu_collect_any.md)
waits on a list of task handles and returns `list(index, value)` for the
first to reach a terminal state — completion order, not submission
order. The other handles stay collectible.
[`mizu_collect_all()`](https://shikokuchuo.net/mizu/reference/mizu_collect_all.md)
waits until every task is terminal and returns all values in input
order, with the names of the handle list carried over. Both take one
overall `timeout` and return the `mizu_timeout` sentinel on expiry,
consuming nothing. An outcome that raises — the task’s own error, a
cancelled task, a dead worker — gains a 1-based `index` field naming the
handle’s position, and only the reported handle is consumed:

``` r

slow <- mizu_submit(p, { Sys.sleep(0.5); "slow" })
fast <- mizu_submit(p, "fast")
mizu_collect_any(list(slow, fast), timeout = 5)
#> $index
#> [1] 2
#> 
#> $value
#> [1] "fast"
mizu_collect(slow)
#> [1] "slow"

ts <- list(
  total = mizu_submit(p, sum(x), x = runif(10)),
  label = mizu_submit(p, "done")
)
mizu_collect_all(ts, timeout = 5)
#> $total
#> [1] 4.978443
#> 
#> $label
#> [1] "done"
```

On the submit side,
[`mizu_submit_batch()`](https://shikokuchuo.net/mizu/reference/mizu_submit_batch.md)
is the batch form: one task per element of a list of pre-quoted
expressions in a single call — one crossing into shared memory and one
wake-up sweep for the whole burst, with the `...` arguments shared by
every task:

``` r

ts <- mizu_submit_batch(p, list(quote(1 + 1), quote(2 + 2)))
mizu_collect_all(ts, timeout = 5)
#> [[1]]
#> [1] 2
#> 
#> [[2]]
#> [1] 4
```

## Promises and handlers

For event-driven consumers, you can call
[`then()`](https://rstudio.github.io/promises/reference/then.html)
directly on a task handle: `then(task, fn)` (or `as.promise(task)`)
resolves with the task’s value and rejects with the classed condition
[`mizu_collect()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
would raise — worker death included.
[`mizu_collect_all_async()`](https://shikokuchuo.net/mizu/reference/mizu_collect_all_async.md)
and
[`mizu_collect_any_async()`](https://shikokuchuo.net/mizu/reference/mizu_collect_all_async.md)
are the async forms of the collective functions, with exactly one
promise per collection: collect-all resolves in input order with names
carried, the first erroring handle by position rejecting with `index`;
collect-any resolves `list(index, value)`. A map handle gets the same
treatment — `then(map_handle, fn)` resolves with the whole gathered
result. Requires the `later` and `promises` packages.

Resolution is per collection, never per task: a promise costs tens of
microseconds of R work against about a microsecond of transport, so a
map of ten thousand elements resolves one promise rather than allocating
ten thousand. A burst of results produces one wake-up no matter how many
workers published it, and every main-thread drain is bounded per
event-loop turn and re-scheduled.

``` r

library(promises)
slow <- mizu_submit(p, { Sys.sleep(0.2); "slow" })
then(slow, print)

m <- mizu_map(p, 1:100, \(i) i * i, .template = integer(1))
then(m, \(v) print(sum(v)))

for (i in 1:8) later::run_now(1) # pump the loop until the resolutions land
#> [1] 338350
#> [1] "slow"
```

A task with a promise attached — or part of a pending async collection —
refuses a blocking
[`mizu_collect()`](https://shikokuchuo.net/mizu/reference/mizu_submit.md)
and a second promise: one consumer per result.
[`mizu_cancel()`](https://shikokuchuo.net/mizu/reference/mizu_cancel.md)
stays allowed throughout; the cancel publishes, and the drain rejects.

## Sizing, sharing and watching a pool

A pool can grow and shrink while it runs. Retirement never interrupts a
task: the worker finishes its current task, and the remaining workers
take over anything still queued to it. Other R processes can join a
running pool as submitters — the join token of the pool is the only
thing they need:

``` r

mizu_spawn_workers(p, n = 2L)     # two more workers join the pool
mizu_retire_worker(p, slot = 0L)  # worker 0 exits after its current task

# in another R process — submit and collect exactly as the creator does:
p2 <- mizu_pool_attach(token)     # token: mizu_pool_status(p)$token on the creator
```

Four read-only tools observe a running pool without disturbing it:

``` r

mizu_pool_status(p)  # snapshot: worker states, queued tasks, result slots
mizu_pool_stats(p)   # cumulative counters: tasks run, steals, parks per worker
mizu_pool_dump(p)    # every slot in full detail — the first tool when a pool hangs
mizu_pool_trace(p, \(event, id) message(event, " ", id))  # task lifecycle hook
```

When you are done,
[`mizu_pool_stop()`](https://shikokuchuo.net/mizu/reference/mizu_pool_stop.md)
cancels the pending tasks, waits for the workers to exit cleanly, and
releases the shared region:

``` r

mizu_pool_stop(p)
```
