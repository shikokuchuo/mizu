# Inspect a Pool

A read-only snapshot of the pool region: registry states, parked-worker
count, queued injection entries, and result-slot occupancy.

## Usage

``` r
mizu_pool_status(pool)
```

## Arguments

- pool:

  a pool handle from
  [`mizu_pool()`](https://shikokuchuo.net/mizu/reference/mizu_pool.md)
  or
  [`mizu_pool_attach()`](https://shikokuchuo.net/mizu/reference/mizu_pool_attach.md);
  inside a task, the evaluating worker's own handle from
  [`mizu_current_pool()`](https://shikokuchuo.net/mizu/reference/mizu_current_pool.md).

## Value

A list with elements `name`, `role`, `max_workers`, `max_submitters`,
`injection_cap`, `result_slots`, `slot_size`, `workers` (per-slot
states), `parked`, `submitters` (per-slot states), `injection` (entries
queued and unclaimed), `tasks` (result slots by state: pending / ok /
err / cancel / died), `deque` (per-worker deque depths), and `shutdown`.

## Examples

``` r
p <- mizu_pool()
mizu_pool_status(p)
#> $name
#> [1] "/mizu_1ac9_aa08ef25"
#> 
#> $role
#> [1] "controller"
#> 
#> $max_workers
#> [1] 1
#> 
#> $max_submitters
#> [1] 8
#> 
#> $injection_cap
#> [1] 1024
#> 
#> $result_slots
#> [1] 4096
#> 
#> $slot_size
#> [1] 512
#> 
#> $workers
#> [1] "live"
#> 
#> $parked
#> [1] 1
#> 
#> $submitters
#> [1] "live" "free" "free" "free" "free" "free" "free" "free"
#> 
#> $injection
#> [1] 0
#> 
#> $tasks
#> pending      ok     err  cancel    died 
#>       0       0       0       0       0 
#> 
#> $deque
#> [1] 0
#> 
#> $shutdown
#> [1] FALSE
#> 
mizu_pool_stop(p)
```
