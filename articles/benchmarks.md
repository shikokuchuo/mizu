# Benchmarks

Two head-to-head measurements against mirai, timed with
[bench](https://bench.r-lib.org). Four mizu workers against four mirai
daemons. Setup is excluded from the timings:

``` r

library(mizu)
library(mirai)

daemons(4L)
p <- mizu_pool(n_workers = 4L)
diamonds <- ggplot2::diamonds

# The task function.
enrich <- evalq(
  function(dat) {
    dat$price_per_carat <- dat$price / dat$carat
    dat
  },
  baseenv()
)

# Speedup multiple of one bench row over another, from the medians
speedup <- function(b, over, base) signif(as.numeric(b$median[over]) / as.numeric(b$median[base]), 2L)
```

**A dataset across the wire, both ways.** The `enrich()` task receives
the 3.3 MB `diamonds` data frame, adds a column, and returns it. mirai
serializes the data once per direction and copies it through a socket.
mizu stages the frame once in shared memory; the worker reads it in
place, and the enriched frame crosses back the same way:

``` r

roundtrip <- bench::mark(
  mizu = mizu_collect(mizu_submit(p, enrich(dat), dat = diamonds, enrich = enrich)),
  mirai = collect_mirai(mirai(enrich(dat), dat = diamonds, enrich = enrich))
)
roundtrip
#> # A tibble: 2 × 6
#>   expression      min   median `itr/sec` mem_alloc `gc/sec`
#>   <bch:expr> <bch:tm> <bch:tm>     <dbl> <bch:byt>    <dbl>
#> 1 mizu       258.42µs 394.46µs     2198.  480.61KB     27.7
#> 2 mirai        7.31ms   7.92ms      125.    3.81MB     21.3
```

End to end, mizu is **20x** faster: every timed run stages the frame,
runs the task — computing the new column over all 53,940 rows — and
returns the enriched frame (medians).

**Two thousand small tasks.** Each task summarizes a sliding window of
1,000 prices — mean and standard deviation, about 10µs of work apiece.
mirai sends one task per element through its dispatcher, whose per-task
cost alone exceeds the task. mizu stages the map once, and the workers
self-schedule element ranges off a shared cursor. The serial
[`lapply()`](https://rdrr.io/r/base/lapply.html) row is the baseline a
parallel map must beat:

``` r

winsum <- evalq(function(i) {
  w <- ggplot2::diamonds$price[i:(i + 999L)]
  c(mean = mean(w), sd = stats::sd(w))
}, baseenv())

tasks <- bench::mark(
  serial = lapply(1:2000, winsum),
  mizu = mizu_map(p, 1:2000, winsum),
  mirai = collect_mirai(mirai_map(1:2000, winsum))
)
tasks
#> # A tibble: 3 × 6
#>   expression      min   median `itr/sec` mem_alloc `gc/sec`
#>   <bch:expr> <bch:tm> <bch:tm>     <dbl> <bch:byt>    <dbl>
#> 1 serial      21.52ms   21.5ms     46.5     30.9MB   976.  
#> 2 mizu         5.08ms    5.7ms    138.     424.6KB     1.91
#> 3 mirai      204.99ms  205.4ms      4.87      19MB     2.43
```

mizu turns the four workers into a **3.8x** gain over the serial loop,
and runs **36x** ahead of mirai (medians).

``` r

daemons(0L)
mizu_pool_stop(p)
```

mirai remains the general solution: it scales across machines, where
serialization through sockets is unavoidable. On one machine, mizu
removes that cost — an order of magnitude on data movement, and per-task
overhead low enough that even microsecond-scale tasks profit from
parallelism.
