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
#> 1 mizu       246.49µs 378.23µs     2343.  480.61KB     29.8
#> 2 mirai        7.03ms   7.57ms      132.    3.81MB     22.8
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
#> 1 serial      17.64ms  17.64ms     56.7     30.8MB  1247.  
#> 2 mizu         4.99ms   5.62ms    144.     423.4KB     0   
#> 3 mirai      210.13ms 210.13ms      4.76      19MB     9.52
```

mizu turns the four workers into a **3.1x** gain over the serial loop,
and runs **37x** ahead of mirai (medians).

``` r

daemons(0L)
mizu_pool_stop(p)
```

mirai remains the general solution: it scales across machines, where
serialization through sockets is unavoidable. On one machine, mizu
removes that cost — an order of magnitude on data movement, and per-task
overhead low enough that even microsecond-scale tasks profit from
parallelism.
