# Benchmarks

Two comparisons with mirai, timed with [bench](https://bench.r-lib.org),
using four mizu workers and four mirai daemons. Setup time is excluded
from the timings:

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
speedup <- function(b, over, base) {
  signif(as.numeric(b$median[over]) / as.numeric(b$median[base]), digits = 2L)
}
```

## A data frame round trip

The `enrich()` task takes the 3.3 MB `diamonds` data frame, adds a
column, and returns it. mirai serializes the data in each direction and
copies it through a socket. mizu stages the frame once in shared memory.
The worker reads it in place, and the enriched frame comes back the same
way:

``` r

roundtrip <- bench::mark(
  mizu = mizu_collect(mizu_submit(p, enrich(dat), dat = diamonds, enrich = enrich)),
  mirai = collect_mirai(mirai(enrich(dat), dat = diamonds, enrich = enrich))
)
roundtrip
#> # A tibble: 2 × 6
#>   expression      min   median `itr/sec` mem_alloc `gc/sec`
#>   <bch:expr> <bch:tm> <bch:tm>     <dbl> <bch:byt>    <dbl>
#> 1 mizu       498.48µs  567.6µs     1162.  483.41KB      0  
#> 2 mirai        6.55ms    7.4ms      133.    3.82MB     17.0
```

End to end, mizu is **13x** faster (median). Each timed run stages the
frame, computes the new column over all 53,940 rows, and returns the
enriched frame.

## Five thousand small tasks

Each task computes the mean and standard deviation of a sliding window
of 1,000 prices, about 10 µs of work.

- mirai sends one task per element through its dispatcher, and the
  per-task cost alone exceeds the work.
- mizu stages the map once, and the workers claim element ranges from a
  shared cursor.

The serial [`lapply()`](https://rdrr.io/r/base/lapply.html) row is the
baseline a parallel map has to beat:

``` r

winsum <- evalq(function(i) {
  w <- ggplot2::diamonds$price[i:(i + 999L)]
  c(mean = mean(w), sd = stats::sd(w))
}, baseenv())

tasks <- bench::mark(
  serial = lapply(1:5000, winsum),
  mizu = mizu_collect(mizu_map(p, 1:5000, winsum)),
  mirai = collect_mirai(mirai_map(1:5000, winsum))
)
#> Warning: Some expressions had a GC in every iteration; so filtering is
#> disabled.
tasks
#> # A tibble: 3 × 6
#>   expression      min   median `itr/sec` mem_alloc `gc/sec`
#>   <bch:expr> <bch:tm> <bch:tm>     <dbl> <bch:byt>    <dbl>
#> 1 serial       43.7ms   59.8ms     16.6     77.1MB    40.6 
#> 2 mizu         12.5ms   14.4ms     57.2    544.4KB     0   
#> 3 mirai       532.6ms  532.6ms      1.88    47.4MB     3.76
```

With four workers, mizu is **4.2x** faster than the serial loop and
**37x** faster than mirai (median).

``` r

daemons(0L)
mizu_pool_stop(p)
```

mirai is the general solution. It scales across machines, where
serialization through sockets is unavoidable. On one machine, mizu
removes that cost: data movement is an order of magnitude cheaper, and
the per-task overhead is low enough that even microsecond-scale tasks
benefit from parallelism.

## How mizu compares

mizu is built for parallelism across processes on one machine, with data
moving through shared memory instead of serialized copies. The tools
below differ in what has to run, how data moves, and where workers can
run:

| Tool | What runs | How data moves | Best for |
|----|----|----|----|
| `parallel` | Nothing beyond base R | Fork copy-on-write (Unix); socket copies from a cluster | Coarse tasks, maximum portability |
| mirai | Daemon processes | Serialized copies over nanonext sockets | Local or remote workers, HPC clusters |
| crew | A mirai-based, auto-scaled controller | Same as mirai | Bursty workloads and pipelines |
| future | A backend (multisession, multicore, mirai, …) | Serialized copies; transport per backend | Backend-agnostic parallel loops |
| callr | A fresh R process per call | Serialized copies per call | Isolated one-off computations |
| mizu | Worker processes, no dispatcher | Shared-memory views, µs handoff | Fine-grained tasks, large vectors and frames, Python interop |

### Base R

`mclapply()` forks workers on Unix. A fork is cheap because it shares
the parent’s pages copy-on-write, but it is unavailable on Windows and
composes poorly with threads, GUI sessions, and some external libraries.
`parLapply()` over a socket cluster is the portable form, and it
serializes every task and result through a socket.

### mirai, crew, and future

mirai dispatches tasks to daemon processes over nanonext sockets. Its
dispatcher runs through callbacks rather than as a separate broker
process. The socket copies are the trade-off: daemons can run anywhere a
network reaches, including other machines and HPC clusters that shared
memory cannot span. crew builds auto-scaling worker management on mirai
for bursty pipelines.

future layers a backend-agnostic interface over these and other backends
(multisession, multicore, or mirai itself via future.mirai), so code
ports between them unchanged. That abstraction costs much higher
per-task overhead than either mirai or crew, on top of the backend’s own
transport cost.

### callr

callr does something different. It evaluates R code in a fresh, isolated
session with no state carried over, paying for the isolation with a
process spawn and a serialized round trip per call. Use it for one-off
isolation, not for splitting a loop.

### Python workers

Only mizu runs a pool whose workers are Python processes, exchanging
vectors and data frames as shared-memory views (see [Python
interop](https://shikokuchuo.net/mizu/articles/interop.md)).

Use mizu when the work fits on one machine, when tasks are fine-grained
or the data is large, or when you need Python workers. Use mirai or crew
when workers must run on other machines or under a scheduler, and callr
when a task needs a fresh session.
