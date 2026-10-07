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
#> 1 mizu        263.1µs  406.8µs     2107.  480.61KB     27.7
#> 2 mirai         7.5ms   7.96ms      124.    3.81MB     19.9
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
#> 1 serial      20.07ms  20.07ms     49.8     30.8MB  1047.  
#> 2 mizu         4.94ms   5.61ms    151.     432.5KB     0   
#> 3 mirai       202.7ms  202.7ms      4.93      19MB     9.87
```

mizu turns the four workers into a **3.6x** gain over the serial loop,
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

## How mizu compares

mizu occupies a specific point in the design space: parallelism across
processes on a single machine, with data moving through shared memory
instead of serialized copies. The tools below overlap at the edges; the
differences are in what has to run, how data moves, and where workers
can live.

| Tool | What runs | How data moves | Best for |
|----|----|----|----|
| `parallel` | Nothing beyond base R | Fork copy-on-write (Unix); socket copies from a cluster | Coarse tasks, maximum portability |
| mirai | Daemon processes | Serialized copies over nanonext sockets | Local or remote workers, HPC clusters |
| crew | A mirai-based, auto-scaled controller | Same as mirai | Bursty workloads and pipelines |
| future | A backend (multisession, multicore, mirai, …) | Serialized copies; transport per backend | Backend-agnostic parallel loops |
| callr | A fresh R process per call | Serialized copies per call | Isolated one-off computations |
| mizu | Nothing; a package | Shared-memory views, µs handoff | Fine-grained tasks, large vectors and frames, Python interop |

### Base R

`mclapply()` forks workers on Unix — cheap there, because a fork shares
the parent’s pages copy-on-write — but fork is unavailable on Windows
and composes poorly with threads, GUI sessions, and some external
libraries; `parLapply()` over a socket cluster is the portable form and
serializes every task and result through a socket.

### mirai, crew, and future

mirai dispatches tasks to daemon processes over nanonext sockets, its
dispatcher working through callbacks rather than as a broker process —
socket copies are the price of its generality: daemons can run anywhere
a network reaches, including other machines and HPC clusters, which
shared memory cannot. crew builds auto-scaling worker management on
mirai for bursty pipelines.

future layers a backend-agnostic interface over these and other backends
(multisession, multicore, or mirai itself via future.mirai), so code
ports between them unchanged; the cost of that abstraction is much
higher per-task overhead than either mirai or crew, ahead of the
backend’s own transport.

### callr

callr answers a different question — evaluating R code in a fresh,
isolated session with no carry-over of state — and pays for the
isolation with a process spawn and a serialized round trip per call. It
is the right tool for one-off isolation, not for dividing a loop.

### The one thing only mizu does

A pool whose workers are Python processes, exchanging vectors and data
frames as shared-memory views ([Python
interop](https://shikokuchuo.net/mizu/articles/interop.md)). None of the
tools above cross the language boundary.

The short version: choose mizu when the work fits one machine, when
tasks are fine-grained or vectors and frames large, or when Python needs
to be in the loop; choose mirai or crew when workers must run on other
machines or under a scheduler, and callr when a task needs a pristine
session.
