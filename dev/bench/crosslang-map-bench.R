# Report-only benchmark: the cross-language map (Phase 5) — mizu_map with
# a mizu_call() spec over a Python worker pool, against the same-language
# map and mirai_map, matched regime-for-regime with dev/bench/
# mizu-mirai-bench.R's scenario 6. Prints each number as it lands and a
# summary table at the end; asserts nothing. Exits with a message when
# python3 with pymizu and NumPy is not on PATH.
#
#   1. overhead regime   trivial f over 10k doubles, 4 workers: serial
#                        lapply (the anchor), mirai_map (the
#                        task-per-element model's per-element cost),
#                        mizu_map on R workers, and mizu_map with a spec
#                        on Python workers — the interop descriptor,
#                        kind-2 runner tasks, and interop result values —
#                        with its template, seed, and prepared variants
#   2. compute regime    ~10 us of work per element as one map call
#                        (n = 2000), native against spec
#
# Timings are bench::mark medians. Run:
#
#   Rscript dev/bench/crosslang-map-bench.R

py <- Sys.which("python3")
if (
  !nzchar(py) ||
    system2(
      py,
      c("-c", shQuote("import pymizu, numpy")),
      stdout = FALSE,
      stderr = FALSE
    ) !=
      0L
) {
  message("python3 with pymizu and numpy not available: skipping")
  quit("no")
}

library(mizu)
library(mirai)

results <- list()

note <- function(scenario, framework, value, unit) {
  cat(sprintf(
    "  %-20s %12s %s\n",
    framework,
    formatC(value, format = "f", digits = 1, big.mark = ","),
    unit
  ))
  results[[length(results) + 1L]] <<-
    data.frame(
      scenario = scenario,
      framework = framework,
      value = value,
      unit = unit
    )
}

# one rep of f() timed by bench::mark: auto-calibrated iterations, the
# median in ms
mark_ms <- function(f) {
  bm <- bench::mark(
    f(),
    min_time = 0.5,
    check = FALSE,
    memory = FALSE,
    filter_gc = FALSE
  )
  as.numeric(bm[["median"]]) * 1000
}

note_us <- function(scenario, framework, ops, f, unit = "us/task") {
  note(scenario, framework, mark_ms(f) * 1000 / ops, unit)
}

with_pool <- function(workers, f, launcher = NULL) {
  p <- if (is.null(launcher)) {
    mizu_pool(workers, max_submitters = 2L)
  } else {
    mizu_pool(workers, max_submitters = 2L, launcher = launcher)
  }
  on.exit(mizu_pool_stop(p), add = TRUE)
  f(p)
}

# 1. overhead regime: trivial f, where per-element cost is the whole story

cat("\n== 1. cross-language map overhead (trivial f, n = 10000, 4 workers) ==\n")

n <- 10000L
x <- runif(n)
f <- function(v) v + 1
spec <- mizu_call("numpy.absolute")

k <- 10L # map calls per rep: a trivial map is sub-ms, so loop to average
note_us(
  "map trivial f",
  "serial lapply",
  k * n,
  function() {
    for (j in seq_len(k)) {
      lapply(x, f)
    }
  },
  "us/elt"
)

daemons(4L, dispatcher = FALSE)
on.exit(daemons(0L), add = TRUE)
invisible(mirai_map(x, f))
note_us(
  "map trivial f",
  "mirai_map",
  k * n,
  function() {
    for (j in seq_len(k)) {
      mirai_map(x, f)
    }
  },
  "us/elt"
)
daemons(0L)

with_pool(4L, function(p) {
  invisible(mizu_map(p, x, f))
  note_us(
    "map trivial f",
    "mizu_map",
    k * n,
    function() {
      for (j in seq_len(k)) {
        mizu_map(p, x, f)
      }
    },
    "us/elt"
  )
})

with_pool(4L, function(p) {
  invisible(mizu_map(p, x, spec))
  note_us(
    "map trivial f",
    "mizu_map spec",
    k * n,
    function() {
      for (j in seq_len(k)) {
        mizu_map(p, x, spec)
      }
    },
    "us/elt"
  )
  # the template path: results land in the region's output area, no
  # per-element result framing at all
  note_us(
    "map trivial f",
    "mizu_map spec template",
    k * n,
    function() {
      for (j in seq_len(k)) {
        mizu_map(p, x, spec, .template = numeric(1))
      }
    },
    "us/elt"
  )
  # the neutral (seed, offset) pair; per-element streams derived
  # worker-side (SHA-256 on the Python side)
  note_us(
    "map trivial f",
    "mizu_map spec .seed",
    k * n,
    function() {
      for (j in seq_len(k)) {
        mizu_map(p, x, mizu_call(.source = "x"), .seed = 42L)
      }
    },
    "us/elt"
  )
  # prepared: stage once, run many — per-run cost is the kind-2 runner
  # submit + collect, the workers' contexts cached
  pmap <- mizu_map_prepare(p, x, spec)
  invisible(mizu_map_run(pmap))
  note_us(
    "map trivial f",
    "mizu_map_run spec",
    k * n,
    function() {
      for (j in seq_len(k)) {
        mizu_map_run(pmap)
      }
    },
    "us/elt"
  )
}, launcher = mizu_py_pool_launcher())

# 2. compute regime: ~10 us of work per element as one map call

cat("\n== 2. cross-language map compute (~10 us elements, n = 2000) ==\n")

n <- 2000L
g <- function(i) sum(runif(2e3))
gspec <- mizu_call(.source = "import numpy as np\nnp.random.random(2000).sum()")

note(
  "map ~10us tasks",
  "serial lapply",
  mark_ms(function() invisible(lapply(seq_len(n), g))),
  "ms wall"
)

with_pool(4L, function(p) {
  invisible(mizu_map(p, seq_len(n), g))
  note(
    "map ~10us tasks",
    "mizu_map",
    mark_ms(function() invisible(mizu_map(p, seq_len(n), g))),
    "ms wall"
  )
})

with_pool(4L, function(p) {
  invisible(mizu_map(p, seq_len(n), gspec))
  note(
    "map ~10us tasks",
    "mizu_map spec",
    mark_ms(function() invisible(mizu_map(p, seq_len(n), gspec))),
    "ms wall"
  )
}, launcher = mizu_py_pool_launcher())

cat("\n== summary ==\n")
print(do.call(rbind, results), row.names = FALSE)
