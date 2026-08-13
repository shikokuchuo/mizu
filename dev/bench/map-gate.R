# kio_map A/B regression gate: save a baseline on the installed reference
# build, check the installed candidate against it. Prints per-row deltas
# and exits nonzero on any regression beyond the noise tolerance. Run
# manually on a quiet machine; the numbers land in the PR description.
# Never CI — runner timing is too variable to assert (the repo's
# report-only rule).
#
#   Rscript dev/bench/map-gate.R --save  base.rds   # on installed main
#   Rscript dev/bench/map-gate.R --check base.rds   # on installed branch
#
# Every value is lower-is-better. Gate classes: "tol" rows fail the exit
# code beyond TOL plus a per-row absolute slack — the latency rows'
# run-to-run noise is a chunk-phase effect (up to one in-flight chunk's
# remaining time, milliseconds), which a bare 5% cannot absorb; "record" rows
# print but never gate — row 13 (collect-tail exposure) is a knowingly
# traded band that gates the value-quota lease follow-up, not the merge,
# and row 7's spill churn is a below-cliff regression check read alongside
# it. Rows 6, 8 and 9 are the morsel plan's headline wins: the merge
# criterion reads them strictly better from the printed table (decisively
# at the large-n variants); row 12 is parity by design — the trim is a
# regression guard, not a win.
#
# Fixed geometry: W = 4 workers, default knobs, fixed seeds.

library(kioto)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L || !args[[1L]] %in% c("--save", "--check")) {
  stop(
    "usage: Rscript dev/bench/map-gate.R --save|--check <base.rds>",
    call. = FALSE
  )
}
mode <- substring(args[[1L]], 3L)
path <- args[[2L]]

TOL <- 0.05
REPS <- 5L

now <- function() as.numeric(Sys.time())
best <- function(f) min(vapply(seq_len(REPS), function(i) f(), 0))

rows <- list()
row <- function(id, desc, value, unit, gate = "tol", slack = 0) {
  cat(sprintf("  %2d  %-34s %14.6g %s\n", id, desc, value, unit))
  rows[[length(rows) + 1L]] <<-
    data.frame(
      id = id,
      desc = desc,
      value = value,
      unit = unit,
      gate = gate,
      slack = slack
    )
}

p <- kio_pool(n_workers = 4L)
set.seed(1)

trivial <- function(i) i + 0

# x vectors are materialized (never ALTREP): a compact 1:n — and even a
# deferred as.numeric(1:n) — serializes to a few dozen bytes, so a huge-n
# map with a tiny f would pass the region-less blob probe and measure
# chunk scheduling instead of the region path these rows target. The + 0
# forces a genuine REALSXP. Row 4 keeps a compact x deliberately — it
# measures the blob path.
xelts <- function(n) seq_len(n) + 0

# warm the pool, the worker map caches, and the spill free lists
invisible(kio_map(p, xelts(1000), trivial))
invisible(kio_map(p, xelts(64), function(i) rep(as.raw(1L), 8192)))

# 1-3: trivial-f overhead, us/element -----------------------------------------
# Measured reality (2026-08-04, M4 Pro, constants frozen): row 2 is at
# parity, rows 1 and 3 carry ~+10-16% — the single-publish trade: chunks
# published progressively, so the collector's consume (unserialize +
# splice of n list elements) pipelined under compute, while runners
# publish at exhaustion and the whole consume lands after it. Trivial-f
# generic maps are consume-bound, so the lost overlap is the whole delta;
# .template escapes it entirely (row 2). The band is structural: the
# value-quota lease (backlog entry 8) is byte-denominated and never trips
# on trivial results — it covers row 13's shape, not this one.
n1 <- 10000L
row(
  1,
  "trivial f, generic",
  1e6 /
    n1 *
    best(function() {
      t0 <- now()
      kio_map(p, xelts(n1), trivial)
      now() - t0
    }),
  "us/elt"
)
row(
  2,
  "trivial f, template",
  1e6 /
    n1 *
    best(function() {
      t0 <- now()
      kio_map(p, xelts(n1), trivial, .template = numeric(1))
      now() - t0
    }),
  "us/elt"
)
row(
  3,
  "trivial f, .seed",
  1e6 /
    n1 *
    best(function() {
      t0 <- now()
      kio_map(p, xelts(n1), function(i) runif(1), .seed = 42L)
      now() - t0
    }),
  "us/elt"
)

# 4: region-less blob path (unchanged by design) ------------------------------
row(
  4,
  "blob map, per call",
  1e6 /
    100 *
    best(function() {
      t0 <- now()
      for (i in 1:100) {
        kio_map(p, 1:8, trivial)
      }
      now() - t0
    }),
  "us/map"
)

# 5: fan-out compute as one map -----------------------------------------------
row(
  5,
  "fan-out 2000 x sum(runif(1e4))",
  best(function() {
    t0 <- now()
    kio_map(p, xelts(2000), function(i) sum(runif(1e4)))
    now() - t0
  }),
  "s"
)

# 6: skewed f, lumpy shape (headline win: strictly better under morsels) ------
# the heavy 1% is clustered at the head, so contiguous chunks concentrate
# it on one worker today while fine self-scheduled claims balance it
f6 <- function(i) sum(runif(if (i <= 40L) 2e5 else 200L))
row(
  6,
  "skewed f, heavy 1% clustered",
  best(function() {
    t0 <- now()
    kio_map(p, xelts(4000), f6)
    now() - t0
  }),
  "s"
)

# 7: big generic results: spill events and free-list reuse --------------------
f7 <- function(i) rep(as.raw(1L), 8192)
invisible(kio_map(p, xelts(256), f7)) # warm the free list at shape
s0 <- kio_pool_stats(p)[["submitters"]]
invisible(kio_map(p, xelts(256), f7))
s1 <- kio_pool_stats(p)[["submitters"]]
spills <- sum(s1[["spills"]]) - sum(s0[["spills"]])
reuse <- sum(s1[["spill_reuse"]]) - sum(s0[["spill_reuse"]])
row(
  7,
  "8KB results: spill churn",
  spills - reuse,
  sprintf("regions (of %d spills)", spills),
  gate = "record"
)

# 8: mixed-workload latency: foreign pickup mid-map ---------------------------
lat_mid <- function(n, settle) {
  best(function() {
    st <- kioto:::map_stage(p, xelts(n), trivial, list())
    kioto:::map_submit(p, st)
    Sys.sleep(settle) # let workers sink into the map
    t0 <- now()
    kio_collect(kio_submit(p, NULL))
    lat <- now() - t0
    kioto:::map_collect(p, st)
    lat
  })
}
row(
  8,
  "foreign pickup mid-map, n=1e4",
  lat_mid(1e4, 0.001) * 1e3,
  "ms",
  slack = 1
)
row(
  8,
  "foreign pickup mid-map, n=1e6",
  lat_mid(1e6, 0.005) * 1e3,
  "ms",
  slack = 5
)

# 9: cancellation latency: .timeout expiry mid-map ----------------------------
lat_cancel <- function(n, timeout) {
  best(function() {
    r <- kio_map(p, xelts(n), trivial, .timeout = timeout)
    # the map must outlive it
    if (!inherits(r, "kio_timeout")) {
      stop("map did not time out")
    }
    t0 <- now()
    kio_collect(kio_submit(p, NULL))
    now() - t0
  })
}
row(
  9,
  "post-timeout pickup, n=1e5",
  lat_cancel(1e5, 0.02) * 1e3,
  "ms",
  slack = 1
)
row(
  9,
  "post-timeout pickup, n=1e6",
  lat_cancel(1e6, 0.05) * 1e3,
  "ms",
  slack = 5
)

# 10: nested map (fork/join: help-mode and deque interactions) ----------------
# nested maps ride the task expression, where the evaluating worker's own
# handle binds as `pool`
row(
  10,
  "nested map 4 x 1000",
  best(function() {
    t0 <- now()
    hs <- lapply(1:4, function(i) {
      kio_submit(p, sum(unlist(kio_map(pool, 1:1000 + 0, function(j) j + 0))))
    })
    for (h in hs) {
      kio_collect(h)
    }
    now() - t0
  }),
  "s"
)

# 11: abrupt cost switch: the ramp worst case ---------------------------------
# f flips from trivial to ~10 ms/element at the midpoint; the timeout
# expires once every worker is inside heavy elements, and the latency to
# the next tiny task bounds batch cap x morsel size x heavy-element cost
f11 <- function(i) {
  if (i > 256L) {
    Sys.sleep(0.01)
  }
  i
}
row(
  11,
  "post-timeout pickup after cost jump",
  best(function() {
    r <- kio_map(p, xelts(512), f11, .timeout = 0.5)
    if (!inherits(r, "kio_timeout")) {
      stop("map did not time out")
    }
    t0 <- now()
    kio_collect(kio_submit(p, NULL))
    now() - t0
  }) *
    1e3,
  "ms",
  slack = 10
)

# 12: busy-peer completion (parity guard: the exhausted-runner trim) ----------
row(
  12,
  "short map, half the pool pinned",
  best(function() {
    pins <- list(kio_submit(p, Sys.sleep(0.6)), kio_submit(p, Sys.sleep(0.6)))
    Sys.sleep(0.05) # both sleepers claimed
    t0 <- now()
    kio_map(p, xelts(2000), trivial)
    lat <- now() - t0
    for (h in pins) {
      kio_collect(h)
    }
    lat
  }),
  "s",
  slack = 0.002
)

# 13: collect-tail exposure (recorded, never gated) ---------------------------
f13 <- function(i) {
  s <- sum(runif(2e4))
  rep(as.raw(1L), 8192)
}
row(
  13,
  "medium f, 8KB generic results",
  best(function() {
    t0 <- now()
    kio_map(p, xelts(2000), f13)
    now() - t0
  }),
  "s",
  gate = "record"
)

kio_pool_stop(p)
tab <- do.call(rbind, rows)

if (mode == "save") {
  saveRDS(tab, path)
  cat(sprintf("\nbaseline saved to %s (%d rows)\n", path, nrow(tab)))
} else {
  base <- readRDS(path)
  if (!identical(base[["desc"]], tab[["desc"]])) {
    stop("baseline descriptions differ")
  }
  delta <- (tab[["value"]] - base[["value"]]) / ifelse(base[["value"]] == 0, 1, base[["value"]])
  over <- tab[["value"]] - (base[["value"]] * (1 + TOL) + tab[["slack"]])
  fail <- tab[["gate"]] == "tol" & over > 0
  cat(
    "\n  id  row                                    base      branch",
    "  delta\n"
  )
  for (i in seq_len(nrow(tab))) {
    cat(sprintf(
      "  %2d  %-34s %10.4g  %10.4g  %+6.1f%%%s\n",
      tab[["id"]][i],
      tab[["desc"]][i],
      base[["value"]][i],
      tab[["value"]][i],
      delta[i] * 100,
      if (fail[i]) {
        "  REGRESSION"
      } else if (tab[["gate"]][i] == "record") {
        "  (recorded)"
      } else {
        ""
      }
    ))
  }
  if (any(fail)) {
    cat(sprintf("\n%d row(s) regressed beyond %.0f%%\n", sum(fail), TOL * 100))
    quit(status = 1L)
  }
  cat("\ngate passed\n")
}
