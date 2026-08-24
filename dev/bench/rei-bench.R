# Report-only benchmark: the rei channel and pool on their own, the R
# counterpart of pyrei's benchmarks/rei-bench.py (the mirai/nanonext
# comparison rows live in dev/bench/rei-mirai.R). Prints each number as
# it lands and a summary table at the end; asserts nothing.
#
#   1. sequential round-trip  evaluate 1L, 1 worker: submit + collect loop
#   2. pipelined throughput   evaluate 1L, 1 worker: fire n, collect n;
#                             channel and pool each carry a batched row —
#                             one crossing per burst instead of per op
#   3. payload round-trip     identity task on numeric vectors of 8 KB /
#                             800 KB / 8 MB, 1 worker: data both ways.
#                             rei slots are sized to the payload where
#                             the 2^20 slot_size cap allows, so 8 KB and
#                             800 KB ride in-slot and 8 MB takes the spill
#                             tier (SHM_VEC views; on Linux the churn
#                             signal falls back to SHM_RAW reuse)
#   4. parallel fan-out       small compute tasks, 4 workers: fire all,
#                             collect all (in-process loop as the anchor)
#   5. streaming              one-way 1L messages: channel send_batch /
#                             recv_batch
#   6. parallel map           rei_map, 4 workers: trivial f per-element
#                             overhead (serial lapply as the anchor, plus
#                             rei_map's .template and .seed variants),
#                             then scenario 4's fan-out work as one map
#                             call, then a skewed-f regime exercising the
#                             self-scheduled morsel claims
#
# Timings are bench::mark medians over its auto-calibrated iteration
# counts, after warm-up, with GC time kept in (filter_gc = FALSE) — the
# streaming row's iterations double as a receive-path stress run.
#
# Run:  Rscript dev/bench/rei-bench.R

library(rei)

# helpers ----------------------------------------------------------------------

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
# median in ms. check = FALSE (reps return large or no values), memory =
# FALSE (no profmem), filter_gc = FALSE (GC is part of the workload)
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

# the two reporting shapes: f() is one rep of `ops` operations, reported
# per operation or as a rate. Keep the measured loop inline in f — one
# closure call is 0.04 us, not nothing against a 1 us row, so there is
# deliberately no helper for it
note_us <- function(scenario, framework, ops, f, unit = "us/task") {
  note(scenario, framework, mark_ms(f) * 1000 / ops, unit)
}

note_rate <- function(scenario, framework, ops, f, unit = "tasks/s") {
  note(scenario, framework, ops / mark_ms(f) * 1000, unit)
}

warmup <- function(f, n = 200L) {
  for (i in seq_len(n)) {
    f()
  }
}

# fire n, then reap n
pipeline <- function(fire, reap, n) {
  ts <- vector("list", n)
  for (i in seq_len(n)) {
    ts[[i]] <- fire()
  }
  for (i in seq_len(n)) {
    reap(ts[[i]])
  }
}

# pool up, f(pool), pool down. args reaches rei_pool, for the rows that size
# slots to the payload
with_pool <- function(workers, f, args = list()) {
  p <- do.call(rei_pool, c(list(workers, max_submitters = 2L), args))
  on.exit(rei_pool_stop(p))
  f(p)
}

with_channel <- function(expr, f, ...) {
  ch <- rei_channel(expr, ...)
  on.exit(rei_close(ch, timeout = 10))
  f(ch)
}

# the canonical echo peer: the raw-transport counterpart of a 1L task
echo_expr <- quote(
  repeat {
    x <- rei_recv(ch, timeout = 30)
    if (inherits(x, "rei_sentinel")) {
      break
    }
    rei_send(ch, x)
  }
)

cat(sprintf(
  "rei %s | R %s | %s\n",
  packageVersion("rei"),
  getRversion(),
  R.version[["platform"]]
))

# 1. sequential round-trip -----------------------------------------------------

cat("\n== 1. sequential round-trip (evaluate 1L, 1 worker) ==\n")
n <- 2000L

nc <- 20000L # channel rt is µs-scale: run long to average out jitter
with_channel(
  echo_expr,
  function(ch) {
    warmup(function() {
      rei_send(ch, 1L)
      rei_recv(ch, 30)
    })
    note_us(
      "sequential rt",
      "rei channel",
      nc,
      function() {
        for (i in seq_len(nc)) {
          rei_send(ch, 1L)
          rei_recv(ch, 30)
        }
      },
      "us/rt"
    )
  },
  capacity = 1024L
)

with_pool(1L, function(p) {
  warmup(function() rei_collect(rei_submit(p, 1L), timeout = 30))
  note_us("sequential rt", "rei pool", n, function() {
    for (i in seq_len(n)) {
      rei_collect(rei_submit(p, 1L), timeout = 30)
    }
  })
})

# 2. pipelined throughput ------------------------------------------------------

cat("\n== 2. pipelined throughput (evaluate 1L, 1 worker) ==\n")
n <- 10000L
k <- 10L # cycles per rep, again averaging out jitter

with_channel(echo_expr, function(ch) {
  # default capacity holds n
  warmup(function() {
    rei_send(ch, 1L)
    rei_recv(ch, 30)
  })
  note_rate(
    "pipelined",
    "rei channel",
    k * n,
    function() {
      for (j in seq_len(k)) {
        for (i in seq_len(n)) {
          rei_send(ch, 1L)
        }
        for (i in seq_len(n)) {
          rei_recv(ch, 30)
        }
      }
    },
    "rt/s"
  )
})

# the channel's batch pair: the peer echoes whole batches, the host sends
# and drains in 4096-chunks — one crossing per chunk each way. stream() is
# per cycle (10 calls per rep), not per message, so the helper stays
with_channel(
  quote(
    repeat {
      xs <- rei_recv_batch(ch, n = 4096L, timeout = 30)
      if (inherits(xs, "rei_sentinel")) {
        break
      }
      rei_send_batch(ch, xs)
    }
  ),
  function(ch) {
    batch <- as.list(rep(1L, 4096L))
    stream <- function(m) {
      sent <- 0L
      while (sent < m) {
        want <- min(4096L, m - sent)
        sent <- sent + rei_send_batch(ch, batch[seq_len(want)])
      }
      got <- 0L
      while (got < m) {
        xs <- rei_recv_batch(ch, 4096L, 30)
        if (inherits(xs, "rei_sentinel")) {
          stop("rei channel batch: peer stopped echoing")
        }
        got <- got + length(xs)
      }
    }
    warmup(function() stream(100L))
    note_rate(
      "pipelined",
      "rei channel batch",
      k * n,
      function() {
        for (j in seq_len(k)) {
          stream(n)
        }
      },
      "rt/s"
    )
  }
)

# a submitter's outstanding tasks are bounded by its result-slot share, so
# fire-n-then-collect needs result_slots / max_submitters >= n
with_pool(
  1L,
  function(p) {
    fire <- function() rei_submit(p, 1L)
    reap <- function(t) rei_collect(t, timeout = 30)
    warmup(function() reap(fire()))
    note_rate("pipelined", "rei pool", n, function() pipeline(fire, reap, n))
  },
  list(result_slots = 20480L)
)

# the batch pair: one submit crossing + one collect crossing per burst.
# exprs is built once outside the timed loop, as a real caller hoists it.
# k bursts per rep: one 10k burst is sub-ms and would quantize against
# any clock's resolution
with_pool(
  1L,
  function(p) {
    exprs <- as.list(rep(1L, n))
    warmup(function() {
      invisible(rei_collect_all(
        rei_submit_batch(p, exprs[seq_len(100L)]),
        timeout = 30
      ))
    })
    note_rate("pipelined", "rei pool batch", k * n, function() {
      for (j in seq_len(k)) {
        invisible(rei_collect_all(rei_submit_batch(p, exprs), timeout = 30))
      }
    })
  },
  list(result_slots = 20480L)
)

# 3. payload round-trip --------------------------------------------------------

cat("\n== 3. payload round-trip (identity task on a numeric vector) ==\n")

# slots sized to the payload where the cap allows: 8 KB and 800 KB ride
# in-slot (RAWVEC, no spill); 8 MB exceeds 2^20 and spills on default
# slots — staged SHM_VEC (one layout write, a view at receive, REF back),
# its recycling GC-gated, so this sequential loop churns a fresh region
# per payload; on Linux the churn signal (payload.c) falls back to
# SHM_RAW's deterministic reuse instead
payloads <- list(
  list(size = 1e3, n = 1000L, args = list(slot_size = 16384L)),
  list(
    size = 1e5,
    n = 200L,
    args = list(
      injection_cap = 16L,
      per_worker_cap = 16L,
      result_slots = 4L,
      slot_size = 1048576L
    )
  ),
  list(size = 1e6, n = 30L, args = list())
)

payload_label <- function(size) {
  sprintf("payload %s B", formatC(8 * size, format = "d", big.mark = ","))
}

for (pl in payloads) {
  x <- runif(pl[["size"]])
  n <- pl[["n"]]
  with_pool(
    1L,
    function(p) {
      if (!identical(rei_collect(rei_submit(p, x, x = x), timeout = 30), x)) {
        stop("pool roundtrip mismatch")
      }
      note_us(payload_label(pl[["size"]]), "rei pool", n, function() {
        for (i in seq_len(n)) {
          rei_collect(rei_submit(p, x, x = x), timeout = 30)
        }
      })
    },
    pl[["args"]]
  )
}

# 4. parallel fan-out ----------------------------------------------------------

cat("\n== 4. parallel fan-out (sum(runif(1e4)) x 2000, 4 workers) ==\n")
n <- 2000L

note_rate("fan-out", "in-process", n, function() {
  for (i in seq_len(n)) {
    sum(runif(1e4))
  }
})

with_pool(4L, function(p) {
  # 2048 default slots for us
  fire <- function() rei_submit(p, sum(runif(1e4)))
  reap <- function(t) rei_collect(t, timeout = 30)
  pipeline(fire, reap, n)
  note_rate("fan-out", "rei pool", n, function() pipeline(fire, reap, n))
})

# 5. streaming -----------------------------------------------------------------

cat("\n== 5. streaming (one-way 1L messages, batched) ==\n")
n <- 200000L
k <- 10L # rounds per rep: one round is short enough to jitter

# the peer counts arrivals and sends one receipt per n, so the same channel
# serves the warm-up round and every rep
with_channel(
  quote({
    total <- 0L
    repeat {
      xs <- rei_recv_batch(ch, n = 4096L, timeout = 30)
      if (inherits(xs, "rei_sentinel")) {
        break
      }
      total <- total + length(xs)
      if (total >= 200000L) {
        rei_send(ch, total)
        total <- 0L
      }
    }
  }),
  function(ch) {
    batch <- as.list(rep(1L, 4096L))
    stream_round <- function() {
      sent <- 0L
      while (sent < n) {
        want <- min(4096L, n - sent)
        sent <- sent + rei_send_batch(ch, batch[seq_len(want)])
      }
      if (!identical(rei_recv(ch, 60), n)) stop("stream count mismatch")
    }
    stream_round()
    note_rate(
      "streaming",
      "rei channel",
      k * n,
      function() {
        for (j in seq_len(k)) {
          stream_round()
        }
      },
      "msg/s"
    )
  }
)

# 6. parallel map ---------------------------------------------------------------

cat("\n== 6. parallel map (f over n elements, 4 workers) ==\n")

# overhead regime: trivial f, where per-element cost is the whole story
n <- 10000L
x <- runif(n)
f <- function(v) v + 1

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

with_pool(4L, function(p) {
  invisible(rei_map(p, x, f))
  note_us(
    "map trivial f",
    "rei_map",
    k * n,
    function() {
      for (j in seq_len(k)) {
        rei_map(p, x, f)
      }
    },
    "us/elt"
  )
  note_us(
    "map trivial f",
    "rei_map template",
    k * n,
    function() {
      for (j in seq_len(k)) {
        rei_map(p, x, f, .template = numeric(1))
      }
    },
    "us/elt"
  )
  # per-element L'Ecuyer-CMRG streams: the price of reproducibility
  note_us(
    "map trivial f",
    "rei_map .seed",
    k * n,
    function() {
      for (j in seq_len(k)) {
        rei_map(p, x, f, .seed = 42L)
      }
    },
    "us/elt"
  )
  # prepared: stage once, run many — per-run cost is submit + collect,
  # and back-to-back runs hit the workers' cached map contexts
  pmap <- rei_map_prepare(p, x, f)
  invisible(rei_map_run(pmap))
  note_us(
    "map trivial f",
    "rei_map_run prepared",
    k * n,
    function() {
      for (j in seq_len(k)) {
        rei_map_run(pmap)
      }
    },
    "us/elt"
  )
})

# compute regime: scenario 4's fan-out work as a single map call — the
# per-element overhead above amortized against real tasks
n <- 2000L
g <- function(i) sum(runif(1e4))

with_pool(4L, function(p) {
  invisible(rei_map(p, seq_len(n), g))
  note_rate(
    "map fan-out",
    "rei_map",
    n,
    function() rei_map(p, seq_len(n), g),
    "elts/s"
  )
})

# skew regime: 1% of elements cost ~100x the rest, clustered at the head —
# fine self-scheduled claims keep the workers level where a coarse static
# split concentrates the heavy heads on one worker
n <- 4000L
xs <- seq_len(n) + 0
h <- function(i) sum(runif(if (i <= 40) 2e5 else 200L))

with_pool(4L, function(p) {
  invisible(rei_map(p, xs, h))
  note(
    "map skewed f",
    "rei_map",
    mark_ms(function() rei_map(p, xs, h)),
    "ms wall"
  )
})

# summary ----------------------------------------------------------------------

cat("\n== summary ==\n")
df <- do.call(rbind, results)
df <- df[order(match(df[["scenario"]], unique(df[["scenario"]]))), ]
cat(
  sprintf(
    "  %-20s %-20s %12s %s\n",
    df[["scenario"]],
    df[["framework"]],
    formatC(df[["value"]], format = "f", digits = 1, big.mark = ","),
    df[["unit"]]
  ),
  sep = ""
)
