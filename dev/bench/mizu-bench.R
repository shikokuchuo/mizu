# Report-only benchmark: the mizu channel and pool on their own, the R
# counterpart of pymizu's benchmarks/mizu-bench.py (the mirai/nanonext
# comparison rows live in dev/bench/mizu-mirai-bench.R). Prints each number as
# it lands and a summary table at the end; asserts nothing.
#
#   1. sequential round-trip  evaluate 1L, 1 worker: submit + collect loop
#   2. pipelined throughput   evaluate 1L, 1 worker: fire n, collect n;
#                             channel and pool each carry a batched row —
#                             one crossing per burst instead of per op
#   3. payload round-trip     identity task on numeric vectors of 8 KB /
#                             800 KB / 8 MB, 1 worker: data both ways.
#                             mizu slots are sized to the payload where
#                             the 2^20 slot_size cap allows, so 8 KB and
#                             800 KB ride in-slot and 8 MB takes the spill
#                             tier (SHM_VEC views; on Linux the churn
#                             signal falls back to SHM_RAW reuse)
#   4. parallel fan-out       ~10 us compute tasks, 4 workers: fire all,
#                             collect all (in-process loop as the anchor);
#                             scenario 6 maps the same work in one call
#   5. streaming              one-way 1L messages: channel send_batch /
#                             recv_batch
#   6. parallel map           mizu_map, 4 workers: trivial f per-element
#                             overhead (serial lapply as the anchor, plus
#                             mizu_map's .template and .seed variants),
#                             then ~10 us tasks as one map call (the README
#                             table's last row), then a skewed-f regime
#                             exercising the self-scheduled morsel claims
#
# Timings are bench::mark medians over its auto-calibrated iteration
# counts, after warm-up, with GC time kept in (filter_gc = FALSE) — the
# streaming row's iterations double as a receive-path stress run.
#
# Run:  Rscript dev/bench/mizu-bench.R

library(mizu)

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

# pool up, f(pool), pool down. args reaches mizu_pool, for the rows that size
# slots to the payload
with_pool <- function(workers, f, args = list()) {
  p <- do.call(mizu_pool, c(list(workers, max_submitters = 2L), args))
  on.exit(mizu_pool_stop(p))
  f(p)
}

with_channel <- function(expr, f, ...) {
  ch <- mizu_channel(expr, ...)
  on.exit(mizu_close(ch, timeout = 10))
  f(ch)
}

# the canonical echo peer: the raw-transport counterpart of a 1L task
echo_expr <- quote(
  repeat {
    x <- mizu_recv(ch, timeout = 30)
    if (inherits(x, "mizu_sentinel")) {
      break
    }
    mizu_send(ch, x)
  }
)

cat(sprintf(
  "mizu %s | R %s | %s\n",
  packageVersion("mizu"),
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
      mizu_send(ch, 1L)
      mizu_recv(ch, 30)
    })
    note_us(
      "sequential rt",
      "mizu channel",
      nc,
      function() {
        for (i in seq_len(nc)) {
          mizu_send(ch, 1L)
          mizu_recv(ch, 30)
        }
      },
      "us/rt"
    )
  },
  capacity = 1024L
)

np <- 20000L # pool rt is as µs-scale as the channel's: same whole-ms tick,
# same remedy
with_pool(1L, function(p) {
  warmup(function() mizu_collect(mizu_submit(p, 1L), timeout = 30))
  note_us("sequential rt", "mizu pool", np, function() {
    for (i in seq_len(np)) {
      mizu_collect(mizu_submit(p, 1L), timeout = 30)
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
    mizu_send(ch, 1L)
    mizu_recv(ch, 30)
  })
  note_rate(
    "pipelined",
    "mizu channel",
    k * n,
    function() {
      for (j in seq_len(k)) {
        for (i in seq_len(n)) {
          mizu_send(ch, 1L)
        }
        for (i in seq_len(n)) {
          mizu_recv(ch, 30)
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
      xs <- mizu_recv_batch(ch, n = 4096L, timeout = 30)
      if (inherits(xs, "mizu_sentinel")) {
        break
      }
      mizu_send_batch(ch, xs)
    }
  ),
  function(ch) {
    batch <- as.list(rep(1L, 4096L))
    stream <- function(m) {
      sent <- 0L
      while (sent < m) {
        want <- min(4096L, m - sent)
        sent <- sent + mizu_send_batch(ch, batch[seq_len(want)])
      }
      got <- 0L
      while (got < m) {
        xs <- mizu_recv_batch(ch, 4096L, 30)
        if (inherits(xs, "mizu_sentinel")) {
          stop("mizu channel batch: peer stopped echoing")
        }
        got <- got + length(xs)
      }
    }
    warmup(function() stream(100L))
    note_rate(
      "pipelined",
      "mizu channel batch",
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
    fire <- function() mizu_submit(p, 1L)
    reap <- function(t) mizu_collect(t, timeout = 30)
    warmup(function() reap(fire()))
    note_rate("pipelined", "mizu pool", n, function() pipeline(fire, reap, n))
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
      invisible(mizu_collect_all(
        mizu_submit_batch(p, exprs[seq_len(100L)]),
        timeout = 30
      ))
    })
    note_rate("pipelined", "mizu pool batch", k * n, function() {
      for (j in seq_len(k)) {
        invisible(mizu_collect_all(mizu_submit_batch(p, exprs), timeout = 30))
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
      if (!identical(mizu_collect(mizu_submit(p, x, x = x), timeout = 30), x)) {
        stop("pool roundtrip mismatch")
      }
      note_us(payload_label(pl[["size"]]), "mizu pool", n, function() {
        for (i in seq_len(n)) {
          mizu_collect(mizu_submit(p, x, x = x), timeout = 30)
        }
      })
    },
    pl[["args"]]
  )
}

# 4. parallel fan-out ----------------------------------------------------------

cat("\n== 4. parallel fan-out (sum(runif(2e3)) x 2000, 4 workers) ==\n")
n <- 2000L

note_rate("fan-out", "in-process", n, function() {
  for (i in seq_len(n)) {
    sum(runif(2e3))
  }
})

with_pool(4L, function(p) {
  # 2048 default slots for us
  fire <- function() mizu_submit(p, sum(runif(2e3)))
  reap <- function(t) mizu_collect(t, timeout = 30)
  pipeline(fire, reap, n)
  note_rate("fan-out", "mizu pool", n, function() pipeline(fire, reap, n))
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
      xs <- mizu_recv_batch(ch, n = 4096L, timeout = 30)
      if (inherits(xs, "mizu_sentinel")) {
        break
      }
      total <- total + length(xs)
      if (total >= 200000L) {
        mizu_send(ch, total)
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
        sent <- sent + mizu_send_batch(ch, batch[seq_len(want)])
      }
      if (!identical(mizu_recv(ch, 60), n)) stop("stream count mismatch")
    }
    stream_round()
    note_rate(
      "streaming",
      "mizu channel",
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
  note_us(
    "map trivial f",
    "mizu_map template",
    k * n,
    function() {
      for (j in seq_len(k)) {
        mizu_map(p, x, f, .template = numeric(1))
      }
    },
    "us/elt"
  )
  # per-element L'Ecuyer-CMRG streams: the price of reproducibility
  note_us(
    "map trivial f",
    "mizu_map .seed",
    k * n,
    function() {
      for (j in seq_len(k)) {
        mizu_map(p, x, f, .seed = 42L)
      }
    },
    "us/elt"
  )
  # prepared: stage once, run many — per-run cost is submit + collect,
  # and back-to-back runs hit the workers' cached map contexts
  pmap <- mizu_map_prepare(p, x, f)
  invisible(mizu_map_run(pmap))
  note_us(
    "map trivial f",
    "mizu_map_run prepared",
    k * n,
    function() {
      for (j in seq_len(k)) {
        mizu_map_run(pmap)
      }
    },
    "us/elt"
  )
})

# compute regime: scenario 4's task work as a single map call —
# sum(runif(2e3)), ~10 us (calibrated 11.5 on the M4 Pro); the README
# table's last row
n <- 2000L
g <- function(i) sum(runif(2e3))

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

# skew regime: 1% of elements cost ~100x the rest, clustered at the head —
# fine self-scheduled claims keep the workers level where a coarse static
# split concentrates the heavy heads on one worker
n <- 4000L
xs <- seq_len(n) + 0
h <- function(i) sum(runif(if (i <= 40) 2e5 else 200L))

with_pool(4L, function(p) {
  invisible(mizu_map(p, xs, h))
  note(
    "map skewed f",
    "mizu_map",
    mark_ms(function() mizu_map(p, xs, h)),
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
