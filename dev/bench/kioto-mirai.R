# Report-only benchmark: kioto's task pool against mirai, matched
# scenario-for-scenario on one machine. Prints each number as it lands and
# a summary table at the end; asserts nothing.
#
#   1. sequential round-trip  evaluate 1L, 1 worker: submit + collect loop
#   2. pipelined throughput   evaluate 1L, 1 worker: fire n, collect n
#   3. payload round-trip     identity task on numeric vectors of 8 KB /
#                             800 KB / 8 MB, 1 worker: data both ways.
#                             kioto slots are sized to the payload where
#                             the 2^20 slot_size cap allows, so 8 KB and
#                             800 KB ride in-slot and 8 MB takes the spill
#                             tier (a fresh region per payload)
#   4. parallel fan-out       small compute tasks, 4 workers: fire all,
#                             collect all (in-process loop as the anchor)
#   5. streaming              one-way 1L messages: channel send_batch /
#                             recv_batch against one mirai task per message
#   6. parallel map           kio_map against mirai_map, 4 workers: trivial
#                             f per-element overhead (serial lapply as the
#                             anchor, plus kio_map's .template and .seed
#                             variants), then scenario 4's fan-out work as
#                             one map call. The models differ by design:
#                             mirai_map submits one mirai per element, so
#                             its per-task cost is its per-element cost;
#                             kio_map stages f/x once and submits ~8 chunk
#                             tasks per worker — that amortization is what
#                             the scenario measures
#
# Scenarios 1 and 2 carry a raw-transport floor row: a kioto channel echoing
# 1L, no task model on top. Scenarios 1 and 5 also carry the socket-stack
# floor — a nanonext ipc:// pair moving the same 1L serialization-free
# (raw out, integer back), the transport under mirai. mirai's smallest
# unit is the task, so its task rows are also its floor. No nanonext row
# in scenario 2: one thread pumping 10k unacknowledged echoes through
# bounded socket buffers can deadlock — the pattern needs a sized ring.
#
# mirai runs every scenario both ways: dispatcher = TRUE and FALSE. Timings
# are best-of-3 after warm-up; single runs on a busy machine still jitter.
#
# Run:  Rscript dev/bench/kioto-mirai.R

library(kioto)
library(mirai)
library(nanonext)

# helpers ----------------------------------------------------------------------

REPS <- 3L
results <- list()

note <- function(scenario, framework, value, unit) {
  cat(sprintf("  %-20s %12s %s\n", framework,
              formatC(value, format = "f", digits = 1, big.mark = ","), unit))
  results[[length(results) + 1L]] <<-
    data.frame(scenario = scenario, framework = framework, value = value,
               unit = unit)
}

timed <- function(expr) {
  t0 <- mclock()
  expr
  mclock() - t0                                   # ms
}

best_ms <- function(f) min(vapply(seq_len(REPS), function(i) f(), 0))

# the two reporting shapes: f() is one rep of `ops` operations, timed
# best-of-REPS, reported per operation or as a rate. Keep the measured loop
# inline in f — one closure call is 0.04 us, not nothing against a 1 us row,
# so there is deliberately no helper for it
note_us <- function(scenario, framework, ops, f, unit = "us/task")
  note(scenario, framework, best_ms(function() timed(f())) * 1000 / ops, unit)

note_rate <- function(scenario, framework, ops, f, unit = "tasks/s")
  note(scenario, framework, ops / best_ms(function() timed(f())) * 1000, unit)

warmup <- function(f, n = 200L) for (i in seq_len(n)) f()

# fire n, then reap n
pipeline <- function(fire, reap, n) {
  ts <- vector("list", n)
  for (i in seq_len(n)) ts[[i]] <- fire()
  for (i in seq_len(n)) reap(ts[[i]])
}

# pool up, f(pool), pool down. args reaches kio_pool, for the rows that size
# slots to the payload
with_pool <- function(workers, f, args = list()) {
  p <- do.call(kio_pool, c(list(workers, max_submitters = 2L), args))
  on.exit(kio_pool_stop(p))
  f(p)
}

with_channel <- function(expr, f, ...) {
  ch <- kio_channel(expr, ...)
  on.exit(kio_close(ch, timeout = 10))
  f(ch)
}

# the canonical echo peer: the raw-transport counterpart of a 1L task
echo_expr <- quote(
  repeat {
    x <- kio_recv(ch, timeout = 30)
    if (inherits(x, "kio_sentinel")) break
    kio_send(ch, x)
  }
)

new_child <- function(code)
  system2(file.path(R.home("bin"), "Rscript"), c("-e", shQuote(code)),
          wait = FALSE, stdout = FALSE, stderr = FALSE)

# an ipc:// pair with `body` looping in a child peer, f(socket) driving it
# from here; a 1-byte message is the poison pill that stops the peer
with_nn_pair <- function(tag, body, f) {
  url <- sprintf("ipc://%s/kioto-bench-%s-%d", tempdir(), tag, Sys.getpid())
  s <- socket("pair", listen = url)
  new_child(sprintf('
library(nanonext)
s <- socket("pair", dial = "%s")
%s
close(s)
', url, body))
  on.exit({
    invisible(send(s, as.raw(0xff), mode = "raw", block = TRUE))
    close(s)
  })
  f(s)
}

# every mirai row runs both ways, dispatcher and direct, on a daemon pool
# torn down after: f gets the framework label to hand note()
each_daemons <- function(n, prefix, f) {
  for (disp in c(TRUE, FALSE)) {
    daemons(n, dispatcher = disp)
    f(paste(prefix, if (disp) "dispatcher" else "direct"))
    daemons(0L)
  }
}

cat(sprintf("kioto %s | mirai %s | R %s | %s\n", packageVersion("kioto"),
            packageVersion("mirai"), getRversion(), R.version$platform))

# 1. sequential round-trip -----------------------------------------------------

cat("\n== 1. sequential round-trip (evaluate 1L, 1 worker) ==\n")
n <- 2000L

with_nn_pair("s1", '
repeat {
  m <- recv(s, mode = "raw", block = TRUE)
  if (length(m) == 1L) break
  send(s, m, mode = "raw", block = TRUE)
}', function(s) {
  warmup(function() {
    send(s, 1L, mode = "raw", block = TRUE)
    recv(s, mode = "integer", block = TRUE)
  })
  note_us("sequential rt", "nanonext pair", n, function()
    for (i in seq_len(n)) {
      send(s, 1L, mode = "raw", block = TRUE)
      recv(s, mode = "integer", block = TRUE)
    }, "us/rt")
})

nc <- 20000L    # channel rt is µs-scale and mclock ticks whole ms: run long
with_channel(echo_expr, function(ch) {
  warmup(function() {
    kio_send(ch, 1L)
    kio_recv(ch, 30)
  })
  note_us("sequential rt", "kioto channel", nc, function()
    for (i in seq_len(nc)) {
      kio_send(ch, 1L)
      kio_recv(ch, 30)
    }, "us/rt")
}, capacity = 1024L)

with_pool(1L, function(p) {
  warmup(function() kio_collect(kio_submit(p, 1L), timeout = 30))
  note_us("sequential rt", "kioto pool", n, function()
    for (i in seq_len(n)) kio_collect(kio_submit(p, 1L), timeout = 30))
})

each_daemons(1L, "mirai", function(fw) {
  warmup(function() mirai(1L)[])
  note_us("sequential rt", fw, n, function()
    for (i in seq_len(n)) mirai(1L)[])
})

# 2. pipelined throughput ------------------------------------------------------

cat("\n== 2. pipelined throughput (evaluate 1L, 1 worker) ==\n")
n <- 10000L
k <- 10L        # cycles per rep, again outrunning mclock's ms granularity

with_channel(echo_expr, function(ch) {            # default capacity holds n
  warmup(function() {
    kio_send(ch, 1L)
    kio_recv(ch, 30)
  })
  note_rate("pipelined", "kioto channel", k * n, function()
    for (j in seq_len(k)) {
      for (i in seq_len(n)) kio_send(ch, 1L)
      for (i in seq_len(n)) kio_recv(ch, 30)
    }, "rt/s")
})

# a submitter's outstanding tasks are bounded by its result-slot share, so
# fire-n-then-collect needs result_slots / max_submitters >= n
with_pool(1L, function(p) {
  fire <- function() kio_submit(p, 1L)
  reap <- function(t) kio_collect(t, timeout = 30)
  warmup(function() reap(fire()))
  note_rate("pipelined", "kioto pool", n, function() pipeline(fire, reap, n))
}, list(result_slots = 20480L))

each_daemons(1L, "mirai", function(fw) {
  warmup(function() mirai(1L)[])
  note_rate("pipelined", fw, n,
            function() pipeline(function() mirai(1L), function(m) m[], n))
})

# 3. payload round-trip --------------------------------------------------------

cat("\n== 3. payload round-trip (identity task on a numeric vector) ==\n")

# slots sized to the payload where the cap allows: 8 KB and 800 KB ride
# in-slot (RAWVEC, no spill); 8 MB exceeds 2^20 and takes the spill tier —
# a fresh region per payload — on default slots
payloads <- list(
  list(size = 1e3, n = 1000L, args = list(slot_size = 16384L)),
  list(size = 1e5, n = 200L,
       args = list(injection_cap = 16L, per_worker_cap = 16L,
                   result_slots = 4L, slot_size = 1048576L)),
  list(size = 1e6, n = 30L, args = list())
)

payload_label <- function(size)
  sprintf("payload %s B", formatC(8 * size, format = "d", big.mark = ","))

for (pl in payloads) {
  x <- runif(pl$size)
  n <- pl$n
  with_pool(1L, function(p) {
    stopifnot(identical(kio_collect(kio_submit(p, x, x = x), timeout = 30), x))
    note_us(payload_label(pl$size), "kioto pool", n, function()
      for (i in seq_len(n)) kio_collect(kio_submit(p, x, x = x), timeout = 30))
  }, pl$args)
}

each_daemons(1L, "mirai", function(fw) {
  mirai(NULL)[]
  for (pl in payloads) {
    x <- runif(pl$size)
    n <- pl$n
    stopifnot(identical(mirai(x, .args = list(x = x))[], x))
    note_us(payload_label(pl$size), fw, n, function()
      for (i in seq_len(n)) mirai(x, .args = list(x = x))[])
  }
})

# 4. parallel fan-out ----------------------------------------------------------

cat("\n== 4. parallel fan-out (sum(runif(1e4)) x 2000, 4 workers) ==\n")
n <- 2000L

note_rate("fan-out", "in-process", n,
          function() for (i in seq_len(n)) sum(runif(1e4)))

with_pool(4L, function(p) {                    # 2048 default slots for us
  fire <- function() kio_submit(p, sum(runif(1e4)))
  reap <- function(t) kio_collect(t, timeout = 30)
  pipeline(fire, reap, n)
  note_rate("fan-out", "kioto pool", n, function() pipeline(fire, reap, n))
})

each_daemons(4L, "mirai", function(fw) {
  pipeline(function() mirai(NULL), function(m) m[], n)
  note_rate("fan-out", fw, n,
            function() pipeline(function() mirai(sum(runif(1e4))),
                                function(m) m[], n))
})

# 5. streaming -----------------------------------------------------------------

cat("\n== 5. streaming (one-way 1L messages, batched) ==\n")
n <- 200000L
k <- 10L        # rounds per rep: one round outruns mclock's ms granularity

# the peer counts arrivals and sends one receipt per n, so the same channel
# serves the warm-up round and every rep
with_channel(quote({
  total <- 0L
  repeat {
    xs <- kio_recv_batch(ch, n = 4096L, timeout = 30)
    if (inherits(xs, "kio_sentinel")) break
    total <- total + length(xs)
    if (total >= 200000L) {
      kio_send(ch, total)
      total <- 0L
    }
  }
}), function(ch) {
  batch <- as.list(rep(1L, 4096L))
  stream_round <- function() {
    sent <- 0L
    while (sent < n) {
      want <- min(4096L, n - sent)
      sent <- sent + kio_send_batch(ch, batch[seq_len(want)])
    }
    stopifnot(identical(kio_recv(ch, 60), n))
  }
  stream_round()
  note_rate("streaming", "kioto channel", k * n,
            function() for (j in seq_len(k)) stream_round(), "msg/s")
})

# the socket has no batch lever: one send per message is its real cost
with_nn_pair("s5", '
total <- 0L
repeat {
  m <- recv(s, mode = "raw", block = TRUE)
  if (length(m) == 1L) break
  total <- total + 1L
  if (total >= 200000L) {
    send(s, total, mode = "raw", block = TRUE)
    total <- 0L
  }
}', function(s) {
  nn_round <- function() {
    for (i in seq_len(n)) send(s, 1L, mode = "raw", block = TRUE)
    stopifnot(identical(recv(s, mode = "integer", block = TRUE), n))
  }
  nn_round()
  note_rate("streaming", "nanonext pair", n, nn_round, "msg/s")
})

n <- 20000L     # a message costs mirai a whole task: 10x fewer keeps the
                # run short, and the rate is the comparison either way
each_daemons(1L, "mirai", function(fw) {
  warmup(function() mirai(1L)[])
  note_rate("streaming", fw, n,
            function() pipeline(function() mirai(1L), function(m) m[], n),
            "msg/s")
})

# 6. parallel map ---------------------------------------------------------------

cat("\n== 6. parallel map (f over n elements, 4 workers) ==\n")

# overhead regime: trivial f, where per-element cost is the whole story
n <- 10000L
x <- runif(n)
f <- function(v) v + 1

k <- 10L        # map calls per rep: a trivial map outruns mclock's ms ticks
note_us("map trivial f", "serial lapply", k * n,
        function() for (j in seq_len(k)) lapply(x, f), "us/elt")

with_pool(4L, function(p) {
  invisible(kio_map(p, x, f))
  note_us("map trivial f", "kio_map", k * n,
          function() for (j in seq_len(k)) kio_map(p, x, f), "us/elt")
  note_us("map trivial f", "kio_map template", k * n, function()
    for (j in seq_len(k)) kio_map(p, x, f, .template = numeric(1)), "us/elt")
  # per-element L'Ecuyer-CMRG streams: the price of reproducibility
  note_us("map trivial f", "kio_map .seed", k * n,
          function() for (j in seq_len(k)) kio_map(p, x, f, .seed = 42L),
          "us/elt")
})

each_daemons(4L, "mirai_map", function(fw) {
  invisible(mirai_map(x[seq_len(200L)], f)[])
  note_us("map trivial f", fw, n, function() invisible(mirai_map(x, f)[]),
          "us/elt")
})

# compute regime: scenario 4's fan-out work as a single map call — the
# per-element overhead above amortized against real tasks
n <- 2000L
g <- function(i) sum(runif(1e4))

with_pool(4L, function(p) {
  invisible(kio_map(p, seq_len(n), g))
  note_rate("map fan-out", "kio_map", n, function() kio_map(p, seq_len(n), g),
            "elts/s")
})

each_daemons(4L, "mirai_map", function(fw) {
  invisible(mirai_map(seq_len(200L), g)[])
  note_rate("map fan-out", fw, n,
            function() invisible(mirai_map(seq_len(n), g)[]), "elts/s")
})

# summary ----------------------------------------------------------------------

cat("\n== summary ==\n")
df <- do.call(rbind, results)
df <- df[order(match(df$scenario, unique(df$scenario))), ]
cat(sprintf("  %-20s %-20s %12s %s\n", df$scenario, df$framework,
            formatC(df$value, format = "f", digits = 1, big.mark = ","),
            df$unit), sep = "")
