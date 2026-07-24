# Report-only benchmark: kioto's task pool against mirai, matched
# scenario-for-scenario on one machine. Prints each number as it lands and
# a summary table at the end; asserts nothing.
#
#   1. sequential round-trip  evaluate 1L, 1 worker: submit + collect loop
#   2. pipelined throughput   evaluate 1L, 1 worker: fire n, collect n
#   3. payload round-trip     identity task on numeric vectors of 8 KB /
#                             800 KB / 8 MB, 1 worker: data both ways
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

new_child <- function(code)
  system2(file.path(R.home("bin"), "Rscript"), c("-e", shQuote(code)),
          wait = FALSE, stdout = FALSE, stderr = FALSE)

cat(sprintf("kioto %s | mirai %s | R %s | %s\n", packageVersion("kioto"),
            packageVersion("mirai"), getRversion(), R.version$platform))

# the canonical echo peer: the raw-transport counterpart of a 1L task
echo_expr <- quote(
  repeat {
    x <- kio_recv(ch, timeout = 30)
    if (inherits(x, "kio_condition")) break
    kio_send(ch, x)
    kio_flush(ch)
  }
)

# 1. sequential round-trip -----------------------------------------------------

cat("\n== 1. sequential round-trip (evaluate 1L, 1 worker) ==\n")
n <- 2000L

url <- sprintf("ipc://%s/kioto-bench-%d", tempdir(), Sys.getpid())
s <- socket("pair", listen = url)
new_child(sprintf('
library(nanonext)
s <- socket("pair", dial = "%s")
repeat {
  m <- recv(s, mode = "raw", block = TRUE)
  if (length(m) == 1L) break
  send(s, m, mode = "raw", block = TRUE)
}
close(s)
', url))
for (i in seq_len(200L)) {
  send(s, 1L, mode = "raw", block = TRUE)
  recv(s, mode = "integer", block = TRUE)
}
ms <- best_ms(function() timed(
  for (i in seq_len(n)) {
    send(s, 1L, mode = "raw", block = TRUE)
    recv(s, mode = "integer", block = TRUE)
  }))
note("sequential rt", "nanonext pair", ms * 1000 / n, "us/rt")
invisible(send(s, as.raw(0xff), mode = "raw", block = TRUE))
close(s)

nc <- 20000L    # channel rt is µs-scale and mclock ticks whole ms: run long
ch <- kio_channel(echo_expr, capacity = 1024L)
for (i in seq_len(200L)) {
  kio_send(ch, 1L)
  kio_flush(ch)
  kio_recv(ch, 30)
}
ms <- best_ms(function() timed(
  for (i in seq_len(nc)) {
    kio_send(ch, 1L)
    kio_flush(ch)
    kio_recv(ch, 30)
  }))
note("sequential rt", "kioto channel", ms * 1000 / nc, "us/rt")
kio_close(ch, timeout = 10)

p <- kio_pool(1L, max_submitters = 2L)
for (i in seq_len(200L)) kio_collect(kio_submit(p, 1L), timeout = 30)
ms <- best_ms(function() timed(
  for (i in seq_len(n)) kio_collect(kio_submit(p, 1L), timeout = 30)))
note("sequential rt", "kioto pool", ms * 1000 / n, "us/task")
kio_pool_stop(p)

for (disp in c(TRUE, FALSE)) {
  daemons(1L, dispatcher = disp)
  for (i in seq_len(200L)) mirai(1L)[]
  ms <- best_ms(function() timed(for (i in seq_len(n)) mirai(1L)[]))
  note("sequential rt", if (disp) "mirai dispatcher" else "mirai direct",
       ms * 1000 / n, "us/task")
  daemons(0L)
}

# 2. pipelined throughput ------------------------------------------------------

cat("\n== 2. pipelined throughput (evaluate 1L, 1 worker) ==\n")
n <- 10000L
pipeline <- function(fire, reap) {
  ts <- vector("list", n)
  for (i in seq_len(n)) ts[[i]] <- fire()
  for (i in seq_len(n)) reap(ts[[i]])
}

k <- 10L        # cycles per rep, again outrunning mclock's ms granularity
ch <- kio_channel(echo_expr)              # default capacity holds n echoes
for (i in seq_len(200L)) {
  kio_send(ch, 1L)
  kio_flush(ch)
  kio_recv(ch, 30)
}
ms <- best_ms(function() timed(
  for (j in seq_len(k)) {
    for (i in seq_len(n)) {
      kio_send(ch, 1L)
      kio_flush(ch)
    }
    for (i in seq_len(n)) kio_recv(ch, 30)
  }))
note("pipelined", "kioto channel", k * n / ms * 1000, "rt/s")
kio_close(ch, timeout = 10)

# a submitter's outstanding tasks are bounded by its result-slot share, so
# fire-n-then-collect needs result_slots / max_submitters >= n
p <- kio_pool(1L, max_submitters = 2L, result_slots = 20480L)
fire <- function() kio_submit(p, 1L)
reap <- function(t) kio_collect(t, timeout = 30)
for (i in seq_len(200L)) reap(fire())
ms <- best_ms(function() timed(pipeline(fire, reap)))
note("pipelined", "kioto pool", n / ms * 1000, "tasks/s")
kio_pool_stop(p)

for (disp in c(TRUE, FALSE)) {
  daemons(1L, dispatcher = disp)
  for (i in seq_len(200L)) mirai(1L)[]
  ms <- best_ms(function() timed(pipeline(function() mirai(1L),
                                          function(m) m[])))
  note("pipelined", if (disp) "mirai dispatcher" else "mirai direct",
       n / ms * 1000, "tasks/s")
  daemons(0L)
}

# 3. payload round-trip --------------------------------------------------------

cat("\n== 3. payload round-trip (identity task on a numeric vector) ==\n")
sizes <- c(1e3, 1e5, 1e6)                          # 8 KB / 800 KB / 8 MB
ns <- c(1000L, 200L, 30L)

p <- kio_pool(1L, max_submitters = 2L, slot_size = 512L)
for (k in seq_along(sizes)) {
  x <- runif(sizes[k])
  n <- ns[k]
  scenario <- sprintf("payload %s B",
                      formatC(8 * sizes[k], format = "d", big.mark = ","))
  stopifnot(identical(kio_collect(kio_submit(p, x, x = x), timeout = 30), x))
  ms <- best_ms(function() timed(
    for (i in seq_len(n)) kio_collect(kio_submit(p, x, x = x), timeout = 30)))
  note(scenario, "kioto pool", ms * 1000 / n, "us/task")
}
kio_pool_stop(p)

for (disp in c(TRUE, FALSE)) {
  daemons(1L, dispatcher = disp)
  mirai(NULL)[]
  for (k in seq_along(sizes)) {
    x <- runif(sizes[k])
    n <- ns[k]
    scenario <- sprintf("payload %s B",
                        formatC(8 * sizes[k], format = "d", big.mark = ","))
    stopifnot(identical(mirai(x, .args = list(x = x))[], x))
    ms <- best_ms(function() timed(
      for (i in seq_len(n)) mirai(x, .args = list(x = x))[]))
    note(scenario, if (disp) "mirai dispatcher" else "mirai direct",
         ms * 1000 / n, "us/task")
  }
  daemons(0L)
}

# 4. parallel fan-out ----------------------------------------------------------

cat("\n== 4. parallel fan-out (sum(runif(1e4)) x 2000, 4 workers) ==\n")
n <- 2000L

ms <- best_ms(function() timed(for (i in seq_len(n)) sum(runif(1e4))))
note("fan-out", "in-process", n / ms * 1000, "tasks/s")

p <- kio_pool(4L, max_submitters = 2L)             # 2048 result slots for us
fire <- function() kio_submit(p, sum(runif(1e4)))
reap <- function(t) kio_collect(t, timeout = 30)
pipeline(fire, reap)
ms <- best_ms(function() timed(pipeline(fire, reap)))
note("fan-out", "kioto pool", n / ms * 1000, "tasks/s")
kio_pool_stop(p)

for (disp in c(TRUE, FALSE)) {
  daemons(4L, dispatcher = disp)
  pipeline(function() mirai(NULL), function(m) m[])
  ms <- best_ms(function() timed(pipeline(function() mirai(sum(runif(1e4))),
                                          function(m) m[])))
  note("fan-out", if (disp) "mirai dispatcher" else "mirai direct",
       n / ms * 1000, "tasks/s")
  daemons(0L)
}

# 5. streaming -----------------------------------------------------------------

cat("\n== 5. streaming (one-way 1L messages, batched) ==\n")
n <- 200000L

# the peer counts arrivals and sends one receipt per n, so the same channel
# serves the warm-up round and every rep
ch <- kio_channel(quote({
  total <- 0L
  repeat {
    xs <- kio_recv_batch(ch, n = 4096L, timeout = 30)
    if (inherits(xs, "kio_condition")) break
    total <- total + length(xs)
    if (total >= 200000L) {
      kio_send(ch, total)
      kio_flush(ch)
      total <- 0L
    }
  }
}))
batch <- as.list(rep(1L, 4096L))
stream_round <- function() {
  sent <- 0L
  while (sent < n) {
    want <- min(4096L, n - sent)
    sent <- sent + kio_send_batch(ch, batch[seq_len(want)])
  }
  kio_flush(ch)
  stopifnot(identical(kio_recv(ch, 60), n))
}
stream_round()
k <- 10L        # rounds per rep: one round outruns mclock's ms granularity
ms <- best_ms(function() timed(for (j in seq_len(k)) stream_round()))
note("streaming", "kioto channel", k * n / ms * 1000, "msg/s")
kio_close(ch, timeout = 10)

# the socket has no batch lever: one send per message is its real cost
url <- sprintf("ipc://%s/kioto-bench-s5-%d", tempdir(), Sys.getpid())
s <- socket("pair", listen = url)
new_child(sprintf('
library(nanonext)
s <- socket("pair", dial = "%s")
total <- 0L
repeat {
  m <- recv(s, mode = "raw", block = TRUE)
  if (length(m) == 1L) break
  total <- total + 1L
  if (total >= 200000L) {
    send(s, total, mode = "raw", block = TRUE)
    total <- 0L
  }
}
close(s)
', url))
nn_round <- function() {
  for (i in seq_len(n)) send(s, 1L, mode = "raw", block = TRUE)
  stopifnot(identical(recv(s, mode = "integer", block = TRUE), n))
}
nn_round()
ms <- best_ms(function() timed(nn_round()))
note("streaming", "nanonext pair", n / ms * 1000, "msg/s")
invisible(send(s, as.raw(0xff), mode = "raw", block = TRUE))
close(s)

n <- 20000L     # a message costs mirai a whole task: 10x fewer keeps the
                # run short, and the rate is the comparison either way
for (disp in c(TRUE, FALSE)) {
  daemons(1L, dispatcher = disp)
  for (i in seq_len(200L)) mirai(1L)[]
  ms <- best_ms(function() timed(pipeline(function() mirai(1L),
                                          function(m) m[])))
  note("streaming", if (disp) "mirai dispatcher" else "mirai direct",
       n / ms * 1000, "msg/s")
  daemons(0L)
}

# 6. parallel map ---------------------------------------------------------------

cat("\n== 6. parallel map (f over n elements, 4 workers) ==\n")

# overhead regime: trivial f, where per-element cost is the whole story
n <- 10000L
x <- runif(n)
f <- function(v) v + 1

k <- 10L        # map calls per rep: a trivial map outruns mclock's ms ticks
ms <- best_ms(function() timed(for (j in seq_len(k)) lapply(x, f)))
note("map trivial f", "serial lapply", ms * 1000 / (k * n), "us/elt")

p <- kio_pool(4L, max_submitters = 2L)
invisible(kio_map(p, x, f))
ms <- best_ms(function() timed(for (j in seq_len(k)) kio_map(p, x, f)))
note("map trivial f", "kio_map", ms * 1000 / (k * n), "us/elt")
ms <- best_ms(function() timed(
  for (j in seq_len(k)) kio_map(p, x, f, .template = numeric(1))))
note("map trivial f", "kio_map template", ms * 1000 / (k * n), "us/elt")
# per-element L'Ecuyer-CMRG streams: the price of reproducibility
ms <- best_ms(function() timed(
  for (j in seq_len(k)) kio_map(p, x, f, .seed = 42L)))
note("map trivial f", "kio_map .seed", ms * 1000 / (k * n), "us/elt")
kio_pool_stop(p)

for (disp in c(TRUE, FALSE)) {
  daemons(4L, dispatcher = disp)
  invisible(mirai_map(x[seq_len(200L)], f)[])
  ms <- best_ms(function() timed(invisible(mirai_map(x, f)[])))
  note("map trivial f",
       if (disp) "mirai_map dispatcher" else "mirai_map direct",
       ms * 1000 / n, "us/elt")
  daemons(0L)
}

# compute regime: scenario 4's fan-out work as a single map call — the
# per-element overhead above amortized against real tasks
n <- 2000L
g <- function(i) sum(runif(1e4))

p <- kio_pool(4L, max_submitters = 2L)
invisible(kio_map(p, seq_len(n), g))
ms <- best_ms(function() timed(kio_map(p, seq_len(n), g)))
note("map fan-out", "kio_map", n / ms * 1000, "elts/s")
kio_pool_stop(p)

for (disp in c(TRUE, FALSE)) {
  daemons(4L, dispatcher = disp)
  invisible(mirai_map(seq_len(200L), g)[])
  ms <- best_ms(function() timed(invisible(mirai_map(seq_len(n), g)[])))
  note("map fan-out",
       if (disp) "mirai_map dispatcher" else "mirai_map direct",
       n / ms * 1000, "elts/s")
  daemons(0L)
}

# summary ----------------------------------------------------------------------

cat("\n== summary ==\n")
df <- do.call(rbind, results)
df <- df[order(match(df$scenario, unique(df$scenario))), ]
cat(sprintf("  %-20s %-20s %12s %s\n", df$scenario, df$framework,
            formatC(df$value, format = "f", digits = 1, big.mark = ","),
            df$unit), sep = "")
