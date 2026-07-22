# Report-only benchmark: mov's task pool against mirai, matched
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
#
# Scenarios 1 and 2 carry a raw-transport floor row: a mov channel echoing
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
# Run:  Rscript dev/bench/mov-mirai.R

library(mov)
library(mirai)
library(nanonext)

REPS <- 3L
results <- list()

note <- function(scenario, framework, value, unit) {
  cat(sprintf("  %-16s %12s %s\n", framework,
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

cat(sprintf("mov %s | mirai %s | R %s | %s\n", packageVersion("mov"),
            packageVersion("mirai"), getRversion(), R.version$platform))

# the canonical echo peer: the raw-transport counterpart of a 1L task
echo_expr <- quote(
  repeat {
    x <- mov_recv(ch, timeout = 30)
    if (inherits(x, "mov_condition")) break
    mov_send(ch, x)
    mov_flush(ch)
  }
)

# 1. sequential round-trip -----------------------------------------------------

cat("\n== 1. sequential round-trip (evaluate 1L, 1 worker) ==\n")
n <- 2000L

url <- sprintf("ipc://%s/mov-bench-%d", tempdir(), Sys.getpid())
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
ch <- mov_channel(echo_expr, capacity = 1024L)
for (i in seq_len(200L)) {
  mov_send(ch, 1L)
  mov_flush(ch)
  mov_recv(ch, 30)
}
ms <- best_ms(function() timed(
  for (i in seq_len(nc)) {
    mov_send(ch, 1L)
    mov_flush(ch)
    mov_recv(ch, 30)
  }))
note("sequential rt", "mov channel", ms * 1000 / nc, "us/rt")
mov_close(ch, timeout = 10)

p <- mov_pool(1L, max_submitters = 2L)
for (i in seq_len(200L)) mov_collect(mov_submit(p, 1L), timeout = 30)
ms <- best_ms(function() timed(
  for (i in seq_len(n)) mov_collect(mov_submit(p, 1L), timeout = 30)))
note("sequential rt", "mov pool", ms * 1000 / n, "us/task")
mov_pool_stop(p)

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
ch <- mov_channel(echo_expr)              # default capacity holds n echoes
for (i in seq_len(200L)) {
  mov_send(ch, 1L)
  mov_flush(ch)
  mov_recv(ch, 30)
}
ms <- best_ms(function() timed(
  for (j in seq_len(k)) {
    for (i in seq_len(n)) {
      mov_send(ch, 1L)
      mov_flush(ch)
    }
    for (i in seq_len(n)) mov_recv(ch, 30)
  }))
note("pipelined", "mov channel", k * n / ms * 1000, "rt/s")
mov_close(ch, timeout = 10)

# a submitter's outstanding tasks are bounded by its result-slot share, so
# fire-n-then-collect needs result_slots / max_submitters >= n
p <- mov_pool(1L, max_submitters = 2L, result_slots = 20480L)
fire <- function() mov_submit(p, 1L)
reap <- function(t) mov_collect(t, timeout = 30)
for (i in seq_len(200L)) reap(fire())
ms <- best_ms(function() timed(pipeline(fire, reap)))
note("pipelined", "mov pool", n / ms * 1000, "tasks/s")
mov_pool_stop(p)

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

p <- mov_pool(1L, max_submitters = 2L, slot_size = 512L)
for (k in seq_along(sizes)) {
  x <- runif(sizes[k])
  n <- ns[k]
  scenario <- sprintf("payload %s B",
                      formatC(8 * sizes[k], format = "d", big.mark = ","))
  stopifnot(identical(mov_collect(mov_submit(p, x, x = x), timeout = 30), x))
  ms <- best_ms(function() timed(
    for (i in seq_len(n)) mov_collect(mov_submit(p, x, x = x), timeout = 30)))
  note(scenario, "mov pool", ms * 1000 / n, "us/task")
}
mov_pool_stop(p)

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

p <- mov_pool(4L, max_submitters = 2L)             # 2048 result slots for us
fire <- function() mov_submit(p, sum(runif(1e4)))
reap <- function(t) mov_collect(t, timeout = 30)
pipeline(fire, reap)
ms <- best_ms(function() timed(pipeline(fire, reap)))
note("fan-out", "mov pool", n / ms * 1000, "tasks/s")
mov_pool_stop(p)

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
ch <- mov_channel(quote({
  total <- 0L
  repeat {
    xs <- mov_recv_batch(ch, n = 4096L, timeout = 30)
    if (inherits(xs, "mov_condition")) break
    total <- total + length(xs)
    if (total >= 200000L) {
      mov_send(ch, total)
      mov_flush(ch)
      total <- 0L
    }
  }
}))
batch <- as.list(rep(1L, 4096L))
stream_round <- function() {
  sent <- 0L
  while (sent < n) {
    want <- min(4096L, n - sent)
    sent <- sent + mov_send_batch(ch, batch[seq_len(want)])
  }
  mov_flush(ch)
  stopifnot(identical(mov_recv(ch, 60), n))
}
stream_round()
k <- 10L        # rounds per rep: one round outruns mclock's ms granularity
ms <- best_ms(function() timed(for (j in seq_len(k)) stream_round()))
note("streaming", "mov channel", k * n / ms * 1000, "msg/s")
mov_close(ch, timeout = 10)

# the socket has no batch lever: one send per message is its real cost
url <- sprintf("ipc://%s/mov-bench-s5-%d", tempdir(), Sys.getpid())
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

# summary ----------------------------------------------------------------------

cat("\n== summary ==\n")
df <- do.call(rbind, results)
df <- df[order(match(df$scenario, unique(df$scenario))), ]
cat(sprintf("  %-20s %-16s %12s %s\n", df$scenario, df$framework,
            formatC(df$value, format = "f", digits = 1, big.mark = ","),
            df$unit), sep = "")
