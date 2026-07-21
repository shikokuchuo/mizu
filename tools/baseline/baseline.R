# Phase 0 gate: incumbent-baseline benchmark (ipc-plan.md, Part I risk 4).
#
# Measures the socket-based stack mov's ring must beat in its target regime
# (high-rate small messages between two local R processes):
#
#   1. nanonext "pair" round-trip over ipc:// (UDS), 8-byte raw payload —
#      the latency floor for socket-based R IPC.
#   2. mirai local dispatch: trivial-task throughput (fire N, collect N)
#      and sequential task round-trip, with and without dispatcher.
#
# Run:  Rscript tools/baseline/baseline.R
# Results are committed in tools/baseline/BASELINE.md.

library(nanonext)

new_child <- function(code) {
  system2(file.path(R.home("bin"), "Rscript"), c("-e", shQuote(code)),
          wait = FALSE, stdout = FALSE, stderr = FALSE)
}

# 1. nanonext ipc:// round-trip ------------------------------------------------

url <- sprintf("ipc://%s/mov-baseline-%d", tempdir(), Sys.getpid())
s <- socket("pair", listen = url)

new_child(sprintf('
library(nanonext)
s <- socket("pair", dial = "%s")
repeat {
  m <- recv(s, mode = "raw", block = TRUE)
  if (length(m) == 1L && m == as.raw(0xff)) break
  send(s, m, mode = "raw", block = TRUE)
}
close(s)
', url))

msg <- as.raw(1:8)
for (i in seq_len(1000L)) {                       # warmup + rendezvous
  send(s, msg, mode = "raw", block = TRUE)
  recv(s, mode = "raw", block = TRUE)
}

reps <- 3L
n <- 100000L
rt <- numeric(reps)
for (r in seq_len(reps)) {
  t0 <- mclock()
  for (i in seq_len(n)) {
    send(s, msg, mode = "raw", block = TRUE)
    recv(s, mode = "raw", block = TRUE)
  }
  rt[r] <- (mclock() - t0) / 1000
}
send(s, as.raw(0xff), mode = "raw", block = TRUE)
close(s)

cat(sprintf("nanonext ipc:// pair round-trip (8 B raw, n = %d, best of %d):\n",
            n, reps))
cat(sprintf("  %.0f round-trips/sec, %.2f us/round-trip (per rep: %s us)\n",
            n / min(rt), min(rt) / n * 1e6,
            paste(sprintf("%.2f", rt / n * 1e6), collapse = ", ")))

# 2. mirai local dispatch ------------------------------------------------------

library(mirai)

bench_mirai <- function(dispatcher) {
  daemons(1, dispatcher = dispatcher)
  on.exit(daemons(0))
  m <- mirai(NULL); m[]                            # warmup

  n <- 10000L
  t0 <- mclock()
  ms <- vector("list", n)
  for (i in seq_len(n)) ms[[i]] <- mirai(NULL)
  for (i in seq_len(n)) ms[[i]][]
  tput <- (mclock() - t0) / 1000

  n2 <- 1000L
  t0 <- mclock()
  for (i in seq_len(n2)) {
    m <- mirai(NULL)
    m[]
  }
  rtt <- (mclock() - t0) / 1000

  cat(sprintf("mirai local (dispatcher = %s):\n", dispatcher))
  cat(sprintf("  throughput (fire %d then collect): %.0f tasks/sec\n",
              n, n / tput))
  cat(sprintf("  sequential round-trip (n = %d): %.1f us/task, %.0f tasks/sec\n",
              n2, rtt / n2 * 1e6, n2 / rtt))
}

bench_mirai(dispatcher = TRUE)
bench_mirai(dispatcher = FALSE)
