# Report-only benchmark: the cross-language channel — mizu_send/mizu_recv
# round trips against a Python echo peer (pymizu), per payload shape. One
# round trip exercises all four halves of the transport: R stage, Python
# read, Python stage, R read. Prints each number as it lands and a summary
# table at the end; asserts nothing. Exits with a message when python3 with
# pymizu and NumPy is not on PATH.
#
#   1. round-trip latency   NULL, scalars, and double vectors from the
#                           inline RAWVEC tier (8 KB) through the zero-copy
#                           SHM_VEC tier (800 KB, 8 MB), against two peers:
#                           plain echo (a received view relays by reference)
#                           and copy echo (the peer materializes, so both
#                           directions pay the full layout write)
#   2. typed payloads       strings, logicals with NA, datetime, frames —
#                           the interchange codec and MIZS/MIZL layouts
#   3. pipelined throughput small vectors in flight, no per-send wait
#
# Timings are bench::mark medians. Run:
#
#   Rscript dev/bench/crosslang-channel-bench.R

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

results <- list()

note <- function(scenario, peer, value, unit) {
  cat(sprintf(
    "  %-10s %12s %s\n",
    peer,
    formatC(value, format = "f", digits = 1, big.mark = ","),
    unit
  ))
  results[[length(results) + 1L]] <<-
    data.frame(
      scenario = scenario,
      peer = peer,
      value = value,
      unit = unit
    )
}

mark_ms <- function(f) {
  bm <- bench::mark(
    f(),
    min_time = 0.3,
    check = FALSE,
    memory = FALSE,
    filter_gc = FALSE
  )
  as.numeric(bm[["median"]]) * 1000
}

# the echo peers: a received view echoes by reference (the REF path); the
# copy peer materializes first, so both directions pay the layout write
py_echo <- "
import pymizu
while True:
    x = ch.recv(30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x)
"

py_copy_echo <- "
import pymizu
while True:
    x = ch.recv(30)
    if pymizu.is_sentinel(x):
        break
    ch.send(x.copy() if hasattr(x, 'copy') else x)
"

# orderly teardown: the echo peers break on a sentinel; mizu_close signals
# close and waits for the peer's own close rendezvous
with_peer <- function(program, f) {
  ch <- mizu_channel(
    program,
    launcher = mizu_py_launcher(py, stdout = FALSE, stderr = FALSE)
  )
  on.exit(mizu_close(ch), add = TRUE)
  f(ch)
}

rt_us <- function(ch, x, k = 1L) {
  mizu_send(ch, x)
  invisible(mizu_recv(ch, 30))
  mark_ms(function() {
    for (j in seq_len(k)) {
      mizu_send(ch, x)
      invisible(mizu_recv(ch, 30))
    }
  }) *
    1000 /
    k
}

cat("\n== 1. cross-language channel round-trip latency ==\n")

payloads <- list(
  "NULL" = NULL,
  "scalar double" = 1.5,
  "scalar string" = "hello",
  "8 KB double" = runif(1000),
  "800 KB double" = runif(100000),
  "8 MB double" = runif(1000000)
)

for (nm in names(payloads)) {
  x <- payloads[[nm]]
  with_peer(py_echo, function(ch) note(nm, "echo", rt_us(ch, x), "us/rt"))
  with_peer(py_copy_echo, function(ch) {
    note(nm, "copy echo", rt_us(ch, x), "us/rt")
  })
}

cat("\n== 2. typed payloads (echo peer) ==\n")

typed <- list(
  "10k strings" = sprintf("value-%05d", seq_len(10000)),
  "100k logical+NA" = rep(c(TRUE, FALSE, NA), length.out = 100000),
  "10k Date" = as.Date("2026-01-01") + seq_len(10000),
  "100k difftime" = as.difftime(runif(100000, 0, 3600), units = "secs"),
  "frame 100k x 2 +td" = data.frame(
    a = runif(100000),
    d = as.difftime(runif(100000, 0, 3600), units = "secs")
  ),
  "frame 1k x 4" = data.frame(
    a = runif(1000),
    b = as.integer(seq_len(1000)),
    c = sprintf("s%04d", seq_len(1000)),
    d = rep(c(TRUE, FALSE), 500)
  ),
  "frame 100k x 3" = data.frame(
    a = runif(100000),
    b = as.integer(seq_len(100000)),
    c = runif(100000)
  )
)

for (nm in names(typed)) {
  x <- typed[[nm]]
  with_peer(py_echo, function(ch) note(nm, "echo", rt_us(ch, x), "us/rt"))
}

cat("\n== 3. pipelined throughput (10k x 8 KB doubles in flight) ==\n")

k <- 10000L
x <- runif(1000)
with_peer(py_echo, function(ch) {
  # warm both directions
  mizu_send(ch, x)
  invisible(mizu_recv(ch, 30))
  ms <- mark_ms(function() {
    for (j in seq_len(k)) {
      mizu_send(ch, x)
    }
    for (j in seq_len(k)) {
      invisible(mizu_recv(ch, 30))
    }
  })
  note("pipelined 8 KB", "echo", k / ms * 1000, "rt/s")
})

cat("\n== summary ==\n")
print(do.call(rbind, results), row.names = FALSE)
