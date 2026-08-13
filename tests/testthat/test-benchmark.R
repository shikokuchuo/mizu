# Benchmarks-as-reports: each run prints its timings next to the Phase 0
# incumbent baseline (M4 Pro, 2026-07 — nanonext ipc:// pair: 31.7 us per
# round-trip, ~31.5k RT/s; mirai local dispatch: 63-124 us per task; target
# regime: >100k small messages/s sustained) for eyeballing in the CI log. Nothing asserts on the numbers — runner timing is too
# variable for thresholds to hold reliably — so only the transport's
# correctness is tested here. The pool reports double as the regression
# tripwire for anything added to the worker's per-task path (Phase 5's
# stat counters deliberately publish only at park/tick cadence — a
# slowdown here is the first place a violation of that rule shows up).

test_that("round-trip latency reports against the socket baseline", {
  skip_if_no_child_kioto()
  ch <- kio_channel(echo_expr, capacity = 1024L)

  rt <- function(n) {
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) {
      kio_send(ch, 0L)
      kio_recv(ch, 30)
    }
    (proc.time()[[3]] - t0) / n * 1e6
  }
  rt(500L) # warm-up
  us <- min(rt(2000L), rt(2000L), rt(2000L))
  cat(sprintf("\nround-trip: %.2f us (baseline 31.73 us)\n", us))
  expect_true(kio_close(ch, timeout = 10))
})

test_that("one-way throughput reports against the >100k msg/s regime", {
  skip_if_no_child_kioto()
  n <- 200000L
  ch <- kio_channel(
    quote({
      total <- 0L
      repeat {
        xs <- kio_recv_batch(ch, n = 4096L, timeout = 30)
        if (inherits(xs, "kio_timeout")) {
          next # an idle producer is not terminal (cf. echo_expr)
        }
        if (inherits(xs, "kio_sentinel")) {
          break
        }
        total <- total + length(xs)
        if (total >= 200000L) {
          kio_send(ch, total)
          break
        }
      }
    }),
    capacity = 16384L
  )

  batch <- as.list(rep(0L, 4096L))
  t0 <- proc.time()[[3]]
  sent <- 0L
  while (sent < n) {
    want <- min(4096L, n - sent)
    sent <- sent + kio_send_batch(ch, batch[seq_len(want)])
  }
  expect_identical(kio_recv(ch, 60), n) # peer's receipt count
  rate <- n / (proc.time()[[3]] - t0)
  cat(sprintf("\none-way: %.0f msg/s (target > 100000)\n", rate))
  expect_true(kio_close(ch, timeout = 10))
})

test_that("pool task dispatch reports against the mirai baseline", {
  skip_if_no_child_kioto()
  p <- kio_pool(1L, max_submitters = 2L) # 2048 result slots for us

  rt <- function(n) {
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) {
      kio_collect(kio_submit(p, NULL), timeout = 30)
    }
    (proc.time()[[3]] - t0) / n * 1e6
  }
  rt(200L) # warm-up
  us <- min(rt(1000L), rt(1000L))
  cat(sprintf(
    "\npool round-trip: %.1f us/task (baseline: mirai 63-124 us)\n",
    us
  ))

  tp <- function(n) {
    t0 <- proc.time()[[3]]
    ts <- vector("list", n)
    for (i in seq_len(n)) {
      ts[[i]] <- kio_submit(p, NULL)
    }
    for (i in seq_len(n)) {
      kio_collect(ts[[i]], timeout = 30)
    }
    n / (proc.time()[[3]] - t0)
  }
  tp(200L)
  rate <- max(tp(2000L), tp(2000L))
  cat(sprintf("pool pipelined: %.0f tasks/s\n", rate))

  # once the worker parks, its stat mirror is exact
  total <- 200 + 2 * 1000 + 200 + 2 * 2000
  expect_true(wait_until(kio_pool_stats(p)$workers$tasks == total))
  expect_true(kio_pool_stop(p))
})

test_that("kio_map reports against serial lapply and per-task dispatch", {
  skip_on_cran() # host + 2 workers exceeds 2 cores
  skip_if_no_child_kioto()
  p <- kio_pool(2L)

  # overhead regime: trivial f, where per-element cost is everything.
  # Baselines (M4 Pro, 2026-07): serial lapply ~0.2 us/element; per-element
  # kio_submit/kio_collect ~4 us (the pool round-trip above); mirai_map
  # ~63-124 us/element (one mirai task per element).
  n <- 100000L
  x <- seq_len(n) + 0L # materialized: RAWVEC path
  f <- function(i) i + 1L
  mp <- function() {
    t0 <- proc.time()[[3]]
    r <- kio_map(p, x, f, .template = numeric(1), .timeout = 60)
    us <- (proc.time()[[3]] - t0) / n * 1e6
    expect_identical(r, x + 1)
    us
  }
  mp() # warm-up
  us <- min(mp(), mp())
  t0 <- proc.time()[[3]]
  base <- vapply(x, f, numeric(1))
  lap <- (proc.time()[[3]] - t0) / n * 1e6
  cat(sprintf(
    "\nkio_map trivial f: %.2f us/element (serial vapply %.2f, per-task dispatch ~4, mirai_map 63-124)\n",
    us,
    lap
  ))

  # compute-bound regime: the win is wall-clock division of real work
  slow <- function(i) {
    t0 <- proc.time()[[3]]
    while (proc.time()[[3]] - t0 < 0.005) {
      NULL
    }
    i
  }
  t0 <- proc.time()[[3]]
  r <- kio_map(p, 1:64, slow, .timeout = 60)
  el <- proc.time()[[3]] - t0
  expect_identical(r, as.list(1:64))
  cat(sprintf("kio_map 64 x 5ms on 2 workers: %.2fs (serial 0.32s)\n", el))
  expect_true(kio_pool_stop(p))
})

# Zero-copy plan, Phase 0: the memcpy-bound regime these next cases measure is
# the "before" column for SHM_VEC / REF (see .claude/zero-copy-plan.md). Today
# a large atomic vector falls from RAWVEC (one memcpy each way) to ARENA
# (serialize + unserialize) or SHM_RAW (count pass + serialize + open + full
# unserialize); the receive-side copy/parse is what Phase 1 removes.
#
# Phase 1 outcome (M4 Pro, 2026-08-12, same host): the receive side of a
# large vector is now an ALTREP wrap (~0.45 us), and the echo peer's re-send
# rides REF (zero bytes move). Channel 64 MiB round trip 19.0 -> 3.00 ms
# (6.3x); pool 64 MiB result 16.0 -> 10.25 ms (the residual is the worker's
# own allocVector + fill + the one send-side memcpy). The full-sweep reads
# (identical() on the view) fault pages on demand under the lazy mapping —
# no populate hint added: the delta is decisively above noise without one.
# The held-results case now reads 8 spills / 0 reused / 8 fresh: held views
# pin their regions in the lent-region ledger by design (the pre-views
# baseline pinned nothing).
#
# Baselines (M4 Pro, 2026-08-11, R 4.6.1; proc.time ticks at ~1 ms on this
# host, so every timed interval is kept >> 1 ms by looping):
#   channel round trip: 0.10 ms at 1 MiB (ARENA), 0.72 at 8 MiB, 19.0 at 64 MiB
#   pool result:        0.18 ms at 1 MiB, 1.22 at 8 MiB, 16.0 at 64 MiB
#   kio_map 64 MiB template: 0.69 s (0.08 us/element)
#   ALTREP 1:2^27 round trip ~3 us (133 B stream — never materializes)
#   100 MiB matrix round trip 36 ms; the 3-of-100-column read then 0.74 ms
#   held 8 x 16 MiB results: 8 spills, 7 reused, 1 fresh region (held R
#     objects pin nothing today; Phase 1 views will — watch this delta)
#   32 MiB named vector round trip 725 ms vs 15 ms bare: the attrs blob is
#     ~97% of cost; Phase 1's eager attrs parse keeps a share of it
# Profile split (macOS sample, 64 MiB payloads): for flat atomic vectors the
# serialize pass IS the send memcpy (mori_write_fixed -> memmove) and
# unserialize is allocVector + memcpy (mori_read_bytes -> memmove) — object-
# graph parse is noise, and region create vanishes after warm-up (free list).
# kio_map's stage + gather memcpys are ~1.5% of trivial-f wall time (per-
# element eval dominates); the gather view matters for reduce-shaped maps.
# Phase 3 (2026-08-12): the template case gained a ".collect = view" arm —
# the result wraps the output area as an ALTREP view (no gather memcpy)
# and the check reduces it (sum), so the read faults pages on demand. A
# view x (a received channel/pool view) maps by reference: the descriptor
# carries its identifier and workers read elements off the shared pages.
# Measured (M4 Pro, 2026-08-12): 0.688 s eager vs 0.666 s view + reduce —
# the gather memcpy is ~3% of trivial-f wall time, as Phase 0 measured;
# the win grows with the gather's share (reduce-shaped maps).
# Floor data (in-process one-way, steady state): ARENA ~1.9-2.2 us at
# 256 B-4 KiB; the SHM_RAW send half alone is ~1.8-2.4 us there, so SHM_VEC
# (send + ~0.45 us wrap) lands ~2.3-2.9 us — it LOSES to ARENA below
# ~8-16 KiB even before region churn (fresh create ~6-7 us vs arena flat).
# Crossover sits in the 16-64 KiB band: an internal floor constant (Phase 1)
# is warranted.

test_that("large-vector channel round trip reports the memcpy-bound regime", {
  skip_if_no_child_kioto()
  ch <- kio_channel(echo_expr, capacity = 64L)

  rt <- function(x, n) {
    kio_send(ch, x)
    expect_identical(kio_recv(ch, 60), x) # correctness on the warm-up
    best <- Inf
    for (r in 1:3) {
      t0 <- proc.time()[[3]]
      for (i in seq_len(n)) {
        kio_send(ch, x)
        kio_recv(ch, 60)
      }
      best <- min(best, proc.time()[[3]] - t0)
    }
    best / n
  }
  # 1 MiB rides ARENA (default 4 MiB arena); 8 and 64 MiB go SHM_RAW
  for (mb in c(1, 8, 64)) {
    x <- seq_len(mb * 131072) + 0
    s <- rt(x, max(4L, 256L %/% mb))
    cat(sprintf(
      "\nchannel %d MiB round trip: %.2f ms (%.0f MiB/s payload)\n",
      mb,
      s * 1e3,
      2 * mb / s
    ))
  }
  expect_true(kio_close(ch, timeout = 10))
})

test_that("pool task returning a large vector reports the memcpy-bound regime", {
  skip_if_no_child_kioto()
  p <- kio_pool(1L, max_submitters = 2L)

  for (mb in c(1, 8, 64)) {
    n <- mb * 131072
    t <- kio_submit(p, seq_len(n) + 0, n = n)
    expect_identical(kio_collect(t, 60), seq_len(n) + 0) # warm-up
    best <- Inf
    for (r in 1:3) {
      t0 <- proc.time()[[3]]
      for (i in seq_len(max(4L, 256L %/% mb))) {
        kio_collect(kio_submit(p, seq_len(n) + 0, n = n), 60)
      }
      best <- min(best, proc.time()[[3]] - t0)
    }
    s <- best / max(4L, 256L %/% mb)
    cat(sprintf(
      "\npool %d MiB result: %.2f ms/task (%.0f MiB/s payload)\n",
      mb,
      s * 1e3,
      mb / s
    ))
  }
  expect_true(kio_pool_stop(p))
})

test_that("template kio_map at large n reports the staging/gather memcpy regime", {
  skip_on_cran() # host + 2 workers exceeds 2 cores
  skip_if_no_child_kioto()
  p <- kio_pool(2L)

  # 64 MiB x section staged once (map.c), 64 MiB template output gathered back
  n <- 8 * 1024 * 1024
  x <- seq_len(n) + 0
  f <- function(i) i + 1
  mp <- function() {
    t0 <- proc.time()[[3]]
    r <- kio_map(p, x, f, .template = numeric(1), .timeout = 60)
    el <- proc.time()[[3]] - t0
    expect_identical(r, x + 1)
    el
  }
  mp() # warm-up
  el <- min(mp(), mp())
  cat(sprintf(
    "\nkio_map 64 MiB template: %.3fs (%.2f us/element)\n",
    el,
    el / n * 1e6
  ))

  # Phase 3: .collect = "view" skips the gather memcpy (the result is an
  # ALTREP view over the output area); the full-sweep read faults pages
  mpv <- function() {
    t0 <- proc.time()[[3]]
    r <- kio_map(
      p,
      x,
      f,
      .template = numeric(1),
      .collect = "view",
      .timeout = 60
    )
    el <- proc.time()[[3]] - t0
    expect_identical(sum(r), sum(x + 1))
    el
  }
  mpv() # warm-up
  elv <- min(mpv(), mpv())
  cat(sprintf(
    "kio_map 64 MiB template, view collect + reduce: %.3fs (%.2f us/element)\n",
    elv,
    elv / n * 1e6
  ))
  expect_true(kio_pool_stop(p))
})

test_that("guard: ALTREP input stays a compact stream", {
  skip_if_no_child_kioto()
  ch <- kio_channel(echo_expr, capacity = 64L)

  x <- 1:(128 * 1024 * 1024) # ALTREP seq: 1 GiB materialized
  len <- length(serialize(x, NULL))
  expect_lt(len, 1024L) # the compact-stream premise
  kio_send(ch, x)
  expect_identical(kio_recv(ch, 60), x)
  n <- 1000L # looped: proc.time ticks at ~1 ms on this platform
  t0 <- proc.time()[[3]]
  for (i in seq_len(n)) {
    kio_send(ch, x)
    kio_recv(ch, 60)
  }
  el <- proc.time()[[3]] - t0
  cat(sprintf(
    "\nALTREP 1:2^27 round trip: %.2f us (%d B serialized)\n",
    el / n * 1e6,
    len
  ))
  expect_true(kio_close(ch, timeout = 10))
})

test_that("guard: partial read of a wide matrix pays full unserialize today", {
  skip_if_no_child_kioto()
  ch <- kio_channel(echo_expr, capacity = 64L)

  m <- matrix(seq_len(131072 * 100) + 0, nrow = 131072) # 100 MiB, 100 cols
  kio_send(ch, m)
  t0 <- proc.time()[[3]]
  y <- kio_recv(ch, 60)
  t1 <- proc.time()[[3]]
  expect_identical(y, m)
  for (i in 1:100) {
    s <- y[, 1:3]
  } # looped: one read is below the timer tick
  t2 <- proc.time()[[3]]
  expect_identical(s, m[, 1:3])
  cat(sprintf(
    "\n100 MiB matrix round trip: %.1f ms; then 3-of-100 cols: %.2f ms/read\n",
    (t1 - t0) * 1e3,
    (t2 - t1) * 10
  ))
  expect_true(kio_close(ch, timeout = 10))
})

test_that("guard: held results do not pin payload regions today", {
  skip_if_no_child_kioto()
  p <- kio_pool(1L, max_submitters = 2L)

  n <- 2 * 1024 * 1024 # 16 MiB results
  held <- vector("list", 8L)
  for (i in seq_len(8L)) {
    held[[i]] <- kio_collect(kio_submit(p, seq_len(n) + 0, n = n), 60)
  }
  st <- kio_pool_stats(p)$submitters
  st <- st[st$status == "live", ] # one row per slot; the idle slot reads zero
  cat(sprintf(
    "\nheld 8 x 16 MiB: spills %d, spill_reuse %d, fresh regions %d\n",
    st$spills,
    st$spill_reuse,
    st$spills - st$spill_reuse
  ))
  expect_identical(held[[8L]], seq_len(n) + 0)
  expect_true(kio_pool_stop(p))
})

test_that("guard: attributed large vector reports the attrs-parse share", {
  skip_if_no_child_kioto()
  ch <- kio_channel(echo_expr, capacity = 64L)

  x <- seq_len(4 * 1024 * 1024) + 0 # 32 MiB
  names(x) <- paste0("n", seq_along(x)) # non-trivial attrs blob
  rt <- function(v, n) {
    kio_send(ch, v)
    expect_identical(kio_recv(ch, 60), v)
    t0 <- proc.time()[[3]]
    for (i in seq_len(n)) {
      kio_send(ch, v)
      kio_recv(ch, 60)
    }
    (proc.time()[[3]] - t0) / n
  }
  s <- rt(x, 3L)
  sx <- rt(unname(x), 3L)
  cat(sprintf(
    "\n32 MiB attributed round trip: %.2f ms (bare %.2f, attrs %.2f)\n",
    s * 1e3,
    sx * 1e3,
    (s - sx) * 1e3
  ))
  expect_true(kio_close(ch, timeout = 10))
})
