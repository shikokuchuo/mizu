# Report-only A/B: the Arrow frame paths against the conversion baselines.
#   send    mizu_send(arrow_table) vs convert_array_stream() + mizu_send(df)
#           — the per-element CHARSXP creation on string columns and the
#           second fixed-width copy the direct path removes
#   receive mizu_as_arrow(df) vs as_nanoarrow_array_stream(df) on a received
#           frame — the zero-copy borrow against a full materialize
# Prints bench::mark medians; asserts nothing. Run: Rscript dev/bench/arrow-bench.R

library(mizu)

n <- 2e5
df <- data.frame(
  x = runif(n),
  i = seq_len(n) + 0L,
  s = rep(c("alpha", "beta", "gamma", "delta", NA), length.out = n),
  f = factor(rep(c("a", "b", "c", NA), length.out = n)),
  t = as.POSIXct("2024-01-01", tz = "UTC") + seq_len(n)
)
tab <- arrow::arrow_table(df)
cat(sprintf(
  "table: %d rows x %d cols (~%.1f MB as a frame)\n",
  nrow(df),
  ncol(df),
  sum(vapply(df, object.size, numeric(1))) / 2^20
))

drain <- quote(
  repeat {
    x <- mizu_recv(ch, timeout = 30)
    if (inherits(x, "mizu_sentinel")) break
  }
)
ch <- mizu_channel(drain)
on.exit(mizu_close(ch, timeout = 10), add = TRUE)

to_df <- function() {
  nanoarrow::convert_array_stream(nanoarrow::as_nanoarrow_array_stream(tab))
}

b <- bench::mark(
  send_table = mizu_send(ch, tab),
  send_converted = mizu_send(ch, to_df()),
  min_iterations = 10L,
  check = FALSE,
  filter_gc = FALSE
)
print(b[, c("expression", "median", "mem_alloc", "itr/sec")])

# the receive side: one frame echoed back, converted out each way
ch2 <- mizu_channel(quote(
  repeat {
    x <- mizu_recv(ch, timeout = 30)
    if (inherits(x, "mizu_sentinel")) {
      break
    }
    mizu_send(ch, x)
  }
))
on.exit(mizu_close(ch2, timeout = 10), add = TRUE)
mizu_send(ch2, tab)
rcv <- mizu_recv(ch2, timeout = 30)
stopifnot(.Call(mizu:::mizu_zc_view_check, rcv$x))

r <- bench::mark(
  as_arrow = mizu_as_arrow(rcv),
  nanoarrow_stream = nanoarrow::as_nanoarrow_array_stream(rcv),
  min_iterations = 10L,
  check = FALSE,
  filter_gc = FALSE
)
print(r[, c("expression", "median", "mem_alloc", "itr/sec")])

# the relay: remote-leaf columns borrow off their referenced regions — the
# export against a full materialize, per column off a different region
ch3 <- mizu_channel(quote(
  repeat {
    x <- mizu_recv(ch, timeout = 30)
    if (inherits(x, "mizu_sentinel")) {
      break
    }
    mizu_send(ch, data.frame(a = x[[1L]], a2 = x[[2L]], b = x[[1L]] * 2))
  }
))
on.exit(mizu_close(ch3, timeout = 10), add = TRUE)
mizu_send(ch3, list(df$x, df$i))
rel <- mizu_recv(ch3, timeout = 30)
stopifnot(.Call(mizu:::mizu_zc_view_check, rel$a))
invisible(rel$a) # resolve + cache the remote views ahead of the marks
invisible(rel$a2)

rr <- bench::mark(
  as_arrow_remote = mizu_as_arrow(rel),
  nanoarrow_stream_remote = nanoarrow::as_nanoarrow_array_stream(rel),
  min_iterations = 10L,
  check = FALSE,
  filter_gc = FALSE
)
print(rr[, c("expression", "median", "mem_alloc", "itr/sec")])
