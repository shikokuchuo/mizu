# W2 fold test helpers: the legacy (pre-fold) selection modeled exactly in
# R — the probe's candidate set, then a size pass per candidate — against
# the live selection read off the worker (a selected candidate arrives as
# a view). Exact under TASKREF-only caps: attribute-free atomic vectors
# past max(inline_max, the 32 KiB floor) are the only zc candidates
# (strings need MIZS, lists MIZL, frames ATTRS|MIZL), so the probe
# descends into every list.

ix_reserve <- 2L + 29L # MIZU_IX_ZC_RESERVE: 2 + (MIZU_NAME_MAX - 1)

# The pool's task inline budget, bisected off the spills counter.
ix_inline_max <- function(p) {
  spills <- function() {
    sum(mizu_pool_stats(p$ctrl)[["submitters"]][["spills"]])
  }
  total_of <- function(fill) {
    x <- list(
      target = 2L,
      kind = 1L,
      ident = 30064771075,
      code = "..1",
      positional = list(fill),
      named = list()
    )
    nchar(ix_write_task(x)) %/% 2L
  }
  lo <- 1L
  hi <- 600L
  while (lo < hi) {
    mid <- (lo + hi + 1L) %/% 2L
    fill <- strrep("a", mid - total_of(""))
    s1 <- spills()
    t <- mizu_submit_call(p$ctrl, mizu_call(NULL, fill, .source = "..1"))
    pool_step(p)
    mizu_collect(t)
    if (spills() > s1) hi <- mid - 1L else lo <- mid
  }
  lo
}

# A node's by-value subtree size: the value stream minus its header.
ix_node_size <- function(x) {
  header <- nchar(ix_write(NULL)) %/% 2L - 1L
  nchar(ix_write(x)) %/% 2L - header
}

ix_elt_size <- function(x) {
  switch(
    typeof(x),
    logical = 4L,
    integer = 4L,
    double = 8L,
    complex = 16L,
    raw = 1L,
    0L
  )
}

# The probe's candidate set in document order (positional then named,
# depth-first, never descending into a candidate).
ix_probe_cands <- function(args, inline_max) {
  out <- list()
  rec <- function(x) {
    es <- ix_elt_size(x)
    if (es > 0L && is.null(attributes(x))) {
      data <- length(x) * es
      if (data > inline_max && data >= 32768) {
        out[[length(out) + 1L]] <<- x
        return()
      }
    }
    if (is.list(x)) {
      for (e in x) {
        rec(e)
      }
    }
  }
  for (a in args) {
    rec(a)
  }
  out
}

# The legacy loop: the first candidate whose adjusted total fits, as the
# 1-based candidate index in document order; 0 = none.
ix_legacy_select <- function(positional, named, inline_max, code) {
  x <- list(
    target = 2L,
    kind = 1L,
    ident = 30064771075,
    code = code,
    positional = positional,
    named = named
  )
  total0 <- nchar(ix_write_task(x)) %/% 2L
  cands <- head(ix_probe_cands(c(positional, named), inline_max), 16L)
  for (i in seq_along(cands)) {
    if (total0 - ix_node_size(cands[[i]]) + ix_reserve <= inline_max) {
      return(i)
    }
  }
  0L
}

# A randomized spec: candidates (atomic vectors past the floor) and small
# fillers at top level or one list deep, with every candidate's probe path
# in document order.
ix_gen_spec <- function(inline_max) {
  ncand <- sample(c(0L, 1L, 1L, 2L, 2L, 3L, 5L, 16L, 17L), 1L)
  nfill <- sample(0:3, 1L)
  nslots <- ncand + nfill
  cand_at <- if (nslots > 0L) sort(sample.int(nslots, ncand)) else integer(0L)
  positional <- list()
  named <- list()
  paths <- character(0L)
  pos_i <- 0L
  named_i <- 0L
  for (s in seq_len(nslots)) {
    is_cand <- s %in% cand_at
    where <- sample(c("pos", "named", "nest_pos", "nest_named"), 1L)
    if (is_cand) {
      data <- sample(c(32776L, 40960L, 65536L, 100000L, 200000L), 1L)
      type <- sample(c("double", "integer", "logical", "raw"), 1L)
      n <- data %/%
        switch(
          type,
          double = 8L,
          integer = 4L,
          logical = 4L,
          raw = 1L
        )
      v <- switch(
        type,
        double = runif(n),
        integer = sample.int(100L, n, replace = TRUE),
        logical = sample(c(TRUE, FALSE), n, replace = TRUE),
        raw = as.raw(sample.int(255L, n, replace = TRUE))
      )
    } else {
      v <- switch(
        sample.int(4L, 1L),
        runif(sample.int(5L, 1L)),
        strrep("x", sample.int(20L, 1L)),
        sample.int(100L, sample.int(5L, 1L)),
        NULL
      )
    }
    if (where == "pos") {
      pos_i <- pos_i + 1L
      positional[pos_i] <- list(v)
      if (is_cand) paths <- c(paths, sprintf("..%d", pos_i))
    } else if (where == "named") {
      named_i <- named_i + 1L
      named[named_i] <- list(v)
      names(named)[named_i] <- sprintf("n%d", named_i)
      if (is_cand) paths <- c(paths, sprintf("n%d", named_i))
    } else {
      first <- sample.int(2L, 1L) == 1L
      small <- runif(3L)
      v2 <- if (first) list(v, small) else list(small, v)
      j <- if (first) 1L else 2L
      if (where == "nest_pos") {
        pos_i <- pos_i + 1L
        positional[pos_i] <- list(v2)
        if (is_cand) paths <- c(paths, sprintf("..%d[[%d]]", pos_i, j))
      } else {
        named_i <- named_i + 1L
        named[named_i] <- list(v2)
        names(named)[named_i] <- sprintf("n%d", named_i)
        if (is_cand) paths <- c(paths, sprintf("n%d[[%d]]", named_i, j))
      }
    }
  }
  list(positional = positional, named = named, paths = paths)
}

# The worker-side probe: a view flag and a checksum per candidate path.
ix_probe_src <- function(paths) {
  if (length(paths) == 0L) {
    return("list(logical(0), 0)")
  }
  checks <- sprintf(".Call(mizu:::mizu_zc_view_check, %s)", paths)
  sums <- sprintf("sum(as.numeric(%s))", paths)
  sprintf(
    "list(c(%s), sum(c(%s)))",
    paste(checks, collapse = ", "),
    paste(sums, collapse = ", ")
  )
}

# One spec through the pool: the model's selection, the live view flags,
# and the checksum verdict.
ix_run_probe <- function(p, positional, named, paths, inline_max) {
  src <- ix_probe_src(paths)
  expected <- ix_legacy_select(positional, named, inline_max, src)
  cands <- ix_probe_cands(c(positional, named), inline_max)
  want_sum <- sum(vapply(cands, function(x) sum(as.numeric(x)), numeric(1L)))
  args <- c(list(NULL), positional, named, list(.source = src))
  t <- do.call(
    mizu_submit_call,
    c(list(p$ctrl), list(do.call(mizu_call, args)))
  )
  pool_step(p)
  got <- mizu_collect(t)
  list(
    expected = expected,
    flags = as.logical(got[[1L]]),
    sum_ok = isTRUE(all.equal(got[[2L]], want_sum))
  )
}
