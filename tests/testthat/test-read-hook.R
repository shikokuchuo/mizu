# Read-hook containment (stage_r.c): an R longjmp out of the read hook — a
# corrupt-payload guard or an R_Unserialize resource raise — must not wedge
# the channel ring head nor re-raise on every later receive, and a pool
# collect surfaces the caught condition in place of the core's generic
# read-failure record (the slot deliberately unconsumed, pymizu's trade-off).
# The trigger here is the budget-asymmetry window between the serializer's
# C-stack budget and the unserializer's protect budget: empty at default
# ppsize (the sender's guard fires first), so the child runs with
# --max-ppsize=10000, where serialize(deep pairlist) succeeds and the
# unserialize protect-overflows. Cross-process by construction — in-process
# at default ppsize no payload can open the window.

# Spawn a child with extra Rscript flags, mizu_spawn's argv otherwise
# (Rscript options precede the script path)
spawn_with_flags <- function(expr, flags, outfile) {
  root <- getNamespaceInfo(asNamespace("mizu"), "path")
  script <- mizu:::mizu_child_script(root)
  libs <- paste(.libPaths(), collapse = .Platform[["path.sep"]])
  system2(
    mizu:::rscript_path(),
    c(flags, shQuote(script), mizu:::to_hex(expr), mizu:::to_hex(libs)),
    wait = FALSE,
    stdout = FALSE,
    stderr = FALSE
  )
}

test_that("a read-hook raise consumes / surfaces, never wedges (R-21, R-12)", {
  skip_if_no_child_mizu()
  out <- tfile()
  on.exit(unlink(out), add = TRUE)
  child <- sprintf(
    '
library(mizu)
deep <- function(n) { x <- NULL; for (i in seq_len(n)) x <- pairlist(x); x }
res <- character(0)
note <- function(k, v) res[[k]] <<- as.character(v)

d <- 15000L
s_ok <- tryCatch({ serialize(deep(d), NULL); TRUE }, error = function(e) FALSE)
u_ok <- tryCatch({ unserialize(serialize(deep(d), NULL)); TRUE }, error = function(e) FALSE)
if (!s_ok || u_ok) {
  writeLines("skip=window closed", %2$s)
  quit(save = "no", status = 0L)
}

# channel arm: the contained raise consumes the slot; the next recv returns
host <- .Call(mizu:::mizu_channel_create, quote(NULL), 64L, 256L, 4096, FALSE)
att <- .Call(mizu:::mizu_channel_attach, .Call(mizu:::mizu_channel_suffix, host), NULL)
peer <- att[[1L]]
.Call(mizu:::mizu_channel_ready_set, peer)
stopifnot(.Call(mizu:::mizu_channel_ready_wait, host, 10))
mizu_send(host, deep(d))
mizu_send(host, "tail")
note("chan_err", tryCatch(mizu_recv(peer, 30), error = conditionMessage))
note("chan_tail", mizu_recv(peer, 30))

ctrl <- .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, 64L, 512L)
sfx <- .Call(mizu:::mizu_pool_suffix, ctrl)
wk <- .Call(mizu:::mizu_pool_worker_join, sfx, 0L, NULL)
.Call(mizu:::mizu_pool_set_eval, wk)
step <- function() {
  e <- tryCatch(
    return(.Call(mizu:::mizu_pool_step, wk, 0)),
    error = function(e) e
  )
  if (.Call(mizu:::mizu_pool_run_outcome, wk, e) != 0L) stop(e)
  invisible()
}

# decode-ERR arm: an environment leaf forces the serialize tiers (a nested
# pairlist rides the MIZL coercion and decodes lazily), so the deep graph
# protect-overflows the worker decode inside the exec containment
e <- new.env()
e$v <- deep(d)
t <- mizu_submit(ctrl, length(x$v), x = e)
step()
note("decode_err", tryCatch(mizu_collect(t, 30), error = conditionMessage))
t2 <- mizu_submit(ctrl, "alive")
step()
note("decode_alive", mizu_collect(t2, 30))

# collect-side arm: the deep pairlist is built on the worker; the result
# publishes fine and the collect-side unserialize protect-overflows — the
# contained raise is stashed and signalled, the slot held for the finalizer
t3 <- mizu_submit(ctrl, { x <- NULL; for (i in seq_len(%1$d)) x <- pairlist(x); x })
step()
note("collect_err", tryCatch(mizu_collect(t3, 30), error = conditionMessage))
t4 <- mizu_submit(ctrl, "alive2")
step()
note("collect_alive", mizu_collect(t4, 30))

# collect_all (R-12): the raise carries the failing index and the consumed
# prefix; the rest stay collectible
t5 <- mizu_submit(ctrl, "a")
t6 <- mizu_submit(ctrl, { x <- NULL; for (i in seq_len(%1$d)) x <- pairlist(x); x })
t7 <- mizu_submit(ctrl, "b")
step(); step(); step()
e4 <- tryCatch(mizu_collect_all(list(t5, t6, t7), 30), error = function(e) e)
note("all_err", conditionMessage(e4))
note("all_index", e4[["index"]])
note("all_values", e4[["values"]][[1L]])
note("all_rest", mizu_collect(t7, 30))

writeLines(paste(names(res), res, sep = "="), %2$s)
quit(save = "no", status = 0L)
',
    15000L,
    deparse(out)
  )
  spawn_with_flags(child, "--max-ppsize=10000", out)
  expect_true(wait_for_file(out, timeout = 60))
  kv <- strsplit(
    sub("=", "\x01", readLines(out), fixed = TRUE),
    "\x01",
    fixed = TRUE
  )
  res <- setNames(vapply(kv, `[[`, "", 2L), vapply(kv, `[[`, "", 1L))
  skip_if(
    "skip" %in% names(res) && identical(unname(res[["skip"]]), "window closed"),
    "no budget window here"
  )
  expect_match(res[["chan_err"]], "protection stack overflow")
  expect_identical(unname(res[["chan_tail"]]), "tail")
  expect_match(res[["decode_err"]], "protection stack overflow")
  expect_identical(unname(res[["decode_alive"]]), "alive")
  expect_match(res[["collect_err"]], "protection stack overflow")
  expect_identical(unname(res[["collect_alive"]]), "alive2")
  expect_match(res[["all_err"]], "protection stack overflow")
  expect_identical(unname(res[["all_index"]]), "2")
  expect_identical(unname(res[["all_values"]]), "a")
  expect_identical(unname(res[["all_rest"]]), "b")
})
