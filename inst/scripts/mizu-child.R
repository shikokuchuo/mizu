# Static Rscript runner for mizu_spawn. A file rather than -e so no per-spawn
# command file is written; argv rather than environment variables so
# concurrent launches share no state. argv: <hex expr> <hex libpath>...
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2L || !all(grepl("^([0-9a-f][0-9a-f])+$", args))) {
  quit(save = "no", status = 2L)
}
unhex <- function(x) {
  n <- nchar(x)
  s <- rawToChar(as.raw(strtoi(
    substring(x, seq.int(1L, n, 2L), seq.int(2L, n, 2L)),
    16L
  )))
  Encoding(s) <- "UTF-8"
  s
}
.libPaths(vapply(args[-1L], unhex, character(1L), USE.NAMES = FALSE))
eval(parse(text = unhex(args[[1L]])), envir = globalenv())
