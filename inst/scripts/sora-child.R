# Static Rscript runner for sora_spawn. A file rather than -e so no per-spawn
# command file is written; argv rather than environment variables so
# concurrent launches share no state. argv: <hex expr> <hex libpaths>.
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L || !all(grepl("^([0-9a-f][0-9a-f])+$", args))) {
  quit(save = "no", status = 2L)
}
n <- nchar(args[[2L]])
libs <- rawToChar(as.raw(strtoi(
  substring(args[[2L]], seq.int(1L, n, 2L), seq.int(2L, n, 2L)),
  16L
)))
Encoding(libs) <- "UTF-8"
.libPaths(strsplit(libs, .Platform[["path.sep"]], fixed = TRUE)[[1L]])
n <- nchar(args[[1L]])
expr <- rawToChar(as.raw(strtoi(
  substring(args[[1L]], seq.int(1L, n, 2L), seq.int(2L, n, 2L)),
  16L
)))
Encoding(expr) <- "UTF-8"
eval(parse(text = expr), envir = globalenv())
