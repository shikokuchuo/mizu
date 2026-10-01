# Vignettes are pre-compiled: they spawn child processes and run benchmarks.
# Run from the package root, with the package installed:
#   Rscript dev/vignettes/precompile.R [name ...]   # no args renders all

# Renders a dev/vignettes/_{name}.qmd to static markdown and reassembles it as
# vignettes/{name}.qmd: the source YAML front matter, then the rendered body
# (its title heading dropped — the YAML supplies the title).
precompile <- function(name) {
  src <- file.path("dev", "vignettes", paste0("_", name, ".qmd"))
  md <- sub("\\.qmd$", ".md", src)
  out <- file.path("vignettes", paste0(name, ".qmd"))
  quarto::quarto_render(
    src,
    output_format = "gfm",
    pandoc_args = "--wrap=preserve",
    quiet = TRUE
  )
  on.exit(unlink(md))
  yaml <- readLines(src)
  yaml <- yaml[seq_len(which(yaml == "---")[2L])]
  body <- readLines(md)
  stopifnot(grepl("^# ", body[1L]))
  body <- body[-1L]
  while (length(body) && !nzchar(body[1L])) {
    body <- body[-1L]
  }
  dir.create("vignettes", showWarnings = FALSE)
  writeLines(c(yaml, "", body), out)
}

vignettes <- c(
  "mizu",
  "channels",
  "pools",
  "map",
  "interop",
  "benchmarks",
  "operations"
)
args <- commandArgs(trailingOnly = TRUE)
if (length(args)) {
  stopifnot(args %in% vignettes)
  vignettes <- args
}
for (name in vignettes) {
  precompile(name)
}
