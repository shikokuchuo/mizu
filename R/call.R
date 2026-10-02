#' Call a Function in Another Language: the Task Specification
#'
#' `mizu_call()` builds a task specification: a qualified name or a source
#' string in the pool workers' language, plus the constant arguments.
#' [mizu_submit_call()] stages it as a neutral task stream any worker
#' language reads. This is how a foreign pool is driven: spawn the workers
#' with [mizu_py_pool_launcher()] and submit specs to them.
#'
#' The spec describes a call; it is not a value. The language never
#' appears at the call site: `mizu_submit_call()` resolves the workers'
#' language from the pool itself, for attached submitters too, and a bare
#' (unqualified) name errors at submit, not at spec construction.
#'
#' @param .name \[character(1)\] a qualified function name: `"pkg::fn"`
#'   (or `"pkg:::fn"`) for R workers, `"mod.fn"` for Python workers. The
#'   qualifier is required: a worker's global namespace is the runner
#'   module, never the submitter's, so the name resolves through the
#'   worker's own namespace machinery.
#' @param ... the constant arguments. Unnamed arguments map to the
#'   positional argument list and named ones to the named argument dict,
#'   matching Python's `*args` and `**kwargs`. Arguments must be portable
#'   values (the interchange subset documented in [mizu_send()]): a
#'   non-portable argument raises `mizu_error_not_portable` at submit,
#'   never a fallback.
#' @param .source \[character(1)\] source code in the workers' language.
#'   Evaluated in a fresh namespace (parented on the global environment in
#'   R, a fresh dict over `__builtins__` in Python) with the named
#'   arguments bound as names and positional arguments bound as `..1`,
#'   `..2`, ... The result is the trailing expression's value, or `NULL`
#'   when the source ends with a statement.
#'
#' @return `mizu_call()`: a `"mizu_call"` specification (a classed list).
#'   `mizu_submit_call()`: a task handle (class `"mizu_task"`), exactly as
#'   [mizu_submit()] returns.
#'
#' @details Results and errors cross in the submitter's own formats: a
#'   task error arrives as a `mizu_error_remote` condition (see
#'   [mizu_is_remote_error()]), and a result that has no portable home
#'   fails the task with one naming the value's type. On a same-language
#'   pool the spec verb keeps the rich private error format.
#'
#'   Large arguments cross to foreign workers by reference rather than by
#'   copy: one fresh value past the zero-copy floor stages a single
#'   layout write into a shared region (the worker reads a view over it),
#'   and an argument that is already a shared view (received from a
#'   channel, a pool result, or a `mori::share()`d vector) crosses as its
#'   identifier alone — zero payload bytes. On a pool whose workers
#'   predate the ref reader, such a task declines locally at submit
#'   naming the remedy.
#'
#' @examples
#' p <- mizu_pool()
#' # same-language pools take specs too
#' t <- mizu_submit_call(p, mizu_call("stats::quantile", runif(100)))
#' mizu_collect(t)
#' mizu_pool_stop(p)
#'
#' \dontrun{
#' p <- mizu_pool(2, launcher = mizu_py_pool_launcher())
#' t <- mizu_submit_call(p, mizu_call("numpy.mean", runif(100)))
#' mizu_collect(t)
#' mizu_pool_stop(p)
#' }
#'
#' @export
mizu_call <- function(.name = NULL, ..., .source = NULL) {
  if (is.null(.name) == is.null(.source)) {
    stop(
      "mizu: exactly one of '.name' or '.source' must be given",
      call. = FALSE
    )
  }
  code <- if (is.null(.source)) .name else .source
  if (!is.character(code) || length(code) != 1L || is.na(code)) {
    stop("mizu: '.name' and '.source' must be a single string", call. = FALSE)
  }
  args <- list(...)
  nms <- names(args)
  if (is.null(nms)) {
    nms <- rep.int("", length(args))
  }
  structure(
    list(
      code = code,
      kind = if (is.null(.source)) 0L else 1L,
      positional = unname(args[nms == ""]),
      named = args[nms != ""]
    ),
    class = "mizu_call"
  )
}

#' @rdname mizu_call
#' @param pool a pool handle (see [mizu_pool()]).
#' @param spec a `"mizu_call"` specification.
#' @param .timeout maximum seconds to wait for submission capacity.
#' @export
mizu_submit_call <- function(pool, spec, .timeout = Inf) {
  if (!inherits(spec, "mizu_call")) {
    stop("mizu: 'spec' must be a mizu_call() specification", call. = FALSE)
  }
  .Call(mizu_pool_submit_spec, pool, spec, .timeout, NULL)
}
