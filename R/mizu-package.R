#' mizu: Zero-Copy Async and Parallel IPC Across R and Python
#'
#' Parallel computation and data exchange between R processes on the same
#' machine. Lock-free channels and work-stealing task pools over 'POSIX'
#' shared memory (Linux, macOS) or 'Win32' file mappings (Windows), with
#' the hot path entirely in user space. Inter-process communication cheap
#' enough that work can be divided at granularities usually reserved for
#' threads.
#'
#' @section Linux memory allocator:
#' This section applies only to Linux with glibc.
#'
#' When mizu starts a channel peer or a pool worker, that process changes
#' two settings of the C memory allocator. It raises the mmap threshold
#' to 32 MB and the trim threshold to 128 MB. This keeps large payloads
#' in fast memory. Without this change, glibc asks the kernel to map and
#' unmap each large payload, and that work is slow.
#'
#' Your own R process does not change. If you want the same settings
#' there, set 'GLIBC_TUNABLES' before R starts:
#' \preformatted{
#' export GLIBC_TUNABLES=glibc.malloc.mmap_threshold=33554432:glibc.malloc.trim_threshold=134217728
#' }
#'
#' If you have set 'GLIBC_TUNABLES', mizu respects your values.
#'
#' @useDynLib mizu, .registration = TRUE
#'
#' @keywords internal
"_PACKAGE"

# Load-time guards live in C (mizu_onload_probe, src/init.c): 64-bit
# pointer width and, on Linux, pidfd_open for the peer death listener.
.onLoad <- function(libname, pkgname) {
  .Call(mizu_onload_probe)
  gc_boundary <- getOption("mizu.gc_boundary")
  if (!is.null(gc_boundary)) {
    if (
      !is.numeric(gc_boundary) ||
        length(gc_boundary) != 1L ||
        !is.finite(gc_boundary) ||
        gc_boundary < 0 ||
        gc_boundary > 2^53
    ) {
      stop(
        "mizu: option 'mizu.gc_boundary' must be a single non-negative ",
        "number of bytes not exceeding 2^53",
        call. = FALSE
      )
    }
    .Call(mizu_zc_gc_boundary_set, as.double(gc_boundary))
  }
}

# No .onUnload: the DLL must stay mapped — live handles carry finalizers
# into it, and unloading would leave R calling unmapped code at process
# exit. R_unload_mizu covers any forced unload (e.g. pkgload).
