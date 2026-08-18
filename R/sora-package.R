#' sora: Lock-Free Shared-Memory Channels and Task Pools
#'
#' Parallel computation and data exchange between R processes on the same
#' machine. Lock-free channels and work-stealing task pools over 'POSIX'
#' shared memory (Linux, macOS) or 'Win32' file mappings (Windows), with
#' the hot path entirely in user space. Inter-process communication cheap
#' enough that work can be divided at granularities usually reserved for
#' threads.
#'
#' @section Linux allocator tuning:
#' On glibc Linux, loading sora raises the mmap and trim thresholds of
#' the C library allocator (to 32 MB and 128 MB). Payloads that cross a
#' process boundary are materialized as fresh vectors, and the glibc
#' defaults map, fault, and unmap every large one. A process that set its
#' own malloc tunables via 'GLIBC_TUNABLES' is left untouched. Other
#' platforms are unaffected.
#'
#' @useDynLib sora, .registration = TRUE
#'
#' @keywords internal
"_PACKAGE"

# Load-time guards live in C (sora_onload_probe, src/init.c): 64-bit
# pointer width and, on Linux, pidfd_open for the peer death listener.
.onLoad <- function(libname, pkgname) {
  .Call(sora_onload_probe)
}

# No .onUnload: the DLL must stay mapped — live handles carry finalizers
# into it, and unloading would leave R calling unmapped code at process
# exit. R_unload_sora covers any forced unload (e.g. pkgload).
