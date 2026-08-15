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
#' @importFrom utils removeSource
#' @useDynLib sora, .registration = TRUE
#'
#' @keywords internal
"_PACKAGE"

# sora is 64-bit only: the wire formats are built on 64-bit monotonic
# positions whose lock-freedom the wrap-arithmetic and crash-atomicity
# arguments depend on; 32-bit targets would fall back to lock-based
# 64-bit atomics, which do not work across processes. On Linux the death
# listener requires pidfd_open (kernel >= 5.3) with no fallback: without
# a listener, host death while a non-interactive peer is parked would be
# a permanent hang, not a late detection.
.onLoad <- function(libname, pkgname) {
  if (.Machine[["sizeof.pointer"]] < 8L) {
    stop(
      "sora requires 64-bit R: its cross-process wire formats depend on ",
      "lock-free 64-bit atomics",
      call. = FALSE
    )
  }
  .Call(sora_onload_probe)
}

.onUnload <- function(libpath) {
  .Call(sora_onunload)
  library.dynam.unload("sora", libpath)
}
