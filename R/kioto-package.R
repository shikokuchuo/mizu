#' kioto: Lock-Free Shared-Memory Channels and Task Pools
#'
#' Parallel computation and data exchange between R processes on the same
#' machine: lock-free channels and work-stealing task pools over 'POSIX'
#' shared memory (Linux, macOS) or 'Win32' file mappings (Windows), with the
#' hot path entirely in user space. Inter-process communication cheap enough
#' that work can be divided at granularities usually reserved for threads.
#'
#' @useDynLib kioto, .registration = TRUE
#'
#' @keywords internal
"_PACKAGE"

# kioto is 64-bit only: the wire formats are built on 64-bit monotonic
# positions whose lock-freedom the wrap-arithmetic and crash-atomicity
# arguments depend on; 32-bit targets would fall back to lock-based
# 64-bit atomics, which do not work across processes. On Linux the death
# listener requires pidfd_open (kernel >= 5.3) with no fallback: without
# a listener, host death while a non-interactive peer is parked would be
# a permanent hang, not a late detection.
.onLoad <- function(libname, pkgname) {
  if (.Machine$sizeof.pointer < 8L) {
    stop(
      "kioto requires 64-bit R: its cross-process wire formats depend on ",
      "lock-free 64-bit atomics",
      call. = FALSE
    )
  }
  .Call(kio_onload_probe)
}

.onUnload <- function(libpath) {
  .Call(kio_onunload)
  library.dynam.unload("kioto", libpath)
}
