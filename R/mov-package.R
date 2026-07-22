#' mov: Lock-Free Shared-Memory Channels and Task Pools
#'
#' High-rate, small-message transport between R processes on the same
#' machine over 'POSIX' shared memory (Linux, macOS) or 'Win32' file
#' mappings (Windows), with the hot path entirely in user space.
#'
#' @useDynLib mov, .registration = TRUE
#'
#' @keywords internal
"_PACKAGE"

# mov is 64-bit only: the wire formats are built on 64-bit monotonic
# positions whose lock-freedom the wrap-arithmetic and crash-atomicity
# arguments depend on; 32-bit targets would fall back to lock-based
# 64-bit atomics, which do not work across processes. On Linux the death
# listener requires pidfd_open (kernel >= 5.3) with no fallback: without
# a listener, host death while a non-interactive peer is parked would be
# a permanent hang, not a late detection.
.onLoad <- function(libname, pkgname) {
  if (.Machine$sizeof.pointer < 8L)
    stop("mov requires 64-bit R: its cross-process wire formats depend on ",
         "lock-free 64-bit atomics", call. = FALSE)
  .Call(mov_onload_probe)
}

.onUnload <- function(libpath) {
  .Call(mov_onunload)
  library.dynam.unload("mov", libpath)
}
