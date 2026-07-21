/* Liveness lock: the death *verdict* (the listener is only ever a wake
   trigger). Each spawned R process holds an exclusive lock on a per-channel
   file for its entire lifetime; the kernel releases it on any exit path.
   Probes run on the fd kept from open, never on a fresh open of the path, so
   a tmp cleaner unlinking the path changes nothing — both kept fds still
   reference the same inode and the verdict stands. */

#include <stdlib.h>
#include "mov.h"

#ifdef _WIN32

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

int mov_live_open(const char *path, intptr_t *out) {
  /* Handles are not inheritable (no SECURITY_ATTRIBUTES), so spawned
     children cannot keep a dead process's lock alive. */
  HANDLE h = CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
                         FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                         NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
  if (h == INVALID_HANDLE_VALUE) return -1;
  *out = (intptr_t) h;
  return 0;
}

int mov_live_try(intptr_t h) {
  OVERLAPPED ov;
  memset(&ov, 0, sizeof(ov));
  if (LockFileEx((HANDLE) h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                 0, 1, 0, &ov))
    return MOV_LIVE_ACQUIRED;
  return GetLastError() == ERROR_LOCK_VIOLATION ? MOV_LIVE_HELD : -1;
}

void mov_live_close(intptr_t h) {
  CloseHandle((HANDLE) h);
}

int mov_live_ident(intptr_t h, uint64_t *dev, uint64_t *ino) {
  BY_HANDLE_FILE_INFORMATION info;
  if (!GetFileInformationByHandle((HANDLE) h, &info)) return -1;
  *dev = (uint64_t) info.dwVolumeSerialNumber;
  *ino = ((uint64_t) info.nFileIndexHigh << 32) | info.nFileIndexLow;
  return 0;
}

#else /* POSIX */

#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

int mov_live_open(const char *path, intptr_t *out) {
  /* O_CLOEXEC is load-bearing: flock is scoped to the open file
     description, so a child spawned by the holder (the peer included)
     inheriting this fd would keep the lock alive past the holder's death
     and manufacture a false ALIVE. */
  int fd = open(path, O_CREAT | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) return -1;
  *out = (intptr_t) fd;
  return 0;
}

int mov_live_try(intptr_t h) {
  if (flock((int) h, LOCK_EX | LOCK_NB) == 0) return MOV_LIVE_ACQUIRED;
  return (errno == EWOULDBLOCK || errno == EAGAIN) ? MOV_LIVE_HELD : -1;
}

void mov_live_close(intptr_t h) {
  close((int) h);
}

int mov_live_ident(intptr_t h, uint64_t *dev, uint64_t *ino) {
  struct stat st;
  if (fstat((int) h, &st) != 0) return -1;
  *dev = (uint64_t) st.st_dev;
  *ino = (uint64_t) st.st_ino;
  return 0;
}

#endif /* _WIN32 */

// .Call test surface -----------------------------------------------------------

static void mov_live_finalizer(SEXP xp) {
  void *addr = R_ExternalPtrAddr(xp);
  if (addr != NULL) {
    mov_live_close((intptr_t) addr);
    R_ClearExternalPtr(xp);
  }
}

static intptr_t mov_live_handle(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP) Rf_error("mov: not a liveness handle");
  void *addr = R_ExternalPtrAddr(xp);
  if (addr == NULL) Rf_error("mov: liveness handle is closed");
  return (intptr_t) addr;
}

SEXP mov_live_open_call(SEXP path) {
  if (TYPEOF(path) != STRSXP || XLENGTH(path) != 1)
    Rf_error("mov: expected a file path");
  intptr_t h;
  if (mov_live_open(CHAR(STRING_ELT(path, 0)), &h) != 0)
    Rf_error("mov: cannot open liveness file '%s'", CHAR(STRING_ELT(path, 0)));
  /* fd 0 / NULL handle cannot occur (R holds stdin), so NULL marks closed */
  SEXP xp = R_MakeExternalPtr((void *) h, R_NilValue, R_NilValue);
  R_RegisterCFinalizerEx(xp, mov_live_finalizer, TRUE);
  return xp;
}

SEXP mov_live_try_call(SEXP xp) {
  int rc = mov_live_try(mov_live_handle(xp));
  if (rc < 0) Rf_error("mov: liveness probe failed");
  return Rf_ScalarInteger(rc);
}

SEXP mov_live_close_call(SEXP xp) {
  mov_live_finalizer(xp);
  return R_NilValue;
}
