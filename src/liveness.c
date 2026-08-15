/* Liveness lock: the death *verdict* (the listener is only ever a wake
   trigger). Each spawned R process holds an exclusive lock on a per-channel
   file for its entire lifetime; the kernel releases it on any exit path.
   Probes run on the fd kept from open, never on a fresh open of the path, so
   a tmp cleaner unlinking the path changes nothing — both kept fds still
   reference the same inode and the verdict stands. */

#include <stdlib.h>
#include "sora.h"

#ifdef _WIN32

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

int sora_live_open(const char *path, intptr_t *out) {
  /* Handles are not inheritable (no SECURITY_ATTRIBUTES), so spawned
     children cannot keep a dead process's lock alive. */
  HANDLE h = CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
                         FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                         NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
  if (h == INVALID_HANDLE_VALUE) return -1;
  *out = (intptr_t) h;
  return 0;
}

int sora_live_open_existing(const char *path, intptr_t *out) {
  HANDLE h = CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
                         FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                         NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
  if (h == INVALID_HANDLE_VALUE) return -1;
  *out = (intptr_t) h;
  return 0;
}

int sora_live_try(intptr_t h) {
  OVERLAPPED ov;
  memset(&ov, 0, sizeof(ov));
  if (LockFileEx((HANDLE) h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                 0, 1, 0, &ov))
    return SORA_LIVE_ACQUIRED;
  return GetLastError() == ERROR_LOCK_VIOLATION ? SORA_LIVE_HELD : -1;
}

void sora_live_unlock(intptr_t h) {
  OVERLAPPED ov;
  memset(&ov, 0, sizeof(ov));
  UnlockFileEx((HANDLE) h, 0, 1, 0, &ov);
}

void sora_live_close(intptr_t h) {
  CloseHandle((HANDLE) h);
}

int sora_live_ident(intptr_t h, uint64_t *dev, uint64_t *ino) {
  BY_HANDLE_FILE_INFORMATION info;
  if (!GetFileInformationByHandle((HANDLE) h, &info)) return -1;
  *dev = (uint64_t) info.dwVolumeSerialNumber;
  *ino = ((uint64_t) info.nFileIndexHigh << 32) | info.nFileIndexLow;
  return 0;
}

/* Platform default: GetTempPathA — per-user local by default, and a TMP
   redirected to SMB keeps first-class LockFileEx semantics, degrading in
   latency only. */
static int sora_live_dir_default(char *buf, size_t size) {
  DWORD n = GetTempPathA((DWORD) size, buf);
  return (n > 0 && (size_t) n < size) ? 0 : -1;
}

#else /* POSIX */

#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

int sora_live_open(const char *path, intptr_t *out) {
  /* O_CLOEXEC is load-bearing: flock is scoped to the open file
     description, so a child spawned by the holder (the peer included)
     inheriting this fd would keep the lock alive past the holder's death
     and manufacture a false ALIVE. */
  int fd = open(path, O_CREAT | O_RDWR | O_CLOEXEC, 0600);
  if (fd < 0) return -1;
  /* The default directory can be world-writable (/dev/shm) and lock-file
     names are derivable from the region name, so refuse a file another
     user pre-created: a squatter taking the flock after the holder dies
     would feed the reaper a permanent false ALIVE. */
  struct stat st;
  if (fstat(fd, &st) != 0 || st.st_uid != geteuid()) {
    close(fd);
    return -1;
  }
  *out = (intptr_t) fd;
  return 0;
}

int sora_live_open_existing(const char *path, intptr_t *out) {
  int fd = open(path, O_RDWR | O_CLOEXEC);
  if (fd < 0) return -1;
  *out = (intptr_t) fd;
  return 0;
}

int sora_live_try(intptr_t h) {
  if (flock((int) h, LOCK_EX | LOCK_NB) == 0) return SORA_LIVE_ACQUIRED;
  return (errno == EWOULDBLOCK || errno == EAGAIN) ? SORA_LIVE_HELD : -1;
}

void sora_live_unlock(intptr_t h) {
  flock((int) h, LOCK_UN);
}

void sora_live_close(intptr_t h) {
  close((int) h);
}

int sora_live_ident(intptr_t h, uint64_t *dev, uint64_t *ino) {
  struct stat st;
  if (fstat((int) h, &st) != 0) return -1;
  *dev = (uint64_t) st.st_dev;
  *ino = (uint64_t) st.st_ino;
  return 0;
}

/* Platform default: /dev/shm on Linux — guaranteed tmpfs, always local
   (the vendored shm.c already opens it directly), so the NFS-degraded
   flock failure mode cannot arise. Elsewhere the per-user temp dir,
   duplicating the resolution of the vendored mori_log_dir()
   (vendor/shm.c), which is static. */
static int sora_live_dir_default(char *buf, size_t size) {
#ifdef __linux__
  int n = snprintf(buf, size, "/dev/shm");
  return (n > 0 && (size_t) n < size) ? 0 : -1;
#else
  const char *tmp = getenv("TMPDIR");
  if (tmp != NULL && tmp[0] != '\0') {
    int n = snprintf(buf, size, "%s", tmp);
    return (n > 0 && (size_t) n < size) ? 0 : -1;
  }
#ifdef __APPLE__
  size_t len = confstr(_CS_DARWIN_USER_TEMP_DIR, buf, size);
  if (len > 0 && len <= size) return 0;
#endif
  int n = snprintf(buf, size, "/tmp");
  return (n > 0 && (size_t) n < size) ? 0 : -1;
#endif
}

#endif /* _WIN32 */

// Lock directory ------------------------------------------------------------------

static size_t sora_live_dir_trim(char *buf, size_t n) {
  while (n > 1 && (buf[n - 1] == '/'
#ifdef _WIN32
                   || buf[n - 1] == '\\'
#endif
                   )) n--;
  buf[n] = '\0';
  return n;
}

const char *sora_live_dir(void) {
  /* The override is read-through so tests can set it per-call, and copied
     out rather than returned from getenv (a later Sys.setenv can invalidate
     that pointer). Oversized values return truncated, for the callers'
     >900-byte guard to reject. */
  static char ovr[1024];
  static char def[1024];
  static int resolved = 0;            /* 0 = untried, 1 = valid, -1 = failed */

  const char *env = getenv("SORA_LIVENESS_DIR");
  if (env != NULL && env[0] != '\0') {
    size_t n = strlen(env);
    if (n >= sizeof(ovr)) n = sizeof(ovr) - 1;
    memcpy(ovr, env, n);
    sora_live_dir_trim(ovr, n);
    return ovr;
  }
  if (resolved == 0) {
    resolved = sora_live_dir_default(def, sizeof(def)) == 0 ? 1 : -1;
    if (resolved > 0) sora_live_dir_trim(def, strlen(def));
  }
  return resolved > 0 ? def : NULL;
}

// .Call test surface -----------------------------------------------------------

static void sora_live_finalizer(SEXP xp) {
  void *addr = R_ExternalPtrAddr(xp);
  if (addr != NULL) {
    sora_live_close((intptr_t) addr);
    R_ClearExternalPtr(xp);
  }
}

static intptr_t sora_live_handle(SEXP xp) {
  if (TYPEOF(xp) != EXTPTRSXP) Rf_error("sora: not a liveness handle");
  void *addr = R_ExternalPtrAddr(xp);
  if (addr == NULL) Rf_error("sora: liveness handle is closed");
  return (intptr_t) addr;
}

SEXP sora_live_open_call(SEXP path) {
  if (TYPEOF(path) != STRSXP || XLENGTH(path) != 1)
    Rf_error("sora: expected a file path");
  intptr_t h;
  if (sora_live_open(CHAR(STRING_ELT(path, 0)), &h) != 0)
    Rf_error("sora: cannot open liveness file '%s'", CHAR(STRING_ELT(path, 0)));
  /* fd 0 / NULL handle cannot occur (R holds stdin), so NULL marks closed */
  SEXP xp = R_MakeExternalPtr((void *) h, R_NilValue, R_NilValue);
  R_RegisterCFinalizerEx(xp, sora_live_finalizer, TRUE);
  return xp;
}

SEXP sora_live_try_call(SEXP xp) {
  int rc = sora_live_try(sora_live_handle(xp));
  if (rc < 0) Rf_error("sora: liveness probe failed");
  return Rf_ScalarInteger(rc);
}

SEXP sora_live_close_call(SEXP xp) {
  sora_live_finalizer(xp);
  return R_NilValue;
}

SEXP sora_live_dir_call(void) {
  const char *dir = sora_live_dir();
  if (dir == NULL) Rf_error("sora: cannot resolve liveness lock directory");
  return Rf_mkString(dir);
}
