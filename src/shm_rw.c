/* Writable attach — the one platform delta the vendored core deliberately
   does not provide. The vendored consumer open maps read-only, which is
   correct for mori's write-once model and for SHM_RAW payload regions; the
   channel region is mutable shared state (both sides write ring indices,
   parker epochs, the control block), so the peer attaches through this
   instead. Kept outside src/vendor/ so re-vendoring can never clobber it. */

#include <stdlib.h>
#include <stdio.h>
#include "kioto.h"

#ifdef _WIN32

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

int kio_shm_open_rw(mori_shm *shm, const char *name, int populate) {

  (void) populate;   /* no Windows equivalent of MAP_POPULATE */
  shm->addr = NULL;
  shm->size = 0;
  shm->handle = NULL;
  size_t nl = strlen(name);
  if (nl >= sizeof(shm->name)) nl = sizeof(shm->name) - 1;
  memcpy(shm->name, name, nl);
  shm->name[nl] = '\0';
  shm->name_len = (uint8_t) nl;

  HANDLE h = OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, name);
  if (h == NULL) return -1;

  void *addr = MapViewOfFile(h, FILE_MAP_ALL_ACCESS, 0, 0, 0);
  if (addr == NULL) {
    CloseHandle(h);
    return -1;
  }

  MEMORY_BASIC_INFORMATION mbi;
  VirtualQuery(addr, &mbi, sizeof(mbi));

  shm->addr = addr;
  shm->size = mbi.RegionSize;
  shm->handle = h;
  return 0;
}

#else /* POSIX */

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#ifndef MAP_POPULATE
#define MAP_POPULATE 0
#endif

/* Same /dev/shm direct-open on Linux as the vendored core (avoids the -lrt
   link dependency of shm_open); macOS has shm_open in libc. */
#ifdef __linux__
static int kio_shm_os_open_rw(const char *name) {
  char path[64];
  snprintf(path, sizeof(path), "/dev/shm%s", name);
  return open(path, O_RDWR, 0);
}
#else
static int kio_shm_os_open_rw(const char *name) {
  return shm_open(name, O_RDWR, 0);
}
#endif

int kio_shm_open_rw(mori_shm *shm, const char *name, int populate) {

  shm->addr = NULL;
  shm->size = 0;
  size_t nl = strlen(name);
  if (nl >= sizeof(shm->name)) nl = sizeof(shm->name) - 1;
  memcpy(shm->name, name, nl);
  shm->name[nl] = '\0';
  shm->name_len = (uint8_t) nl;

  int fd = kio_shm_os_open_rw(name);
  if (fd < 0) return -1;

  struct stat st;
  if (fstat(fd, &st) != 0) {
    close(fd);
    return -1;
  }
  size_t size = (size_t) st.st_size;

  /* MAP_POPULATE (when asked), unlike the vendored read-only consumer
     open: the whole ring is hot on the peer, so pre-faulting once beats
     faulting on the hot path. kio_map's template contexts opt out — see
     kioto.h. */
  void *addr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                    MAP_SHARED | (populate ? MAP_POPULATE : 0), fd, 0);
  if (addr == MAP_FAILED) {
    close(fd);
    return -1;
  }

  close(fd);

  shm->addr = addr;
  shm->size = size;
  return 0;
}

#endif /* _WIN32 */

mori_shm *kio_shm_open_rw_heap(const char *name, int populate) {
  mori_shm *shm = malloc(sizeof(mori_shm));
  if (shm == NULL) return NULL;
  if (kio_shm_open_rw(shm, name, populate) != 0) {
    free(shm);
    return NULL;
  }
  return shm;
}
