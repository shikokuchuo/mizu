/* Writable attach and populated create — the platform deltas the vendored
   core deliberately does not provide. The vendored consumer open maps
   read-only, which is correct for mori's write-once model and for SHM_RAW
   payload regions; the channel region is mutable shared state (both sides
   write ring indices, parker epochs, the control block), so the peer
   attaches through this instead. Kept outside src/vendor/ so re-vendoring
   can never clobber it. */

#include <stdlib.h>
#include <stdio.h>
#include "sora.h"

#ifdef _WIN32

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

int sora_shm_open_rw(mori_shm *shm, const char *name, int populate) {

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

  /* No mapping-time flag on Windows: read-touch to populate. Reads only —
     the region is live, creator-initialized state. */
  if (populate)
    for (size_t off = 0; off < mbi.RegionSize; off += 4096)
      (void) ((const volatile unsigned char *) addr)[off];

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
static int sora_shm_os_open_rw(const char *name) {
  char path[64];
  snprintf(path, sizeof(path), "/dev/shm%s", name);
  return open(path, O_RDWR, 0);
}
#else
static int sora_shm_os_open_rw(const char *name) {
  return shm_open(name, O_RDWR, 0);
}
#endif

int sora_shm_open_rw(mori_shm *shm, const char *name, int populate) {

  shm->addr = NULL;
  shm->size = 0;
  size_t nl = strlen(name);
  if (nl >= sizeof(shm->name)) nl = sizeof(shm->name) - 1;
  memcpy(shm->name, name, nl);
  shm->name[nl] = '\0';
  shm->name_len = (uint8_t) nl;

  int fd = sora_shm_os_open_rw(name);
  if (fd < 0) return -1;

  struct stat st;
  if (fstat(fd, &st) != 0) {
    close(fd);
    return -1;
  }
  size_t size = (size_t) st.st_size;

  /* Pre-fault when asked, unlike the vendored read-only consumer open:
     the whole ring is hot on the peer, so pre-faulting once beats faulting
     on the hot path. sora_map's template contexts opt out — see sora.h. */
  void *addr = mmap(NULL, size, PROT_READ | PROT_WRITE,
                    MAP_SHARED | (populate ? MAP_POPULATE : 0), fd, 0);
  if (addr == MAP_FAILED) {
    close(fd);
    return -1;
  }

  close(fd);

#ifndef __linux__
  /* No MAP_POPULATE here: read-touch is the pre-fault. Reads only — the
     region is live, creator-initialized state whose pages the creator's
     populated create already materialized; each touch just fills this
     process's page table off the hot path. */
  if (populate)
    for (size_t off = 0; off < size; off += 4096)
      (void) ((const volatile unsigned char *) addr)[off];
#endif

#if defined(__linux__) && defined(MADV_COLLAPSE)
  /* Collapse works on shm under shmem_enabled=[never] (kernel >= 6.1; see
     the spill-reuse note in payload.c) and installs PMD mappings for this
     process even when the creator already collapsed the folios. Populated
     attaches only: a lazy attach (sora_map contexts) must keep demand
     paging. Failure is benign. */
  if (populate && size >= ((size_t) 2 << 20))
    (void) madvise(addr, size, MADV_COLLAPSE);
#endif

  shm->addr = addr;
  shm->size = size;
  return 0;
}

#ifdef __linux__
/* Read-only populated open for SHM_RAW payload reads. The vendored
   consumer open deliberately skips MAP_POPULATE — right for mori shared
   objects read lazily, wrong for a payload stream sora_payload_read
   unserializes in full immediately: 2,048 read faults per 8 MB (each a
   stage-2 walk under a VM) where one populate syscall does. Linux-only —
   macOS has no mapping-time populate flag and fault-on-demand there is
   the vendored position (see the fallback below). NULL on failure, so
   the caller's gone semantics are unchanged. */
mori_shm *sora_shm_open_ro_heap(const char *name) {
  mori_shm *shm = malloc(sizeof(mori_shm));
  if (shm == NULL) return NULL;
  shm->addr = NULL;
  shm->size = 0;
  size_t nl = strlen(name);
  if (nl >= sizeof(shm->name)) nl = sizeof(shm->name) - 1;
  memcpy(shm->name, name, nl);
  shm->name[nl] = '\0';
  shm->name_len = (uint8_t) nl;

  char path[64];
  snprintf(path, sizeof(path), "/dev/shm%s", name);
  int fd = open(path, O_RDONLY, 0);
  if (fd < 0) {
    free(shm);
    return NULL;
  }
  struct stat st;
  if (fstat(fd, &st) != 0) {
    close(fd);
    free(shm);
    return NULL;
  }
  size_t size = (size_t) st.st_size;
  void *addr = mmap(NULL, size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
  if (addr == MAP_FAILED) {
    close(fd);
    free(shm);
    return NULL;
  }
  close(fd);
#ifdef MADV_HUGEPAGE
  if (size >= 2 * 1024 * 1024)
    madvise(addr, size, MADV_HUGEPAGE);
#endif
  shm->addr = addr;
  shm->size = size;
  return shm;
}
#endif /* __linux__ */

#endif /* _WIN32 */

#ifndef __linux__
mori_shm *sora_shm_open_ro_heap(const char *name) {
  return mori_shm_open_heap(name);
}
#endif

mori_shm *sora_shm_open_rw_heap(const char *name, int populate) {
  mori_shm *shm = malloc(sizeof(mori_shm));
  if (shm == NULL) return NULL;
  if (sora_shm_open_rw(shm, name, populate) != 0) {
    free(shm);
    return NULL;
  }
  return shm;
}

/* Create + populate, for the pool / channel control regions: slot arrays
   walked incrementally over thousands of operations, where a lazy first
   touch is a zero-fill fault inside a µs-scale round trip. The vendored
   create already maps MAP_POPULATE on Linux; macOS and Windows have no
   mmap-time flag, so touch one byte per page here — the creator pays the
   zero-fill once at create, and every later first access on either side
   is a soft fault against a resident page. Stride 4096 never exceeds a
   supported page size, so no page is skipped (16 KiB pages on arm64 macOS
   just take four stores). Payload regions keep the vendored create: they
   are written in full immediately, so prefault would be a redundant pass. */
int sora_shm_create_populate(mori_shm *shm, size_t size) {
  int rc = mori_shm_create(shm, size);
#ifndef __linux__
  if (rc == MORI_OK) {
    volatile unsigned char *b = (volatile unsigned char *) shm->addr;
    for (size_t off = 0; off < size; off += 4096) b[off] = 0;
  }
#endif
#if defined(__linux__) && defined(MADV_COLLAPSE)
  /* The vendored create's MADV_HUGEPAGE is inert under the stock
     shmem_enabled=[never]; a synchronous collapse works regardless, and
     the MAP_POPULATE pages already exist. Failure is benign. */
  if (rc == MORI_OK && size >= ((size_t) 2 << 20))
    (void) madvise(shm->addr, size, MADV_COLLAPSE);
#endif
  return rc;
}
