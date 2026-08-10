/* Process-local allocator tuning for payload-heavy traffic, applied at
   library load: every payload that crosses a process boundary is
   materialized as a fresh vector, and under glibc's defaults (mmap at
   128 KB, aggressive heap trim) each large one is mapped by the kernel,
   faulted in page by page, and unmapped again at free — paid twice per
   large round trip, once per side. Pinning the thresholds keeps big
   vectors on the main arena and their pages resident. Gated to glibc:
   musl's mallocng hardcodes its mmap cutoff and its mallopt is a stub,
   and the macOS / Windows allocators already recycle large blocks
   in-process. A process that set its own malloc tunables keeps them. */

#include "kioto.h"

#ifdef __GLIBC__
#include <malloc.h>
#include <stdlib.h>
#include <string.h>
#endif

void kio_tune_malloc(void) {
#ifdef __GLIBC__
  const char *gt = getenv("GLIBC_TUNABLES");
  if (gt != NULL && strstr(gt, "glibc.malloc") != NULL)
    return;
  mallopt(M_MMAP_THRESHOLD, 32 << 20);
  mallopt(M_TRIM_THRESHOLD, 128 << 20);
#endif
}
