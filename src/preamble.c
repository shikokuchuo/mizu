/* sora-specific magic + version preamble and its validator. The preamble is
   host-written before spawn and immutable thereafter; the peer validates it
   before any shared atomic is read or written, so a version mismatch or a
   corrupt region errors out before the ring protocol is engaged. */

#include "sora.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <unistd.h>
#endif

static int sora_pow2_u32(uint32_t v) {
  return v != 0 && (v & (v - 1)) == 0;
}

void sora_preamble_write(void *region, const sora_preamble *p) {
  memcpy(region, p, sizeof(*p));
}

const char *sora_preamble_validate(const void *region, size_t region_size,
                                  sora_preamble *out) {

  if (region_size < SORA_FIXED_LAYOUT_SIZE)
    return "region is smaller than the fixed channel layout";

  sora_preamble p;
  memcpy(&p, region, sizeof(p));

  if (p.magic != SORA_MAGIC)
    return "bad magic: not a sora channel region";
  if (p.version != SORA_ABI_VERSION)
    return "ABI version mismatch: peer and host were built against "
           "different sora wire formats";
  if (!sora_pow2_u32(p.cap))
    return "ring capacity is not a power of two";
  if (!sora_pow2_u32(p.slot))
    return "slot size is not a power of two";
  if (p.arena_size % 64 != 0)
    return "arena size is not a multiple of 64";

  /* Overflow-safe extent checks: every division/subtraction form below
     avoids computing a sum that could wrap. */
  uint64_t ring_bytes = (uint64_t) p.cap * p.slot;
  uint64_t after_fixed = (uint64_t) region_size - SORA_FIXED_LAYOUT_SIZE;
  if (ring_bytes > after_fixed / 2)
    return "rings exceed the mapped region";
  if (p.arena_size > (after_fixed - 2 * ring_bytes) / 2)
    return "arenas exceed the mapped region";
  if (p.drop_offset > region_size || p.drop_size > region_size - p.drop_offset)
    return "drop slot lies outside the mapped region";
  if (p.livedir_offset > region_size ||
      p.livedir_size > region_size - p.livedir_offset)
    return "liveness-dir string lies outside the mapped region";

  if (out != NULL) *out = p;
  return NULL;
}

// .Call test surface -----------------------------------------------------------

mori_shm *sora_region(SEXP xp);   /* wrap.c */

SEXP sora_preamble_write_call(SEXP xp, SEXP cap, SEXP slot, SEXP arena,
                             SEXP drop, SEXP livedir) {
  mori_shm *shm = sora_region(xp);
  if (shm->size < SORA_FIXED_LAYOUT_SIZE)
    Rf_error("sora: region too small for a channel preamble");
  if (TYPEOF(drop) != REALSXP || XLENGTH(drop) < 2 ||
      TYPEOF(livedir) != REALSXP || XLENGTH(livedir) < 2)
    Rf_error("sora: drop and livedir must be numeric vectors of length 2");

  sora_preamble p = {
    .magic = SORA_MAGIC,
    .version = SORA_ABI_VERSION,
    .cap = (uint32_t) Rf_asInteger(cap),
    .slot = (uint32_t) Rf_asInteger(slot),
#ifdef _WIN32
    .host_pid = (uint64_t) GetCurrentProcessId(),
#else
    .host_pid = (uint64_t) getpid(),
#endif
    .arena_size = (uint64_t) Rf_asReal(arena),
    .drop_offset = (uint64_t) REAL(drop)[0],
    .drop_size = (uint64_t) REAL(drop)[1],
    .livedir_offset = (uint64_t) REAL(livedir)[0],
    .livedir_size = (uint64_t) REAL(livedir)[1],
  };
  sora_preamble_write(shm->addr, &p);
  return R_NilValue;
}

SEXP sora_preamble_validate_call(SEXP xp) {
  mori_shm *shm = sora_region(xp);

  sora_preamble p;
  const char *err = sora_preamble_validate(shm->addr, shm->size, &p);
  if (err != NULL) Rf_error("sora: invalid channel region: %s", err);

  const char *names[] = {"version", "cap", "slot", "host_pid", "arena_size",
                         "drop_offset", "drop_size", "livedir_offset",
                         "livedir_size", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, Rf_ScalarReal((double) p.version));
  SET_VECTOR_ELT(out, 1, Rf_ScalarReal((double) p.cap));
  SET_VECTOR_ELT(out, 2, Rf_ScalarReal((double) p.slot));
  SET_VECTOR_ELT(out, 3, Rf_ScalarReal((double) p.host_pid));
  SET_VECTOR_ELT(out, 4, Rf_ScalarReal((double) p.arena_size));
  SET_VECTOR_ELT(out, 5, Rf_ScalarReal((double) p.drop_offset));
  SET_VECTOR_ELT(out, 6, Rf_ScalarReal((double) p.drop_size));
  SET_VECTOR_ELT(out, 7, Rf_ScalarReal((double) p.livedir_offset));
  SET_VECTOR_ELT(out, 8, Rf_ScalarReal((double) p.livedir_size));
  UNPROTECT(1);
  return out;
}
