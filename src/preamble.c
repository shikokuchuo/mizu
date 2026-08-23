/* The .Call test surface over the core's preamble write/validate
   (vendor/librei/preamble.c): the wire format itself is librei's. */

#include "sora.h"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <unistd.h>
#endif

// .Call test surface -----------------------------------------------------------

rei_shm *sora_region(SEXP xp);   /* wrap.c */

SEXP sora_preamble_write_call(SEXP xp, SEXP cap, SEXP slot, SEXP arena,
                             SEXP drop, SEXP livedir) {
  rei_shm *shm = sora_region(xp);
  if (shm->size < REI_FIXED_LAYOUT_SIZE)
    Rf_error("sora: region too small for a channel preamble");
  if (TYPEOF(drop) != REALSXP || XLENGTH(drop) < 2 ||
      TYPEOF(livedir) != REALSXP || XLENGTH(livedir) < 2)
    Rf_error("sora: drop and livedir must be numeric vectors of length 2");

  rei_preamble p = {
    .magic = REI_MAGIC,
    .version = REI_ABI_VERSION,
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
  rei_preamble_write(shm->addr, &p);
  return R_NilValue;
}

SEXP sora_preamble_validate_call(SEXP xp) {
  rei_shm *shm = sora_region(xp);

  rei_preamble p;
  const char *err = rei_preamble_validate(shm->addr, shm->size, &p);
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
