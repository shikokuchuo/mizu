/* L'Ecuyer-CMRG RNG stream advancement for sora_map(.seed = ) ----------------
 *
 * Pure-C implementation of MRG32k3a stream jumping, vendored from nanonext's
 * src/dispatcher.c. Moduli and jump matrix constants below are from the
 * RngStreams package by Pierre L'Ecuyer, University of Montreal
 * (https://github.com/umontreal-simul/RngStreams), licensed under the Apache
 * License, Version 2.0. The original copyright notice requests citation of:
 *
 *   P. L'Ecuyer, "Good Parameter Sets for Combined Multiple Recursive Random
 *     Number Generators", Operations Research, 47, 1 (1999), 159-164.
 *   P. L'Ecuyer, R. Simard, E. J. Chen, and W. D. Kelton, "An Objected-
 *     Oriented Random-Number Package with Many Long Streams and Substreams",
 *     Operations Research, 50, 6 (2002), 1073-1075.
 *
 * sora's additions around the vendored kernel: the scalar-to-base-state
 * derivation mirroring R's own RNG_Init scrambling for L'Ecuyer-CMRG (so
 * .seed = s derives exactly the state set.seed(s, "L'Ecuyer-CMRG") would,
 * without touching the caller's .Random.seed), the O(log k) matrix-power
 * seek (square-and-multiply over the same jump matrices, so a chunk
 * starting at element k seeds in ~log2(k) matrix products rather than k
 * jumps — what keeps per-element streams chunking-invariant at no
 * per-chunk O(k) cost), and the per-element .Random.seed install. */

#include "sora.h"

#define SORA_RNG_M1 4294967087ULL
#define SORA_RNG_M2 4294944443ULL

/* Jump matrices A1^(2^127) mod m1 and A2^(2^127) mod m2 */
static const unsigned long long A1p127[3][3] = {
  { 2427906178ULL, 3580155704ULL,  949770784ULL },
  {  226153695ULL, 1230515664ULL, 3580155704ULL },
  { 1988835001ULL,  986791581ULL, 1230515664ULL }
};
static const unsigned long long A2p127[3][3] = {
  { 1464411153ULL,  277697599ULL, 1610723613ULL },
  {   32183930ULL, 1464411153ULL, 1022607788ULL },
  { 2824425944ULL,   32183930ULL, 2093834863ULL }
};

static void mat_vec_mod(const unsigned long long A[3][3],
                        const unsigned long long *v,
                        unsigned long long *out, unsigned long long m) {
  for (int i = 0; i < 3; i++) {
    unsigned long long s = 0;
    for (int j = 0; j < 3; j++) {
      s = (s + (A[i][j] * v[j]) % m) % m;
    }
    out[i] = s;
  }
}

static void mat_mat_mod(const unsigned long long A[3][3],
                        const unsigned long long B[3][3],
                        unsigned long long out[3][3], unsigned long long m) {
  for (int i = 0; i < 3; i++)
    for (int j = 0; j < 3; j++) {
      unsigned long long s = 0;
      for (int k = 0; k < 3; k++)
        s = (s + (A[i][k] * B[k][j]) % m) % m;
      out[i][j] = s;
    }
}

/* A^k mod m by square-and-multiply: the seek's O(log k). */
static void mat_pow_mod(const unsigned long long A[3][3], uint64_t k,
                        unsigned long long out[3][3], unsigned long long m) {
  unsigned long long base[3][3], tmp[3][3];
  memcpy(base, A, sizeof(base));
  for (int i = 0; i < 3; i++)
    for (int j = 0; j < 3; j++)
      out[i][j] = i == j;
  while (k != 0) {
    if (k & 1) {
      mat_mat_mod(out, base, tmp, m);
      memcpy(out, tmp, sizeof(tmp));
    }
    k >>= 1;
    if (k != 0) {
      mat_mat_mod(base, base, tmp, m);
      memcpy(base, tmp, sizeof(tmp));
    }
  }
}

/* One 2^127-step stream jump in place over a 6-word CMRG state, stored as
   R stores it: signed ints holding values in [0, m). Non-static: the map
   batch loop (map.c) jumps its local state per element. */
void sora_rng_jump(int *seed) {
  unsigned long long v1[3] = { (unsigned int) seed[0], (unsigned int) seed[1],
                               (unsigned int) seed[2] };
  unsigned long long v2[3] = { (unsigned int) seed[3], (unsigned int) seed[4],
                               (unsigned int) seed[5] };
  unsigned long long out1[3], out2[3];
  mat_vec_mod(A1p127, v1, out1, SORA_RNG_M1);
  mat_vec_mod(A2p127, v2, out2, SORA_RNG_M2);
  for (int i = 0; i < 3; i++) {
    seed[i]     = (int) out1[i];
    seed[i + 3] = (int) out2[i];
  }
}

static void sora_rng_state_check(SEXP state) {
  if (TYPEOF(state) != INTSXP || XLENGTH(state) != 6)
    Rf_error("sora: invalid RNG stream state");
}

/* Derive the base 6-word CMRG state from a scalar seed, replicating R's
   RNG_Init for LECUYER_CMRG (RNG.c): 50 warm-up rounds of the 69069 LCG,
   then one draw per state word rejected until it falls below m2 — so the
   result is bit-identical to what set.seed(seed, "L'Ecuyer-CMRG") installs,
   with no write to the caller's .Random.seed. */
SEXP sora_map_rng_base(SEXP seed_sexp) {
  unsigned int seed = (unsigned int) Rf_asInteger(seed_sexp);
  SEXP out = Rf_allocVector(INTSXP, 6);
  int *o = INTEGER(out);
  for (int j = 0; j < 50; j++) seed = 69069 * seed + 1;
  for (int j = 0; j < 6; j++) {
    do { seed = 69069 * seed + 1; } while (seed >= (unsigned int) SORA_RNG_M2);
    o[j] = (int) seed;
  }
  /* an all-zero triple is a degenerate CMRG state; R re-randomizes, but a
     fixed .seed owes determinism more than fidelity to an astronomically
     unlikely branch */
  if (o[0] == 0 && o[1] == 0 && o[2] == 0) o[0] = 1;
  if (o[3] == 0 && o[4] == 0 && o[5] == 0) o[3] = 1;
  return out;
}

/* state advanced k stream jumps, in O(log k): element k's stream is the
   base advanced k jumps, identical for any chunking or steal order. */
SEXP sora_map_rng_seek(SEXP state, SEXP k_sexp) {
  sora_rng_state_check(state);
  double kd = Rf_asReal(k_sexp);
  if (!(kd >= 0) || kd > 9.007199254740992e15)
    Rf_error("sora: invalid stream index");
  unsigned long long P1[3][3], P2[3][3], v1[3], v2[3], o1[3], o2[3];
  mat_pow_mod(A1p127, (uint64_t) kd, P1, SORA_RNG_M1);
  mat_pow_mod(A2p127, (uint64_t) kd, P2, SORA_RNG_M2);
  const int *s = INTEGER(state);
  for (int i = 0; i < 3; i++) {
    v1[i] = (unsigned int) s[i];
    v2[i] = (unsigned int) s[i + 3];
  }
  mat_vec_mod(P1, v1, o1, SORA_RNG_M1);
  mat_vec_mod(P2, v2, o2, SORA_RNG_M2);
  SEXP out = Rf_allocVector(INTSXP, 6);
  for (int i = 0; i < 3; i++) {
    INTEGER(out)[i] = (int) o1[i];
    INTEGER(out)[i + 3] = (int) o2[i];
  }
  return out;
}

/* Install state as the evaluating process's .Random.seed (L'Ecuyer-CMRG
   form: kind word 10407, then the 6 state words) and return the state
   advanced one jump — the chunk loop's per-element step. The worker's own
   RNG state is saved and restored around the chunk loop on the R side. */
SEXP sora_map_rng_install(SEXP state) {
  sora_rng_state_check(state);
  SEXP seed = PROTECT(Rf_allocVector(INTSXP, 7));
  INTEGER(seed)[0] = 10407;
  memcpy(INTEGER(seed) + 1, INTEGER(state), 6 * sizeof(int));
  Rf_defineVar(Rf_install(".Random.seed"), seed, R_GlobalEnv);
  SEXP nxt = Rf_allocVector(INTSXP, 6);
  memcpy(INTEGER(nxt), INTEGER(state), 6 * sizeof(int));
  sora_rng_jump(INTEGER(nxt));
  UNPROTECT(1);
  return nxt;
}
