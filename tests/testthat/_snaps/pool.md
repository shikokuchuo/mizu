# mizu_pool_create rejects fractional and NA geometry

    Code
      .Call(mizu:::mizu_pool_create, 2.5, 8L, 64L, 64L, 64L, 256L)
    Condition
      Error:
      ! mizu: max_workers must be a whole number

---

    Code
      .Call(mizu:::mizu_pool_create, 1L, 8L, 64.5, 64L, 64L, 256L)
    Condition
      Error:
      ! mizu: injection_cap must be a whole number

---

    Code
      .Call(mizu:::mizu_pool_create, 1L, 8L, 64L, 64L, NA_real_, 256L)
    Condition
      Error:
      ! mizu: result_slots must be a whole number

# the pool attach and worker join entries reject an NA suffix

    Code
      .Call(mizu:::mizu_pool_attach_call, NA_character_)
    Condition
      Error:
      ! mizu: suffix must not be NA

---

    Code
      .Call(mizu:::mizu_pool_worker_join, NA_character_, 0L, NULL)
    Condition
      Error:
      ! mizu: suffix must not be NA

# the R-level constructor validates max_workers (L35)

    Code
      mizu_pool(max_workers = NA)
    Condition
      Error:
      ! mizu: max_workers must be at least 1

---

    Code
      mizu_pool(max_workers = 0L)
    Condition
      Error:
      ! mizu: max_workers must be at least 1

# the pool constructors reject misspelled and positional options

    Code
      mizu_pool(1L, 2L)
    Condition
      Error:
      ! mizu: unused argument(s) — options after '...' must be given by name

---

    Code
      mizu_pool(slot_sise = 1024L)
    Condition
      Error:
      ! mizu: unused argument(s): 'slot_sise' — options after '...' must be given by name

---

    Code
      mizu_spawn_workers(NULL, 1L, 2L)
    Condition
      Error:
      ! mizu: unused argument(s) — options after '...' must be given by name

# mizu_spawn_workers is controller-only, spawning nothing otherwise (L36)

    Code
      mizu_spawn_workers(sub, 1L, launcher = function(token, slot) spawned <<- TRUE,
      startup_timeout = 0.2)
    Condition
      Error:
      ! mizu: only the controller can resize a pool

# the default pool validates its type

    Code
      mizu_set_default_pool(42)
    Condition
      Error:
      ! mizu: pool must be a mizu pool handle or NULL

