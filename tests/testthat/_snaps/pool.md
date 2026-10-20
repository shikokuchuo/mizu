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

# the default pool validates its type

    Code
      mizu_set_default_pool(42)
    Condition
      Error:
      ! mizu: pool must be a mizu pool handle or NULL

