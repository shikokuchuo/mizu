# the default pool validates its type

    Code
      mizu_set_default_pool(42)
    Condition
      Error:
      ! mizu: pool must be a mizu pool handle or NULL

