# .collect validates against the template

    Code
      mizu_map(p[["ctrl"]], 1:4, identity, .collect = "view")
    Condition
      Error:
      ! mizu: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

---

    Code
      mizu_map(p[["ctrl"]], 1:4, identity, .template = character(1), .collect = "view")
    Condition
      Error:
      ! mizu: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

---

    Code
      mizu_map(p[["ctrl"]], 1:4, identity, .collect = "all")
    Condition
      Error:
      ! mizu: .collect must be "value" or "view"

---

    Code
      mizu_map_run(pm, .collect = "view")
    Condition
      Error:
      ! mizu: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

# a vector .seed is validated

    Code
      run_map(p, 1:4, identity, seed = c(1, 2, 3))
    Condition
      Error:
      ! mizu: .seed must be a numeric vector of length 1 or 2

---

    Code
      run_map(p, 1:4, identity, seed = c(42, -1))
    Condition
      Error:
      ! mizu: .seed[2] (stream offset) must be a non-negative integer no larger than 2^53

---

    Code
      run_map(p, 1:4, identity, seed = c(42, 0.5))
    Condition
      Error:
      ! mizu: .seed[2] (stream offset) must be a non-negative integer no larger than 2^53

---

    Code
      run_map(p, 1:4, identity, seed = "x")
    Condition
      Error:
      ! mizu: .seed must be a numeric vector of length 1 or 2

---

    Code
      run_map(p, 1:4, identity, seed = c(42, Inf))
    Condition
      Error:
      ! mizu: .seed[2] (stream offset) must be a non-negative integer no larger than 2^53

---

    Code
      run_map(p, 1:4, identity, seed = c(42, 2^53 + 2))
    Condition
      Error:
      ! mizu: .seed[2] (stream offset) must be a non-negative integer no larger than 2^53

---

    Code
      run_map(p, 1:4, identity, seed = 1.5)
    Condition
      Error:
      ! mizu: .seed[1] must be an integer

# map_run rejects an NA .timeout instead of waiting forever (L31)

    Code
      mizu:::map_run(p[["ctrl"]], st, NA_real_)
    Condition
      Error:
      ! mizu: .timeout must be a number of seconds, not NA

---

    Code
      mizu:::map_run(p[["ctrl"]], st, NaN)
    Condition
      Error:
      ! mizu: .timeout must be a number of seconds, not NA

---

    Code
      mizu:::map_run(p[["ctrl"]], st, "30")
    Condition
      Error:
      ! mizu: .timeout must be a number of seconds, not NA

