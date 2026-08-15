# .collect validates against the template

    Code
      sora_map(p[["ctrl"]], 1:4, identity, .collect = "view")
    Condition
      Error:
      ! sora: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

---

    Code
      sora_map(p[["ctrl"]], 1:4, identity, .template = character(1), .collect = "view")
    Condition
      Error:
      ! sora: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

---

    Code
      sora_map(p[["ctrl"]], 1:4, identity, .collect = "all")
    Condition
      Error:
      ! sora: .collect must be "value" or "view"

---

    Code
      sora_map_run(pm, .collect = "view")
    Condition
      Error:
      ! sora: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

# a vector .seed is validated

    Code
      run_map(p, 1:4, identity, seed = c(1, 2, 3))
    Condition
      Error:
      ! sora: .seed must be a numeric vector of length 1 or 2

---

    Code
      run_map(p, 1:4, identity, seed = c(42, -1))
    Condition
      Error:
      ! sora: .seed[2] (stream offset) must be a non-negative integer

---

    Code
      run_map(p, 1:4, identity, seed = c(42, 0.5))
    Condition
      Error:
      ! sora: .seed[2] (stream offset) must be a non-negative integer

---

    Code
      run_map(p, 1:4, identity, seed = "x")
    Condition
      Error:
      ! sora: .seed must be a numeric vector of length 1 or 2

---

    Code
      run_map(p, 1:4, identity, seed = c(42, 2^53 + 2))
    Condition
      Error in `map_seed_state()`:
      ! sora: invalid stream index

