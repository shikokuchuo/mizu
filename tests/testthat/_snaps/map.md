# .collect validates against the template

    Code
      rei_map(p[["ctrl"]], 1:4, identity, .collect = "view")
    Condition
      Error:
      ! rei: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

---

    Code
      rei_map(p[["ctrl"]], 1:4, identity, .template = character(1), .collect = "view")
    Condition
      Error:
      ! rei: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

---

    Code
      rei_map(p[["ctrl"]], 1:4, identity, .collect = "all")
    Condition
      Error:
      ! rei: .collect must be "value" or "view"

---

    Code
      rei_map_run(pm, .collect = "view")
    Condition
      Error:
      ! rei: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

# a vector .seed is validated

    Code
      run_map(p, 1:4, identity, seed = c(1, 2, 3))
    Condition
      Error:
      ! rei: .seed must be a numeric vector of length 1 or 2

---

    Code
      run_map(p, 1:4, identity, seed = c(42, -1))
    Condition
      Error:
      ! rei: .seed[2] (stream offset) must be a non-negative integer

---

    Code
      run_map(p, 1:4, identity, seed = c(42, 0.5))
    Condition
      Error:
      ! rei: .seed[2] (stream offset) must be a non-negative integer

---

    Code
      run_map(p, 1:4, identity, seed = "x")
    Condition
      Error:
      ! rei: .seed must be a numeric vector of length 1 or 2

---

    Code
      run_map(p, 1:4, identity, seed = c(42, 2^53 + 2))
    Condition
      Error in `map_seed_state()`:
      ! rei: invalid stream index

