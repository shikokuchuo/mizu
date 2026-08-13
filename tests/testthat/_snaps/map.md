# .collect validates against the template

    Code
      kio_map(p[["ctrl"]], 1:4, identity, .collect = "view")
    Condition
      Error:
      ! kioto: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

---

    Code
      kio_map(p[["ctrl"]], 1:4, identity, .template = character(1), .collect = "view")
    Condition
      Error:
      ! kioto: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

---

    Code
      kio_map(p[["ctrl"]], 1:4, identity, .collect = "all")
    Condition
      Error:
      ! kioto: .collect must be "value" or "view"

---

    Code
      kio_map_run(pm, .collect = "view")
    Condition
      Error:
      ! kioto: .collect = "view" requires an atomic .template (logical, integer, double, complex or raw)

