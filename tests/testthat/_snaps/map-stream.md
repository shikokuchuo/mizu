# view collect errors on a streaming spec map, informatively

    Code
      mizu_map(p[["ctrl"]], 1:4, mizu_call("base::abs"), .template = numeric(1),
      .collect = "view", .stream = TRUE)
    Condition
      Error:
      ! mizu: .collect = "view" cannot compose a streaming spec map — per-chunk output areas cannot compose into one view

---

    Code
      mizu_map_prepare(p[["ctrl"]], 1:4, mizu_call("base::abs"), .stream = TRUE)
    Condition
      Error:
      ! mizu: a prepared map with a mizu_call() spec as '.f' cannot stream — a spec stream stages its chunk regions at run time, so there is nothing to prepare

