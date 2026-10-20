# a spec stream declines a seed offset escaping exact arithmetic (L33)

    Code
      mizu:::map_stage_stream(p[["ctrl"]], 1:30, f, list(), seed = c(42, 2^53 - 14))
    Condition
      Error:
      ! mizu: .seed[2] + length(.x) must stay within 2^53, the exact seed offset range

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

