# a spec .f errors with .stream, informatively

    Code
      mizu:::map_stage_stream(p[["ctrl"]], 1:4, mizu_call("base::abs"), list())
    Condition
      Error:
      ! mizu: a mizu_call() spec as '.f' cannot stream — chunk slices cross as same-language task payloads

