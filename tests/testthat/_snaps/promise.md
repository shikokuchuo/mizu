# the channel async verbs validate their arguments

    Code
      mizu_recv_async(p[["host"]], NA)
    Condition
      Error:
      ! mizu: timeout must be a number of seconds, not NA

---

    Code
      mizu_recv_async(p[["host"]], "5")
    Condition
      Error:
      ! mizu: timeout must be a number of seconds, not NA

---

    Code
      mizu_recv_handler(p[["host"]], 1)
    Condition
      Error:
      ! mizu: fn must be a function or NULL

# the collective async verbs validate their arguments

    Code
      mizu_collect_all_async(list(t), NA)
    Condition
      Error:
      ! mizu: timeout must be a number of seconds, not NA

---

    Code
      mizu_collect_all_async(list())
    Condition
      Error:
      ! mizu: tasks must be a non-empty list of task handles

---

    Code
      mizu_collect_any_async(list())
    Condition
      Error:
      ! mizu: tasks must be a non-empty list of task handles

