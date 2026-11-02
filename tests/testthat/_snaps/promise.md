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

# mizu_collect_async validates and names the pending consumer

    Code
      mizu_collect_async(t, NA)
    Condition
      Error:
      ! mizu: timeout must be a number of seconds, not NA

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

---

    Code
      mizu_collect_any_async(list(t), NA)
    Condition
      Error:
      ! mizu: timeout must be a number of seconds, not NA

# a map promise is cached; collected and streaming handles refuse one

    Code
      print(m)
    Output
      <mizu_map: 4 elements, promise pending>

