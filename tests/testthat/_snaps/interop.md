# corrupt and unknown streams raise informatively

    Code
      ix_read("49017f")
    Condition
      Error in `ix_read()`:
      ! mizu: unsupported interop tag 0x7F — the peer uses a newer format

---

    Code
      ix_read("490200")
    Condition
      Error in `ix_read()`:
      ! mizu: interop format version 0x02 — the peer uses a newer format

---

    Code
      ix_read("4901020102030405")
    Condition
      Error in `ix_read()`:
      ! mizu: truncated interop stream

---

    Code
      ix_read("49010f0f")
    Condition
      Error in `ix_read()`:
      ! mizu: malformed interop stream: an attr wraps an attr

---

    Code
      ix_read(ix_corpus()[["err-frame-shape"]])
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "names", "class", "row.names"; class: "data.frame")

---

    Code
      .Call(mizu:::mizu_interop_read_call, s)
    Condition
      Error:
      ! mizu: no portable home for an attributed value (attributes: "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk

# an attribute set outside the whitelist and a mismatched frame decline

    Code
      ix_read(bad)
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "foo", "class"; class: "bar")

# corrupt dim and row.names shapes decline at the writer

    Code
      ix_write(structure(integer(0), dim = c(2L, 0L)))
    Condition
      Error in `sprintf()`:
      ! mizu: not portable at x (an attribute set outside the portable whitelist)

# the runner stream writes and rejects as a plain task

    Code
      .Call(mizu:::mizu_interop_runner_call, "nm", 1, 5, 2L, 1)
    Condition
      Error:
      ! mizu: expected NULL or a numeric (seed, offset) pair

---

    Code
      .Call(mizu:::mizu_interop_runner_call, "nm", 1, c(1, NA), 2L, 1)
    Condition
      Error:
      ! mizu: expected integral (seed, offset) elements within int64 range

---

    Code
      .Call(mizu:::mizu_interop_runner_call, 42, 1, NULL, 2L, 1)
    Condition
      Error:
      ! mizu: expected a length-1 non-NA region name

---

    Code
      .Call(mizu:::mizu_interop_runner_call, "nm", NA_real_, NULL, 2L, 1)
    Condition
      Error:
      ! mizu: expected a non-negative integer generation field below 2^63

# a malformed foreign factor declines

    Code
      ix_read(ix_factor(1:2, c("a", NA)))
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "levels", "class"; class: "factor")

---

    Code
      ix_read(ix_factor(1:2, c("a", "a")))
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "levels", "class"; class: "factor")

---

    Code
      ix_read(ix_factor(c(1L, 5L), c("a", "b")))
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "levels", "class"; class: "factor")

