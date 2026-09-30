# corpus read-err texts stay informative

    Code
      ix_read(corpus[["err-unknown-tag"]])
    Condition
      Error in `ix_read()`:
      ! mizu: unsupported interop tag 0x7F — the peer uses a newer format

---

    Code
      ix_read(corpus[["err-version"]])
    Condition
      Error in `ix_read()`:
      ! mizu: interop format version 0x02 — the peer uses a newer format

---

    Code
      ix_read(corpus[["err-task-kind"]])
    Condition
      Error in `ix_read()`:
      ! mizu: unknown task kind 0x63 — the peer uses a newer format

---

    Code
      ix_read(corpus[["err-trailing"]])
    Condition
      Error in `ix_read()`:
      ! mizu: malformed interop stream: bytes past the one value

---

    Code
      ix_read(corpus[["err-dup-key"]])
    Condition
      Error in `ix_read()`:
      ! mizu: malformed interop stream: a duplicate dict key

---

    Code
      ix_read(corpus[["err-frame-shape"]])
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "names", "class", "row.names"; class: "data.frame")

---

    Code
      ix_read(corpus[["err-attr-unknown"]])
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "foo", "class"; class: "bar")

---

    Code
      ix_read(corpus[["err-date-fractional"]])
    Condition
      Error in `ix_read()`:
      ! mizu: no portable home for an attributed value (attributes: "class"; class: "Date")

