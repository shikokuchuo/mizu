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

---

    Code
      ix_read(corpus[["err-task-value"]])
    Condition
      Error in `ix_read()`:
      ! mizu: an interop task is not a value

---

    Code
      ix_read(corpus[["err-task-nested"]])
    Condition
      Error in `ix_read()`:
      ! mizu: an interop task is not a value

---

    Code
      ix_read_task(corpus[["err-taskdec-shape"]])
    Condition
      Error in `ix_read_task()`:
      ! mizu: malformed task stream: the code field is not a string

---

    Code
      ix_read_task(corpus[["err-taskdec-list"]])
    Condition
      Error in `ix_read_task()`:
      ! mizu: malformed task stream: the positional field is not a list

---

    Code
      ix_read_task(corpus[["err-taskdec-dict"]])
    Condition
      Error in `ix_read_task()`:
      ! mizu: malformed task stream: the named field is not a dict

