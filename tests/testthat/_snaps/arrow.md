# declines name the column

    Code
      mizu_send(p$host, dec)
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'd': an Arrow type with no portable home (format 'd:5,2,32') at the Arrow frame)

---

    Code
      mizu_send(p$host, ord)
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'o': an ordered dictionary column has no portable home (polars Enum: cast to pl.Categorical) at the Arrow frame)

---

    Code
      mizu_send(p$host, u8)
    Condition
      Error:
      ! mizu: value is not portable to the peer (Arrow nulls cannot cross in a uint8 column (R raw vectors have no NA) at the Arrow frame)

# dictionary declines name the reason

    Code
      mizu_send(p$host, iv)
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'v': a dictionary column of non-string values has no portable home at the Arrow frame)

---

    Code
      mizu_send(p$host, nl)
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'v': a dictionary column with a null level has no portable home at the Arrow frame)

---

    Code
      mizu_send(p$host, arrow::RecordBatchReader$create(rb1, rb2))
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'f': a dictionary column whose levels differ across batches at the Arrow frame)

# a binary column declines

    Code
      mizu_send(p$host, s)
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'v': an Arrow type with no portable home (format 'z') at the Arrow frame)

# a zero-column table declines

    Code
      mizu_send(p$host, mk())
    Condition
      Error:
      ! mizu: value is not portable to the peer (a zero-column Arrow frame has no portable home at the Arrow frame)

# frame schema declines name the reason

    Code
      mizu_send(p$host, arrow_one_col_stream(sch, nanoarrow::nanoarrow_array_init(sch)))
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'v': a timezone name past 63 bytes has no portable home at the Arrow frame)

---

    Code
      mizu_send(p$host, arrow_one_col_stream(sch, nanoarrow::nanoarrow_array_init(sch)))
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'v': a dictionary column with a non-integer index has no portable home at the Arrow frame)

---

    Code
      mizu_send(p$host, nanoarrow::basic_array_stream(list(parent), schema = sch,
      validate = FALSE))
    Condition
      Error:
      ! mizu: value is not portable to the peer (an Arrow frame with duplicate column names at the Arrow frame)

# frame batch declines name the reason

    Code
      mizu_send(p$host, nanoarrow::basic_array_stream(list(parent), schema = sch,
      validate = FALSE))
    Condition
      Error:
      ! mizu: value is not portable to the peer (an Arrow batch with null struct rows or a struct offset has no portable home at the Arrow frame)

---

    Code
      mizu_send(p$host, nanoarrow::basic_array_stream(list(parent), schema = sch,
      validate = FALSE))
    Condition
      Error:
      ! mizu: value is not portable to the peer (an Arrow batch whose columns differ in length at the Arrow frame)

---

    Code
      mizu_send(p$host, arrow_one_col_stream(nanoarrow::na_string(), s))
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'v': an invalid Arrow string export (a malformed offset or view record) at the Arrow frame)

---

    Code
      mizu_send(p$host, arrow_one_col_stream(sch, a))
    Condition
      Error:
      ! mizu: value is not portable to the peer (column 'v': a dictionary index outside the levels at the Arrow frame)

