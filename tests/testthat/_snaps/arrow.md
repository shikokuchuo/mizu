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

