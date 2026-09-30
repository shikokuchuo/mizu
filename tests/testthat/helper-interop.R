# The corpus notation's R reader: parses tests/testthat/interop-corpus/
# cases.txt values into the R homes the spec's tag table pins, so the
# corpus test compares against spec-authored fixtures, never against the
# writer's own output. Mirrors tools/interop_corpus.py's grammar.

ix_hex_to_raw <- function(hex) {
  as.raw(strtoi(
    substring(hex, seq(1L, nchar(hex), 2L), seq(2L, nchar(hex), 2L)),
    16L
  ))
}

ix_raw_to_hex <- function(r) {
  paste(sprintf("%02x", as.integer(r)), collapse = "")
}

ix_cases <- function() {
  lines <- readLines(
    test_path("interop-corpus", "cases.txt"),
    warn = FALSE,
    encoding = "UTF-8"
  )
  lines <- trimws(lines)
  lines <- lines[nzchar(lines) & !startsWith(lines, "#")]
  parts <- strsplit(lines, "|", fixed = TRUE)
  data.frame(
    id = trimws(vapply(parts, `[[`, "", 1L)),
    kind = trimws(vapply(parts, `[[`, "", 2L)),
    langs = trimws(vapply(parts, `[[`, "", 3L)),
    value = trimws(vapply(parts, `[[`, "", 4L)),
    note = trimws(vapply(
      parts,
      function(p) if (length(p) == 5L) p[[5L]] else "",
      ""
    )),
    stringsAsFactors = FALSE
  )
}

ix_corpus <- function() {
  lines <- readLines(
    test_path("interop-corpus", "corpus.txt"),
    warn = FALSE,
    encoding = "UTF-8"
  )
  lines <- trimws(lines)
  lines <- lines[nzchar(lines) & !startsWith(lines, "#")]
  parts <- strsplit(lines, "|", fixed = TRUE)
  setNames(
    trimws(vapply(parts, `[[`, "", 2L)),
    trimws(vapply(parts, `[[`, "", 1L))
  )
}

ix_bits_to_real <- function(hex) {
  b <- ix_hex_to_raw(hex)
  readBin(rev(b), "double", size = 8L)
}

# The parser state rides an environment: i is the 1-based character
# position in s.
ix_new_parser <- function(text) {
  st <- new.env()
  st$s <- text
  st$i <- 1L
  st
}

ix_ws <- function(st) {
  while (st$i <= nchar(st$s) && substr(st$s, st$i, st$i) %in% c(" ", "\t")) {
    st$i <- st$i + 1L
  }
}

ix_peek <- function(st) {
  ix_ws(st)
  if (st$i > nchar(st$s)) "" else substr(st$s, st$i, st$i)
}

ix_expect <- function(st, ch) {
  ix_ws(st)
  if (ix_peek(st) != ch) {
    stop("expected '", ch, "' at ", st$i)
  }
  st$i <- st$i + 1L
}

ix_token <- function(st, stop) {
  ix_ws(st)
  j <- st$i
  while (st$i <= nchar(st$s) && !substr(st$s, st$i, st$i) %in% stop) {
    st$i <- st$i + 1L
  }
  trimws(substr(st$s, j, st$i - 1L))
}

ix_string <- function(st) {
  ix_expect(st, '"')
  out <- character(0L)
  repeat {
    if (st$i > nchar(st$s)) {
      stop("unterminated string")
    }
    c <- substr(st$s, st$i, st$i)
    st$i <- st$i + 1L
    if (c == '"') {
      return(paste0(out, collapse = ""))
    }
    if (c == "\\") {
      e <- substr(st$s, st$i, st$i)
      st$i <- st$i + 1L
      out <- c(
        out,
        switch(
          e,
          "n" = "\n",
          "t" = "\t",
          "r" = "\r",
          "\\" = "\\",
          "\"" = "\"",
          "u" = {
            code <- substr(st$s, st$i, st$i + 3L)
            st$i <- st$i + 4L
            intToUtf8(strtoi(code, 16L))
          },
          stop("bad escape \\", e)
        )
      )
    } else {
      out <- c(out, c)
    }
  }
}

ix_real_atom <- function(st, stop) {
  tok <- ix_token(st, stop)
  switch(
    tok,
    "na" = NA_real_,
    "nan" = NaN,
    "inf" = Inf,
    "-inf" = -Inf,
    if (startsWith(tok, "bits:")) {
      ix_bits_to_real(substring(tok, 6L))
    } else {
      as.numeric(tok)
    }
  )
}

ix_int_atom <- function(st, stop) {
  tok <- ix_token(st, stop)
  if (tok == "na") {
    return(NA)
  }
  tok
}

# int atoms stay strings until the container knows whether it wants an
# R integer, an integer64, or a double: values past 2^53 need bit64.

ix_value <- function(st) {
  ix_ws(st)
  rest <- substring(st$s, st$i)
  name <- NULL
  for (nm in c(
    "lglv",
    "intv",
    "realv",
    "cplxv",
    "rawv",
    "strv",
    "i64v",
    "list",
    "tuple",
    "dict",
    "attr",
    "lgl",
    "int",
    "real",
    "cplx",
    "str",
    "bytes",
    "err",
    "task",
    "nil"
  )) {
    if (startsWith(rest, nm)) {
      name <- nm
      st$i <- st$i + nchar(nm)
      break
    }
  }
  if (is.null(name)) {
    stop("bad value at ", st$i, " of ", sQuote(st$s))
  }
  switch(
    name,
    "nil" = NULL,
    "lgl" = {
      ix_expect(st, "(")
      v <- switch(ix_token(st, ")"), "0" = FALSE, "1" = TRUE, "na" = NA)
      ix_expect(st, ")")
      v
    },
    "int" = {
      ix_expect(st, "(")
      v <- ix_int_atom(st, ")")
      ix_expect(st, ")")
      if (is.na(v)) {
        NA_integer_
      } else {
        d <- as.numeric(v)
        if (d > -2147483648 && d <= 2147483647) {
          as.integer(d)
        } else {
          bit64::as.integer64(v)
        }
      }
    },
    "real" = {
      ix_expect(st, "(")
      v <- ix_real_atom(st, ")")
      ix_expect(st, ")")
      v
    },
    "cplx" = {
      ix_expect(st, "(")
      re <- ix_real_atom(st, ",")
      ix_expect(st, ",")
      im <- ix_real_atom(st, ")")
      ix_expect(st, ")")
      complex(real = re, imaginary = im)
    },
    "str" = {
      ix_expect(st, "(")
      v <- if (ix_peek(st) == '"') ix_string(st) else NA_character_
      if (ix_peek(st) != '"') {
        ix_token(st, ")")
      }
      ix_expect(st, ")")
      v
    },
    "bytes" = {
      ix_expect(st, "(")
      v <- ix_hex_to_raw(ix_token(st, ")"))
      ix_expect(st, ")")
      v
    },
    "list" = ,
    "tuple" = ix_seq(st, "("),
    "dict" = ix_dict(st),
    "attr" = ix_attr(st),
    "err" = {
      ix_expect(st, "(")
      type <- ix_string(st)
      ix_expect(st, ",")
      msg <- ix_string(st)
      ix_expect(st, ",")
      detail <- ix_string(st)
      fields <- list(message = msg, remote_type = type, detail = detail)
      ix_ws(st)
      if (ix_peek(st) == ",") {
        st$i <- st$i + 1L
        if (ix_token(st, "=") != "index") {
          stop("bad err field")
        }
        ix_expect(st, "=")
        # the wire index is 0-based; the R field adds 1
        fields$index <- as.integer(ix_token(st, ")")) + 1L
      }
      ix_expect(st, ")")
      structure(
        fields,
        class = c("mizu_error_remote", "mizu_error", "error", "condition")
      )
    },
    "task" = {
      ix_expect(st, "(")
      target <- as.integer(ix_token(st, ","))
      ix_expect(st, ",")
      kind <- as.integer(ix_token(st, ","))
      ix_expect(st, ",")
      ident <- as.numeric(ix_token(st, ","))
      ix_expect(st, ",")
      code <- ix_value(st)
      ix_expect(st, ",")
      positional <- ix_value(st)
      ix_expect(st, ",")
      named <- ix_value(st)
      ix_expect(st, ")")
      list(
        target = target,
        kind = kind,
        ident = ident,
        code = code,
        positional = positional,
        named = named
      )
    },
    "strv" = {
      ix_expect(st, "[")
      elts <- character(0L)
      while (ix_peek(st) != "]") {
        if (ix_peek(st) == '"') {
          elts <- c(elts, ix_string(st))
        } else {
          if (ix_token(st, c(",", "]")) != "na") {
            stop("bad strv element")
          }
          elts <- c(elts, NA_character_)
        }
        if (ix_peek(st) == ",") st$i <- st$i + 1L
      }
      ix_expect(st, "]")
      elts
    },
    "lglv" = {
      ix_expect(st, "[")
      elts <- logical(0L)
      while (ix_peek(st) != "]") {
        tok <- ix_token(st, c(",", "]"))
        elts <- c(elts, switch(tok, "0" = FALSE, "1" = TRUE, "na" = NA))
        if (ix_peek(st) == ",") st$i <- st$i + 1L
      }
      ix_expect(st, "]")
      elts
    },
    "intv" = {
      ix_expect(st, "[")
      elts <- integer(0L)
      while (ix_peek(st) != "]") {
        tok <- ix_token(st, c(",", "]"))
        elts <- c(elts, if (tok == "na") NA_integer_ else as.integer(tok))
        if (ix_peek(st) == ",") st$i <- st$i + 1L
      }
      ix_expect(st, "]")
      elts
    },
    "realv" = {
      ix_expect(st, "[")
      elts <- numeric(0L)
      while (ix_peek(st) != "]") {
        elts <- c(elts, ix_real_atom(st, c(",", "]")))
        if (ix_peek(st) == ",") st$i <- st$i + 1L
      }
      ix_expect(st, "]")
      elts
    },
    "cplxv" = {
      ix_expect(st, "[")
      elts <- complex(0L)
      while (ix_peek(st) != "]") {
        if (!startsWith(substring(st$s, st$i), "cplx(")) {
          stop("bad cplxv element")
        }
        st$i <- st$i + 5L
        re <- ix_real_atom(st, ",")
        ix_expect(st, ",")
        im <- ix_real_atom(st, ")")
        ix_expect(st, ")")
        elts <- c(elts, complex(real = re, imaginary = im))
        if (ix_peek(st) == ",") st$i <- st$i + 1L
      }
      ix_expect(st, "]")
      elts
    },
    "rawv" = {
      ix_expect(st, "[")
      elts <- raw(0L)
      while (ix_peek(st) != "]") {
        elts <- c(elts, as.raw(strtoi(ix_token(st, c(",", "]")), 16L)))
        if (ix_peek(st) == ",") st$i <- st$i + 1L
      }
      ix_expect(st, "]")
      elts
    },
    "i64v" = {
      ix_expect(st, "[")
      elts <- character(0L)
      while (ix_peek(st) != "]") {
        tok <- ix_token(st, c(",", "]"))
        elts <- c(elts, if (tok == "na") NA else tok)
        if (ix_peek(st) == ",") st$i <- st$i + 1L
      }
      ix_expect(st, "]")
      bit64::as.integer64(ifelse(is.na(elts), NA_character_, elts))
    },
    stop("unhandled ", name)
  )
}

ix_seq <- function(st, open) {
  ix_expect(st, open)
  items <- list()
  while (ix_peek(st) != ")") {
    items <- c(items, list(ix_value(st)))
    if (ix_peek(st) == ",") st$i <- st$i + 1L
  }
  ix_expect(st, ")")
  items
}

ix_key <- function(st) {
  if (ix_peek(st) == '"') {
    return(ix_string(st))
  }
  ix_token(st, "=")
}

ix_pairs <- function(st) {
  pairs <- list()
  keys <- character(0L)
  while (ix_peek(st) != ")") {
    k <- ix_key(st)
    ix_expect(st, "=")
    keys <- c(keys, k)
    pairs <- c(pairs, list(ix_value(st)))
    if (ix_peek(st) == ",") st$i <- st$i + 1L
  }
  list(keys = keys, values = pairs)
}

ix_dict <- function(st) {
  ix_expect(st, "(")
  p <- ix_pairs(st)
  ix_expect(st, ")")
  # duplicates stand (the write-decline row): the writer rejects them
  setNames(p$values, p$keys)
}

ix_attr <- function(st) {
  ix_expect(st, "(")
  x <- ix_value(st)
  ix_expect(st, ",")
  p <- ix_pairs(st)
  ix_expect(st, ")")
  klass <- NULL
  for (j in seq_along(p$keys)) {
    if (p$keys[[j]] == "class") {
      klass <- p$values[[j]]
    } else {
      attr(x, p$keys[[j]]) <- p$values[[j]]
    }
  }
  if (!is.null(klass)) {
    class(x) <- klass
  }
  x
}

ix_parse <- function(text) {
  st <- ix_new_parser(text)
  v <- ix_value(st)
  ix_ws(st)
  if (st$i <= nchar(text)) {
    stop("trailing notation at ", st$i)
  }
  v
}

# The corpus-facing helpers: read / write through the C hooks.

ix_read <- function(hex) {
  .Call(mizu:::mizu_interop_read_call, ix_hex_to_raw(hex))
}

ix_write_err <- function(x, budget = 240L) {
  ix_raw_to_hex(.Call(mizu:::mizu_interop_write_err_call, x, budget))
}

ix_write <- function(x) {
  if (inherits(x, "mizu_error_remote")) {
    return(ix_write_err(x))
  }
  ix_raw_to_hex(.Call(mizu:::mizu_interop_write_call, x))
}

# Task rows: the exec-hook decode (components, no resolution) and the task
# writer off the spec components.
ix_read_task <- function(hex) {
  .Call(mizu:::mizu_interop_read_task_call, ix_hex_to_raw(hex))
}

ix_write_task <- function(x) {
  ix_raw_to_hex(.Call(
    mizu:::mizu_interop_write_task_call,
    list(x$code, x$kind, x$positional, x$named),
    x$target,
    x$ident
  ))
}
