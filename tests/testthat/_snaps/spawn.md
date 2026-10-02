# mizu_py_launcher probes the interpreter for pymizu

    Code
      mizu_py_launcher(python = "/nonexistent/python3")
    Condition
      Error:
      ! mizu: mizu_py_launcher() needs the Python package 'pymizu' installed for /nonexistent/python3

# mizu_py_launcher needs a python3 on the PATH by default

    Code
      out
    Output
      [1] "mizu: mizu_py_launcher() needs python3 on the PATH (or pass python)"

# mizu_py_pool_launcher probes the interpreter for pymizu

    Code
      mizu_py_pool_launcher(python = "/nonexistent/python3")
    Condition
      Error:
      ! mizu: mizu_py_pool_launcher() needs the Python package 'pymizu' installed for /nonexistent/python3

# mizu_py_pool_launcher needs a python3 on the PATH by default

    Code
      out
    Output
      [1] "mizu: mizu_py_pool_launcher() needs python3 on the PATH (or pass python)"

