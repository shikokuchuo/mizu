# rei_py_launcher probes the interpreter for pyrei

    Code
      rei_py_launcher(python = "/nonexistent/python3")
    Condition
      Error:
      ! rei: rei_py_launcher() needs the Python package 'pyrei' installed for /nonexistent/python3

# rei_py_launcher needs a python3 on the PATH by default

    Code
      out
    Output
      [1] "rei: rei_py_launcher() needs python3 on the PATH (or pass python)"

