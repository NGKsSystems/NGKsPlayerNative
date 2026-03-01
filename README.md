<!-- markdownlint-disable -->
# NGKsPlayerNative

Default build flow uses NGKsGraph.

- CMake is deprecated and retained only for migration parity validation.
- Use `powershell -NoProfile -ExecutionPolicy Bypass -File tools\ngksplayer_graph_run.ps1` from repo root.
- Use `powershell -NoProfile -ExecutionPolicy Bypass -File tools\ngksplayer_graph_lock.ps1` to generate a lock proof run.
