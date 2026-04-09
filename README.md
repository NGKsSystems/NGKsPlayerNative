<!-- markdownlint-disable -->
# NGKsPlayerNative

Default build flow uses NGKsGraph. **Always build and run with `--profile release`.**

## Quick start

```powershell
# Build
powershell -NoProfile -ExecutionPolicy Bypass -File tools\build_native_release.ps1

# Run (builds first)
powershell -NoProfile -ExecutionPolicy Bypass -File tools\build_native_release.ps1 -Launch
```

Output binary: `build_graph\release\bin\NGKsPlayerNative.exe`

**Do NOT use `build\`, `build\debug\`, or `build\win-msvc-*\` paths — those are legacy stale outputs.**

- CMake is deprecated and retained only for migration parity validation.
- Use `powershell -NoProfile -ExecutionPolicy Bypass -File tools\ngksplayer_graph_run.ps1` from repo root.
- Use `powershell -NoProfile -ExecutionPolicy Bypass -File tools\ngksplayer_graph_lock.ps1` to generate a lock proof run.
- See `.github\copilot-instructions.md` for agent-specific build instructions including the `NGKS_ALLOW_DIRECT_BUILDCORE` bypass.
