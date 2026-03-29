# NGKsPlayerNative — Copilot Agent Instructions

## !! WRONG BINARY PATHS — DO NOT LAUNCH THESE !!

The following directories are **legacy/stale build outputs**. Do NOT build to them, do NOT launch binaries from them, do NOT run windeployqt on them.

| ❌ WRONG — never use | Reason |
|---|---|
| `build\debug\bin\NGKsPlayerNative.exe` | Legacy MSVC project build, stale, missing Qt6Sqld.dll → fwrite crash |
| `build\win-msvc-debug\NGKsPlayerNative.exe` | Legacy path, do not use |
| `build\win-msvc-release\NGKsPlayerNative.exe` | Legacy path, do not use |

**Correct binary locations (ngksgraph builds only):**
- Release: `build_graph\release\bin\NGKsPlayerNative.exe`
- Debug (only if explicitly needed): `build_graph\debug\bin\native.exe`

---

## Build Profile
- **ALWAYS use the `release` profile**, not `debug`.
- Correct ngk_build.ps1 verb: `run_native_release`
- Correct ngksgraph invocation: `ngksgraph run --project . --target native --profile release`
- The `default_profile` TOML key is NOT supported. `--profile` is always required when profiles exist.

## Build System (DevFabEco / NGKsBuildCore)

### How to actually compile (critical)
`ngksgraph build` only generates `ngksbuildcore_plan.json` — it does NOT invoke the compiler.

To actually compile, run ngksbuildcore with the bypass flag:
```powershell
$env:NGKS_ALLOW_DIRECT_BUILDCORE = "1"
$py = "C:\Users\suppo\Desktop\NGKsSystems\NGKsDevFabEco\.venv\Scripts\python.exe"
Set-Location "C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
Remove-Item ".ngksbuildcore\state.sqlite" -ErrorAction SilentlyContinue
cmd /d /c "call tools\msvc_x64.cmd && set NGKS_ALLOW_DIRECT_BUILDCORE=1 && `"$py`" -m ngksbuildcore run --plan build_graph\release\ngksbuildcore_plan.json"
```

Without `NGKS_ALLOW_DIRECT_BUILDCORE=1`, ngksbuildcore intercepts the `run` command and delegates
to the DevFabEco orchestrator (which does not compile). Exit code 0 with no compilation is the symptom.

### Proof of compilation
After a successful build, verify:
```powershell
Get-Item "build_graph\release\bin\NGKsPlayerNative.exe" | Select-Object LastWriteTime
```
The timestamp must be recent (today).

## Launch / Test

Use the release binary at `build_graph\release\bin\NGKsPlayerNative.exe`.

```powershell
Set-Location "build_graph\release\bin"
$p = Start-Process -FilePath ".\NGKsPlayerNative.exe" -PassThru
Start-Sleep 6
if ($p.HasExited) { "FAIL exit=$($p.ExitCode)" } else { "PASS PID=$($p.Id)"; $p.Kill() }
```

### Exit code 2 diagnosis
If the app exits with code 2, check `data\runtime\ui_qt.log`:
- `DllProbe=FAIL` → Qt DLLs missing from the exe directory. Run windeployqt.
- `RuntimeDirReady=FAIL` → `data\runtime\` directory missing.
- `LogWritable=FAIL` → log file cannot be written.

## Qt / windeployqt

Qt 6.10.2 at `C:\Qt\6.10.2\msvc2022_64`.

To deploy Qt DLLs after a build:
```powershell
$qt = "C:\Qt\6.10.2\msvc2022_64\bin"
& "$qt\windeployqt.exe" --release "build_graph\release\bin\NGKsPlayerNative.exe"
```

For a debug build (if needed):
```powershell
& "$qt\windeployqt.exe" --debug "build_graph\debug\bin\native.exe"
```
