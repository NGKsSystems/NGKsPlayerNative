<!-- markdownlint-disable -->
# Error Correction Log — NGKsPlayerNative

Chronological record of failures, root causes, and corrective actions taken.

---

## Incident 1 — Build Not Actually Compiling

**Date:** 2026-03-27  
**Symptom:** `ngksbuildcore run --plan` reports success (exit_code=0) and prints "BuildCore direct run intercepted: delegating to DevFabEco orchestrator" but no obj files are created and the binary is not recompiled.  
**Discovered by:** Watching `main.obj` timestamp after repeated "build" invocations — timestamp never changed.

### Root Cause
`ngksbuildcore`'s `run` subcommand checks `NGKS_ALLOW_DIRECT_BUILDCORE` environment variable. When absent (the default), it delegates to `ngksdevfabric build` which does not invoke the compiler — it only generates plan JSON. No error is raised. Exit code is 0.

In `NGKsBuildCore/ngksbuildcore/cli.py` line 30:
```python
if not _allow_direct_buildcore():
    return _route_to_devfabeco_pipeline(args.plan, proof_root)
```

Additionally, `ngksgraph build` itself **only generates `ngksbuildcore_plan.json`** — it does not compile either. Multiple agents were unaware of this two-step separation.

### Corrective Actions
1. **Bypass env var documented:** `NGKS_ALLOW_DIRECT_BUILDCORE=1` must be set inside the `cmd` shell (not just the PowerShell session) so the Python subprocess inherits it.
2. **Compile invocation:**
   ```powershell
   $py = "C:\Users\suppo\Desktop\NGKsSystems\NGKsDevFabEco\.venv\Scripts\python.exe"
   cmd /d /c "call tools\msvc_x64.cmd && set NGKS_ALLOW_DIRECT_BUILDCORE=1 && `"$py`" -m ngksbuildcore run --plan build_graph\release\ngksbuildcore_plan.json"
   ```
3. **Proof of compilation:** Verify binary timestamp updated after the run. A fresh timestamp on `build_graph\release\bin\NGKsPlayerNative.exe` is the only acceptable proof.
4. **Documented in:** `.github\copilot-instructions.md`

---

## Incident 2 — DLL Probe Always Fails on Debug Binary (Exit Code 2)

**Date:** 2026-03-27  
**Symptom:** `native.exe` exits immediately with code 2. Log shows:
```
DllProbe=FAIL missing=Qt6Core.dll,Qt6Gui.dll,Qt6Qml.dll,Qt6Quick.dll,Qt6Widgets.dll
UiSelfCheck=FAIL reasons=dll_probe_failed
```
**Discovered by:** Reading `data\runtime\ui_qt.log` and cross-referencing with `dumpbin /imports native.exe`.

### Root Cause
`runDllProbe()` in `src/ui/main.cpp` (line 757) had a hardcoded list of **release** Qt DLL names (`Qt6Core.dll`, `Qt6Widgets.dll`, etc.) with no `#ifdef _DEBUG` guard. Debug builds link against `Qt6Cored.dll`, `Qt6Widgetsd.dll`, etc. The probe always failed in debug mode because it looked for names that don't exist on disk.

Additionally, the probe checked `Qt6Qml.dll` and `Qt6Quick.dll` which `native.exe` does not actually import (confirmed via `dumpbin /imports`).

### Corrective Actions
1. **Code fix applied** to `src/ui/main.cpp`:
   ```cpp
   #ifdef _DEBUG
   static const wchar_t* kDllNames[] = {
       L"Qt6Cored.dll", L"Qt6Guid.dll", L"Qt6Sqld.dll",
       L"Qt6Widgetsd.dll", L"vcruntime140d.dll", L"msvcp140d.dll"
   };
   #else
   static const wchar_t* kDllNames[] = {
       L"Qt6Core.dll", L"Qt6Gui.dll", L"Qt6Sql.dll",
       L"Qt6Widgets.dll", L"vcruntime140.dll", L"msvcp140.dll"
   };
   #endif
   ```
2. **Removed** `Qt6Qml.dll` / `Qt6Quick.dll` from probe — not imported by native.exe.
3. **Recompiled** with bypass flag (see Incident 1 fix). Confirmed: `RUNNING PID=41788 -- SUCCESS` (6-second live test).
4. **Exit code 2 diagnosis guide added** to `.github\copilot-instructions.md`.

---

## Incident 3 — Every Agent Uses Debug Profile

**Date:** 2026-03-27 (recurring across multiple prior sessions)  
**Symptom:** Agents always build and launch the debug binary, never release. Persisted across sessions and agents even when instructed otherwise.

### Root Causes (three independent causes — all present simultaneously)

**A. `tasks.json` had no release tasks.**  
Only four debug tasks existed: `NGKsGraph: Build native (debug)`, `NGKsGraph: Run native (debug)`, etc. Agents enumerate available VS Code tasks to determine workflow — the only "run native" option was debug.

**B. `ngksgraph.toml` global defines contained `"DEBUG"` and `"_DEBUG"`.**  
The top-level `defines` array (before any profile section) included debug defines. Agents scanning the TOML for project configuration concluded "this is a debug project." These defines belong only in `[profiles.debug]`, not at the global level.

**C. `README.md` mentioned no profile.**  
The README pointed agents at `ngksplayer_graph_run.ps1` which has no `--profile` parameter, and never stated that release is the required profile.

### Corrective Actions
1. **`tasks.json` restructured:** Added `!! DEFAULT: Build native (RELEASE) !!` and `!! DEFAULT: Run native (RELEASE) !!` as `isDefault: true` tasks in the build/test groups. All debug tasks renamed to append `[DEV ONLY]` suffix.
2. **`ngksgraph.toml` global defines cleaned:** Removed `"DEBUG"` and `"_DEBUG"` from the global `defines` array. Added a comment: `# DEFAULT PROFILE IS RELEASE.` Profile-specific defines remain only inside `[profiles.debug]` and `[profiles.release]`.
3. **Global cflags cleaned:** Removed `/MDd` and `/Od` from global `cflags` (debug-only flags). They remain in `[profiles.debug]`.
4. **`README.md` updated:** Now explicitly states `--profile release` and shows the exact build/run commands. Added a warning table listing legacy binary paths that must not be used.
5. **`.github\copilot-instructions.md` created** with a `!! WRONG BINARY PATHS !!` section at the top.

---

## Incident 4 — Wrong Binary Path Used (Legacy `build\debug\bin\`)

**Date:** 2026-03-27  
**Symptom:** Agent launched `build\debug\bin\NGKsPlayerNative.exe`. App crashed with:
```
Microsoft Visual C++ Runtime Library
Debug Assertion Failed!
File: minkernel\crts\ucrt\src\appcrt\stdio\fwrite.cpp
Line: 72
Expression: buffer != nullptr
```
**Discovered by:** Screenshot from user.

### Root Cause
The `build\` directory tree is a **legacy MSVC project build** output — not produced by ngksgraph. It is stale and missing `Qt6Sqld.dll`. When Qt SQL initializes and `Qt6Sqld.dll` is absent, the Qt SQL module fails to load, corrupting an internal stdio buffer. The subsequent `fwrite` call asserts `buffer != nullptr`.

The agent chose this path likely because the `build\debug\bin\NGKsPlayerNative.exe` filename looks authoritative, and no prior instruction explicitly listed the legacy paths as forbidden.

### Corrective Actions
1. **`.github\copilot-instructions.md`** updated with a hard prohibition table at the very top:
   ```
   ❌ build\debug\bin\NGKsPlayerNative.exe        — DO NOT USE
   ❌ build\win-msvc-debug\NGKsPlayerNative.exe   — DO NOT USE
   ❌ build\win-msvc-release\NGKsPlayerNative.exe — DO NOT USE
   ```
2. **README.md** now contains an explicit "Do NOT use `build\`, `build\debug\`, or `build\win-msvc-*\` paths" warning.
3. **Known issue:** `build\debug\bin\` is missing `Qt6Sqld.dll` — this directory should not be patched; it should be ignored entirely. The correct output directory is `build_graph\release\bin\`.

---

## Incident 5 — BOOTSTRAP_PLACEHOLDER_ONLY Certification Recurrence

**Date:** 2026-03-27 (recurring)  
**Symptom:** Certification state shows `generation_mode: BOOTSTRAP_PLACEHOLDER_ONLY` — cert files contain placeholder markers, not real attestation values. Certification gate falsely appears to pass.

### Root Cause
Agents generating certification artifacts wrote placeholder values (literal strings like `PLACEHOLDER`) instead of running the actual certification workflow. The cert JSON schema did not reject these values. The condition persisted across sessions because no automated check caught it.

### Corrective Actions
1. **Fix script created:** `tools\operator\fix_cert_bootstrap_placeholder.ps1`  
   - Auto-detects `"generation_mode"` marker in cert files  
   - Rewrites 8 cert files with correct values  
   - Renames scenario folder  
   - Triggers real `certify` run  
   - Tested: runs cleanly when no markers present (exits early with "no markers detected")

---

## Standing Rules (derived from all incidents)

These apply to every agent session in this repo without exception:

| Rule | Detail |
|---|---|
| **Always `--profile release`** | Never `--profile debug` unless explicitly doing debug-only work |
| **Compile requires bypass** | `NGKS_ALLOW_DIRECT_BUILDCORE=1` must be set in the cmd shell, not just PowerShell |
| **Verify binary timestamp** | After any build, check `build_graph\release\bin\NGKsPlayerNative.exe` LastWriteTime |
| **Launch test is mandatory** | `Start-Process` + 6-second wait + check `HasExited` before claiming success |
| **Check the log on failure** | `build_graph\release\bin\data\runtime\ui_qt.log` — always check before concluding |
| **`build\` is forbidden** | Never touch `build\`, `build\debug\`, `build\win-msvc-*\` |
| **`ngksgraph build` ≠ compile** | It only generates `ngksbuildcore_plan.json` |
