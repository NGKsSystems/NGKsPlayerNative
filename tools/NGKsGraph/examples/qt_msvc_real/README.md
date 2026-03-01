<!-- markdownlint-disable -->
# qt_msvc_real

Real Qt6 Widgets + MSVC example for NGKsGraph Phase 11A.

## Requirements

- Visual Studio Developer Command Prompt (so `cl` and `link` resolve in PATH)
- Qt installation with MSVC kit (example uses `C:/Qt/6.10.2/msvc2022_64`)

## Commands (no CMake/qmake)

Run from repository root:

```powershell
$env:PYTHONPATH = (Get-Location).Path
Push-Location examples/qt_msvc_real

python -m ngksgraph doctor --toolchain --profile debug
python -m ngksgraph configure --profile debug
python -m ngksgraph build --profile debug
python -m ngksgraph run --profile debug

python -m ngksgraph configure --profile release
python -m ngksgraph build --profile release
python -m ngksgraph run --profile release

Pop-Location
```

Expected outputs:

- `build/debug/bin/app.exe`
- `build/release/bin/app.exe`
- `build/<profile>/ngksgraph_build_report.json`
