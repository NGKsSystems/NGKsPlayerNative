<!-- markdownlint-disable -->
# NGKsGraph

NGKsGraph is a deterministic Python CLI for Windows C++ builds using Ninja + MSVC.
It scans sources, generates `build/build.ninja`, runs `ninja` with `cl/link`, and can
iteratively apply deterministic repair actions for common MSVC linker/compiler failures.

At configure time it also exports:

- `build/compile_commands.json` for clangd/IDE IntelliSense/tooling
- `build/ngksgraph_graph.json` as a structured target graph snapshot

## Requirements

- Python 3.11+
- Ninja installed and available on `PATH`
- MSVC developer environment (run inside **x64 Native Tools Command Prompt**)

## Quickstart

```powershell
cd examples/hello_msvc
python -m ngksgraph configure
python -m ngksgraph build
python -m ngksgraph run
```

or after install:

```powershell
ngksgraph configure
ngksgraph build
ngksgraph run
```

## Building without opening VS Developer Prompt

From a normal PowerShell session, NGKsGraph can bootstrap MSVC for the build process:

```powershell
python -m ngksgraph configure
python -m ngksgraph build --msvc-auto
python -m ngksgraph run
```

With `--msvc-auto`, NGKsGraph discovers Visual Studio via `vswhere`, captures an
MSVC build environment using `VsDevCmd.bat`, and passes that environment only to
the Ninja subprocess. Your current shell environment is not modified.

## Commands

- `ngksgraph init --template <default|basic|qt-app|multi-target>` - create `ngksgraph.toml` from template
- `ngksgraph import --cmake <path>` - create `ngksgraph.toml` from `CMakeLists.txt` starter mapping
- `ngksgraph configure` - scan sources and generate Ninja/state
- `ngksgraph build` - build with deterministic repair loop
- `ngksgraph build --target <name>` - build a specific target
- `ngksgraph run` - run generated executable
- `ngksgraph clean` - remove output directory
- `ngksgraph doctor` - verify MSVC + Ninja toolchain availability
- `ngksgraph doctor --binary` - verify packaged binary integrity against manifest
- `ngksgraph graph` - export structured build graph JSON
- `ngksgraph explain src/main.cpp` - explain why/how a source file is compiled
- `ngksgraph explain --link` - print exact link command
- `ngksgraph diff` - structural diff between two snapshots
- `ngksgraph trace <path>` - trace impacted targets/executables for a source
- `ngksgraph trace --timing --profile <name>` - print compact timing/cache report
- `ngksgraph freeze` - create deterministic reproducibility capsule
- `ngksgraph thaw <capsule.zip>` - reconstruct generated outputs from a capsule
- `ngksgraph verify <capsule.zip>` - verify capsule hashes
- `ngksgraph why <target>` - explain dependency/rebuild attribution for target
- `ngksgraph rebuild-cause <target>` - classify structural vs command rebuild causes

## Standalone Windows Build

Build a standalone Windows distribution (no pip/venv required for end users):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/package_windows.ps1
```

Smoke test packaged executable:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/smoke_standalone.ps1
```

Package output is written to:

- `artifacts/package/phase10/<timestamp>/`

Integrity checks from packaged binary:

```powershell
.\ngksgraph.exe --version
.\ngksgraph.exe doctor --binary
```

## Multi-target config schema

Use `[[targets]]` to define multiple build targets (for example static libraries and executables):

```toml
out_dir = "build"

[build]
default_target = "app"

[[targets]]
name = "core"
type = "staticlib"
src_glob = ["src/core/**/*.cpp"]
include_dirs = ["src/core"]
links = []

[[targets]]
name = "app"
type = "exe"
src_glob = ["src/app/**/*.cpp"]
include_dirs = ["src/core"]
links = ["core"]
libs = ["user32"]
```

Single-target legacy top-level config remains supported and is upgraded internally.

## Multi-target example

```powershell
cd examples/multi_target_msvc
python -m ngksgraph configure
python -m ngksgraph build --target app --msvc-auto
python -m ngksgraph explain src/core/core.cpp
python -m ngksgraph explain --link --target app
```

## Determinism

- Sorted source list and config collections (`include_dirs`, `defines`, `libs`, `lib_dirs`)
- Stable Ninja rule ordering
- Forward-slash normalized paths

## Temporal + Graph Intelligence (Phase 5)

NGKsGraph now writes deterministic snapshots under `build/.ngksgraph_snapshots/<timestamp>/`.

Each snapshot stores:

- `graph.json` (always)
- `compdb.json` (optional via `[snapshots].write_compdb`)
- `build.ninja` (optional via `[snapshots].write_ninja`)
- `ngksgraph.toml` (optional via `[snapshots].write_config`)
- `meta.json` with stable hashes (`graph_hash`, `compdb_hash`, `ninja_hash`, `config_hash`, `closure_hashes`)

Snapshot retention is controlled by `[snapshots].keep` and oldest snapshots are pruned automatically.

### Diff snapshots

```powershell
ngksgraph diff
ngksgraph diff --json
ngksgraph diff --a 2026-02-26T06-39-21-082Z --b 2026-02-26T06-40-44-645Z
```

The diff includes:

- added/removed/changed targets
- added/removed graph edges
- hash changes
- compile command deltas and reason tokens

### Trace source impact

```powershell
ngksgraph trace src/core/core.cpp
ngksgraph trace src/core/core.cpp --json
```

Trace reports:

- owning target(s) for the source
- reverse-dependency impacted targets
- impacted executable closure

## Reproducibility Capsules (Phase 6B)

Capsules are deterministic portable build-state artifacts:

- default path: `build/ngksgraph_capsules/<timestamp>_<project>_<target>.ngkcapsule.zip`
- deterministic ZIP ordering and fixed archive timestamps
- stable JSON/Ninja newline normalization (`\n`)

Capsule content:

- `capsule_meta.json`
- `graph.json`
- `compdb.json`
- `build.ninja`
- `config.normalized.json`
- `hashes.json`
- `toolchain.json` (sanitized summary only)
- `snapshot_ref.json` (only when freezing from snapshot)

Security/privacy:

- No API keys, tokens, or raw env dumps
- No `PATH=`, `INCLUDE=`, or `LIB=` environment captures
- `toolchain.json` includes only version/path summary fields

### Freeze

```powershell
ngksgraph freeze
ngksgraph freeze --target app
ngksgraph freeze --from-snapshot 2026-02-26T06-40-44-645Z
ngksgraph freeze --out build/ngksgraph_capsules/custom.ngkcapsule.zip
```

### Verify

```powershell
ngksgraph verify build/ngksgraph_capsules/<capsule>.ngkcapsule.zip
```

### Thaw

```powershell
ngksgraph thaw build/ngksgraph_capsules/<capsule>.ngkcapsule.zip
ngksgraph thaw build/ngksgraph_capsules/<capsule>.ngkcapsule.zip --out-dir thawed_build --force
```

Typical usage: freeze a known-good build state before refactors, then verify/thaw later to reproduce generated graph/compdb/Ninja/config outputs.

## Build Forensics (Phase 6A)

Forensics mode explains **why** targets rebuild and **where** dependency/link behavior comes from.

### Why attribution

```powershell
ngksgraph why app
ngksgraph why app --json
ngksgraph why app --from-snapshot 2026-02-26T06-40-44-645Z
ngksgraph why app --from-capsule build/ngksgraph_capsules/<capsule>.ngkcapsule.zip
```

`why` output includes:

- target overview (type, direct links, closure, closure hash)
- direct edge attribution with origin metadata (`config_field`, `field`, `target_index`)
- closure path chains (direct vs indirect vs duplicate-path attribution)
- rebuild reasoning from latest snapshot comparison when baseline exists

### Rebuild cause

```powershell
ngksgraph rebuild-cause app
ngksgraph rebuild-cause app --json
```

`rebuild-cause` separates:

- STRUCTURAL CHANGE (closure hash, field-level root cause)
- COMMAND CHANGE (compdb/ninja deltas + mapped command tokens)
- NO CHANGE (likely timestamp-only or non-structural trigger)

### Symbol-level heuristic

When logs contain unresolved-symbol errors (`LNK2019`, `undefined reference`), forensics performs a lightweight symbol search across sources and suggests missing link edges when symbol ownership appears outside the current closure.

### Capsule forensic mode

`why --from-capsule` verifies capsule hashes first, then analyzes in-memory payloads (no extraction required).

## Qt Toolchain Integration (Phase 6D)

NGKsGraph supports explicit deterministic Qt generation with:

- `moc` for headers containing `Q_OBJECT`
- `uic` for `.ui` files
- `rcc` for `.qrc` files

No implicit CMake-like behavior is used. Qt paths must be explicit.

### Config

```toml
[qt]
enabled = true
moc_path = "C:/Qt/6.6.0/msvc2019_64/bin/moc.exe"
uic_path = "C:/Qt/6.6.0/msvc2019_64/bin/uic.exe"
rcc_path = "C:/Qt/6.6.0/msvc2019_64/bin/rcc.exe"
include_dirs = ["C:/Qt/6.6.0/msvc2019_64/include", "C:/Qt/6.6.0/msvc2019_64/include/QtCore", "C:/Qt/6.6.0/msvc2019_64/include/QtWidgets"]
lib_dirs = ["C:/Qt/6.6.0/msvc2019_64/lib"]
libs = ["Qt6Core.lib", "Qt6Widgets.lib"]
```

If `qt.enabled = true` and a required tool path is missing or invalid, configure fails hard.

### Generated outputs

- `build/qt/moc_<basename>.cpp`
- `build/qt/ui_<basename>.h`
- `build/qt/qrc_<basename>.cpp`

Generator fingerprints include:

- input file hash
- tool binary hash
- tool version (`-v`)
- generator command arguments
- for `rcc`: `.qrc` referenced-file hashes

### Trace and attribution

`ngksgraph trace <path>` includes Qt generator evidence keys:

- `qt.moc.generated` / `qt.moc.skipped`
- `qt.uic.generated` / `qt.uic.skipped`
- `qt.rcc.generated` / `qt.rcc.skipped`
- `qt.generator.reason`
- `qt.generator.tool_hash`
- `qt.include.injected`
- `qt.lib.injected`

### Capsule integration

`freeze` includes generated Qt artifacts and Qt tool provenance (path/hash/version). `verify` fails if Qt tool hash/version mismatches capsule metadata or generated Qt payload hashes differ.

### End-to-end commands

```powershell
ngksgraph configure
ngksgraph build
ngksgraph trace src/main.cpp
ngksgraph freeze
ngksgraph verify build/ngksgraph_capsules/<capsule>.ngkcapsule.zip
```

## AI Repair Plugin Model

AI support is optional and plugin-based.

- AI **never** writes Ninja, edits files directly, or runs shell commands.
- AI can only suggest structured config actions such as:
  - add include directory
  - add library
  - add library directory
  - add define

If AI is enabled, deterministic repair is always attempted first.
