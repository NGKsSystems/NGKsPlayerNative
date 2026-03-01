<!-- markdownlint-disable -->
# NGKsGraph Adoption Quickstart

## 1) Start from template

```powershell
ngksgraph init --template basic
# or
ngksgraph init --template qt-app
# or
ngksgraph init --template multi-target
```

## 2) Configure profile builds

```powershell
ngksgraph configure --profile debug
ngksgraph build --profile debug
ngksgraph run --profile debug
```

Use `release` for optimized builds:

```powershell
ngksgraph configure --profile release
ngksgraph build --profile release
```

## 3) Migrate from CMake (starter mapping)

```powershell
ngksgraph import --cmake .
```

This reads `CMakeLists.txt` and writes `ngksgraph.toml` with mapped targets, compile options, defines, include dirs, links/libs, and detected Qt modules.

If `ngksgraph.toml` already exists:

```powershell
ngksgraph import --cmake . --force
```

## 4) Validate contracts

```powershell
ngksgraph doctor --compdb --profile debug
ngksgraph doctor --graph --profile debug
ngksgraph doctor --profiles
```
