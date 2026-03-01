<!-- markdownlint-disable -->
# CMake → NGKsGraph Migration Map

## Supported mapping (`ngksgraph import --cmake`)

- `project(<name>)` → project/target naming fallback
- `set(CMAKE_CXX_STANDARD <n>)` → `cxx_std`
- `add_library(name STATIC ...)` → `[[targets]]` staticlib
- `add_executable(name ...)` → `[[targets]]` exe
- `target_include_directories` → target `include_dirs`
- `target_compile_definitions` → target `defines`
- `target_compile_options` → target `cflags`
- `target_link_options` → target `ldflags`
- `target_link_libraries`
  - known imported target names → `links`
  - external libs → `libs`
  - `Qt6::<Module>` → `[qt].modules` and `qt.enabled = true`

## Notes

- Import is a conservative starter conversion, not a full CMake semantic clone.
- Generator expressions (`$<...>`) are skipped.
- Existing `ngksgraph.toml` is never overwritten unless `--force` is set.
- Imported config includes `debug`/`release` profile blocks for immediate profile gates.

## Recommended post-import steps

1. Run `ngksgraph configure --profile debug`
2. Run `ngksgraph build --profile debug`
3. If Qt modules were detected, fill explicit `qt.moc_path`, `qt.uic_path`, `qt.rcc_path`
4. Run `ngksgraph doctor --profiles`
