from __future__ import annotations
from ngksgraph.progress import stage, ticker, parse_ninja_line_for_counts
from datetime import datetime, timezone
from copy import deepcopy
import difflib
import json
import os
import shutil
import subprocess
from pathlib import Path
from time import perf_counter
from typing import Any

from ngksgraph.compdb import build_compile_command, build_link_command_for_graph, generate_compile_commands
from ngksgraph.compdb_contract import load_compdb, validate_compdb
from ngksgraph.config import Config, load_config, save_config
from ngksgraph.diff import list_snapshots, structural_diff, summarize_diff
from ngksgraph.graph import BuildGraph, build_graph_from_project
from ngksgraph.graph_contract import compute_structural_graph_hash, validate_graph_integrity
from ngksgraph.hashutil import sha256_json, sha256_text as stable_sha256_text, stable_json_dumps
from ngksgraph.log import write_json, write_text
from ngksgraph.msvc import bootstrap_msvc, has_cl_link, resolve_msvc_toolchain_paths
from ngksgraph.ninja import ninja_target_output, render_ninja
from ngksgraph.plan_cache import (
    build_plan_key,
    build_scan_fingerprint,
    cache_paths,
    clear_profile_cache,
    json_sha,
    read_json_file,
    save_cache_record,
    touch_cache_hit,
)
from ngksgraph.plugins.loader import load_plugin
from ngksgraph.qt import QtGeneratorNode, QtIntegrationResult, integrate_qt
from ngksgraph.repair import (
    apply_action,
    deterministic_fix,
    parse_errors,
    sanitize_for_ai,
    validate_ai_actions,
)
from ngksgraph.sanitize import sanitize_compile_commands, sanitize_graph_dict
from ngksgraph.scan import scan_sources_by_target
from ngksgraph.util import normalize_path, sha256_text


def _paths(repo_root: Path, config: Config) -> dict[str, Path]:
    out_dir = repo_root / config.out_dir
    return {
        "out_dir": out_dir,
        "ninja": out_dir / "build.ninja",
        "compdb": out_dir / "compile_commands.json",
        "graph": out_dir / "ngksgraph_graph.json",
        "state": out_dir / ".ngksgraph_state.json",
        "last_log": out_dir / "ngksgraph_last_log.txt",
        "last_report": out_dir / "ngksgraph_last_report.json",
        "build_report": out_dir / "ngksgraph_build_report.json",
    }


def _upsert_build_report(
    paths: dict[str, Path],
    *,
    profile: str,
    cache_hit: bool | None = None,
    cache_reason: str | None = None,
    durations: dict[str, Any] | None = None,
    plan_key_sha: str | None = None,
    fingerprint_sha: str | None = None,
    compdb_contract_pass: bool | None = None,
    graph_contract_pass: bool | None = None,
    toolchain: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report_path = paths.get("build_report")
    if not isinstance(report_path, Path):
        out_dir = paths.get("out_dir")
        if isinstance(out_dir, Path):
            report_path = out_dir / "ngksgraph_build_report.json"
        else:
            ninja_path = paths.get("ninja")
            if isinstance(ninja_path, Path):
                report_path = ninja_path.parent / "ngksgraph_build_report.json"
            else:
                raise KeyError("build_report")

    report: dict[str, Any] = {}
    if report_path.exists():
        try:
            loaded = json.loads(report_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                report = loaded
        except Exception:
            report = {}

    report["ts"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    report["profile"] = profile
    if cache_hit is not None:
        report["cache_hit"] = bool(cache_hit)
    if cache_reason is not None:
        report["cache_reason"] = str(cache_reason)

    if plan_key_sha is not None or fingerprint_sha is not None:
        report["key_hashes"] = {
            "plan_key_sha": str(plan_key_sha or report.get("key_hashes", {}).get("plan_key_sha", "")),
            "fingerprint_sha": str(fingerprint_sha or report.get("key_hashes", {}).get("fingerprint_sha", "")),
        }

    current_durations = report.get("durations", {})
    if not isinstance(current_durations, dict):
        current_durations = {}
    if durations:
        current_durations.update(durations)
    report["durations"] = current_durations

    contract_summary = report.get("contract_outcomes", {})
    if not isinstance(contract_summary, dict):
        contract_summary = {}
    if compdb_contract_pass is not None:
        contract_summary["compdb_contract_pass"] = bool(compdb_contract_pass)
    if graph_contract_pass is not None:
        contract_summary["graph_contract_pass"] = bool(graph_contract_pass)
    report["contract_outcomes"] = contract_summary

    if toolchain is not None:
        report["toolchain"] = dict(toolchain)

    write_json(report_path, report)
    return report


def _report_base(msvc_auto: bool) -> dict[str, Any]:
    return {
        "msvc_auto": msvc_auto,
        "vswhere_path": None,
        "vs_install_path": None,
        "vsdevcmd_path": None,
        "msvc_env_keys": [],
        "compile_commands_path": None,
        "graph_path": None,
        "graph_targets_count": 0,
        "graph_edges_count": 0,
        "snapshot_path": None,
        "hashes": {},
    }


def _env_get_case_insensitive(env: dict[str, str], key: str) -> str | None:
    for existing_key, value in env.items():
        if existing_key.upper() == key.upper():
            return value
    return None


def _merge_env_case_insensitive(base_env: dict[str, str], overlay_env: dict[str, str]) -> dict[str, str]:
    merged = dict(base_env)
    key_map = {k.upper(): k for k in merged}
    for key, value in overlay_env.items():
        upper = key.upper()
        if upper in key_map:
            merged[key_map[upper]] = value
        else:
            merged[key] = value
            key_map[upper] = key

    overlay_path = _env_get_case_insensitive(overlay_env, "PATH")
    if overlay_path is not None:
        merged["PATH"] = overlay_path
        for key in list(merged.keys()):
            if key != "PATH" and key.upper() == "PATH":
                del merged[key]
    return merged


def _graph_payload(graph: BuildGraph, repo_root: Path, qt_result: QtIntegrationResult | None = None) -> dict[str, Any]:
    body = graph.to_json_dict()
    payload: dict[str, Any] = {
        "schema_version": 1,
        "repo_root": normalize_path(repo_root.resolve()),
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "targets": body["targets"],
        "edges": body["edges"],
        "build_order": body["build_order"],
    }
    if qt_result is not None and qt_result.generator_nodes:
        payload["generator_nodes"] = [
            {
                "kind": node.kind,
                "target": node.target,
                "input": node.input,
                "output": node.output,
                "tool_path": node.tool_path,
                "tool_hash": node.tool_hash,
                "tool_version": node.tool_version,
                "fingerprint": node.fingerprint,
            }
            for node in qt_result.generator_nodes
        ]
        payload["generator_edges"] = [
            {
                "from": node.input,
                "to": node.output,
                "type": f"qt_{node.kind}_generates",
                "origin": {
                    "type": "qt_generator",
                    "generator": node.kind,
                    "target": node.target,
                },
            }
            for node in qt_result.generator_nodes
        ]
    return payload


def _snapshot_root(out_dir: Path) -> Path:
    return out_dir / ".ngksgraph_snapshots"


def _snapshot_now_dir(out_dir: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%fZ")
    return _snapshot_root(out_dir) / stamp


def _target_closure_hashes(graph: BuildGraph) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for target_name in sorted(graph.targets.keys()):
        target = graph.targets[target_name]
        payload = {
            "target": target_name,
            "kind": target.kind,
            "sources": list(target.sources),
            "include_dirs": list(target.include_dirs),
            "defines": list(target.defines),
            "cflags": list(target.cflags),
            "libs": list(target.libs),
            "lib_dirs": list(target.lib_dirs),
            "ldflags": list(target.ldflags),
            "links": list(target.links),
            "closure": graph.link_closure(target_name),
        }
        hashes[target_name] = stable_sha256_text(stable_json_dumps(payload))
    return hashes


def _snapshot_hashes(config_path: Path, graph_payload: dict[str, Any], compdb: list[dict[str, Any]], ninja_text: str, graph: BuildGraph) -> dict[str, Any]:
    config_text = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    graph_for_hash = dict(graph_payload)
    graph_for_hash.pop("generated_at", None)
    return {
        "config_hash": stable_sha256_text(config_text),
        "graph_hash": sha256_json(graph_for_hash),
        "compdb_hash": sha256_json(compdb),
        "ninja_hash": stable_sha256_text(ninja_text),
        "closure_hashes": _target_closure_hashes(graph),
    }


def _prune_snapshots(out_dir: Path, keep: int) -> None:
    root = _snapshot_root(out_dir)
    snaps = list_snapshots(root)
    if len(snaps) <= keep:
        return
    for stale in snaps[: len(snaps) - keep]:
        shutil.rmtree(stale, ignore_errors=True)


def _write_snapshot(
    repo_root: Path,
    config_path: Path,
    config: Config,
    out_dir: Path,
    graph_payload: dict[str, Any],
    compdb: list[dict[str, Any]],
    ninja_text: str,
    graph: BuildGraph,
) -> dict[str, Any]:
    if not config.snapshots.enabled:
        return {"snapshot_path": None, "hashes": _snapshot_hashes(config_path, graph_payload, compdb, ninja_text, graph)}

    snap_dir = _snapshot_now_dir(out_dir)
    snap_dir.mkdir(parents=True, exist_ok=True)

    write_json(snap_dir / "graph.json", graph_payload)
    if config.snapshots.write_compdb:
        write_text(snap_dir / "compdb.json", json.dumps(compdb, indent=2, sort_keys=True))
    if config.snapshots.write_ninja:
        write_text(snap_dir / "build.ninja", ninja_text)
    if config.snapshots.write_config and config_path.exists():
        write_text(snap_dir / "ngksgraph.toml", config_path.read_text(encoding="utf-8"))

    hashes = _snapshot_hashes(config_path, graph_payload, compdb, ninja_text, graph)
    meta = {
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "repo_root": normalize_path(repo_root.resolve()),
        "hashes": hashes,
        "sizes": {
            "targets": len(graph_payload.get("targets", {})),
            "edges": len(graph_payload.get("edges", [])),
            "compile_commands": len(compdb),
        },
    }
    write_json(snap_dir / "meta.json", meta)
    _prune_snapshots(out_dir, config.snapshots.keep)

    return {
        "snapshot_path": normalize_path(snap_dir.resolve()),
        "hashes": hashes,
    }


def latest_diff_summary(out_dir: Path) -> dict[str, Any] | None:
    root = _snapshot_root(out_dir)
    snaps = list_snapshots(root)
    if len(snaps) < 2:
        return None
    try:
        return summarize_diff(structural_diff(snaps[-2], snaps[-1]))
    except Exception:
        return None


def trace_source(
    repo_root: Path,
    config_path: Path,
    source_path: str,
    msvc_auto: bool = False,
    profile: str | None = None,
) -> dict[str, Any]:
    configured = configure_project(repo_root, config_path, msvc_auto=msvc_auto, profile=profile)
    graph: BuildGraph = configured["graph"]

    candidate = normalize_path(source_path)
    if Path(source_path).is_absolute():
        abs_candidate = normalize_path(Path(source_path).resolve())
    else:
        abs_candidate = normalize_path((repo_root / source_path).resolve())

    source_owners: list[str] = []
    for target_name, sources in configured["source_map"].items():
        for src in sources:
            src_abs = normalize_path((repo_root / src).resolve())
            if candidate == src or abs_candidate == src_abs:
                source_owners.append(target_name)
                break

    if not source_owners:
        all_sources: list[str] = []
        for srcs in configured["source_map"].values():
            all_sources.extend(srcs)
        return {
            "status": "NOT_IN_GRAPH",
            "source": source_path,
            "candidates": difflib.get_close_matches(candidate, sorted(set(all_sources)), n=10, cutoff=0.2),
        }

    reverse_deps: dict[str, list[str]] = {name: [] for name in graph.targets.keys()}
    for edge in graph.edges:
        if edge.type == "links_to":
            reverse_deps[edge.to].append(edge.frm)

    impacted: set[str] = set(source_owners)
    queue = list(source_owners)
    while queue:
        current = queue.pop(0)
        for dep in sorted(reverse_deps.get(current, [])):
            if dep not in impacted:
                impacted.add(dep)
                queue.append(dep)

    impacted_exes = sorted([name for name in impacted if graph.targets[name].kind == "exe"])
    qt_trace: dict[str, Any] = {}
    state_path = configured["paths"]["state"]
    if state_path.exists():
        try:
            qt_trace = json.loads(state_path.read_text(encoding="utf-8")).get("qt_trace", {})
        except Exception:
            qt_trace = {}

    return {
        "status": "OK",
        "source": source_path,
        "normalized_source": candidate,
        "owners": sorted(source_owners),
        "impacted_targets": sorted(impacted),
        "impacted_executables": impacted_exes,
        **qt_trace,
    }


def _selected_target(config: Config, explicit_target: str | None) -> str:
    config.normalize()
    if explicit_target:
        config.get_target(explicit_target)
        return explicit_target
    return config.default_target_name()


def _generate_artifacts(
    repo_root: Path,
    config: Config,
    source_map: dict[str, list[str]],
    paths: dict[str, Path],
    msvc_auto: bool,
    qt_result: QtIntegrationResult | None = None,
) -> dict[str, Any]:
    plan_start = perf_counter()
    graph = build_graph_from_project(config, source_map=source_map, msvc_auto=msvc_auto)
    payload = _graph_payload(graph, repo_root, qt_result=qt_result)
    write_json(paths["graph"], payload)
    plan_build_ms = int((perf_counter() - plan_start) * 1000)

    compdb_start = perf_counter()
    compdb = generate_compile_commands(graph, config, str(repo_root.resolve()))
    paths["compdb"].write_text(json.dumps(compdb, indent=2, sort_keys=True), encoding="utf-8")
    emit_compdb_ms = int((perf_counter() - compdb_start) * 1000)

    return {
        "graph": graph,
        "graph_payload": payload,
        "compdb": compdb,
        "plan_build_ms": plan_build_ms,
        "emit_compdb_ms": emit_compdb_ms,
    }


def _validate_configure_contracts(paths: dict[str, Path], graph: BuildGraph, config: Config) -> tuple[bool, bool, list[dict[str, Any]]]:
    compdb_entries = load_compdb(paths["compdb"])
    compdb_violations = validate_compdb(compdb_entries, graph, config)
    graph_violations = validate_graph_integrity(graph, config, paths["out_dir"])
    return (len(compdb_violations) == 0, len(graph_violations) == 0, compdb_violations + graph_violations)


def _apply_cached_target_overrides(config: Config, plan: dict[str, Any]) -> None:
    overrides = plan.get("target_overrides", {}) if isinstance(plan.get("target_overrides", {}), dict) else {}
    for target in config.targets:
        body = overrides.get(target.name, {}) if isinstance(overrides.get(target.name, {}), dict) else {}
        if "include_dirs" in body and isinstance(body["include_dirs"], list):
            target.include_dirs = list(body["include_dirs"])
        if "lib_dirs" in body and isinstance(body["lib_dirs"], list):
            target.lib_dirs = list(body["lib_dirs"])
        if "libs" in body and isinstance(body["libs"], list):
            target.libs = list(body["libs"])
        if "defines" in body and isinstance(body["defines"], list):
            target.defines = list(body["defines"])
        if "cflags" in body and isinstance(body["cflags"], list):
            target.cflags = list(body["cflags"])
        if "ldflags" in body and isinstance(body["ldflags"], list):
            target.ldflags = list(body["ldflags"])
        target.normalize()


def _qt_result_from_plan(plan: dict[str, Any]) -> QtIntegrationResult:
    raw_nodes = plan.get("generator_nodes", []) if isinstance(plan.get("generator_nodes", []), list) else []
    nodes: list[QtGeneratorNode] = []
    for item in raw_nodes:
        if not isinstance(item, dict):
            continue
        nodes.append(
            QtGeneratorNode(
                kind=str(item.get("kind", "")),
                target=str(item.get("target", "")),
                input=str(item.get("input", "")),
                output=str(item.get("output", "")),
                status="skipped",
                reason="qt.generator.reason.unchanged",
                tool_path=str(item.get("tool_path", "")),
                tool_hash=str(item.get("tool_hash", "")),
                tool_version=str(item.get("tool_version", "")),
                fingerprint=str(item.get("fingerprint", "")),
            )
        )

    qt_trace = plan.get("qt_trace", {}) if isinstance(plan.get("qt_trace", {}), dict) else {}
    qt_lib = qt_trace.get("qt.lib.injected", {}) if isinstance(qt_trace.get("qt.lib.injected", {}), dict) else {}
    return QtIntegrationResult(
        generator_nodes=nodes,
        generated_files=list(plan.get("qt_generated_files", [])) if isinstance(plan.get("qt_generated_files", []), list) else [],
        include_injected=list(qt_trace.get("qt.include.injected", [])) if isinstance(qt_trace.get("qt.include.injected", []), list) else [],
        lib_dirs_injected=list(qt_lib.get("lib_dirs", [])) if isinstance(qt_lib.get("lib_dirs", []), list) else [],
        libs_injected=list(qt_lib.get("libs", [])) if isinstance(qt_lib.get("libs", []), list) else [],
        tool_info=dict(plan.get("qt_tool_info", {})) if isinstance(plan.get("qt_tool_info", {}), dict) else {},
    )


def _build_plan_payload(config: Config, source_map: dict[str, list[str]], selected_target: str, qt_result: QtIntegrationResult) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "profile": "",
        "selected_target": selected_target,
        "source_map": {k: list(v) for k, v in sorted(source_map.items())},
        "target_overrides": {
            target.name: {
                "include_dirs": list(target.include_dirs),
                "lib_dirs": list(target.lib_dirs),
                "libs": list(target.libs),
                "defines": list(target.defines),
                "cflags": list(target.cflags),
                "ldflags": list(target.ldflags),
            }
            for target in sorted(config.targets, key=lambda t: t.name)
        },
        "qt_trace": qt_result.trace_dict(),
        "qt_generated_files": list(qt_result.generated_files),
        "qt_tool_info": dict(qt_result.tool_info),
        "generator_nodes": [node.to_json() for node in qt_result.generator_nodes],
    }


def inspect_plan_cache(
    repo_root: Path,
    config_path: Path,
    profile: str | None,
    target: str | None,
) -> dict[str, Any]:
    config = load_config(config_path)
    selected_profile = config.apply_profile(profile)
    selected_target = _selected_target(config, target)
    paths = cache_paths(repo_root, selected_profile)

    plan_key, key_status = read_json_file(paths["plan_key"])
    fingerprint, fp_status = read_json_file(paths["fingerprint"])
    plan, plan_status = read_json_file(paths["plan"])
    if key_status == "corrupt" or fp_status == "corrupt" or plan_status == "corrupt":
        return {
            "cache": "MISS",
            "reason": "CORRUPT",
            "corrupt": True,
            "profile": selected_profile,
            "key_sha": "",
            "fingerprint_sha": "",
        }
    if key_status == "missing" or fp_status == "missing" or plan_status == "missing":
        return {
            "cache": "MISS",
            "reason": "NO_CACHE",
            "corrupt": False,
            "profile": selected_profile,
            "key_sha": "",
            "fingerprint_sha": "",
        }

    source_map = scan_sources_by_target(repo_root, config)
    current_fingerprint = build_scan_fingerprint(repo_root, config, source_map)
    current_fingerprint_sha = json_sha(current_fingerprint)
    cached_fingerprint_sha = str(paths["fingerprint_sha"].read_text(encoding="utf-8").strip()) if paths["fingerprint_sha"].exists() else ""
    if cached_fingerprint_sha != current_fingerprint_sha:
        return {
            "cache": "MISS",
            "reason": "FINGERPRINT_CHANGED",
            "corrupt": False,
            "profile": selected_profile,
            "key_sha": "",
            "fingerprint_sha": current_fingerprint_sha,
        }

    if not isinstance(plan, dict) or "source_map" not in plan:
        return {
            "cache": "MISS",
            "reason": "CORRUPT",
            "corrupt": True,
            "profile": selected_profile,
            "key_sha": "",
            "fingerprint_sha": current_fingerprint_sha,
        }

    _apply_cached_target_overrides(config, plan)
    cached_source_map = {k: list(v) for k, v in dict(plan.get("source_map", {})).items()}
    graph = build_graph_from_project(config, source_map=cached_source_map, msvc_auto=False)
    structural_hash = compute_structural_graph_hash(graph)
    current_key = build_plan_key(config_path, selected_profile, selected_target, structural_hash, config)
    current_key_sha = json_sha(current_key)
    cached_key_sha = str(paths["plan_key_sha"].read_text(encoding="utf-8").strip()) if paths["plan_key_sha"].exists() else ""

    if current_key_sha != cached_key_sha:
        return {
            "cache": "MISS",
            "reason": "KEY_CHANGED",
            "corrupt": False,
            "profile": selected_profile,
            "key_sha": current_key_sha,
            "fingerprint_sha": current_fingerprint_sha,
        }

    return {
        "cache": "HIT",
        "reason": "OK",
        "corrupt": False,
        "profile": selected_profile,
        "key_sha": current_key_sha,
        "fingerprint_sha": current_fingerprint_sha,
    }


def configure_project(
    repo_root: Path,
    config_path: Path,
    msvc_auto: bool = False,
    target: str | None = None,
    profile: str | None = None,
    no_cache: bool = False,
    clear_cache: bool = False,
) -> dict[str, Any]:
    total_started = perf_counter()
    durations: dict[str, Any] = {
        "load_config_ms": 0,
        "scan_tree_ms": 0,
        "qt_detect_ms": 0,
        "plan_build_ms": 0,
        "emit_ninja_ms": 0,
        "emit_compdb_ms": 0,
        "validate_contracts_ms": 0,
        "total_configure_ms": 0,
        "ninja_build_ms": None,
        "total_build_ms": None,
    }

    load_started = perf_counter()
    config = load_config(config_path)
    durations["load_config_ms"] = int((perf_counter() - load_started) * 1000)
    selected_profile = config.apply_profile(profile)

    if clear_cache:
        clear_profile_cache(repo_root, selected_profile)

    selected = _selected_target(config, target)
    paths = _paths(repo_root, config)
    paths["out_dir"].mkdir(parents=True, exist_ok=True)
    tool_paths = resolve_msvc_toolchain_paths(dict(os.environ))
    msvc_tools = {
        "cl": tool_paths.cl_path,
        "link": tool_paths.link_path,
        "lib": tool_paths.lib_path,
        "rc": tool_paths.rc_path,
    }

    cache_state = {
        "cache_hit": False,
        "cache_reason": "NO_CACHE",
        "plan_key_sha": "",
        "fingerprint_sha": "",
    }

    cache_files = cache_paths(repo_root, selected_profile)
    plan_data, plan_status = read_json_file(cache_files["plan"])

    if not no_cache and plan_status is None and isinstance(plan_data, dict):
        scan_started = perf_counter()
        source_map_probe = scan_sources_by_target(repo_root, config)
        probe_fingerprint = build_scan_fingerprint(repo_root, config, source_map_probe)
        probe_fingerprint_sha = json_sha(probe_fingerprint)
        durations["scan_tree_ms"] += int((perf_counter() - scan_started) * 1000)
        cache_state["fingerprint_sha"] = probe_fingerprint_sha

        cached_fingerprint_sha = cache_files["fingerprint_sha"].read_text(encoding="utf-8").strip() if cache_files["fingerprint_sha"].exists() else ""
        if cached_fingerprint_sha == probe_fingerprint_sha:
            config_for_probe = deepcopy(config)
            _apply_cached_target_overrides(config_for_probe, plan_data)
            cached_source_map = {k: list(v) for k, v in dict(plan_data.get("source_map", {})).items()}
            graph_for_key = build_graph_from_project(config_for_probe, source_map=cached_source_map, msvc_auto=msvc_auto)
            key_payload = build_plan_key(
                config_path=config_path,
                profile=selected_profile,
                selected_target=selected,
                structural_graph_hash=compute_structural_graph_hash(graph_for_key),
                config=config_for_probe,
            )
            key_sha = json_sha(key_payload)
            cache_state["plan_key_sha"] = key_sha
            cached_key_sha = cache_files["plan_key_sha"].read_text(encoding="utf-8").strip() if cache_files["plan_key_sha"].exists() else ""

            if key_sha == cached_key_sha:
                _apply_cached_target_overrides(config, plan_data)
                qt_result = _qt_result_from_plan(plan_data)
                artifacts = _generate_artifacts(repo_root, config, cached_source_map, paths, msvc_auto=msvc_auto, qt_result=qt_result)
                durations["qt_detect_ms"] += 0
                durations["plan_build_ms"] += int(artifacts.get("plan_build_ms", 0))
                durations["emit_compdb_ms"] += int(artifacts.get("emit_compdb_ms", 0))
                ninja_started = perf_counter()
                ninja_text = render_ninja(
                    config,
                    cached_source_map.get(selected, []),
                    graph=artifacts["graph"],
                    default_target_name=selected,
                    qt_result=qt_result,
                    repo_root=repo_root,
                    msvc_tools=msvc_tools,
                )
                write_text(paths["ninja"], ninja_text)
                durations["emit_ninja_ms"] += int((perf_counter() - ninja_started) * 1000)
                snapshot_info = _write_snapshot(
                    repo_root=repo_root,
                    config_path=config_path,
                    config=config,
                    out_dir=paths["out_dir"],
                    graph_payload=artifacts["graph_payload"],
                    compdb=artifacts["compdb"],
                    ninja_text=ninja_text,
                    graph=artifacts["graph"],
                )

                validate_started = perf_counter()
                compdb_pass, graph_pass, violations = _validate_configure_contracts(paths, artifacts["graph"], config)
                durations["validate_contracts_ms"] += int((perf_counter() - validate_started) * 1000)
                if compdb_pass and graph_pass:
                    cache_state["cache_hit"] = True
                    cache_state["cache_reason"] = "HIT"
                    touch_cache_hit(repo_root, selected_profile)
                    durations["total_configure_ms"] = int((perf_counter() - total_started) * 1000)

                    toolchain_info = {
                        "cl_path": shutil.which("cl") or "",
                        "link_path": shutil.which("link") or "",
                        "qt_tools": dict(qt_result.tool_info),
                        "qt_enabled": bool(config.qt.enabled),
                        "qt_root": str(config.qt.qt_root),
                    }

                    _upsert_build_report(
                        paths,
                        profile=selected_profile,
                        cache_hit=True,
                        cache_reason="HIT",
                        durations=durations,
                        plan_key_sha=cache_state["plan_key_sha"],
                        fingerprint_sha=cache_state["fingerprint_sha"],
                        compdb_contract_pass=True,
                        graph_contract_pass=True,
                        toolchain=toolchain_info,
                    )

                    state = {
                        "config_path": str(config_path),
                        "profile": selected_profile,
                        "sources_by_target": cached_source_map,
                        "ninja_sha256": sha256_text(ninja_text),
                        "compile_commands_path": normalize_path(paths["compdb"].resolve()),
                        "graph_path": normalize_path(paths["graph"].resolve()),
                        "graph_targets_count": len(artifacts["graph"].targets),
                        "graph_edges_count": len(artifacts["graph"].edges),
                        "snapshot_path": snapshot_info["snapshot_path"],
                        "hashes": snapshot_info["hashes"],
                        "qt_trace": qt_result.trace_dict(),
                        "qt_generated_files": qt_result.generated_files,
                        "qt_tool_info": qt_result.tool_info,
                        "repairs": [],
                        "cache_hit": True,
                        "cache_reason": "HIT",
                        "plan_key_sha": cache_state["plan_key_sha"],
                        "fingerprint_sha": cache_state["fingerprint_sha"],
                        "compdb_contract_pass": True,
                        "graph_contract_pass": True,
                        "durations": durations,
                    }
                    write_json(paths["state"], state)

                    return {
                        "ok": True,
                        "config": config,
                        "profile": selected_profile,
                        "source_map": cached_source_map,
                        "selected_target": selected,
                        "graph": artifacts["graph"],
                        "graph_payload": artifacts["graph_payload"],
                        "compdb": artifacts["compdb"],
                        "snapshot_info": snapshot_info,
                        "qt_result": qt_result,
                        "paths": paths,
                        **cache_state,
                        "compdb_contract_pass": True,
                        "graph_contract_pass": True,
                        "durations": durations,
                    }
                cache_state["cache_reason"] = "CACHE_CONTRACT_FAIL"
            else:
                cache_state["cache_reason"] = "KEY_CHANGED"
        else:
            cache_state["cache_reason"] = "FINGERPRINT_CHANGED"
    elif not no_cache and plan_status == "corrupt":
        cache_state["cache_reason"] = "CORRUPT_PLAN"
    elif no_cache:
        cache_state["cache_reason"] = "DISABLED"

    scan_started = perf_counter()
    source_map = scan_sources_by_target(repo_root, config)
    durations["scan_tree_ms"] += int((perf_counter() - scan_started) * 1000)

    owner_map: dict[str, list[str]] = {}
    for target_name, sources in source_map.items():
        for src in sources:
            key = normalize_path(src)
            owner_map.setdefault(key, []).append(target_name)
    ambiguous = {src: sorted(set(owners)) for src, owners in owner_map.items() if len(set(owners)) > 1}
    if ambiguous:
        details = "; ".join(f"{src} -> {', '.join(owners)}" for src, owners in sorted(ambiguous.items()))
        raise ValueError(f"AMBIGUOUS_OWNERSHIP: {details}")

    qt_started = perf_counter()
    qt_result = integrate_qt(repo_root, config, source_map, paths["out_dir"])
    durations["qt_detect_ms"] += int((perf_counter() - qt_started) * 1000)

    toolchain_info = {
        "cl_path": shutil.which("cl") or "",
        "link_path": shutil.which("link") or "",
        "qt_tools": dict(qt_result.tool_info),
        "qt_enabled": bool(config.qt.enabled),
        "qt_root": str(config.qt.qt_root),
    }

    artifacts = _generate_artifacts(repo_root, config, source_map, paths, msvc_auto=msvc_auto, qt_result=qt_result)
    durations["plan_build_ms"] += int(artifacts.get("plan_build_ms", 0))
    durations["emit_compdb_ms"] += int(artifacts.get("emit_compdb_ms", 0))
    ninja_started = perf_counter()
    ninja_text = render_ninja(
        config,
        source_map.get(selected, []),
        graph=artifacts["graph"],
        default_target_name=selected,
        qt_result=qt_result,
        repo_root=repo_root,
        msvc_tools=msvc_tools,
    )
    write_text(paths["ninja"], ninja_text)
    durations["emit_ninja_ms"] += int((perf_counter() - ninja_started) * 1000)
    snapshot_info = _write_snapshot(
        repo_root=repo_root,
        config_path=config_path,
        config=config,
        out_dir=paths["out_dir"],
        graph_payload=artifacts["graph_payload"],
        compdb=artifacts["compdb"],
        ninja_text=ninja_text,
        graph=artifacts["graph"],
    )

    fingerprint = build_scan_fingerprint(repo_root, config, source_map)
    fingerprint_sha = json_sha(fingerprint)
    structural_hash = compute_structural_graph_hash(artifacts["graph"])
    plan_key = build_plan_key(config_path, selected_profile, selected, structural_hash, config)
    cache_state["plan_key_sha"] = json_sha(plan_key)
    if not no_cache:
        plan_payload = _build_plan_payload(config, source_map, selected, qt_result)
        plan_payload["profile"] = selected_profile
        key_info = save_cache_record(repo_root, selected_profile, plan_payload, plan_key, fingerprint)
        cache_state["plan_key_sha"] = key_info["plan_key_sha"]
    cache_state["fingerprint_sha"] = fingerprint_sha

    validate_started = perf_counter()
    compdb_pass, graph_pass, _ = _validate_configure_contracts(paths, artifacts["graph"], config)
    durations["validate_contracts_ms"] += int((perf_counter() - validate_started) * 1000)
    durations["total_configure_ms"] = int((perf_counter() - total_started) * 1000)

    _upsert_build_report(
        paths,
        profile=selected_profile,
        cache_hit=False,
        cache_reason=cache_state["cache_reason"],
        durations=durations,
        plan_key_sha=cache_state["plan_key_sha"],
        fingerprint_sha=cache_state["fingerprint_sha"],
        compdb_contract_pass=compdb_pass,
        graph_contract_pass=graph_pass,
        toolchain=toolchain_info,
    )

    state = {
        "config_path": str(config_path),
        "profile": selected_profile,
        "sources_by_target": source_map,
        "ninja_sha256": sha256_text(ninja_text),
        "compile_commands_path": normalize_path(paths["compdb"].resolve()),
        "graph_path": normalize_path(paths["graph"].resolve()),
        "graph_targets_count": len(artifacts["graph"].targets),
        "graph_edges_count": len(artifacts["graph"].edges),
        "snapshot_path": snapshot_info["snapshot_path"],
        "hashes": snapshot_info["hashes"],
        "qt_trace": qt_result.trace_dict(),
        "qt_generated_files": qt_result.generated_files,
        "qt_tool_info": qt_result.tool_info,
        "repairs": [],
        "cache_hit": False,
        "cache_reason": cache_state["cache_reason"],
        "plan_key_sha": cache_state["plan_key_sha"],
        "fingerprint_sha": cache_state["fingerprint_sha"],
        "compdb_contract_pass": compdb_pass,
        "graph_contract_pass": graph_pass,
        "durations": durations,
    }
    write_json(paths["state"], state)

    return {
        "ok": True,
        "config": config,
        "profile": selected_profile,
        "source_map": source_map,
        "selected_target": selected,
        "graph": artifacts["graph"],
        "graph_payload": artifacts["graph_payload"],
        "compdb": artifacts["compdb"],
        "snapshot_info": snapshot_info,
        "qt_result": qt_result,
        "paths": paths,
        **cache_state,
        "compdb_contract_pass": compdb_pass,
        "graph_contract_pass": graph_pass,
        "durations": durations,
    }


def _run_ninja(
    repo_root: Path,
    ninja_file: Path,
    env: dict[str, str] | None = None,
    ninja_target: str | None = None,
) -> tuple[int, str]:
    ninja_dir = ninja_file.parent.resolve()
    use_chdir_mode = False
    try:
        rel_dir = ninja_dir.relative_to(repo_root.resolve())
        use_chdir_mode = len(rel_dir.parts) > 1
    except Exception:
        use_chdir_mode = False

    cmd = ["ninja", "-C", str(ninja_dir)] if use_chdir_mode else ["ninja", "-f", str(ninja_file)]
    if ninja_target:
        target_path = Path(str(ninja_target))
        if use_chdir_mode:
            if not target_path.is_absolute():
                abs_target = (repo_root / target_path).resolve()
            else:
                abs_target = target_path.resolve()
            cmd.append(normalize_path(Path(os.path.relpath(str(abs_target), str(ninja_dir)))))
        else:
            if not target_path.is_absolute():
                cmd.append(normalize_path(target_path))
            else:
                cmd.append(normalize_path(target_path.resolve()))

    proc = subprocess.run(
        cmd,
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        shell=False,
    )
    out = (proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")
    return proc.returncode, out


def _update_state_repairs(state_path: Path, repairs: list[dict[str, Any]]) -> None:
    if not state_path.exists():
        return
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return
    state["repairs"] = repairs
    write_json(state_path, state)


def _find_failing_file(output: str) -> str | None:
    for line in output.splitlines():
        if "CXX" in line and ".cpp" in line.lower():
            suffix = line.split("CXX", 1)[-1].strip()
            if suffix:
                return normalize_path(suffix)
    return None


def _target_owner_for_source(source_map: dict[str, list[str]], source: str) -> str | None:
    normalized = normalize_path(source)
    owners: list[str] = []
    for target_name, sources in source_map.items():
        for src in sources:
            if normalize_path(src) == normalized:
                owners.append(target_name)
    if not owners:
        return None
    return owners[0]


def build_project(
    repo_root: Path,
    config_path: Path,
    max_attempts: int = 5,
    msvc_auto: bool = False,
    target: str | None = None,
    profile: str | None = None,
) -> int:
    # Progress / visibility (stdout + any stage sink you already have)
    stage(
        "BUILD_START",
        repo=str(repo_root),
        config=str(config_path),
        target=target or "",
        profile=profile or "",
        max_attempts=int(max_attempts),
        msvc_auto=bool(msvc_auto),
    )

    build_started = perf_counter()

    stage("CONFIG_LOAD_BEGIN")
    config = load_config(config_path)
    stage("CONFIG_LOAD_OK")

    stage("PROFILE_APPLY_BEGIN", profile=profile or "")
    selected_profile = config.apply_profile(profile)
    stage("PROFILE_APPLY_OK", selected_profile=selected_profile)

    stage("PATHS_RESOLVE_BEGIN")
    paths = _paths(repo_root, config)
    stage(
        "PATHS_RESOLVE_OK",
        out_dir=str(paths.get("out_dir", "")),
        last_log=str(paths.get("last_log", "")),
        last_report=str(paths.get("last_report", "")),
    )

    history: list[dict[str, Any]] = []

    stage("PLUGIN_LOAD_BEGIN", ai_enabled=bool(getattr(config.ai, "enabled", False)))
    plugin = load_plugin(config.ai)
    stage("PLUGIN_LOAD_OK")

    report_base = _report_base(msvc_auto)
    accumulated_ninja_ms = 0
    ran_ninja = False

    merged_env: dict[str, str] | None = None
    if msvc_auto and not has_cl_link(dict(os.environ)):
        stage("MSVC_AUTO_BOOTSTRAP_BEGIN")
        boot = bootstrap_msvc()
        report_base["vswhere_path"] = boot.vswhere_path
        report_base["vs_install_path"] = boot.vs_install_path
        report_base["vsdevcmd_path"] = boot.vsdevcmd_path
        report_base["msvc_env_keys"] = sorted(boot.env.keys())

        if not boot.success:
            msg = f"MSVC auto-bootstrap failed: {boot.error}"
            stage("MSVC_AUTO_BOOTSTRAP_FAIL", error=str(boot.error))

            write_text(paths["last_log"], msg)
            write_json(
                paths["last_report"],
                {
                    **report_base,
                    "ok": False,
                    "attempts": 0,
                    "history": history,
                    "errors": [{"type": "MSVC_BOOTSTRAP", "line": msg}],
                },
            )
            _upsert_build_report(
                paths,
                profile=selected_profile,
                durations={
                    "ninja_build_ms": None,
                    "total_build_ms": int((perf_counter() - build_started) * 1000),
                },
            )
            stage("BUILD_EARLY_EXIT", rc=1, reason="MSVC_BOOTSTRAP_FAIL")
            return 1

        merged_env = _merge_env_case_insensitive(dict(os.environ), boot.env)
        stage("MSVC_AUTO_BOOTSTRAP_OK")

    active_env = merged_env
    env_for_tools = active_env if active_env is not None else dict(os.environ)

    stage("MSVC_ENV_CHECK_BEGIN")
    if not has_cl_link(env_for_tools):
        msg = "MSVC tools missing. Run in x64 Native Tools prompt or pass --msvc-auto."
        stage("MSVC_ENV_CHECK_FAIL")

        write_text(paths["last_log"], msg)
        write_json(
            paths["last_report"],
            {
                **report_base,
                "ok": False,
                "attempts": 0,
                "history": history,
                "errors": [{"type": "MSVC_MISSING", "line": msg}],
            },
        )
        _upsert_build_report(
            paths,
            profile=selected_profile,
            durations={
                "ninja_build_ms": None,
                "total_build_ms": int((perf_counter() - build_started) * 1000),
            },
        )
        stage("BUILD_EARLY_EXIT", rc=1, reason="MSVC_MISSING")
        return 1

    stage("MSVC_ENV_CHECK_OK")

    for attempt in range(1, max_attempts + 1):
        if profile is None:
            configured = configure_project(repo_root, config_path, msvc_auto=msvc_auto, target=target)
        else:
            configured = configure_project(repo_root, config_path, msvc_auto=msvc_auto, target=target, profile=profile)
        selected_target = configured["selected_target"]
        graph: BuildGraph = configured["graph"]
        ninja_target = ninja_target_output(graph, selected_target)

        report_base["compile_commands_path"] = normalize_path(configured["paths"]["compdb"].resolve())
        report_base["graph_path"] = normalize_path(configured["paths"]["graph"].resolve())
        report_base["graph_targets_count"] = len(graph.targets)
        report_base["graph_edges_count"] = len(graph.edges)
        snapshot_info = configured.get("snapshot_info", {})
        report_base["snapshot_path"] = snapshot_info.get("snapshot_path")
        report_base["hashes"] = snapshot_info.get("hashes", {})

        ninja_started = perf_counter()
        retcode, output = _run_ninja(repo_root, configured["paths"]["ninja"], env=active_env, ninja_target=ninja_target)
        elapsed_ninja = int((perf_counter() - ninja_started) * 1000)
        accumulated_ninja_ms += elapsed_ninja
        ran_ninja = True
        write_text(paths["last_log"], output)

        if retcode == 0:
            write_json(
                paths["last_report"],
                {
                    **report_base,
                    "ok": True,
                    "attempts": attempt,
                    "target": selected_target,
                    "history": history,
                },
            )
            _upsert_build_report(
                configured["paths"],
                profile=str(configured.get("profile", selected_profile)),
                cache_hit=configured.get("cache_hit"),
                cache_reason=configured.get("cache_reason"),
                plan_key_sha=configured.get("plan_key_sha"),
                fingerprint_sha=configured.get("fingerprint_sha"),
                compdb_contract_pass=configured.get("compdb_contract_pass"),
                graph_contract_pass=configured.get("graph_contract_pass"),
                durations={
                    "ninja_build_ms": accumulated_ninja_ms if ran_ninja else None,
                    "total_build_ms": int((perf_counter() - build_started) * 1000),
                },
            )
            return 0

        errors = parse_errors(output)
        failing_file = _find_failing_file(output)
        owner_target = _target_owner_for_source(configured["source_map"], failing_file) if failing_file else None
        repair_scope_target = owner_target or selected_target

        fix = deterministic_fix(config, errors, repo_root, target_name=repair_scope_target)
        if fix:
            history.append({"attempt": attempt, **fix, "target": repair_scope_target})
            _update_state_repairs(paths["state"], history)
            save_config(config_path, config)
            continue

        ai_applied: list[dict[str, Any]] = []
        ai_notes = ""
        if config.ai.enabled:
            tail = "\n".join(output.splitlines()[-config.ai.log_tail_lines :])
            context = sanitize_for_ai(repo_root, config, errors, tail, attempt)
            context["target_name"] = selected_target
            context["link_closure"] = graph.link_closure(selected_target)
            context["diff_summary"] = latest_diff_summary(paths["out_dir"])

            compdb = configured["compdb"]
            if failing_file:
                failing_name = Path(failing_file).name
                excerpt = [entry for entry in compdb if Path(entry.get("file", "")).name == failing_name]
            else:
                excerpt = compdb[:5]

            if config.ai.redact_paths:
                out_abs = normalize_path((repo_root / config.out_dir).resolve())
                context["graph_json"] = sanitize_graph_dict(
                    configured["graph_payload"],
                    repo_root=normalize_path(repo_root.resolve()),
                    out_dir=out_abs,
                )
                context["compile_commands_excerpt"] = sanitize_compile_commands(excerpt)
            else:
                context["repo_root"] = normalize_path(repo_root.resolve())
                context["graph_json"] = configured["graph_payload"]
                context["compile_commands_excerpt"] = excerpt

            try:
                suggested = plugin.suggest(context) or {}
            except Exception as exc:
                suggested = {"actions": [], "notes": f"plugin error: {exc}"}

            ai_notes = str(suggested.get("notes", ""))
            validated = validate_ai_actions(suggested.get("actions", []), config.ai.max_actions)
            for action in validated:
                if apply_action(config, action, target_name=repair_scope_target):
                    ai_applied.append({"validated": True, **action})
                if len(ai_applied) >= config.ai.max_actions:
                    break

        if ai_applied:
            history.append(
                {
                    "attempt": attempt,
                    "source": "ai",
                    "target": repair_scope_target,
                    "reason": ai_notes,
                    "actions": ai_applied,
                }
            )
            _update_state_repairs(paths["state"], history)
            save_config(config_path, config)
            continue

        write_json(
            paths["last_report"],
            {
                **report_base,
                "ok": False,
                "attempts": attempt,
                "target": selected_target,
                "history": history,
                "errors": errors,
            },
        )
        _upsert_build_report(
            configured["paths"],
            profile=str(configured.get("profile", selected_profile)),
            cache_hit=configured.get("cache_hit"),
            cache_reason=configured.get("cache_reason"),
            plan_key_sha=configured.get("plan_key_sha"),
            fingerprint_sha=configured.get("fingerprint_sha"),
            compdb_contract_pass=configured.get("compdb_contract_pass"),
            graph_contract_pass=configured.get("graph_contract_pass"),
            durations={
                "ninja_build_ms": accumulated_ninja_ms if ran_ninja else None,
                "total_build_ms": int((perf_counter() - build_started) * 1000),
            },
        )
        return retcode

    write_json(
        paths["last_report"],
        {
            **report_base,
            "ok": False,
            "attempts": max_attempts,
            "history": history,
            "error": "max attempts reached",
        },
    )
    _upsert_build_report(
        paths,
        profile=selected_profile,
        durations={
            "ninja_build_ms": accumulated_ninja_ms if ran_ninja else None,
            "total_build_ms": int((perf_counter() - build_started) * 1000),
        },
    )
    return 1


def load_graph_payload(
    repo_root: Path,
    config_path: Path,
    msvc_auto: bool = False,
    profile: str | None = None,
) -> tuple[dict[str, Any], dict[str, Path]]:
    configured = configure_project(repo_root, config_path, msvc_auto=msvc_auto, profile=profile)
    return configured["graph_payload"], configured["paths"]


def explain_source(
    repo_root: Path,
    config_path: Path,
    source_path: str,
    msvc_auto: bool = False,
    profile: str | None = None,
) -> dict[str, Any]:
    configured = configure_project(repo_root, config_path, msvc_auto=msvc_auto, profile=profile)
    graph: BuildGraph = configured["graph"]

    candidate = normalize_path(source_path)
    if Path(source_path).is_absolute():
        abs_candidate = normalize_path(Path(source_path).resolve())
    else:
        abs_candidate = normalize_path((repo_root / source_path).resolve())

    owners: list[tuple[str, str]] = []
    for target_name in configured["source_map"]:
        for src in configured["source_map"][target_name]:
            src_abs = normalize_path((repo_root / src).resolve())
            if candidate == src or abs_candidate == src_abs:
                owners.append((target_name, src))

    if not owners:
        all_sources: list[str] = []
        for v in configured["source_map"].values():
            all_sources.extend(v)
        nearest = difflib.get_close_matches(candidate, sorted(set(all_sources)), n=10, cutoff=0.2)
        return {"status": "NOT_IN_GRAPH", "source": source_path, "candidates": nearest}

    owner_target_name, owner_src = owners[0]
    target = graph.targets[owner_target_name]

    obj_path = normalize_path((repo_root / target.obj_dir / Path(owner_src).with_suffix(".obj")).resolve())
    link_closure = graph.link_closure(owner_target_name)

    repairs: list[dict[str, Any]] = []
    if configured["paths"]["state"].exists():
        try:
            state = json.loads(configured["paths"]["state"].read_text(encoding="utf-8"))
            repairs = state.get("repairs", []) if isinstance(state.get("repairs", []), list) else []
        except Exception:
            repairs = []

    return {
        "status": "IN_GRAPH",
        "target": owner_target_name,
        "source": owner_src,
        "object_path": obj_path,
        "compile_command": build_compile_command(target, owner_src),
        "include_dirs": target.include_dirs,
        "defines": target.defines,
        "libs": target.libs,
        "link_closure": link_closure,
        "repairs": repairs,
    }


def explain_link(
    repo_root: Path,
    config_path: Path,
    msvc_auto: bool = False,
    target: str | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    configured = configure_project(repo_root, config_path, msvc_auto=msvc_auto, target=target, profile=profile)
    graph: BuildGraph = configured["graph"]
    target_name = configured["selected_target"]
    selected = graph.targets[target_name]

    if selected.kind != "exe":
        return {"status": "NOT_EXE", "target": target_name}

    return {
        "status": "OK",
        "target": target_name,
        "link_closure": graph.link_closure(target_name),
        "link_command": build_link_command_for_graph(graph, target_name),
    }


def run_binary(repo_root: Path, config_path: Path, target: str | None = None, profile: str | None = None) -> int:
    configured = configure_project(repo_root, config_path, msvc_auto=False, target=target, profile=profile)
    selected = configured["selected_target"]
    graph = configured["graph"]
    output_rel = ninja_target_output(graph, selected)
    exe = repo_root / output_rel
    if not exe.exists():
        return 1
    proc = subprocess.run([str(exe)], cwd=repo_root, shell=False)
    return proc.returncode


def clean_project(repo_root: Path, config_path: Path) -> None:
    config = load_config(config_path)
    out_dir = repo_root / config.out_dir
    if out_dir.exists():
        shutil.rmtree(out_dir)
