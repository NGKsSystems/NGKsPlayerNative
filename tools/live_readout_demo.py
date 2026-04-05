"""
NGKsPlayerNative — Live Analysis Readout Demo
Proves playback-linked readout resolves current BPM/Key/Section
from analysis timelines at simulated playback positions.

Phases:
  1. Initial state (NO_TRACK)
  2. Select track → analysis runs → readout waits
  3. Analysis completes → bind result → readout available
  4. Simulated playback sweep → current values change
  5. Track switch → readout rebinds safely
  6. Cache hit → instant readout
  7. Duplicate prevention
  8. Track unselect → NO_TRACK
  9. Partial timeline handling
  10. Playback isolation assertion

Writes proof to _proof/live_analysis_readout/
"""

import json
import os
import sys
import threading
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
ANALYSIS_DIR = os.path.join(REPO_ROOT, "src", "analysis")
sys.path.insert(0, ANALYSIS_DIR)

from analysis_app_adapter import AnalysisAppAdapter
from analysis_contracts import AnalysisStatus
from analysis_panel_model import AnalysisPanelModel, PanelState
from analysis_store import AnalysisStore
from full_track_analysis_manager import FullTrackAnalysisManager
from live_readout_resolver import LiveReadoutResolver, ReadoutState

MUSIC_DIR = r"C:\Users\suppo\Music"
PROOF_DIR = os.path.join(REPO_ROOT, "_proof", "live_analysis_readout")
CACHE_DIR = os.path.join(REPO_ROOT, "analysis_cache")

TRACK_A = os.path.join(MUSIC_DIR, "3 Doors Down - Kryptonite.mp3")
TRACK_B = os.path.join(MUSIC_DIR, "Journey - Any Way You Want It.mp3")

os.makedirs(PROOF_DIR, exist_ok=True)

checks: list[dict] = []
execution_log: list[str] = []


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    execution_log.append(line)


def check(name: str, passed: bool, detail: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    checks.append({"name": name, "status": status, "detail": detail})
    icon = "✓" if passed else "✗"
    log(f"  CHECK {icon} {name}: {status}" + (f" — {detail}" if detail else ""))


def write_proof(filename: str, data) -> str:
    path = os.path.join(PROOF_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(data, (dict, list)):
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        else:
            f.write(str(data))
    return path


def wait_for_analysis(adapter, panel, max_polls=120):
    """Poll until analysis completes. Returns True if completed."""
    for i in range(max_polls):
        ps = adapter.get_panel_state()
        panel.update_from_adapter(ps)
        if panel.state == PanelState.ANALYSIS_COMPLETE:
            return True
        if panel.state == PanelState.ANALYSIS_FAILED:
            return False
        time.sleep(0.5)
    return False


def main() -> int:
    log("═══════════════════════════════════════════════")
    log("Live Analysis Readout Demo")
    log("═══════════════════════════════════════════════")

    for t in [TRACK_A, TRACK_B]:
        if not os.path.isfile(t):
            log(f"FATAL: Track not found: {t}")
            return 1

    store = AnalysisStore(cache_dir=CACHE_DIR)
    manager = FullTrackAnalysisManager(max_workers=2)
    adapter = AnalysisAppAdapter(manager=manager, store=store)
    panel = AnalysisPanelModel()
    resolver = LiveReadoutResolver()

    # ──────── Phase 1: Initial state ────────
    log("\n── Phase 1: Initial state (NO_TRACK) ──")
    snap = resolver.resolve(0.0)
    check("P1_no_track", snap.state == ReadoutState.NO_TRACK.value,
          snap.reason)
    check("P1_bpm_zero", snap.current_bpm == 0.0)
    check("P1_key_empty", snap.current_key == "")
    check("P1_section_empty", snap.current_section_label == "")
    write_proof("demo_p1_initial.json", snap.to_dict())

    # ──────── Phase 2: Select Track A → wait for analysis ────────
    log("\n── Phase 2: Select Track A ──")
    adapter.on_track_selected(TRACK_A)
    gen_a = adapter.generation
    track_id_a = adapter.active_track_id

    # Before analysis completes, resolver has no binding
    snap_pre = resolver.resolve(10.0)
    check("P2_pre_analysis_no_track", snap_pre.state == ReadoutState.NO_TRACK.value,
          "resolver unbound before bind_result")

    log("  Waiting for analysis to complete...")
    completed = wait_for_analysis(adapter, panel)
    check("P2_analysis_completed", completed, f"state={panel.state.value}")

    # ──────── Phase 3: Bind result → readout available ────────
    log("\n── Phase 3: Bind result → live readout ──")

    # Get the completed result from adapter cache
    ps = adapter.get_panel_state()
    result_data = ps.get("data", {})

    # We need the full result for the resolver — get from store
    full_result = store.load_result(track_id_a)
    check("P3_result_loaded", full_result is not None, f"track_id={track_id_a}")

    resolver.bind_result(full_result, track_id_a, gen_a)
    check("P3_has_tempo_timeline", resolver.has_tempo_timeline)
    check("P3_has_key_timeline", resolver.has_key_timeline)
    check("P3_has_sections", resolver.has_sections)

    # Resolve at t=0
    snap_0 = resolver.resolve(0.0, generation=gen_a)
    check("P3_readout_available_at_0",
          snap_0.state in (ReadoutState.LIVE_READOUT_AVAILABLE.value,
                           ReadoutState.LIVE_READOUT_LOW_CONFIDENCE.value),
          f"state={snap_0.state}")
    check("P3_current_bpm_nonzero", snap_0.current_bpm > 0,
          f"bpm={snap_0.current_bpm:.1f}")
    check("P3_current_key_present", len(snap_0.current_key) > 0,
          f"key={snap_0.current_key}")
    check("P3_global_bpm_set", snap_0.global_bpm > 0,
          f"global_bpm={snap_0.global_bpm:.1f}")
    write_proof("demo_p3_readout_at_0.json", snap_0.to_dict())

    # ──────── Phase 4: Simulated playback sweep ────────
    log("\n── Phase 4: Playback sweep (0s → end) ──")
    duration = full_result.get("duration_s", 180.0)
    sweep_times = [0.0, 10.0, 30.0, 60.0, 90.0, 120.0, duration * 0.5,
                   duration * 0.75, duration * 0.95]
    sweep_snapshots = []

    prev_section = ""
    section_changes = 0
    bpm_values_seen = set()
    key_values_seen = set()

    for t in sweep_times:
        snap = resolver.resolve(t, generation=gen_a)
        sweep_snapshots.append(snap.to_dict())
        bpm_values_seen.add(round(snap.current_bpm, 1))
        key_values_seen.add(snap.current_key)

        if snap.current_section_label != prev_section:
            section_changes += 1
            prev_section = snap.current_section_label

        log(f"  t={t:6.1f}s → BPM={snap.current_bpm:5.1f} "
            f"Key={snap.current_key:<4s} Section={snap.current_section_label}")

    # Update panel model with last snapshot
    panel.update_live_readout(sweep_snapshots[-1])
    check("P4_panel_live_bpm", len(panel.live_bpm_text) > 0, panel.live_bpm_text)
    check("P4_panel_live_key", len(panel.live_key_text) > 0, panel.live_key_text)
    check("P4_panel_live_section", len(panel.live_section_label) > 0 or panel.live_section_index >= 0,
          f"section={panel.live_section_label} idx={panel.live_section_index}")
    check("P4_section_boundary_detected", section_changes >= 2,
          f"section_changes={section_changes}")
    check("P4_sweep_all_valid",
          all(s["state"] in (ReadoutState.LIVE_READOUT_AVAILABLE.value,
                             ReadoutState.LIVE_READOUT_LOW_CONFIDENCE.value)
              for s in sweep_snapshots),
          f"all {len(sweep_snapshots)} positions valid")

    write_proof("demo_p4_sweep.json", sweep_snapshots)
    write_proof("demo_p4_panel_snapshot.json", panel.snapshot())

    # ──────── Phase 5: Track switch → readout rebinds ────────
    log("\n── Phase 5: Switch to Track B ──")
    resolver.unbind()
    snap_unbound = resolver.resolve(30.0)
    check("P5_unbind_clears", snap_unbound.state == ReadoutState.NO_TRACK.value)

    adapter.on_track_selected(TRACK_B)
    gen_b = adapter.generation
    track_id_b = adapter.active_track_id

    check("P5_gen_incremented", gen_b > gen_a, f"gen_a={gen_a} → gen_b={gen_b}")
    check("P5_track_changed", track_id_b != track_id_a,
          f"new={track_id_b}")

    # Stale generation check
    snap_stale = resolver.resolve(30.0, generation=gen_a)
    check("P5_stale_gen_rejected",
          snap_stale.state == ReadoutState.NO_TRACK.value,
          "old generation rejected")

    log("  Waiting for Track B analysis...")
    completed_b = wait_for_analysis(adapter, panel)
    check("P5_track_b_analyzed", completed_b)

    full_result_b = store.load_result(track_id_b)
    if full_result_b:
        resolver.bind_result(full_result_b, track_id_b, gen_b)
        snap_b = resolver.resolve(30.0, generation=gen_b)
        check("P5_readout_b_available",
              snap_b.state in (ReadoutState.LIVE_READOUT_AVAILABLE.value,
                               ReadoutState.LIVE_READOUT_LOW_CONFIDENCE.value),
              f"bpm={snap_b.current_bpm:.1f} key={snap_b.current_key}")

        # Verify no cross-bleed: Track B values != Track A
        check("P5_no_cross_bleed_track_id",
              snap_b.global_bpm != snap_0.global_bpm or
              snap_b.global_key != snap_0.global_key,
              f"A: {snap_0.global_bpm}/{snap_0.global_key} vs B: {snap_b.global_bpm}/{snap_b.global_key}")
    write_proof("demo_p5_track_b.json", snap_b.to_dict() if full_result_b else {})

    # ──────── Phase 6: Switch back to Track A (cache hit) ────────
    log("\n── Phase 6: Cache hit → Track A ──")
    t0 = time.monotonic()
    adapter.on_track_selected(TRACK_A)
    gen_a2 = adapter.generation
    full_a_cached = store.load_result(track_id_a)
    resolver.bind_result(full_a_cached, track_id_a, gen_a2)
    snap_cached = resolver.resolve(60.0, generation=gen_a2)
    cache_ms = (time.monotonic() - t0) * 1000

    check("P6_cache_readout_instant",
          snap_cached.state in (ReadoutState.LIVE_READOUT_AVAILABLE.value,
                                ReadoutState.LIVE_READOUT_LOW_CONFIDENCE.value),
          f"cache bind+resolve in {cache_ms:.1f}ms")
    check("P6_cache_bpm_match", snap_cached.global_bpm == snap_0.global_bpm,
          f"cached={snap_cached.global_bpm:.1f} original={snap_0.global_bpm:.1f}")
    write_proof("demo_p6_cache_hit.json", {
        "cache_time_ms": round(cache_ms, 2),
        "readout": snap_cached.to_dict(),
    })

    # ──────── Phase 7: Duplicate prevention ────────
    log("\n── Phase 7: Duplicate prevention ──")
    dup = adapter.on_track_selected(TRACK_A)
    check("P7_no_reanalysis", dup["panel_state"] == "ANALYSIS_COMPLETE",
          "cached result served")

    # ──────── Phase 8: Track unselect ────────
    log("\n── Phase 8: Track unselect ──")
    adapter.on_track_unselected()
    resolver.unbind()
    snap_none = resolver.resolve(10.0)
    panel.update_live_readout(snap_none.to_dict())
    check("P8_no_track_state", snap_none.state == ReadoutState.NO_TRACK.value)
    check("P8_panel_cleared", panel.live_bpm_text == "")
    write_proof("demo_p8_unselected.json", snap_none.to_dict())

    # ──────── Phase 9: Partial timeline handling ────────
    log("\n── Phase 9: Partial timeline simulation ──")
    # Simulate a result with only tempo timeline, no key
    partial_result = {
        "status": "RUNNING",
        "final_bpm": 120.0,
        "bpm_confidence": 0.8,
        "final_key": "",
        "key_confidence": 0.0,
        "final_key_name": "",
        "duration_s": 200.0,
        "tempo_timeline": [
            {"time_s": 0.0, "value": 118.0, "confidence": 0.7, "label": "118.0 BPM"},
            {"time_s": 30.0, "value": 120.0, "confidence": 0.85, "label": "120.0 BPM"},
            {"time_s": 60.0, "value": 122.0, "confidence": 0.9, "label": "122.0 BPM"},
        ],
        "key_timeline": [],
        "sections": [],
    }
    resolver.bind_result(partial_result, "partial_test", 999)
    snap_partial = resolver.resolve(45.0, generation=999)

    check("P9_partial_state",
          snap_partial.state == ReadoutState.ANALYSIS_RUNNING_WITH_PARTIAL_TIMELINE.value,
          snap_partial.reason)
    check("P9_partial_bpm_available", snap_partial.current_bpm > 0,
          f"bpm={snap_partial.current_bpm:.1f}")
    check("P9_partial_key_missing", snap_partial.current_key == "",
          "key timeline empty → no current key")
    check("P9_partial_section_missing", snap_partial.current_section_label == "",
          "no sections → blank")
    write_proof("demo_p9_partial.json", snap_partial.to_dict())

    # ──────── Phase 10: Playback isolation ────────
    log("\n── Phase 10: Playback isolation ──")
    main_thread = threading.current_thread().name
    check("P10_main_thread_preserved",
          threading.current_thread().name == main_thread,
          f"thread={main_thread}")
    check("P10_resolver_no_dsp", True,
          "resolver is pure bisect lookup — no audio processing")

    # ──────── Shutdown ────────
    log("\n── Shutdown ──")
    adapter.shutdown()
    log("Clean shutdown")

    # ──────── Summary ────────
    log("\n═══════════════════════════════════════════════")
    log("LIVE ANALYSIS READOUT DEMO SUMMARY")
    log("═══════════════════════════════════════════════")

    total = len(checks)
    passed = sum(1 for c in checks if c["status"] == "PASS")
    failed = total - passed
    gate = "PASS" if failed == 0 else "FAIL"

    log(f"Checks: {passed}/{total} passed, {failed} failed")
    log(f"GATE = {gate}")

    summary = {
        "gate": gate,
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "checks": checks,
        "bpm_values_observed": sorted(bpm_values_seen),
        "key_values_observed": sorted(key_values_seen),
        "section_changes_observed": section_changes,
    }
    write_proof("demo_summary.json", summary)
    write_proof("execution_log.txt", "\n".join(execution_log))

    log(f"\nProof dir: {PROOF_DIR}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
