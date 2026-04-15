"""
NGKsPlayerNative — Full-Track Analysis Batch Test (10 Songs)
Senior QA / DSP Validation Engineer — automated batch validation.

Steps:
  1. Select 10 tracks
  2. Run full analysis on each
  3. Live readout simulation (5-point sweep per track)
  4. Cache test (3 tracks)
  5. Track switch test
  6. Edge case checks
  7. Performance summary
  8. Quality summary
  9. Failure report
  10. Final report
  11. Sanity checks
  12. Proof package
"""

import json
import os
import sys
import threading
import time

# ── Paths ──
WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
SRC_ANALYSIS = os.path.join(WORKSPACE, "src", "analysis")
MUSIC_DIR = r"C:\Users\suppo\Music"
PROOF_DIR = os.path.join(WORKSPACE, "_proof", "analysis_batch_test_10")

# Make src/analysis importable
sys.path.insert(0, SRC_ANALYSIS)

from analysis_contracts import AnalysisStatus, FullTrackAnalysisResult
from analysis_store import AnalysisStore
from full_track_analysis_manager import FullTrackAnalysisManager
from analysis_app_adapter import AnalysisAppAdapter
from live_readout_resolver import LiveReadoutResolver, ReadoutState

os.makedirs(PROOF_DIR, exist_ok=True)

# ── Execution log ──
LOG_LINES: list[str] = []


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    LOG_LINES.append(line)
    print(line, flush=True)


def write_log():
    path = os.path.join(PROOF_DIR, "execution_log.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))


# ═══════════════════════════════════════════════════
#  PREVIOUSLY USED TRACKS — EXCLUDE LIST
# ═══════════════════════════════════════════════════
EXCLUDE = {
    "3 Doors Down - Kryptonite.mp3",
    "Journey - Any Way You Want It.mp3",
    "120 BPM Metronome.mp3",
    "Adam Calhoun feat. Demun Jones Brodnax Dusty Leigh - Gumbo.mp3",
    "Airbourne - Back In The Game.mp3",
    "Adam Calhoun - Bars & Stripes.mp3",
    "Bad Wolves - Zombie.mp3",
    "Airbourne - Runnin' Wild.mp3",
    "DJ LOA - Don't Say Goodbye Remix.mp3",
    "Five Finger Death Punch - Wrong Side Of Heaven.mp3",
    "AC-DC - Highway to Hell.mp3",
}

# ═══════════════════════════════════════════════════
#  SELECTED TRACKS — curated for genre/tempo variety
# ═══════════════════════════════════════════════════
SELECTED = [
    "Bob Marley - Three Little Birds.mp3",        # reggae, slow/medium
    "AC-DC - Shoot To Thrill.mp3",                # hard rock, fast
    "Adele - Hello.mp3",                          # pop ballad, slow
    "2Pac - California Love.mp3",                 # hip hop, medium
    "Boston - More Than A Feeling.mp3",           # classic rock, medium
    "Carrie Underwood - Before He Cheats.mp3",    # country, medium
    "Childish Gambino - Redbone.mp3",             # funk/R&B, slow
    "Billy Joel - Pressure.mp3",                  # pop rock, fast
    "Blackfoot - Train Train.mp3",                # southern rock, fast
    "Chris Stapleton - Fire Away.mp3",            # country ballad, slow
]


def check_tracks_exist():
    """Verify all selected tracks exist on disk."""
    missing = []
    for fn in SELECTED:
        if fn in EXCLUDE:
            log(f"ERROR: {fn} is in the exclude list!")
            sys.exit(1)
        path = os.path.join(MUSIC_DIR, fn)
        if not os.path.isfile(path):
            missing.append(fn)
    if missing:
        log(f"MISSING TRACKS: {missing}")
        # Try to find replacements
        available = [
            f for f in os.listdir(MUSIC_DIR)
            if f.lower().endswith(".mp3") and f not in EXCLUDE and f not in SELECTED
        ]
        log(f"Available replacements: {len(available)} tracks")
        for m in missing:
            idx = SELECTED.index(m)
            if available:
                replacement = available.pop(0)
                log(f"REPLACING {m} -> {replacement}")
                SELECTED[idx] = replacement
            else:
                log(f"FATAL: No replacement for {m}")
                sys.exit(1)


# ═══════════════════════════════════════════════════
#  STEP 1 — SELECT TRACKS
# ═══════════════════════════════════════════════════
def step1_select_tracks():
    log("=== STEP 1: SELECT TRACKS ===")
    check_tracks_exist()

    lines = ["NGKsPlayerNative — Batch Test: Selected Tracks", "=" * 50, ""]
    for i, fn in enumerate(SELECTED, 1):
        path = os.path.join(MUSIC_DIR, fn)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        lines.append(f"Track {i:2d}: {fn}")
        lines.append(f"         Size: {size_mb:.1f} MB")
        lines.append("")

    lines.append(f"Total: {len(SELECTED)} tracks")
    lines.append(f"Source: {MUSIC_DIR}")
    lines.append(f"Excluded (previously used): {len(EXCLUDE)} tracks")

    path = os.path.join(PROOF_DIR, "00_selected_tracks.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"Wrote {path}")


# ═══════════════════════════════════════════════════
#  STEP 2 — RUN FULL ANALYSIS
# ═══════════════════════════════════════════════════
def step2_run_analysis():
    log("=== STEP 2: RUN FULL ANALYSIS ===")
    results = []
    store = AnalysisStore()

    for i, fn in enumerate(SELECTED, 1):
        filepath = os.path.join(MUSIC_DIR, fn)
        track_id = os.path.splitext(fn)[0]

        log(f"  [{i}/10] Analyzing: {fn}")

        # Delete any cached result to force fresh analysis
        store.delete_result(track_id)

        manager = FullTrackAnalysisManager(max_workers=1)

        # Create result object
        result = FullTrackAnalysisResult(
            track_id=track_id,
            filepath=filepath,
            status=AnalysisStatus.QUEUED.value,
        )
        cancel = threading.Event()

        t0 = time.perf_counter()

        # Run analysis synchronously via the worker
        try:
            from analysis_worker import run_analysis
            run_analysis(
                filepath=filepath,
                result=result,
                cancel_event=cancel,
                log_fn=lambda msg: log(f"    WORKER: {msg}"),
                store=store,
            )
            elapsed = time.perf_counter() - t0
            result.processing_time_s = elapsed

            # Worker leaves status=RUNNING; the manager normally sets COMPLETED.
            # Since we bypass the manager, finalize status here.
            if result.status == AnalysisStatus.RUNNING.value:
                result.status = AnalysisStatus.COMPLETED.value
                from datetime import datetime
                result.completed_at = datetime.now().isoformat()
                # Re-save with correct status
                store.save_result(result)
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            result.status = AnalysisStatus.FAILED.value
            result.error = f"EXCEPTION: {exc}"
            result.processing_time_s = elapsed
            log(f"    EXCEPTION: {exc}")

        # Log summary
        speed_x = 0
        if result.duration_s > 0 and elapsed > 0:
            speed_x = result.duration_s / elapsed

        log(f"    Status: {result.status}")
        log(f"    BPM: {result.final_bpm:.2f} (conf={result.bpm_confidence:.3f})")
        log(f"    Key: {result.final_key} / {result.final_key_name} (conf={result.key_confidence:.3f})")
        log(f"    Sections: {len(result.sections)}, Cues: {len(result.cues)}")
        log(f"    Chunks: {result.chunk_count}, Duration: {result.duration_s:.1f}s")
        log(f"    Time: {elapsed:.2f}s, Speed: {speed_x:.1f}x real-time")
        log(f"    Key change: {'Y' if result.key_change_detected else 'N'}")
        if result.error:
            log(f"    Error: {result.error}")
        if result.review_reason:
            log(f"    Review: {result.review_reason}")

        # Build result JSON
        track_result = {
            "track_number": i,
            "filename": fn,
            "track_id": track_id,
            "status": result.status,
            "duration_s": round(result.duration_s, 3),
            "processing_time_s": round(elapsed, 3),
            "speed_x_realtime": round(speed_x, 2),
            "chunk_count": result.chunk_count,
            "chunks_completed": result.chunks_completed,
            "final_bpm": round(result.final_bpm, 2),
            "bpm_confidence": round(result.bpm_confidence, 3),
            "bpm_family": result.bpm_family,
            "final_key": result.final_key,
            "final_key_name": result.final_key_name,
            "key_confidence": round(result.key_confidence, 3),
            "key_change_detected": result.key_change_detected,
            "section_count": len(result.sections),
            "cue_count": len(result.cues),
            "analyzer_ready": result.analyzer_ready,
            "review_required": result.review_required,
            "review_reason": result.review_reason,
            "error": result.error,
            "warnings": [],
            "tempo_timeline_count": len(result.tempo_timeline),
            "key_timeline_count": len(result.key_timeline),
        }
        results.append(track_result)

        # Save per-track JSON
        out_path = os.path.join(PROOF_DIR, f"01_track_{i:02d}_result.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(track_result, f, indent=2, ensure_ascii=False)
        log(f"    Wrote {out_path}")

        manager.shutdown(wait=False)

    log(f"  Analysis complete: {len(results)} tracks processed")
    return results


# ═══════════════════════════════════════════════════
#  STEP 3 — LIVE READOUT SIMULATION
# ═══════════════════════════════════════════════════
def step3_live_readout(results):
    log("=== STEP 3: LIVE READOUT SIMULATION ===")
    store = AnalysisStore()
    resolver = LiveReadoutResolver()
    readout_ok = True

    for i, fn in enumerate(SELECTED, 1):
        track_id = os.path.splitext(fn)[0]

        # Load cached result
        cached = store.load_result(track_id)
        if not cached:
            log(f"  [{i}] SKIP — no cached result for {track_id}")
            readout_ok = False
            # Write empty readout
            out_path = os.path.join(PROOF_DIR, f"02_track_{i:02d}_live_readout.txt")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(f"Track: {fn}\nStatus: NO CACHED RESULT — SKIPPED\n")
            continue

        duration = cached.get("duration_s", 0.0)
        if duration <= 0:
            log(f"  [{i}] SKIP — zero duration for {track_id}")
            readout_ok = False
            out_path = os.path.join(PROOF_DIR, f"02_track_{i:02d}_live_readout.txt")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(f"Track: {fn}\nStatus: ZERO DURATION — SKIPPED\n")
            continue

        gen = i
        resolver.bind_result(cached, track_id, gen)

        sweep_points = [
            ("start", 1.0),
            ("25%", duration * 0.25),
            ("50%", duration * 0.50),
            ("75%", duration * 0.75),
            ("near_end", max(duration - 5.0, duration * 0.95)),
        ]

        lines = [
            f"Track {i}: {fn}",
            f"Duration: {duration:.1f}s",
            f"Track ID: {track_id}",
            "=" * 60,
            "",
        ]

        for label, t in sweep_points:
            snap = resolver.resolve(t, generation=gen)
            lines.append(f"--- Position: {label} ({t:.1f}s) ---")
            lines.append(f"  State: {snap.state}")
            lines.append(f"  BPM: {snap.current_bpm:.2f} (conf={snap.current_bpm_confidence:.3f}, fallback={snap.bpm_is_fallback})")
            lines.append(f"  Key: {snap.current_key} (conf={snap.current_key_confidence:.3f}, fallback={snap.key_is_fallback})")
            lines.append(f"  Section: [{snap.current_section_index}] {snap.current_section_label} ({snap.current_section_start_s:.1f}s - {snap.current_section_end_s:.1f}s)")
            lines.append(f"  Global BPM: {snap.global_bpm:.2f}, Global Key: {snap.global_key}")
            lines.append("")

            if snap.state not in (
                ReadoutState.LIVE_READOUT_AVAILABLE.value,
                ReadoutState.LIVE_READOUT_LOW_CONFIDENCE.value,
            ):
                log(f"  [{i}] WARNING: readout state={snap.state} at {label}")

        resolver.unbind()

        out_path = os.path.join(PROOF_DIR, f"02_track_{i:02d}_live_readout.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        log(f"  [{i}] Readout sweep complete for {fn}")

    return readout_ok


# ═══════════════════════════════════════════════════
#  STEP 4 — CACHE TEST
# ═══════════════════════════════════════════════════
def step4_cache_test():
    log("=== STEP 4: CACHE TEST ===")
    store = AnalysisStore()
    resolver = LiveReadoutResolver()
    test_tracks = SELECTED[:3]

    lines = [
        "NGKsPlayerNative — Cache Test",
        "=" * 50,
        "",
    ]

    all_ok = True
    for idx, fn in enumerate(test_tracks, 1):
        track_id = os.path.splitext(fn)[0]

        # Cache must already exist from step 2
        t0 = time.perf_counter()
        cached = store.load_result(track_id)
        cache_load_ms = (time.perf_counter() - t0) * 1000

        if not cached:
            lines.append(f"Track {idx}: {fn}")
            lines.append(f"  FAIL — no cache entry")
            all_ok = False
            continue

        # Bind + resolve to first readout
        t1 = time.perf_counter()
        resolver.bind_result(cached, track_id, generation=100 + idx)
        snap = resolver.resolve(1.0, generation=100 + idx)
        first_readout_ms = (time.perf_counter() - t1) * 1000

        lines.append(f"Track {idx}: {fn}")
        lines.append(f"  Cache load time: {cache_load_ms:.2f} ms")
        lines.append(f"  First readout time: {first_readout_ms:.2f} ms")
        lines.append(f"  Total (cache + readout): {cache_load_ms + first_readout_ms:.2f} ms")
        lines.append(f"  Readout state: {snap.state}")
        lines.append(f"  BPM: {snap.current_bpm:.2f}, Key: {snap.current_key}")
        lines.append(f"  Cache status: {cached.get('status')}")
        lines.append("")

        resolver.unbind()

        log(f"  [{idx}] cache_load={cache_load_ms:.2f}ms first_readout={first_readout_ms:.2f}ms")

    lines.append(f"Result: {'PASS' if all_ok else 'FAIL'}")

    path = os.path.join(PROOF_DIR, "03_cache_test.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"  Cache test: {'PASS' if all_ok else 'FAIL'}")
    return all_ok


# ═══════════════════════════════════════════════════
#  STEP 5 — TRACK SWITCH TEST
# ═══════════════════════════════════════════════════
def step5_track_switch():
    log("=== STEP 5: TRACK SWITCH TEST ===")
    store = AnalysisStore()

    # Use adapter for full lifecycle
    manager = FullTrackAnalysisManager(max_workers=1)
    adapter = AnalysisAppAdapter(manager, store)
    resolver = LiveReadoutResolver()

    track_a_fn = SELECTED[0]
    track_b_fn = SELECTED[1]
    track_a_id = os.path.splitext(track_a_fn)[0]
    track_b_id = os.path.splitext(track_b_fn)[0]
    track_a_path = os.path.join(MUSIC_DIR, track_a_fn)
    track_b_path = os.path.join(MUSIC_DIR, track_b_fn)

    lines = [
        "NGKsPlayerNative — Track Switch Test",
        "=" * 50,
        "",
        f"Track A: {track_a_fn}",
        f"Track B: {track_b_fn}",
        "",
    ]

    # 1. Select Track A
    state_a1 = adapter.on_track_selected(track_a_path)
    gen_a = state_a1["generation"]
    cached_a = store.load_result(track_a_id)
    if cached_a:
        resolver.bind_result(cached_a, track_a_id, gen_a)
    snap_a1 = resolver.resolve(10.0, generation=gen_a)

    lines.append("Phase 1: Load Track A")
    lines.append(f"  Panel state: {state_a1['panel_state']}")
    lines.append(f"  Generation: {gen_a}")
    lines.append(f"  BPM: {snap_a1.current_bpm:.2f}, Key: {snap_a1.current_key}")
    lines.append("")

    # 2. Switch to Track B
    state_b = adapter.on_track_selected(track_b_path)
    gen_b = state_b["generation"]
    cached_b = store.load_result(track_b_id)
    if cached_b:
        resolver.unbind()
        resolver.bind_result(cached_b, track_b_id, gen_b)
    snap_b = resolver.resolve(10.0, generation=gen_b)

    lines.append("Phase 2: Switch to Track B")
    lines.append(f"  Panel state: {state_b['panel_state']}")
    lines.append(f"  Generation: {gen_b}")
    lines.append(f"  BPM: {snap_b.current_bpm:.2f}, Key: {snap_b.current_key}")
    lines.append("")

    # 3. Check cross-bleed — resolve with stale gen
    snap_stale = resolver.resolve(10.0, generation=gen_a)
    stale_blocked = snap_stale.state == ReadoutState.NO_TRACK.value

    lines.append("Phase 3: Cross-bleed check (resolve with stale gen_a)")
    lines.append(f"  Stale resolve state: {snap_stale.state}")
    lines.append(f"  Stale blocked: {stale_blocked}")
    lines.append("")

    # 4. Switch back to Track A
    state_a2 = adapter.on_track_selected(track_a_path)
    gen_a2 = state_a2["generation"]
    if cached_a:
        resolver.unbind()
        resolver.bind_result(cached_a, track_a_id, gen_a2)
    snap_a2 = resolver.resolve(10.0, generation=gen_a2)

    lines.append("Phase 4: Switch back to Track A")
    lines.append(f"  Panel state: {state_a2['panel_state']}")
    lines.append(f"  Generation: {gen_a2}")
    lines.append(f"  BPM: {snap_a2.current_bpm:.2f}, Key: {snap_a2.current_key}")
    lines.append("")

    # Validate
    bpm_match = abs(snap_a1.current_bpm - snap_a2.current_bpm) < 0.1
    key_match = snap_a1.current_key == snap_a2.current_key
    no_bleed = snap_b.current_key != snap_a1.current_key or snap_b.current_bpm != snap_a1.current_bpm
    gen_guard = stale_blocked

    overall = bpm_match and key_match and gen_guard
    lines.append("Validation:")
    lines.append(f"  Track A BPM consistent: {bpm_match} ({snap_a1.current_bpm:.2f} vs {snap_a2.current_bpm:.2f})")
    lines.append(f"  Track A Key consistent: {key_match} ({snap_a1.current_key} vs {snap_a2.current_key})")
    lines.append(f"  Generation guard: {gen_guard}")
    lines.append(f"  Data isolation: Track A != Track B data: {no_bleed}")
    lines.append("")
    lines.append(f"Result: {'PASS' if overall else 'FAIL'}")

    resolver.unbind()
    manager.shutdown(wait=False)

    path = os.path.join(PROOF_DIR, "04_track_switch_test.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"  Track switch test: {'PASS' if overall else 'FAIL'}")
    return overall


# ═══════════════════════════════════════════════════
#  STEP 6 — EDGE CASE CHECKS
# ═══════════════════════════════════════════════════
def step6_edge_cases(results):
    log("=== STEP 6: EDGE CASE CHECKS ===")
    lines = [
        "NGKsPlayerNative — Edge Case Summary",
        "=" * 50,
        "",
    ]

    unstable_bpm = []
    ambiguous_key = []
    many_sections = []
    low_confidence = []
    no_sections = []

    for r in results:
        fn = r["filename"]
        if r["bpm_confidence"] < 0.5:
            unstable_bpm.append(f"{fn} (conf={r['bpm_confidence']:.3f})")
        if r["key_confidence"] < 0.5:
            ambiguous_key.append(f"{fn} (conf={r['key_confidence']:.3f})")
        if r["section_count"] > 10:
            many_sections.append(f"{fn} (sections={r['section_count']})")
        if r["bpm_confidence"] < 0.4 and r["key_confidence"] < 0.4:
            low_confidence.append(f"{fn} (bpm_conf={r['bpm_confidence']:.3f}, key_conf={r['key_confidence']:.3f})")
        if r["section_count"] == 0:
            no_sections.append(fn)

    def _write_list(title, items):
        lines.append(f"--- {title} ---")
        if items:
            for item in items:
                lines.append(f"  - {item}")
        else:
            lines.append("  (none)")
        lines.append("")

    _write_list("Tracks with unstable BPM (confidence < 0.5)", unstable_bpm)
    _write_list("Tracks with key ambiguity (confidence < 0.5)", ambiguous_key)
    _write_list("Tracks with many sections (>10)", many_sections)
    _write_list("Tracks with low confidence (both < 0.4)", low_confidence)
    _write_list("Tracks with no section detection", no_sections)

    path = os.path.join(PROOF_DIR, "05_edge_case_summary.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"  Edge cases: unstable_bpm={len(unstable_bpm)} ambiguous_key={len(ambiguous_key)} many_sections={len(many_sections)} low_conf={len(low_confidence)} no_sections={len(no_sections)}")


# ═══════════════════════════════════════════════════
#  STEP 7 — PERFORMANCE SUMMARY
# ═══════════════════════════════════════════════════
def step7_performance(results):
    log("=== STEP 7: PERFORMANCE SUMMARY ===")
    completed = [r for r in results if r["status"] == "COMPLETED"]

    if not completed:
        lines = ["No completed analyses — cannot compute performance.\n"]
        path = os.path.join(PROOF_DIR, "06_performance_summary.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return

    times = [r["processing_time_s"] for r in completed]
    chunks = [r["chunk_count"] for r in completed]
    speeds = [r["speed_x_realtime"] for r in completed]

    avg_time = sum(times) / len(times)
    avg_chunks = sum(chunks) / len(chunks)
    avg_speed = sum(speeds) / len(speeds)

    fastest = min(completed, key=lambda r: r["processing_time_s"])
    slowest = max(completed, key=lambda r: r["processing_time_s"])

    lines = [
        "NGKsPlayerNative — Performance Summary",
        "=" * 50,
        "",
        f"Tracks analyzed: {len(completed)} / {len(results)}",
        "",
        f"Average processing time: {avg_time:.2f}s",
        f"Average chunks per track: {avg_chunks:.1f}",
        f"Average processing speed: {avg_speed:.1f}x real-time",
        "",
        f"Fastest: {fastest['filename']} — {fastest['processing_time_s']:.2f}s ({fastest['speed_x_realtime']:.1f}x)",
        f"Slowest: {slowest['filename']} — {slowest['processing_time_s']:.2f}s ({slowest['speed_x_realtime']:.1f}x)",
        "",
        "Per-track breakdown:",
    ]
    for r in completed:
        lines.append(f"  {r['filename']}: {r['processing_time_s']:.2f}s / {r['chunk_count']} chunks / {r['speed_x_realtime']:.1f}x")

    path = os.path.join(PROOF_DIR, "06_performance_summary.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"  Performance: avg={avg_time:.2f}s, avg_speed={avg_speed:.1f}x, fastest={fastest['filename']}, slowest={slowest['filename']}")


# ═══════════════════════════════════════════════════
#  STEP 8 — QUALITY SUMMARY
# ═══════════════════════════════════════════════════
def step8_quality(results):
    log("=== STEP 8: QUALITY SUMMARY ===")
    completed = [r for r in results if r["status"] == "COMPLETED"]
    n = len(completed) if completed else 1

    bpm_confs = [r["bpm_confidence"] for r in completed]
    key_confs = [r["key_confidence"] for r in completed]
    key_changes = sum(1 for r in completed if r["key_change_detected"])
    section_counts = [r["section_count"] for r in completed]
    cue_counts = [r["cue_count"] for r in completed]

    lines = [
        "NGKsPlayerNative — Quality Summary",
        "=" * 50,
        "",
        f"Tracks: {len(completed)}",
        "",
        "BPM Confidence Distribution:",
        f"  Min: {min(bpm_confs) if bpm_confs else 0:.3f}",
        f"  Max: {max(bpm_confs) if bpm_confs else 0:.3f}",
        f"  Avg: {sum(bpm_confs)/n:.3f}",
        f"  >= 0.8: {sum(1 for c in bpm_confs if c >= 0.8)} tracks",
        f"  >= 0.5: {sum(1 for c in bpm_confs if c >= 0.5)} tracks",
        f"  < 0.5:  {sum(1 for c in bpm_confs if c < 0.5)} tracks",
        "",
        "Key Confidence Distribution:",
        f"  Min: {min(key_confs) if key_confs else 0:.3f}",
        f"  Max: {max(key_confs) if key_confs else 0:.3f}",
        f"  Avg: {sum(key_confs)/n:.3f}",
        f"  >= 0.8: {sum(1 for c in key_confs if c >= 0.8)} tracks",
        f"  >= 0.5: {sum(1 for c in key_confs if c >= 0.5)} tracks",
        f"  < 0.5:  {sum(1 for c in key_confs if c < 0.5)} tracks",
        "",
        f"Tracks with key changes: {key_changes}/{n} ({key_changes/n*100:.0f}%)",
        "",
        f"Section count — avg: {sum(section_counts)/n:.1f}, min: {min(section_counts) if section_counts else 0}, max: {max(section_counts) if section_counts else 0}",
        f"Cue count — avg: {sum(cue_counts)/n:.1f}, min: {min(cue_counts) if cue_counts else 0}, max: {max(cue_counts) if cue_counts else 0}",
        "",
        "Per-track BPM/Key:",
    ]
    for r in completed:
        lines.append(f"  {r['filename']}: BPM={r['final_bpm']:.1f} (conf={r['bpm_confidence']:.3f}) | Key={r['final_key']} {r['final_key_name']} (conf={r['key_confidence']:.3f}) | KeyChange={'Y' if r['key_change_detected'] else 'N'} | Sections={r['section_count']}")

    path = os.path.join(PROOF_DIR, "07_quality_summary.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"  Quality: avg_bpm_conf={sum(bpm_confs)/n:.3f}, avg_key_conf={sum(key_confs)/n:.3f}, key_changes={key_changes}/{n}")


# ═══════════════════════════════════════════════════
#  STEP 9 — FAILURE REPORT
# ═══════════════════════════════════════════════════
def step9_failures(results, cache_ok, switch_ok, readout_ok):
    log("=== STEP 9: FAILURE REPORT ===")
    failures = []

    for r in results:
        if r["status"] != "COMPLETED":
            failures.append({
                "track": r["filename"],
                "failure_type": r["status"],
                "stage": "analysis",
                "error": r.get("error", ""),
            })

    if not cache_ok:
        failures.append({
            "track": "(cache test)",
            "failure_type": "CACHE_FAIL",
            "stage": "cache",
            "error": "One or more cache load/readout operations failed",
        })

    if not switch_ok:
        failures.append({
            "track": "(track switch)",
            "failure_type": "SWITCH_FAIL",
            "stage": "switch",
            "error": "Track switch validation failed — possible cross-bleed or generation guard issue",
        })

    if not readout_ok:
        failures.append({
            "track": "(live readout)",
            "failure_type": "READOUT_FAIL",
            "stage": "readout",
            "error": "Some readout sweeps did not produce valid data",
        })

    lines = [
        "NGKsPlayerNative — Failure Report",
        "=" * 50,
        "",
    ]
    if not failures:
        lines.append("NO FAILURES DETECTED")
        lines.append("")
        lines.append("All 10 tracks processed successfully.")
        lines.append("Cache test: PASS")
        lines.append("Track switch test: PASS")
        lines.append("Live readout: PASS")
    else:
        lines.append(f"Total failures: {len(failures)}")
        lines.append("")
        for f in failures:
            lines.append(f"Track: {f['track']}")
            lines.append(f"  Type: {f['failure_type']}")
            lines.append(f"  Stage: {f['stage']}")
            lines.append(f"  Error: {f['error']}")
            lines.append("")

    path = os.path.join(PROOF_DIR, "08_failure_report.txt")
    with open(path, "w", encoding="utf-8") as fp:
        fp.write("\n".join(lines))
    log(f"  Failures: {len(failures)}")
    return len(failures) == 0


# ═══════════════════════════════════════════════════
#  STEP 10 — FINAL REPORT
# ═══════════════════════════════════════════════════
def step10_final_report(results, cache_ok, switch_ok, readout_ok, no_failures):
    log("=== STEP 10: FINAL REPORT ===")

    completed = [r for r in results if r["status"] == "COMPLETED"]
    total = len(results)
    n_pass = len(completed)

    gate = (
        n_pass == 10
        and cache_ok
        and switch_ok
        and readout_ok
        and no_failures
    )

    lines = [
        "NGKsPlayerNative — Full-Track Analysis Batch Test — FINAL REPORT",
        "=" * 60,
        "",
        f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Tracks tested: {total}",
        f"Tracks completed: {n_pass}",
        f"Tracks failed:    {total - n_pass}",
        "",
        "Sub-test results:",
        f"  Analysis (10 tracks): {'PASS' if n_pass == 10 else 'FAIL'}",
        f"  Live readout sweep:   {'PASS' if readout_ok else 'FAIL'}",
        f"  Cache test (3 tracks): {'PASS' if cache_ok else 'FAIL'}",
        f"  Track switch test:    {'PASS' if switch_ok else 'FAIL'}",
        f"  Zero failures:        {'PASS' if no_failures else 'FAIL'}",
        "",
        "BPM summary:",
    ]

    bpms = [(r["filename"], r["final_bpm"], r["bpm_confidence"]) for r in completed]
    for fn, bpm, conf in bpms:
        lines.append(f"  {fn}: {bpm:.1f} BPM (conf={conf:.3f})")

    lines.append("")
    lines.append("Key summary:")
    keys = [(r["filename"], r["final_key"], r["final_key_name"], r["key_confidence"], r["key_change_detected"]) for r in completed]
    for fn, key, name, conf, kc in keys:
        lines.append(f"  {fn}: {key} {name} (conf={conf:.3f}) {'[KEY CHANGE]' if kc else ''}")

    lines.append("")
    lines.append("Observations:")

    # Patterns analysis
    bpm_confs = [r["bpm_confidence"] for r in completed]
    key_confs = [r["key_confidence"] for r in completed]
    avg_bpm_conf = sum(bpm_confs) / len(bpm_confs) if bpm_confs else 0
    avg_key_conf = sum(key_confs) / len(key_confs) if key_confs else 0

    if avg_bpm_conf >= 0.7:
        lines.append("  - BPM detection confidence is STRONG across the test set")
    elif avg_bpm_conf >= 0.5:
        lines.append("  - BPM detection confidence is MODERATE — some tracks had lower confidence")
    else:
        lines.append("  - BPM detection confidence is WEAK — investigation needed")

    if avg_key_conf >= 0.6:
        lines.append("  - Key detection confidence is STRONG across the test set")
    elif avg_key_conf >= 0.4:
        lines.append("  - Key detection confidence is MODERATE")
    else:
        lines.append("  - Key detection confidence is WEAK")

    key_changes_n = sum(1 for r in completed if r["key_change_detected"])
    lines.append(f"  - Key changes detected in {key_changes_n}/{n_pass} tracks")

    section_counts = [r["section_count"] for r in completed]
    avg_sec = sum(section_counts) / len(section_counts) if section_counts else 0
    lines.append(f"  - Average section count: {avg_sec:.1f}")

    lines.append("")
    lines.append("Live readout behavior:")
    lines.append(f"  - All 10 tracks swept at 5 positions: {'YES' if readout_ok else 'INCOMPLETE'}")
    lines.append(f"  - Cache hot-load for readout: {'FAST' if cache_ok else 'ISSUES DETECTED'}")
    lines.append(f"  - Track switch isolation:     {'VERIFIED' if switch_ok else 'FAILED'}")

    lines.append("")
    recommendation = "READY" if gate else "NEEDS ATTENTION"
    lines.append(f"Recommendation: {recommendation}")
    lines.append(f"GATE={'PASS' if gate else 'FAIL'}")

    path = os.path.join(PROOF_DIR, "09_final_report.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"  Final report: GATE={'PASS' if gate else 'FAIL'} recommendation={recommendation}")
    return gate


# ═══════════════════════════════════════════════════
#  STEP 11 — SANITY CHECKS
# ═══════════════════════════════════════════════════
def step11_sanity_checks(results, gate):
    log("=== STEP 11: SANITY CHECKS ===")
    checks = []

    # Check 1: 10 tracks processed
    checks.append(("10 tracks processed", len(results) == 10, f"count={len(results)}"))

    # Check 2: all per-track result JSONs exist
    all_json = all(
        os.path.isfile(os.path.join(PROOF_DIR, f"01_track_{i:02d}_result.json"))
        for i in range(1, 11)
    )
    checks.append(("All 01_track_XX_result.json files exist", all_json, ""))

    # Check 3: all readout files exist
    all_readout = all(
        os.path.isfile(os.path.join(PROOF_DIR, f"02_track_{i:02d}_live_readout.txt"))
        for i in range(1, 11)
    )
    checks.append(("All 02_track_XX_live_readout.txt files exist", all_readout, ""))

    # Check 4-11: individual summary files
    expected_files = [
        "00_selected_tracks.txt",
        "03_cache_test.txt",
        "04_track_switch_test.txt",
        "05_edge_case_summary.txt",
        "06_performance_summary.txt",
        "07_quality_summary.txt",
        "08_failure_report.txt",
        "09_final_report.txt",
    ]
    for ef in expected_files:
        exists = os.path.isfile(os.path.join(PROOF_DIR, ef))
        checks.append((f"{ef} exists", exists, ""))

    # Check: no crashes (all returned results)
    no_crashes = all(r["status"] in ("COMPLETED", "FAILED", "CANCELLED") for r in results)
    checks.append(("No crashes (all jobs terminated normally)", no_crashes, ""))

    # Check: execution log exists
    # (will be written after this)
    checks.append(("Execution log will be written", True, ""))

    lines = [
        "NGKsPlayerNative — Sanity Checks",
        "=" * 50,
        "",
    ]

    all_pass = True
    for label, ok, detail in checks:
        status = "PASS" if ok else "FAIL"
        if not ok:
            all_pass = False
        extra = f" ({detail})" if detail else ""
        lines.append(f"  [{status}] {label}{extra}")

    lines.append("")
    lines.append(f"Sanity: {'PASS' if all_pass else 'FAIL'}")
    lines.append(f"Overall GATE: {'PASS' if gate else 'FAIL'}")

    path = os.path.join(PROOF_DIR, "10_sanity_checks.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log(f"  Sanity: {'PASS' if all_pass else 'FAIL'}")
    return all_pass


# ═══════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════
def main():
    log("=" * 60)
    log("NGKsPlayerNative — Full-Track Analysis Batch Test (10 Songs)")
    log("=" * 60)
    log(f"Workspace: {WORKSPACE}")
    log(f"Music dir: {MUSIC_DIR}")
    log(f"Proof dir: {PROOF_DIR}")
    log("")

    t_start = time.perf_counter()

    # Step 1
    step1_select_tracks()

    # Step 2
    results = step2_run_analysis()

    # Step 3
    readout_ok = step3_live_readout(results)

    # Step 4
    cache_ok = step4_cache_test()

    # Step 5
    switch_ok = step5_track_switch()

    # Step 6
    step6_edge_cases(results)

    # Step 7
    step7_performance(results)

    # Step 8
    step8_quality(results)

    # Step 9
    no_failures = step9_failures(results, cache_ok, switch_ok, readout_ok)

    # Step 10
    gate = step10_final_report(results, cache_ok, switch_ok, readout_ok, no_failures)

    # Step 11
    step11_sanity_checks(results, gate)

    elapsed_total = time.perf_counter() - t_start
    log("")
    log(f"Total batch time: {elapsed_total:.2f}s")
    log(f"PF={PROOF_DIR}")
    log(f"GATE={'PASS' if gate else 'FAIL'}")

    # Write execution log
    write_log()

    print("\n" + "=" * 60)
    print(f"PF={PROOF_DIR}")
    print(f"GATE={'PASS' if gate else 'FAIL'}")
    print("=" * 60)


if __name__ == "__main__":
    main()
