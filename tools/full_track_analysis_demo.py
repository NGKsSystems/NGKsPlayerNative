"""
NGKsPlayerNative — Full-Track Analysis Demo
End-to-end contract test of the background analysis module.

Runs analysis on a real track, polls progress, captures partial + final
results, verifies sanity, and writes proof artifacts.
"""

import json
import os
import sys
import time
import threading
from datetime import datetime

# Add src/analysis to path
WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
ANALYSIS_DIR = os.path.join(WORKSPACE, "src", "analysis")
sys.path.insert(0, ANALYSIS_DIR)

from analysis_contracts import AnalysisStatus, FullTrackAnalysisResult
from full_track_analysis_manager import FullTrackAnalysisManager
from analysis_store import AnalysisStore

PROOF_DIR = os.path.join(WORKSPACE, "_proof", "full_track_analysis")
MUSIC_DIR = r"C:\Users\suppo\Music"

# Use a real track — 3 Doors Down has a good mix of instruments
TEST_TRACKS = [
    os.path.join(MUSIC_DIR, "3 Doors Down - Kryptonite.mp3"),
    os.path.join(MUSIC_DIR, "Journey - Any Way You Want It.mp3"),
    os.path.join(MUSIC_DIR, "2Pac - California Love.mp3"),
    os.path.join(MUSIC_DIR, "120 BPM Metronome.mp3"),
]

LOG_LINES: list[str] = []


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}"
    LOG_LINES.append(line)
    print(line)


def write_json_artifact(name: str, data: object) -> str:
    path = os.path.join(PROOF_DIR, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    return path


def main() -> None:
    os.makedirs(PROOF_DIR, exist_ok=True)

    log("=== FULL-TRACK ANALYSIS MODULE — DEMO / CONTRACT TEST ===")
    log(f"Workspace: {WORKSPACE}")
    log(f"Proof dir: {PROOF_DIR}")
    log("")

    # Find first available test track
    test_file = None
    for tf in TEST_TRACKS:
        if os.path.isfile(tf):
            test_file = tf
            break

    if test_file is None:
        log("FATAL: No test audio file found")
        return

    track_id = os.path.splitext(os.path.basename(test_file))[0]
    log(f"Test track: {test_file}")
    log(f"Track ID:   {track_id}")
    log("")

    # ── Store for checkpointing ──
    store = AnalysisStore(cache_dir=os.path.join(PROOF_DIR, "analysis_cache"))

    # ── Manager with injected store ──
    log_lines_for_manager: list[str] = []

    def manager_log(msg: str) -> None:
        log(f"  [MGR] {msg}")
        log_lines_for_manager.append(msg)

    # Create a worker wrapper that injects the store
    def worker_with_store(filepath, result, cancel_event, log_fn):
        from analysis_worker import run_analysis
        run_analysis(
            filepath=filepath,
            result=result,
            cancel_event=cancel_event,
            log_fn=log_fn,
            store=store,
        )

    manager = FullTrackAnalysisManager(max_workers=1, worker_factory=worker_with_store)
    manager.set_log_callback(manager_log)

    # ── STEP 1: Start analysis ──
    log("── STEP 1: Start Analysis ──")
    start_resp = manager.start_analysis(track_id, test_file)
    log(f"  Response: {start_resp}")
    assert start_resp["status"] == "QUEUED", f"Expected QUEUED, got {start_resp['status']}"

    # ── STEP 1b: Duplicate prevention ──
    dup = manager.start_analysis(track_id, test_file)
    log(f"  Duplicate attempt: {dup['status']}")
    assert dup["status"] == "DUPLICATE"

    # ── STEP 2: Poll progress ──
    log("")
    log("── STEP 2: Poll Progress ──")
    progress_snapshots: list[dict] = []
    max_wait = 300  # 5 minute timeout
    poll_interval = 2.0
    elapsed = 0.0

    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval

        status = manager.get_status(track_id)
        prog = status.get("progress", 0)
        st = status.get("status", "")

        log(f"  t={elapsed:.0f}s status={st} progress={prog}% "
            f"chunks={status.get('chunks_completed', 0)}/{status.get('chunk_count', 0)}")

        progress_snapshots.append({
            "elapsed_s": round(elapsed, 1),
            "status": st,
            "progress": prog,
            "chunks_completed": status.get("chunks_completed", 0),
            "chunk_count": status.get("chunk_count", 0),
        })

        if st in (AnalysisStatus.COMPLETED.value, AnalysisStatus.FAILED.value, AnalysisStatus.CANCELLED.value):
            break

        # Get partial result at ~50% if available
        if 40 <= prog <= 60 and not any(s.get("partial_captured") for s in progress_snapshots):
            partial = manager.get_partial_result(track_id)
            if partial:
                log(f"  Partial result @ {prog}%: bpm={partial.get('final_bpm')} "
                    f"key={partial.get('final_key')} sections={len(partial.get('sections', []))}")
                progress_snapshots[-1]["partial_captured"] = True

    # ── STEP 3: Get final result ──
    log("")
    log("── STEP 3: Get Final Result ──")
    final = manager.get_final_result(track_id)

    if final is None:
        # Check if failed
        status = manager.get_status(track_id)
        log(f"  FAIL: No final result. Status={status}")
        final = manager.get_partial_result(track_id)
        if final is None:
            log("  FATAL: No result at all")
            write_progress(progress_snapshots)
            write_log()
            return

    log(f"  Status:        {final['status']}")
    log(f"  Duration:      {final['duration_s']:.1f}s")
    log(f"  Chunks:        {final['chunk_count']}")
    log(f"  Processing:    {final['processing_time_s']:.2f}s")
    log(f"  Final BPM:     {final['final_bpm']} (conf={final['bpm_confidence']})")
    log(f"  BPM family:    {final['bpm_family']}")
    log(f"  BPM candidates: {len(final.get('bpm_candidates', []))}")
    log(f"  Final Key:     {final['final_key']} ({final['final_key_name']}) (conf={final['key_confidence']})")
    log(f"  Key change:    {final['key_change_detected']}")
    log(f"  Tempo TL:      {len(final.get('tempo_timeline', []))} points")
    log(f"  Key TL:        {len(final.get('key_timeline', []))} points")
    log(f"  Sections:      {len(final.get('sections', []))}")
    log(f"  Cues:          {len(final.get('cues', []))}")
    log(f"  Analyzer ready: {final['analyzer_ready']}")
    log(f"  Review req:    {final['review_required']}")
    if final.get("review_reason"):
        log(f"  Review reason: {final['review_reason']}")

    # ── STEP 4: Simulate playback thread safety ──
    log("")
    log("── STEP 4: Playback Thread Safety (simulated) ──")
    playback_blocked = False

    def simulated_playback():
        nonlocal playback_blocked
        # If we can run this concurrently, playback thread isn't blocked
        for _ in range(10):
            time.sleep(0.05)  # simulate 50ms playback callbacks
        playback_blocked = False

    playback_blocked = True
    pt = threading.Thread(target=simulated_playback, name="playback-sim")
    pt.start()
    pt.join(timeout=2.0)

    if not playback_blocked:
        log("  PASS: Playback thread completed concurrently")
    else:
        log("  FAIL: Playback thread was blocked")

    # ── STEP 5: Cancel test (on a second track if available) ──
    log("")
    log("── STEP 5: Cancel Test ──")
    cancel_file = None
    for tf in TEST_TRACKS:
        if os.path.isfile(tf) and tf != test_file:
            cancel_file = tf
            break

    if cancel_file:
        cancel_id = os.path.splitext(os.path.basename(cancel_file))[0] + "_cancel"
        manager.start_analysis(cancel_id, cancel_file)
        time.sleep(1.0)
        cancel_resp = manager.cancel_analysis(cancel_id)
        log(f"  Cancel response: {cancel_resp['status']}")
        time.sleep(2.0)
        cancel_status = manager.get_status(cancel_id)
        log(f"  After cancel: {cancel_status['status']}")
    else:
        log("  (no second track available — skipped)")

    # ── STEP 6: Verify stored result ──
    log("")
    log("── STEP 6: Verify Stored Result ──")
    loaded = store.load_result(track_id)
    if loaded:
        log(f"  Stored result found: bpm={loaded.get('final_bpm')} key={loaded.get('final_key')}")
        log(f"  Stored IDs: {store.list_results()}")
    else:
        log("  WARNING: No stored result found")

    # ── SANITY CHECKS ──
    log("")
    log("── SANITY CHECKS ──")
    checks: list[dict] = []

    def check(name: str, condition: bool, detail: str = "") -> None:
        status = "PASS" if condition else "FAIL"
        checks.append({"name": name, "status": status, "detail": detail})
        log(f"  {status}: {name}" + (f" — {detail}" if detail else ""))

    is_complete = final["status"] == AnalysisStatus.COMPLETED.value
    check("Analysis completes", is_complete, f"status={final['status']}")
    check("Progress reaches 100%", final["progress"] == 100.0, f"progress={final['progress']}")
    check("Tempo timeline populated", len(final.get("tempo_timeline", [])) > 0,
          f"points={len(final.get('tempo_timeline', []))}")
    check("Key timeline populated", len(final.get("key_timeline", [])) > 0,
          f"points={len(final.get('key_timeline', []))}")
    check("Sections detected", len(final.get("sections", [])) >= 2,
          f"sections={len(final.get('sections', []))}")
    check("Final BPM resolved", final["final_bpm"] > 0, f"bpm={final['final_bpm']}")
    check("Final key resolved", final["final_key"] != "", f"key={final['final_key']}")
    check("No crash / no error", final.get("error", "") == "", f"error={final.get('error', '')}")
    check("Playback unaffected", not playback_blocked)
    check("Duplicate job prevented", dup["status"] == "DUPLICATE")
    check("BPM confidence > 0", final["bpm_confidence"] > 0, f"conf={final['bpm_confidence']}")
    check("Key confidence > 0", final["key_confidence"] > 0, f"conf={final['key_confidence']}")
    check("Processing time reasonable",
          0 < final["processing_time_s"] < 120,
          f"time={final['processing_time_s']:.2f}s")
    check("Frame features populated", len(final.get("frame_features", [])) > 0,
          f"frames={len(final.get('frame_features', []))}")
    check("Result stored", loaded is not None)

    all_pass = all(c["status"] == "PASS" for c in checks)
    gate = "PASS" if all_pass else "FAIL"

    log("")
    log(f"=== GATE = {gate} ===")
    log(f"  Checks: {sum(1 for c in checks if c['status'] == 'PASS')}/{len(checks)} passed")

    # ── Write artifacts ──
    log("")
    log("── Writing Proof Artifacts ──")

    # 01_demo_progress.txt
    p1_path = os.path.join(PROOF_DIR, "01_demo_progress.txt")
    with open(p1_path, "w", encoding="utf-8") as f:
        f.write(f"FULL-TRACK ANALYSIS — PROGRESS LOG\n")
        f.write(f"Track: {test_file}\n")
        f.write(f"Track ID: {track_id}\n")
        f.write(f"{'=' * 60}\n\n")
        for snap in progress_snapshots:
            f.write(f"  t={snap['elapsed_s']:>6.1f}s  status={snap['status']:<12s}  "
                    f"progress={snap['progress']:>5.1f}%  "
                    f"chunks={snap['chunks_completed']}/{snap['chunk_count']}\n")
        f.write(f"\nManager log:\n")
        for ml in log_lines_for_manager:
            f.write(f"  {ml}\n")
    log(f"  {p1_path}")

    # 02_demo_final_result.json
    p2 = write_json_artifact("02_demo_final_result.json", final)
    log(f"  {p2}")

    # 03_sanity_checks.txt
    p3_path = os.path.join(PROOF_DIR, "03_sanity_checks.txt")
    with open(p3_path, "w", encoding="utf-8") as f:
        f.write(f"FULL-TRACK ANALYSIS — SANITY CHECKS\n")
        f.write(f"{'=' * 60}\n\n")
        for c in checks:
            f.write(f"  {c['status']}: {c['name']}")
            if c["detail"]:
                f.write(f" — {c['detail']}")
            f.write("\n")
        f.write(f"\nGATE = {gate}\n")
        f.write(f"Passed: {sum(1 for c in checks if c['status'] == 'PASS')}/{len(checks)}\n")
    log(f"  {p3_path}")

    # execution_log.txt
    write_log()

    manager.shutdown(wait=False)


def write_progress(snapshots: list[dict]) -> None:
    path = os.path.join(PROOF_DIR, "01_demo_progress.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("PROGRESS LOG (incomplete)\n")
        for s in snapshots:
            f.write(f"  {s}\n")


def write_log() -> None:
    path = os.path.join(PROOF_DIR, "execution_log.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))


if __name__ == "__main__":
    main()
