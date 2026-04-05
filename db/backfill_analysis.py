#!/usr/bin/env python3
"""
Analysis Backfill — Phase 3
Populates analyzer_runs, analysis_summary, section_events, and track_corrections
from trusted historical analysis data sources.

Sources (priority order):
  1. _artifacts/exports/NGKs_final_analyzer_export_OVERRIDDEN.csv  (907 rows — authoritative BPM/key with override audit)
  2. Validated 02_analysis_results.csv                              (907 rows — LUFS, energy, danceability, cue/section data)
  3. %APPDATA%/ngksplayer/library.db                                (960 rows — fallback BPM/energy/LUFS for unmatched tracks)

Match strategy: basename(tracks.file_path) == CSV Filename
"""

import csv
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

# ── paths ──────────────────────────────────────────────────────────────────
WORKSPACE     = Path(__file__).resolve().parent.parent
ANALYSIS_DB   = WORKSPACE / "db" / "song_analysis.db"
OVERRIDE_CSV  = WORKSPACE / "_artifacts" / "exports" / "NGKs_final_analyzer_export_OVERRIDDEN.csv"
VALIDATED_CSV = WORKSPACE / "Validated 02_analysis_results.csv"
LEGACY_DB     = Path(os.environ["APPDATA"]) / "ngksplayer" / "library.db"
PROOF_DIR     = WORKSPACE / "_proof" / "analysis_backfill"

ANALYZER_NAME    = "ngks_analysis_pipeline"
ANALYZER_VERSION = "v1.0-historical-backfill"
OVERRIDE_ANALYZER = "manual_override"
LEGACY_ANALYZER   = "legacy_library_import"
LEGACY_VERSION    = "v0.9-legacy"

# ── helpers ────────────────────────────────────────────────────────────────
def safe_float(val):
    """Convert to float, return None if empty/invalid."""
    if val is None or str(val).strip() == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_int(val):
    if val is None or str(val).strip() == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def normalize_0_100(val):
    """Convert 0-100 scale to 0.0-1.0. Return None if out of range or missing."""
    f = safe_float(val)
    if f is None:
        return None
    if f < 0 or f > 100:
        return None
    return round(f / 100.0, 6)


def read_csv_utf8(path):
    """Read CSV with utf-8-sig to handle BOM."""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


# ── main ───────────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    log: list[str] = []
    conflicts: list[str] = []

    def emit(msg: str):
        log.append(msg)
        print(msg)

    # ── validate inputs ────────────────────────────────────────────────────
    for p, label in [
        (ANALYSIS_DB, "analysis DB"),
        (OVERRIDE_CSV, "override CSV"),
        (VALIDATED_CSV, "validated CSV"),
        (LEGACY_DB, "legacy DB"),
    ]:
        if not p.exists():
            emit(f"FATAL: {label} not found: {p}")
            sys.exit(1)
        emit(f"SOURCE OK: {label} → {p} ({p.stat().st_size:,} bytes)")

    # ── load tracks from analysis DB ───────────────────────────────────────
    ana = sqlite3.connect(str(ANALYSIS_DB))
    ana.execute("PRAGMA journal_mode=WAL;")
    ana.execute("PRAGMA foreign_keys=ON;")

    tracks = {}  # basename → (id, file_path)
    tracks_by_path = {}  # full_path_lower → id
    cur = ana.cursor()
    cur.execute("SELECT id, file_path FROM tracks")
    for row in cur.fetchall():
        tid, fp = row
        bn = os.path.basename(fp)
        if bn in tracks:
            conflicts.append(f"BASENAME_DUP tracks.id={tid} basename={bn} (existing id={tracks[bn][0]})")
        else:
            tracks[bn] = (tid, fp)
        tracks_by_path[fp.lower()] = tid
    emit(f"Tracks in DB: {len(tracks_by_path)} total, {len(tracks)} unique basenames")

    # ── load override CSV ──────────────────────────────────────────────────
    override_rows = read_csv_utf8(str(OVERRIDE_CSV))
    emit(f"Override CSV: {len(override_rows)} rows")

    # ── load validated CSV ─────────────────────────────────────────────────
    validated_rows = read_csv_utf8(str(VALIDATED_CSV))
    validated_by_fn = {r["Filename"]: r for r in validated_rows}
    emit(f"Validated CSV: {len(validated_rows)} rows")

    # ── load legacy DB ─────────────────────────────────────────────────────
    legacy = sqlite3.connect(str(LEGACY_DB))
    legacy.row_factory = sqlite3.Row
    legacy_rows = legacy.execute(
        "SELECT filePath, bpm, bpmConfidence, camelotKey, key, keyConfidence, "
        "loudnessLUFS, energy, danceability, acousticness, instrumentalness, liveness "
        "FROM tracks WHERE bpm IS NOT NULL AND bpm > 0"
    ).fetchall()
    legacy.close()
    legacy_by_fn = {os.path.basename(r["filePath"]): dict(r) for r in legacy_rows if r["filePath"]}
    emit(f"Legacy DB: {len(legacy_rows)} rows with BPM > 0")

    # ══════════════════════════════════════════════════════════════════════
    # PHASE A: PRIMARY — Override CSV → analyzer_runs + analysis_summary
    # ══════════════════════════════════════════════════════════════════════
    emit("\n=== PHASE A: Override CSV → analyzer_runs + analysis_summary ===")

    runs_inserted = 0
    summaries_inserted = 0
    sections_inserted = 0
    corrections_inserted = 0
    matched_csv = 0
    unmatched_csv = 0
    csv_matched_set: set[str] = set()  # basenames matched from CSV
    legacy_only_set: set[str] = set()

    for ov_row in override_rows:
        filename = ov_row.get("Filename", "").strip()
        if not filename:
            conflicts.append(f"SKIP override row: empty Filename")
            continue

        if filename not in tracks:
            unmatched_csv += 1
            conflicts.append(f"UNMATCHED_CSV filename={filename} (no tracks.file_path with this basename)")
            continue

        track_id, file_path = tracks[filename]
        matched_csv += 1
        csv_matched_set.add(filename)

        # ── get validated row for supplementary fields ─────────────────
        val_row = validated_by_fn.get(filename, {})

        # ── BPM / Key from override CSV ────────────────────────────────
        final_bpm = safe_float(ov_row.get("FinalBPM"))
        bpm_conf = safe_float(ov_row.get("FinalBPMConfidence"))
        final_key = ov_row.get("FinalKey", "").strip() or None
        key_conf = safe_float(ov_row.get("FinalKeyConfidence"))

        # ── Supplementary from validated CSV ───────────────────────────
        lufs = safe_float(val_row.get("LUFS_I"))
        energy = normalize_0_100(val_row.get("Energy"))
        danceability = normalize_0_100(val_row.get("Danceability"))

        # ── skip if no usable analysis at all ──────────────────────────
        if final_bpm is None and final_key is None:
            conflicts.append(f"SKIP_NODATA filename={filename} track_id={track_id}")
            continue

        # ── INSERT analyzer_run ────────────────────────────────────────
        config = {
            "source_csv": "NGKs_final_analyzer_export_OVERRIDDEN.csv",
            "trust_level_bpm": ov_row.get("FinalBPMTrustLevel", ""),
            "trust_level_key": ov_row.get("FinalKeyTrustLevel", ""),
            "decision_source_bpm": ov_row.get("FinalBPMDecisionSource", ""),
            "decision_source_key": ov_row.get("FinalKeyDecisionSource", ""),
        }
        cur.execute(
            """INSERT INTO analyzer_runs
               (track_id, analyzer_name, analyzer_version, config_json, status, finished_at)
               VALUES (?, ?, ?, ?, 'completed', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))""",
            (track_id, ANALYZER_NAME, ANALYZER_VERSION, json.dumps(config)),
        )
        run_id = cur.lastrowid
        runs_inserted += 1

        # ── INSERT analysis_summary ────────────────────────────────────
        cur.execute(
            """INSERT INTO analysis_summary
               (run_id, track_id, bpm, bpm_confidence, key_label, key_confidence,
                loudness_lufs, energy, danceability)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, track_id, final_bpm, bpm_conf, final_key, key_conf,
             lufs, energy, danceability),
        )
        summaries_inserted += 1

        # ── INSERT section_events from validated CSV ───────────────────
        cue_in = safe_float(val_row.get("CueIn_s"))
        cue_out = safe_float(val_row.get("CueOut_s"))
        intro_dur = safe_float(val_row.get("IntroDuration_s"))
        outro_dur = safe_float(val_row.get("OutroDuration_s"))
        duration = safe_float(val_row.get("Duration_s"))

        sec_idx = 0
        if intro_dur is not None and intro_dur > 0:
            start = cue_in if cue_in is not None else 0.0
            end = start + intro_dur
            if end > start:
                cur.execute(
                    """INSERT INTO section_events
                       (run_id, track_id, section_index, label, start_sec, end_sec)
                       VALUES (?, ?, ?, 'intro', ?, ?)""",
                    (run_id, track_id, sec_idx, round(start, 3), round(end, 3)),
                )
                sections_inserted += 1
                sec_idx += 1

        if cue_in is not None and cue_out is not None and cue_out > cue_in:
            cur.execute(
                """INSERT INTO section_events
                   (run_id, track_id, section_index, label, start_sec, end_sec)
                   VALUES (?, ?, ?, 'playable', ?, ?)""",
                (run_id, track_id, sec_idx, round(cue_in, 3), round(cue_out, 3)),
            )
            sections_inserted += 1
            sec_idx += 1

        if outro_dur is not None and outro_dur > 0 and duration is not None:
            start = duration - outro_dur
            if start > 0 and start < duration:
                cur.execute(
                    """INSERT INTO section_events
                       (run_id, track_id, section_index, label, start_sec, end_sec)
                       VALUES (?, ?, ?, 'outro', ?, ?)""",
                    (run_id, track_id, sec_idx, round(start, 3), round(duration, 3)),
                )
                sections_inserted += 1

        # ── INSERT track_corrections if override was applied ───────────
        override_applied = ov_row.get("OverrideApplied", "").strip().lower() == "true"
        if override_applied:
            bpm_ov = ov_row.get("OverrideBPMApplied", "").strip()
            key_ov = ov_row.get("OverrideKeyApplied", "").strip()
            orig_bpm = ov_row.get("FinalBPM_Original", "").strip()
            orig_key = ov_row.get("FinalKey_Original", "").strip()
            reason = ov_row.get("OverrideReasonSummary", "").strip()
            source = ov_row.get("OverrideSource", "manual").strip()

            if bpm_ov:
                cur.execute(
                    """INSERT INTO track_corrections
                       (track_id, field, original_value, corrected_value, reason, corrected_by)
                       VALUES (?, 'bpm', ?, ?, ?, ?)""",
                    (track_id, orig_bpm or None, bpm_ov, reason or "manual override", source or "user"),
                )
                corrections_inserted += 1

            if key_ov:
                cur.execute(
                    """INSERT INTO track_corrections
                       (track_id, field, original_value, corrected_value, reason, corrected_by)
                       VALUES (?, 'key', ?, ?, ?, ?)""",
                    (track_id, orig_key or None, key_ov, reason or "manual override", source or "user"),
                )
                corrections_inserted += 1

    emit(f"Phase A: matched={matched_csv}, unmatched={unmatched_csv}")
    emit(f"  runs_inserted={runs_inserted}  summaries={summaries_inserted}")
    emit(f"  sections={sections_inserted}  corrections={corrections_inserted}")

    # ══════════════════════════════════════════════════════════════════════
    # PHASE B: LEGACY FALLBACK — for tracks NOT covered by CSV
    # ══════════════════════════════════════════════════════════════════════
    emit("\n=== PHASE B: Legacy DB fallback for uncovered tracks ===")
    legacy_inserted = 0
    legacy_skipped = 0

    for bn, (track_id, fp) in tracks.items():
        if bn in csv_matched_set:
            continue  # already covered by Phase A

        leg = legacy_by_fn.get(bn)
        if not leg:
            legacy_skipped += 1
            continue

        bpm = safe_float(leg.get("bpm"))
        if bpm is None or bpm <= 0:
            legacy_skipped += 1
            continue

        bpm_conf = safe_float(leg.get("bpmConfidence"))
        camelot = (leg.get("camelotKey") or "").strip() or None
        key_conf = safe_float(leg.get("keyConfidence"))
        lufs = safe_float(leg.get("loudnessLUFS"))
        energy_raw = safe_float(leg.get("energy"))
        energy = round(energy_raw / 100.0, 6) if energy_raw is not None and 0 <= energy_raw <= 100 else None
        dance_raw = safe_float(leg.get("danceability"))
        dance = round(dance_raw / 100.0, 6) if dance_raw is not None and 0 <= dance_raw <= 100 else None

        config = {"source": "legacy_library.db", "note": "fallback for tracks not in primary analysis CSV"}
        cur.execute(
            """INSERT INTO analyzer_runs
               (track_id, analyzer_name, analyzer_version, config_json, status, finished_at)
               VALUES (?, ?, ?, ?, 'completed', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))""",
            (track_id, LEGACY_ANALYZER, LEGACY_VERSION, json.dumps(config)),
        )
        run_id = cur.lastrowid
        runs_inserted += 1

        cur.execute(
            """INSERT INTO analysis_summary
               (run_id, track_id, bpm, bpm_confidence, key_label, key_confidence,
                loudness_lufs, energy, danceability)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (run_id, track_id, bpm, bpm_conf, camelot, key_conf, lufs, energy, dance),
        )
        summaries_inserted += 1
        legacy_inserted += 1
        legacy_only_set.add(bn)

    emit(f"Phase B: legacy_inserted={legacy_inserted}  legacy_skipped={legacy_skipped}")
    emit(f"  Total runs={runs_inserted}  summaries={summaries_inserted}")
    emit(f"  Total sections={sections_inserted}  corrections={corrections_inserted}")

    # ── commit ─────────────────────────────────────────────────────────────
    ana.commit()
    emit("\nCOMMITTED")

    # ══════════════════════════════════════════════════════════════════════
    # VALIDATION
    # ══════════════════════════════════════════════════════════════════════
    emit("\n=== VALIDATION ===")
    counts = {}
    for table in ["analyzer_runs", "analysis_summary", "section_events", "track_corrections"]:
        c = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        counts[table] = c
        emit(f"  {table}: {c}")

    # orphan check
    orphan_runs = cur.execute(
        "SELECT COUNT(*) FROM analyzer_runs ar LEFT JOIN tracks t ON ar.track_id = t.id WHERE t.id IS NULL"
    ).fetchone()[0]
    orphan_summary = cur.execute(
        "SELECT COUNT(*) FROM analysis_summary a LEFT JOIN tracks t ON a.track_id = t.id WHERE t.id IS NULL"
    ).fetchone()[0]
    orphan_sections = cur.execute(
        "SELECT COUNT(*) FROM section_events se LEFT JOIN tracks t ON se.track_id = t.id WHERE t.id IS NULL"
    ).fetchone()[0]
    orphan_corrections = cur.execute(
        "SELECT COUNT(*) FROM track_corrections tc LEFT JOIN tracks t ON tc.track_id = t.id WHERE t.id IS NULL"
    ).fetchone()[0]
    emit(f"  Orphan runs: {orphan_runs}  summaries: {orphan_summary}  sections: {orphan_sections}  corrections: {orphan_corrections}")

    # FK check
    fk_violations = cur.execute("PRAGMA foreign_key_check;").fetchall()
    emit(f"  FK violations: {len(fk_violations)}")

    # duplicate run check (same track_id + analyzer_name should not appear more than once)
    dup_runs = cur.execute(
        "SELECT COUNT(*) FROM (SELECT track_id, analyzer_name FROM analyzer_runs GROUP BY track_id, analyzer_name HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    emit(f"  Duplicate runs (same track+analyzer): {dup_runs}")

    ana.close()

    # ── gate ───────────────────────────────────────────────────────────────
    all_ok = (
        orphan_runs == 0
        and orphan_summary == 0
        and orphan_sections == 0
        and orphan_corrections == 0
        and len(fk_violations) == 0
        and dup_runs == 0
        and runs_inserted > 0
    )
    gate = "PASS" if all_ok else "FAIL"
    elapsed = round(time.time() - t0, 2)

    summary = {
        "gate": gate,
        "elapsed_sec": elapsed,
        "csv_matched": matched_csv,
        "csv_unmatched": unmatched_csv,
        "legacy_fallback": legacy_inserted,
        "legacy_skipped": legacy_skipped,
        "total_analyzer_runs": counts["analyzer_runs"],
        "total_analysis_summary": counts["analysis_summary"],
        "total_section_events": counts["section_events"],
        "total_track_corrections": counts["track_corrections"],
        "orphan_runs": orphan_runs,
        "orphan_summaries": orphan_summary,
        "orphan_sections": orphan_sections,
        "orphan_corrections": orphan_corrections,
        "fk_violations": len(fk_violations),
        "duplicate_runs": dup_runs,
    }

    emit(f"\n{'='*60}")
    for k, v in summary.items():
        emit(f"  {k:35s}: {v}")
    emit(f"{'='*60}")
    emit(f"GATE={gate}")

    # ══════════════════════════════════════════════════════════════════════
    # PROOF FILES
    # ══════════════════════════════════════════════════════════════════════
    w = lambda name, text: (PROOF_DIR / name).write_text(text, encoding="utf-8")

    # 00_source_inventory
    w("00_source_inventory.txt", "\n".join([
        "=== ANALYSIS DATA SOURCE INVENTORY ===",
        f"",
        f"1. Override CSV: {OVERRIDE_CSV}",
        f"   Rows: {len(override_rows)}   Size: {OVERRIDE_CSV.stat().st_size:,} bytes",
        f"   Fields: FinalBPM, FinalBPMConfidence, FinalBPMTrustLevel, FinalKey, FinalKeyConfidence,",
        f"           OverrideApplied, OverrideBPMApplied, OverrideKeyApplied, etc.",
        f"   Authority: HIGHEST — contains final validated values with override audit trail",
        f"",
        f"2. Validated CSV: {VALIDATED_CSV}",
        f"   Rows: {len(validated_rows)}   Size: {VALIDATED_CSV.stat().st_size:,} bytes",
        f"   Fields: BPM, Key, LUFS_I, Energy, Danceability, CueIn_s, CueOut_s, IntroDuration_s,",
        f"           OutroDuration_s, SpectralCentroid_Hz, etc.",
        f"   Authority: HIGH — raw analysis results, used for supplementary fields",
        f"",
        f"3. Legacy library DB: {LEGACY_DB}",
        f"   Rows w/ BPM > 0: {len(legacy_rows)}   Size: {LEGACY_DB.stat().st_size:,} bytes",
        f"   Fields: bpm, bpmConfidence, camelotKey, loudnessLUFS, energy, danceability",
        f"   Authority: FALLBACK — used only for tracks not covered by CSVs",
        f"",
        f"4. JSON analysis cache: {WORKSPACE / 'analysis_cache'} (51 files)",
        f"   NOT USED for backfill — granular per-5s timeline data, not summary-level",
        f"   Reserved for future section_events enhancement",
    ]))

    # 01_field_mapping
    w("01_field_mapping.txt", "\n".join([
        "=== FIELD MAPPING ===",
        "",
        "analyzer_runs:",
        "  track_id        ← tracks.id (matched by basename)",
        "  analyzer_name   ← 'ngks_analysis_pipeline' (CSV) | 'legacy_library_import' (legacy)",
        "  analyzer_version← 'v1.0-historical-backfill' (CSV) | 'v0.9-legacy' (legacy)",
        "  config_json     ← {source_csv, trust levels, decision sources}",
        "  status          ← 'completed'",
        "",
        "analysis_summary:",
        "  bpm             ← FinalBPM (override CSV)",
        "  bpm_confidence  ← FinalBPMConfidence (override CSV) | bpmConfidence (legacy)",
        "  key_label       ← FinalKey (Camelot notation, override CSV) | camelotKey (legacy)",
        "  key_confidence  ← FinalKeyConfidence (override CSV) | keyConfidence (legacy)",
        "  loudness_lufs   ← LUFS_I (validated CSV) | loudnessLUFS (legacy)",
        "  energy          ← Energy/100 (validated CSV, normalized 0-1) | energy/100 (legacy)",
        "  danceability    ← Danceability/100 (validated CSV, normalized 0-1) | danceability/100 (legacy)",
        "",
        "section_events:",
        "  intro           ← CueIn_s + IntroDuration_s (validated CSV)",
        "  playable        ← CueIn_s → CueOut_s (validated CSV)",
        "  outro           ← (Duration_s - OutroDuration_s) → Duration_s (validated CSV)",
        "",
        "track_corrections:",
        "  field           ← 'bpm' if OverrideBPMApplied, 'key' if OverrideKeyApplied",
        "  original_value  ← FinalBPM_Original / FinalKey_Original",
        "  corrected_value ← OverrideBPMApplied / OverrideKeyApplied",
        "  reason          ← OverrideReasonSummary",
        "  corrected_by    ← OverrideSource",
    ]))

    # 02_tracks_matched
    match_lines = [f"CSV matched: {matched_csv}  CSV unmatched: {unmatched_csv}  Legacy fallback: {legacy_inserted}"]
    match_lines.append(f"Total tracks with analysis: {matched_csv + legacy_inserted} / {len(tracks_by_path)}")
    match_lines.append("")
    match_lines.append("--- CSV-matched basenames (first 30) ---")
    for bn in sorted(csv_matched_set)[:30]:
        tid, fp = tracks[bn]
        match_lines.append(f"  track_id={tid:4d}  {bn}")
    if legacy_only_set:
        match_lines.append("")
        match_lines.append("--- Legacy-only basenames (first 30) ---")
        for bn in sorted(legacy_only_set)[:30]:
            tid, fp = tracks[bn]
            match_lines.append(f"  track_id={tid:4d}  {bn}")
    w("02_tracks_matched.txt", "\n".join(match_lines))

    # 03_rows_inserted
    w("03_rows_inserted.txt", "\n".join([
        "=== ROWS INSERTED ===",
        f"analyzer_runs:      {counts['analyzer_runs']}",
        f"analysis_summary:   {counts['analysis_summary']}",
        f"section_events:     {counts['section_events']}",
        f"track_corrections:  {counts['track_corrections']}",
        f"",
        f"Breakdown:",
        f"  Phase A (CSV):    {matched_csv} runs + summaries",
        f"  Phase B (legacy): {legacy_inserted} runs + summaries",
        f"  Sections:         {sections_inserted} (from validated CSV cue/intro/outro)",
        f"  Corrections:      {corrections_inserted} (from override audit trail)",
    ]))

    # 04_conflicts_and_skips
    w("04_conflicts_and_skips.txt", "\n".join(conflicts) if conflicts else "(no conflicts)")

    # 05_validation_queries — will be written after running standalone queries
    ana2 = sqlite3.connect(str(ANALYSIS_DB))
    vq_lines = ["=== VALIDATION QUERIES ===", ""]
    for q in [
        "SELECT COUNT(*) FROM analyzer_runs;",
        "SELECT COUNT(*) FROM analysis_summary;",
        "SELECT COUNT(*) FROM section_events;",
        "SELECT COUNT(*) FROM track_corrections;",
        "SELECT COUNT(*) FROM analyzer_runs ar LEFT JOIN tracks t ON ar.track_id = t.id WHERE t.id IS NULL;",
        "PRAGMA foreign_key_check;",
        "SELECT analyzer_name, COUNT(*) FROM analyzer_runs GROUP BY analyzer_name;",
        "SELECT AVG(bpm), MIN(bpm), MAX(bpm) FROM analysis_summary WHERE bpm IS NOT NULL;",
        "SELECT label, COUNT(*) FROM section_events GROUP BY label ORDER BY COUNT(*) DESC;",
    ]:
        result = ana2.execute(q).fetchall()
        vq_lines.append(f"-- {q}")
        for r in result:
            vq_lines.append(f"   {r}")
        vq_lines.append("")
    ana2.close()
    w("05_validation_queries.txt", "\n".join(vq_lines))

    # 06_final_report
    w("06_final_report.txt", "\n".join([
        "=== ANALYSIS BACKFILL — FINAL REPORT ===",
        f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"GATE: {gate}",
        f"Elapsed: {elapsed}s",
        f"",
        f"Sources used:",
        f"  1. Override CSV:  {len(override_rows)} rows → {matched_csv} matched",
        f"  2. Validated CSV: {len(validated_rows)} rows → supplementary fields for matched tracks",
        f"  3. Legacy DB:     {len(legacy_rows)} rows → {legacy_inserted} fallback inserts",
        f"",
        f"Tables populated:",
        f"  analyzer_runs:     {counts['analyzer_runs']}",
        f"  analysis_summary:  {counts['analysis_summary']}",
        f"  section_events:    {counts['section_events']}",
        f"  track_corrections: {counts['track_corrections']}",
        f"",
        f"Integrity checks:",
        f"  Orphan analyzer_runs:     {orphan_runs}",
        f"  Orphan analysis_summary:  {orphan_summary}",
        f"  Orphan section_events:    {orphan_sections}",
        f"  Orphan track_corrections: {orphan_corrections}",
        f"  FK violations:            {len(fk_violations)}",
        f"  Duplicate runs:           {dup_runs}",
        f"",
        f"Conflicts/skips: {len(conflicts)}",
        f"  See 04_conflicts_and_skips.txt for details",
    ]))

    # execution_log
    w("execution_log.txt", "\n".join(log))

    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
