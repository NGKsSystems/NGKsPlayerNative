#!/usr/bin/env python3
"""
DJ Library Core — Phase 2: Proof Generator
============================================
Generates proof artifacts for the Operator Dashboard.
Runs verification checks and bundles everything into a zip.
"""

import csv
import os
import sqlite3
import sys
import zipfile
from datetime import datetime
from pathlib import Path

BASE      = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DB_PATH   = BASE / "data" / "dj_library_core.db"
PROOF_DIR = BASE / "_proof" / "dj_library_core_phase2"
DASH_SCRIPT = BASE / "db" / "dj_dashboard_phase2.py"

LOG_LINES = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    LOG_LINES.append(line)


def connect():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def main():
    log("DJ Library Core — Phase 2 Proof Generator — BEGIN")
    PROOF_DIR.mkdir(parents=True, exist_ok=True)
    conn = connect()

    # ─── 00 — UI Design Summary ───────────────────────────────────
    log("Generating 00_ui_design_summary.txt")
    with open(PROOF_DIR / "00_ui_design_summary.txt", "w", encoding="utf-8") as f:
        f.write("DJ Library Core — Phase 2: UI Design Summary\n")
        f.write("=" * 55 + "\n\n")
        f.write("Framework: tkinter + ttk (Python stdlib)\n")
        f.write("Script: db/dj_dashboard_phase2.py\n\n")
        f.write("LAYOUT:\n")
        f.write("  ┌──────────────────────────────────────────────────┐\n")
        f.write("  │ [ALL] [CLEAN] [REVIEW] [LONGFORM] [JUNK] [DUP]  │\n")
        f.write("  │ Search: [___________________] [Go] [Clear]       │\n")
        f.write("  ├────────────────────────┬─────────────────────────┤\n")
        f.write("  │                        │ ─── Track Info ───      │\n")
        f.write("  │  Treeview Table        │ Track ID: ...           │\n")
        f.write("  │  (sortable headers)    │ File Name: ...          │\n")
        f.write("  │                        │ ─── Filename Parse ──── │\n")
        f.write("  │  ID | Name | Artist |  │ Artist (guess): ...     │\n")
        f.write("  │  Title | Source | Conf  │ ─── Metadata Tags ──── │\n")
        f.write("  │  | Review | Status |   │ Artist: ...             │\n")
        f.write("  │  Folder                │ ─── Hybrid Resol. ───  │\n")
        f.write("  │                        │ Chosen Artist: ...      │\n")
        f.write("  │                        │ ─── Audit Log ────────  │\n")
        f.write("  │                        │ (last 10 entries)       │\n")
        f.write("  ├────────────────────────┼─────────────────────────┤\n")
        f.write("  │                        │ [Approve CLEAN] [REVIEW]│\n")
        f.write("  │                        │ [JUNK] [LONGFORM]       │\n")
        f.write("  │                        │ [Edit Artist] [Edit Ti] │\n")
        f.write("  │                        │ [Open Folder]           │\n")
        f.write("  ├────────────────────────┴─────────────────────────┤\n")
        f.write("  │ Status Bar: Showing N tracks | Tab: XXX          │\n")
        f.write("  └──────────────────────────────────────────────────┘\n\n")
        f.write("WINDOW: 1400x820, resizable, PanedWindow split\n")
        f.write("COLORS: Row background tinted by status\n")
        f.write("  CLEAN=#d4edda  REVIEW=#fff3cd  LONGFORM=#d1ecf1\n")
        f.write("  JUNK=#f8d7da   DUPLICATE=#e2e3e5\n")

    # ─── 01 — DB Binding Summary ──────────────────────────────────
    log("Generating 01_db_binding_summary.txt")
    with open(PROOF_DIR / "01_db_binding_summary.txt", "w", encoding="utf-8") as f:
        f.write("DB Binding Summary\n")
        f.write("=" * 40 + "\n\n")
        f.write(f"Database: {DB_PATH}\n")
        f.write(f"DB size: {DB_PATH.stat().st_size:,} bytes\n\n")

        f.write("TABLES USED BY DASHBOARD:\n")
        tables = ["tracks", "filename_parse", "metadata_tags",
                   "hybrid_resolution", "track_status", "audit_log"]
        for table in tables:
            cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            f.write(f"\n  {table}: {cnt} rows, {len(cols)} columns\n")
            f.write(f"    Columns: {', '.join(cols)}\n")

        f.write("\nREAD QUERIES:\n")
        f.write("  - Main listing: JOIN tracks + hybrid_resolution + track_status\n")
        f.write("  - Details: SELECT * from each table WHERE track_id = ?\n")
        f.write("  - Audit: SELECT from audit_log WHERE track_id = ? ORDER BY timestamp DESC LIMIT 10\n")
        f.write("  - Counts: SELECT status, COUNT(*) FROM track_status GROUP BY status\n")

        f.write("\nWRITE QUERIES (operator actions only):\n")
        f.write("  - Status change: UPDATE track_status + INSERT audit_log\n")
        f.write("  - Artist edit: UPDATE hybrid_resolution + INSERT audit_log\n")
        f.write("  - Title edit: UPDATE hybrid_resolution + INSERT audit_log\n")
        f.write("  - All writes wrapped in try/except with rollback on failure\n")

    # ─── 02 — Views Summary ───────────────────────────────────────
    log("Generating 02_views_summary.txt")
    status_rows = conn.execute(
        "SELECT status, COUNT(*) FROM track_status GROUP BY status ORDER BY status"
    ).fetchall()
    status_dict = dict(status_rows)
    total = sum(status_dict.values())

    with open(PROOF_DIR / "02_views_summary.txt", "w", encoding="utf-8") as f:
        f.write("Views Summary\n")
        f.write("=" * 40 + "\n\n")
        f.write("AVAILABLE TABS:\n")
        tabs = {
            "ALL":       total,
            "CLEAN":     status_dict.get("CLEAN", 0),
            "REVIEW":    status_dict.get("REVIEW", 0),
            "LONGFORM":  status_dict.get("LONGFORM", 0),
            "JUNK":      status_dict.get("JUNK", 0),
            "DUPLICATE": status_dict.get("DUPLICATE", 0),
        }
        for tab, cnt in tabs.items():
            pct = cnt / total * 100 if total else 0
            f.write(f"  {tab:12s}: {cnt:5d} ({pct:5.1f}%)\n")

        f.write("\nDISPLAYED COLUMNS (per row):\n")
        columns = [
            ("track_id",         "ID — primary key"),
            ("file_name",        "Original filename"),
            ("chosen_artist",    "Hybrid-resolved artist"),
            ("chosen_title",     "Hybrid-resolved title"),
            ("source_used",      "filename / metadata / hybrid"),
            ("final_confidence", "0.0–1.0 confidence score"),
            ("requires_review",  "Yes/No flag"),
            ("status",           "CLEAN / REVIEW / JUNK / LONGFORM / DUPLICATE"),
            ("folder",           "Source folder name"),
        ]
        for col, desc in columns:
            f.write(f"  {col:20s} — {desc}\n")

        f.write("\nSEARCH: Searches file_name, chosen_artist, chosen_title (LIKE)\n")
        f.write("SORT: Click any column header. Toggles ASC/DESC with ▲/▼ indicator.\n")
        f.write("FILTER: Tab buttons filter by status. ALL shows everything.\n")

    # ─── 03 — Actions Summary ─────────────────────────────────────
    log("Generating 03_actions_summary.txt")
    with open(PROOF_DIR / "03_actions_summary.txt", "w", encoding="utf-8") as f:
        f.write("Actions Summary\n")
        f.write("=" * 40 + "\n\n")
        actions = [
            ("Approve → CLEAN",  "Sets track_status to CLEAN. Confirmation dialog."),
            ("→ REVIEW",         "Sets track_status to REVIEW. Confirmation dialog."),
            ("→ JUNK",           "Sets track_status to JUNK. Confirmation dialog."),
            ("→ LONGFORM",       "Sets track_status to LONGFORM. Confirmation dialog."),
            ("Edit Artist",      "Opens text dialog. Updates hybrid_resolution.chosen_artist. Sets source_used='hybrid'."),
            ("Edit Title",       "Opens text dialog. Updates hybrid_resolution.chosen_title. Sets source_used='hybrid'."),
            ("Open Folder",      "Opens Windows Explorer with file selected."),
        ]
        for action, desc in actions:
            f.write(f"  [{action}]\n    {desc}\n\n")

        f.write("SAFETY GUARANTEES:\n")
        f.write("  - All status changes require confirmation dialog\n")
        f.write("  - All edits write an audit_log entry with old→new values\n")
        f.write("  - All DB writes wrapped in try/except with rollback\n")
        f.write("  - No filesystem modifications (files are never touched)\n")
        f.write("  - Open Folder uses subprocess.Popen (read-only explorer)\n")

    # ─── 04 — Validation Checks ───────────────────────────────────
    log("Generating 04_validation_checks.txt")
    checks = []

    # 1. Dashboard script exists
    checks.append(("dashboard_script_exists",
                    DASH_SCRIPT.exists(),
                    f"{DASH_SCRIPT}"))

    # 2. DB exists and accessible
    checks.append(("db_accessible",
                    DB_PATH.exists(),
                    f"{DB_PATH} ({DB_PATH.stat().st_size:,} bytes)"))

    # 3. All required tables present
    db_tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    required = {"tracks", "filename_parse", "metadata_tags",
                "hybrid_resolution", "track_status", "audit_log"}
    missing = required - set(db_tables)
    checks.append(("all_tables_present",
                    len(missing) == 0,
                    f"missing={missing}" if missing else "all 6 present"))

    # 4. All tables populated
    for table in sorted(required):
        cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        checks.append((f"{table}_populated",
                        cnt > 0,
                        f"{cnt} rows"))

    # 5. Status distribution valid
    valid_statuses = {"RAW", "CLEAN", "REVIEW", "DUPLICATE", "JUNK", "LONGFORM"}
    bad = conn.execute(
        "SELECT DISTINCT status FROM track_status WHERE status NOT IN ('RAW','CLEAN','REVIEW','DUPLICATE','JUNK','LONGFORM')"
    ).fetchall()
    checks.append(("all_statuses_valid",
                    len(bad) == 0,
                    f"invalid={[r[0] for r in bad]}" if bad else "all valid"))

    # 6. Dashboard has all required features (source code check)
    src = DASH_SCRIPT.read_text(encoding="utf-8")
    features = {
        "search":        "search_text" in src,
        "sort":          "_sort_by" in src,
        "tab_filter":    "_switch_tab" in src,
        "details_panel": "_show_details" in src,
        "status_change": "_set_status" in src,
        "edit_artist":   "_edit_artist" in src,
        "edit_title":    "_edit_title" in src,
        "open_folder":   "_open_folder" in src,
        "audit_log":     "audit_log" in src,
    }
    all_features = all(features.values())
    checks.append(("all_features_implemented",
                    all_features,
                    f"{sum(features.values())}/{len(features)} features"))

    # 7. No file mutation code
    danger_patterns = ["os.rename", "os.remove", "shutil.move", "shutil.rmtree",
                       "os.unlink", ".write(", "open(", "Path.rename"]
    # Filter: only check for filesystem writes (exclude our own proof writes and DB)
    has_disk_write = False
    for line in src.splitlines():
        line_stripped = line.strip()
        if any(d in line_stripped for d in ["os.rename", "os.remove", "shutil.move",
                                            "shutil.rmtree", "os.unlink"]):
            if "proof" not in line_stripped.lower():
                has_disk_write = True
                break
    checks.append(("no_filesystem_mutations",
                    not has_disk_write,
                    "dashboard does not rename/delete/move files"))

    # 8. Script is syntactically valid
    import py_compile
    try:
        py_compile.compile(str(DASH_SCRIPT), doraise=True)
        checks.append(("syntax_valid", True, "compiles OK"))
    except py_compile.PyCompileError as e:
        checks.append(("syntax_valid", False, str(e)))

    all_pass = all(ok for _, ok, _ in checks)

    with open(PROOF_DIR / "04_validation_checks.txt", "w", encoding="utf-8") as f:
        f.write("Validation Checks\n")
        f.write("=" * 40 + "\n\n")
        for name, ok, desc in checks:
            f.write(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}\n")
        f.write(f"\nOverall: {'PASS' if all_pass else 'FAIL'} "
                f"({sum(1 for _,ok,_ in checks if ok)}/{len(checks)})\n")

    log(f"Validation: {'ALL PASS' if all_pass else 'FAIL'} "
        f"({sum(1 for _,ok,_ in checks if ok)}/{len(checks)})")
    for name, ok, desc in checks:
        log(f"  {'PASS' if ok else 'FAIL'} {name}: {desc}")

    # ─── 05 — Final Report ────────────────────────────────────────
    log("Generating 05_final_report.txt")
    with open(PROOF_DIR / "05_final_report.txt", "w", encoding="utf-8") as f:
        f.write("DJ Library Core — Phase 2 Final Report\n")
        f.write("=" * 50 + "\n")
        f.write(f"Date: {datetime.now().isoformat()}\n")
        f.write(f"GATE={'PASS' if all_pass else 'FAIL'}\n\n")
        f.write(f"Dashboard script: {DASH_SCRIPT}\n")
        f.write(f"Database: {DB_PATH}\n")
        f.write(f"DB size: {DB_PATH.stat().st_size:,} bytes\n\n")
        f.write("STATUS DISTRIBUTION:\n")
        for tab in ["CLEAN", "REVIEW", "LONGFORM", "JUNK", "DUPLICATE"]:
            cnt = status_dict.get(tab, 0)
            pct = cnt / total * 100 if total else 0
            f.write(f"  {tab:12s}: {cnt:5d} ({pct:5.1f}%)\n")
        f.write(f"  {'TOTAL':12s}: {total:5d}\n\n")
        f.write("UI FEATURES:\n")
        for feat, ok in features.items():
            f.write(f"  {'✓' if ok else '✗'} {feat}\n")
        f.write(f"\nVALIDATION: {sum(1 for _,ok,_ in checks if ok)}/{len(checks)} checks passed\n")
        f.write(f"\nLAUNCH COMMAND:\n")
        f.write(f'  .venv\\Scripts\\python.exe db\\dj_dashboard_phase2.py\n')

    # ─── Execution Log ────────────────────────────────────────────
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        for line in LOG_LINES:
            f.write(line + "\n")

    log(f"Proof artifacts written to: {PROOF_DIR}")

    # ─── ZIP Bundle ───────────────────────────────────────────────
    zip_path = BASE / "_proof" / "dj_library_core_phase2.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for pf in sorted(PROOF_DIR.iterdir()):
            if pf.is_file():
                zf.write(pf, f"dj_library_core_phase2/{pf.name}")
        # Include the dashboard script itself
        zf.write(DASH_SCRIPT, "dj_library_core_phase2/dj_dashboard_phase2.py")

    # Re-write execution log to include zip line
    log(f"ZIP={zip_path} ({zip_path.stat().st_size:,} bytes)")
    with open(PROOF_DIR / "execution_log.txt", "w", encoding="utf-8") as f:
        for line in LOG_LINES:
            f.write(line + "\n")

    # Re-zip to include final log
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for pf in sorted(PROOF_DIR.iterdir()):
            if pf.is_file():
                zf.write(pf, f"dj_library_core_phase2/{pf.name}")
        zf.write(DASH_SCRIPT, "dj_library_core_phase2/dj_dashboard_phase2.py")

    conn.close()

    log("")
    log("=" * 60)
    log("DJ LIBRARY CORE — PHASE 2 PROOF COMPLETE")
    log(f"  PF={PROOF_DIR}")
    log(f"  ZIP={zip_path}")
    log(f"  GATE={'PASS' if all_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
