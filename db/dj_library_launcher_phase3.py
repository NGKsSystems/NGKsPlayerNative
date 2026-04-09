#!/usr/bin/env python3
"""
DJ Library Core — Phase 3: One-Click Intake Launcher
======================================================
Orchestrates incremental intake of new audio files into dj_library_core.db.
Reuses Phase 1 parsing/metadata/hybrid/classification logic via direct import.

READ-ONLY on the filesystem. DB writes only for new tracks.
All intake runs are fully auditable.
"""

import csv
import os
import sqlite3
import subprocess
import sys
import threading
import tkinter as tk
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

# ─── Ensure Phase 1 module is importable ───────────────────────────
BASE    = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DB_DIR  = BASE / "db"
DATA    = BASE / "data"
DB_PATH = DATA / "dj_library_core.db"

sys.path.insert(0, str(DB_DIR))
import dj_library_core_phase1 as p1

AUDIO_EXTS = {".mp3"}
PROOF_DIR  = BASE / "_proof" / "dj_library_core_phase3"
DASHBOARD  = DB_DIR / "dj_dashboard_phase2.py"


# ═══════════════════════════════════════════════════════════════════
# INCREMENTAL INGEST ENGINE
# ═══════════════════════════════════════════════════════════════════

class IntakeRun:
    """Executes an incremental intake run against the DB."""

    def __init__(self, intake_folder: Path, log_fn=None):
        self.intake_folder = intake_folder
        self.log = log_fn or print
        self.results = []          # list of dicts for CSV
        self.counters = Counter()  # inserted/skipped_existing/blocked/error
        self.status_counts = Counter()
        self.conn: sqlite3.Connection
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    def execute(self):
        """Full intake pipeline. Returns True on success."""
        self.log(f"═══ INTAKE RUN {self.run_id} ═══")
        self.log(f"Folder: {self.intake_folder}")

        if not self.intake_folder.exists():
            self.log(f"ERROR: Folder does not exist: {self.intake_folder}")
            return False
        if not self.intake_folder.is_dir():
            self.log(f"ERROR: Not a directory: {self.intake_folder}")
            return False

        # Scan candidates
        candidates = self._scan_folder()
        if not candidates:
            self.log("No audio files found in selected folder.")
            return True  # Not an error, just empty

        self.log(f"Found {len(candidates)} audio file(s)")

        # Open DB
        try:
            self.conn = sqlite3.connect(str(DB_PATH))
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA foreign_keys=ON")
        except Exception as e:
            self.log(f"FATAL: Cannot open DB: {e}")
            return False

        now = datetime.now().isoformat()

        # Log intake run start
        self.conn.execute(
            """INSERT INTO audit_log (track_id, event_type, event_description, timestamp)
               VALUES (NULL, ?, ?, ?)""",
            ("INTAKE_RUN_START",
             f"Intake run {self.run_id} from {self.intake_folder} ({len(candidates)} candidates)",
             now)
        )

        # Process each file
        for filepath in candidates:
            self._process_file(filepath, now)

        # Commit + log completion
        self.conn.execute(
            """INSERT INTO audit_log (track_id, event_type, event_description, timestamp)
               VALUES (NULL, ?, ?, ?)""",
            ("INTAKE_RUN_END",
             f"Intake run {self.run_id} complete: {dict(self.counters)}",
             datetime.now().isoformat())
        )
        self.conn.commit()
        self.conn.close()

        # Write output files
        self._write_results_csv()
        self._write_summary_csv()
        self._write_runlog()

        self.log("")
        self.log(f"═══ INTAKE COMPLETE ═══")
        self.log(f"  Inserted:         {self.counters['inserted']}")
        self.log(f"  Skipped existing: {self.counters['skipped_existing']}")
        self.log(f"  Blocked:          {self.counters['blocked']}")
        self.log(f"  Errors:           {self.counters['error']}")
        if self.status_counts:
            self.log(f"  New status: {dict(self.status_counts)}")
        return True

    def _scan_folder(self):
        """Enumerate audio files in folder (recursive)."""
        candidates = []
        for root, dirs, files in os.walk(str(self.intake_folder)):
            for fname in sorted(files):
                if Path(fname).suffix.lower() in AUDIO_EXTS:
                    candidates.append(Path(root) / fname)
        return candidates

    def _process_file(self, filepath: Path, now: str):
        """Process a single file through the full pipeline."""
        fp_str = str(filepath)
        source = "unknown"

        # Check if already in DB
        existing = self.conn.execute(
            "SELECT track_id FROM tracks WHERE file_path = ?", (fp_str,)
        ).fetchone()

        if existing:
            self.counters["skipped_existing"] += 1
            self.results.append({
                "file_path": fp_str,
                "result": "skipped_existing",
                "reason": f"Already in DB as track_id={existing[0]}",
                "track_id": existing[0],
            })
            return

        # Validate file
        if not filepath.exists():
            self.counters["blocked"] += 1
            self.results.append({
                "file_path": fp_str,
                "result": "blocked",
                "reason": "File does not exist",
                "track_id": "",
            })
            return

        try:
            st = filepath.stat()
            if st.st_size == 0:
                self.counters["blocked"] += 1
                self.results.append({
                    "file_path": fp_str,
                    "result": "blocked",
                    "reason": "Zero-byte file",
                    "track_id": "",
                })
                return
        except OSError as e:
            self.counters["error"] += 1
            self.results.append({
                "file_path": fp_str,
                "result": "error",
                "reason": f"stat failed: {e}",
                "track_id": "",
            })
            return

        # ─── STEP 1: Insert into tracks ───────────────────────────
        try:
            from mutagen.mp3 import MP3
            duration = None
            try:
                audio = MP3(fp_str)
                if audio.info:
                    duration = round(audio.info.length, 1)
            except Exception:
                pass

            folder_name = filepath.parent.name
            cur = self.conn.execute(
                """INSERT INTO tracks
                   (file_path, file_name, folder, file_size, duration, ingest_timestamp)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (fp_str, filepath.name, folder_name, st.st_size, duration, now)
            )
            track_id = cur.lastrowid
        except sqlite3.IntegrityError:
            # Race-condition guard: file_path UNIQUE constraint
            self.counters["skipped_existing"] += 1
            self.results.append({
                "file_path": fp_str,
                "result": "skipped_existing",
                "reason": "UNIQUE constraint (concurrent insert)",
                "track_id": "",
            })
            return
        except Exception as e:
            self.counters["error"] += 1
            self.results.append({
                "file_path": fp_str,
                "result": "error",
                "reason": f"tracks insert: {e}",
                "track_id": "",
            })
            return

        # ─── STEP 2: Filename parse (reuse Phase 1) ───────────────
        try:
            artist, title, conf, method = p1.parse_filename(filepath.name)
            self.conn.execute(
                """INSERT INTO filename_parse
                   (track_id, artist_guess, title_guess, parse_confidence, parse_method)
                   VALUES (?, ?, ?, ?, ?)""",
                (track_id, artist, title, conf, method)
            )
        except Exception as e:
            self.log(f"  WARN filename_parse for {filepath.name}: {e}")
            artist, title, conf = "", filepath.stem, 0.3

        # ─── STEP 3: Metadata extract (reuse Phase 1) ─────────────
        try:
            meta = p1.extract_and_score_metadata(fp_str)
            self.conn.execute(
                """INSERT INTO metadata_tags
                   (track_id, artist_tag, title_tag, album, genre, track_number,
                    tag_version, metadata_confidence, metadata_junk_flag, metadata_junk_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (track_id,
                 meta["artist_tag"], meta["title_tag"], meta["album"],
                 meta["genre"], meta["track_number"], meta["tag_version"],
                 meta["metadata_confidence"],
                 meta["metadata_junk_flag"], meta["metadata_junk_reason"])
            )
        except Exception as e:
            self.log(f"  WARN metadata_tags for {filepath.name}: {e}")
            meta = {
                "artist_tag": "", "title_tag": "", "metadata_confidence": 0.0,
                "metadata_junk_flag": 0, "metadata_junk_reason": "",
            }

        # ─── STEP 4: Hybrid resolution (reuse Phase 1 logic) ──────
        try:
            fn_artist = artist or ""
            fn_title = title or ""
            meta_artist = meta.get("artist_tag", "") or ""
            meta_title = meta.get("title_tag", "") or ""
            fn_conf = conf
            meta_conf = meta.get("metadata_confidence", 0.0)
            junk_flag = meta.get("metadata_junk_flag", 0)

            was_reversed = p1.check_reversed(fn_artist, fn_title, meta_artist, meta_title)
            artist_sim = p1.compute_similarity(fn_artist, meta_artist)
            title_sim = p1.compute_similarity(fn_title, meta_title)

            requires_review = False

            if artist_sim >= 0.7 and title_sim >= 0.7:
                if junk_flag:
                    chosen_artist, chosen_title, source = fn_artist, fn_title, "filename"
                else:
                    chosen_artist = meta_artist or fn_artist
                    chosen_title = meta_title or fn_title
                    source = "hybrid"
                final_conf = max(fn_conf, meta_conf) * ((artist_sim + title_sim) / 2)
            elif not meta_artist and not meta_title:
                chosen_artist, chosen_title, source = fn_artist, fn_title, "filename"
                final_conf = fn_conf * 0.8
            elif junk_flag:
                chosen_artist, chosen_title, source = fn_artist, fn_title, "filename"
                final_conf = fn_conf * 0.7
            elif was_reversed:
                chosen_artist, chosen_title, source = fn_artist, fn_title, "filename"
                final_conf = fn_conf * 0.6
                requires_review = True
            elif fn_conf >= meta_conf and fn_artist:
                chosen_artist, chosen_title, source = fn_artist, fn_title, "filename"
                final_conf = fn_conf * 0.7
                if artist_sim < 0.3 and meta_artist:
                    requires_review = True
            elif meta_conf > fn_conf and not junk_flag:
                chosen_artist, chosen_title, source = meta_artist, meta_title, "metadata"
                final_conf = meta_conf * 0.8
                if artist_sim < 0.3 and fn_artist:
                    requires_review = True
            else:
                chosen_artist = fn_artist or meta_artist
                chosen_title = fn_title or meta_title
                source = "filename" if fn_artist else "metadata"
                final_conf = max(fn_conf, meta_conf) * 0.5
                requires_review = True

            final_conf = round(min(1.0, max(0.0, final_conf)), 3)

            self.conn.execute(
                """INSERT INTO hybrid_resolution
                   (track_id, chosen_artist, chosen_title, source_used,
                    final_confidence, was_reversed, requires_review)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (track_id, chosen_artist, chosen_title, source,
                 final_conf, int(was_reversed), int(requires_review))
            )
        except Exception as e:
            self.log(f"  WARN hybrid_resolution for {filepath.name}: {e}")
            final_conf = 0.3
            requires_review = True
            junk_flag = 0

        # ─── STEP 5: Status assignment (reuse Phase 1 logic) ──────
        try:
            if p1.is_longform(filepath.name, st.st_size, duration):
                status = "LONGFORM"
            elif junk_flag and final_conf < 0.3:
                status = "JUNK"
            elif requires_review:
                status = "REVIEW"
            elif final_conf >= 0.5:
                status = "CLEAN"
            else:
                status = "REVIEW"

            self.conn.execute(
                """INSERT INTO track_status
                   (track_id, status, duplicate_group_id, is_primary)
                   VALUES (?, ?, NULL, 1)""",
                (track_id, status)
            )
            self.status_counts[status] += 1
        except Exception as e:
            self.log(f"  WARN track_status for {filepath.name}: {e}")
            status = "REVIEW"

        # ─── STEP 6: Audit log ────────────────────────────────────
        self.conn.execute(
            """INSERT INTO audit_log (track_id, event_type, event_description, timestamp)
               VALUES (?, ?, ?, ?)""",
            (track_id, "INTAKE_INSERTED",
             f"Intake run {self.run_id}: inserted from {self.intake_folder.name}, status={status}",
             now)
        )

        self.counters["inserted"] += 1
        self.results.append({
            "file_path": fp_str,
            "result": "inserted",
            "reason": f"status={status}, conf={final_conf:.3f}, source={source}",
            "track_id": track_id,
        })

    # ─── Output Writers ────────────────────────────────────────────

    def _write_results_csv(self):
        path = DATA / "intake_run_results_v1.csv"
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["file_path", "result", "reason", "track_id"])
            w.writeheader()
            w.writerows(self.results)
        self.log(f"Results CSV: {path} ({len(self.results)} rows)")

    def _write_summary_csv(self):
        path = DATA / "intake_run_summary_v1.csv"
        total = sum(self.counters.values())
        row = {
            "intake_folder": str(self.intake_folder),
            "files_scanned": total,
            "inserted": self.counters["inserted"],
            "skipped_existing": self.counters["skipped_existing"],
            "blocked": self.counters["blocked"],
            "errors": self.counters["error"],
            "clean_count": self.status_counts.get("CLEAN", 0),
            "review_count": self.status_counts.get("REVIEW", 0),
            "longform_count": self.status_counts.get("LONGFORM", 0),
            "junk_count": self.status_counts.get("JUNK", 0),
        }
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            w.writeheader()
            w.writerow(row)
        self.log(f"Summary CSV: {path}")

    def _write_runlog(self):
        path = DATA / "intake_launcher_runlog_v1.txt"
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"INTAKE RUN {self.run_id}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Folder: {self.intake_folder}\n")
            f.write(f"Results: {dict(self.counters)}\n")
            if self.status_counts:
                f.write(f"New statuses: {dict(self.status_counts)}\n")
            f.write(f"{'='*60}\n")
        self.log(f"Run log appended: {path}")


# ═══════════════════════════════════════════════════════════════════
# LAUNCHER UI
# ═══════════════════════════════════════════════════════════════════

class IntakeLauncher:
    """One-click intake launcher with tkinter UI."""

    def __init__(self, root):
        self.root = root
        self.root.title("DJ Library Core — Intake Launcher (Phase 3)")
        self.root.geometry("820x620")
        self.root.minsize(700, 500)

        self.folder_var = tk.StringVar()
        self.running = False

        self._build_ui()
        self._refresh_summary()

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(2, weight=1)

        # ─── Header ───────────────────────────────────────────────
        header = ttk.Frame(self.root, padding=8)
        header.grid(row=0, column=0, sticky="ew")
        ttk.Label(header, text="DJ Library Core — Intake Launcher",
                  font=("Segoe UI", 14, "bold")).pack(anchor="w")

        # ─── Controls ─────────────────────────────────────────────
        ctrl = ttk.LabelFrame(self.root, text="Intake Controls", padding=8)
        ctrl.grid(row=1, column=0, sticky="ew", padx=8, pady=4)
        ctrl.columnconfigure(1, weight=1)

        # Folder row
        ttk.Label(ctrl, text="Intake Folder:").grid(row=0, column=0, sticky="w", padx=(0, 6))
        folder_entry = ttk.Entry(ctrl, textvariable=self.folder_var)
        folder_entry.grid(row=0, column=1, sticky="ew", padx=2)
        ttk.Button(ctrl, text="Browse…", command=self._browse).grid(row=0, column=2, padx=2)

        # Buttons row
        btn_row = ttk.Frame(ctrl)
        btn_row.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(8, 0))

        self.run_btn = ttk.Button(btn_row, text="▶ Run Intake", command=self._run_intake)
        self.run_btn.pack(side="left", padx=4)

        ttk.Button(btn_row, text="Open Dashboard",
                   command=self._open_dashboard).pack(side="left", padx=4)
        ttk.Button(btn_row, text="Refresh Summary",
                   command=self._refresh_summary).pack(side="left", padx=4)

        # Progress bar
        self.progress = ttk.Progressbar(ctrl, mode="indeterminate")
        self.progress.grid(row=2, column=0, columnspan=3, sticky="ew", pady=(8, 0))

        # ─── Output area ──────────────────────────────────────────
        output_frame = ttk.LabelFrame(self.root, text="Output", padding=4)
        output_frame.grid(row=2, column=0, sticky="nsew", padx=8, pady=4)
        output_frame.columnconfigure(0, weight=1)
        output_frame.rowconfigure(0, weight=1)

        self.output = tk.Text(output_frame, wrap="word",
                              font=("Consolas", 9), state="disabled",
                              bg="#1e1e1e", fg="#d4d4d4",
                              insertbackground="#d4d4d4")
        self.output.grid(row=0, column=0, sticky="nsew")
        vsb = ttk.Scrollbar(output_frame, orient="vertical", command=self.output.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        self.output.configure(yscrollcommand=vsb.set)

        # ─── Summary ─────────────────────────────────────────────
        summary_frame = ttk.LabelFrame(self.root, text="DB Summary", padding=4)
        summary_frame.grid(row=3, column=0, sticky="ew", padx=8, pady=(0, 8))

        self.summary_label = ttk.Label(summary_frame, text="Loading…",
                                       font=("Consolas", 9))
        self.summary_label.pack(anchor="w")

        # ─── Status bar ──────────────────────────────────────────
        self.statusbar = ttk.Label(self.root, text="Ready", relief="sunken",
                                   anchor="w", padding=(6, 2))
        self.statusbar.grid(row=4, column=0, sticky="ew")

    # ─── Logging to output area ────────────────────────────────────

    def _log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}\n"
        self.output.configure(state="normal")
        self.output.insert("end", line)
        self.output.see("end")
        self.output.configure(state="disabled")
        self.root.update_idletasks()

    # ─── Browse ────────────────────────────────────────────────────

    def _browse(self):
        folder = filedialog.askdirectory(title="Select intake folder")
        if folder:
            self.folder_var.set(folder)
            self._log(f"Selected folder: {folder}")

    # ─── Run Intake ────────────────────────────────────────────────

    def _run_intake(self):
        folder = self.folder_var.get().strip()
        if not folder:
            messagebox.showwarning("No Folder", "Select an intake folder first.")
            return

        folder_path = Path(folder)
        if not folder_path.exists() or not folder_path.is_dir():
            messagebox.showerror("Invalid Folder",
                                 f"Not a valid directory:\n{folder}")
            return

        if self.running:
            self._log("Intake already running — please wait.")
            return

        self.running = True
        self.run_btn.state(["disabled"])
        self.progress.start(20)
        self.statusbar.config(text="Intake running…")

        # Clear output
        self.output.configure(state="normal")
        self.output.delete("1.0", "end")
        self.output.configure(state="disabled")

        # Run in background thread
        thread = threading.Thread(target=self._intake_worker,
                                  args=(folder_path,), daemon=True)
        thread.start()

    def _intake_worker(self, folder_path):
        """Worker thread for intake run."""
        try:
            run = IntakeRun(folder_path, log_fn=self._log_threadsafe)
            success = run.execute()

            self.root.after(0, self._intake_done, success, run)
        except Exception as e:
            self.root.after(0, self._intake_error, str(e))

    def _log_threadsafe(self, msg):
        """Thread-safe logging to the output area."""
        self.root.after(0, self._log, msg)

    def _intake_done(self, success, run):
        self.running = False
        self.run_btn.state(["!disabled"])
        self.progress.stop()

        if success:
            self.statusbar.config(
                text=f"Intake complete — {run.counters['inserted']} inserted, "
                     f"{run.counters['skipped_existing']} skipped")
            self._log("✓ Intake run finished successfully.")
        else:
            self.statusbar.config(text="Intake FAILED — see output")
            self._log("✗ Intake run failed.")

        self._refresh_summary()

    def _intake_error(self, error_msg):
        self.running = False
        self.run_btn.state(["!disabled"])
        self.progress.stop()
        self.statusbar.config(text="Intake ERROR")
        self._log(f"FATAL ERROR: {error_msg}")
        messagebox.showerror("Intake Error", f"Intake failed:\n{error_msg}")

    # ─── Dashboard ─────────────────────────────────────────────────

    def _open_dashboard(self):
        if not DASHBOARD.exists():
            messagebox.showerror("Not Found",
                                 f"Dashboard not found:\n{DASHBOARD}")
            return
        python = sys.executable
        subprocess.Popen([python, str(DASHBOARD)])
        self._log(f"Launched dashboard: {DASHBOARD.name}")
        self.statusbar.config(text="Dashboard launched")

    # ─── Summary ───────────────────────────────────────────────────

    def _refresh_summary(self):
        if not DB_PATH.exists():
            self.summary_label.config(text="DB not found")
            return
        try:
            conn = sqlite3.connect(str(DB_PATH))
            total = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM track_status GROUP BY status"
            ).fetchall()
            counts = dict(rows)
            audit = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
            conn.close()

            parts = [f"Total: {total}"]
            for s in ["CLEAN", "REVIEW", "LONGFORM", "JUNK", "DUPLICATE"]:
                parts.append(f"{s}: {counts.get(s, 0)}")
            parts.append(f"Audit entries: {audit}")
            self.summary_label.config(text="  |  ".join(parts))
        except Exception as e:
            self.summary_label.config(text=f"DB error: {e}")

    # ─── Cleanup ───────────────────────────────────────────────────

    def on_close(self):
        self.root.destroy()


# ═══════════════════════════════════════════════════════════════════
# CLI MODE (for automated testing / proof generation)
# ═══════════════════════════════════════════════════════════════════

def cli_intake(folder_path: str):
    """Run intake from CLI without UI. Returns IntakeRun."""
    run = IntakeRun(Path(folder_path), log_fn=print)
    run.execute()
    return run


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--cli":
        # CLI mode for scripted runs
        if len(sys.argv) < 3:
            print("Usage: dj_library_launcher_phase3.py --cli <folder>")
            sys.exit(1)
        cli_intake(sys.argv[2])
    else:
        # GUI mode
        root = tk.Tk()
        app = IntakeLauncher(root)
        root.protocol("WM_DELETE_WINDOW", app.on_close)
        root.mainloop()


if __name__ == "__main__":
    main()
