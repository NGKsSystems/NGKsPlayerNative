#!/usr/bin/env python3
"""
DJ Library Core — Phase 2: Operator Dashboard
===============================================
Local tkinter dashboard for viewing and managing the DJ Library Core database.

READ-ONLY on the filesystem. DB writes only through explicit operator actions.
All edits are auditable via audit_log table.
"""

import os
import re
import sqlite3
import subprocess
import sys
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, simpledialog, ttk
from typing import Callable, Literal

lookup_recording = None
ensure_config = None

try:
    from db.dj_musicbrainz import lookup_recording, ensure_config
    _HAS_MUSICBRAINZ = True
except ImportError:
    try:
        from dj_musicbrainz import lookup_recording, ensure_config
        _HAS_MUSICBRAINZ = True
    except ImportError:
        _HAS_MUSICBRAINZ = False

# ─── Configuration ─────────────────────────────────────────────────
BASE    = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative")
DB_PATH = BASE / "data" / "dj_library_core.db"

STATUS_TABS = ["ALL", "CLEAN", "REVIEW", "FAST REVIEW", "LONGFORM", "JUNK", "DUPLICATE"]
STATUS_COLORS = {
    "CLEAN":     "#d4edda",
    "REVIEW":    "#fff3cd",
    "LONGFORM":  "#d1ecf1",
    "JUNK":      "#f8d7da",
    "DUPLICATE": "#e2e3e5",
    "RAW":       "#f5f5f5",
}

COLUMNS = [
    ("track_id",         "ID",          50),
    ("file_name",        "File Name",   280),
    ("chosen_artist",    "Artist",      180),
    ("chosen_title",     "Title",       200),
    ("source_used",      "Source",      70),
    ("final_confidence", "Conf",        50),
    ("requires_review",  "Review?",     55),
    ("status",           "Status",      70),
    ("folder",           "Folder",      140),
]


class DJDashboard:
    """Operator dashboard for the DJ Library Core database."""

    def __init__(self, root):
        self.root = root
        self.root.title("DJ Library Core — Operator Dashboard")
        self.root.geometry("1400x820")
        self.root.minsize(1000, 600)

        self.conn: sqlite3.Connection
        self.current_tab = "ALL"
        self.search_text = tk.StringVar()
        self.sort_col = "track_id"
        self.sort_asc = True
        self.selected_track_id = None
        self.fast_review_active = False
        self._fr_row_widgets = []  # track per-row widget refs for cleanup
        self._fr_pending = {}     # {track_id: {"action": "ACCEPT"|"REJECT"|"MANUAL", ...}}
        self._fr_sort_col = "final_confidence"
        self._fr_sort_asc = True
        self._fr_zoom = 0         # -2..+4 zoom steps
        self._fr_rows_cache = []  # cached query results
        self._fr_filter_mode = "rename"  # "rename" | "same" | "all"
        self._fs_sync_job = None
        self._fs_sync_running = False

        self._connect_db()
        self._build_ui()
        self._load_data()
        self._schedule_fs_sync(30000)

    # ─── DB Connection ─────────────────────────────────────────────

    def _connect_db(self):
        if not DB_PATH.exists():
            messagebox.showerror("DB Missing", f"Database not found:\n{DB_PATH}")
            sys.exit(1)
        self.conn = sqlite3.connect(str(DB_PATH))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.row_factory = sqlite3.Row

    def _close_db(self):
        self.conn.close()

    # ─── UI Construction ───────────────────────────────────────────

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

        self._build_toolbar()
        self._build_main_area()
        self._build_statusbar()

    def _build_toolbar(self):
        toolbar = ttk.Frame(self.root, padding=4)
        toolbar.grid(row=0, column=0, sticky="ew")
        toolbar.columnconfigure(1, weight=1)

        # Status tabs
        tab_frame = ttk.Frame(toolbar)
        tab_frame.grid(row=0, column=0, sticky="w", padx=(0, 12))

        self.tab_buttons = {}
        for i, tab in enumerate(STATUS_TABS):
            btn = ttk.Button(tab_frame, text=tab, width=10,
                             command=lambda t=tab: self._switch_tab(t))
            btn.grid(row=0, column=i, padx=1)
            self.tab_buttons[tab] = btn

        # Search
        search_frame = ttk.Frame(toolbar)
        search_frame.grid(row=0, column=1, sticky="ew", padx=8)
        search_frame.columnconfigure(1, weight=1)

        ttk.Label(search_frame, text="Search:").grid(row=0, column=0, padx=(0, 4))
        search_entry = ttk.Entry(search_frame, textvariable=self.search_text)
        search_entry.grid(row=0, column=1, sticky="ew")
        search_entry.bind("<Return>", lambda e: self._load_data())
        ttk.Button(search_frame, text="Go", width=4,
                   command=self._load_data).grid(row=0, column=2, padx=2)
        ttk.Button(search_frame, text="Clear", width=5,
                   command=self._clear_search).grid(row=0, column=3)
        ttk.Button(search_frame, text="Sync Now", width=8,
               command=self._sync_now).grid(row=0, column=4, padx=(6, 0))

        # Counts label
        self.counts_label = ttk.Label(toolbar, text="", font=("Consolas", 9))
        self.counts_label.grid(row=0, column=2, sticky="e", padx=4)

    def _build_main_area(self):
        # Container that holds both normal paned view and fast review view
        self.main_container = ttk.Frame(self.root)
        self.main_container.grid(row=1, column=0, sticky="nsew", padx=4, pady=2)
        self.main_container.columnconfigure(0, weight=1)
        self.main_container.rowconfigure(0, weight=1)

        # ── Normal paned view ──────────────────────────────────────
        paned = ttk.PanedWindow(self.main_container, orient=tk.HORIZONTAL)
        paned.grid(row=0, column=0, sticky="nsew")
        self.normal_paned = paned

        # ── Fast Review view (built once, shown/hidden) ───────────
        self._build_fast_review_panel()

        # Left: Treeview
        left = ttk.Frame(paned)
        paned.add(left, weight=3)
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)

        col_ids = [c[0] for c in COLUMNS]
        self.tree = ttk.Treeview(left, columns=col_ids, show="headings",
                                 selectmode="extended")

        for col_id, heading, width in COLUMNS:
            self.tree.heading(col_id, text=heading,
                              command=lambda c=col_id: self._sort_by(c))
            self.tree.column(col_id, width=width, minwidth=40)

        vsb = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(left, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        # Right: Details + Actions
        right = ttk.Frame(paned, padding=6)
        paned.add(right, weight=2)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)

        # Details panel (scrollable)
        details_outer = ttk.LabelFrame(right, text="Track Details", padding=6)
        details_outer.grid(row=0, column=0, sticky="nsew")
        details_outer.columnconfigure(0, weight=1)
        details_outer.rowconfigure(0, weight=1)

        canvas = tk.Canvas(details_outer, highlightthickness=0)
        det_vsb = ttk.Scrollbar(details_outer, orient="vertical", command=canvas.yview)
        self.details_frame = ttk.Frame(canvas)

        self.details_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=self.details_frame, anchor="nw")
        canvas.configure(yscrollcommand=det_vsb.set)

        canvas.grid(row=0, column=0, sticky="nsew")
        det_vsb.grid(row=0, column=1, sticky="ns")

        # Mouse wheel scroll for details
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel, add="+")

        # Actions panel
        actions = ttk.LabelFrame(right, text="Actions", padding=6)
        actions.grid(row=1, column=0, sticky="ew", pady=(6, 0))

        btn_row1 = ttk.Frame(actions)
        btn_row1.pack(fill="x", pady=2)
        ttk.Button(btn_row1, text="Approve → CLEAN",
                   command=lambda: self._set_status("CLEAN")).pack(side="left", padx=2)
        ttk.Button(btn_row1, text="→ REVIEW",
                   command=lambda: self._set_status("REVIEW")).pack(side="left", padx=2)
        ttk.Button(btn_row1, text="→ JUNK",
                   command=lambda: self._set_status("JUNK")).pack(side="left", padx=2)
        ttk.Button(btn_row1, text="→ LONGFORM",
                   command=lambda: self._set_status("LONGFORM")).pack(side="left", padx=2)

        btn_row2 = ttk.Frame(actions)
        btn_row2.pack(fill="x", pady=2)
        ttk.Button(btn_row2, text="Edit Artist",
                   command=self._edit_artist).pack(side="left", padx=2)
        ttk.Button(btn_row2, text="Edit Title",
                   command=self._edit_title).pack(side="left", padx=2)
        ttk.Button(btn_row2, text="Delete Selected",
               command=self._delete_selected_file).pack(side="left", padx=2)
        ttk.Button(btn_row2, text="Open Folder",
                   command=self._open_folder).pack(side="left", padx=2)

        # ─── Review Workstation Panel ──────────────────────────────
        self._build_review_workstation(right)

    # ─── Fast Review Panel Construction ────────────────────────────

    def _build_fast_review_panel(self):
        """Build the Fast Review Queue panel (full-width, hidden initially)."""
        self.fr_frame = ttk.Frame(self.main_container)
        # Not gridded yet

        # ── Header bar ────────────────────────────────────────────
        fr_header = ttk.Frame(self.fr_frame, padding=4)
        fr_header.pack(fill="x")

        ttk.Label(fr_header, text="⚡ FAST REVIEW QUEUE",
                  font=("Segoe UI", 12, "bold")).pack(side="left", padx=4)
        self.fr_count_label = ttk.Label(fr_header, text="",
                                         font=("Consolas", 10))
        self.fr_count_label.pack(side="left", padx=12)

        # Shortcut hint
        ttk.Label(fr_header, text="Keys: A=Accept  M=Manual  R=Reject  D=Delete  Enter=Details  ↑↓=Navigate",
                  font=("Consolas", 8), foreground="#777").pack(side="right", padx=8)

        # ── Pending toolbar ───────────────────────────────────────
        fr_toolbar = ttk.Frame(self.fr_frame, padding=4)
        fr_toolbar.pack(fill="x")

        self.fr_pending_label = ttk.Label(
            fr_toolbar, text="Pending: 0", font=("Consolas", 10, "bold"))
        self.fr_pending_label.pack(side="left", padx=4)

        self.fr_save_btn = tk.Button(
            fr_toolbar, text="💾 Save Pending", bg="#28a745", fg="white",
            font=("Segoe UI", 9, "bold"), relief="raised",
            command=self._fr_save_pending, state="disabled")
        self.fr_save_btn.pack(side="left", padx=4)

        self.fr_discard_btn = tk.Button(
            fr_toolbar, text="✖ Discard All", bg="#dc3545", fg="white",
            font=("Segoe UI", 9, "bold"), relief="raised",
            command=self._fr_discard_pending, state="disabled")
        self.fr_discard_btn.pack(side="left", padx=4)

        # Separator
        ttk.Separator(fr_toolbar, orient="vertical").pack(
            side="left", fill="y", padx=8, pady=2)

        # Zoom controls
        ttk.Label(fr_toolbar, text="Zoom:").pack(side="left", padx=(4, 2))
        tk.Button(fr_toolbar, text="−", width=2, font=("Segoe UI", 9),
                  command=self._fr_zoom_out).pack(side="left", padx=1)
        self.fr_zoom_label = ttk.Label(fr_toolbar, text="100%",
                                        font=("Consolas", 9))
        self.fr_zoom_label.pack(side="left", padx=2)
        tk.Button(fr_toolbar, text="+", width=2, font=("Segoe UI", 9),
                  command=self._fr_zoom_in).pack(side="left", padx=1)
        tk.Button(fr_toolbar, text="Reset", font=("Segoe UI", 8),
                  command=self._fr_zoom_reset).pack(side="left", padx=4)

        ttk.Button(fr_toolbar, text="↻ Refresh",
                   command=self._fr_load_data).pack(side="right", padx=4)

        # ── Filter bar ────────────────────────────────────────────
        fr_filter_bar = ttk.Frame(self.fr_frame, padding=4)
        fr_filter_bar.pack(fill="x")
        ttk.Label(fr_filter_bar, text="Filter:",
                  font=("Segoe UI", 9, "bold")).pack(side="left", padx=(4, 8))
        self._fr_filter_var = tk.StringVar(value="rename")
        for val, label in [("rename", "Actionable Renames"),
                            ("same", "Same-Name Review"),
                            ("all", "All Review")]:
            ttk.Radiobutton(fr_filter_bar, text=label,
                             variable=self._fr_filter_var,
                             value=val,
                             command=self._fr_apply_filter).pack(side="left", padx=4)
        self.fr_filter_count_label = ttk.Label(fr_filter_bar, text="",
                                                font=("Consolas", 9),
                                                foreground="#555")
        self.fr_filter_count_label.pack(side="left", padx=12)

        # ── Treeview table ────────────────────────────────────────
        fr_tree_frame = ttk.Frame(self.fr_frame)
        fr_tree_frame.pack(fill="both", expand=True)

        self._fr_columns = ("file_name", "title", "suggested", "size",
                            "conf", "source_reason", "pending")
        self.fr_tree = ttk.Treeview(fr_tree_frame,
                                     columns=self._fr_columns,
                                     show="headings", selectmode="extended")

        # Column config
        col_cfg = [
            ("file_name",      "File Name",        280, "w"),
            ("title",          "Title",             160, "w"),
            ("suggested",      "Suggested File",    260, "w"),
            ("size",           "Size (MB)",          70, "e"),
            ("conf",           "Conf",               60, "center"),
            ("source_reason",  "Source / Reason",   150, "w"),
            ("pending",        "Pending",            80, "center"),
        ]
        for col_id, heading, width, anchor in col_cfg:
            self.fr_tree.heading(col_id, text=heading,
                                 command=lambda c=col_id: self._fr_sort_by(c))
            self.fr_tree.column(col_id, width=width, minwidth=50,
                                anchor=self._fr_tree_anchor(anchor))

        # Scrollbars
        fr_vsb = ttk.Scrollbar(fr_tree_frame, orient="vertical",
                                command=self.fr_tree.yview)
        fr_hsb = ttk.Scrollbar(fr_tree_frame, orient="horizontal",
                                command=self.fr_tree.xview)
        self.fr_tree.configure(yscrollcommand=fr_vsb.set,
                                xscrollcommand=fr_hsb.set)
        self.fr_tree.grid(row=0, column=0, sticky="nsew")
        fr_vsb.grid(row=0, column=1, sticky="ns")
        fr_hsb.grid(row=1, column=0, sticky="ew")
        fr_tree_frame.rowconfigure(0, weight=1)
        fr_tree_frame.columnconfigure(0, weight=1)

        # Row tags for pending state coloring
        self.fr_tree.tag_configure("accept", background="#d4edda")
        self.fr_tree.tag_configure("manual", background="#fff3cd")
        self.fr_tree.tag_configure("reject", background="#f8d7da")
        self.fr_tree.tag_configure("even",   background="#fffbe6")
        self.fr_tree.tag_configure("odd",    background="#fff8e1")
        self.fr_tree.tag_configure("hold",   background="#d6e9f8")
        self.fr_tree.tag_configure("delete", background="#e0b0b0",
                                   foreground="#4a0000")

        # Selection event
        self.fr_tree.bind("<<TreeviewSelect>>", self._fr_on_select)

        # Keyboard shortcuts (bound to the tree widget)
        self.fr_tree.bind("<a>", lambda e: self._fr_key_accept())
        self.fr_tree.bind("<A>", lambda e: self._fr_key_accept())
        self.fr_tree.bind("<m>", lambda e: self._fr_key_manual())
        self.fr_tree.bind("<M>", lambda e: self._fr_key_manual())
        self.fr_tree.bind("<r>", lambda e: self._fr_key_reject())
        self.fr_tree.bind("<R>", lambda e: self._fr_key_reject())
        self.fr_tree.bind("<Return>", lambda e: self._fr_key_details())
        self.fr_tree.bind("<u>", lambda e: self._fr_key_unstage())
        self.fr_tree.bind("<U>", lambda e: self._fr_key_unstage())
        self.fr_tree.bind("<d>", lambda e: self._fr_key_delete())
        self.fr_tree.bind("<D>", lambda e: self._fr_key_delete())

        # ── Action bar (below treeview) ───────────────────────────
        action_frame = ttk.Frame(self.fr_frame, padding=4)
        action_frame.pack(fill="x")

        ttk.Label(action_frame, text="Actions:",
                  font=("Segoe UI", 9, "bold")).pack(side="left", padx=(4, 8))

        tk.Button(action_frame, text="✔ Accept (A)", bg="#28a745", fg="white",
                  font=("Segoe UI", 9, "bold"), relief="flat", padx=8,
                  command=self._fr_key_accept).pack(side="left", padx=2)
        tk.Button(action_frame, text="✏ Manual (M)", bg="#ffc107", fg="black",
                  font=("Segoe UI", 9, "bold"), relief="flat", padx=8,
                  command=self._fr_key_manual).pack(side="left", padx=2)
        tk.Button(action_frame, text="✖ Reject (R)", bg="#dc3545", fg="white",
                  font=("Segoe UI", 9, "bold"), relief="flat", padx=8,
                  command=self._fr_key_reject).pack(side="left", padx=2)
        tk.Button(action_frame, text="↩ Unstage (U)", bg="#6c757d", fg="white",
                  font=("Segoe UI", 9, "bold"), relief="flat", padx=8,
                  command=self._fr_key_unstage).pack(side="left", padx=2)
        tk.Button(action_frame, text="⋯ Details (Enter)", bg="#495057", fg="white",
                  font=("Segoe UI", 9, "bold"), relief="flat", padx=8,
                  command=self._fr_key_details).pack(side="left", padx=2)

        # Bulk actions separator + buttons
        ttk.Separator(action_frame, orient="vertical").pack(
            side="left", fill="y", padx=8, pady=2)
        ttk.Label(action_frame, text="Bulk:",
                  font=("Segoe UI", 9, "bold")).pack(side="left", padx=(4, 4))
        tk.Button(action_frame, text="✔ Accept Selected", bg="#28a745",
                  fg="white", font=("Segoe UI", 9), relief="flat", padx=6,
                  command=self._fr_bulk_accept).pack(side="left", padx=2)
        tk.Button(action_frame, text="✖ Reject Selected", bg="#dc3545",
                  fg="white", font=("Segoe UI", 9), relief="flat", padx=6,
                  command=self._fr_bulk_reject).pack(side="left", padx=2)
        tk.Button(action_frame, text="⏸ Hold Selected", bg="#17a2b8",
                  fg="white", font=("Segoe UI", 9), relief="flat", padx=6,
                  command=self._fr_bulk_hold).pack(side="left", padx=2)
        tk.Button(action_frame, text="🗑 Delete Selected", bg="#6f1d1b",
                  fg="white", font=("Segoe UI", 9), relief="flat", padx=6,
                  command=self._fr_bulk_delete).pack(side="left", padx=2)

        # MusicBrainz lookup button
        ttk.Separator(action_frame, orient="vertical").pack(
            side="left", fill="y", padx=8, pady=2)
        tk.Button(action_frame, text="🔍 Lookup MusicBrainz", bg="#6610f2",
                  fg="white", font=("Segoe UI", 9, "bold"), relief="flat",
                  padx=6, command=self._fr_lookup_musicbrainz).pack(
                      side="left", padx=2)

        # ── Detail pane (shows selected row info) ─────────────────
        self.fr_detail_frame = ttk.LabelFrame(self.fr_frame,
                                               text="Selected Track",
                                               padding=6)
        self.fr_detail_frame.pack(fill="x", padx=4, pady=(2, 4))

        self.fr_detail_text = tk.Text(self.fr_detail_frame, height=4,
                                       wrap="word", font=("Consolas", 9),
                                       bg="#f8f9fa", relief="flat",
                                       state="disabled")
        self.fr_detail_text.pack(fill="x")

        # Keep references for frozen-header and canvas attrs used by
        # validation and stale refs (compatibility)
        self.fr_col_header_frame = ttk.Frame(self.fr_frame)
        self.fr_canvas = self.fr_tree
        self.fr_table_frame = self.fr_tree

    def _build_review_workstation(self, parent):
        """Build the Interactive Review Workstation panel."""
        self.review_frame = ttk.LabelFrame(
            parent, text="▶ REVIEW WORKSTATION", padding=8
        )
        self.review_frame.grid(row=2, column=0, sticky="nsew", pady=(6, 0))
        parent.rowconfigure(2, weight=1)

        # Inner scrollable area
        rv_canvas = tk.Canvas(self.review_frame, highlightthickness=0, height=260)
        rv_vsb = ttk.Scrollbar(self.review_frame, orient="vertical",
                                command=rv_canvas.yview)
        self.rv_inner = ttk.Frame(rv_canvas)
        self.rv_inner.bind(
            "<Configure>",
            lambda e: rv_canvas.configure(scrollregion=rv_canvas.bbox("all"))
        )
        rv_canvas.create_window((0, 0), window=self.rv_inner, anchor="nw")
        rv_canvas.configure(yscrollcommand=rv_vsb.set)
        rv_canvas.pack(side="left", fill="both", expand=True)
        rv_vsb.pack(side="right", fill="y")

        # Manual edit entry vars
        self.rv_edit_artist = tk.StringVar()
        self.rv_edit_title = tk.StringVar()
        self.rv_manual_mode = False
        self.rv_track_id = None

        # Initial state: hidden
        self.review_frame.grid_remove()

    def _build_statusbar(self):
        self.statusbar = ttk.Label(self.root, text="Ready", relief="sunken",
                                   anchor="w", padding=(6, 2))
        self.statusbar.grid(row=2, column=0, sticky="ew")

    # ─── Filesystem Sync ──────────────────────────────────────────

    _SYNC_AUDIO_EXTS = {".mp3", ".wav", ".flac", ".m4a"}
    _FS_SYNC_INTERVAL_MS = 30000

    def _schedule_fs_sync(self, delay_ms=None):
        if self._fs_sync_job is not None:
            try:
                self.root.after_cancel(self._fs_sync_job)
            except Exception:
                pass
        self._fs_sync_job = self.root.after(
            delay_ms or self._FS_SYNC_INTERVAL_MS,
            self._run_fs_sync,
        )

    def _sync_now(self):
        self.statusbar.config(text="Syncing library with filesystem...")
        self.root.update_idletasks()
        self._run_fs_sync(reschedule=False)

    def _run_fs_sync(self, reschedule=True):
        if self._fs_sync_running:
            if reschedule:
                self._schedule_fs_sync()
            return

        self._fs_sync_running = True
        try:
            summary = self._sync_library_with_filesystem()
            self.statusbar.config(text=summary)
        except Exception as e:
            self.statusbar.config(text=f"Filesystem sync failed: {e}")
        finally:
            self._fs_sync_running = False
            if reschedule:
                self._schedule_fs_sync()

    def _sync_library_with_filesystem(self):
        roots = [root for root in self._DELETE_ALLOWED_ROOTS if root.exists()]
        if not roots:
            return "Filesystem sync skipped: managed roots missing"

        fs_by_path = {}
        fs_by_size = {}
        for root in roots:
            for dirpath, _dirnames, filenames in os.walk(root):
                for name in filenames:
                    path = Path(dirpath) / name
                    if path.suffix.lower() not in self._SYNC_AUDIO_EXTS:
                        continue
                    try:
                        stat = path.stat()
                    except OSError:
                        continue
                    resolved = str(path.resolve()).lower()
                    record = {
                        "path": path,
                        "resolved": resolved,
                        "size": stat.st_size,
                        "ext": path.suffix.lower(),
                        "folder": path.parent.name,
                        "parent": str(path.parent.resolve()).lower(),
                    }
                    fs_by_path[resolved] = record
                    fs_by_size.setdefault((stat.st_size, record["ext"]), []).append(record)

        track_rows = self.conn.execute(
            "SELECT t.track_id, t.file_path, t.file_name, t.folder, t.file_size, "
            "ts.status FROM tracks t "
            "JOIN track_status ts ON ts.track_id = t.track_id"
        ).fetchall()

        claimed_paths = set()
        refreshed = 0
        renamed = 0
        deleted = 0
        now = datetime.now().isoformat()

        for row in track_rows:
            db_path = Path(row["file_path"])
            resolved = str(db_path.resolve()).lower()
            fs_rec = fs_by_path.get(resolved)
            if not fs_rec:
                continue
            claimed_paths.add(fs_rec["resolved"])
            if (row["file_name"] != fs_rec["path"].name or
                    row["folder"] != fs_rec["folder"] or
                    row["file_path"] != str(fs_rec["path"])):
                self.conn.execute(
                    "UPDATE tracks SET file_path = ?, file_name = ?, folder = ? "
                    "WHERE track_id = ?",
                    (str(fs_rec["path"]), fs_rec["path"].name,
                     fs_rec["folder"], row["track_id"])
                )
                refreshed += 1

        for row in track_rows:
            db_path = Path(row["file_path"])
            resolved = str(db_path.resolve()).lower()
            if resolved in fs_by_path:
                continue

            ext = db_path.suffix.lower()
            size_key = (row["file_size"], ext)
            candidates = [
                rec for rec in fs_by_size.get(size_key, [])
                if rec["resolved"] not in claimed_paths
            ]
            old_parent = str(db_path.parent.resolve()).lower()
            same_parent = [rec for rec in candidates if rec["parent"] == old_parent]

            chosen = None
            if len(same_parent) == 1:
                chosen = same_parent[0]
            elif len(candidates) == 1:
                chosen = candidates[0]

            if chosen is not None:
                self.conn.execute(
                    "UPDATE tracks SET file_path = ?, file_name = ?, folder = ? "
                    "WHERE track_id = ?",
                    (str(chosen["path"]), chosen["path"].name,
                     chosen["folder"], row["track_id"])
                )
                self.conn.execute(
                    "INSERT INTO audit_log "
                    "(track_id, event_type, event_description, timestamp) "
                    "VALUES (?, ?, ?, ?)",
                    (row["track_id"], "FS_SYNC_RENAME",
                     f"Filesystem sync updated path: {row['file_name']} -> {chosen['path'].name}",
                     now)
                )
                claimed_paths.add(chosen["resolved"])
                renamed += 1
                continue

            if row["status"] != "JUNK":
                self.conn.execute(
                    "UPDATE track_status SET status = 'JUNK' WHERE track_id = ?",
                    (row["track_id"],)
                )
                self.conn.execute(
                    "INSERT INTO audit_log "
                    "(track_id, event_type, event_description, timestamp) "
                    "VALUES (?, ?, ?, ?)",
                    (row["track_id"], "FS_SYNC_MISSING",
                     f"Filesystem sync marked missing file as JUNK: {row['file_path']}",
                     now)
                )
                deleted += 1

        if refreshed or renamed or deleted:
            self.conn.commit()
            current_selection = self._selected_track_ids()
            self._load_data()
            if current_selection:
                surviving = [str(tid) for tid in current_selection if self.tree.exists(str(tid))]
                if surviving:
                    self.tree.selection_set(surviving)
                    self.tree.see(surviving[-1])
        else:
            self.conn.rollback()

        scanned = len(fs_by_path)
        return (
            f"Filesystem sync: scanned {scanned} files | "
            f"refreshed {refreshed} | renamed {renamed} | deleted {deleted}"
        )

    # ─── Data Loading ──────────────────────────────────────────────

    def _load_data(self):
        if self.current_tab == "FAST REVIEW":
            self._fr_load_data()
            return

        for item in self.tree.get_children():
            self.tree.delete(item)

        search = self.search_text.get().strip()
        tab = self.current_tab

        if tab == "DUPLICATE":
            query = """
                WITH filename_dups AS (
                    SELECT LOWER(t.file_name) AS dup_key,
                           COUNT(*) AS dup_count
                    FROM tracks t
                    JOIN track_status ts ON ts.track_id = t.track_id
                    WHERE ts.status != 'JUNK'
                    GROUP BY LOWER(t.file_name)
                    HAVING COUNT(*) > 1
                ),
                meta_dups AS (
                    SELECT LOWER(COALESCE(hr.chosen_artist, '')) || '|' ||
                           LOWER(COALESCE(hr.chosen_title, '')) AS dup_key,
                           COUNT(*) AS dup_count
                    FROM hybrid_resolution hr
                    JOIN track_status ts ON ts.track_id = hr.track_id
                    WHERE COALESCE(hr.chosen_artist, '') != ''
                      AND COALESCE(hr.chosen_title, '') != ''
                      AND ts.status != 'JUNK'
                    GROUP BY LOWER(COALESCE(hr.chosen_artist, '')),
                             LOWER(COALESCE(hr.chosen_title, ''))
                    HAVING COUNT(*) > 1
                )
                SELECT DISTINCT
                       t.track_id, t.file_name, t.folder, t.file_path,
                       hr.chosen_artist, hr.chosen_title, hr.source_used,
                       hr.final_confidence, hr.requires_review,
                       'DUPLICATE' AS status,
                       CASE
                           WHEN md.dup_key IS NOT NULL THEN 'META|' || md.dup_key
                           WHEN fd.dup_key IS NOT NULL THEN 'FILE|' || fd.dup_key
                           ELSE 'NONE'
                       END AS duplicate_group_key,
                       CASE
                           WHEN md.dup_count IS NOT NULL THEN md.dup_count
                           WHEN fd.dup_count IS NOT NULL THEN fd.dup_count
                           ELSE 1
                       END AS duplicate_group_size
                FROM tracks t
                JOIN hybrid_resolution hr ON hr.track_id = t.track_id
                JOIN track_status ts ON ts.track_id = t.track_id
                LEFT JOIN filename_dups fd ON fd.dup_key = LOWER(t.file_name)
                LEFT JOIN meta_dups md ON md.dup_key =
                    LOWER(COALESCE(hr.chosen_artist, '')) || '|' ||
                    LOWER(COALESCE(hr.chosen_title, ''))
                WHERE (fd.dup_key IS NOT NULL OR md.dup_key IS NOT NULL)
                  AND ts.status != 'JUNK'
            """
        else:
            query = """
                SELECT t.track_id, t.file_name, t.folder, t.file_path,
                       hr.chosen_artist, hr.chosen_title, hr.source_used,
                       hr.final_confidence, hr.requires_review,
                       ts.status
                FROM tracks t
                JOIN hybrid_resolution hr ON hr.track_id = t.track_id
                JOIN track_status ts ON ts.track_id = t.track_id
            """
        params = []
        wheres = []

        if tab != "ALL" and tab != "DUPLICATE":
            wheres.append("ts.status = ?")
            params.append(tab)

        if search:
            wheres.append(
                "(t.file_name LIKE ? OR hr.chosen_artist LIKE ? OR hr.chosen_title LIKE ?)"
            )
            like = f"%{search}%"
            params.extend([like, like, like])

        if wheres:
            query += " WHERE " + " AND ".join(wheres)

        # Sort
        sort_map = {
            "track_id": "t.track_id",
            "file_name": "t.file_name",
            "chosen_artist": "hr.chosen_artist",
            "chosen_title": "hr.chosen_title",
            "source_used": "hr.source_used",
            "final_confidence": "hr.final_confidence",
            "requires_review": "hr.requires_review",
            "status": "ts.status",
            "folder": "t.folder",
        }
        sort_col_sql = sort_map.get(self.sort_col, "t.track_id")
        direction = "ASC" if self.sort_asc else "DESC"
        if tab == "DUPLICATE":
            query += (
                " ORDER BY duplicate_group_key ASC, "
                "duplicate_group_size DESC, t.file_name COLLATE NOCASE ASC, "
                "t.track_id ASC"
            )
        else:
            query += f" ORDER BY {sort_col_sql} {direction}"

        rows = self.conn.execute(query, params).fetchall()

        for row in rows:
            values = (
                row["track_id"],
                row["file_name"],
                row["chosen_artist"] or "",
                row["chosen_title"] or "",
                row["source_used"] or "",
                f"{row['final_confidence']:.2f}" if row["final_confidence"] else "",
                "Yes" if row["requires_review"] else "",
                row["status"],
                row["folder"],
            )
            tag = row["status"]
            self.tree.insert("", "end", iid=str(row["track_id"]),
                             values=values, tags=(tag,))

        # Apply row colors
        for status, color in STATUS_COLORS.items():
            self.tree.tag_configure(status, background=color)

        # Update counts
        total = len(rows)
        self._update_counts()
        self.statusbar.config(text=f"Showing {total} tracks | Tab: {tab}")

    def _update_counts(self):
        rows = self.conn.execute(
            "SELECT status, COUNT(*) FROM track_status GROUP BY status"
        ).fetchall()
        counts = dict(rows)
        duplicate_count = self.conn.execute("""
            WITH duplicate_keys AS (
                SELECT LOWER(t.file_name) AS dup_key
                FROM tracks t
                GROUP BY LOWER(t.file_name)
                HAVING COUNT(*) > 1

                UNION

                SELECT LOWER(COALESCE(hr.chosen_artist, '')) || '|' ||
                       LOWER(COALESCE(hr.chosen_title, '')) AS dup_key
                FROM hybrid_resolution hr
                WHERE COALESCE(hr.chosen_artist, '') != ''
                  AND COALESCE(hr.chosen_title, '') != ''
                GROUP BY LOWER(COALESCE(hr.chosen_artist, '')),
                         LOWER(COALESCE(hr.chosen_title, ''))
                HAVING COUNT(*) > 1
            )
            SELECT COUNT(DISTINCT t.track_id)
            FROM tracks t
            JOIN hybrid_resolution hr ON hr.track_id = t.track_id
            JOIN track_status ts ON ts.track_id = t.track_id
            WHERE (
                LOWER(t.file_name) IN (
                    SELECT dup_key FROM duplicate_keys
                    WHERE dup_key NOT LIKE '%|%'
                )
                OR LOWER(COALESCE(hr.chosen_artist, '')) || '|' ||
                   LOWER(COALESCE(hr.chosen_title, '')) IN (
                    SELECT dup_key FROM duplicate_keys
                    WHERE dup_key LIKE '%|%'
                )
            )
            AND ts.status != 'JUNK'
        """).fetchone()[0]
        counts["DUPLICATE"] = duplicate_count
        parts = []
        for tab in STATUS_TABS[1:]:
            if tab == "FAST REVIEW":
                continue  # not a DB status
            cnt = counts.get(tab, 0)
            parts.append(f"{tab}:{cnt}")
        total = sum(counts.values())
        self.counts_label.config(text=f"Total:{total}  " + "  ".join(parts))

    # ─── Tab Switching ─────────────────────────────────────────────

    def _switch_tab(self, tab):
        self.current_tab = tab
        # Visual feedback — bold the active tab
        for t, btn in self.tab_buttons.items():
            if t == tab:
                btn.state(["pressed"])
            else:
                btn.state(["!pressed"])

        if tab == "FAST REVIEW":
            self._activate_fast_review()
        else:
            self._deactivate_fast_review()
            self._load_data()

    # ─── Search ────────────────────────────────────────────────────

    def _clear_search(self):
        self.search_text.set("")
        self._load_data()

    # ─── Sorting ───────────────────────────────────────────────────

    def _sort_by(self, col):
        if self.sort_col == col:
            self.sort_asc = not self.sort_asc
        else:
            self.sort_col = col
            self.sort_asc = True

        # Update header indicators
        for col_id, heading, _ in COLUMNS:
            indicator = ""
            if col_id == self.sort_col:
                indicator = " ▲" if self.sort_asc else " ▼"
            self.tree.heading(col_id, text=heading + indicator)

        self._load_data()

    # ─── Selection / Details ───────────────────────────────────────

    def _on_select(self, event):
        sel = self.tree.selection()
        if not sel:
            return
        track_id = int(sel[-1])
        self.selected_track_id = track_id
        self._show_details(track_id)
        self._populate_review_workstation(track_id)

    def _selected_track_ids(self):
        sel = self.tree.selection()
        return [int(item) for item in sel]

    def _show_details(self, track_id):
        # Clear previous
        for w in self.details_frame.winfo_children():
            w.destroy()

        row_num = 0

        def add_section(title):
            nonlocal row_num
            lbl = ttk.Label(self.details_frame, text=title,
                            font=("Segoe UI", 10, "bold"))
            lbl.grid(row=row_num, column=0, columnspan=2, sticky="w", pady=(8, 2))
            row_num += 1

        def add_field(label, value):
            nonlocal row_num
            ttk.Label(self.details_frame, text=f"{label}:",
                      font=("Segoe UI", 9)).grid(row=row_num, column=0,
                                                   sticky="ne", padx=(0, 6))
            val_lbl = ttk.Label(self.details_frame, text=str(value or "—"),
                                wraplength=320, font=("Consolas", 9))
            val_lbl.grid(row=row_num, column=1, sticky="nw")
            row_num += 1

        # Track info
        t = self.conn.execute(
            "SELECT * FROM tracks WHERE track_id = ?", (track_id,)
        ).fetchone()
        if not t:
            add_section("Track not found")
            return

        add_section("─── Track Info ───")
        add_field("Track ID", t["track_id"])
        add_field("File Name", t["file_name"])
        add_field("Folder", t["folder"])
        add_field("File Path", t["file_path"])
        size_mb = t["file_size"] / (1024 * 1024) if t["file_size"] else 0
        add_field("Size", f"{size_mb:.1f} MB ({t['file_size']:,} bytes)")
        if t["duration"]:
            mins = int(t["duration"] // 60)
            secs = int(t["duration"] % 60)
            add_field("Duration", f"{mins}:{secs:02d} ({t['duration']:.1f}s)")

        # Filename parse
        fp = self.conn.execute(
            "SELECT * FROM filename_parse WHERE track_id = ?", (track_id,)
        ).fetchone()
        if fp:
            add_section("─── Filename Parse ───")
            add_field("Artist (guess)", fp["artist_guess"])
            add_field("Title (guess)", fp["title_guess"])
            add_field("Confidence", f"{fp['parse_confidence']:.2f}")
            add_field("Method", fp["parse_method"])

        # Metadata tags
        mt = self.conn.execute(
            "SELECT * FROM metadata_tags WHERE track_id = ?", (track_id,)
        ).fetchone()
        if mt:
            add_section("─── Metadata Tags ───")
            add_field("Artist", mt["artist_tag"])
            add_field("Title", mt["title_tag"])
            add_field("Album", mt["album"])
            add_field("Genre", mt["genre"])
            add_field("Track #", mt["track_number"])
            add_field("Tag Version", mt["tag_version"])
            add_field("Confidence", f"{mt['metadata_confidence']:.2f}")
            junk = "YES" if mt["metadata_junk_flag"] else "No"
            add_field("Junk Flag", junk)
            if mt["metadata_junk_reason"]:
                add_field("Junk Reason", mt["metadata_junk_reason"])

        # Hybrid resolution
        hr = self.conn.execute(
            "SELECT * FROM hybrid_resolution WHERE track_id = ?", (track_id,)
        ).fetchone()
        if hr:
            add_section("─── Hybrid Resolution ───")
            add_field("Chosen Artist", hr["chosen_artist"])
            add_field("Chosen Title", hr["chosen_title"])
            add_field("Source Used", hr["source_used"])
            add_field("Final Confidence", f"{hr['final_confidence']:.3f}")
            add_field("Was Reversed", "Yes" if hr["was_reversed"] else "No")
            add_field("Requires Review", "Yes" if hr["requires_review"] else "No")

        # Status
        ts = self.conn.execute(
            "SELECT * FROM track_status WHERE track_id = ?", (track_id,)
        ).fetchone()
        if ts:
            add_section("─── Track Status ───")
            add_field("Status", ts["status"])
            if ts["duplicate_group_id"]:
                add_field("Duplicate Group", ts["duplicate_group_id"])
            add_field("Is Primary", "Yes" if ts["is_primary"] else "No")

        dup_rows = self._get_duplicate_candidates(track_id)
        if len(dup_rows) > 1:
            add_section("─── Duplicate Candidates ───")
            add_field("Candidate Count", len(dup_rows))
            for dup in dup_rows:
                marker = "KEEP" if dup["track_id"] == track_id else "ALT"
                add_field(
                    f"{marker} #{dup['track_id']}",
                    f"{dup['file_name']} | {dup['folder']} | {dup['status']}"
                )

        # Audit log
        audit_rows = self.conn.execute(
            """SELECT event_type, event_description, timestamp
               FROM audit_log WHERE track_id = ?
               ORDER BY timestamp DESC LIMIT 10""",
            (track_id,)
        ).fetchall()
        if audit_rows:
            add_section("─── Audit Log (last 10) ───")
            for ar in audit_rows:
                add_field(ar["event_type"],
                          f"{ar['event_description']}  [{ar['timestamp']}]")

    # ─── Actions ───────────────────────────────────────────────────

    def _require_selection(self):
        if not self._selected_track_ids():
            messagebox.showinfo("No Selection", "Select a track first.")
            return False
        return True

    def _set_status(self, new_status):
        if not self._require_selection():
            return
        tid = self.selected_track_id

        current = self.conn.execute(
            "SELECT status FROM track_status WHERE track_id = ?", (tid,)
        ).fetchone()
        old_status = current["status"] if current else "UNKNOWN"

        if old_status == new_status:
            messagebox.showinfo("No Change", f"Track is already {new_status}.")
            return

        confirm = messagebox.askyesno(
            "Confirm Status Change",
            f"Change track {tid} from {old_status} → {new_status}?"
        )
        if not confirm:
            return

        now = datetime.now().isoformat()
        try:
            self.conn.execute(
                "UPDATE track_status SET status = ? WHERE track_id = ?",
                (new_status, tid)
            )
            self.conn.execute(
                """INSERT INTO audit_log (track_id, event_type, event_description, timestamp)
                   VALUES (?, ?, ?, ?)""",
                (tid, "STATUS_CHANGE",
                 f"Operator changed status: {old_status} → {new_status}", now)
            )
            self.conn.commit()
            self.statusbar.config(
                text=f"Track {tid}: {old_status} → {new_status}")
            self._load_data()
            # Re-select and show details
            try:
                self.tree.selection_set(str(tid))
                self.tree.see(str(tid))
                self._show_details(tid)
            except tk.TclError:
                pass  # Track may no longer be in current tab view
        except Exception as e:
            self.conn.rollback()
            messagebox.showerror("DB Error", f"Failed to update status:\n{e}")

    def _get_duplicate_candidates(self, track_id):
        row = self.conn.execute(
            "SELECT t.file_name, hr.chosen_artist, hr.chosen_title "
            "FROM tracks t "
            "JOIN hybrid_resolution hr ON hr.track_id = t.track_id "
            "WHERE t.track_id = ?",
            (track_id,),
        ).fetchone()
        if not row:
            return []

        file_name = (row["file_name"] or "").lower()
        artist = (row["chosen_artist"] or "").lower()
        title = (row["chosen_title"] or "").lower()

        params = [file_name]
        where = ["LOWER(t.file_name) = ?"]
        if artist and title:
            where.append(
                "(LOWER(COALESCE(hr.chosen_artist, '')) = ? "
                "AND LOWER(COALESCE(hr.chosen_title, '')) = ?)"
            )
            params.extend([artist, title])

        query = f"""
            SELECT DISTINCT t.track_id, t.file_name, t.folder, ts.status
            FROM tracks t
            JOIN hybrid_resolution hr ON hr.track_id = t.track_id
            JOIN track_status ts ON ts.track_id = t.track_id
            WHERE ({' OR '.join(where)})
              AND ts.status != 'JUNK'
            ORDER BY t.file_name COLLATE NOCASE ASC, t.track_id ASC
        """
        return self.conn.execute(query, params).fetchall()

    def _delete_selected_file(self):
        if not self._require_selection():
            return

        tids = self._selected_track_ids()
        rows = self.conn.execute(
            "SELECT t.track_id, t.file_name, t.file_path, ts.status "
            "FROM tracks t "
            "JOIN track_status ts ON ts.track_id = t.track_id "
            f"WHERE t.track_id IN ({','.join('?' for _ in tids)}) "
            "ORDER BY t.track_id",
            tids,
        ).fetchall()
        if not rows:
            messagebox.showerror("Delete File", "Selected tracks not found in DB.")
            return

        if len(rows) == 1:
            row = rows[0]
            prompt = (
                f"Delete this file from disk?\n\n"
                f"Track ID: {row['track_id']}\n"
                f"File: {row['file_name']}\n"
                f"Path: {row['file_path']}\n\n"
                "This only deletes files inside the managed intake scope.\n"
                "The track will be marked JUNK after deletion."
            )
        else:
            preview = "\n".join(
                f"{row['track_id']}: {row['file_name']}" for row in rows[:8]
            )
            if len(rows) > 8:
                preview += f"\n... and {len(rows) - 8} more"
            prompt = (
                f"Delete {len(rows)} selected files from disk?\n\n"
                f"{preview}\n\n"
                "This only deletes files inside the managed intake scope.\n"
                "Deleted tracks will be marked JUNK."
            )

        confirm = messagebox.askyesno(
            "Confirm File Delete",
            prompt,
        )
        if not confirm:
            return

        now = datetime.now().isoformat()
        try:
            deleted = 0
            blocked = []
            for row in rows:
                tid = row["track_id"]
                self.conn.execute(
                    "INSERT INTO audit_log "
                    "(track_id, event_type, event_description, timestamp) "
                    "VALUES (?, ?, ?, ?)",
                    (tid, "DASHBOARD_DELETE_STAGED",
                     f"Dashboard delete requested: {row['file_name']}", now)
                )

                ok, reason = self._fr_delete_file_safe(tid)
                if not ok:
                    blocked.append(f"{tid}: {reason}")
                    self.conn.execute(
                        "INSERT INTO audit_log "
                        "(track_id, event_type, event_description, timestamp) "
                        "VALUES (?, ?, ?, ?)",
                        (tid, "DASHBOARD_DELETE_BLOCKED",
                         f"Delete blocked: {reason}", now)
                    )
                    continue

                self.conn.execute(
                    "UPDATE track_status SET status = 'JUNK' WHERE track_id = ?",
                    (tid,)
                )
                self.conn.execute(
                    "INSERT INTO audit_log "
                    "(track_id, event_type, event_description, timestamp) "
                    "VALUES (?, ?, ?, ?)",
                    (tid, "DASHBOARD_DELETE_COMMIT",
                     f"File deleted: {reason}", now)
                )
                deleted += 1

            self.conn.commit()
            self.statusbar.config(
                text=f"Deleted {deleted} file(s); blocked {len(blocked)}"
            )
            self.selected_track_id = None
            self._load_data()
            if blocked:
                messagebox.showwarning(
                    "Delete Summary",
                    f"Deleted {deleted} file(s).\n\nBlocked:\n" +
                    "\n".join(blocked[:10])
                )
        except Exception as e:
            self.conn.rollback()
            messagebox.showerror("DB Error", f"Delete failed:\n{e}")

    def _edit_artist(self):
        if not self._require_selection():
            return
        tid = self.selected_track_id

        hr = self.conn.execute(
            "SELECT chosen_artist FROM hybrid_resolution WHERE track_id = ?",
            (tid,)
        ).fetchone()
        old_val = hr["chosen_artist"] if hr else ""

        new_val = simpledialog.askstring(
            "Edit Artist", f"Track {tid} — Artist:",
            initialvalue=old_val, parent=self.root
        )
        if new_val is None or new_val == old_val:
            return

        now = datetime.now().isoformat()
        try:
            self.conn.execute(
                """UPDATE hybrid_resolution
                   SET chosen_artist = ?, source_used = 'hybrid'
                   WHERE track_id = ?""",
                (new_val, tid)
            )
            self.conn.execute(
                """INSERT INTO audit_log (track_id, event_type, event_description, timestamp)
                   VALUES (?, ?, ?, ?)""",
                (tid, "ARTIST_EDIT",
                 f"Operator edited artist: '{old_val}' → '{new_val}'", now)
            )
            self.conn.commit()
            self.statusbar.config(text=f"Track {tid}: artist updated")
            self._load_data()
            try:
                self.tree.selection_set(str(tid))
                self.tree.see(str(tid))
                self._show_details(tid)
            except tk.TclError:
                pass
        except Exception as e:
            self.conn.rollback()
            messagebox.showerror("DB Error", f"Failed to edit artist:\n{e}")

    def _edit_title(self):
        if not self._require_selection():
            return
        tid = self.selected_track_id

        hr = self.conn.execute(
            "SELECT chosen_title FROM hybrid_resolution WHERE track_id = ?",
            (tid,)
        ).fetchone()
        old_val = hr["chosen_title"] if hr else ""

        new_val = simpledialog.askstring(
            "Edit Title", f"Track {tid} — Title:",
            initialvalue=old_val, parent=self.root
        )
        if new_val is None or new_val == old_val:
            return

        now = datetime.now().isoformat()
        try:
            self.conn.execute(
                """UPDATE hybrid_resolution
                   SET chosen_title = ?, source_used = 'hybrid'
                   WHERE track_id = ?""",
                (new_val, tid)
            )
            self.conn.execute(
                """INSERT INTO audit_log (track_id, event_type, event_description, timestamp)
                   VALUES (?, ?, ?, ?)""",
                (tid, "TITLE_EDIT",
                 f"Operator edited title: '{old_val}' → '{new_val}'", now)
            )
            self.conn.commit()
            self.statusbar.config(text=f"Track {tid}: title updated")
            self._load_data()
            try:
                self.tree.selection_set(str(tid))
                self.tree.see(str(tid))
                self._show_details(tid)
            except tk.TclError:
                pass
        except Exception as e:
            self.conn.rollback()
            messagebox.showerror("DB Error", f"Failed to edit title:\n{e}")

    def _open_folder(self):
        if not self._require_selection():
            return
        tid = self.selected_track_id

        t = self.conn.execute(
            "SELECT file_path FROM tracks WHERE track_id = ?", (tid,)
        ).fetchone()
        if not t:
            return

        folder = Path(t["file_path"]).parent
        if folder.exists():
            subprocess.Popen(["explorer", "/select,", str(t["file_path"])])
            self.statusbar.config(text=f"Opened folder: {folder}")
        else:
            messagebox.showwarning("Folder Missing",
                                   f"Folder not found:\n{folder}")

    # ─── Cleanup ───────────────────────────────────────────────────

    def on_close(self):
        if self._fs_sync_job is not None:
            try:
                self.root.after_cancel(self._fs_sync_job)
            except Exception:
                pass
        self._close_db()
        self.root.destroy()

    # ─── Fast Review Queue Logic ───────────────────────────────────

    def _activate_fast_review(self):
        """Switch to the Fast Review Queue full-width view."""
        self.fast_review_active = True
        self.normal_paned.grid_remove()
        self.fr_frame.grid(row=0, column=0, sticky="nsew")
        self._fr_load_data()

    def _deactivate_fast_review(self):
        """Switch back to normal paned view."""
        self.fast_review_active = False
        self.fr_frame.grid_remove()
        self.normal_paned.grid(row=0, column=0, sticky="nsew")

    # ── Zoom helpers ──────────────────────────────────────────────

    def _fr_zoom_font_size(self, base):
        return max(6, base + self._fr_zoom)

    def _fr_zoom_in(self):
        if self._fr_zoom < 4:
            self._fr_zoom += 1
            self._fr_update_zoom_label()
            self._fr_render_rows()

    def _fr_zoom_out(self):
        if self._fr_zoom > -2:
            self._fr_zoom -= 1
            self._fr_update_zoom_label()
            self._fr_render_rows()

    def _fr_zoom_reset(self):
        self._fr_zoom = 0
        self._fr_update_zoom_label()
        self._fr_render_rows()

    def _fr_update_zoom_label(self):
        pct = 100 + self._fr_zoom * 15
        self.fr_zoom_label.config(text=f"{pct}%")

    # ── Pending state helpers ─────────────────────────────────────

    def _fr_update_pending_ui(self):
        """Update pending counts label and button states."""
        n = len(self._fr_pending)
        acc = sum(1 for v in self._fr_pending.values() if v["action"] == "ACCEPT")
        man = sum(1 for v in self._fr_pending.values() if v["action"] == "MANUAL")
        rej = sum(1 for v in self._fr_pending.values() if v["action"] == "REJECT")
        hld = sum(1 for v in self._fr_pending.values() if v["action"] == "HOLD")
        dlt = sum(1 for v in self._fr_pending.values() if v["action"] == "DELETE")
        parts = []
        if acc:
            parts.append(f"{acc} accept")
        if man:
            parts.append(f"{man} manual")
        if rej:
            parts.append(f"{rej} reject")
        if hld:
            parts.append(f"{hld} hold")
        if dlt:
            parts.append(f"{dlt} delete")
        summary = ", ".join(parts) if parts else "none"
        self.fr_pending_label.config(text=f"Pending: {n} ({summary})")
        state = "normal" if n > 0 else "disabled"
        self.fr_save_btn.config(state=state)
        self.fr_discard_btn.config(state=state)

    # ── Sort helpers ──────────────────────────────────────────────

    _FR_SORT_KEYS = {
        "file_name":        lambda r: (r["file_name"] or "").lower(),
        "chosen_title":     lambda r: (r["chosen_title"] or "").lower(),
        "final_confidence": lambda r: r["final_confidence"] or 0.0,
        "source_used":      lambda r: (r["source_used"] or "").lower(),
        "track_id":         lambda r: r["track_id"],
        "file_size":        lambda r: r["file_size"] or 0,
    }

    # Map Treeview column ids → sort keys in _FR_SORT_KEYS
    _FR_COL_TO_SORT = {
        "file_name": "file_name", "title": "chosen_title",
        "size": "file_size", "conf": "final_confidence",
        "source_reason": "source_used",
    }

    @staticmethod
    def _fr_default_sort_key(_row):
        return 0

    @staticmethod
    def _fr_tree_anchor(anchor: str) -> Literal["nw", "n", "ne", "w", "center", "e", "sw", "s", "se"]:
        if anchor == "nw":
            return "nw"
        if anchor == "n":
            return "n"
        if anchor == "ne":
            return "ne"
        if anchor == "w":
            return "w"
        if anchor == "center":
            return "center"
        if anchor == "e":
            return "e"
        if anchor == "sw":
            return "sw"
        if anchor == "s":
            return "s"
        if anchor == "se":
            return "se"
        return "w"

    def _fr_sort_by(self, col_key):
        """Sort the cached rows by the given column key."""
        # Translate Treeview column id to sort key if needed
        sort_key = self._FR_COL_TO_SORT.get(col_key, col_key) or "track_id"
        if self._fr_sort_col == sort_key:
            self._fr_sort_asc = not self._fr_sort_asc
        else:
            self._fr_sort_col = sort_key
            self._fr_sort_asc = True
        key_fn = self._FR_SORT_KEYS[sort_key] if sort_key in self._FR_SORT_KEYS else self._fr_default_sort_key
        self._fr_rows_cache.sort(key=key_fn, reverse=not self._fr_sort_asc)
        self._fr_render_rows()

    # ── Data loading ──────────────────────────────────────────────

    @staticmethod
    def _fr_suggested_fn(row):
        """Compute the suggested filename for a REVIEW row."""
        file_name = row["file_name"]
        chosen_artist = row["chosen_artist"] or ""
        chosen_title = row["chosen_title"] or ""
        ext = Path(file_name).suffix
        if chosen_artist and chosen_title:
            suggested = f"{chosen_artist} - {chosen_title}{ext}"
        elif chosen_title:
            suggested = f"{chosen_title}{ext}"
        else:
            suggested = file_name
        return re.sub(r'[<>:"/\\|?*]', '_', suggested)

    def _fr_load_data(self):
        """Load REVIEW rows, apply filter, populate table."""
        all_rows = list(self.conn.execute("""
            SELECT t.track_id, t.file_name, t.file_size,
                   hr.chosen_artist, hr.chosen_title, hr.source_used,
                   hr.final_confidence, hr.was_reversed,
                   hr.authority_reason
            FROM tracks t
            JOIN hybrid_resolution hr ON hr.track_id = t.track_id
            JOIN track_status ts ON ts.track_id = t.track_id
            WHERE ts.status = 'REVIEW'
            ORDER BY hr.final_confidence ASC
        """).fetchall())

        # Apply filter mode
        mode = self._fr_filter_mode
        if mode == "rename":
            self._fr_rows_cache = [
                r for r in all_rows
                if r["file_name"] != self._fr_suggested_fn(r)
                or r["track_id"] in self._fr_pending
            ]
        elif mode == "same":
            self._fr_rows_cache = [
                r for r in all_rows
                if r["file_name"] == self._fr_suggested_fn(r)
                and r["track_id"] not in self._fr_pending
            ]
        else:  # "all"
            self._fr_rows_cache = all_rows

        # Apply current sort
        active_sort_col = self._fr_sort_col or "track_id"
        key_fn = self._FR_SORT_KEYS[active_sort_col] if active_sort_col in self._FR_SORT_KEYS else self._fr_default_sort_key
        self._fr_rows_cache.sort(key=key_fn, reverse=not self._fr_sort_asc)

        total = len(all_rows)
        shown = len(self._fr_rows_cache)
        self.fr_count_label.config(text=f"{shown}/{total} review rows")
        self.fr_filter_count_label.config(
            text=f"({total - shown} hidden by filter)" if shown < total else "")
        self._fr_render_rows()
        self._fr_update_pending_ui()

    _FR_HEADERS = [
        ("file_name",      "File Name",        280, "file_name"),
        ("title",          "Title",             160, "chosen_title"),
        ("suggested",      "Suggested File",    260, None),
        ("size",           "Size (MB)",          70, "file_size"),
        ("conf",           "Conf",               60, "final_confidence"),
        ("source_reason",  "Source / Reason",   150, "source_used"),
        ("pending",        "Pending",            80, None),
    ]

    def _fr_render_headers(self):
        """Update treeview column headings with sort arrows."""
        for col_id, heading, _width, sort_key in self._FR_HEADERS:
            arrow = ""
            if sort_key and sort_key == self._fr_sort_col:
                arrow = " ▲" if self._fr_sort_asc else " ▼"
            self.fr_tree.heading(col_id, text=heading + arrow)

    def _fr_render_rows(self):
        """Populate the Treeview from cached data — single-widget, instant."""
        # Remember current selection
        sel = self.fr_tree.selection()
        sel_tid = sel[0] if sel else None

        # Clear all items
        self.fr_tree.delete(*self.fr_tree.get_children())
        self._fr_row_widgets.clear()

        # Update header arrows
        self._fr_render_headers()

        # Apply zoom to row height via style
        sz = self._fr_zoom_font_size(9)
        style = ttk.Style()
        style.configure("FRTree.Treeview", font=("Segoe UI", sz),
                        rowheight=max(20, sz * 2 + 6))
        style.configure("FRTree.Treeview.Heading",
                        font=("Segoe UI", self._fr_zoom_font_size(9), "bold"))
        self.fr_tree.configure(style="FRTree.Treeview")

        for row_i, row in enumerate(self._fr_rows_cache):
            tid = row["track_id"]
            vals = self._fr_row_values(row, tid)
            tag = self._fr_row_tag(tid, row_i)
            iid = str(tid)
            self.fr_tree.insert("", "end", iid=iid, values=vals, tags=(tag,))
            self._fr_row_widgets.append({"tid": tid, "row_i": row_i})

        # Restore selection
        if sel_tid and self.fr_tree.exists(sel_tid):
            self.fr_tree.selection_set(sel_tid)
            self.fr_tree.see(sel_tid)

        self._update_counts()
        self.statusbar.config(text=f"Fast Review: {len(self._fr_rows_cache)} rows | "
                              f"Sorted by {self._fr_sort_col} "
                              f"{'ASC' if self._fr_sort_asc else 'DESC'}")

    def _fr_row_values(self, row, tid):
        """Build the tuple of cell values for a Treeview row."""
        file_name = row["file_name"]
        chosen_artist = row["chosen_artist"] or ""
        chosen_title = row["chosen_title"] or ""
        ext = Path(file_name).suffix

        # Suggested filename (reuses helper)
        suggested_fn = self._fr_suggested_fn(row)

        conf = f"{row['final_confidence']:.2f}" if row["final_confidence"] else "—"
        source = row["source_used"] or ""
        try:
            if row["authority_reason"]:
                source += f" | {row['authority_reason']}"
        except (IndexError, KeyError):
            pass

        fs = row["file_size"]
        size_str = f"{fs / (1024*1024):.1f}" if fs else "—"

        # Override display values for pending MANUAL edits
        pending = self._fr_pending.get(tid)
        display_title = chosen_title
        display_suggested = suggested_fn
        if pending and pending["action"] == "MANUAL":
            ma = pending.get("artist", chosen_artist)
            mt = pending.get("title", chosen_title)
            display_title = mt
            if ma and mt:
                display_suggested = f"{ma} - {mt}{ext}"
            elif mt:
                display_suggested = f"{mt}{ext}"
            display_suggested = re.sub(r'[<>:"/\\|?*]', '_', display_suggested)

        badge = pending["action"] if pending else "—"

        return (file_name, display_title, display_suggested,
                size_str, conf, source, badge)

    def _fr_row_tag(self, tid, row_i):
        """Determine the Treeview tag for row coloring."""
        pending = self._fr_pending.get(tid)
        if pending:
            return pending["action"].lower()   # "accept", "manual", "reject"
        return "even" if row_i % 2 == 0 else "odd"

    @staticmethod
    def _fr_row_bg(pending, row_i):
        """Determine row background color based on pending action."""
        if pending:
            action = pending["action"]
            if action == "ACCEPT":
                return "#d4edda"
            elif action == "MANUAL":
                return "#fff3cd"
            elif action == "REJECT":
                return "#f8d7da"
            elif action == "HOLD":
                return "#d6e9f8"
            elif action == "DELETE":
                return "#e0b0b0"
        return "#fffbe6" if row_i % 2 == 0 else "#fff8e1"

    def _fr_make_badge(self, bg, sz, pending):
        """Return badge text for a pending action (compat helper)."""
        if pending:
            return pending["action"]
        return "—"

    def _fr_update_row_visual(self, track_id):
        """Instant in-place update of a single row's tag and values in Treeview."""
        iid = str(track_id)
        if not self.fr_tree.exists(iid):
            return
        # Find row data + index
        row_data = None
        row_i = 0
        for i, r in enumerate(self._fr_rows_cache):
            if r["track_id"] == track_id:
                row_data = r
                row_i = i
                break
        if not row_data:
            return
        # Update values and tag
        vals = self._fr_row_values(row_data, track_id)
        self.fr_tree.item(iid, values=vals,
                          tags=(self._fr_row_tag(track_id, row_i),))
        # Refresh detail pane if this row is selected
        sel = self.fr_tree.selection()
        if sel and sel[0] == iid:
            self._fr_show_detail(track_id)

    # ── Selection + Detail pane ───────────────────────────────────

    def _fr_on_select(self, event=None):
        """Handle Treeview row selection — update detail pane."""
        sel = self.fr_tree.selection()
        if not sel:
            return
        if len(sel) == 1:
            self._fr_show_detail(int(sel[0]))
        else:
            self.fr_detail_text.config(state="normal")
            self.fr_detail_text.delete("1.0", "end")
            self.fr_detail_text.insert("1.0",
                                       f"{len(sel)} rows selected — use Bulk actions")
            self.fr_detail_text.config(state="disabled")

    def _fr_show_detail(self, track_id):
        """Populate the detail pane with info for the selected track."""
        row = None
        for r in self._fr_rows_cache:
            if r["track_id"] == track_id:
                row = r
                break
        if not row:
            return
        pending = self._fr_pending.get(track_id)
        ext = Path(row["file_name"]).suffix
        artist = row["chosen_artist"] or ""
        title = row["chosen_title"] or ""
        conf = f"{row['final_confidence']:.2f}" if row["final_confidence"] else "N/A"
        source = row["source_used"] or ""
        fs = row["file_size"]
        size_mb = f"{fs / (1024*1024):.1f} MB" if fs else "N/A"
        pending_str = pending["action"] if pending else "none"
        if pending and pending["action"] == "MANUAL":
            artist = pending.get("artist", artist)
            title = pending.get("title", title)

        lines = [
            f"Track ID: {track_id}  |  File: {row['file_name']}  |  Size: {size_mb}",
            f"Artist: {artist}  |  Title: {title}  |  Source: {source}  |  Conf: {conf}",
            f"Pending: {pending_str}  |  Reversed: {row.get('was_reversed', '')}",
        ]
        self.fr_detail_text.config(state="normal")
        self.fr_detail_text.delete("1.0", "end")
        self.fr_detail_text.insert("1.0", "\n".join(lines))
        self.fr_detail_text.config(state="disabled")

    # ── Keyboard shortcut handlers ────────────────────────────────

    def _fr_selected_tid(self):
        """Return the currently selected track_id or None."""
        sel = self.fr_tree.selection()
        if not sel:
            return None
        return int(sel[0])

    def _fr_key_accept(self):
        tid = self._fr_selected_tid()
        if tid:
            self._fr_accept(tid)

    def _fr_key_manual(self):
        tid = self._fr_selected_tid()
        if tid:
            self._fr_manual(tid)

    def _fr_key_reject(self):
        tid = self._fr_selected_tid()
        if tid:
            self._fr_reject(tid)

    def _fr_key_details(self):
        tid = self._fr_selected_tid()
        if tid:
            self._fr_details(tid)

    def _fr_key_unstage(self):
        tid = self._fr_selected_tid()
        if tid:
            self._fr_unstage(tid)

    def _fr_key_delete(self):
        tid = self._fr_selected_tid()
        if tid:
            self._fr_delete(tid)

    # ── Filter ────────────────────────────────────────────────────

    def _fr_apply_filter(self):
        """Switch filter mode from the radio buttons and reload."""
        self._fr_filter_mode = self._fr_filter_var.get()
        self._fr_load_data()

    # ── Multi-select helpers ──────────────────────────────────────

    def _fr_selected_tids(self):
        """Return list of all selected track_ids."""
        return [int(iid) for iid in self.fr_tree.selection()]

    # ── Bulk actions (staged, no DB writes) ───────────────────────

    def _fr_bulk_accept(self):
        """Stage ACCEPT for all selected rows."""
        tids = self._fr_selected_tids()
        for tid in tids:
            self._fr_pending[tid] = {"action": "ACCEPT"}
        if tids:
            self._fr_update_pending_ui()
            for tid in tids:
                self._fr_update_row_visual(tid)

    def _fr_bulk_reject(self):
        """Stage REJECT for all selected rows."""
        tids = self._fr_selected_tids()
        for tid in tids:
            self._fr_pending[tid] = {"action": "REJECT"}
        if tids:
            self._fr_update_pending_ui()
            for tid in tids:
                self._fr_update_row_visual(tid)

    def _fr_bulk_hold(self):
        """Stage HOLD for all selected rows — defers review, no rename."""
        tids = self._fr_selected_tids()
        for tid in tids:
            self._fr_pending[tid] = {"action": "HOLD"}
        if tids:
            self._fr_update_pending_ui()
            for tid in tids:
                self._fr_update_row_visual(tid)

    def _fr_bulk_delete(self):
        """Stage DELETE for all selected rows — no immediate file mutation."""
        tids = self._fr_selected_tids()
        for tid in tids:
            self._fr_pending[tid] = {"action": "DELETE"}
        if tids:
            self._fr_update_pending_ui()
            for tid in tids:
                self._fr_update_row_visual(tid)

    # ── Staged row actions (no DB writes) ─────────────────────────

    def _fr_accept(self, track_id):
        """Stage an ACCEPT — visual only, no DB write yet."""
        self._fr_pending[track_id] = {"action": "ACCEPT"}
        self._fr_update_pending_ui()
        self._fr_update_row_visual(track_id)

    def _fr_reject(self, track_id):
        """Stage a REJECT — visual only, no DB write yet."""
        self._fr_pending[track_id] = {"action": "REJECT"}
        self._fr_update_pending_ui()
        self._fr_update_row_visual(track_id)

    def _fr_unstage(self, track_id):
        """Remove a pending action from a row."""
        self._fr_pending.pop(track_id, None)
        self._fr_update_pending_ui()
        self._fr_update_row_visual(track_id)

    def _fr_delete(self, track_id):
        """Stage a DELETE — visual only, no file mutation yet."""
        self._fr_pending[track_id] = {"action": "DELETE"}
        self._fr_update_pending_ui()
        self._fr_update_row_visual(track_id)

    def _fr_manual(self, track_id):
        """Open a manual edit dialog — stages the result, no DB write yet."""
        # Find row data from cache
        row_data = None
        for r in self._fr_rows_cache:
            if r["track_id"] == track_id:
                row_data = r
                break
        if not row_data:
            return

        existing = self._fr_pending.get(track_id, {})
        old_artist = existing.get("artist", row_data["chosen_artist"] or "")
        old_title = existing.get("title", row_data["chosen_title"] or "")
        ext = Path(row_data["file_name"]).suffix

        # Modal dialog
        dlg = tk.Toplevel(self.root)
        dlg.title(f"Manual Edit — Track {track_id}")
        dlg.geometry("520x250")
        dlg.transient(self.root)
        dlg.grab_set()

        ttk.Label(dlg, text=f"File: {row_data['file_name']}",
                  font=("Consolas", 9), wraplength=500).pack(
                      anchor="w", padx=10, pady=(10, 4))

        form = ttk.Frame(dlg, padding=10)
        form.pack(fill="x")

        artist_var = tk.StringVar(value=old_artist)
        title_var = tk.StringVar(value=old_title)
        preview_var = tk.StringVar()

        ttk.Label(form, text="Artist:").grid(row=0, column=0, sticky="e", padx=(0, 4))
        ttk.Entry(form, textvariable=artist_var, width=50).grid(
            row=0, column=1, sticky="ew", pady=2)
        ttk.Label(form, text="Title:").grid(row=1, column=0, sticky="e", padx=(0, 4))
        ttk.Entry(form, textvariable=title_var, width=50).grid(
            row=1, column=1, sticky="ew", pady=2)
        ttk.Label(form, text="Preview:").grid(row=2, column=0, sticky="e", padx=(0, 4))
        ttk.Label(form, textvariable=preview_var,
                  font=("Consolas", 9), foreground="#555").grid(
                      row=2, column=1, sticky="w", pady=2)
        form.columnconfigure(1, weight=1)

        def _upd(*_):
            a = artist_var.get().strip()
            tv = title_var.get().strip()
            if a and tv:
                p = f"{a} - {tv}{ext}"
            elif tv:
                p = f"{tv}{ext}"
            else:
                p = "(empty)"
            preview_var.set(re.sub(r'[<>:"/\\|?*]', '_', p))
        artist_var.trace_add("write", _upd)
        title_var.trace_add("write", _upd)
        _upd()

        def _stage():
            new_artist = artist_var.get().strip()
            new_title = title_var.get().strip()
            if not new_artist and not new_title:
                messagebox.showwarning("Empty",
                                       "Artist and title cannot both be empty.",
                                       parent=dlg)
                return
            self._fr_pending[track_id] = {
                "action": "MANUAL",
                "artist": new_artist,
                "title": new_title,
            }
            dlg.destroy()
            self._fr_update_pending_ui()
            self._fr_update_row_visual(track_id)

        btn_frame = ttk.Frame(dlg)
        btn_frame.pack(pady=10)
        tk.Button(btn_frame, text="✔ STAGE", bg="#17a2b8", fg="white",
                  font=("Segoe UI", 10, "bold"), width=14,
                  command=_stage).pack(side="left", padx=6)
        tk.Button(btn_frame, text="Cancel", width=10,
                  command=dlg.destroy).pack(side="left", padx=6)

    # ── Bulk commit / discard ─────────────────────────────────────

    def _fr_save_pending(self):
        """Commit all pending staged actions to the DB in one transaction."""
        if not self._fr_pending:
            return
        n = len(self._fr_pending)
        now = datetime.now().isoformat()
        try:
            for tid, entry in self._fr_pending.items():
                action = entry["action"]
                hr = self.conn.execute(
                    "SELECT chosen_artist, chosen_title "
                    "FROM hybrid_resolution WHERE track_id = ?", (tid,)
                ).fetchone()
                if not hr:
                    continue
                artist = hr["chosen_artist"] or ""
                title = hr["chosen_title"] or ""

                if action == "ACCEPT":
                    self.conn.execute(
                        "UPDATE track_status SET status = 'CLEAN' "
                        "WHERE track_id = ?", (tid,))
                    self.conn.execute(
                        "UPDATE hybrid_resolution SET requires_review = 0 "
                        "WHERE track_id = ?", (tid,))
                    # Rename file on disk to match accepted artist/title
                    rename_ok, rename_reason, rename_path = \
                        self._fr_rename_file_safe(tid, artist, title)
                    rename_note = ""
                    if rename_ok and rename_path:
                        self.conn.execute(
                            "UPDATE tracks SET file_name = ?, file_path = ? "
                            "WHERE track_id = ?",
                            (rename_path.name, str(rename_path), tid))
                        rename_note = f" | renamed→{rename_path.name}"
                    elif not rename_ok:
                        rename_note = f" | rename skipped: {rename_reason}"
                    self.conn.execute(
                        "INSERT INTO audit_log "
                        "(track_id, event_type, event_description, timestamp) "
                        "VALUES (?, ?, ?, ?)",
                        (tid, "FAST_REVIEW_ACCEPT",
                         f"Fast-accept (staged): '{artist}' - '{title}'"
                         f"{rename_note}", now))
                    self._feed_authority(artist, title,
                                         "operator_fast_accept", 0.90, now)

                elif action == "REJECT":
                    self.conn.execute(
                        "INSERT INTO audit_log "
                        "(track_id, event_type, event_description, timestamp) "
                        "VALUES (?, ?, ?, ?)",
                        (tid, "FAST_REVIEW_REJECT",
                         f"Fast-reject (staged): '{artist}' - '{title}'", now))

                elif action == "HOLD":
                    self.conn.execute(
                        "INSERT INTO audit_log "
                        "(track_id, event_type, event_description, timestamp) "
                        "VALUES (?, ?, ?, ?)",
                        (tid, "FAST_REVIEW_HOLD",
                         f"Fast-hold (staged): '{artist}' - '{title}'", now))

                elif action == "DELETE":
                    # Staged audit entry first
                    self.conn.execute(
                        "INSERT INTO audit_log "
                        "(track_id, event_type, event_description, timestamp) "
                        "VALUES (?, ?, ?, ?)",
                        (tid, "REVIEW_DELETE_STAGED",
                         f"Delete staged: '{artist}' - '{title}'", now))
                    # Safe delete with scope + existence checks
                    ok, reason = self._fr_delete_file_safe(tid)
                    if ok:
                        self.conn.execute(
                            "UPDATE track_status SET status = 'JUNK' "
                            "WHERE track_id = ?", (tid,))
                        self.conn.execute(
                            "INSERT INTO audit_log "
                            "(track_id, event_type, event_description, "
                            "timestamp) VALUES (?, ?, ?, ?)",
                            (tid, "REVIEW_DELETE_COMMIT",
                             f"File deleted: {reason}", now))
                    else:
                        self.conn.execute(
                            "INSERT INTO audit_log "
                            "(track_id, event_type, event_description, "
                            "timestamp) VALUES (?, ?, ?, ?)",
                            (tid, "REVIEW_DELETE_BLOCKED",
                             f"Delete blocked: {reason}", now))

                elif action == "MANUAL":
                    new_artist = entry.get("artist", artist)
                    new_title = entry.get("title", title)
                    self.conn.execute(
                        "UPDATE hybrid_resolution SET chosen_artist = ?, "
                        "chosen_title = ?, source_used = 'hybrid', "
                        "requires_review = 0 WHERE track_id = ?",
                        (new_artist, new_title, tid))
                    self.conn.execute(
                        "UPDATE track_status SET status = 'CLEAN' "
                        "WHERE track_id = ?", (tid,))
                    self.conn.execute(
                        "INSERT INTO audit_log "
                        "(track_id, event_type, event_description, timestamp) "
                        "VALUES (?, ?, ?, ?)",
                        (tid, "FAST_REVIEW_MANUAL",
                         f"Fast-manual (staged): '{new_artist}' - '{new_title}' "
                         f"(was: '{artist}' - '{title}')", now))
                    try:
                        self.conn.execute(
                            "UPDATE authority_parse_history "
                            "SET operator_verified = 1, "
                            "resolved_artist = ?, resolved_title = ? "
                            "WHERE track_id = ?",
                            (new_artist, new_title, tid))
                    except Exception:
                        pass
                    self._feed_authority(new_artist, new_title,
                                         "operator_fast_manual", 1.0, now)

            self.conn.commit()
            self._fr_pending.clear()
            self.statusbar.config(text=f"SAVED {n} staged actions")
            self._fr_load_data()  # single refresh
        except Exception as e:
            self.conn.rollback()
            messagebox.showerror("DB Error", f"Staged save failed:\n{e}")

    # ── Delete safety ───────────────────────────────────────────────

    # Managed scope: only allow deletes under these roots
    _DELETE_ALLOWED_ROOTS = [
        Path(r"C:\Users\suppo\Downloads\New Music"),
        Path(r"C:\Users\suppo\Downloads\70s-80s"),
    ]

    def _fr_delete_file_safe(self, track_id):
        """
        Safely delete a file for the given track_id.
        Returns (success: bool, reason: str).
        """
        row = self.conn.execute(
            "SELECT file_path FROM tracks WHERE track_id = ?",
            (track_id,),
        ).fetchone()
        if not row or not row["file_path"]:
            return False, "no file_path in DB"

        fp = Path(row["file_path"])

        # Must not be a symlink
        if fp.is_symlink():
            return False, f"symlink refused: {fp}"

        # Must be under managed scope
        resolved = fp.resolve()
        in_scope = any(
            str(resolved).startswith(str(root.resolve()))
            for root in self._DELETE_ALLOWED_ROOTS
        )
        if not in_scope:
            return False, f"outside managed scope: {fp}"

        # Must exist
        if not fp.exists():
            return False, f"file already missing: {fp}"

        # Single file delete — no wildcards
        try:
            fp.unlink()
            return True, str(fp)
        except OSError as e:
            return False, f"OS error: {e}"

    def _fr_rename_file_safe(self, track_id, artist, title):
        """
        Safely rename a file for the given track_id within managed roots.
        Returns (success: bool, reason: str, new_path: Path | None).
        """
        row = self.conn.execute(
            "SELECT file_path FROM tracks WHERE track_id = ?",
            (track_id,),
        ).fetchone()
        if not row or not row["file_path"]:
            return False, "no file_path in DB", None

        fp = Path(row["file_path"])
        if fp.is_symlink():
            return False, f"symlink refused: {fp}", None
        if not fp.exists():
            return False, f"file missing: {fp}", None

        resolved = fp.resolve()
        in_scope = any(
            str(resolved).startswith(str(root.resolve()))
            for root in self._DELETE_ALLOWED_ROOTS
        )
        if not in_scope:
            return False, f"outside managed scope: {fp}", None

        ext = fp.suffix
        if artist and title:
            target_name = f"{artist} - {title}{ext}"
        elif title:
            target_name = f"{title}{ext}"
        else:
            return False, "empty target name", None

        target_name = re.sub(r'[<>:"/\\|?*]', '_', target_name).strip()
        if not target_name:
            return False, "sanitized target name empty", None

        new_path = fp.with_name(target_name)
        if new_path == fp:
            return False, "already matches target name", None
        if new_path.exists():
            return False, f"target exists: {new_path.name}", None

        try:
            fp.rename(new_path)
            return True, "renamed", new_path
        except OSError as e:
            return False, f"OS error: {e}", None

    # ── MusicBrainz lookup ────────────────────────────────────────

    def _fr_lookup_musicbrainz(self):
        """Lookup the selected row on MusicBrainz — single-row only."""
        tid = self._fr_selected_tid()
        if not tid:
            messagebox.showinfo("MusicBrainz", "Select a row first.")
            return

        if not _HAS_MUSICBRAINZ or lookup_recording is None:
            messagebox.showwarning("MusicBrainz",
                                    "MusicBrainz module not available.\n"
                                    "Ensure db/dj_musicbrainz.py exists.")
            return

        # Find row data
        row_data = None
        for r in self._fr_rows_cache:
            if r["track_id"] == tid:
                row_data = r
                break
        if not row_data:
            return

        artist = row_data["chosen_artist"] or ""
        title = row_data["chosen_title"] or ""
        filename = row_data["file_name"] or ""

        # Show searching indicator
        self.statusbar.config(text="Searching MusicBrainz...")
        self.root.update_idletasks()

        results, error, attempt_log = lookup_recording(
            artist=artist, title=title, filename=filename)

        self._fr_show_mb_results(tid, row_data, results, error, attempt_log)

    def _fr_show_mb_results(self, track_id, row_data, results,
                            error, attempt_log):
        """Display MusicBrainz results + debug log in a modal dialog."""
        dlg = tk.Toplevel(self.root)
        dlg.title(f"MusicBrainz Results — Track {track_id}")
        dlg.geometry("820x580")
        dlg.transient(self.root)
        dlg.grab_set()

        ttk.Label(dlg, text=f"File: {row_data['file_name']}",
                  font=("Consolas", 9), wraplength=790).pack(
                      anchor="w", padx=10, pady=(10, 2))
        ttk.Label(dlg, text=f"Current: {row_data['chosen_artist'] or '?'}"
                  f" — {row_data['chosen_title'] or '?'}",
                  font=("Consolas", 9), foreground="#555").pack(
                      anchor="w", padx=10, pady=(0, 4))

        # ── Debug / Attempt Log panel ─────────────────────────────
        debug_frame = ttk.LabelFrame(dlg, text="Query Attempts (debug)")
        debug_frame.pack(fill="x", padx=10, pady=(0, 4))

        debug_text = tk.Text(debug_frame, height=6, font=("Consolas", 8),
                             wrap="word", state="normal", bg="#f8f8f0")
        debug_text.pack(fill="x", padx=4, pady=4)

        for entry in attempt_log:
            marker = "→ CHOSEN" if entry["chosen"] else ""
            err_str = f"  ERR={entry['error']}" if entry["error"] else ""
            line = (f"  {'✔' if entry['chosen'] else '·'} "
                    f"{entry['label']}  "
                    f"q=\"{entry['query']}\"  "
                    f"hits={entry['count']}{err_str}  {marker}\n")
            debug_text.insert("end", line)

        if error:
            debug_text.insert("end", f"\n  ✖ FINAL ERROR: {error}\n")
        if not attempt_log:
            debug_text.insert("end", "  (no attempts made)\n")
        debug_text.config(state="disabled")

        # ── Results or error ──────────────────────────────────────
        if error and not results:
            ttk.Label(dlg, text=f"Error: {error}",
                      font=("Segoe UI", 10), foreground="red").pack(
                          anchor="w", padx=10, pady=6)
            tk.Button(dlg, text="Close", width=10,
                      command=dlg.destroy).pack(pady=8)
            self.statusbar.config(text=f"MusicBrainz: {error}")
            return

        if not results:
            ttk.Label(dlg, text="No results found across all attempts.",
                      font=("Segoe UI", 10), foreground="#856404").pack(
                          anchor="w", padx=10, pady=6)
            tk.Button(dlg, text="Close", width=10,
                      command=dlg.destroy).pack(pady=8)
            self.statusbar.config(text="MusicBrainz: no results")
            return

        # Results treeview
        cols = ("score", "artist", "title", "release", "date", "mbid")
        tree = ttk.Treeview(dlg, columns=cols, show="headings",
                            selectmode="browse", height=8)
        for cid, hdr, w in [("score", "Score", 50), ("artist", "Artist", 180),
                             ("title", "Title", 180), ("release", "Release", 140),
                             ("date", "Date", 60), ("mbid", "MBID", 80)]:
            tree.heading(cid, text=hdr)
            tree.column(cid, width=w, minwidth=40)
        for i, r in enumerate(results):
            tree.insert("", "end", iid=str(i), values=(
                r["score"], r["artist"], r["title"],
                r["release"], r["release_date"],
                r["mbid"][:12] + "…" if len(r["mbid"]) > 12 else r["mbid"]
            ))
        tree.pack(fill="both", expand=True, padx=10, pady=4)

        # Select first result by default
        if results:
            tree.selection_set("0")

        btn_frame = ttk.Frame(dlg)
        btn_frame.pack(fill="x", padx=10, pady=8)

        def _apply():
            sel = tree.selection()
            if not sel:
                messagebox.showinfo("Select", "Select a result first.",
                                     parent=dlg)
                return
            idx = int(sel[0])
            chosen = results[idx]
            self._fr_pending[track_id] = {
                "action": "MANUAL",
                "artist": chosen["artist"],
                "title": chosen["title"],
                "mb_source": f"musicbrainz:{chosen['mbid']}",
            }
            dlg.destroy()
            self._fr_update_pending_ui()
            self._fr_update_row_visual(track_id)
            self.statusbar.config(
                text=f"MusicBrainz: staged '{chosen['artist']}' - "
                     f"'{chosen['title']}' for track {track_id}")

        tk.Button(btn_frame, text="✔ Use Artist/Title",
                  bg="#28a745", fg="white",
                  font=("Segoe UI", 10, "bold"), padx=10,
                  command=_apply).pack(side="left", padx=6)
        tk.Button(btn_frame, text="Cancel", width=10,
                  command=dlg.destroy).pack(side="left", padx=6)

        chosen_entry = next((e for e in attempt_log if e["chosen"]), None)
        if chosen_entry:
            self.statusbar.config(
                text=f"MusicBrainz: {len(results)} results via "
                     f"{chosen_entry['label']}")

    def _fr_discard_pending(self):
        """Discard all pending staged actions, restore display."""
        self._fr_pending.clear()
        self._fr_update_pending_ui()
        self._fr_render_rows()
        self.statusbar.config(text="Discarded all pending changes")

    def _fr_details(self, track_id):
        """Switch to normal view and show full details for this track."""
        self.current_tab = "REVIEW"
        for t, btn in self.tab_buttons.items():
            if t == "REVIEW":
                btn.state(["pressed"])
            else:
                btn.state(["!pressed"])
        self._deactivate_fast_review()
        self._load_data()
        self.selected_track_id = track_id
        try:
            self.tree.selection_set(str(track_id))
            self.tree.see(str(track_id))
        except tk.TclError:
            pass
        self._show_details(track_id)
        self._populate_review_workstation(track_id)

    # ─── Review Workstation Logic ──────────────────────────────────

    def _populate_review_workstation(self, track_id):
        """Populate the review workstation for a REVIEW-status track."""
        # Check if track is REVIEW
        ts = self.conn.execute(
            "SELECT status FROM track_status WHERE track_id = ?", (track_id,)
        ).fetchone()
        if not ts or ts["status"] != "REVIEW":
            self.review_frame.grid_remove()
            return

        self.rv_track_id = track_id
        self.rv_manual_mode = False

        # Clear inner frame
        for w in self.rv_inner.winfo_children():
            w.destroy()

        # Fetch all needed data
        t = self.conn.execute(
            "SELECT * FROM tracks WHERE track_id = ?", (track_id,)
        ).fetchone()
        fp = self.conn.execute(
            "SELECT * FROM filename_parse WHERE track_id = ?", (track_id,)
        ).fetchone()
        mt = self.conn.execute(
            "SELECT * FROM metadata_tags WHERE track_id = ?", (track_id,)
        ).fetchone()
        hr = self.conn.execute(
            "SELECT * FROM hybrid_resolution WHERE track_id = ?", (track_id,)
        ).fetchone()

        if not t or not hr:
            self.review_frame.grid_remove()
            return

        self.review_frame.grid()
        row = 0

        def add_header(text, fg="#1a5276"):
            nonlocal row
            lbl = ttk.Label(self.rv_inner, text=text,
                            font=("Segoe UI", 10, "bold"), foreground=fg)
            lbl.grid(row=row, column=0, columnspan=3, sticky="w", pady=(6, 2))
            row += 1

        def add_rv_field(label, value, col_start=0, bold=False):
            nonlocal row
            font = ("Segoe UI", 9, "bold") if bold else ("Segoe UI", 9)
            ttk.Label(self.rv_inner, text=f"{label}:",
                      font=("Segoe UI", 9)).grid(
                          row=row, column=col_start, sticky="ne", padx=(0, 4))
            ttk.Label(self.rv_inner, text=str(value or "—"),
                      font=font, wraplength=300).grid(
                          row=row, column=col_start + 1, sticky="nw")
            row += 1

        # ── Section 1: Current File ───────────────────────────────
        add_header("📄 CURRENT FILE")
        add_rv_field("File Name", t["file_name"], bold=True)
        add_rv_field("Folder", t["folder"])

        # ── Section 2: Metadata ───────────────────────────────────
        add_header("🏷️ METADATA")
        if mt:
            add_rv_field("Meta Artist", mt["artist_tag"])
            add_rv_field("Meta Title", mt["title_tag"])
            if mt["album"]:
                add_rv_field("Album", mt["album"])
        else:
            add_rv_field("Metadata", "(none)")

        # ── Section 3: Filename Parse ─────────────────────────────
        add_header("📋 FILENAME PARSE")
        if fp:
            add_rv_field("Parsed Artist", fp["artist_guess"])
            add_rv_field("Parsed Title", fp["title_guess"])
        else:
            add_rv_field("Parse", "(none)")

        # ── Section 4: Suggested Resolution (DECISION-FIRST) ─────
        add_header("✅ SUGGESTED RESOLUTION", fg="#196f3d")
        add_rv_field("Suggested Artist", hr["chosen_artist"], bold=True)
        add_rv_field("Suggested Title", hr["chosen_title"], bold=True)

        # Build suggested filename preview
        s_artist = hr["chosen_artist"] or ""
        s_title = hr["chosen_title"] or ""
        ext = Path(t["file_name"]).suffix
        if s_artist and s_title:
            suggested_fn = f"{s_artist} - {s_title}{ext}"
        elif s_title:
            suggested_fn = f"{s_title}{ext}"
        else:
            suggested_fn = t["file_name"]
        # Sanitize for display
        suggested_fn = re.sub(r'[<>:"/\\|?*]', '_', suggested_fn)
        add_rv_field("Suggested File Name", suggested_fn)

        # ── Section 5: Confidence & Context ───────────────────────
        add_header("📊 CONFIDENCE")
        add_rv_field("Confidence",
                     f"{hr['final_confidence']:.3f}" if hr['final_confidence'] else "—")
        add_rv_field("Source Used", hr["source_used"])
        add_rv_field("Was Reversed", "Yes" if hr["was_reversed"] else "No")

        # Authority context (if Phase 5 columns exist)
        try:
            auth_used = hr["authority_used"]
            auth_reason = hr["authority_reason"]
            if auth_used:
                add_header("🔑 AUTHORITY CONTEXT", fg="#7d3c98")
                add_rv_field("Authority Used", "Yes")
                add_rv_field("Reason", auth_reason)
                add_rv_field("Artist Score",
                             f"{hr['authority_artist_score']:.3f}")
                add_rv_field("Title Score",
                             f"{hr['authority_title_score']:.3f}")
                add_rv_field("Pair Score",
                             f"{hr['authority_pair_score']:.3f}")
                add_rv_field("Reversal Score",
                             f"{hr['authority_reversal_score']:.3f}")
        except (IndexError, KeyError):
            pass  # authority columns not yet added

        # ── Section 6: Action Buttons ─────────────────────────────
        add_header("⚡ DECISION")

        btn_frame = ttk.Frame(self.rv_inner)
        btn_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=4)
        row += 1

        accept_btn = tk.Button(
            btn_frame, text="✔ ACCEPT", bg="#28a745", fg="white",
            font=("Segoe UI", 10, "bold"), width=12, relief="raised",
            command=lambda: self._rv_accept(track_id)
        )
        accept_btn.pack(side="left", padx=4, pady=2)

        manual_btn = tk.Button(
            btn_frame, text="✏ MANUAL", bg="#ffc107", fg="black",
            font=("Segoe UI", 10, "bold"), width=12, relief="raised",
            command=lambda: self._rv_manual_toggle(track_id)
        )
        manual_btn.pack(side="left", padx=4, pady=2)

        reject_btn = tk.Button(
            btn_frame, text="✖ REJECT", bg="#dc3545", fg="white",
            font=("Segoe UI", 10, "bold"), width=12, relief="raised",
            command=lambda: self._rv_reject(track_id)
        )
        reject_btn.pack(side="left", padx=4, pady=2)

        # ── Manual edit fields (initially hidden) ─────────────────
        self.rv_manual_frame = ttk.LabelFrame(
            self.rv_inner, text="Manual Correction", padding=6
        )
        # Not gridded yet — shown when MANUAL is clicked

        self.rv_edit_artist.set(hr["chosen_artist"] or "")
        self.rv_edit_title.set(hr["chosen_title"] or "")

        ttk.Label(self.rv_manual_frame, text="Artist:").grid(
            row=0, column=0, sticky="e", padx=(0, 4))
        ttk.Entry(self.rv_manual_frame, textvariable=self.rv_edit_artist,
                  width=40).grid(row=0, column=1, sticky="ew", pady=2)

        ttk.Label(self.rv_manual_frame, text="Title:").grid(
            row=1, column=0, sticky="e", padx=(0, 4))
        ttk.Entry(self.rv_manual_frame, textvariable=self.rv_edit_title,
                  width=40).grid(row=1, column=1, sticky="ew", pady=2)

        # Preview label for suggested filename
        self.rv_preview_var = tk.StringVar()
        ttk.Label(self.rv_manual_frame, text="Preview:").grid(
            row=2, column=0, sticky="e", padx=(0, 4))
        ttk.Label(self.rv_manual_frame, textvariable=self.rv_preview_var,
                  font=("Consolas", 9), foreground="#555").grid(
                      row=2, column=1, sticky="w", pady=2)

        def _update_preview(*args):
            a = self.rv_edit_artist.get().strip()
            t_val = self.rv_edit_title.get().strip()
            if a and t_val:
                preview = f"{a} - {t_val}{ext}"
            elif t_val:
                preview = f"{t_val}{ext}"
            else:
                preview = "(empty)"
            self.rv_preview_var.set(re.sub(r'[<>:"/\\|?*]', '_', preview))

        self.rv_edit_artist.trace_add("write", _update_preview)
        self.rv_edit_title.trace_add("write", _update_preview)
        _update_preview()

        save_btn = tk.Button(
            self.rv_manual_frame, text="💾 SAVE MANUAL",
            bg="#17a2b8", fg="white", font=("Segoe UI", 9, "bold"),
            command=lambda: self._rv_manual_save(track_id)
        )
        save_btn.grid(row=3, column=0, columnspan=2, pady=6)

        self.rv_manual_row = row

    def _rv_accept(self, track_id):
        """Accept the suggested resolution — mark CLEAN, audit, feed authority."""
        hr = self.conn.execute(
            "SELECT chosen_artist, chosen_title, final_confidence "
            "FROM hybrid_resolution WHERE track_id = ?", (track_id,)
        ).fetchone()
        if not hr:
            return

        artist = hr["chosen_artist"] or ""
        title = hr["chosen_title"] or ""

        confirm = messagebox.askyesno(
            "Accept Resolution",
            f"Accept this resolution?\n\n"
            f"Artist: {artist}\nTitle: {title}\n\n"
            f"This will mark the track CLEAN."
        )
        if not confirm:
            return

        now = datetime.now().isoformat()
        try:
            # Mark CLEAN
            self.conn.execute(
                "UPDATE track_status SET status = 'CLEAN' WHERE track_id = ?",
                (track_id,)
            )
            # Clear review flag
            self.conn.execute(
                "UPDATE hybrid_resolution SET requires_review = 0 WHERE track_id = ?",
                (track_id,)
            )
            # Audit log
            self.conn.execute(
                "INSERT INTO audit_log (track_id, event_type, event_description, timestamp) "
                "VALUES (?, ?, ?, ?)",
                (track_id, "REVIEW_ACCEPT",
                 f"Operator accepted resolution: '{artist}' - '{title}'", now)
            )
            # Feed authority layer (if tables exist)
            self._feed_authority(artist, title, "operator_review_accept", 0.95, now)

            self.conn.commit()
            self.statusbar.config(text=f"Track {track_id}: ACCEPTED → CLEAN")
            self._load_data()
            self.review_frame.grid_remove()
        except Exception as e:
            self.conn.rollback()
            messagebox.showerror("DB Error", f"Accept failed:\n{e}")

    def _rv_manual_toggle(self, track_id):
        """Toggle manual edit mode."""
        if self.rv_manual_mode:
            self.rv_manual_frame.grid_remove()
            self.rv_manual_mode = False
        else:
            self.rv_manual_frame.grid(
                row=self.rv_manual_row, column=0, columnspan=3,
                sticky="ew", pady=4
            )
            self.rv_manual_mode = True

    def _rv_manual_save(self, track_id):
        """Save manual correction — update DB, audit, feed authority."""
        new_artist = self.rv_edit_artist.get().strip()
        new_title = self.rv_edit_title.get().strip()

        if not new_artist and not new_title:
            messagebox.showwarning("Empty", "Artist and title cannot both be empty.")
            return

        hr = self.conn.execute(
            "SELECT chosen_artist, chosen_title FROM hybrid_resolution WHERE track_id = ?",
            (track_id,)
        ).fetchone()
        old_artist = hr["chosen_artist"] if hr else ""
        old_title = hr["chosen_title"] if hr else ""

        confirm = messagebox.askyesno(
            "Save Manual Correction",
            f"Save manual correction?\n\n"
            f"Artist: {old_artist} → {new_artist}\n"
            f"Title: {old_title} → {new_title}\n\n"
            f"This will mark the track CLEAN and operator-verified."
        )
        if not confirm:
            return

        now = datetime.now().isoformat()
        try:
            # Update hybrid resolution
            self.conn.execute(
                "UPDATE hybrid_resolution SET chosen_artist = ?, chosen_title = ?, "
                "source_used = 'hybrid', requires_review = 0 WHERE track_id = ?",
                (new_artist, new_title, track_id)
            )
            # Mark CLEAN
            self.conn.execute(
                "UPDATE track_status SET status = 'CLEAN' WHERE track_id = ?",
                (track_id,)
            )
            # Audit: artist change
            if new_artist != old_artist:
                self.conn.execute(
                    "INSERT INTO audit_log (track_id, event_type, event_description, timestamp) "
                    "VALUES (?, ?, ?, ?)",
                    (track_id, "ARTIST_EDIT",
                     f"Manual review edit: '{old_artist}' → '{new_artist}'", now)
                )
            # Audit: title change
            if new_title != old_title:
                self.conn.execute(
                    "INSERT INTO audit_log (track_id, event_type, event_description, timestamp) "
                    "VALUES (?, ?, ?, ?)",
                    (track_id, "TITLE_EDIT",
                     f"Manual review edit: '{old_title}' → '{new_title}'", now)
                )
            # Audit: review action
            self.conn.execute(
                "INSERT INTO audit_log (track_id, event_type, event_description, timestamp) "
                "VALUES (?, ?, ?, ?)",
                (track_id, "REVIEW_MANUAL",
                 f"Operator manual correction: '{new_artist}' - '{new_title}' "
                 f"(was: '{old_artist}' - '{old_title}')", now)
            )
            # Mark operator_verified in authority_parse_history
            self.conn.execute(
                "UPDATE authority_parse_history SET operator_verified = 1, "
                "resolved_artist = ?, resolved_title = ? WHERE track_id = ?",
                (new_artist, new_title, track_id)
            )
            # Feed authority layer
            self._feed_authority(new_artist, new_title,
                                 "operator_manual_correction", 1.0, now)

            self.conn.commit()
            self.statusbar.config(
                text=f"Track {track_id}: MANUAL CORRECTION saved → CLEAN")
            self._load_data()
            self.review_frame.grid_remove()
        except Exception as e:
            self.conn.rollback()
            messagebox.showerror("DB Error", f"Manual save failed:\n{e}")

    def _rv_reject(self, track_id):
        """Reject the suggestion — keep in review, audit the rejection."""
        hr = self.conn.execute(
            "SELECT chosen_artist, chosen_title FROM hybrid_resolution WHERE track_id = ?",
            (track_id,)
        ).fetchone()
        artist = hr["chosen_artist"] if hr else ""
        title = hr["chosen_title"] if hr else ""

        confirm = messagebox.askyesno(
            "Reject Resolution",
            f"Reject this resolution?\n\n"
            f"Artist: {artist}\nTitle: {title}\n\n"
            f"Track stays in REVIEW. No files will be modified."
        )
        if not confirm:
            return

        now = datetime.now().isoformat()
        try:
            self.conn.execute(
                "INSERT INTO audit_log (track_id, event_type, event_description, timestamp) "
                "VALUES (?, ?, ?, ?)",
                (track_id, "REVIEW_REJECT",
                 f"Operator rejected resolution: '{artist}' - '{title}'", now)
            )
            self.conn.commit()
            self.statusbar.config(
                text=f"Track {track_id}: REJECTED — stays in REVIEW")
            self.review_frame.grid_remove()
        except Exception as e:
            self.conn.rollback()
            messagebox.showerror("DB Error", f"Reject failed:\n{e}")

    def _feed_authority(self, artist, title, source, confidence, now):
        """Feed artist/title into authority tables if they exist."""
        try:
            # Check if authority tables exist
            tables = [r[0] for r in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'authority_%'"
            ).fetchall()]
            if "authority_artists" not in tables:
                return  # authority layer not yet installed

            # Lazy import normalization
            sys.path.insert(0, str(BASE / "db"))
            try:
                from dj_authority_phase4 import normalize_artist, normalize_title
            except ImportError:
                return

            norm_a = normalize_artist(artist)
            norm_t = normalize_title(title)
            artist_id = None
            title_id = None

            if norm_a:
                existing = self.conn.execute(
                    "SELECT authority_artist_id, times_seen FROM authority_artists "
                    "WHERE normalized_artist = ?", (norm_a,)
                ).fetchone()
                if existing:
                    self.conn.execute(
                        "UPDATE authority_artists SET times_seen = times_seen + 1, "
                        "last_seen_timestamp = ?, confidence = MAX(confidence, ?) "
                        "WHERE authority_artist_id = ?",
                        (now, confidence, existing["authority_artist_id"])
                    )
                    artist_id = existing["authority_artist_id"]
                else:
                    cur = self.conn.execute(
                        "INSERT INTO authority_artists "
                        "(canonical_artist, normalized_artist, source, confidence, "
                        "times_seen, last_seen_timestamp) VALUES (?, ?, ?, ?, 1, ?)",
                        (artist, norm_a, source, confidence, now)
                    )
                    artist_id = cur.lastrowid

            if norm_t:
                existing = self.conn.execute(
                    "SELECT authority_title_id, times_seen FROM authority_titles "
                    "WHERE normalized_title = ?", (norm_t,)
                ).fetchone()
                if existing:
                    self.conn.execute(
                        "UPDATE authority_titles SET times_seen = times_seen + 1, "
                        "last_seen_timestamp = ?, confidence = MAX(confidence, ?) "
                        "WHERE authority_title_id = ?",
                        (now, confidence, existing["authority_title_id"])
                    )
                    title_id = existing["authority_title_id"]
                else:
                    cur = self.conn.execute(
                        "INSERT INTO authority_titles "
                        "(canonical_title, normalized_title, source, confidence, "
                        "times_seen, last_seen_timestamp) VALUES (?, ?, ?, ?, 1, ?)",
                        (title, norm_t, source, confidence, now)
                    )
                    title_id = cur.lastrowid

            # Create pair if both exist
            if artist_id is not None and title_id is not None:
                existing_pair = self.conn.execute(
                    "SELECT pair_id FROM authority_artist_title_pairs "
                    "WHERE authority_artist_id = ? AND authority_title_id = ?",
                    (artist_id, title_id)
                ).fetchone()
                if existing_pair:
                    self.conn.execute(
                        "UPDATE authority_artist_title_pairs "
                        "SET times_seen = times_seen + 1, last_seen_timestamp = ?, "
                        "pair_confidence = MAX(pair_confidence, ?) WHERE pair_id = ?",
                        (now, confidence, existing_pair["pair_id"])
                    )
                else:
                    self.conn.execute(
                        "INSERT INTO authority_artist_title_pairs "
                        "(authority_artist_id, authority_title_id, pair_confidence, "
                        "times_seen, source, last_seen_timestamp) "
                        "VALUES (?, ?, ?, 1, ?, ?)",
                        (artist_id, title_id, confidence, source, now)
                    )

            # Authority audit log
            self.conn.execute(
                "INSERT INTO authority_audit_log "
                "(event_type, event_description, timestamp) VALUES (?, ?, ?)",
                ("REVIEW_FEED",
                 f"Fed from review: '{artist}' - '{title}' src={source}", now)
            )
        except Exception:
            pass  # best-effort authority feed, don't block review action


def main():
    root = tk.Tk()
    app = DJDashboard(root)
    root.protocol("WM_DELETE_WINDOW", app.on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
