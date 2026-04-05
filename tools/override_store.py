"""
NGKsPlayerNative — Override Store
Persistence adapter for override entries, events, and merge history.
Uses JSONL append-only event log + JSON state snapshot.
"""

import json
import os
import shutil
from dataclasses import asdict
from datetime import datetime
from typing import Optional

from override_manager_state_model import (
    AuditEvent,
    MergeBatch,
    OverrideEntry,
    OverrideState,
    RowIdentity,
)

WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
DEFAULT_STORE_DIR = os.path.join(WORKSPACE, "data", "overrides")

EVENTS_FILE = "override_events.jsonl"
SNAPSHOT_FILE = "override_state.json"
SNAPSHOT_BAK = "override_state.json.bak"
SNAPSHOT_TMP = "override_state.json.tmp"
MERGE_HISTORY_FILE = "merge_history.jsonl"
ADAPTER_LOG_FILE = "adapter_log.jsonl"


def _norm_key(filename: str) -> str:
    return (filename or "").strip().lower()


def _entry_to_dict(e: OverrideEntry) -> dict:
    return {
        "override_id": e.override_id,
        "revision": e.revision,
        "row_identity": {
            "filename": e.row_identity.filename,
            "artist": e.row_identity.artist,
            "title": e.row_identity.title,
            "row_number": e.row_identity.row_number,
        },
        "state": e.state.value if isinstance(e.state, OverrideState) else e.state,
        "override_bpm": e.override_bpm,
        "override_key": e.override_key,
        "override_scope": e.override_scope,
        "bpm_reason": e.bpm_reason,
        "key_reason": e.key_reason,
        "notes": e.notes,
        "entered_by": e.entered_by,
        "validation_message": e.validation_message,
        "conflict_flag": e.conflict_flag,
        "original_bpm": e.original_bpm,
        "original_key": e.original_key,
        "created_at": e.created_at,
        "updated_at": e.updated_at,
        "validated_at": e.validated_at,
        "approved_at": e.approved_at,
        "applied_at": e.applied_at,
        "disabled_at": e.disabled_at,
        "merge_batch_id": e.merge_batch_id,
        "superseded_by": e.superseded_by,
    }


def _dict_to_entry(d: dict) -> OverrideEntry:
    ri = d["row_identity"]
    return OverrideEntry(
        override_id=d["override_id"],
        revision=d["revision"],
        row_identity=RowIdentity(
            filename=ri["filename"],
            artist=ri["artist"],
            title=ri["title"],
            row_number=ri["row_number"],
        ),
        state=OverrideState(d["state"]),
        override_bpm=d.get("override_bpm"),
        override_key=d.get("override_key"),
        override_scope=d.get("override_scope", ""),
        bpm_reason=d.get("bpm_reason", ""),
        key_reason=d.get("key_reason", ""),
        notes=d.get("notes", ""),
        entered_by=d.get("entered_by", ""),
        validation_message=d.get("validation_message", ""),
        conflict_flag=d.get("conflict_flag", False),
        original_bpm=d.get("original_bpm"),
        original_key=d.get("original_key"),
        created_at=d.get("created_at", ""),
        updated_at=d.get("updated_at", ""),
        validated_at=d.get("validated_at", ""),
        approved_at=d.get("approved_at", ""),
        applied_at=d.get("applied_at", ""),
        disabled_at=d.get("disabled_at", ""),
        merge_batch_id=d.get("merge_batch_id", ""),
        superseded_by=d.get("superseded_by", ""),
    )


def _event_to_dict(ev: AuditEvent) -> dict:
    return {
        "event_id": ev.event_id,
        "override_id": ev.override_id,
        "row_identity_key": ev.row_identity_key,
        "timestamp": ev.timestamp,
        "state_before": ev.state_before,
        "state_after": ev.state_after,
        "trigger": ev.trigger,
        "user": ev.user,
        "reason": ev.reason,
        "bpm_before": ev.bpm_before,
        "bpm_after": ev.bpm_after,
        "key_before": ev.key_before,
        "key_after": ev.key_after,
        "merge_batch_id": ev.merge_batch_id,
    }


def _batch_to_dict(b: MergeBatch) -> dict:
    return {
        "batch_id": b.batch_id,
        "timestamp": b.timestamp,
        "user": b.user,
        "base_export_path": b.base_export_path,
        "base_export_row_count": b.base_export_row_count,
        "overrides_submitted": b.overrides_submitted,
        "overrides_applied": b.overrides_applied,
        "overrides_skipped_invalid": b.overrides_skipped_invalid,
        "overrides_skipped_conflict": b.overrides_skipped_conflict,
        "merged_export_path": b.merged_export_path,
        "status": b.status,
        "error_message": b.error_message,
    }


class OverrideStore:
    """Persistence layer for the override system.

    Storage layout:
        {store_dir}/override_events.jsonl      — append-only event log (source of truth)
        {store_dir}/override_state.json        — current state snapshot (derived)
        {store_dir}/merge_history.jsonl         — merge batch records
        {store_dir}/adapter_log.jsonl           — operational log
    """

    def __init__(self, store_dir: Optional[str] = None):
        self.store_dir = store_dir or DEFAULT_STORE_DIR
        os.makedirs(self.store_dir, exist_ok=True)

        self._events_path = os.path.join(self.store_dir, EVENTS_FILE)
        self._snapshot_path = os.path.join(self.store_dir, SNAPSHOT_FILE)
        self._snapshot_bak = os.path.join(self.store_dir, SNAPSHOT_BAK)
        self._snapshot_tmp = os.path.join(self.store_dir, SNAPSHOT_TMP)
        self._merge_path = os.path.join(self.store_dir, MERGE_HISTORY_FILE)
        self._log_path = os.path.join(self.store_dir, ADAPTER_LOG_FILE)

        # In-memory state
        self._overrides: dict[str, OverrideEntry] = {}  # override_id -> entry
        self._last_event_id: str = ""

        self._recover_snapshot()
        self._load_snapshot()

    # ──────────────────────────────────────────────────────────
    #  CRASH RECOVERY
    # ──────────────────────────────────────────────────────────

    def _recover_snapshot(self) -> None:
        """Check for crash artifacts and recover if needed."""
        has_json = os.path.isfile(self._snapshot_path)
        has_tmp = os.path.isfile(self._snapshot_tmp)
        has_bak = os.path.isfile(self._snapshot_bak)

        if has_json and has_tmp:
            # Stale tmp — delete it
            os.remove(self._snapshot_tmp)
            self._log_operation("RECOVERY", "Deleted stale .tmp file")
        elif not has_json and has_tmp and has_bak:
            # Crash between bak rename and tmp rename
            os.rename(self._snapshot_tmp, self._snapshot_path)
            os.remove(self._snapshot_bak)
            self._log_operation("RECOVERY", "Recovered snapshot from .tmp")
        elif not has_json and not has_tmp and has_bak:
            os.rename(self._snapshot_bak, self._snapshot_path)
            self._log_operation("RECOVERY", "Recovered snapshot from .bak")
        elif not has_json and has_tmp and not has_bak:
            os.rename(self._snapshot_tmp, self._snapshot_path)
            self._log_operation("RECOVERY", "Promoted .tmp to snapshot")

    # ──────────────────────────────────────────────────────────
    #  SNAPSHOT LOAD / SAVE
    # ──────────────────────────────────────────────────────────

    def _load_snapshot(self) -> None:
        """Load current state from snapshot file."""
        if not os.path.isfile(self._snapshot_path):
            self._overrides = {}
            self._last_event_id = ""
            return

        with open(self._snapshot_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._last_event_id = data.get("last_event_id", "")
        overrides_raw = data.get("overrides", {})
        self._overrides = {}
        for oid, d in overrides_raw.items():
            self._overrides[oid] = _dict_to_entry(d)

        # Reconcile with event log if snapshot is behind
        self._replay_missing_events()

    def _replay_missing_events(self) -> None:
        """If event log has events newer than snapshot, replay them."""
        if not os.path.isfile(self._events_path):
            return

        found_last = self._last_event_id == ""
        events_to_replay: list[dict] = []

        with open(self._events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue  # skip corrupt lines
                if not found_last:
                    if ev.get("event_id") == self._last_event_id:
                        found_last = True
                    continue
                events_to_replay.append(ev)

        if events_to_replay:
            for ev in events_to_replay:
                oid = ev.get("override_id", "")
                if oid in self._overrides:
                    entry = self._overrides[oid]
                    new_state_str = ev.get("state_after", "")
                    if new_state_str:
                        try:
                            entry.state = OverrideState(new_state_str)
                            entry.updated_at = ev.get("timestamp", "")
                        except ValueError:
                            pass
                self._last_event_id = ev.get("event_id", self._last_event_id)

            self._write_snapshot()
            self._log_operation("RECOVERY", f"Replayed {len(events_to_replay)} events")

    def _write_snapshot(self) -> None:
        """Write current state to snapshot using atomic rename."""
        data = {
            "version": 1,
            "last_event_id": self._last_event_id,
            "last_updated": datetime.now().isoformat(),
            "overrides": {
                oid: _entry_to_dict(e) for oid, e in self._overrides.items()
            },
        }

        # Write to tmp
        with open(self._snapshot_tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())

        # Atomic rename sequence
        if os.path.isfile(self._snapshot_path):
            if os.path.isfile(self._snapshot_bak):
                os.remove(self._snapshot_bak)
            os.rename(self._snapshot_path, self._snapshot_bak)

        os.rename(self._snapshot_tmp, self._snapshot_path)

        # Clean up bak
        if os.path.isfile(self._snapshot_bak):
            os.remove(self._snapshot_bak)

    # ──────────────────────────────────────────────────────────
    #  EVENT LOG
    # ──────────────────────────────────────────────────────────

    def append_event(self, event: AuditEvent) -> None:
        """Append an audit event to the event log, then update snapshot."""
        self._last_event_id = event.event_id

        with open(self._events_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_event_to_dict(event), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

        self._write_snapshot()

    def load_events_for_row(self, identity_key: str) -> list[dict]:
        """Load all events matching a row identity key."""
        key = _norm_key(identity_key)
        events: list[dict] = []
        if not os.path.isfile(self._events_path):
            return events
        with open(self._events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if _norm_key(ev.get("row_identity_key", "")) == key:
                    events.append(ev)
        return events

    def load_all_events(self) -> list[dict]:
        """Load all events from the log."""
        events: list[dict] = []
        if not os.path.isfile(self._events_path):
            return events
        with open(self._events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                events.append(ev)
        return events

    # ──────────────────────────────────────────────────────────
    #  OVERRIDE CRUD
    # ──────────────────────────────────────────────────────────

    def put_override(self, entry: OverrideEntry) -> None:
        """Store/update an override entry in the in-memory state."""
        self._overrides[entry.override_id] = entry

    def get_override_by_id(self, override_id: str) -> Optional[OverrideEntry]:
        """Get override by ID."""
        return self._overrides.get(override_id)

    def get_active_override_for_row(self, identity_key: str) -> Optional[OverrideEntry]:
        """Get the active (non-DISABLED, non-SUPERSEDED) override for a row."""
        key = _norm_key(identity_key)
        terminal = {OverrideState.DISABLED, OverrideState.SUPERSEDED}
        for entry in self._overrides.values():
            if entry.row_identity.identity_key == key and entry.state not in terminal:
                return entry
        return None

    def list_overrides(self, filter_state: Optional[OverrideState] = None) -> list[OverrideEntry]:
        """List all overrides, optionally filtered by state."""
        entries = list(self._overrides.values())
        if filter_state is not None:
            entries = [e for e in entries if e.state == filter_state]
        return entries

    def get_all_overrides_for_row(self, identity_key: str) -> list[OverrideEntry]:
        """Get all overrides (including disabled/superseded) for a row."""
        key = _norm_key(identity_key)
        return [e for e in self._overrides.values()
                if e.row_identity.identity_key == key]

    # ──────────────────────────────────────────────────────────
    #  MERGE HISTORY
    # ──────────────────────────────────────────────────────────

    def append_merge_batch(self, batch: MergeBatch) -> None:
        """Append a merge batch record."""
        with open(self._merge_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(_batch_to_dict(batch), ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def load_merge_history(self) -> list[dict]:
        """Load all merge batch records."""
        batches: list[dict] = []
        if not os.path.isfile(self._merge_path):
            return batches
        with open(self._merge_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    batches.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return batches

    # ──────────────────────────────────────────────────────────
    #  OPERATIONAL LOG
    # ──────────────────────────────────────────────────────────

    def _log_operation(self, operation: str, detail: str) -> None:
        """Write an operational log entry."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "detail": detail,
        }
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def log(self, operation: str, detail: str) -> None:
        """Public log method for adapter to use."""
        self._log_operation(operation, detail)

    # ──────────────────────────────────────────────────────────
    #  RESET (for testing only)
    # ──────────────────────────────────────────────────────────

    def reset(self) -> None:
        """Clear all data. For testing only."""
        self._overrides.clear()
        self._last_event_id = ""
        for path in [self._events_path, self._snapshot_path,
                     self._snapshot_bak, self._snapshot_tmp,
                     self._merge_path, self._log_path]:
            if os.path.isfile(path):
                os.remove(path)
