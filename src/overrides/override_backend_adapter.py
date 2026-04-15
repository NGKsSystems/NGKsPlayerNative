"""
NGKsPlayerNative — Override Backend Adapter
App-facing service layer for the override management system.
All 12 API operations are implemented here.
"""

import csv
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from override_manager_state_model import (
    ALLOWED_TRANSITIONS,
    TRANSITION_TRIGGERS,
    VALIDATION_RESULT_TO_STATE,
    VALIDATION_RESULT_TO_TRIGGER,
    AuditEvent,
    MergeBatch,
    OverrideEntry,
    OverrideState,
    OverrideStateMachine,
    RowIdentity,
    TransitionTrigger,
)
from override_store import OverrideStore, _entry_to_dict
from override_validation import (
    VALID_CAMELOT,
    VALID_SCOPES,
    parse_key_to_camelot,
    validate_overrides,
)

WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
BASE_EXPORT_PATH = os.path.join(
    WORKSPACE, "_proof", "final_export_schema", "NGKs_final_analyzer_export.csv"
)


# ══════════════════════════════════════════════════════════════════
#  ERROR MODEL
# ══════════════════════════════════════════════════════════════════


@dataclass
class OverrideError:
    """Structured error returned by adapter operations."""

    code: str
    message: str
    details: Optional[dict] = None
    recoverable: bool = True

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "recoverable": self.recoverable,
        }


class OverrideOperationError(Exception):
    """Raised when an adapter operation fails."""

    def __init__(self, error: OverrideError):
        self.error = error
        super().__init__(error.message)


# ══════════════════════════════════════════════════════════════════
#  RESULT TYPES
# ══════════════════════════════════════════════════════════════════


def _make_validation_result(
    status: str,
    messages: list[str],
    conflict_flag: bool = False,
    bpm_valid: bool = True,
    key_valid: bool = True,
    scope_valid: bool = True,
    identity_valid: bool = True,
) -> dict:
    return {
        "status": status,
        "messages": messages,
        "conflict_flag": conflict_flag,
        "bpm_valid": bpm_valid,
        "key_valid": key_valid,
        "scope_valid": scope_valid,
        "identity_valid": identity_valid,
    }


# ══════════════════════════════════════════════════════════════════
#  BACKEND ADAPTER
# ══════════════════════════════════════════════════════════════════


class OverrideBackendAdapter:
    """App-facing service layer for override management.

    Sits between UI screens and override storage/validation/merge logic.
    Enforces state machine rules, emits audit events, and provides
    deterministic error handling.
    """

    def __init__(
        self,
        store: Optional[OverrideStore] = None,
        base_export_path: Optional[str] = None,
    ):
        self.store = store or OverrideStore()
        self.base_export_path = base_export_path or BASE_EXPORT_PATH

        # Lazy-loaded base data
        self._base_rows: Optional[list[dict]] = None
        self._base_lookup_by_row: Optional[dict[str, dict]] = None
        self._base_lookup_by_filename: Optional[dict[str, dict]] = None

    # ──────────────────────────────────────────────────────────
    #  BASE EXPORT LOADING
    # ──────────────────────────────────────────────────────────

    def _ensure_base_loaded(self) -> None:
        """Load base export if not already cached."""
        if self._base_rows is not None:
            return
        if not os.path.isfile(self.base_export_path):
            raise OverrideOperationError(
                OverrideError(
                    code="BASE_EXPORT_MISSING",
                    message=f"Base export not found: {self.base_export_path}",
                    recoverable=False,
                )
            )
        with open(self.base_export_path, "r", encoding="utf-8-sig") as f:
            self._base_rows = list(csv.DictReader(f))
        self._base_lookup_by_row = {
            r.get("Row", ""): r for r in self._base_rows
        }
        self._base_lookup_by_filename = {
            (r.get("Filename", "")).strip().lower(): r for r in self._base_rows
        }

    def _find_base_row(self, identity: RowIdentity) -> dict:
        """Find a base row by filename. Raises ROW_NOT_FOUND if missing."""
        self._ensure_base_loaded()
        assert self._base_lookup_by_filename is not None
        key = identity.filename.strip().lower()
        row = self._base_lookup_by_filename.get(key)
        if row is None:
            raise OverrideOperationError(
                OverrideError(
                    code="ROW_NOT_FOUND",
                    message=f"Row not found in base export: '{identity.filename}'",
                    details={"filename": identity.filename},
                )
            )
        return row

    # ──────────────────────────────────────────────────────────
    #  HELPERS
    # ──────────────────────────────────────────────────────────

    def _new_event_id(self) -> str:
        return f"evt-{uuid.uuid4().hex[:12]}"

    def _new_override_id(self) -> str:
        return f"ovr-{uuid.uuid4().hex[:12]}"

    def _new_batch_id(self) -> str:
        return f"MB-{uuid.uuid4().hex[:8]}"

    def _emit_event(self, event: AuditEvent) -> AuditEvent:
        """Assign event ID if missing, persist, and log."""
        if not event.event_id:
            event.event_id = self._new_event_id()
        self.store.append_event(event)
        self.store.log(
            "EVENT",
            f"{event.state_before}->{event.state_after} "
            f"[{event.trigger}] override={event.override_id}",
        )
        return event

    def _validate_payload(self, payload: dict) -> list[str]:
        """Check basic payload validity. Returns list of error messages."""
        errors: list[str] = []
        scope = (payload.get("override_scope") or "").upper()
        if scope not in VALID_SCOPES:
            errors.append(
                f"Invalid scope: '{scope}' (expected BPM/KEY/BPM_AND_KEY)"
            )

        bpm = payload.get("override_bpm")
        if bpm is not None:
            try:
                bpm_f = float(bpm)
                if bpm_f <= 20 or bpm_f >= 300:
                    errors.append(f"BPM out of range: {bpm_f} (must be 20-300)")
            except (ValueError, TypeError):
                errors.append(f"BPM not numeric: '{bpm}'")

        key = payload.get("override_key")
        if key is not None and key != "":
            cam, _ = parse_key_to_camelot(str(key))
            if cam is None:
                errors.append(f"Key unrecognized: '{key}'")

        # Scope vs field consistency
        if not errors:
            if scope == "BPM" and bpm is None:
                errors.append("Scope=BPM but override_bpm is not provided")
            if scope == "KEY" and (key is None or key == ""):
                errors.append("Scope=KEY but override_key is not provided")
            if scope == "BPM_AND_KEY" and bpm is None and (key is None or key == ""):
                errors.append(
                    "Scope=BPM_AND_KEY but neither override_bpm nor override_key provided"
                )

        return errors

    # ══════════════════════════════════════════════════════════════
    #  API OPERATIONS
    # ══════════════════════════════════════════════════════════════

    # ── 1. load_review_queue ──

    def load_review_queue(self) -> list[dict]:
        """Load all base export rows where ReviewRequired == True.

        Returns list of dicts with base row fields plus override status.
        """
        self._ensure_base_loaded()
        assert self._base_rows is not None

        queue: list[dict] = []
        for row in self._base_rows:
            if str(row.get("ReviewRequired", "")).upper() == "TRUE":
                filename = (row.get("Filename") or "").strip()
                active_ov = self.store.get_active_override_for_row(filename)
                entry = {
                    "row_number": row.get("Row", ""),
                    "artist": row.get("Artist", ""),
                    "title": row.get("Title", ""),
                    "filename": filename,
                    "final_bpm": row.get("FinalBPM", ""),
                    "final_key": row.get("FinalKey", ""),
                    "bpm_trust": row.get("FinalBPMTrustLevel", ""),
                    "key_trust": row.get("FinalKeyTrustLevel", ""),
                    "confidence_tier": row.get("ConfidenceTier", ""),
                    "review_reason": row.get("ReviewReason", ""),
                    "override_status": active_ov.state.value if active_ov else None,
                }
                queue.append(entry)

        self.store.log("QUERY", f"load_review_queue: {len(queue)} rows")
        return queue

    # ── 2. list_overrides ──

    def list_overrides(self, filter_state: Optional[str] = None) -> list[dict]:
        """List all override entries, optionally filtered by state."""
        state_filter: Optional[OverrideState] = None
        if filter_state is not None:
            try:
                state_filter = OverrideState(filter_state)
            except ValueError:
                raise OverrideOperationError(
                    OverrideError(
                        code="INVALID_PAYLOAD",
                        message=f"Invalid filter state: '{filter_state}'",
                        details={"valid_states": [s.value for s in OverrideState]},
                    )
                )

        entries = self.store.list_overrides(state_filter)
        result = [_entry_to_dict(e) for e in entries]
        self.store.log(
            "QUERY",
            f"list_overrides(filter={filter_state}): {len(result)} entries",
        )
        return result

    # ── 3. get_override ──

    def get_override(self, row_identity: dict) -> Optional[dict]:
        """Get the active override for a row by filename."""
        filename = (row_identity.get("filename") or "").strip()
        if not filename:
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_PAYLOAD",
                    message="row_identity.filename is required",
                )
            )

        entry = self.store.get_active_override_for_row(filename)
        if entry is None:
            return None

        self.store.log("QUERY", f"get_override: {filename}")
        return _entry_to_dict(entry)

    # ── 4. create_override_draft ──

    def create_override_draft(
        self, row_identity: dict, payload: dict
    ) -> dict:
        """Create a new override entry in DRAFT state."""
        filename = (row_identity.get("filename") or "").strip()
        artist = (row_identity.get("artist") or "").strip()
        title = (row_identity.get("title") or "").strip()

        if not filename:
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_PAYLOAD",
                    message="row_identity.filename is required",
                )
            )

        # Verify row exists in base export
        base_row = self._find_base_row(
            RowIdentity(
                filename=filename,
                artist=artist,
                title=title,
                row_number=int(row_identity.get("row_number", 0)),
            )
        )

        # Check no active override exists
        existing = self.store.get_active_override_for_row(filename)
        if existing is not None:
            raise OverrideOperationError(
                OverrideError(
                    code="DUPLICATE_ACTIVE_DRAFT",
                    message=f"Active override already exists for '{filename}'",
                    details={
                        "existing_override_id": existing.override_id,
                        "existing_state": existing.state.value,
                    },
                )
            )

        # Validate payload
        payload_errors = self._validate_payload(payload)
        if payload_errors:
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_PAYLOAD",
                    message="Invalid override payload",
                    details={"errors": payload_errors},
                )
            )

        # Determine revision (check for superseded/disabled entries)
        all_for_row = self.store.get_all_overrides_for_row(filename)
        revision = max((e.revision for e in all_for_row), default=0) + 1

        now = datetime.now().isoformat()
        row_num = int(base_row.get("Row", 0))

        identity = RowIdentity(
            filename=filename,
            artist=artist or base_row.get("Artist", ""),
            title=title or base_row.get("Title", ""),
            row_number=row_num,
        )

        # Parse key if provided
        override_key = payload.get("override_key")
        if override_key:
            cam, _ = parse_key_to_camelot(str(override_key))
            if cam:
                override_key = cam

        bpm_val = payload.get("override_bpm")
        if bpm_val is not None:
            bpm_val = round(float(bpm_val), 1)

        entry = OverrideEntry(
            override_id=self._new_override_id(),
            revision=revision,
            row_identity=identity,
            state=OverrideState.NEW,
            override_bpm=bpm_val,
            override_key=override_key,
            override_scope=(payload.get("override_scope") or "").upper(),
            bpm_reason=payload.get("bpm_reason", ""),
            key_reason=payload.get("key_reason", ""),
            notes=payload.get("notes", ""),
            entered_by=payload.get("entered_by", ""),
            original_bpm=float(base_row.get("FinalBPM", 0)),
            original_key=base_row.get("FinalKey", ""),
            created_at=now,
            updated_at=now,
        )

        # Transition NEW -> DRAFT
        self.store.put_override(entry)

        # Emit creation event
        create_event = AuditEvent(
            event_id=self._new_event_id(),
            override_id=entry.override_id,
            row_identity_key=identity.identity_key,
            timestamp=now,
            state_before="",
            state_after=OverrideState.NEW.value,
            trigger="SYSTEM",
            user=payload.get("entered_by", "system"),
            reason="Override created",
        )
        self._emit_event(create_event)

        # Now transition to DRAFT
        edit_event = OverrideStateMachine.transition(
            entry,
            OverrideState.DRAFT,
            TransitionTrigger.USER_EDIT,
            payload.get("entered_by", "system"),
            reason="Initial draft",
        )
        self._emit_event(edit_event)
        self.store.put_override(entry)

        self.store.log(
            "CREATE",
            f"Created override {entry.override_id} for '{filename}' rev={revision}",
        )
        return _entry_to_dict(entry)

    # ── 5. update_override_draft ──

    def update_override_draft(self, override_id: str, payload: dict) -> dict:
        """Update an existing DRAFT override."""
        entry = self.store.get_override_by_id(override_id)
        if entry is None:
            raise OverrideOperationError(
                OverrideError(
                    code="OVERRIDE_NOT_FOUND",
                    message=f"Override not found: '{override_id}'",
                    recoverable=False,
                )
            )

        if entry.state != OverrideState.DRAFT:
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_TRANSITION",
                    message=f"Cannot update: override is in {entry.state.value}, not DRAFT",
                    details={"current_state": entry.state.value},
                    recoverable=False,
                )
            )

        # Validate payload
        payload_errors = self._validate_payload(payload)
        if payload_errors:
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_PAYLOAD",
                    message="Invalid override payload",
                    details={"errors": payload_errors},
                )
            )

        # Update fields
        if "override_bpm" in payload and payload["override_bpm"] is not None:
            entry.override_bpm = round(float(payload["override_bpm"]), 1)
        if "override_key" in payload and payload["override_key"] is not None:
            cam, _ = parse_key_to_camelot(str(payload["override_key"]))
            entry.override_key = cam if cam else payload["override_key"]
        if "override_scope" in payload:
            entry.override_scope = (payload["override_scope"] or "").upper()
        if "bpm_reason" in payload:
            entry.bpm_reason = payload["bpm_reason"]
        if "key_reason" in payload:
            entry.key_reason = payload["key_reason"]
        if "notes" in payload:
            entry.notes = payload["notes"]
        if "entered_by" in payload:
            entry.entered_by = payload["entered_by"]

        # DRAFT -> DRAFT transition
        event = OverrideStateMachine.transition(
            entry,
            OverrideState.DRAFT,
            TransitionTrigger.USER_EDIT,
            payload.get("entered_by", entry.entered_by),
            reason="Draft updated",
        )
        self._emit_event(event)
        self.store.put_override(entry)

        self.store.log("UPDATE", f"Updated draft {override_id}")
        return _entry_to_dict(entry)

    # ── 6. validate_override ──

    def validate_override(self, override_id: str) -> dict:
        """Run validation on a DRAFT override."""
        entry = self.store.get_override_by_id(override_id)
        if entry is None:
            raise OverrideOperationError(
                OverrideError(
                    code="OVERRIDE_NOT_FOUND",
                    message=f"Override not found: '{override_id}'",
                    recoverable=False,
                )
            )

        if entry.state != OverrideState.DRAFT:
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_TRANSITION",
                    message=f"Cannot validate: override is in {entry.state.value}, not DRAFT",
                    details={"current_state": entry.state.value},
                    recoverable=False,
                )
            )

        # Transition DRAFT -> PENDING_VALIDATION
        submit_event = OverrideStateMachine.transition(
            entry,
            OverrideState.PENDING_VALIDATION,
            TransitionTrigger.USER_SUBMIT,
            entry.entered_by,
            reason="Submitted for validation",
        )
        self._emit_event(submit_event)
        self.store.put_override(entry)

        # Run validation using existing engine
        self._ensure_base_loaded()
        assert self._base_lookup_by_row is not None

        # Build a validation row compatible with existing validate_overrides()
        override_row = {
            "Row": str(entry.row_identity.row_number),
            "Artist": entry.row_identity.artist,
            "Title": entry.row_identity.title,
            "Filename": entry.row_identity.filename,
            "OverrideFinalBPM": str(entry.override_bpm) if entry.override_bpm else "",
            "OverrideFinalKey": entry.override_key or "",
            "OverrideScope": entry.override_scope,
            "OverrideEnabled": "TRUE",
        }

        results = validate_overrides([override_row], self._base_lookup_by_row)
        vr = results[0] if results else {"OverrideStatus": "INVALID", "OverrideValidationMessage": "No result"}

        val_status = vr.get("OverrideStatus", "INVALID")
        val_message = vr.get("OverrideValidationMessage", "")
        val_conflict = vr.get("OverrideConflictFlag", False)

        entry.validation_message = val_message
        entry.conflict_flag = val_conflict

        # Determine target state
        target_state = VALIDATION_RESULT_TO_STATE.get(val_status, OverrideState.INVALID)
        trigger = VALIDATION_RESULT_TO_TRIGGER.get(val_status, TransitionTrigger.VALIDATION_FAIL)

        val_event = OverrideStateMachine.transition(
            entry, target_state, trigger, "system", reason=val_message
        )
        self._emit_event(val_event)
        self.store.put_override(entry)

        bpm_valid = "BPM" not in val_message.upper() or val_status == "VALID"
        key_valid = "Key" not in val_message or val_status == "VALID"
        scope_valid = "Scope" not in val_message or val_status == "VALID"
        identity_valid = not val_conflict

        result = _make_validation_result(
            status=val_status,
            messages=val_message.split("; ") if val_message else ["OK"],
            conflict_flag=val_conflict,
            bpm_valid=bpm_valid,
            key_valid=key_valid,
            scope_valid=scope_valid,
            identity_valid=identity_valid,
        )

        self.store.log(
            "VALIDATE",
            f"Validated {override_id}: {val_status} — {val_message}",
        )
        return result

    # ── 7. approve_override ──

    def approve_override(self, override_id: str, approver: str) -> dict:
        """Approve a VALID override for merge."""
        entry = self.store.get_override_by_id(override_id)
        if entry is None:
            raise OverrideOperationError(
                OverrideError(
                    code="OVERRIDE_NOT_FOUND",
                    message=f"Override not found: '{override_id}'",
                    recoverable=False,
                )
            )

        if entry.state != OverrideState.VALID:
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_TRANSITION",
                    message=f"Cannot approve: override is in {entry.state.value}, not VALID",
                    details={
                        "current_state": entry.state.value,
                        "allowed_from": "VALID",
                    },
                    recoverable=False,
                )
            )

        event = OverrideStateMachine.transition(
            entry,
            OverrideState.APPROVED,
            TransitionTrigger.USER_APPROVE,
            approver,
            reason=f"Approved by {approver}",
        )
        self._emit_event(event)
        self.store.put_override(entry)

        self.store.log("APPROVE", f"Approved {override_id} by {approver}")
        return _entry_to_dict(entry)

    # ── 8. apply_approved_overrides ──

    def apply_approved_overrides(
        self, batch_label: str, user: str
    ) -> dict:
        """Merge all APPROVED overrides into a new export."""
        self._ensure_base_loaded()
        assert self._base_rows is not None
        assert self._base_lookup_by_row is not None

        approved = self.store.list_overrides(OverrideState.APPROVED)
        if not approved:
            raise OverrideOperationError(
                OverrideError(
                    code="MERGE_FAILED",
                    message="No APPROVED overrides to apply",
                    details={"approved_count": 0},
                )
            )

        # Re-validate all approved overrides
        validation_rows = []
        for entry in approved:
            validation_rows.append({
                "Row": str(entry.row_identity.row_number),
                "Artist": entry.row_identity.artist,
                "Title": entry.row_identity.title,
                "Filename": entry.row_identity.filename,
                "OverrideFinalBPM": str(entry.override_bpm) if entry.override_bpm else "",
                "OverrideFinalKey": entry.override_key or "",
                "OverrideScope": entry.override_scope,
                "OverrideEnabled": "TRUE",
            })

        validated = validate_overrides(validation_rows, self._base_lookup_by_row)

        # Import merge logic
        from override_merge_flow import merge_overrides

        merged_rows, applied_log, conflicts = merge_overrides(
            self._base_rows, validated
        )

        # Write merged export atomically
        merged_dir = os.path.join(WORKSPACE, "_artifacts", "exports")
        os.makedirs(merged_dir, exist_ok=True)
        merged_path = os.path.join(
            merged_dir, "NGKs_final_analyzer_export_OVERRIDDEN.csv"
        )
        tmp_path = merged_path + ".tmp"

        try:
            if merged_rows:
                fieldnames = list(merged_rows[0].keys())
                with open(tmp_path, "w", encoding="utf-8", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=fieldnames)
                    w.writeheader()
                    w.writerows(merged_rows)
                    f.flush()
                    os.fsync(f.fileno())

                # Atomic rename
                if os.path.isfile(merged_path):
                    os.remove(merged_path)
                os.rename(tmp_path, merged_path)
        except Exception as exc:
            # Clean up tmp on failure
            if os.path.isfile(tmp_path):
                os.remove(tmp_path)
            raise OverrideOperationError(
                OverrideError(
                    code="MERGE_FAILED",
                    message=f"Merge write failed: {exc}",
                    details={"exception": str(exc)},
                )
            ) from exc

        # Transition each applied override to APPLIED
        now = datetime.now().isoformat()
        batch_id = self._new_batch_id()
        applied_count = 0
        skipped_invalid = 0
        skipped_conflict = 0

        for i, entry in enumerate(approved):
            vr = validated[i] if i < len(validated) else {}
            vr_status = vr.get("OverrideStatus", "")

            if vr_status == "VALID":
                event = OverrideStateMachine.transition(
                    entry,
                    OverrideState.APPLIED,
                    TransitionTrigger.MERGE_SUCCESS,
                    "system",
                    reason=f"Applied in batch {batch_id}",
                )
                event.merge_batch_id = batch_id
                self._emit_event(event)
                entry.merge_batch_id = batch_id
                self.store.put_override(entry)
                applied_count += 1
            elif vr_status == "INVALID":
                skipped_invalid += 1
            elif vr_status == "CONFLICT":
                skipped_conflict += 1

        # Record merge batch
        batch = MergeBatch(
            batch_id=batch_id,
            timestamp=now,
            user=user,
            base_export_path=self.base_export_path,
            base_export_row_count=len(self._base_rows),
            overrides_submitted=len(approved),
            overrides_applied=applied_count,
            overrides_skipped_invalid=skipped_invalid,
            overrides_skipped_conflict=skipped_conflict,
            merged_export_path=merged_path,
            status="SUCCESS" if applied_count > 0 else "FAILED",
        )
        self.store.append_merge_batch(batch)

        result = {
            "batch_id": batch_id,
            "timestamp": now,
            "submitted_count": len(approved),
            "applied_count": applied_count,
            "skipped_invalid": skipped_invalid,
            "skipped_conflict": skipped_conflict,
            "merged_export_path": merged_path,
            "status": batch.status,
        }

        self.store.log(
            "MERGE",
            f"Batch {batch_id}: {applied_count} applied, "
            f"{skipped_invalid} invalid, {skipped_conflict} conflict",
        )
        return result

    # ── 9. disable_override ──

    def disable_override(self, override_id: str, reason: str) -> dict:
        """Disable an override (soft delete)."""
        entry = self.store.get_override_by_id(override_id)
        if entry is None:
            raise OverrideOperationError(
                OverrideError(
                    code="OVERRIDE_NOT_FOUND",
                    message=f"Override not found: '{override_id}'",
                    recoverable=False,
                )
            )

        if not OverrideStateMachine.can_transition(
            entry.state, OverrideState.DISABLED
        ):
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_TRANSITION",
                    message=f"Cannot disable from state {entry.state.value}",
                    details={
                        "current_state": entry.state.value,
                        "allowed_targets": [
                            s.value
                            for s in ALLOWED_TRANSITIONS.get(entry.state, set())
                        ],
                    },
                    recoverable=False,
                )
            )

        event = OverrideStateMachine.transition(
            entry,
            OverrideState.DISABLED,
            TransitionTrigger.USER_DISABLE,
            entry.entered_by,
            reason=reason,
        )
        self._emit_event(event)
        self.store.put_override(entry)

        self.store.log("DISABLE", f"Disabled {override_id}: {reason}")
        return _entry_to_dict(entry)

    # ── 10. get_override_history ──

    def get_override_history(self, row_identity: dict) -> list[dict]:
        """Get all audit events for a given row, sorted by timestamp."""
        filename = (row_identity.get("filename") or "").strip()
        if not filename:
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_PAYLOAD",
                    message="row_identity.filename is required",
                )
            )

        events = self.store.load_events_for_row(filename)
        events.sort(key=lambda e: e.get("timestamp", ""))

        self.store.log("QUERY", f"get_override_history: {filename}, {len(events)} events")
        return events

    # ── 11. get_merge_history ──

    def get_merge_history(self) -> list[dict]:
        """Get all merge batch records."""
        batches = self.store.load_merge_history()
        self.store.log("QUERY", f"get_merge_history: {len(batches)} batches")
        return batches

    # ── 12. get_effective_row ──

    def get_effective_row(self, row_identity: dict) -> dict:
        """Return effective values for a row (base vs overridden).

        If an active APPLIED override exists, effective values reflect it.
        Otherwise, effective values are the base export values.
        """
        filename = (row_identity.get("filename") or "").strip()
        if not filename:
            raise OverrideOperationError(
                OverrideError(
                    code="INVALID_PAYLOAD",
                    message="row_identity.filename is required",
                )
            )

        identity = RowIdentity(
            filename=filename,
            artist=row_identity.get("artist", ""),
            title=row_identity.get("title", ""),
            row_number=int(row_identity.get("row_number", 0)),
        )
        base_row = self._find_base_row(identity)

        base_bpm_raw = base_row.get("FinalBPM", "0")
        try:
            base_bpm = float(base_bpm_raw)
        except (ValueError, TypeError):
            base_bpm = 0.0

        base_values = {
            "bpm": base_bpm,
            "key": base_row.get("FinalKey", ""),
            "key_name": base_row.get("FinalKeyName", ""),
            "bpm_trust": base_row.get("FinalBPMTrustLevel", ""),
            "key_trust": base_row.get("FinalKeyTrustLevel", ""),
            "confidence_tier": base_row.get("ConfidenceTier", ""),
            "review_required": str(base_row.get("ReviewRequired", "")).upper() == "TRUE",
        }

        # Check for active override (APPLIED takes priority, then any active)
        active = self.store.get_active_override_for_row(filename)
        override_values: Optional[dict] = None
        effective_bpm = base_bpm
        effective_key = base_row.get("FinalKey", "")
        bpm_source = "BASE"
        key_source = "BASE"

        if active is not None:
            override_values = {
                "bpm": active.override_bpm,
                "key": active.override_key,
                "scope": active.override_scope,
                "state": active.state.value,
            }

            # Only APPLIED overrides change effective values
            if active.state == OverrideState.APPLIED:
                scope = active.override_scope.upper()
                if scope in ("BPM", "BPM_AND_KEY") and active.override_bpm is not None:
                    effective_bpm = active.override_bpm
                    bpm_source = "OVERRIDE"
                if scope in ("KEY", "BPM_AND_KEY") and active.override_key:
                    effective_key = active.override_key
                    key_source = "OVERRIDE"

        ri_dict = {
            "filename": identity.filename,
            "artist": base_row.get("Artist", ""),
            "title": base_row.get("Title", ""),
            "row_number": int(base_row.get("Row", 0)),
        }

        result = {
            "row_identity": ri_dict,
            "base_values": base_values,
            "override_values": override_values,
            "effective_values": {
                "bpm": effective_bpm,
                "key": effective_key,
                "bpm_source": bpm_source,
                "key_source": key_source,
            },
        }

        self.store.log("QUERY", f"get_effective_row: {filename}")
        return result
