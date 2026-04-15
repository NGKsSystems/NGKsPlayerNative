"""
NGKsPlayerNative — Override Manager State Model
Formal state machine for override lifecycle.

Each override entry progresses through well-defined states.
Only explicitly declared transitions are legal.
All transitions are recorded in the audit log.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


# ══════════════════════════════════════════════════════════════════════
#  OVERRIDE STATES
# ══════════════════════════════════════════════════════════════════════

class OverrideState(str, Enum):
    """All legal states an override entry can occupy."""
    NEW = "NEW"                           # Row flagged for review; no override values entered yet
    DRAFT = "DRAFT"                       # User has entered override values but not submitted for validation
    PENDING_VALIDATION = "PENDING_VALIDATION"  # Submitted for validation; waiting for result
    VALID = "VALID"                       # Passed all validation checks
    INVALID = "INVALID"                   # Failed one or more validation checks
    CONFLICT = "CONFLICT"                 # Identity mismatch or duplicate detected
    APPROVED = "APPROVED"                 # Human reviewed validation result and approved for merge
    APPLIED = "APPLIED"                   # Override was merged into export successfully
    DISABLED = "DISABLED"                 # Explicitly turned off by user (soft delete)
    SUPERSEDED = "SUPERSEDED"             # A newer override replaced this one


# ══════════════════════════════════════════════════════════════════════
#  TRANSITION TABLE
# ══════════════════════════════════════════════════════════════════════

# Maps (from_state) -> set of allowed (to_state) values.
# Any transition NOT listed here is FORBIDDEN.

ALLOWED_TRANSITIONS: dict[OverrideState, set[OverrideState]] = {
    OverrideState.NEW: {
        OverrideState.DRAFT,              # User begins editing
        OverrideState.DISABLED,           # User dismisses without editing
    },
    OverrideState.DRAFT: {
        OverrideState.PENDING_VALIDATION, # User submits for validation
        OverrideState.DRAFT,              # User continues editing (re-save)
        OverrideState.DISABLED,           # User abandons draft
    },
    OverrideState.PENDING_VALIDATION: {
        OverrideState.VALID,              # Validation passed
        OverrideState.INVALID,            # Validation failed
        OverrideState.CONFLICT,           # Identity conflict detected
    },
    OverrideState.VALID: {
        OverrideState.APPROVED,           # User approves for merge
        OverrideState.DRAFT,              # User wants to re-edit
        OverrideState.DISABLED,           # User disables
    },
    OverrideState.INVALID: {
        OverrideState.DRAFT,              # User goes back to fix values
        OverrideState.DISABLED,           # User gives up
    },
    OverrideState.CONFLICT: {
        OverrideState.DRAFT,              # User goes back to fix identity
        OverrideState.DISABLED,           # User gives up
    },
    OverrideState.APPROVED: {
        OverrideState.APPLIED,            # Merge executed successfully
        OverrideState.DRAFT,              # User revokes approval to re-edit
        OverrideState.DISABLED,           # User revokes approval and disables
    },
    OverrideState.APPLIED: {
        OverrideState.SUPERSEDED,         # New override revision created for same row
        OverrideState.DISABLED,           # User explicitly disables applied override
    },
    OverrideState.DISABLED: {
        OverrideState.DRAFT,              # User re-enables and edits
    },
    OverrideState.SUPERSEDED: set(),      # Terminal state — no transitions out
}


# ══════════════════════════════════════════════════════════════════════
#  TRANSITION TRIGGERS
# ══════════════════════════════════════════════════════════════════════

class TransitionTrigger(str, Enum):
    """What caused the state transition."""
    USER_EDIT = "USER_EDIT"               # User entered or modified override values
    USER_SUBMIT = "USER_SUBMIT"           # User clicked "Validate"
    VALIDATION_PASS = "VALIDATION_PASS"   # Validation engine returned VALID
    VALIDATION_FAIL = "VALIDATION_FAIL"   # Validation engine returned INVALID
    VALIDATION_CONFLICT = "VALIDATION_CONFLICT"  # Validation engine returned CONFLICT
    USER_APPROVE = "USER_APPROVE"         # User clicked "Approve"
    USER_REVOKE = "USER_REVOKE"           # User revoked approval
    MERGE_SUCCESS = "MERGE_SUCCESS"       # Merge completed successfully
    USER_DISABLE = "USER_DISABLE"         # User explicitly disabled override
    USER_REENABLE = "USER_REENABLE"       # User re-enabled a disabled override
    NEW_REVISION = "NEW_REVISION"         # New override created for same row → old one superseded
    SYSTEM_RECOVERY = "SYSTEM_RECOVERY"   # Crash recovery restored state


# Map of (from_state, to_state) -> expected trigger
TRANSITION_TRIGGERS: dict[tuple[OverrideState, OverrideState], TransitionTrigger] = {
    (OverrideState.NEW, OverrideState.DRAFT): TransitionTrigger.USER_EDIT,
    (OverrideState.NEW, OverrideState.DISABLED): TransitionTrigger.USER_DISABLE,
    (OverrideState.DRAFT, OverrideState.PENDING_VALIDATION): TransitionTrigger.USER_SUBMIT,
    (OverrideState.DRAFT, OverrideState.DRAFT): TransitionTrigger.USER_EDIT,
    (OverrideState.DRAFT, OverrideState.DISABLED): TransitionTrigger.USER_DISABLE,
    (OverrideState.PENDING_VALIDATION, OverrideState.VALID): TransitionTrigger.VALIDATION_PASS,
    (OverrideState.PENDING_VALIDATION, OverrideState.INVALID): TransitionTrigger.VALIDATION_FAIL,
    (OverrideState.PENDING_VALIDATION, OverrideState.CONFLICT): TransitionTrigger.VALIDATION_CONFLICT,
    (OverrideState.VALID, OverrideState.APPROVED): TransitionTrigger.USER_APPROVE,
    (OverrideState.VALID, OverrideState.DRAFT): TransitionTrigger.USER_EDIT,
    (OverrideState.VALID, OverrideState.DISABLED): TransitionTrigger.USER_DISABLE,
    (OverrideState.INVALID, OverrideState.DRAFT): TransitionTrigger.USER_EDIT,
    (OverrideState.INVALID, OverrideState.DISABLED): TransitionTrigger.USER_DISABLE,
    (OverrideState.CONFLICT, OverrideState.DRAFT): TransitionTrigger.USER_EDIT,
    (OverrideState.CONFLICT, OverrideState.DISABLED): TransitionTrigger.USER_DISABLE,
    (OverrideState.APPROVED, OverrideState.APPLIED): TransitionTrigger.MERGE_SUCCESS,
    (OverrideState.APPROVED, OverrideState.DRAFT): TransitionTrigger.USER_REVOKE,
    (OverrideState.APPROVED, OverrideState.DISABLED): TransitionTrigger.USER_DISABLE,
    (OverrideState.APPLIED, OverrideState.SUPERSEDED): TransitionTrigger.NEW_REVISION,
    (OverrideState.APPLIED, OverrideState.DISABLED): TransitionTrigger.USER_DISABLE,
    (OverrideState.DISABLED, OverrideState.DRAFT): TransitionTrigger.USER_REENABLE,
}


# ══════════════════════════════════════════════════════════════════════
#  ROW IDENTITY
# ══════════════════════════════════════════════════════════════════════

@dataclass
class RowIdentity:
    """Canonical identity for a library row.
    
    Primary key: Filename (most stable across re-analysis).
    Secondary verification: Artist + Title (case-insensitive).
    Row number is stored but NOT authoritative — it may change on re-analysis.
    """
    filename: str                          # PRIMARY — stable file path
    artist: str                            # SECONDARY — verification
    title: str                             # SECONDARY — verification
    row_number: int                        # DISPLAY — not identity-authoritative

    def matches(self, other: "RowIdentity") -> bool:
        """Two identities match if filenames are identical (case-insensitive)."""
        return self.filename.strip().lower() == other.filename.strip().lower()

    def verify(self, other: "RowIdentity") -> tuple[bool, str]:
        """Full verification: filename match + artist/title cross-check.
        Returns (ok, message).
        """
        if not self.matches(other):
            return False, f"Filename mismatch: '{self.filename}' vs '{other.filename}'"
        warnings = []
        if self.artist.strip().lower() != other.artist.strip().lower():
            warnings.append(f"Artist mismatch: '{self.artist}' vs '{other.artist}'")
        if self.title.strip().lower() != other.title.strip().lower():
            warnings.append(f"Title mismatch: '{self.title}' vs '{other.title}'")
        if warnings:
            return False, "; ".join(warnings)
        return True, "OK"

    @property
    def identity_key(self) -> str:
        """Stable lookup key."""
        return self.filename.strip().lower()


# ══════════════════════════════════════════════════════════════════════
#  OVERRIDE ENTRY
# ══════════════════════════════════════════════════════════════════════

@dataclass
class OverrideEntry:
    """A single override record with full lifecycle tracking."""

    # Identity
    override_id: str                       # UUID, assigned on creation
    revision: int                          # 1 = first override, 2+ = subsequent revisions
    row_identity: RowIdentity

    # Override values
    override_bpm: Optional[float] = None
    override_key: Optional[str] = None     # Camelot code (e.g. "10B")
    override_scope: str = ""               # BPM / KEY / BPM_AND_KEY
    bpm_reason: str = ""
    key_reason: str = ""
    notes: str = ""
    entered_by: str = ""

    # State
    state: OverrideState = OverrideState.NEW
    validation_message: str = ""
    conflict_flag: bool = False

    # Original values (captured when override is created)
    original_bpm: Optional[float] = None
    original_key: Optional[str] = None

    # Timestamps
    created_at: str = ""                   # ISO timestamp
    updated_at: str = ""                   # ISO timestamp
    validated_at: str = ""                 # ISO timestamp
    approved_at: str = ""                  # ISO timestamp
    applied_at: str = ""                   # ISO timestamp
    disabled_at: str = ""                  # ISO timestamp

    # Merge tracking
    merge_batch_id: str = ""               # Links to a specific merge operation
    superseded_by: str = ""                # override_id of the replacement


# ══════════════════════════════════════════════════════════════════════
#  AUDIT EVENT
# ══════════════════════════════════════════════════════════════════════

@dataclass
class AuditEvent:
    """Immutable record of a state transition."""
    event_id: str                          # UUID
    override_id: str                       # Which override
    row_identity_key: str                  # Stable filename key
    timestamp: str                         # ISO timestamp
    state_before: str                      # OverrideState value
    state_after: str                       # OverrideState value
    trigger: str                           # TransitionTrigger value
    user: str                              # Who triggered it
    reason: str = ""                       # Free-text context

    # Snapshot of values at time of event
    bpm_before: Optional[float] = None
    bpm_after: Optional[float] = None
    key_before: Optional[str] = None
    key_after: Optional[str] = None

    # Merge context (only for APPLIED transitions)
    merge_batch_id: str = ""
    source_file: str = ""                  # Path to export snapshot


# ══════════════════════════════════════════════════════════════════════
#  MERGE BATCH
# ══════════════════════════════════════════════════════════════════════

@dataclass
class MergeBatch:
    """Record of a single merge operation."""
    batch_id: str                          # UUID
    timestamp: str                         # ISO timestamp
    user: str
    base_export_path: str                  # Which base export was used
    base_export_row_count: int
    overrides_submitted: int
    overrides_applied: int
    overrides_skipped_invalid: int
    overrides_skipped_conflict: int
    merged_export_path: str                # Where merged output was written
    status: str = "SUCCESS"                # SUCCESS / PARTIAL / FAILED
    error_message: str = ""


# ══════════════════════════════════════════════════════════════════════
#  STATE MACHINE LOGIC
# ══════════════════════════════════════════════════════════════════════

class OverrideStateMachine:
    """Enforces legal state transitions for override entries."""

    @staticmethod
    def can_transition(from_state: OverrideState, to_state: OverrideState) -> bool:
        """Check if a transition is allowed."""
        allowed = ALLOWED_TRANSITIONS.get(from_state, set())
        return to_state in allowed

    @staticmethod
    def transition(entry: OverrideEntry, to_state: OverrideState,
                   trigger: TransitionTrigger, user: str,
                   reason: str = "") -> AuditEvent:
        """Execute a state transition. Returns the audit event.
        Raises ValueError if the transition is forbidden.
        """
        from_state = entry.state

        if not OverrideStateMachine.can_transition(from_state, to_state):
            raise ValueError(
                f"FORBIDDEN transition: {from_state.value} -> {to_state.value}. "
                f"Allowed from {from_state.value}: "
                f"{[s.value for s in ALLOWED_TRANSITIONS.get(from_state, set())]}"
            )

        now = datetime.now().isoformat()

        # Capture pre-transition snapshot
        event = AuditEvent(
            event_id="",  # Caller assigns UUID
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=now,
            state_before=from_state.value,
            state_after=to_state.value,
            trigger=trigger.value,
            user=user,
            reason=reason,
            bpm_before=entry.override_bpm if from_state != OverrideState.NEW else entry.original_bpm,
            bpm_after=entry.override_bpm,
            key_before=entry.override_key if from_state != OverrideState.NEW else entry.original_key,
            key_after=entry.override_key,
        )

        # Apply transition
        entry.state = to_state
        entry.updated_at = now

        # Update phase-specific timestamps
        if to_state == OverrideState.VALID or to_state == OverrideState.INVALID or to_state == OverrideState.CONFLICT:
            entry.validated_at = now
        elif to_state == OverrideState.APPROVED:
            entry.approved_at = now
        elif to_state == OverrideState.APPLIED:
            entry.applied_at = now
        elif to_state == OverrideState.DISABLED:
            entry.disabled_at = now

        return event

    @staticmethod
    def get_allowed_actions(state: OverrideState) -> list[str]:
        """Return human-readable action names available from a given state."""
        action_map: dict[tuple[OverrideState, OverrideState], str] = {
            (OverrideState.NEW, OverrideState.DRAFT): "Edit Override",
            (OverrideState.NEW, OverrideState.DISABLED): "Dismiss",
            (OverrideState.DRAFT, OverrideState.PENDING_VALIDATION): "Validate",
            (OverrideState.DRAFT, OverrideState.DRAFT): "Save Draft",
            (OverrideState.DRAFT, OverrideState.DISABLED): "Discard",
            (OverrideState.VALID, OverrideState.APPROVED): "Approve",
            (OverrideState.VALID, OverrideState.DRAFT): "Edit Again",
            (OverrideState.VALID, OverrideState.DISABLED): "Disable",
            (OverrideState.INVALID, OverrideState.DRAFT): "Fix & Re-edit",
            (OverrideState.INVALID, OverrideState.DISABLED): "Discard",
            (OverrideState.CONFLICT, OverrideState.DRAFT): "Fix Identity",
            (OverrideState.CONFLICT, OverrideState.DISABLED): "Discard",
            (OverrideState.APPROVED, OverrideState.APPLIED): "Apply Merge",
            (OverrideState.APPROVED, OverrideState.DRAFT): "Revoke & Edit",
            (OverrideState.APPROVED, OverrideState.DISABLED): "Revoke & Disable",
            (OverrideState.APPLIED, OverrideState.SUPERSEDED): "(auto) Supersede",
            (OverrideState.APPLIED, OverrideState.DISABLED): "Disable Override",
            (OverrideState.DISABLED, OverrideState.DRAFT): "Re-enable & Edit",
        }
        allowed = ALLOWED_TRANSITIONS.get(state, set())
        actions = []
        for to_state in allowed:
            label = action_map.get((state, to_state), f"-> {to_state.value}")
            actions.append(label)
        return actions

    @staticmethod
    def format_transition_table() -> str:
        """Return a human-readable transition table."""
        lines = ["OVERRIDE STATE TRANSITION TABLE", "=" * 60, ""]
        for from_state in OverrideState:
            allowed = ALLOWED_TRANSITIONS.get(from_state, set())
            if not allowed:
                lines.append(f"  {from_state.value:25s} -> (terminal state)")
            else:
                for to_state in sorted(allowed, key=lambda s: s.value):
                    trigger = TRANSITION_TRIGGERS.get((from_state, to_state), "?")
                    trigger_name = trigger.value if isinstance(trigger, TransitionTrigger) else str(trigger)
                    lines.append(f"  {from_state.value:25s} -> {to_state.value:25s}  [{trigger_name}]")
            lines.append("")
        return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════
#  VALIDATION STATUS MAPPING
# ══════════════════════════════════════════════════════════════════════

# Maps validation engine output status to state machine state
VALIDATION_RESULT_TO_STATE: dict[str, OverrideState] = {
    "VALID": OverrideState.VALID,
    "INVALID": OverrideState.INVALID,
    "CONFLICT": OverrideState.CONFLICT,
    "PENDING": OverrideState.DRAFT,  # Not yet validated
}

VALIDATION_RESULT_TO_TRIGGER: dict[str, TransitionTrigger] = {
    "VALID": TransitionTrigger.VALIDATION_PASS,
    "INVALID": TransitionTrigger.VALIDATION_FAIL,
    "CONFLICT": TransitionTrigger.VALIDATION_CONFLICT,
}


# ══════════════════════════════════════════════════════════════════════
#  SELF-TEST
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(OverrideStateMachine.format_transition_table())
    print()
    print("AVAILABLE ACTIONS BY STATE:")
    print("-" * 40)
    for s in OverrideState:
        actions = OverrideStateMachine.get_allowed_actions(s)
        print(f"  {s.value:25s}: {', '.join(actions) if actions else '(none)'}")
    print()

    # Verify all declared transitions have triggers
    missing = []
    for from_s, to_set in ALLOWED_TRANSITIONS.items():
        for to_s in to_set:
            if (from_s, to_s) not in TRANSITION_TRIGGERS:
                missing.append(f"  {from_s.value} -> {to_s.value}")
    if missing:
        print(f"WARNING: {len(missing)} transitions missing triggers:")
        for m in missing:
            print(m)
    else:
        print("ALL transitions have assigned triggers. ✓")

    # Verify no self-transitions except DRAFT->DRAFT
    bad_self = []
    for from_s, to_set in ALLOWED_TRANSITIONS.items():
        if from_s in to_set and from_s != OverrideState.DRAFT:
            bad_self.append(from_s.value)
    if bad_self:
        print(f"WARNING: unexpected self-transitions: {bad_self}")
    else:
        print("No unexpected self-transitions. ✓")

    # Verify SUPERSEDED is terminal
    if ALLOWED_TRANSITIONS.get(OverrideState.SUPERSEDED) == set():
        print("SUPERSEDED is terminal. ✓")
    else:
        print("WARNING: SUPERSEDED has outgoing transitions!")
