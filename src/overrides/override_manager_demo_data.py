"""
NGKsPlayerNative — Override Manager Demo Data Generator

Generates 6 example override entries in different lifecycle states,
their associated audit events, and a sample merge batch.
Writes example persistence data to the proof directory.
"""

import json
import os
import sys
import uuid
from datetime import datetime, timedelta

# Allow importing from the same tools directory
sys.path.insert(0, os.path.dirname(__file__))

from override_manager_state_model import (
    AuditEvent,
    MergeBatch,
    OverrideEntry,
    OverrideState,
    OverrideStateMachine,
    RowIdentity,
    TransitionTrigger,
)

# ──────────────────────────────────────────────────────────────────
#  Time helpers
# ──────────────────────────────────────────────────────────────────

BASE_TIME = datetime(2026, 4, 3, 10, 0, 0)

def ts(minutes_offset: int) -> str:
    return (BASE_TIME + timedelta(minutes=minutes_offset)).isoformat()

def make_id(prefix: str, n: int) -> str:
    return f"{prefix}-demo-{n:04d}"


# ──────────────────────────────────────────────────────────────────
#  Entity 1: NEW — flagged for review, not yet edited
# ──────────────────────────────────────────────────────────────────

def make_entity_new() -> tuple[OverrideEntry, list[AuditEvent]]:
    entry = OverrideEntry(
        override_id=make_id("ovr", 1),
        revision=1,
        row_identity=RowIdentity(
            filename="Unknown Artist - Track 05.mp3",
            artist="Unknown Artist",
            title="Track 05",
            row_number=1,
        ),
        state=OverrideState.NEW,
        original_bpm=120.2,
        original_key="2A",
        created_at=ts(0),
        updated_at=ts(0),
    )

    events = [
        AuditEvent(
            event_id=make_id("evt", 1),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(0),
            state_before="",
            state_after="NEW",
            trigger="SYSTEM",
            user="system",
            reason="Row flagged: ReviewRequired=True",
        ),
    ]
    return entry, events


# ──────────────────────────────────────────────────────────────────
#  Entity 2: INVALID — user entered bad BPM, validation failed
# ──────────────────────────────────────────────────────────────────

def make_entity_invalid() -> tuple[OverrideEntry, list[AuditEvent]]:
    entry = OverrideEntry(
        override_id=make_id("ovr", 2),
        revision=1,
        row_identity=RowIdentity(
            filename="Adam Calhoun - Bars & Stripes.mp3",
            artist="Adam Calhoun",
            title="Bars & Stripes",
            row_number=30,
        ),
        state=OverrideState.INVALID,
        override_bpm=999.0,
        override_scope="BPM",
        bpm_reason="Tried to fix BPM",
        entered_by="NGK",
        validation_message="BPM out of range: 999.0 (must be 20–300)",
        original_bpm=95.7,
        original_key="8B",
        created_at=ts(5),
        updated_at=ts(8),
        validated_at=ts(8),
    )

    events = [
        AuditEvent(
            event_id=make_id("evt", 2),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(5),
            state_before="",
            state_after="NEW",
            trigger="SYSTEM",
            user="system",
            reason="Row flagged for review",
        ),
        AuditEvent(
            event_id=make_id("evt", 3),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(6),
            state_before="NEW",
            state_after="DRAFT",
            trigger="USER_EDIT",
            user="NGK",
            reason="Entered override BPM=999",
            bpm_after=999.0,
        ),
        AuditEvent(
            event_id=make_id("evt", 4),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(7),
            state_before="DRAFT",
            state_after="PENDING_VALIDATION",
            trigger="USER_SUBMIT",
            user="NGK",
        ),
        AuditEvent(
            event_id=make_id("evt", 5),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(8),
            state_before="PENDING_VALIDATION",
            state_after="INVALID",
            trigger="VALIDATION_FAIL",
            user="system",
            reason="BPM out of range: 999.0 (must be 20–300)",
            bpm_after=999.0,
        ),
    ]
    return entry, events


# ──────────────────────────────────────────────────────────────────
#  Entity 3: CONFLICT — artist mismatch detected
# ──────────────────────────────────────────────────────────────────

def make_entity_conflict() -> tuple[OverrideEntry, list[AuditEvent]]:
    entry = OverrideEntry(
        override_id=make_id("ovr", 3),
        revision=1,
        row_identity=RowIdentity(
            filename="Bad Wolves - Zombie.mp3",
            artist="WRONG_NAME",
            title="Zombie",
            row_number=66,
        ),
        state=OverrideState.CONFLICT,
        override_key="8B",
        override_scope="KEY",
        key_reason="Verified via Tunebat",
        entered_by="NGK",
        validation_message="Artist mismatch: 'WRONG_NAME' vs 'Bad Wolves'",
        conflict_flag=True,
        original_bpm=80.0,
        original_key="2A",
        created_at=ts(10),
        updated_at=ts(13),
        validated_at=ts(13),
    )

    events = [
        AuditEvent(
            event_id=make_id("evt", 6),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(10),
            state_before="",
            state_after="NEW",
            trigger="SYSTEM",
            user="system",
        ),
        AuditEvent(
            event_id=make_id("evt", 7),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(11),
            state_before="NEW",
            state_after="DRAFT",
            trigger="USER_EDIT",
            user="NGK",
            key_after="8B",
        ),
        AuditEvent(
            event_id=make_id("evt", 8),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(12),
            state_before="DRAFT",
            state_after="PENDING_VALIDATION",
            trigger="USER_SUBMIT",
            user="NGK",
        ),
        AuditEvent(
            event_id=make_id("evt", 9),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(13),
            state_before="PENDING_VALIDATION",
            state_after="CONFLICT",
            trigger="VALIDATION_CONFLICT",
            user="system",
            reason="Artist mismatch: 'WRONG_NAME' vs 'Bad Wolves'",
        ),
    ]
    return entry, events


# ──────────────────────────────────────────────────────────────────
#  Entity 4: APPROVED — valid and approved, waiting for merge
# ──────────────────────────────────────────────────────────────────

def make_entity_approved() -> tuple[OverrideEntry, list[AuditEvent]]:
    entry = OverrideEntry(
        override_id=make_id("ovr", 4),
        revision=1,
        row_identity=RowIdentity(
            filename="Airbourne - Runnin' Wild.mp3",
            artist="Airbourne",
            title="Runnin' Wild",
            row_number=34,
        ),
        state=OverrideState.APPROVED,
        override_bpm=140.0,
        override_key="10B",
        override_scope="BPM_AND_KEY",
        bpm_reason="Counted beats manually — 140 confirmed",
        key_reason="Verified via Mixxx",
        entered_by="NGK",
        validation_message="VALID",
        original_bpm=95.7,
        original_key="3A",
        created_at=ts(15),
        updated_at=ts(20),
        validated_at=ts(18),
        approved_at=ts(20),
    )

    events = [
        AuditEvent(
            event_id=make_id("evt", 10),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(15),
            state_before="",
            state_after="NEW",
            trigger="SYSTEM",
            user="system",
        ),
        AuditEvent(
            event_id=make_id("evt", 11),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(16),
            state_before="NEW",
            state_after="DRAFT",
            trigger="USER_EDIT",
            user="NGK",
            bpm_after=140.0,
            key_after="10B",
        ),
        AuditEvent(
            event_id=make_id("evt", 12),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(17),
            state_before="DRAFT",
            state_after="PENDING_VALIDATION",
            trigger="USER_SUBMIT",
            user="NGK",
        ),
        AuditEvent(
            event_id=make_id("evt", 13),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(18),
            state_before="PENDING_VALIDATION",
            state_after="VALID",
            trigger="VALIDATION_PASS",
            user="system",
        ),
        AuditEvent(
            event_id=make_id("evt", 14),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(20),
            state_before="VALID",
            state_after="APPROVED",
            trigger="USER_APPROVE",
            user="NGK",
            reason="Values confirmed correct",
        ),
    ]
    return entry, events


# ──────────────────────────────────────────────────────────────────
#  Entity 5: APPLIED — merged into export
# ──────────────────────────────────────────────────────────────────

def make_entity_applied() -> tuple[OverrideEntry, list[AuditEvent]]:
    entry = OverrideEntry(
        override_id=make_id("ovr", 5),
        revision=1,
        row_identity=RowIdentity(
            filename="DJ LOA - Don't Say Goodbye Remix.mp3",
            artist="DJ LOA",
            title="Don't Say Goodbye Remix",
            row_number=294,
        ),
        state=OverrideState.APPLIED,
        override_bpm=128.0,
        override_scope="BPM",
        bpm_reason="Verified: 128 BPM in Mixxx + manual count",
        entered_by="NGK",
        validation_message="VALID",
        original_bpm=120.2,
        original_key="3A",
        created_at=ts(25),
        updated_at=ts(35),
        validated_at=ts(28),
        approved_at=ts(30),
        applied_at=ts(35),
        merge_batch_id="MB-demo-0001",
    )

    events = [
        AuditEvent(
            event_id=make_id("evt", 15),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(25),
            state_before="",
            state_after="NEW",
            trigger="SYSTEM",
            user="system",
        ),
        AuditEvent(
            event_id=make_id("evt", 16),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(26),
            state_before="NEW",
            state_after="DRAFT",
            trigger="USER_EDIT",
            user="NGK",
            bpm_after=128.0,
        ),
        AuditEvent(
            event_id=make_id("evt", 17),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(27),
            state_before="DRAFT",
            state_after="PENDING_VALIDATION",
            trigger="USER_SUBMIT",
            user="NGK",
        ),
        AuditEvent(
            event_id=make_id("evt", 18),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(28),
            state_before="PENDING_VALIDATION",
            state_after="VALID",
            trigger="VALIDATION_PASS",
            user="system",
        ),
        AuditEvent(
            event_id=make_id("evt", 19),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(30),
            state_before="VALID",
            state_after="APPROVED",
            trigger="USER_APPROVE",
            user="NGK",
        ),
        AuditEvent(
            event_id=make_id("evt", 20),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(35),
            state_before="APPROVED",
            state_after="APPLIED",
            trigger="MERGE_SUCCESS",
            user="system",
            merge_batch_id="MB-demo-0001",
        ),
    ]
    return entry, events


# ──────────────────────────────────────────────────────────────────
#  Entity 6: SUPERSEDED — replaced by a newer revision
# ──────────────────────────────────────────────────────────────────

def make_entity_superseded() -> tuple[OverrideEntry, list[AuditEvent]]:
    entry = OverrideEntry(
        override_id=make_id("ovr", 6),
        revision=1,
        row_identity=RowIdentity(
            filename="Five Finger Death Punch - Wrong Side Of Heaven.mp3",
            artist="Five Finger Death Punch",
            title="Wrong Side Of Heaven",
            row_number=321,
        ),
        state=OverrideState.SUPERSEDED,
        override_bpm=85.0,
        override_scope="BPM",
        bpm_reason="Initial BPM correction",
        entered_by="NGK",
        validation_message="VALID",
        original_bpm=170.0,
        original_key="5A",
        created_at=ts(40),
        updated_at=ts(55),
        validated_at=ts(43),
        approved_at=ts(45),
        applied_at=ts(50),
        merge_batch_id="MB-demo-0001",
        superseded_by=make_id("ovr", 7),
    )

    # The replacement entry (rev 2)
    replacement = OverrideEntry(
        override_id=make_id("ovr", 7),
        revision=2,
        row_identity=RowIdentity(
            filename="Five Finger Death Punch - Wrong Side Of Heaven.mp3",
            artist="Five Finger Death Punch",
            title="Wrong Side Of Heaven",
            row_number=321,
        ),
        state=OverrideState.DRAFT,
        override_bpm=85.0,
        override_key="10A",
        override_scope="BPM_AND_KEY",
        bpm_reason="Initial BPM correction (kept)",
        key_reason="Also fixing key — was wrong Camelot code",
        entered_by="NGK",
        original_bpm=170.0,
        original_key="5A",
        created_at=ts(55),
        updated_at=ts(56),
    )

    events = [
        # Original override lifecycle (abbreviated — showing key events)
        AuditEvent(
            event_id=make_id("evt", 21),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(40),
            state_before="",
            state_after="NEW",
            trigger="SYSTEM",
            user="system",
        ),
        AuditEvent(
            event_id=make_id("evt", 22),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(50),
            state_before="APPROVED",
            state_after="APPLIED",
            trigger="MERGE_SUCCESS",
            user="system",
            merge_batch_id="MB-demo-0001",
        ),
        AuditEvent(
            event_id=make_id("evt", 23),
            override_id=entry.override_id,
            row_identity_key=entry.row_identity.identity_key,
            timestamp=ts(55),
            state_before="APPLIED",
            state_after="SUPERSEDED",
            trigger="NEW_REVISION",
            user="NGK",
            reason=f"Replaced by revision 2 ({replacement.override_id})",
        ),
        # New revision created
        AuditEvent(
            event_id=make_id("evt", 24),
            override_id=replacement.override_id,
            row_identity_key=replacement.row_identity.identity_key,
            timestamp=ts(55),
            state_before="",
            state_after="NEW",
            trigger="SYSTEM",
            user="system",
            reason=f"New revision replacing {entry.override_id}",
        ),
        AuditEvent(
            event_id=make_id("evt", 25),
            override_id=replacement.override_id,
            row_identity_key=replacement.row_identity.identity_key,
            timestamp=ts(56),
            state_before="NEW",
            state_after="DRAFT",
            trigger="USER_EDIT",
            user="NGK",
            bpm_after=85.0,
            key_after="10A",
        ),
    ]
    return entry, events, replacement  # type: ignore[return-value]


# ──────────────────────────────────────────────────────────────────
#  Sample Merge Batch
# ──────────────────────────────────────────────────────────────────

def make_merge_batch() -> MergeBatch:
    return MergeBatch(
        batch_id="MB-demo-0001",
        timestamp=ts(35),
        user="NGK",
        base_export_path="_artifacts/exports/NGKs_final_analyzer_export.csv",
        base_export_row_count=907,
        overrides_submitted=2,
        overrides_applied=2,
        overrides_skipped_invalid=0,
        overrides_skipped_conflict=0,
        merged_export_path="_artifacts/exports/NGKs_final_analyzer_export_OVERRIDDEN.csv",
        status="SUCCESS",
    )


# ──────────────────────────────────────────────────────────────────
#  Serialization helpers
# ──────────────────────────────────────────────────────────────────

def entry_to_dict(e: OverrideEntry) -> dict:
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


def event_to_dict(ev: AuditEvent) -> dict:
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


def batch_to_dict(b: MergeBatch) -> dict:
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


# ──────────────────────────────────────────────────────────────────
#  Main: generate and write demo data
# ──────────────────────────────────────────────────────────────────

def main() -> None:
    proof_dir = os.path.join(os.path.dirname(__file__), "..", "_proof", "override_manager_ui")
    os.makedirs(proof_dir, exist_ok=True)

    # Collect all entities
    all_entries: list[OverrideEntry] = []
    all_events: list[AuditEvent] = []

    e1, evts1 = make_entity_new()
    all_entries.append(e1)
    all_events.extend(evts1)

    e2, evts2 = make_entity_invalid()
    all_entries.append(e2)
    all_events.extend(evts2)

    e3, evts3 = make_entity_conflict()
    all_entries.append(e3)
    all_events.extend(evts3)

    e4, evts4 = make_entity_approved()
    all_entries.append(e4)
    all_events.extend(evts4)

    e5, evts5 = make_entity_applied()
    all_entries.append(e5)
    all_events.extend(evts5)

    result6 = make_entity_superseded()
    e6_old = result6[0]
    evts6 = result6[1]
    e6_new = result6[2]
    all_entries.append(e6_old)
    all_entries.append(e6_new)
    all_events.extend(evts6)

    merge = make_merge_batch()

    # Sort events by timestamp
    all_events.sort(key=lambda x: x.timestamp)

    # ── Write override_events.jsonl (simulated event log) ──
    events_path = os.path.join(proof_dir, "demo_override_events.jsonl")
    with open(events_path, "w", encoding="utf-8") as f:
        for ev in all_events:
            f.write(json.dumps(event_to_dict(ev), ensure_ascii=False) + "\n")

    # ── Write override_state.json (simulated state snapshot) ──
    state_snapshot = {
        "version": 1,
        "last_event_id": all_events[-1].event_id,
        "last_updated": all_events[-1].timestamp,
        "overrides": {e.override_id: entry_to_dict(e) for e in all_entries},
    }
    state_path = os.path.join(proof_dir, "demo_override_state.json")
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state_snapshot, f, indent=2, ensure_ascii=False)

    # ── Write merge_history.jsonl ──
    merge_path = os.path.join(proof_dir, "demo_merge_history.jsonl")
    with open(merge_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(batch_to_dict(merge), ensure_ascii=False) + "\n")

    # ── Print summary ──
    print(f"Demo data generated in: {proof_dir}")
    print(f"  Entries:      {len(all_entries)}")
    print(f"  Audit events: {len(all_events)}")
    print(f"  Merge batches: 1")
    print()
    print("Entity summary:")
    for e in all_entries:
        state_val = e.state.value if isinstance(e.state, OverrideState) else e.state
        print(f"  {e.override_id}  rev={e.revision}  state={state_val:<12s}  "
              f"file={e.row_identity.filename}")
    print()
    print("Files written:")
    print(f"  {events_path}")
    print(f"  {state_path}")
    print(f"  {merge_path}")

    # ── Verify state machine transitions ──
    print()
    print("State machine transition table verification:")
    sm = OverrideStateMachine()
    table = sm.format_transition_table()
    print(table)


if __name__ == "__main__":
    main()
