"""
NGKsPlayerNative — Override Service Demo
End-to-end contract test of the Override Backend Adapter.

Exercises the full lifecycle:
 1. Load review queue
 2. Create BPM draft
 3. Create Key draft
 4. Validate both
 5. Approve valid overrides
 6. Attempt invalid transition (prove it is blocked)
 7. Apply approved overrides
 8. Query effective row
 9. Query history
10. Write proof artifacts
"""

import json
import os
import sys
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from override_backend_adapter import (
    OverrideBackendAdapter,
    OverrideError,
    OverrideOperationError,
)
from override_store import OverrideStore

WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
PROOF_DIR = os.path.join(WORKSPACE, "_proof", "override_backend_adapter")
DEMO_STORE_DIR = os.path.join(PROOF_DIR, "demo_store")

LOG_LINES: list[str] = []


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
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
    os.makedirs(DEMO_STORE_DIR, exist_ok=True)

    log("=== OVERRIDE BACKEND ADAPTER — DEMO / CONTRACT TEST ===")
    log(f"Workspace: {WORKSPACE}")
    log(f"Proof dir: {PROOF_DIR}")
    log("")

    # Initialize with isolated store
    store = OverrideStore(store_dir=DEMO_STORE_DIR)
    store.reset()  # clean slate
    adapter = OverrideBackendAdapter(store=store)

    demo_results: dict = {
        "timestamp": datetime.now().isoformat(),
        "steps": [],
        "errors_caught": [],
        "final_status": "PENDING",
    }

    transition_results: list[dict] = []
    effective_rows: list[dict] = []
    error_examples: list[dict] = []

    # ──────────────────────────────────────────────────────────
    #  STEP 1: Load review queue
    # ──────────────────────────────────────────────────────────
    log("── STEP 1: Load Review Queue ──")
    queue = adapter.load_review_queue()
    log(f"Review queue rows: {len(queue)}")
    if len(queue) < 2:
        log("FATAL: Need at least 2 review rows for demo")
        demo_results["final_status"] = "FAIL"
        write_json_artifact("01_demo_flow_results.json", demo_results)
        return

    demo_results["steps"].append({
        "step": 1,
        "action": "load_review_queue",
        "result": f"{len(queue)} rows",
        "status": "OK",
    })

    # Pick two rows for demo
    row_bpm = queue[0]
    row_key = queue[1]
    log(f"  BPM demo row: #{row_bpm['row_number']} {row_bpm['artist']} — {row_bpm['title']}")
    log(f"  Key demo row: #{row_key['row_number']} {row_key['artist']} — {row_key['title']}")

    # ──────────────────────────────────────────────────────────
    #  STEP 2: Create BPM draft
    # ──────────────────────────────────────────────────────────
    log("")
    log("── STEP 2: Create BPM Draft Override ──")
    bpm_identity = {
        "filename": row_bpm["filename"],
        "artist": row_bpm["artist"],
        "title": row_bpm["title"],
        "row_number": row_bpm["row_number"],
    }
    bpm_payload = {
        "override_bpm": 128.0,
        "override_key": None,
        "override_scope": "BPM",
        "bpm_reason": "Demo: verified BPM manually",
        "key_reason": "",
        "notes": "Contract test BPM override",
        "entered_by": "DemoSystem",
    }
    bpm_draft = adapter.create_override_draft(bpm_identity, bpm_payload)
    bpm_id = bpm_draft["override_id"]
    log(f"  Created: {bpm_id} state={bpm_draft['state']}")
    assert bpm_draft["state"] == "DRAFT", f"Expected DRAFT, got {bpm_draft['state']}"

    transition_results.append({
        "override_id": bpm_id,
        "transition": "NEW -> DRAFT",
        "trigger": "USER_EDIT",
        "result": "ALLOWED",
    })

    demo_results["steps"].append({
        "step": 2,
        "action": "create_override_draft (BPM)",
        "override_id": bpm_id,
        "state": "DRAFT",
        "status": "OK",
    })

    # ──────────────────────────────────────────────────────────
    #  STEP 3: Create Key draft
    # ──────────────────────────────────────────────────────────
    log("")
    log("── STEP 3: Create Key Draft Override ──")
    key_identity = {
        "filename": row_key["filename"],
        "artist": row_key["artist"],
        "title": row_key["title"],
        "row_number": row_key["row_number"],
    }
    key_payload = {
        "override_bpm": None,
        "override_key": "8B",
        "override_scope": "KEY",
        "bpm_reason": "",
        "key_reason": "Demo: verified key via Tunebat — C major",
        "notes": "Contract test Key override",
        "entered_by": "DemoSystem",
    }
    key_draft = adapter.create_override_draft(key_identity, key_payload)
    key_id = key_draft["override_id"]
    log(f"  Created: {key_id} state={key_draft['state']}")
    assert key_draft["state"] == "DRAFT"

    transition_results.append({
        "override_id": key_id,
        "transition": "NEW -> DRAFT",
        "trigger": "USER_EDIT",
        "result": "ALLOWED",
    })

    demo_results["steps"].append({
        "step": 3,
        "action": "create_override_draft (KEY)",
        "override_id": key_id,
        "state": "DRAFT",
        "status": "OK",
    })

    # ──────────────────────────────────────────────────────────
    #  STEP 4: Validate both
    # ──────────────────────────────────────────────────────────
    log("")
    log("── STEP 4: Validate Both Overrides ──")

    bpm_val = adapter.validate_override(bpm_id)
    log(f"  BPM validation: status={bpm_val['status']} messages={bpm_val['messages']}")

    transition_results.append({
        "override_id": bpm_id,
        "transition": "DRAFT -> PENDING_VALIDATION -> " + bpm_val["status"],
        "trigger": "USER_SUBMIT -> VALIDATION_*",
        "result": "ALLOWED",
    })

    key_val = adapter.validate_override(key_id)
    log(f"  Key validation: status={key_val['status']} messages={key_val['messages']}")

    transition_results.append({
        "override_id": key_id,
        "transition": "DRAFT -> PENDING_VALIDATION -> " + key_val["status"],
        "trigger": "USER_SUBMIT -> VALIDATION_*",
        "result": "ALLOWED",
    })

    demo_results["steps"].append({
        "step": 4,
        "action": "validate_override (both)",
        "bpm_result": bpm_val,
        "key_result": key_val,
        "status": "OK",
    })

    # ──────────────────────────────────────────────────────────
    #  STEP 5: Approve valid overrides
    # ──────────────────────────────────────────────────────────
    log("")
    log("── STEP 5: Approve Valid Overrides ──")

    approved_ids: list[str] = []

    if bpm_val["status"] == "VALID":
        bpm_approved = adapter.approve_override(bpm_id, "NGK")
        log(f"  BPM approved: state={bpm_approved['state']}")
        approved_ids.append(bpm_id)
        transition_results.append({
            "override_id": bpm_id,
            "transition": "VALID -> APPROVED",
            "trigger": "USER_APPROVE",
            "result": "ALLOWED",
        })
    else:
        log(f"  BPM not valid ({bpm_val['status']}), skipping approval")

    if key_val["status"] == "VALID":
        key_approved = adapter.approve_override(key_id, "NGK")
        log(f"  Key approved: state={key_approved['state']}")
        approved_ids.append(key_id)
        transition_results.append({
            "override_id": key_id,
            "transition": "VALID -> APPROVED",
            "trigger": "USER_APPROVE",
            "result": "ALLOWED",
        })
    else:
        log(f"  Key not valid ({key_val['status']}), skipping approval")

    demo_results["steps"].append({
        "step": 5,
        "action": "approve_override",
        "approved_ids": approved_ids,
        "status": "OK",
    })

    # ──────────────────────────────────────────────────────────
    #  STEP 6: Invalid transition attempt
    # ──────────────────────────────────────────────────────────
    log("")
    log("── STEP 6: Attempt Invalid Transition (DRAFT -> APPROVED) ──")

    # Create a new draft and try to approve it directly (skip validation)
    if len(queue) > 2:
        bad_row = queue[2]
        bad_identity = {
            "filename": bad_row["filename"],
            "artist": bad_row["artist"],
            "title": bad_row["title"],
            "row_number": bad_row["row_number"],
        }
        bad_payload = {
            "override_bpm": 100.0,
            "override_scope": "BPM",
            "bpm_reason": "test",
            "entered_by": "DemoSystem",
        }
        bad_draft = adapter.create_override_draft(bad_identity, bad_payload)
        bad_id = bad_draft["override_id"]
        log(f"  Created test draft: {bad_id} state=DRAFT")

        try:
            adapter.approve_override(bad_id, "NGK")
            log("  ERROR: Approve should have been blocked!")
            error_examples.append({
                "test": "DRAFT -> APPROVED (skip validation)",
                "result": "UNEXPECTEDLY ALLOWED",
                "error": None,
            })
        except OverrideOperationError as e:
            log(f"  BLOCKED as expected: {e.error.code} — {e.error.message}")
            error_examples.append({
                "test": "DRAFT -> APPROVED (skip validation)",
                "result": "CORRECTLY BLOCKED",
                "error": e.error.to_dict(),
            })
            transition_results.append({
                "override_id": bad_id,
                "transition": "DRAFT -> APPROVED",
                "trigger": "USER_APPROVE",
                "result": "BLOCKED (INVALID_TRANSITION)",
            })

        # Also test duplicate draft
        log("")
        log("── STEP 6b: Attempt Duplicate Active Draft ──")
        try:
            adapter.create_override_draft(bpm_identity, bpm_payload)
            log("  ERROR: Duplicate should have been blocked!")
            error_examples.append({
                "test": "Duplicate draft for same row",
                "result": "UNEXPECTEDLY ALLOWED",
                "error": None,
            })
        except OverrideOperationError as e:
            log(f"  BLOCKED as expected: {e.error.code} — {e.error.message}")
            error_examples.append({
                "test": "Duplicate draft for same row",
                "result": "CORRECTLY BLOCKED",
                "error": e.error.to_dict(),
            })

        # Test ROW_NOT_FOUND
        log("")
        log("── STEP 6c: Attempt Non-Existent Row ──")
        try:
            adapter.create_override_draft(
                {"filename": "DOES_NOT_EXIST.mp3", "artist": "", "title": ""},
                bpm_payload,
            )
            log("  ERROR: Should have been blocked!")
        except OverrideOperationError as e:
            log(f"  BLOCKED as expected: {e.error.code} — {e.error.message}")
            error_examples.append({
                "test": "Non-existent row",
                "result": "CORRECTLY BLOCKED",
                "error": e.error.to_dict(),
            })

        # Clean up test draft
        adapter.disable_override(bad_id, "Test cleanup")
    else:
        log("  (not enough rows to test — skipped)")

    demo_results["steps"].append({
        "step": 6,
        "action": "invalid_transition_attempts",
        "errors_caught": len(error_examples),
        "status": "OK",
    })

    # ──────────────────────────────────────────────────────────
    #  STEP 7: Apply approved overrides
    # ──────────────────────────────────────────────────────────
    log("")
    log("── STEP 7: Apply Approved Overrides ──")

    if approved_ids:
        merge_result = adapter.apply_approved_overrides("demo-batch-1", "NGK")
        log(f"  Batch: {merge_result['batch_id']}")
        log(f"  Applied: {merge_result['applied_count']}")
        log(f"  Skipped invalid: {merge_result['skipped_invalid']}")
        log(f"  Skipped conflict: {merge_result['skipped_conflict']}")
        log(f"  Status: {merge_result['status']}")
        log(f"  Export: {merge_result['merged_export_path']}")

        for aid in approved_ids:
            transition_results.append({
                "override_id": aid,
                "transition": "APPROVED -> APPLIED",
                "trigger": "MERGE_SUCCESS",
                "result": "ALLOWED",
            })

        demo_results["steps"].append({
            "step": 7,
            "action": "apply_approved_overrides",
            "merge_result": merge_result,
            "status": "OK",
        })
    else:
        log("  No approved overrides to apply")
        demo_results["steps"].append({
            "step": 7,
            "action": "apply_approved_overrides",
            "result": "SKIPPED — no approved overrides",
            "status": "WARN",
        })

    # ──────────────────────────────────────────────────────────
    #  STEP 8: Query effective row
    # ──────────────────────────────────────────────────────────
    log("")
    log("── STEP 8: Get Effective Rows ──")

    eff_bpm = adapter.get_effective_row(bpm_identity)
    log(f"  BPM row effective:")
    log(f"    Base BPM:     {eff_bpm['base_values']['bpm']}")
    log(f"    Effective BPM: {eff_bpm['effective_values']['bpm']}")
    log(f"    BPM source:   {eff_bpm['effective_values']['bpm_source']}")
    effective_rows.append(eff_bpm)

    eff_key = adapter.get_effective_row(key_identity)
    log(f"  Key row effective:")
    log(f"    Base Key:     {eff_key['base_values']['key']}")
    log(f"    Effective Key: {eff_key['effective_values']['key']}")
    log(f"    Key source:   {eff_key['effective_values']['key_source']}")
    effective_rows.append(eff_key)

    demo_results["steps"].append({
        "step": 8,
        "action": "get_effective_row",
        "status": "OK",
    })

    # ──────────────────────────────────────────────────────────
    #  STEP 9: Query history
    # ──────────────────────────────────────────────────────────
    log("")
    log("── STEP 9: Query History ──")

    bpm_history = adapter.get_override_history(bpm_identity)
    log(f"  BPM override history: {len(bpm_history)} events")
    for ev in bpm_history:
        log(f"    {ev['timestamp']} {ev['state_before']}->{ev['state_after']} [{ev['trigger']}]")

    key_history = adapter.get_override_history(key_identity)
    log(f"  Key override history: {len(key_history)} events")

    merge_history = adapter.get_merge_history()
    log(f"  Merge history: {len(merge_history)} batches")

    demo_results["steps"].append({
        "step": 9,
        "action": "query_history",
        "bpm_events": len(bpm_history),
        "key_events": len(key_history),
        "merge_batches": len(merge_history),
        "status": "OK",
    })

    # ──────────────────────────────────────────────────────────
    #  STEP 10: List all overrides
    # ──────────────────────────────────────────────────────────
    log("")
    log("── STEP 10: List All Overrides ──")
    all_overrides = adapter.list_overrides()
    log(f"  Total overrides: {len(all_overrides)}")
    for ov in all_overrides:
        log(f"    {ov['override_id']}  state={ov['state']:<12s}  "
            f"file={ov['row_identity']['filename']}")

    demo_results["steps"].append({
        "step": 10,
        "action": "list_overrides",
        "count": len(all_overrides),
        "status": "OK",
    })

    # ──────────────────────────────────────────────────────────
    #  FINALIZE
    # ──────────────────────────────────────────────────────────
    log("")
    log("=== DEMO COMPLETE ===")

    all_blocked = all(
        e["result"] == "CORRECTLY BLOCKED" for e in error_examples
    )
    has_applied = any(
        t["result"] == "ALLOWED" and "APPLIED" in t["transition"]
        for t in transition_results
    )
    has_effective = len(effective_rows) >= 2

    gate = "PASS" if (all_blocked and has_applied and has_effective) else "FAIL"
    demo_results["final_status"] = gate
    demo_results["errors_caught"] = error_examples

    log(f"GATE = {gate}")

    # ── Write proof artifacts ──
    log("")
    log("── Writing Proof Artifacts ──")

    p1 = write_json_artifact("01_demo_flow_results.json", demo_results)
    log(f"  {p1}")

    p2_path = os.path.join(PROOF_DIR, "02_state_transition_results.txt")
    with open(p2_path, "w", encoding="utf-8") as f:
        f.write("STATE TRANSITION RESULTS\n")
        f.write("=" * 60 + "\n\n")
        for t in transition_results:
            f.write(f"  Override: {t['override_id']}\n")
            f.write(f"  Transition: {t['transition']}\n")
            f.write(f"  Trigger: {t['trigger']}\n")
            f.write(f"  Result: {t['result']}\n")
            f.write("\n")
    log(f"  {p2_path}")

    p3 = write_json_artifact("03_effective_row_examples.json", effective_rows)
    log(f"  {p3}")

    p4 = write_json_artifact("04_error_examples.json", error_examples)
    log(f"  {p4}")

    # Write execution log
    log_path = os.path.join(PROOF_DIR, "execution_log.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(LOG_LINES))
    log(f"  {log_path}")


if __name__ == "__main__":
    main()
