"""
NGKsPlayerNative — Override Validation
Validates override entries against the base export before merge.
Writes validation results and conflict reports.
"""

import csv
import os
import sys
from datetime import datetime

WORKSPACE = r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"

CAMELOT_MAP = {
    "C major": "8B",  "G major": "9B",  "D major": "10B", "A major": "11B",
    "E major": "12B", "B major": "1B",  "F# major": "2B", "Db major": "3B",
    "Ab major": "4B", "Eb major": "5B", "Bb major": "6B", "F major": "7B",
    "A minor": "8A",  "E minor": "9A",  "B minor": "10A", "F# minor": "11A",
    "C# minor": "12A", "Ab minor": "1A", "Eb minor": "2A", "Bb minor": "3A",
    "F minor": "4A",  "C minor": "5A",  "G minor": "6A",  "D minor": "7A",
    "C# major": "3B", "D# major": "5B", "D# minor": "2A",
    "G# major": "4B", "G# minor": "1A",
    "A# major": "6B", "A# minor": "3A",
}
REVERSE_CAMELOT = {}
for _kn, _cam in CAMELOT_MAP.items():
    if _cam not in REVERSE_CAMELOT:
        REVERSE_CAMELOT[_cam] = _kn

FLAT_TO_SHARP = {
    "Db": "C#", "Eb": "D#", "Fb": "E", "Gb": "F#",
    "Ab": "G#", "Bb": "A#", "Cb": "B",
}

VALID_CAMELOT = set()
for n in range(1, 13):
    VALID_CAMELOT.add(f"{n}A")
    VALID_CAMELOT.add(f"{n}B")

TRUTHY = {"TRUE", "1", "YES", "Y"}
FALSY = {"FALSE", "0", "NO", "N", ""}
VALID_SCOPES = {"BPM", "KEY", "BPM_AND_KEY"}


def _norm(s):
    return (s or "").strip()


def _norm_lower(s):
    return _norm(s).lower()


def parse_key_to_camelot(raw):
    """Parse a key string to Camelot notation. Returns (camelot, name) or (None, None)."""
    raw = _norm(raw)
    if not raw:
        return None, None
    # Already Camelot?
    up = raw.upper()
    if up in VALID_CAMELOT:
        return up, REVERSE_CAMELOT.get(up, up)
    # Western name?
    # Normalize flats
    parts = raw.split()
    if len(parts) == 2:
        root, mode = parts[0], parts[1].lower()
        for flat, sharp in FLAT_TO_SHARP.items():
            if root == flat:
                root = sharp
                break
        key_name = f"{root} {mode}"
        cam = CAMELOT_MAP.get(key_name)
        if cam:
            return cam, key_name
        # Try with capitalized root
        key_name2 = f"{root.capitalize()} {mode}"
        cam = CAMELOT_MAP.get(key_name2)
        if cam:
            return cam, key_name2
    return None, None


def validate_overrides(override_rows, base_lookup):
    """Validate a list of override dicts against the base export lookup.
    
    Returns list of validated override dicts with Status/ConflictFlag/ValidationMessage set.
    """
    results = []
    seen_rows = {}  # row_num -> index for duplicate detection

    for idx, ov in enumerate(override_rows):
        row_num = _norm(ov.get("Row", ""))
        artist = _norm(ov.get("Artist", ""))
        title = _norm(ov.get("Title", ""))
        filename = _norm(ov.get("Filename", ""))
        enabled_raw = _norm(ov.get("OverrideEnabled", "")).upper()
        scope = _norm(ov.get("OverrideScope", "")).upper()
        bpm_raw = _norm(ov.get("OverrideFinalBPM", ""))
        key_raw = _norm(ov.get("OverrideFinalKey", ""))

        result = dict(ov)
        messages = []
        status = "VALID"
        conflict = False

        # ── Enabled check ──
        if enabled_raw in FALSY:
            result["OverrideStatus"] = "PENDING"
            result["OverrideConflictFlag"] = False
            result["OverrideValidationMessage"] = "disabled"
            results.append(result)
            continue

        if enabled_raw not in TRUTHY:
            status = "INVALID"
            messages.append(f"OverrideEnabled unrecognized: '{enabled_raw}'")

        # ── Row exists ──
        if not row_num or not row_num.isdigit():
            status = "INVALID"
            messages.append(f"Row missing or non-numeric: '{row_num}'")
        else:
            base = base_lookup.get(row_num)
            if not base:
                status = "INVALID"
                messages.append(f"Row {row_num} not found in base export")
            else:
                # Identity check
                base_artist = _norm(base.get("Artist", ""))
                base_title = _norm(base.get("Title", ""))
                base_filename = _norm(base.get("Filename", ""))
                if _norm_lower(artist) != _norm_lower(base_artist):
                    conflict = True
                    status = "CONFLICT"
                    messages.append(f"Artist mismatch: override='{artist}' base='{base_artist}'")
                if _norm_lower(title) != _norm_lower(base_title):
                    conflict = True
                    status = "CONFLICT"
                    messages.append(f"Title mismatch: override='{title}' base='{base_title}'")
                if filename and _norm_lower(filename) != _norm_lower(base_filename):
                    messages.append(f"Filename mismatch (warning): override='{filename}' base='{base_filename}'")

        # ── Duplicate detection ──
        if row_num and row_num.isdigit():
            if row_num in seen_rows:
                conflict = True
                status = "CONFLICT"
                messages.append(f"Duplicate override for Row {row_num} (first at index {seen_rows[row_num]})")
            else:
                seen_rows[row_num] = idx

        # ── Scope check ──
        if scope not in VALID_SCOPES:
            status = "INVALID"
            messages.append(f"OverrideScope invalid: '{scope}' (expected BPM/KEY/BPM_AND_KEY)")

        # ── BPM validation ──
        bpm_valid = False
        bpm_value = None
        if bpm_raw:
            try:
                bpm_value = float(bpm_raw)
                if bpm_value <= 20 or bpm_value >= 300:
                    status = "INVALID"
                    messages.append(f"BPM out of range: {bpm_value} (must be 20-300)")
                else:
                    bpm_valid = True
            except ValueError:
                status = "INVALID"
                messages.append(f"BPM not numeric: '{bpm_raw}'")

        # ── Key validation ──
        key_valid = False
        key_camelot = None
        key_name = None
        if key_raw:
            key_camelot, key_name = parse_key_to_camelot(key_raw)
            if key_camelot is None:
                status = "INVALID"
                messages.append(f"Key unrecognized: '{key_raw}'")
            else:
                key_valid = True

        # ── Scope vs field consistency ──
        if status != "INVALID" and status != "CONFLICT":
            if scope == "BPM" and not bpm_raw:
                status = "INVALID"
                messages.append("Scope=BPM but OverrideFinalBPM is blank")
            if scope == "KEY" and not key_raw:
                status = "INVALID"
                messages.append("Scope=KEY but OverrideFinalKey is blank")
            if scope == "BPM_AND_KEY" and not bpm_raw and not key_raw:
                status = "INVALID"
                messages.append("Scope=BPM_AND_KEY but both override fields are blank")

            if scope == "BPM" and bpm_raw and not bpm_valid:
                pass  # already flagged
            if scope == "KEY" and key_raw and not key_valid:
                pass  # already flagged

        # ── Store normalized values ──
        if bpm_valid and bpm_value is not None:
            result["_bpm_value"] = round(bpm_value, 1)
        if key_valid:
            result["_key_camelot"] = key_camelot
            result["_key_name"] = key_name

        if not messages:
            messages.append("OK")

        result["OverrideStatus"] = status
        result["OverrideConflictFlag"] = conflict
        result["OverrideValidationMessage"] = "; ".join(messages)
        results.append(result)

    return results


def write_validation_outputs(results, proof_dir):
    """Write validation summary and results CSV."""
    os.makedirs(proof_dir, exist_ok=True)

    # Summary
    total = len(results)
    valid_count = sum(1 for r in results if r["OverrideStatus"] == "VALID")
    invalid_count = sum(1 for r in results if r["OverrideStatus"] == "INVALID")
    conflict_count = sum(1 for r in results if r["OverrideStatus"] == "CONFLICT")
    pending_count = sum(1 for r in results if r["OverrideStatus"] == "PENDING")

    summary_path = os.path.join(proof_dir, "00_override_validation_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("OVERRIDE VALIDATION SUMMARY\n")
        f.write(f"Date: {datetime.now().isoformat()}\n\n")
        f.write(f"Total override rows: {total}\n")
        f.write(f"  VALID:    {valid_count}\n")
        f.write(f"  INVALID:  {invalid_count}\n")
        f.write(f"  CONFLICT: {conflict_count}\n")
        f.write(f"  PENDING:  {pending_count}\n\n")
        for r in results:
            f.write(f"  Row {r.get('Row','?'):>4s}  {r['OverrideStatus']:10s}  {r['OverrideValidationMessage']}\n")

    # Results CSV
    result_fields = ["Row", "Artist", "Title", "Filename",
                     "OverrideFinalBPM", "OverrideFinalKey",
                     "OverrideScope", "OverrideEnabled",
                     "OverrideStatus", "OverrideConflictFlag",
                     "OverrideValidationMessage"]
    results_path = os.path.join(proof_dir, "01_override_validation_results.csv")
    with open(results_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=result_fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(results)

    return summary_path, results_path, valid_count, invalid_count, conflict_count


if __name__ == "__main__":
    # Standalone validation run
    proof_dir = os.path.join(WORKSPACE, "_proof", "manual_override_system")
    override_path = os.path.join(proof_dir, "NGKs_overrides.csv")
    base_path = os.path.join(WORKSPACE, "_proof", "final_export_schema",
                             "NGKs_final_analyzer_export.csv")

    if not os.path.isfile(override_path):
        print(f"No override file found at {override_path}")
        sys.exit(1)
    if not os.path.isfile(base_path):
        print(f"No base export found at {base_path}")
        sys.exit(1)

    with open(base_path, "r", encoding="utf-8-sig") as f:
        base_rows = list(csv.DictReader(f))
    base_lookup = {r.get("Row", ""): r for r in base_rows}

    with open(override_path, "r", encoding="utf-8-sig") as f:
        override_rows = list(csv.DictReader(f))

    results = validate_overrides(override_rows, base_lookup)
    sp, rp, v, i, c = write_validation_outputs(results, proof_dir)
    print(f"Valid: {v}, Invalid: {i}, Conflict: {c}")
    print(f"Summary: {sp}")
    print(f"Results: {rp}")
