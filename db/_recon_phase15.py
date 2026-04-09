#!/usr/bin/env python3
"""Recon for Phase 15 — inspect key data sources for decision CSV generation."""
import csv, os, pathlib
from collections import Counter

DATA = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\data")

def peek(name, max_rows=5):
    p = DATA / name
    if not p.exists():
        print(f"\n  MISSING: {name}")
        return []
    with open(p, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"\n  {name}: {len(rows)} rows")
    if rows:
        print(f"    Columns: {list(rows[0].keys())}")
        for i, r in enumerate(rows[:max_rows]):
            vals = {k: str(v)[:40] for k, v in r.items()}
            print(f"    [{i}] {vals}")
    return rows

print("=" * 60)
print("RECON: Phase 15 Data Sources")
print("=" * 60)

# True duplicate pack
td = peek("true_duplicate_review_pack_v1.csv", 3)

# True duplicate resolution (source data)
tdr = peek("true_duplicate_resolution_v1.csv", 3)

# Duplicate primary selection
dps = peek("duplicate_primary_selection_v1.csv", 3)

# Duplicate state
ds = peek("duplicate_state_v1.csv", 3)

# Duplicate alternate plan
dap = peek("duplicate_alternate_plan_v1.csv", 3)

# Near duplicate groups
ndg = peek("near_duplicate_groups_v2.csv", 3)

# Destination conflict review pack
dc = peek("destination_conflict_review_pack_v1.csv", 3)

# Manual review pack (63 rows)
mr = peek("manual_review_pack_v1.csv", 3)

# Held classification
hc = peek("held_classification_v1.csv", 3)
if hc:
    cats = Counter(r.get("classification", "?") for r in hc)
    print(f"    Classification dist: {dict(sorted(cats.items(), key=lambda x: -x[1]))}")

# Held problem breakdown
hpb = peek("held_problem_breakdown_v1.csv", 3)
if hpb:
    issues = Counter(r.get("issue_type", "?") for r in hpb)
    print(f"    Issue types: {dict(sorted(issues.items(), key=lambda x: -x[1]))}")

# No-parse recovery
npr = peek("no_parse_recovery_v1.csv", 3)

# Low-confidence recovery
lcr = peek("low_confidence_recovery_v1.csv", 3)

# Review required clean
rrc = peek("review_required_clean_v1.csv", 3)

# Held rows
hr = peek("held_rows.csv", 3)

# Batch normalization plan (for original file list)
bnp = peek("batch_normalization_plan.csv", 2)

# Candidate recovery
cr = peek("candidate_recovery_v1.csv", 3)

# Collision resolution plan
crp = peek("collision_resolution_plan_v1.csv", 3)

print(f"\n{'=' * 60}")
print("READY on disk:", len([f for f in pathlib.Path(r"C:\Users\suppo\Downloads\New Music\READY_NORMALIZED").iterdir() if f.is_file()]))
