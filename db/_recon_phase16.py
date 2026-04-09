#!/usr/bin/env python3
"""Recon for Phase 16 — scan for .tmp files + review dup resolution CSV."""
import csv, os, pathlib
from collections import Counter

BATCH = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")
READY = BATCH / "READY_NORMALIZED"
DATA = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\data")

print("=" * 60)
print("RECON: Phase 16 — TMP Extension Scan + Dedupe Review")
print("=" * 60)

# 1. Scan for .tmp files in ALL intake + READY
tmp_files = []
scan_dirs = [BATCH]
for root, dirs, files in os.walk(BATCH):
    for f in files:
        lower = f.lower()
        if lower.endswith(".tmp.mp3") or lower.endswith(".mp3.tmp") or lower.endswith(".tmp"):
            tmp_files.append(os.path.join(root, f))

print(f"\n.tmp files found: {len(tmp_files)}")
for t in tmp_files[:30]:
    print(f"  {t}")
if len(tmp_files) > 30:
    print(f"  ... and {len(tmp_files) - 30} more")

# 2. Check all subfolders
print(f"\nSubfolders in {BATCH}:")
for d in sorted(BATCH.iterdir()):
    if d.is_dir():
        count = sum(1 for _ in d.iterdir() if _.is_file())
        print(f"  {d.name}: {count} files")

# 3. Review duplicate_resolution_v1.csv
dr = DATA / "duplicate_resolution_v1.csv"
if dr.exists():
    with open(dr, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"\nduplicate_resolution_v1.csv: {len(rows)} rows")
    print(f"  Columns: {list(rows[0].keys())}")

    actions = Counter(r["recommended_action"] for r in rows)
    print(f"  Actions: {dict(actions)}")

    # Identify KEEP vs DELETE under strict mode
    keep = [r for r in rows if r["file_path"] == r["primary_file"]]
    delete = [r for r in rows if r["file_path"] != r["primary_file"]]
    print(f"\n  Strict interpretation:")
    print(f"    KEEP  (file_path == primary_file): {len(keep)}")
    print(f"    DELETE (file_path != primary_file): {len(delete)}")

    # Check how many DELETE targets exist
    exist_count = sum(1 for r in delete if os.path.exists(r["file_path"]))
    missing = sum(1 for r in delete if not os.path.exists(r["file_path"]))
    print(f"    DELETE exist on disk: {exist_count}")
    print(f"    DELETE missing on disk: {missing}")

    # Show some examples
    print(f"\n  Sample DELETE rows:")
    for r in delete[:5]:
        exists = os.path.exists(r["file_path"])
        print(f"    [{r['recommended_action']}] {os.path.basename(r['file_path'])[:50]}")
        print(f"      primary: {os.path.basename(r['primary_file'])[:50]}")
        print(f"      exists: {exists}")

    # Check scope — all paths inside BATCH?
    out_of_scope = [r for r in delete if not r["file_path"].startswith(str(BATCH))]
    print(f"\n  Out-of-scope DELETE paths: {len(out_of_scope)}")
    for r in out_of_scope[:5]:
        print(f"    {r['file_path'][:80]}")

    # Any primary files that would be deleted?
    primary_set = set(r["primary_file"] for r in rows)
    delete_set = set(r["file_path"] for r in delete)
    danger = primary_set & delete_set
    print(f"  Primary files in DELETE set (DANGER): {len(danger)}")
    for d in list(danger)[:5]:
        print(f"    {d[:80]}")
else:
    print(f"\nMISSING: {dr}")

print(f"\nREADY count: {len([f for f in READY.iterdir() if f.is_file()])}")
