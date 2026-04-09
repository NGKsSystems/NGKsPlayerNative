#!/usr/bin/env python3
"""Deep investigation of duplicate_resolution_v1.csv — safety audit before deletions."""
import csv, os, pathlib, hashlib
from collections import Counter, defaultdict

DATA = pathlib.Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\data")
BATCH = pathlib.Path(r"C:\Users\suppo\Downloads\New Music")

dr = DATA / "duplicate_resolution_v1.csv"
with open(dr, encoding="utf-8") as f:
    rows = list(csv.DictReader(f))

delete_rows = [r for r in rows if r["file_path"] != r["primary_file"]]

print("=" * 60)
print("SAFETY AUDIT: DELETE rows in duplicate_resolution_v1.csv")
print(f"Total: {len(delete_rows)} DELETE candidates")
print("=" * 60)

# Group by primary_file to see if groups contain obviously different songs
primary_groups = defaultdict(list)
for r in delete_rows:
    primary_groups[r["primary_file"]].append(r)

print(f"\n{len(primary_groups)} primary groups with DELETE targets")

# Check for suspicious groups: different base names mapped to same primary
suspicious = []
safe = []
for pf, members in primary_groups.items():
    pf_base = os.path.splitext(os.path.basename(pf))[0].lower().strip()
    for m in members:
        mf_base = os.path.splitext(os.path.basename(m["file_path"]))[0].lower().strip()
        # Check if the names are substantially different
        # Simple heuristic: if neither name contains the other and they differ by more than minor suffix
        pf_words = set(pf_base.split())
        mf_words = set(mf_base.split())
        common = pf_words & mf_words
        total = pf_words | mf_words
        if total:
            overlap = len(common) / len(total)
        else:
            overlap = 0
        if overlap < 0.3:
            suspicious.append({
                "delete_file": m["file_path"],
                "delete_name": os.path.basename(m["file_path"]),
                "primary_file": pf,
                "primary_name": os.path.basename(pf),
                "overlap": f"{overlap:.2f}",
                "action": m["recommended_action"],
                "notes": m.get("notes", "")[:60],
            })
        else:
            safe.append(m)

print(f"\nSAFE (name overlap >= 0.3): {len(safe)}")
print(f"SUSPICIOUS (name overlap < 0.3): {len(suspicious)}")

if suspicious:
    print(f"\n--- SUSPICIOUS ROWS (different songs being deleted as dups) ---\n")
    for i, s in enumerate(suspicious, 1):
        print(f"  {i}. DELETE: {s['delete_name'][:55]}")
        print(f"     PRIMARY: {s['primary_name'][:55]}")
        print(f"     Overlap: {s['overlap']}")
        print(f"     Notes: {s['notes']}")
        print()

# Also check the near_duplicate_groups source
ndg = DATA / "near_duplicate_groups_v2.csv"
if ndg.exists():
    with open(ndg, encoding="utf-8") as f:
        nd_rows = list(csv.DictReader(f))
    # Check group_type distribution
    types = Counter(r.get("group_type", "?") for r in nd_rows)
    print(f"\nNear-duplicate group types: {dict(types)}")

    # Check which groups contain clearly different songs
    nd_groups = defaultdict(list)
    for r in nd_rows:
        nd_groups[r["group_id"]].append(r)

    diff_groups = 0
    for gid, members in nd_groups.items():
        names = [r.get("original_name", "") for r in members]
        if len(names) >= 2:
            n1 = set(names[0].lower().split())
            n2 = set(names[1].lower().split())
            common = n1 & n2
            total = n1 | n2
            if total and len(common) / len(total) < 0.3:
                diff_groups += 1
    print(f"Near-dup groups with different-sounding names: {diff_groups}/{len(nd_groups)}")

# Also check: do any DELETE targets have different file hashes from their primary?
print(f"\n--- HASH VERIFICATION (sample) ---")
checked = 0
hash_mismatch = 0
hash_match = 0
for r in delete_rows[:30]:
    fp = r["file_path"]
    pf = r["primary_file"]
    if os.path.exists(fp) and os.path.exists(pf):
        # Quick size check first
        fp_size = os.path.getsize(fp)
        pf_size = os.path.getsize(pf)
        checked += 1
        if fp_size != pf_size:
            hash_mismatch += 1
            if checked <= 10:
                print(f"  SIZE DIFF: {os.path.basename(fp)[:40]} ({fp_size}) vs {os.path.basename(pf)[:40]} ({pf_size})")
        else:
            hash_match += 1

print(f"\nSize-checked: {checked}")
print(f"  Same size: {hash_match}")
print(f"  Different size: {hash_mismatch}")
print(f"\nCONCLUSION: {hash_mismatch} DELETE targets differ in size from primary → NOT true duplicates")
