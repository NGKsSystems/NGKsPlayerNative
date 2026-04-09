#!/usr/bin/env python3
"""
Music Library Index Generator
=============================
Scans all subdirectories under a root music folder and produces
a single UTF-8 CSV index of every audio file found.

Columns:
  folder        — genre/subfolder name
  filename      — file name on disk
  extension     — lowercase extension (.mp3, .flac, etc.)
  size_bytes    — file size in bytes
  size_mb       — file size in megabytes (2 decimal places)

Output: data/music_index.csv
"""

import csv
import os
from datetime import datetime
from pathlib import Path

MUSIC_ROOT = Path(r"C:\Users\suppo\Downloads\New Music")
OUTPUT     = Path(r"C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\data\music_index.csv")

# Audio extensions to index (everything else like .png/.jpg is skipped)
AUDIO_EXTS = {".mp3", ".flac", ".wav", ".m4a", ".ogg", ".wma", ".aac", ".opus", ".webm"}


def scan(root: Path):
    rows = []
    for folder in sorted(root.iterdir()):
        if not folder.is_dir():
            continue
        for f in sorted(folder.iterdir()):
            if not f.is_file():
                continue
            ext = f.suffix.lower()
            if ext not in AUDIO_EXTS:
                continue
            size = f.stat().st_size
            rows.append({
                "folder":     folder.name,
                "filename":   f.name,
                "extension":  ext,
                "size_bytes": size,
                "size_mb":    round(size / (1024 * 1024), 2),
            })
    return rows


def write_csv(rows):
    cols = ["folder", "filename", "extension", "size_bytes", "size_mb"]
    with open(OUTPUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def main():
    print(f"Scanning: {MUSIC_ROOT}")
    rows = scan(MUSIC_ROOT)

    write_csv(rows)
    print(f"Wrote {OUTPUT}  ({len(rows)} audio files)")

    # Summary
    from collections import Counter
    by_folder = Counter(r["folder"] for r in rows)
    total_mb = sum(r["size_mb"] for r in rows)
    print(f"\nTotal size: {total_mb:,.1f} MB")
    print(f"{'Folder':<35} {'Files':>6}")
    print("-" * 43)
    for folder, count in sorted(by_folder.items()):
        print(f"{folder:<35} {count:>6}")


if __name__ == "__main__":
    main()
