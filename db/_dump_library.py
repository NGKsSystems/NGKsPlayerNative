import sqlite3
from pathlib import Path
from datetime import date

db = Path(__file__).resolve().parent / "song_analysis.db"
out = Path(__file__).resolve().parent.parent / "data" / "library_contents.txt"

conn = sqlite3.connect(str(db))
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT id, artist, title, file_path FROM tracks ORDER BY id").fetchall()
conn.close()

lines = []
lines.append("NGKsPlayerNative - Full Library Contents")
lines.append(f"Total tracks: {len(rows)}")
lines.append(f"Generated: {date.today().isoformat()}")
lines.append("")
hdr = f"{'ID':>5s}  {'Artist':30s}  {'Title':50s}  File Path"
lines.append(hdr)
lines.append("-" * 160)
for r in rows:
    artist = (r["artist"] or "Unknown")[:30]
    title = (r["title"] or "Unknown")[:50]
    fp = r["file_path"] or ""
    lines.append(f"{r['id']:5d}  {artist:30s}  {title:50s}  {fp}")

out.write_text("\n".join(lines), encoding="utf-8")
print(f"Written {len(rows)} tracks to {out}")
