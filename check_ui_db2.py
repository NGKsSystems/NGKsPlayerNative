import sqlite3
import os

db_path = r'build_graph\release\bin\data\runtime\ngks_library.db'
if not os.path.exists(db_path):
    print("Database not found at:", db_path)
else:
    db = sqlite3.connect(db_path)
    cur = db.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    print("Tables:", cur.fetchall())
    
    try:
        cur.execute("SELECT title, duration_str FROM library_tracks LIMIT 10")
        rows = cur.fetchall()
        for row in rows:
            print(row[0], "->", row[1])
    except Exception as e:
        print("Error reading tracks:", e)
