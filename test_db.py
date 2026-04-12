import sqlite3

db_path = r"c:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\data\dj_library_core.db"
conn = sqlite3.connect(db_path)
cur = conn.execute("PRAGMA table_info(tracks)")
cols = cur.fetchall()
print([c[1] for c in cols])
print(conn.execute("SELECT duration FROM tracks LIMIT 5").fetchall())
