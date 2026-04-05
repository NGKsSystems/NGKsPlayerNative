"""Quick test: verify analysis phase tracking is reported correctly."""
import json
import subprocess
import sys
import time
import os

TRACK = r"C:\Users\suppo\Music\3 Doors Down - Here Without You.mp3"
CACHE = r"analysis_cache\3 Doors Down - Here Without You.analysis.json"

# Remove cache for fresh analysis
if os.path.exists(CACHE):
    os.remove(CACHE)
    print("Removed cache entry")

proc = subprocess.Popen(
    [sys.executable, r"src\analysis\analysis_ipc_server.py"],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    bufsize=1,
)

ready = json.loads(proc.stdout.readline())
print(f"READY pid={ready.get('pid')}")

proc.stdin.write(json.dumps({"cmd": "track_selected", "filepath": TRACK}) + "\n")
proc.stdin.flush()
r = json.loads(proc.stdout.readline())
p = r["panel"]
print(f"t=0  state={p['state']:25s} status_text={p['status_text']:30s} progress_text={p['progress_text']}")

for i in range(90):
    time.sleep(1)
    proc.stdin.write(json.dumps({"cmd": "poll"}) + "\n")
    proc.stdin.flush()
    r = json.loads(proc.stdout.readline())
    p = r["panel"]
    print(f"t={i+1:<3d} state={p['state']:25s} status_text={p['status_text']:30s} progress_text={p['progress_text']}")
    if p["state"] in ("ANALYSIS_COMPLETE", "ANALYSIS_FAILED"):
        break

proc.stdin.write(json.dumps({"cmd": "shutdown"}) + "\n")
proc.stdin.flush()
proc.wait(timeout=5)
print("DONE")
