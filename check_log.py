import os
import sys

def check_log():
    try:
        with open('build_graph/release/bin/data/runtime/ui_qt.log', 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            for line in lines[-1000:]:
                if "DECK_" in line or "DRAG" in line:
                    print(line.strip())
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    check_log()
