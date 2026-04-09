import sys

path = sys.argv[1] if len(sys.argv) > 1 else '_proof/modularization_pass_01/build_out2.txt'

with open(path, encoding='utf-8', errors='replace') as f:
    lines = f.readlines()

print(f'Total lines: {len(lines)}')
# Find lines with relevant patterns
for i, l in enumerate(lines):
    s = l.rstrip()
    if not s: continue
    if any(x in s for x in ['DiagnosticsDialog', 'RuntimeLogSupport', 'LibraryPersistence', 'LibraryScanner', 'VisualizerWidget', 'AudioProfileStore', 'LegacyLibrary']):
        if any(y in s for y in [': error ', ': warning ', 'fatal error', 'error C']):
            print(f'L{i+1}: {s[:300]}')

# Also show all errors
print('\n=== ALL ERRORS ===')
for i, l in enumerate(lines):
    s = l.rstrip()
    if ': error C' in s or 'fatal error' in s.lower() or 'error LNK' in s:
        print(f'L{i+1}: {s[:300]}')
