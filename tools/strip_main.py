with open('src/ui/main.cpp', encoding='utf-8') as f:
    lines = f.readlines()

# Indices (0-based):
# 80 = DiagLog.h include
# 82-88 = NGKS macro guards
# 90-95 = forward declarations (remove)
# 96 = blank (remove)
# 97 = 'namespace {' (keep, but remove content 98-1844)
# 1845 = class MainWindow (keep)
# 5060-5159 = bottom functions (remove)

new_includes = [
    '',
    '#include "ui/diagnostics/RuntimeLogSupport.h"',
    '#include "ui/library/LibraryPersistence.h"',
    '#include "ui/library/LibraryScanner.h"',
    '#include "ui/library/LegacyLibraryImport.h"',
    '#include "ui/audio/AudioProfileStore.h"',
    '#include "ui/diagnostics/DiagnosticsDialog.h"',
    '#include "ui/widgets/VisualizerWidget.h"',
    '',
]

out = []
# Part 1: lines 0-80 (Qt includes + DiagLog)
for x in lines[0:81]:
    out.append(x.rstrip('\n').rstrip('\r'))
# New includes block
out.extend(new_includes)
# Part 2: NGKS macro guards (lines 82-88, skipping blank line 81)
for x in lines[82:89]:
    out.append(x.rstrip('\n').rstrip('\r'))
# Part 3: namespace opener
out.append('')
out.append('namespace {')
out.append('')
# Part 4: MainWindow + namespace close + main() (lines 1845-5059 inclusive)
for x in lines[1845:5060]:
    out.append(x.rstrip('\n').rstrip('\r'))

with open('src/ui/main.cpp', 'w', encoding='utf-8', newline='\n') as f:
    f.write('\n'.join(out) + '\n')
print(f'Done. New line count: {len(out)}')
