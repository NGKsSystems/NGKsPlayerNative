import ctypes

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    content = f.read()

old = '        return page;\n    {\n        auto* page = new QWidget(); // djmode-anchor'
new = '        return page;\n    }\n\n    QWidget* buildDjModePage()\n    {\n        auto* page = new QWidget();'

if old not in content:
    # Try without anchor marker
    old2 = '        return page;\n    {\n        auto* page = new QWidget();'
    if old2 in content:
        print('Found basic pattern (no marker)')
        new2 = '        return page;\n    }\n\n    QWidget* buildDjModePage()\n    {\n        auto* page = new QWidget();'
        fixed = content.replace(old2, new2, 1)
    else:
        print('PATTERN NOT FOUND')
        exit(1)
else:
    print('Found marker pattern')
    fixed = content.replace(old, new, 1)

with open('src/ui/main.cpp.new', 'w', encoding='utf-8') as f:
    f.write(fixed)
print('WROTE .new file')

MOVEFILE_REPLACE_EXISTING = 0x1
MOVEFILE_WRITE_THROUGH = 0x8
ok = ctypes.windll.kernel32.MoveFileExW(
    'src/ui/main.cpp.new',
    'src/ui/main.cpp',
    MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH
)
if ok:
    print('RENAME/MOVE OK - disk updated')
else:
    err = ctypes.windll.kernel32.GetLastError()
    print(f'MOVE FAILED err={err}')
    # Check what the error is
    if err == 5:
        print('Error 5 = ACCESS_DENIED')
    elif err == 32:
        print('Error 32 = SHARING_VIOLATION')
    elif err == 183:
        print('Error 183 = ALREADY_EXISTS (replace failed)')
