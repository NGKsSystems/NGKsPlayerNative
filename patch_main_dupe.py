import re
with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('''        QString libErr;
        if (!libraryPane->initialize(runtimePath("data/runtime/dj_library.db"), &libErr)) {
            qWarning() << "Browser DB init failed:" << libErr;
        }
        QString libErr;
        if (!libraryPane->initialize(runtimePath("data/runtime/dj_library.db"), &libErr)) {
            qWarning() << "Browser DB init failed:" << libErr;
        }''', '''        QString libErr;
        if (!libraryPane->initialize(runtimePath("data/runtime/dj_library.db"), &libErr)) {
            qWarning() << "Browser DB init failed:" << libErr;
        }''')

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(content)
