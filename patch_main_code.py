import re

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

# Replace include
text = text.replace('#include "library/dj/DjLibraryWidget.h"', '#include "library/dj/DjLibraryWidget.h"\n#include "dj/browser/DjBrowserPane.h"')

# Replace instance creation
old_layout = '''        auto* libraryPane = new QWidget(page);
        libraryPane->setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Expanding);
        auto* libraryLayout = new QVBoxLayout(libraryPane);
        libraryLayout->setContentsMargins(0, 0, 0, 0);
        auto* djLibrary = new DjLibraryWidget(libraryPane);
        djLibrary->setObjectName("djLibraryWidget");
        djLibrary->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        libraryLayout->addWidget(djLibrary);
        // Note: removed layout->addWidget(libraryPane) as it moves to splitter'''

new_layout = '''        auto* libraryPane = new DjBrowserPane(page);
        libraryPane->setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Expanding);'''

text = text.replace(old_layout, new_layout)

# In case the djLibraryWidget logic was used later, we need to remove the initialization
init_logic = '''        // ── DJ Library ──
        QString err;
        if (!djLibrary->initialize(runtimePath("data/runtime/dj_library.db"), &err)) {
            qWarning() << "DJ Library init failed: " << err;
        }'''

text = text.replace(init_logic, '''        // ── DJ Library (now using DjBrowserPane) ──''')

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print("MAIN_PATCH_COMPLETE")
