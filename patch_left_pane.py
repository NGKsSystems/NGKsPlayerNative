import re

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Change QVBoxLayout to QHBoxLayout for the page, rename it or keep it as layout
text = re.sub(
    r'auto\* layout = new QVBoxLayout\(page\);\s*layout->setContentsMargins\(8, 8, 8, 8\);\s*layout->setSpacing\(4\);',
    r'''auto* layout = new QHBoxLayout(page);
        layout->setContentsMargins(0, 0, 0, 0);
        layout->setSpacing(0);''',
    text
)

# 2. Add libraryPane at the start of buildDjModePage, right before topHalf creation
# Wait, let's inject it right after layout creation
text = re.sub(
    r'auto\* topHalf = new QWidget\(page\);',
    r'''auto* libraryPane = new QWidget(page);
        libraryPane->setMinimumWidth(320);
        libraryPane->setMaximumWidth(600);
        auto* libraryLayout = new QVBoxLayout(libraryPane);
        libraryLayout->setContentsMargins(0, 0, 0, 0);
        auto* djLibrary = new DjLibraryWidget(libraryPane);
        djLibrary->setObjectName("djLibraryWidget");
        djLibrary->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        libraryLayout->addWidget(djLibrary);
        layout->addWidget(libraryPane, 3);

        auto* topHalf = new QWidget(page);
        topHalf->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);''',
    text
)

# 3. Add topHalf to the layout (we replaced the bottom container, so let's strip the bottom container and add topHalf)
old_bottom = r'''auto\* djLibrary = new DjLibraryWidget\(page\);\s*djLibrary->setObjectName\("djLibraryWidget"\);\s*QString err;\s*if \(!djLibrary->initialize\(runtimePath\("data/runtime/dj_library\.db"\), &err\)\) \{\s*qWarning\(\) << "DJ Library init failed: " << err;\s*\}\s*layout->addWidget\(topHalf, 0\);\s*auto\* djLibraryContainer = new QWidget\(page\);\s*djLibraryContainer->setObjectName\("djLibraryContainer"\);\s*auto\* djLibLayout = new QVBoxLayout\(djLibraryContainer\);\s*djLibLayout->setContentsMargins\(0, 0, 0, 0\);\s*djLibLayout->addWidget\(djLibrary, 1\);\s*layout->addWidget\(djLibraryContainer, 1\);'''

new_bottom = r'''QString err;
        if (!djLibrary->initialize(runtimePath("data/runtime/dj_library.db"), &err)) {
            qWarning() << "DJ Library init failed: " << err;
        }
        layout->addWidget(topHalf, 7);'''

text = re.sub(old_bottom, new_bottom, text)

# 4. Remove aggressive height constraints
# "ToRemove: setFixedHeight, setMaximumHeight, aggressive setMinimumHeight on containers. DO NOT touch knobs/waveforms unless required."
# TopHalf has no fixed height, but we need to make sure we don't break simple player.
# We should only strip constraints inside buildDjModePage. 
# We'll do this in a second pass if needed, let's verify text first.

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print("PATCH APPLIED")
