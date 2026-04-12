import re

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Root update: QHBoxLayout to QVBoxLayout, rename variable to root
text = re.sub(
    r'''auto\* layout = new QHBoxLayout\(page\);\s*layout->setContentsMargins\(0, 0, 0, 0\);\s*layout->setSpacing\(0\);''',
    r'''auto* root = new QVBoxLayout(page);
        root->setContentsMargins(0, 0, 0, 0);
        root->setSpacing(0);''',
    text
)

# 2. libraryPane setup: remove width max/min and direct addWidget, update SizePolicy
# We'll replace the block that builds libraryPane.
old_lib_pane = r'''auto\* libraryPane = new QWidget\(page\);\s*libraryPane->setMinimumWidth\(320\);\s*libraryPane->setMaximumWidth\(600\);\s*auto\* libraryLayout = new QVBoxLayout\(libraryPane\);\s*libraryLayout->setContentsMargins\(0, 0, 0, 0\);\s*auto\* djLibrary = new DjLibraryWidget\(libraryPane\);\s*djLibrary->setObjectName\("djLibraryWidget"\);\s*djLibrary->setSizePolicy\(QSizePolicy::Expanding, QSizePolicy::Expanding\);\s*libraryLayout->addWidget\(djLibrary\);\s*layout->addWidget\(libraryPane, 3\);'''
new_lib_pane = r'''auto* libraryPane = new QWidget(page);
        libraryPane->setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Expanding);
        auto* libraryLayout = new QVBoxLayout(libraryPane);
        libraryLayout->setContentsMargins(0, 0, 0, 0);
        auto* djLibrary = new DjLibraryWidget(libraryPane);
        djLibrary->setObjectName("djLibraryWidget");
        djLibrary->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        libraryLayout->addWidget(djLibrary);
        // Note: removed layout->addWidget(libraryPane) as it moves to splitter'''
text = re.sub(old_lib_pane, new_lib_pane, text)

# 3. Handle bottom connection 
old_bottom = r'''QString err;\s*if \(!djLibrary->initialize\(runtimePath\("data/runtime/dj_library\.db"\), &err\)\) \{\s*qWarning\(\) << "DJ Library init failed: " << err;\s*\}\s*layout->addWidget\(topHalf, 7\);'''

new_bottom = r'''QString err;
        if (!djLibrary->initialize(runtimePath("data/runtime/dj_library.db"), &err)) {
            qWarning() << "DJ Library init failed: " << err;
        }
        
        auto* mixerPane = new QWidget(page);
        mixerPane->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
        auto* mixerLayout = new QVBoxLayout(mixerPane);
        mixerLayout->setContentsMargins(0, 0, 0, 0);
        mixerLayout->addWidget(topHalf, 1);

        auto* splitter = new QSplitter(Qt::Horizontal, page);
        splitter->addWidget(libraryPane);
        splitter->addWidget(mixerPane);
        
        QList<int> sizes;
        sizes << 420 << 1180;
        splitter->setSizes(sizes);
        
        splitter->setStretchFactor(0, 0);
        splitter->setStretchFactor(1, 1);
        
        root->addWidget(splitter, 1);'''

text = re.sub(old_bottom, new_bottom, text)

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print("PATCH APPLIED")
