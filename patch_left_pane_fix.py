import re

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

# Fix the duplicate insertion
dup_pattern = r'''        auto\* libraryPane = new QWidget\(page\);
        libraryPane->setMinimumWidth\(320\);
        libraryPane->setMaximumWidth\(600\);
        auto\* libraryLayout = new QVBoxLayout\(libraryPane\);
        libraryLayout->setContentsMargins\(0, 0, 0, 0\);
        auto\* djLibrary = new DjLibraryWidget\(libraryPane\);
        djLibrary->setObjectName\("djLibraryWidget"\);
        djLibrary->setSizePolicy\(QSizePolicy::Expanding, QSizePolicy::Expanding\);
        libraryLayout->addWidget\(djLibrary\);
        layout->addWidget\(libraryPane, 3\);

        auto\* libraryPane = new QWidget\(page\);
        libraryPane->setMinimumWidth\(320\);
        libraryPane->setMaximumWidth\(600\);
        auto\* libraryLayout = new QVBoxLayout\(libraryPane\);
        libraryLayout->setContentsMargins\(0, 0, 0, 0\);
        auto\* djLibrary = new DjLibraryWidget\(libraryPane\);
        djLibrary->setObjectName\("djLibraryWidget"\);
        djLibrary->setSizePolicy\(QSizePolicy::Expanding, QSizePolicy::Expanding\);
        libraryLayout->addWidget\(djLibrary\);
        layout->addWidget\(libraryPane, 3\);

        auto\* topHalf = new QWidget\(page\);
        topHalf->setSizePolicy\(QSizePolicy::Expanding, QSizePolicy::Expanding\);
        topHalf->setSizePolicy\(QSizePolicy::Expanding, QSizePolicy::Expanding\);'''

correct_pattern = r'''        auto* libraryPane = new QWidget(page);
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
        topHalf->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);'''

if re.search(dup_pattern, text):
    text = re.sub(dup_pattern, correct_pattern, text)
    print("DUPLICATE FIXED")
else:
    print("DUPLICATE NOT FOUND")

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

