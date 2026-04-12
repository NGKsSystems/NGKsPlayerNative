import re

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

text = re.sub(r'QSplitter\* mainSplitter = page->findChild<QSplitter\*>\(\);\s*if \(!mainSplitter\) return;\s*QWidget\* djLib = mainSplitter->widget\(1\);', r'QWidget* djLib = page->findChild<QWidget*>("djLibraryWidget");\n                  if (!djLib) return;', text)

text = re.sub(r'auto\* djLibrary = new DjLibraryWidget\(page\);\s*QString err;\s*if \(!djLibrary->initialize\(runtimePath\("data/runtime/dj_library\.db"\), &err\)\) \{\s*qWarning\(\) << "DJ Library init failed: " << err;\s*\}\s*auto\* mainSplitter = new QSplitter\(Qt::Vertical, page\);\s*mainSplitter->setObjectName\("pageSplitter"\);\s*mainSplitter->setChildrenCollapsible\(false\);\s*mainSplitter->addWidget\(topHalf\);\s*mainSplitter->addWidget\(djLibrary\);\s*mainSplitter->setStretchFactor\(0, 1\);\s*mainSplitter->setStretchFactor\(1, 2\);\s*QList<int> initSizes;\s*initSizes << 300 << 600;\s*mainSplitter->setSizes\(initSizes\);\s*layout->addWidget\(mainSplitter, 1\);', r'auto* djLibrary = new DjLibraryWidget(page);\n        djLibrary->setObjectName("djLibraryWidget");\n        QString err;\n        if (!djLibrary->initialize(runtimePath("data/runtime/dj_library.db"), &err)) {\n            qWarning() << "DJ Library init failed: " << err;\n        }\n\n        layout->addWidget(topHalf, 0);\n        auto* djLibraryContainer = new QWidget(page);\n        djLibraryContainer->setObjectName("djLibraryContainer");\n        auto* djLibLayout = new QVBoxLayout(djLibraryContainer);\n        djLibLayout->setContentsMargins(0, 0, 0, 0);\n        djLibLayout->addWidget(djLibrary, 1);\n        layout->addWidget(djLibraryContainer, 1);', text)

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print("DONE REGEX")
