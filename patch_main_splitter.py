import re

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Add red background to libraryPane temporary validation
text = text.replace(
    'auto* libraryPane = new QWidget(page);\n        libraryPane->setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Expanding);',
    'auto* libraryPane = new QWidget(page);\n        libraryPane->setSizePolicy(QSizePolicy::Preferred, QSizePolicy::Expanding);\n        libraryPane->setStyleSheet("background: rgba(255,0,0,40);");'
)

# 2. Fix stretch factor dominance
text = text.replace(
    'splitter->setStretchFactor(0, 0);',
    'splitter->setStretchFactor(0, 1);'
)
text = text.replace(
    'splitter->setStretchFactor(1, 1);',
    'splitter->setStretchFactor(1, 3);'
)

# 3. Strip layout->addWidget(djDeviceLostBanner_) and replace with Option A overlay
text = text.replace(
    'layout->addWidget(djDeviceLostBanner_);',
    'djDeviceLostBanner_->setParent(splitter);\n        djDeviceLostBanner_->raise();'
)

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print("SURGERY COMPLETE")
