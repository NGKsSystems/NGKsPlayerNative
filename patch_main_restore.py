import re

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

# I need to change back root to layout if it broke layout-> declarations inside buildDjModePage. 
# Wait, let's see if there are any layout->addWidget calls in buildDjModePage.
# Yes, topLayout->addLayout(deckRow, 1);
# Wait, was it layout->addWidget(djDeviceLostBanner_) ?
# Yes! layout->addWidget(djDeviceLostBanner_); is at the end.
# If I changed uto* layout to uto* root, layout is undefined.

text = text.replace('auto* root = new QVBoxLayout(page);', 'auto* layout = new QVBoxLayout(page);')
text = text.replace('root->setContentsMargins(0, 0, 0, 0);', 'layout->setContentsMargins(0, 0, 0, 0);')
text = text.replace('root->setSpacing(0);', 'layout->setSpacing(0);')
text = text.replace('root->addWidget(splitter, 1);', 'layout->addWidget(splitter, 1);')

# Also, the instruction said:
# "Remove the previous temporary sentinel after proving this build is live, unless it is still needed for verification."
# The sentinel is:
# auto* sentinel_truth = new QLabel(QStringLiteral("REAL MAIN CPP REBUILD"), page);
# sentinel_truth->setStyleSheet(QStringLiteral("background: magenta; color: white; padding: 4px; font-weight: bold; border-radius: 4px;"));
# headerRow->addWidget(sentinel_truth);
text = re.sub(
    r'\s+auto\* sentinel_truth = new QLabel\(QStringLiteral\("REAL MAIN CPP REBUILD"\), page\);\s+sentinel_truth->setStyleSheet\(QStringLiteral\("background: magenta; color: white; padding: 4px; font-weight: bold; border-radius: 4px;"\)\);\s+headerRow->addWidget\(sentinel_truth\);',
    '',
    text
)

# And remove any explicit stretch added to libraryPane just in case it is still there.
# Oh, we already removed layout->addWidget(libraryPane, 3);

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print("FIX APPLIED")
