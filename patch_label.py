import re
with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()
text = text.replace('        headerRow->addSpacing(60);  // balance the back button', '        auto* sentinel = new QLabel(QStringLiteral("REAL MAIN CPP REBUILD"), page);\n        sentinel->setStyleSheet(QStringLiteral("background: magenta; color: white; padding: 4px; font-weight: bold; border-radius: 4px;"));\n        headerRow->addWidget(sentinel);\n\n        headerRow->addSpacing(60);  // balance the back button')
with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)
print('DONE')
