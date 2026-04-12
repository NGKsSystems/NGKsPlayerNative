import re

path = 'src/ui/main.cpp'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

# Add necessary includes if missing
if '<QTableView>' not in text:
    text = '#include <QTableView>\n' + text
if 'DjLibraryWidget.h' not in text:
    text = '#include "library/dj/DjLibraryWidget.h"\n' + text

# Define topHalf and topLayout
bad_decl = '''    QWidget* buildDjModePage()
    {
        auto* page = new QWidget();
        page->setStyleSheet(QStringLiteral("background: #080b10;"));       
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(8, 8, 8, 8);
        layout->setSpacing(4);'''

good_decl = '''    QWidget* buildDjModePage()
    {
        auto* page = new QWidget();
        page->setStyleSheet(QStringLiteral("background: #080b10;"));       
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(8, 8, 8, 8);
        layout->setSpacing(4);

        auto* topHalf = new QWidget(page);
        auto* topLayout = new QVBoxLayout(topHalf);
        topLayout->setContentsMargins(0, 0, 0, 0);'''

if 'auto* topHalf = new QWidget(page);' not in text:
    text = text.replace(bad_decl, good_decl)

# Fix layout -> topLayout in specific places inside buildDjModePage before mainSplitter
# Since we only want to change the ones related to topHalf, we find headerRow and deckRow adds.

text = text.replace('layout->addLayout(headerRow);', 'topLayout->addLayout(headerRow);')
text = text.replace('layout->addLayout(deckRow, 1);', 'topLayout->addLayout(deckRow, 1);')

# The crossfader was changed previously from layout to topLayout by the regex, so it's already topLayout->addLayout(xfadeRow);

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("MAIN_FIXED")
