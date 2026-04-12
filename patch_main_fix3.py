import re

path = 'src/ui/main.cpp'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

bad_decl = '''    QWidget* buildDjModePage()
    {
        auto* page = new QWidget();
        page->setStyleSheet(QStringLiteral("background: #080b10;"));
        auto* layout = new QVBoxLayout(page);
        layout->setContentsMargins(8, 8, 8, 8);
        layout->setSpacing(4);'''

good_decl = bad_decl + '''

        auto* topHalf = new QWidget(page);
        auto* topLayout = new QVBoxLayout(topHalf);
        topLayout->setContentsMargins(0, 0, 0, 0);'''

if 'auto* topHalf = new QWidget(page);' not in text:
    text = text.replace(bad_decl, good_decl)

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("MAIN_FIXED3")
