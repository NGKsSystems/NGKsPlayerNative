import re
with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()
text = text.replace('auto* sentinel', 'auto* real_sentinel')
text = text.replace('sentinel->', 'real_sentinel->')
text = text.replace('headerRow->addWidget(sentinel)', 'headerRow->addWidget(real_sentinel)')
with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)
print('DONE')
