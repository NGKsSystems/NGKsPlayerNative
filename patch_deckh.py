import re

path = 'src/ui/DeckStrip.h'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

bad_height = '        setMinimumHeight(120);'
good_height = '        setMinimumHeight(40);'

text = text.replace(bad_height, good_height)

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("DECKSTRIP_H_PATCHED")
