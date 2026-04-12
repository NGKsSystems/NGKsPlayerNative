lines = []
with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    lines = f.readlines()

start = 0
for i, l in enumerate(lines):
    if "auto* djLibrary = new DjLibraryWidget(page);" in l:
        start = i
        break
    if "auto* djLibraryContainer = new QWidget(page);" in l:
        start = i
        break
    if "QString err;" in l and "djLibrary" in lines[i+1]:
        start = i
        break

print(''.join(lines[start:min(start+20, len(lines))]))
