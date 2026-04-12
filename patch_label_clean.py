import re

with open('src/ui/library/dj/DjLibraryWidget.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

text = re.sub(r'\s+searchEdit_->setStyleSheet\(QStringLiteral\("background: magenta; color: white; border: 2px solid #ff00ff; font-weight: bold;"\)\);', '', text)

with open('src/ui/library/dj/DjLibraryWidget.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

text = re.sub(r'\s+libraryPane->setStyleSheet\("background: rgba\(255,0,0,40\);"\);', '', text)

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print("CLEANUP APPLIED")
