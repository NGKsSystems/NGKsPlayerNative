with open("src/ui/main.cpp", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace('#include <QApplication>', '#include <QApplication>\n#include "library/DjBrowserPane.h"')

with open("src/ui/main.cpp", "w", encoding="utf-8") as f:
    f.write(text)