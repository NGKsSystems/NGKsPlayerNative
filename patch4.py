with open("src/ui/main.cpp", "r", encoding="utf-8") as f:
    text = f.read()
text = text.replace('#include "library/LibraryBrowserWidget.h"', '#include "library/LibraryBrowserWidget.h"\n#include "library/DjBrowserPane.h"')
with open("src/ui/main.cpp", "w", encoding="utf-8") as f:
    f.write(text)