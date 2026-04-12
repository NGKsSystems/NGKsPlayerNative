import re

# --- 1. DjBrowserPane.h ---
with open('src/ui/dj/browser/DjBrowserPane.h', 'r', encoding='utf-8') as f:
    text = f.read()

if 'DjLibraryDatabase' not in text:
    text = text.replace('class DjBrowserController;', 'class DjBrowserController;\nclass DjLibraryDatabase;')
    text = text.replace('public:\n    explicit DjBrowserPane(QWidget* parent = nullptr);', 'public:\n    explicit DjBrowserPane(QWidget* parent = nullptr);\n    bool initialize(const QString& dbPath, QString* errorOut = nullptr);')
    text = text.replace('private:', 'private:\n    DjLibraryDatabase* db_;')

with open('src/ui/dj/browser/DjBrowserPane.h', 'w', encoding='utf-8') as f:
    f.write(text)

# --- 2. DjBrowserPane.cpp ---
with open('src/ui/dj/browser/DjBrowserPane.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

if 'DjLibraryDatabase' not in text:
    text = text.replace('#include "DjBrowserController.h"', '#include "DjBrowserController.h"\n#include "../../library/dj/DjLibraryDatabase.h"')
    text = text.replace('layout->setSpacing(4);', 'layout->setSpacing(4);\n\n    db_ = new DjLibraryDatabase();')
    text = text.replace('controller_ = new DjBrowserController(searchBar_, sourceTree_, trackTable_, this);', 'controller_ = new DjBrowserController(searchBar_, sourceTree_, trackTable_, db_, this);')
    
    text += "\n\nbool DjBrowserPane::initialize(const QString& dbPath, QString* errorOut) {\n    return db_->open(dbPath, errorOut);\n}\n"

with open('src/ui/dj/browser/DjBrowserPane.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print("Pane updated")
