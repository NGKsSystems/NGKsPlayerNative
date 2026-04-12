import re
with open('src/ui/dj/browser/DjSourceTreeWidget.cpp', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('fileModel_->setRootPath(QString());', 'fileModel_->setRootPath("C:/Users/suppo");')
content = content.replace('setModel(fileModel_);', 'setModel(fileModel_);\n    setRootIndex(fileModel_->index("C:/Users/suppo"));')

with open('src/ui/dj/browser/DjSourceTreeWidget.cpp', 'w', encoding='utf-8') as f:
    f.write(content)
