import re

path = 'src/ui/main.cpp'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Fix compile error (un-escaped newlines in string literals)
# We find the QTimer block:
bad_block = '''                QString metrics = QString("dj_page_height_px=%1
dj_library_region_height_px=%2
dj_library_table_viewport_height_px=%3
").arg(pHeight).arg(lHeight).arg(tvpH);
                QFile f("C:\\Users\\suppo\\Desktop\\NGKsSystems\\NGKsPlayerNative\\data\\runtime\\metrics.txt");
                std::cout << "MEASUREMENTS: " << metrics.toStdString() << "
";'''

good_block = '''                QString metrics = QString("dj_page_height_px=%1\\ndj_library_region_height_px=%2\\ndj_library_table_viewport_height_px=%3\\n").arg(pHeight).arg(lHeight).arg(tvpH);
                QFile f("C:\\\\Users\\\\suppo\\\\Desktop\\\\NGKsSystems\\\\NGKsPlayerNative\\\\data\\\\runtime\\\\metrics.txt");
                std::cout << "MEASUREMENTS: " << metrics.toStdString() << "\\n";'''

# It's better to just regex the specific bad lines:
text = re.sub(
    r'QString metrics = QString\("dj_page_height_px=%1\n.*?\"\)',
    r'QString metrics = QString("dj_page_height_px=%1\\ndj_library_region_height_px=%2\\ndj_library_table_viewport_height_px=%3\\n")',
    text,
    flags=re.DOTALL
)

text = re.sub(
    r'QFile f\("C:\\Users\\suppo\\Desktop\\NGKsSystems\\NGKsPlayerNative\\data\\runtime\\metrics\.txt"\);',
    r'QFile f("C:\\\\Users\\\\suppo\\\\Desktop\\\\NGKsSystems\\\\NGKsPlayerNative\\\\data\\\\runtime\\\\metrics.txt");',
    text
)

text = re.sub(
    r'std::cout << "MEASUREMENTS: " << metrics\.toStdString\(\) << "\n";',
    r'std::cout << "MEASUREMENTS: " << metrics.toStdString() << "\\n";',
    text
)

# 2. Add Sentinel
sentinel_code = '''          title->setAlignment(Qt::AlignCenter);
          headerRow->addWidget(title, 1);
          
          auto* sentinel = new QLabel(QStringLiteral("BUILD SENTINEL ACTIVE"), page);
          sentinel->setStyleSheet(QStringLiteral("background: magenta; color: white; padding: 4px; font-weight: bold; border-radius: 4px;"));
          headerRow->addWidget(sentinel);'''

if "BUILD SENTINEL ACTIVE" not in text:
    text = text.replace('          title->setAlignment(Qt::AlignCenter);\n          headerRow->addWidget(title, 1);', sentinel_code)

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("MAIN_PATCHED")
