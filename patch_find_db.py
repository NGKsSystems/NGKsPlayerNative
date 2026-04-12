import sys

file_path = r"c:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\src\ui\library\LegacyLibraryImport.cpp"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

old_func = """QString findLegacyDbPath()
{
    const QStringList candidates = {
        QDir::homePath() + QStringLiteral("/AppData/Roaming/ngksplayer/library.db"),
        QDir::homePath() + QStringLiteral("/AppData/Roaming/proproductionsuite/library.db"),
        QDir::homePath() + QStringLiteral("/AppData/Roaming/proaudioclipper/library.db"),
    };"""

new_func = """#include <QCoreApplication>
QString findLegacyDbPath()
{
    const QStringList candidates = {
        QCoreApplication::applicationDirPath() + QStringLiteral("/../../../data/dj_library_core.db"),
        QDir::currentPath() + QStringLiteral("/data/dj_library_core.db"),
        QCoreApplication::applicationDirPath() + QStringLiteral("/../data/dj_library_core.db"),
        QDir::homePath() + QStringLiteral("/AppData/Roaming/ngksplayer/library.db"),
        QDir::homePath() + QStringLiteral("/AppData/Roaming/proproductionsuite/library.db"),
        QDir::homePath() + QStringLiteral("/AppData/Roaming/proaudioclipper/library.db"),
    };"""

content = content.replace(old_func, new_func)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)
print("done2")
