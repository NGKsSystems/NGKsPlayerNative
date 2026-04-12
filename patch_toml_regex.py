import re

path = 'ngksgraph.toml'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

# find src_glob = [...] and replace it for the native target
# Let's just find the exact block for native
text = re.sub(
    r'(?s)(\[\[targets\]\]\n\s*name\s*=\s*"native".*?src_glob\s*=\s*\[.*?)(\])',
    r'\1, "src/ui/library/dj/DjLibraryWidget.cpp", "src/ui/library/dj/DjLibraryModel.cpp", "src/ui/library/dj/DjLibraryDatabase.cpp"]',
    text
)

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("TOML_PATCHED_REGEX")
