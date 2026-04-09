import sys

path = "src/ui/main.cpp"
with open(path, "r", encoding="utf-8", newline="") as f:
    content = f.read()

old = "        return page;\n    {"
new = "        return page;\n    }\n\n    QWidget* buildDjModePage()\n    {"

count = content.count(old)
print(f"Occurrences of pattern: {count}")

if count == 0:
    print("NOT FOUND - checking nearby content:")
    idx = content.find("return page;")
    while idx != -1:
        print(repr(content[idx:idx+40]))
        idx = content.find("return page;", idx+1)
    sys.exit(1)

if count > 1:
    print("MULTIPLE MATCHES - aborting")
    sys.exit(1)

fixed = content.replace(old, new, 1)
with open(path, "w", encoding="utf-8", newline="") as f:
    f.write(fixed)

print("WRITTEN OK")

# Verify
with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()
for i, line in enumerate(lines[1514:1525], start=1515):
    print(f"L{i}: {line}", end="")
