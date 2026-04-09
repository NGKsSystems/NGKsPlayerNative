with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    lines = f.readlines()

print(f"Total lines: {len(lines)}")
for i in range(1515, 1525):
    print(f"L{i+1}: {repr(lines[i])}")
