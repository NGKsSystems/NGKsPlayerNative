
import os
out=open('found_table.txt','w')
for root, _, files in os.walk('src/ui/library'):
    for f in files:
        if f.endswith('.cpp') or f.endswith('.h'):
            path = os.path.join(root, f)
            with open(path, 'r', encoding='utf-8', errors='ignore') as file:
                content = file.read()
                if 'data(' in content and 'role' in content:
                    out.write(f'Found data() in {path}\n')
