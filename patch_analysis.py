import re

path = 'src/ui/DjAnalysisPanelWidget.h'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

bad_fixed = '        setFixedHeight(96);'
bad_policy = '        setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Fixed);'

good_fixed = '        setMinimumHeight(32);'
good_policy = '        setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Minimum);'

text = text.replace(bad_fixed, good_fixed)
text = text.replace(bad_policy, good_policy)

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("DJANALYSIS_PATCHED")
