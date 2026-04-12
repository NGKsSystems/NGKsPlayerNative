import re

path = 'src/ui/DeckStrip.cpp'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. FaderScale min height
text = text.replace('setMinimumHeight(50);', 'setMinimumHeight(24);', 2)

# 2. waveformOverview min height
text = text.replace('waveformOverview_->setMinimumHeight(48);', 'waveformOverview_->setMinimumHeight(24);')

# 3. size policy from Fixed to Minimum
text = text.replace('analysisDash_->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Fixed);', 'analysisDash_->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Minimum);')

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("DECKSTRIP_CPP_PATCHED")
