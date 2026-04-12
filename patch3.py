import re

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    m = f.read()

q='''auto* deckRow = new QHBoxLayout();
        deckRow->setSpacing(6);

        // ── Deck A column: strip + library ──
        auto* colA = new QVBoxLayout();
        colA->setSpacing(4);
        djDeckA_ = new DeckStrip(0, QStringLiteral("#e07020"), &bridge_, page);
        colA->addWidget(djDeckA_, 1);
        deckRow->addLayout(colA, 5);'''

new_block_A = '''auto* deckSplitter = new QSplitter(Qt::Horizontal, page);
        auto* djBrowser = new DjBrowserPane(page);
        deckSplitter->addWidget(djBrowser);

        auto* deckWidget = new QWidget(page);
        auto* deckRow = new QHBoxLayout(deckWidget);
        deckRow->setContentsMargins(0, 0, 0, 0);
        deckRow->setSpacing(6);

        // ── Deck A column: strip + library ──
        auto* colA = new QVBoxLayout();
        colA->setSpacing(4);
        djDeckA_ = new DeckStrip(0, QStringLiteral("#e07020"), &bridge_, page);
        colA->addWidget(djDeckA_, 1);
        colA->addStretch();
        deckRow->addLayout(colA, 5);'''

if q in m:
    m = m.replace(q, new_block_A)
    print('Patched A')
else:
    print('Failed A')

q2 = '''// ── Deck B column: strip + library ──
        auto* colB = new QVBoxLayout();
        colB->setSpacing(4);
        djDeckB_ = new DeckStrip(1, QStringLiteral("#2080e0"), &bridge_, page);
        colB->addWidget(djDeckB_, 1);
        deckRow->addLayout(colB, 5);

        layout->addLayout(deckRow, 1);'''

new_block_B = '''// ── Deck B column: strip + library ──
        auto* colB = new QVBoxLayout();
        colB->setSpacing(4);
        djDeckB_ = new DeckStrip(1, QStringLiteral("#2080e0"), &bridge_, page);
        colB->addWidget(djDeckB_, 1);
        colB->addStretch();
        deckRow->addLayout(colB, 5);

        deckSplitter->addWidget(deckWidget);
        deckSplitter->setSizes({250, 1000});
        layout->addWidget(deckSplitter, 1);'''

if q2 in m:
    m = m.replace(q2, new_block_B)
    print('Patched B')
else:
    print('Failed B')

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(m)
