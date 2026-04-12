import re

with open('src/ui/main.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

# Fields
if 'AnalysisBridge* djAnalysisA_' not in text:
    text = text.replace('DeckStrip* djDeckA_{nullptr};', 'DeckStrip* djDeckA_{nullptr};\n    AnalysisBridge* djAnalysisA_{nullptr};\n    AnalysisBridge* djAnalysisB_{nullptr};')

# Instantiation
spot1 = 'stack_->addWidget(buildSplashPage());    // 0'
if 'djAnalysisA_ = new AnalysisBridge(this);' not in text:
    text = text.replace(spot1, 'djAnalysisA_ = new AnalysisBridge(this);\n        djAnalysisA_->start();\n        djAnalysisB_ = new AnalysisBridge(this);\n        djAnalysisB_->start();\n        ' + spot1)

# Wiring
spotA = 'djDeckA_ = new DeckStrip(0, QStringLiteral("#e07020"), &bridge_, page);\n        colA->addWidget(djDeckA_, 1);'
if 'djDeckA_->updateAnalysisPanel' not in text:
    text = text.replace(spotA, spotA + '\n        QObject::connect(djAnalysisA_, &AnalysisBridge::panelStateChanged, djDeckA_, &DeckStrip::updateAnalysisPanel);')

spotB = 'djDeckB_ = new DeckStrip(1, QStringLiteral("#2080e0"), &bridge_, page);\n        colB->addWidget(djDeckB_, 1);'
if 'djDeckB_->updateAnalysisPanel' not in text:
    text = text.replace(spotB, spotB + '\n        QObject::connect(djAnalysisB_, &AnalysisBridge::panelStateChanged, djDeckB_, &DeckStrip::updateAnalysisPanel);')

# Lambda
m = re.search(r'QObject::connect\(&bridge_, &EngineBridge::djSnapshotUpdated.*?\}\);', text, re.DOTALL)
if m and 'djAnalysisA_->isReady' not in m.group(0):
    old_lambda = m.group(0)
    new_lambda = '''QObject::connect(&bridge_, &EngineBridge::djSnapshotUpdated, this, [this]() {
            if (djDeckA_) djDeckA_->refreshFromSnapshot();
            if (djDeckB_) djDeckB_->refreshFromSnapshot();
            if (djMasterMeterL_) djMasterMeterL_->setLevel(static_cast<float>(bridge_.masterPeakL()));
            if (djMasterMeterR_) djMasterMeterR_->setLevel(static_cast<float>(bridge_.masterPeakR()));

            static QString lastPathA, lastPathB;
            if (djAnalysisA_ && djAnalysisA_->isReady()) {
                const QString pathA = bridge_.deckFilePath(0);
                if (pathA != lastPathA) {
                    lastPathA = pathA;
                    if (pathA.isEmpty()) djAnalysisA_->unselectTrack();
                    else djAnalysisA_->selectTrack(pathA);
                }
                if (!pathA.isEmpty()) djAnalysisA_->resolvePlayhead(bridge_.deckPlayhead(0));
            }
            if (djAnalysisB_ && djAnalysisB_->isReady()) {
                const QString pathB = bridge_.deckFilePath(1);
                if (pathB != lastPathB) {
                    lastPathB = pathB;
                    if (pathB.isEmpty()) djAnalysisB_->unselectTrack();
                    else djAnalysisB_->selectTrack(pathB);
                }
                if (!pathB.isEmpty()) djAnalysisB_->resolvePlayhead(bridge_.deckPlayhead(1));
            }
        });'''
    text = text.replace(old_lambda, new_lambda)

with open('src/ui/main.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

print('ALL PATCHED!')
