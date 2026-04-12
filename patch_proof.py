
import os

patch1 = '''
            if (!wfData.empty()) {
                auto* wf = static_cast<WaveformOverview*>(waveformOverview_);
                // DEBUG PROOF LOG
                qWarning().noquote() << QStringLiteral("WF_BIND_CALL deck=%1 bins=%2").arg(deckIndex_ == 0 ? 'A' : 'B').arg(wfData.size());
                wf->setWaveformData(wfData);
'''

# We patch DeckStrip first.
with open('src/ui/DeckStrip.cpp', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('''
            if (!wfData.empty()) {
                auto* wf = static_cast<WaveformOverview*>(waveformOverview_);
                wf->setWaveformData(wfData);
'''.strip(), patch1.strip())

with open('src/ui/DeckStrip.cpp', 'w', encoding='utf-8') as f:
    f.write(text)

patch2 = '''    void paintEvent(QPaintEvent*) override
    {
        static int paintLogTicker[2] = {0,0};
        int dIdx = deckIndex_ & 1;
        bool doLog = (paintLogTicker[dIdx]++ % 30 == 0);

        if (doLog) {
            std::fprintf(stderr, "WF_WIDGET_STATE deck=%c state=%d\n", (deckIndex_==0?'A':'B'), (int)viewState_);
            std::fprintf(stderr, "WF_WIDGET_PATH deck=%c path='...'\n", (deckIndex_==0?'A':'B')); // Path isn't stored in overview
        }

        QPainter p(this);
'''

with open('src/ui/WaveformOverview.h', 'r', encoding='utf-8') as f:
    text2 = f.read()
text2 = text2.replace('''    void paintEvent(QPaintEvent*) override
    {
        QPainter p(this);
'''.strip(), patch2.strip())

patch3 = '''        if (!hasData_ || bins_.empty()) {
            if (doLog) {
                std::fprintf(stderr, "WF_PAINT_EMPTY deck=%c reason='No data or empty bins'\n", (deckIndex_==0?'A':'B'));
                std::fflush(stderr);
            }
'''
text2 = text2.replace('''        if (!hasData_ || bins_.empty()) {'''.strip(), patch3.strip())

patch4 = '''        // ═══ Viewport window ═══
        if (doLog) {
            std::fprintf(stderr, "WF_PAINT_WAVE deck=%c bins=%zu\n", (deckIndex_==0?'A':'B'), bins_.size());
            std::fflush(stderr);
        }
        float viewStart = 0.0f;
'''
text2 = text2.replace('''        // ═══ Viewport window ═══
        float viewStart = 0.0f;'''.strip(), patch4.strip())


with open('src/ui/WaveformOverview.h', 'w', encoding='utf-8') as f:
    f.write(text2)

print("Patch injected OK.")
