#include "ui/EqPanel.h"
#include "ui/EngineBridge.h"

#include <cstdio>
#include <QFont>
#include <QFrame>
#include <QHBoxLayout>
#include <QSignalBlocker>
#include <QVBoxLayout>

// Preset gain curves (16 values each, in dB)
static constexpr double kPresets[][16] = {
    // 0: Flat
    { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    // 1: Bass Boost
    { 4, 3.5, 3, 2.5, 1.5, 0.5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
    // 2: Treble Boost
    { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.5, 1.5, 2.5, 3, 3.5, 4, 4 },
    // 3: V-Shape
    { 3.5, 3, 2, 1, 0, -1, -1.5, -1.5, -1.5, -1, 0, 1, 2, 3, 3.5, 3.5 },
    // 4: Mid Scoop
    { 1.5, 1, 0.5, 0, -1, -2, -2.5, -2.5, -2.5, -2, -1, 0, 0.5, 1, 1.5, 1.5 },
    // 5: Vocal Boost
    { -1, -0.5, 0, 0, 0.5, 1.5, 2.5, 3, 2.5, 1.5, 0.5, 0, 0, -0.5, -1, -1 },
    // 6: Loudness
    { 3, 2.5, 1.5, 0.5, 0, 0, -0.5, -0.5, -0.5, 0, 0, 0.5, 1.5, 2.5, 3, 3 },
    // 7: De-Mud
    { 0, 0, 0, -1, -2, -2.5, -1.5, -0.5, 0, 0.5, 1, 1, 0.5, 0, 0, 0 },
};
static constexpr int kPresetCount = 8;

static constexpr const char* kFreqLabels[16] = {
    "20", "32", "50", "80", "125", "200", "315", "500",
    "800", "1.2k", "2k", "3.1k", "5k", "8k", "12k", "16k"
};

EqPanel::EqPanel(EngineBridge* bridge, QWidget* parent)
    : QWidget(parent), bridge_(bridge)
{
    buildUi();
    wireSignals();
    setCollapsed(true);
}

void EqPanel::setCollapsed(bool collapsed)
{
    bodyWidget_->setVisible(!collapsed);
    toggleBtn_->setText(collapsed
        ? QStringLiteral("\u25B6 EQ") : QStringLiteral("\u25BC EQ"));
}

bool EqPanel::isCollapsed() const
{
    return !bodyWidget_->isVisible();
}

void EqPanel::setDeckIndex(int deckIndex) { deckIndex_ = deckIndex; }
int EqPanel::deckIndex() const { return deckIndex_; }

void EqPanel::setAccentColor(const QString& hexColor)
{
    accentColor_ = hexColor;
    // Restyle toggle button
    toggleBtn_->setStyleSheet(QStringLiteral(
        "QPushButton { background: transparent; border: none; color: %1;"
        "  font-size: 10px; font-weight: bold; padding: 2px 4px; }"
        "QPushButton:hover { color: %1; }").arg(hexColor));
    // Restyle EQ frame — remove border for integrated look
    if (auto* frame = findChild<QFrame*>(QStringLiteral("eqFrame"))) {
        frame->setStyleSheet(QStringLiteral(
            "QFrame#eqFrame { background: transparent; border: none; }"));
    }
    // Restyle slider handles
    for (int i = 0; i < 16; ++i) {
        bandSliders_[i]->setStyleSheet(QStringLiteral(
            "QSlider::groove:vertical { background: #1a1a2e; width: 3px; border-radius: 1px; }"
            "QSlider::handle:vertical { background: %1; width: 12px; height: 8px;"
            "  margin: 0 -5px; border-radius: 4px; }"
            "QSlider::sub-page:vertical { background: #1a1a2e; border-radius: 1px; }"
            "QSlider::add-page:vertical { background: %1; border-radius: 1px; }").arg(hexColor));
    }
}

void EqPanel::buildUi()
{
    auto* outerLayout = new QHBoxLayout(this);
    outerLayout->setContentsMargins(0, 0, 0, 0);
    outerLayout->setSpacing(0);

    auto* eqFrame = new QFrame(this);
    eqFrame->setObjectName(QStringLiteral("eqFrame"));
    eqFrame->setStyleSheet(QStringLiteral(
        "QFrame#eqFrame { background: #0d1117; border: 1px solid #1a1a2e;"
        "  border-radius: 8px; }"));

    auto* eqOuter = new QVBoxLayout(eqFrame);
    eqOuter->setContentsMargins(10, 6, 10, 6);
    eqOuter->setSpacing(4);

    // Header row: toggle + presets + bypass + reset
    auto* headerRow = new QHBoxLayout();
    headerRow->setSpacing(6);

    toggleBtn_ = new QPushButton(QStringLiteral("\u25BC EQ"), this);
    {
        QFont f = toggleBtn_->font();
        f.setPointSize(10);
        f.setBold(true);
        toggleBtn_->setFont(f);
    }
    toggleBtn_->setCursor(Qt::PointingHandCursor);
    toggleBtn_->setStyleSheet(QStringLiteral(
        "QPushButton { background: transparent; border: none; color: #e94560;"
        "  font-size: 10px; font-weight: bold; padding: 2px 4px; }"
        "QPushButton:hover { color: #ff6b81; }"));
    headerRow->addWidget(toggleBtn_);

    headerRow->addStretch(1);

    presetCombo_ = new QComboBox(this);
    presetCombo_->addItems({
        QStringLiteral("Flat"),
        QStringLiteral("Bass Boost"),
        QStringLiteral("Treble Boost"),
        QStringLiteral("V-Shape"),
        QStringLiteral("Mid Scoop"),
        QStringLiteral("Vocal Boost"),
        QStringLiteral("Loudness"),
        QStringLiteral("De-Mud"),
    });
    presetCombo_->setMinimumWidth(100);
    presetCombo_->setMaximumWidth(140);
    presetCombo_->setStyleSheet(QStringLiteral(
        "QComboBox { background: #16213e; color: #e0e0e0; border: 1px solid #0f3460;"
        "  border-radius: 4px; padding: 2px 6px; font-size: 9px; }"
        "QComboBox::drop-down { border: none; }"
        "QComboBox QAbstractItemView { background: #16213e; color: #e0e0e0;"
        "  selection-background-color: #533483; }"));
    headerRow->addWidget(presetCombo_);

    bypassBtn_ = new QPushButton(QStringLiteral("Bypass: OFF"), this);
    bypassBtn_->setMinimumSize(80, 24);
    bypassBtn_->setCursor(Qt::PointingHandCursor);
    bypassBtn_->setStyleSheet(QStringLiteral(
        "QPushButton { background: rgba(26,58,92,200); border: 1px solid #0f3460;"
        "  border-radius: 4px; color: #8cc; font-size: 9px; font-weight: bold; padding: 2px 6px; }"
        "QPushButton:hover { background: rgba(31,74,112,220); }"));
    headerRow->addWidget(bypassBtn_);

    resetBtn_ = new QPushButton(QStringLiteral("Reset"), this);
    resetBtn_->setMinimumSize(56, 24);
    resetBtn_->setCursor(Qt::PointingHandCursor);
    resetBtn_->setStyleSheet(QStringLiteral(
        "QPushButton { background: rgba(26,58,92,200); border: 1px solid #0f3460;"
        "  border-radius: 4px; color: #aaccee; font-size: 9px; font-weight: bold; padding: 2px 6px; }"
        "QPushButton:hover { background: rgba(31,74,112,220); }"));
    headerRow->addWidget(resetBtn_);

    eqOuter->addLayout(headerRow);

    // Collapsible body
    bodyWidget_ = new QWidget(this);
    auto* bodyLayout = new QVBoxLayout(bodyWidget_);
    bodyLayout->setContentsMargins(0, 0, 0, 0);
    bodyLayout->setSpacing(0);

    auto* bandRow = new QHBoxLayout();
    bandRow->setSpacing(1);

    for (int band = 0; band < 16; ++band) {
        auto* bandCol = new QVBoxLayout();
        bandCol->setSpacing(1);
        bandCol->setAlignment(Qt::AlignHCenter);

        auto* dbLabel = new QLabel(QStringLiteral("0"), this);
        dbLabel->setAlignment(Qt::AlignCenter);
        dbLabel->setFixedWidth(26);
        {
            QFont f = dbLabel->font();
            f.setPointSize(6);
            dbLabel->setFont(f);
        }
        dbLabel->setStyleSheet(QStringLiteral("color: #888; background: transparent;"));
        bandDbLabels_[band] = dbLabel;
        bandCol->addWidget(dbLabel);

        auto* slider = new QSlider(Qt::Vertical, this);
        slider->setRange(-60, 60);  // +/-6.0 dB in tenths
        slider->setValue(0);
        slider->setFixedWidth(22);
        slider->setMinimumHeight(60);
        slider->setMaximumHeight(90);
        slider->setStyleSheet(QStringLiteral(
            "QSlider::groove:vertical { background: #1a1a2e; width: 3px; border-radius: 1px; }"
            "QSlider::handle:vertical { background: #e94560; width: 12px; height: 8px;"
            "  margin: 0 -5px; border-radius: 4px; }"
            "QSlider::sub-page:vertical { background: #1a1a2e; border-radius: 1px; }"
            "QSlider::add-page:vertical { background: #533483; border-radius: 1px; }"));
        bandSliders_[band] = slider;
        bandCol->addWidget(slider);

        auto* freqLabel = new QLabel(QString::fromLatin1(kFreqLabels[band]), this);
        freqLabel->setAlignment(Qt::AlignCenter);
        freqLabel->setFixedWidth(26);
        {
            QFont f = freqLabel->font();
            f.setPointSize(6);
            freqLabel->setFont(f);
        }
        freqLabel->setStyleSheet(QStringLiteral("color: #666; background: transparent;"));
        bandCol->addWidget(freqLabel);

        bandRow->addLayout(bandCol);
    }

    bodyLayout->addLayout(bandRow);
    eqOuter->addWidget(bodyWidget_);

    outerLayout->addStretch(1);
    outerLayout->addWidget(eqFrame, 1);
    outerLayout->addStretch(1);
}

void EqPanel::wireSignals()
{
    // Collapse/expand
    connect(toggleBtn_, &QPushButton::clicked, this, [this]() {
        setCollapsed(!isCollapsed());
        qInfo().noquote() << QStringLiteral("EQ_PANEL=%1").arg(isCollapsed() ? "COLLAPSED" : "EXPANDED");
    });

    // Slider → bridge
    for (int band = 0; band < 16; ++band) {
        connect(bandSliders_[band], &QSlider::valueChanged, this,
            [this, band](int value) {
                const double gainDb = static_cast<double>(value) / 10.0;
                if (deckIndex_ >= 0)
                    bridge_->setDeckEqBandGain(deckIndex_, band, gainDb);
                else
                    bridge_->setEqBandGain(band, gainDb);
                bandDbLabels_[band]->setText(QString::number(gainDb, 'f', 1));
                std::fprintf(stderr, "MIX_EQ_SET deck=%d band=%d gainDb=%.1f\n",
                             deckIndex_, band, gainDb);
                std::fflush(stderr);
            });
    }

    // Bypass
    connect(bypassBtn_, &QPushButton::clicked, this, [this]() {
        bypassed_ = !bypassed_;
        if (deckIndex_ >= 0)
            bridge_->setDeckEqBypass(deckIndex_, bypassed_);
        else
            bridge_->setEqBypass(bypassed_);
        bypassBtn_->setText(bypassed_
            ? QStringLiteral("Bypass: ON") : QStringLiteral("Bypass: OFF"));
        bypassBtn_->setStyleSheet(bypassed_
            ? QStringLiteral(
                "QPushButton { background: rgba(83,52,131,200); border: 1px solid #e94560;"
                "  border-radius: 4px; color: #e94560; font-size: 9px; font-weight: bold; padding: 2px 6px; }"
                "QPushButton:hover { background: rgba(100,60,150,220); }")
            : QStringLiteral(
                "QPushButton { background: rgba(26,58,92,200); border: 1px solid #0f3460;"
                "  border-radius: 4px; color: #8cc; font-size: 9px; font-weight: bold; padding: 2px 6px; }"
                "QPushButton:hover { background: rgba(31,74,112,220); }"));
        qInfo().noquote() << QStringLiteral("EQ_BYPASS=%1").arg(bypassed_ ? "ON" : "OFF");
    });

    // Reset
    connect(resetBtn_, &QPushButton::clicked, this, [this]() {
        resetFlat();
    });

    // Presets
    connect(presetCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged), this,
        [this](int index) {
            applyPreset(index);
        });
}

void EqPanel::applyPreset(int index)
{
    if (index < 0 || index >= kPresetCount) return;
    for (int band = 0; band < 16; ++band) {
        const int tenths = static_cast<int>(kPresets[index][band] * 10.0);
        QSignalBlocker blocker(bandSliders_[band]);
        bandSliders_[band]->setValue(tenths);
        bandDbLabels_[band]->setText(QString::number(kPresets[index][band], 'f', 1));
        if (deckIndex_ >= 0)
            bridge_->setDeckEqBandGain(deckIndex_, band, kPresets[index][band]);
        else
            bridge_->setEqBandGain(band, kPresets[index][band]);
    }
    qInfo().noquote() << QStringLiteral("EQ_PRESET=%1").arg(presetCombo_->currentText());
}

void EqPanel::resetFlat()
{
    for (int band = 0; band < 16; ++band) {
        QSignalBlocker blocker(bandSliders_[band]);
        bandSliders_[band]->setValue(0);
        bandDbLabels_[band]->setText(QStringLiteral("0"));
        if (deckIndex_ >= 0)
            bridge_->setDeckEqBandGain(deckIndex_, band, 0.0);
        else
            bridge_->setEqBandGain(band, 0.0);
    }
    {
        QSignalBlocker blocker(presetCombo_);
        presetCombo_->setCurrentIndex(0);
    }
    qInfo().noquote() << QStringLiteral("EQ_RESET=FLAT");
}
