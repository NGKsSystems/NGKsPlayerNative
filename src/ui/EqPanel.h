#pragma once

#include <array>
#include <QWidget>
#include <QSlider>
#include <QLabel>
#include <QPushButton>
#include <QComboBox>

class EngineBridge;

/// Reusable 16-band EQ panel widget.
/// Self-contained: owns all sliders, labels, buttons, presets.
/// Communicates with the engine via an EngineBridge pointer.
/// Can be dropped into any layout in the app.
class EqPanel : public QWidget {
    Q_OBJECT
public:
    explicit EqPanel(EngineBridge* bridge, QWidget* parent = nullptr);

    void setCollapsed(bool collapsed);
    bool isCollapsed() const;

    /// Set target deck index for DJ mode (0-3).
    /// When < 0 (default), uses Simple Mode bridge methods (DECK_A hardcoded).
    void setDeckIndex(int deckIndex);
    int deckIndex() const;

    /// Set accent color for integrated deck styling.
    void setAccentColor(const QString& hexColor);

private:
    void buildUi();
    void wireSignals();
    void applyPreset(int index);
    void resetFlat();

    EngineBridge* bridge_{nullptr};

    // Widgets
    QPushButton* toggleBtn_{nullptr};
    QComboBox* presetCombo_{nullptr};
    QPushButton* bypassBtn_{nullptr};
    QPushButton* resetBtn_{nullptr};
    QWidget* bodyWidget_{nullptr};

    std::array<QSlider*, 16> bandSliders_{};
    std::array<QLabel*, 16> bandDbLabels_{};

    bool bypassed_{false};
    int deckIndex_{-1};  // -1 = simple mode, 0-3 = DJ deck
    QString accentColor_;
};
