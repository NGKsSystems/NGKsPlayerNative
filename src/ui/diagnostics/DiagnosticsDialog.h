#pragma once

#include "ui/diagnostics/RuntimeLogSupport.h"
#include "ui/EngineBridge.h"

#include <QDialog>
#include <QString>

// ── Status / telemetry formatting helpers ─────────────────────────────────────
// (Declared here for use from both DiagnosticsDialog and MainWindow::pollStatus)

inline QString boolToFlag(bool v) { return v ? QStringLiteral("TRUE") : QStringLiteral("FALSE"); }
inline QString passFail(bool v)   { return v ? QStringLiteral("PASS") : QStringLiteral("FAIL"); }

QString utcNowIso();
QString statusSummaryLine(const UIStatus& status);
QString healthSummaryLine(const UIHealthSnapshot& health);
QString telemetrySummaryLine(const UIEngineTelemetrySnapshot& telemetry);
QString agSummaryLine(const UIEngineTelemetrySnapshot& telemetry);
QString telemetrySparkline(const UIEngineTelemetrySnapshot& telemetry);
QString rtWatchdogStateText(int32_t code);
QString foundationReportLine(const UIFoundationSnapshot& foundation);
QString foundationBlockText(const UIFoundationSnapshot& foundation, const UISelfTestSnapshot* selfTests);

// ── DiagnosticsDialog ─────────────────────────────────────────────────────────
class QLabel;
class QPlainTextEdit;

class DiagnosticsDialog : public QDialog {
public:
    explicit DiagnosticsDialog(EngineBridge& engineBridge, QWidget* parent = nullptr);

    void setStatus(const UIStatus& status);
    void setHealth(const UIHealthSnapshot& health);
    void setTelemetry(const UIEngineTelemetrySnapshot& telemetry);
    void setFoundation(const UIFoundationSnapshot& foundation, const UISelfTestSnapshot* selfTests);
    void setRtAudio(const UIEngineTelemetrySnapshot& telemetry);
    void refreshLogTail();
    void copyReportToClipboard();

private:
    EngineBridge&   bridge_;
    QLabel*         statusLabel_{nullptr};
    QLabel*         detailsLabel_{nullptr};
    QLabel*         lastUpdateLabel_{nullptr};
    QLabel*         healthLabel_{nullptr};
    QLabel*         telemetryLabel_{nullptr};
    QLabel*         foundationLabel_{nullptr};
    QLabel*         rtAudioLabel_{nullptr};
    QPlainTextEdit* logTailBox_{nullptr};
    QString         foundationText_;
};
