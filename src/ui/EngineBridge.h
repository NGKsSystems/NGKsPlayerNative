#pragma once

#include <cstdint>

#include <QObject>
#include <QTimer>

#include "engine/EngineCore.h"

class EngineBridge final : public QObject
{
    Q_OBJECT
    Q_PROPERTY(double meterL READ meterL NOTIFY meterLChanged)
    Q_PROPERTY(double meterR READ meterR NOTIFY meterRChanged)
    Q_PROPERTY(bool running READ running NOTIFY runningChanged)

public:
    explicit EngineBridge(QObject* parent = nullptr);

    Q_INVOKABLE void start();
    Q_INVOKABLE void stop();
    Q_INVOKABLE void setMasterGain(double linear01);

    double meterL() const noexcept;
    double meterR() const noexcept;
    bool running() const noexcept;

signals:
    void meterLChanged();
    void meterRChanged();
    void runningChanged();

private:
    void pollSnapshot();

    EngineCore engine;
    QTimer meterTimer;
    double meterLeftValue = 0.0;
    double meterRightValue = 0.0;
    bool runningValue = false;
    uint32_t nextCommandSeq = 1;
};