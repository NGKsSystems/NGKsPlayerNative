#pragma once

#include <QJsonObject>
#include <QString>
#include <map>

// ── UiAudioProfile ────────────────────────────────────────────────────────────
struct UiAudioProfile {
    QString deviceId;
    QString deviceName;
    int     sampleRate{0};
    int     bufferFrames{0};
    int     channelsOut{2};
};

// ── UiAudioProfilesStore ──────────────────────────────────────────────────────
struct UiAudioProfilesStore {
    QString                          activeProfile;
    std::map<QString, UiAudioProfile> profiles;
    QJsonObject                      root;
};

// ── Load / write active audio profiles ───────────────────────────────────────
bool loadUiAudioProfiles(UiAudioProfilesStore& outStore, QString& outError);
bool writeUiAudioProfilesActiveProfile(const UiAudioProfilesStore& store,
                                       const QString& activeProfile,
                                       QString& outError);
