#include "ui/audio/AudioProfileStore.h"
#include "ui/diagnostics/RuntimeLogSupport.h"

#include <QFile>
#include <QJsonDocument>
#include <QJsonParseError>
#include <QSaveFile>

// ── loadUiAudioProfiles ───────────────────────────────────────────────────────
bool loadUiAudioProfiles(UiAudioProfilesStore& outStore, QString& outError)
{
    outStore = {};
    outError.clear();

    QFile file(kAudioProfilesPath());
    if (!file.exists()) { outError = QStringLiteral("No profiles found"); return false; }
    if (!file.open(QIODevice::ReadOnly)) { outError = QStringLiteral("Unable to open profiles file"); return false; }

    QJsonParseError parseError{};
    const QJsonDocument doc = QJsonDocument::fromJson(file.readAll(), &parseError);
    if (parseError.error != QJsonParseError::NoError || !doc.isObject()) {
        outError = QStringLiteral("Invalid profiles JSON");
        return false;
    }

    outStore.root          = doc.object();
    outStore.activeProfile = outStore.root.value(QStringLiteral("active_profile")).toString();

    const QJsonObject profilesObj = outStore.root.value(QStringLiteral("profiles")).toObject();
    for (auto it = profilesObj.begin(); it != profilesObj.end(); ++it) {
        if (!it.value().isObject()) continue;
        const QJsonObject p = it.value().toObject();
        UiAudioProfile profile{};
        profile.deviceId     = p.value(QStringLiteral("device_id")).toString();
        profile.deviceName   = p.value(QStringLiteral("device_name")).toString();
        profile.sampleRate   = p.value(QStringLiteral("sample_rate")).toInt(p.value(QStringLiteral("sr")).toInt(0));
        profile.bufferFrames = p.value(QStringLiteral("buffer_frames")).toInt(p.value(QStringLiteral("buffer")).toInt(128));
        profile.channelsOut  = p.value(QStringLiteral("channels_out")).toInt(p.value(QStringLiteral("ch_out")).toInt(2));
        outStore.profiles[it.key()] = profile;
    }

    if (outStore.profiles.empty()) { outError = QStringLiteral("No profiles found"); return false; }

    if (outStore.activeProfile.isEmpty() ||
        outStore.profiles.find(outStore.activeProfile) == outStore.profiles.end()) {
        outStore.activeProfile = outStore.profiles.begin()->first;
    }

    return true;
}

// ── writeUiAudioProfilesActiveProfile ────────────────────────────────────────
bool writeUiAudioProfilesActiveProfile(const UiAudioProfilesStore& store,
                                       const QString& activeProfile,
                                       QString& outError)
{
    outError.clear();
    QJsonObject root = store.root;
    if (root.isEmpty()) root.insert(QStringLiteral("profiles"), QJsonObject());
    root.insert(QStringLiteral("active_profile"), activeProfile);

    QSaveFile saveFile(kAudioProfilesPath());
    if (!saveFile.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
        outError = QStringLiteral("Unable to open profiles file for write");
        return false;
    }

    const QByteArray payload = QJsonDocument(root).toJson(QJsonDocument::Indented);
    if (saveFile.write(payload) != payload.size()) {
        outError = QStringLiteral("Failed writing profiles file");
        saveFile.cancelWriting();
        return false;
    }

    if (!saveFile.commit()) { outError = QStringLiteral("Failed to commit profiles file"); return false; }
    return true;
}
