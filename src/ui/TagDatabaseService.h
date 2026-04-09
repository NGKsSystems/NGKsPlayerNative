#pragma once
#include <QObject>
#include <QString>
#include <QVariant>
#include <QHash>

class QSqlDatabase;

// Dual-database service:
//   READ  from existing NGKsPlayer library.db (tracks table, 66 cols)
//   WRITE to local tag_overlay.db (overlay edits only)
class TagDatabaseService : public QObject
{
    Q_OBJECT
public:
    explicit TagDatabaseService(QObject* parent = nullptr);
    ~TagDatabaseService();

    bool init(const QString& overlayDbPath, const QString& libraryDbPath);

    // Load overlay fields for a file.
    // First reads from library.db tracks table, then overlays with tag_overlay.
    QHash<QString, QVariant> loadByFile(const QString& filePath);

    // Save a single field for a file (writes to overlay DB only).
    bool saveField(const QString& filePath, const QString& fieldName, const QVariant& value);

    // Bulk-save multiple fields for a file (writes to overlay DB only).
    bool saveBulk(const QString& filePath, const QHash<QString, QVariant>& fields);

    bool hasRecord(const QString& filePath) const;

private:
    bool ensureOverlaySchema();
    QString canonicalPath(const QString& filePath) const;
    QHash<QString, QVariant> loadFromLibrary(const QString& filePath);
    QHash<QString, QVariant> loadFromOverlay(const QString& filePath);

    QString overlayConn_;
    QString libraryConn_;
    bool libraryAvailable_{false};
};
