#pragma once

#include "ui/library/DjBrowserUiFeedback.h"
#include "ui/library/DjBrowserTrackOps.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/LibraryScanner.h"

#include <QAbstractTableModel>
#include <QDateTime>
#include <QFileIconProvider>
#include <QFileInfo>
#include <QLineEdit>
#include <QLocale>
#include <QMimeData>
#include <QMimeDatabase>
#include <QStyledItemDelegate>
#include <QUrl>

#include <algorithm>
#include <optional>

namespace DjBrowserFileTableModelInternal {

inline QString normalized(const QString& value)
{
    return value.trimmed().toLower();
}

inline std::optional<double> parseReal(const QString& text)
{
    bool ok = false;
    const double value = text.trimmed().toDouble(&ok);
    return ok ? std::optional<double>(value) : std::nullopt;
}

inline QString fileTypeLabel(const QFileInfo& info)
{
    static QMimeDatabase mimeDb;
    const auto mime = mimeDb.mimeTypeForFile(info, QMimeDatabase::MatchExtension);
    const QString comment = mime.comment();
    if (!comment.isEmpty()) return comment;
    if (info.suffix().isEmpty()) return QStringLiteral("File");
    return QStringLiteral("%1 File").arg(info.suffix().toUpper());
}

} // namespace DjBrowserFileTableModelInternal

class DjBrowserFileTableModel final : public QAbstractTableModel {
public:
    enum Column {
        NameColumn = 0,
        SizeColumn,
        TypeColumn,
        DateModifiedColumn,
        BpmColumn,
        KeyColumn,
        CamelotColumn,
        LufsColumn,
        GenreColumn,
        ColumnCount
    };

    struct Row {
        QFileInfo fileInfo;
        QString typeLabel;
        std::optional<TrackInfo> track;
    };

    explicit DjBrowserFileTableModel(DjLibraryDatabase* db, QObject* parent = nullptr)
        : QAbstractTableModel(parent), db_(db)
    {}

    int rowCount(const QModelIndex& parent = {}) const override
    {
        if (parent.isValid()) return 0;
        return visibleRows_.size();
    }

    int columnCount(const QModelIndex& parent = {}) const override
    {
        if (parent.isValid()) return 0;
        return ColumnCount;
    }

    QVariant data(const QModelIndex& index, int role = Qt::DisplayRole) const override
    {
        if (!index.isValid() || index.row() < 0 || index.row() >= visibleRows_.size()) return {};

        const Row& row = visibleRows_.at(index.row());
        const TrackInfo* track = row.track ? &*row.track : nullptr;

        if (role == Qt::DecorationRole && index.column() == NameColumn)
            return iconProvider_.icon(row.fileInfo);

        if (role == Qt::UserRole)
            return row.fileInfo.absoluteFilePath();

        if (role == Qt::ToolTipRole)
            return row.fileInfo.absoluteFilePath();

        if (role == Qt::EditRole && index.column() == NameColumn)
            return row.fileInfo.completeBaseName();

        if (role == Qt::TextAlignmentRole) {
            switch (index.column()) {
            case SizeColumn:
            case BpmColumn:
            case LufsColumn:
                return static_cast<int>(Qt::AlignRight | Qt::AlignVCenter);
            default:
                return static_cast<int>(Qt::AlignLeft | Qt::AlignVCenter);
            }
        }

        if (role != Qt::DisplayRole) return {};

        switch (index.column()) {
        case NameColumn:
            return row.fileInfo.fileName();
        case SizeColumn:
            return formatFileSize(row.fileInfo.size());
        case TypeColumn:
            return row.typeLabel;
        case DateModifiedColumn: {
            QDateTime modified = row.fileInfo.lastModified();
            if (!modified.isValid()) {
                const QFileInfo refreshedInfo(row.fileInfo.absoluteFilePath());
                modified = refreshedInfo.lastModified();
            }
            return modified.isValid()
                ? QLocale().toString(modified, QLocale::ShortFormat)
                : QStringLiteral("-");
        }
        case BpmColumn:
            return (track && !track->bpm.isEmpty()) ? track->bpm : QStringLiteral("-");
        case KeyColumn:
            return (track && !track->musicalKey.isEmpty()) ? track->musicalKey : QStringLiteral("-");
        case CamelotColumn:
            return (track && !track->camelotKey.isEmpty()) ? track->camelotKey : QStringLiteral("-");
        case LufsColumn:
            return (track && track->loudnessLUFS != 0.0)
                ? QString::number(track->loudnessLUFS, 'f', 1)
                : QStringLiteral("-");
        case GenreColumn:
            return (track && !track->genre.isEmpty()) ? track->genre : QStringLiteral("-");
        default:
            return {};
        }
    }

    QVariant headerData(int section, Qt::Orientation orientation, int role = Qt::DisplayRole) const override
    {
        if (orientation != Qt::Horizontal || role != Qt::DisplayRole) return {};

        switch (section) {
        case NameColumn:         return QStringLiteral("Name");
        case SizeColumn:         return QStringLiteral("Size");
        case TypeColumn:         return QStringLiteral("Type");
        case DateModifiedColumn: return QStringLiteral("Date Modified");
        case BpmColumn:          return QStringLiteral("BPM");
        case KeyColumn:          return QStringLiteral("Key");
        case CamelotColumn:      return QStringLiteral("Camelot");
        case LufsColumn:         return QStringLiteral("LUFS");
        case GenreColumn:        return QStringLiteral("Genre");
        default:                 return {};
        }
    }

    Qt::ItemFlags flags(const QModelIndex& index) const override
    {
        if (!index.isValid()) return Qt::NoItemFlags;
        Qt::ItemFlags itemFlags = Qt::ItemIsEnabled | Qt::ItemIsSelectable | Qt::ItemIsDragEnabled;
        if (index.column() == NameColumn) itemFlags |= Qt::ItemIsEditable;
        return itemFlags;
    }

    bool setData(const QModelIndex& index, const QVariant& value, int role = Qt::EditRole) override
    {
        if (role != Qt::EditRole || !index.isValid() || index.column() != NameColumn) return false;
        if (index.row() < 0 || index.row() >= visibleRows_.size()) return false;

        Row& row = visibleRows_[index.row()];
        const QString oldPath = row.fileInfo.absoluteFilePath();
        const QString oldFileName = row.fileInfo.fileName();
        QString editedText = value.toString().trimmed();
        if (editedText.isEmpty()) return false;

        QString newFileName = editedText;
        if (!editedText.contains(QLatin1Char('.')) && !row.fileInfo.suffix().isEmpty()) {
            newFileName += QStringLiteral(".") + row.fileInfo.suffix();
        }
        if (newFileName == oldFileName) return false;

        const QString newPath = row.fileInfo.absoluteDir().filePath(newFileName);
        if (!DjBrowserTrackOps::renameFileAndSyncTrack(db_, oldPath, newPath)) return false;

        reload();
        return true;
    }

    QStringList mimeTypes() const override
    {
        return { QStringLiteral("text/uri-list") };
    }

    QMimeData* mimeData(const QModelIndexList& indexes) const override
    {
        if (indexes.isEmpty()) return nullptr;

        const QModelIndex first = indexes.first();
        if (!first.isValid() || first.row() < 0 || first.row() >= visibleRows_.size()) return nullptr;

        auto* mime = new QMimeData();
        mime->setUrls({QUrl::fromLocalFile(visibleRows_.at(first.row()).fileInfo.absoluteFilePath())});
        return mime;
    }

    void setFolderPath(const QString& folderPath)
    {
        folderPath_ = folderPath;
        reload();
    }

    void setSearchText(const QString& text)
    {
        if (searchText_ == text) return;
        searchText_ = text;
        rebuildVisibleRows();
    }

    void refresh()
    {
        reload();
    }

    QString folderPath() const
    {
        return folderPath_;
    }

    QString filePathAt(int row) const
    {
        if (row < 0 || row >= visibleRows_.size()) return QString();
        return visibleRows_.at(row).fileInfo.absoluteFilePath();
    }

    void sort(int column, Qt::SortOrder order = Qt::AscendingOrder) override
    {
        sortColumn_ = column;
        sortOrder_ = order;
        rebuildVisibleRows();
    }

private:
    bool matchesSearch(const Row& row) const
    {
        const QString needle = DjBrowserFileTableModelInternal::normalized(searchText_);
        if (needle.isEmpty()) return true;

        QStringList haystack;
        haystack << row.fileInfo.fileName() << row.typeLabel;
        if (row.track) {
            haystack << row.track->genre
                     << row.track->bpm
                     << row.track->musicalKey
                     << row.track->camelotKey;
        }

        return DjBrowserFileTableModelInternal::normalized(haystack.join(QLatin1Char(' '))).contains(needle);
    }

    int compareRows(const Row& lhs, const Row& rhs) const
    {
        const TrackInfo* leftTrack = lhs.track ? &*lhs.track : nullptr;
        const TrackInfo* rightTrack = rhs.track ? &*rhs.track : nullptr;

        switch (sortColumn_) {
        case NameColumn:
            return QString::localeAwareCompare(lhs.fileInfo.fileName(), rhs.fileInfo.fileName());
        case SizeColumn:
            if (lhs.fileInfo.size() < rhs.fileInfo.size()) return -1;
            if (lhs.fileInfo.size() > rhs.fileInfo.size()) return 1;
            return 0;
        case TypeColumn:
            return QString::localeAwareCompare(lhs.typeLabel, rhs.typeLabel);
        case DateModifiedColumn:
            if (lhs.fileInfo.lastModified() < rhs.fileInfo.lastModified()) return -1;
            if (lhs.fileInfo.lastModified() > rhs.fileInfo.lastModified()) return 1;
            return 0;
        case BpmColumn: {
            const auto a = leftTrack ? DjBrowserFileTableModelInternal::parseReal(leftTrack->bpm) : std::nullopt;
            const auto b = rightTrack ? DjBrowserFileTableModelInternal::parseReal(rightTrack->bpm) : std::nullopt;
            if (a && b) {
                if (*a < *b) return -1;
                if (*a > *b) return 1;
                return 0;
            }
            if (a) return -1;
            if (b) return 1;
            return 0;
        }
        case KeyColumn: {
            const QString a = leftTrack ? DjBrowserFileTableModelInternal::normalized(leftTrack->musicalKey) : QString();
            const QString b = rightTrack ? DjBrowserFileTableModelInternal::normalized(rightTrack->musicalKey) : QString();
            if (!a.isEmpty() && !b.isEmpty()) return QString::localeAwareCompare(a, b);
            if (!a.isEmpty()) return -1;
            if (!b.isEmpty()) return 1;
            return 0;
        }
        case CamelotColumn: {
            const auto a = leftTrack ? DjBrowserTrackOps::parseCamelotKey(leftTrack->camelotKey) : std::nullopt;
            const auto b = rightTrack ? DjBrowserTrackOps::parseCamelotKey(rightTrack->camelotKey) : std::nullopt;
            if (a && b) {
                if (a->first < b->first) return -1;
                if (a->first > b->first) return 1;
                if (a->second < b->second) return -1;
                if (a->second > b->second) return 1;
                return 0;
            }
            if (a) return -1;
            if (b) return 1;
            return 0;
        }
        case LufsColumn: {
            const std::optional<double> a = (leftTrack && leftTrack->loudnessLUFS != 0.0)
                ? std::optional<double>(leftTrack->loudnessLUFS) : std::nullopt;
            const std::optional<double> b = (rightTrack && rightTrack->loudnessLUFS != 0.0)
                ? std::optional<double>(rightTrack->loudnessLUFS) : std::nullopt;
            if (a && b) {
                if (*a < *b) return -1;
                if (*a > *b) return 1;
                return 0;
            }
            if (a) return -1;
            if (b) return 1;
            return 0;
        }
        case GenreColumn: {
            const QString a = leftTrack ? DjBrowserFileTableModelInternal::normalized(leftTrack->genre) : QString();
            const QString b = rightTrack ? DjBrowserFileTableModelInternal::normalized(rightTrack->genre) : QString();
            if (!a.isEmpty() && !b.isEmpty()) return QString::localeAwareCompare(a, b);
            if (!a.isEmpty()) return -1;
            if (!b.isEmpty()) return 1;
            return 0;
        }
        default:
            return 0;
        }
    }

    void rebuildVisibleRows()
    {
        beginResetModel();
        visibleRows_.clear();
        visibleRows_.reserve(allRows_.size());

        for (const Row& row : allRows_) {
            if (matchesSearch(row)) visibleRows_.push_back(row);
        }

        std::stable_sort(visibleRows_.begin(), visibleRows_.end(), [this](const Row& lhs, const Row& rhs) {
            const int cmp = compareRows(lhs, rhs);
            if (cmp == 0)
                return QString::localeAwareCompare(lhs.fileInfo.fileName(), rhs.fileInfo.fileName()) < 0;
            return (sortOrder_ == Qt::AscendingOrder) ? (cmp < 0) : (cmp > 0);
        });

        endResetModel();
    }

    void reload()
    {
        QVector<Row> rows;
        if (!folderPath_.isEmpty()) {
            QDir dir(folderPath_);
            dir.setNameFilters({QStringLiteral("*.mp3"), QStringLiteral("*.wav"), QStringLiteral("*.flac"), QStringLiteral("*.ogg")});
            const QFileInfoList entries = dir.entryInfoList(QDir::Files | QDir::NoDotAndDotDot, QDir::Name);
            rows.reserve(entries.size());

            for (const QFileInfo& fileInfo : entries) {
                Row row;
                row.fileInfo = fileInfo;
                row.typeLabel = DjBrowserFileTableModelInternal::fileTypeLabel(fileInfo);
                if (db_) {
                    row.track = db_->trackByPath(fileInfo.absoluteFilePath());
                    if (!row.track) {
                        row.track = db_->trackByFileNameAndSize(fileInfo.fileName(), fileInfo.size());
                    }
                    if (!row.track) {
                        const QString fingerprint = computeTrackFingerprint(fileInfo.absoluteFilePath());
                        if (!fingerprint.isEmpty()) {
                            row.track = db_->trackByFingerprint(fingerprint);
                        }
                    }
                }
                rows.push_back(std::move(row));
            }
        }

        beginResetModel();
        allRows_ = std::move(rows);
        visibleRows_.clear();
        endResetModel();
        rebuildVisibleRows();
    }

    DjLibraryDatabase* db_{nullptr};
    QFileIconProvider iconProvider_;
    QString folderPath_;
    QString searchText_;
    int sortColumn_{NameColumn};
    Qt::SortOrder sortOrder_{Qt::AscendingOrder};
    QVector<Row> allRows_;
    QVector<Row> visibleRows_;
};

class DjBrowserNameDelegate final : public QStyledItemDelegate {
public:
    using QStyledItemDelegate::QStyledItemDelegate;

    QWidget* createEditor(QWidget* parent, const QStyleOptionViewItem&, const QModelIndex&) const override
    {
        auto* editor = new QLineEdit(parent);
        editor->setFrame(false);
        DjBrowserUiFeedback::applyInputChrome(editor);
        editor->selectAll();
        return editor;
    }

    void setEditorData(QWidget* editor, const QModelIndex& index) const override
    {
        auto* lineEdit = qobject_cast<QLineEdit*>(editor);
        if (!lineEdit) return;
        lineEdit->setText(index.model()->data(index, Qt::EditRole).toString());
        lineEdit->selectAll();
    }

    void setModelData(QWidget* editor, QAbstractItemModel* model, const QModelIndex& index) const override
    {
        auto* lineEdit = qobject_cast<QLineEdit*>(editor);
        if (!lineEdit) return;
        model->setData(index, lineEdit->text(), Qt::EditRole);
    }
};