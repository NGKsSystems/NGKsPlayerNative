#pragma once

#include <QDir>
#include <QFileSystemModel>
#include <QSet>
#include <QSortFilterProxyModel>
#include <QStringList>

class DjBrowserFolderTreeProxyModel final : public QSortFilterProxyModel
{
public:
    explicit DjBrowserFolderTreeProxyModel(QObject* parent = nullptr)
        : QSortFilterProxyModel(parent)
    {
        setDynamicSortFilter(true);
    }

    void setFileSystemModel(QFileSystemModel* model)
    {
        fileSystemModel_ = model;
        setSourceModel(model);
    }

    QString filePath(const QModelIndex& proxyIndex) const
    {
        if (!fileSystemModel_ || !proxyIndex.isValid()) return QString();
        return normalizePath(fileSystemModel_->filePath(mapToSource(proxyIndex)));
    }

    QModelIndex indexForPath(const QString& path) const
    {
        if (!fileSystemModel_) return QModelIndex();
        return mapFromSource(fileSystemModel_->index(normalizePath(path)));
    }

    QStringList hiddenPaths() const
    {
        return hiddenFolderPaths_;
    }

    void setHiddenPaths(const QStringList& paths)
    {
        hiddenFolderPaths_.clear();
        hiddenFolderKeys_.clear();

        for (const QString& path : paths) {
            const QString normalized = normalizePath(path);
            if (normalized.isEmpty()) continue;
            const QString key = pathKey(normalized);
            if (hiddenFolderKeys_.contains(key)) continue;
            hiddenFolderPaths_.append(normalized);
            hiddenFolderKeys_.insert(key);
        }

        hiddenFolderPaths_.sort(Qt::CaseInsensitive);
        invalidateFilter();
    }

    void resetHiddenPaths()
    {
        if (hiddenFolderPaths_.isEmpty()) return;
        hiddenFolderPaths_.clear();
        hiddenFolderKeys_.clear();
        invalidateFilter();
    }

    bool hasHiddenPaths() const
    {
        return !hiddenFolderPaths_.isEmpty();
    }

    bool isPathHiddenOrUnderHiddenParent(const QString& path) const
    {
        QString current = normalizePath(path);
        if (current.isEmpty()) return false;

        while (!current.isEmpty()) {
            if (hiddenFolderKeys_.contains(pathKey(current))) return true;

            const QString parentPath = QDir(current).absolutePath();
            if (parentPath == current) break;
            current = normalizePath(parentPath);
        }

        return false;
    }

    Qt::ItemFlags flags(const QModelIndex& index) const override
    {
        Qt::ItemFlags baseFlags = QSortFilterProxyModel::flags(index);
        if (!index.isValid() || index.column() != 0) return baseFlags;
        return baseFlags | Qt::ItemIsUserCheckable;
    }

    QVariant data(const QModelIndex& index, int role) const override
    {
        if (index.isValid() && index.column() == 0 && role == Qt::CheckStateRole) {
            return isPathHiddenOrUnderHiddenParent(filePath(index)) ? Qt::Unchecked : Qt::Checked;
        }

        return QSortFilterProxyModel::data(index, role);
    }

    bool setData(const QModelIndex& index, const QVariant& value, int role) override
    {
        if (!index.isValid() || index.column() != 0 || role != Qt::CheckStateRole) {
            return QSortFilterProxyModel::setData(index, value, role);
        }

        const QString normalized = filePath(index);
        if (normalized.isEmpty()) return false;

        const bool shouldHide = value.toInt() != Qt::Checked;
        if (!setPathHidden(normalized, shouldHide)) return false;

        emit dataChanged(index, index, {Qt::CheckStateRole});
        invalidateFilter();
        return true;
    }

protected:
    bool filterAcceptsRow(int sourceRow, const QModelIndex& sourceParent) const override
    {
        if (!fileSystemModel_) return true;

        const QModelIndex sourceIndex = fileSystemModel_->index(sourceRow, 0, sourceParent);
        if (!sourceIndex.isValid()) return true;

        return !isPathHiddenOrUnderHiddenParent(fileSystemModel_->filePath(sourceIndex));
    }

private:
    static QString normalizePath(const QString& path)
    {
        if (path.trimmed().isEmpty()) return QString();
        return QDir::fromNativeSeparators(QDir(path).absolutePath());
    }

    static QString pathKey(const QString& path)
    {
        const QString normalized = normalizePath(path);
#ifdef Q_OS_WIN
        return normalized.toLower();
#else
        return normalized;
#endif
    }

    bool setPathHidden(const QString& path, bool hidden)
    {
        const QString normalized = normalizePath(path);
        const QString key = pathKey(normalized);

        if (hidden) {
            if (hiddenFolderKeys_.contains(key)) return false;
            hiddenFolderPaths_.append(normalized);
            hiddenFolderPaths_.sort(Qt::CaseInsensitive);
            hiddenFolderKeys_.insert(key);
            return true;
        }

        const int removeIndex = hiddenFolderPaths_.indexOf(normalized);
        if (removeIndex >= 0) hiddenFolderPaths_.removeAt(removeIndex);
        return hiddenFolderKeys_.remove(key) > 0;
    }

    QFileSystemModel* fileSystemModel_{nullptr};
    QStringList hiddenFolderPaths_;
    QSet<QString> hiddenFolderKeys_;
};