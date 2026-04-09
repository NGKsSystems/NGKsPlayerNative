#pragma once

#include "ui/library/DjLibraryWidget.h"

#include <QStringList>
#include <QWidget>

#include <optional>

class QComboBox;
class QHeaderView;
class QLabel;
class QLineEdit;
class DjLibraryDatabase;
struct TrackInfo;

// ─────────────────────────────────────────────────────────────────────────────
// LibraryBrowserWidget
//
// Owns all library-browser UI: search bar, sort/mode combos, column table.
// Two modes:
//   MainPanel   — search-mode combo + search bar + sort combo above a wide table.
//   PlayerPanel — search bar + sort combo header row above a compact table.
//
// Usage (main.cpp bootstrap):
//   libraryTree_ = new LibraryBrowserWidget(LibraryBrowserWidget::Mode::MainPanel, splitter);
//   libraryTree_->setDatabase(&djDb_);
//   connect(libraryTree_, &LibraryBrowserWidget::trackActivated, this, [...]);
//   connect(libraryTree_, &LibraryBrowserWidget::trackCountChanged, this, [...]);
// ─────────────────────────────────────────────────────────────────────────────
class LibraryBrowserWidget : public QWidget {
    Q_OBJECT
public:
    enum class Mode { MainPanel, PlayerPanel };

    explicit LibraryBrowserWidget(Mode mode, QWidget* parent = nullptr);

    // Must be called before the first filter refresh.
    void setDatabase(DjLibraryDatabase* db);

    // Set the active playlist-path filter (empty list = show all tracks).
    // Call refresh() after changing the filter.
    void setPlaylistFilter(const QStringList& paths);

    // Re-apply the current filter.  Call after setPlaylistFilter() or whenever
    // external state (e.g. active playlist) changes.
    void refresh();

    // ── DjLibraryWidget passthroughs ─────────────────────────────────────
    qint64 currentTrackId() const;
    void   setCurrentTrackId(qint64 id);
    void   scrollToTrackId(qint64 id);
    qint64 firstVisibleTrackId() const;
    qint64 trackIdAt(int row) const;
    int    rowOfTrackId(qint64 id) const;
    int    totalFilteredCount() const;
    std::optional<TrackInfo> trackById(qint64 id) const;

signals:
    void trackActivated(qint64 trackId);
    void trackSelected(qint64 trackId);
    void contextMenuRequested(qint64 trackId, QPoint globalPos);

    /// Emitted after every filter change with the new total row count.
    void trackCountChanged(int count);

private slots:
    void onFilterChanged();

private:
    void buildMainPanel();
    void buildPlayerPanel();
    void applyCurrentFilter();

    Mode             mode_;
    DjLibraryWidget* view_{nullptr};
    QLineEdit*       searchBar_{nullptr};
    QComboBox*       searchModeCombo_{nullptr};  ///< MainPanel only
    QComboBox*       sortCombo_{nullptr};
    QLabel*          countLabel_{nullptr};        ///< PlayerPanel only
    QStringList      playlistFilter_;
};
