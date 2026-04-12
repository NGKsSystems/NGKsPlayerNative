import re

# --- 1. DjTrackTableModel.h ---
with open('src/ui/dj/browser/DjTrackTableModel.h', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('#include <QString>', '#include <QString>\n#include "../../library/dj/DjLibraryDatabase.h"')
text = text.replace('void loadDummyData(const QString& folderPath);', 'void loadDatabaseRows(const QVector<DjTrackRow>& rows);')
text = text.replace('QList<DjTrackRecord> tracks_;', 'QVector<DjTrackRow> tracks_;')

with open('src/ui/dj/browser/DjTrackTableModel.h', 'w', encoding='utf-8') as f:
    f.write(text)

# --- 2. DjTrackTableModel.cpp ---
cpp_content = '''#include "DjTrackTableModel.h"

DjTrackTableModel::DjTrackTableModel(QObject* parent) : QAbstractTableModel(parent) {
}

int DjTrackTableModel::rowCount(const QModelIndex& parent) const {
    if (parent.isValid()) return 0;
    return tracks_.size();
}

int DjTrackTableModel::columnCount(const QModelIndex& parent) const {      
    if (parent.isValid()) return 0;
    return 6;
}

QVariant DjTrackTableModel::data(const QModelIndex& index, int role) const {
    if (!index.isValid() || index.row() >= tracks_.size()) return {};      
    if (role == Qt::DisplayRole) {
        const auto& t = tracks_[index.row()];
        switch (index.column()) {
            case 0: return t.title;
            case 1: return t.artist;
            case 2: return t.album;
            case 3: return t.duration;
            case 4: return t.bpm;
            case 5: return t.musicalKey;
        }
    }
    return {};
}

QVariant DjTrackTableModel::headerData(int section, Qt::Orientation orientation, int role) const {
    if (role == Qt::DisplayRole && orientation == Qt::Horizontal) {        
        switch (section) {
            case 0: return "Title";
            case 1: return "Artist";
            case 2: return "Album";
            case 3: return "Time";
            case 4: return "BPM";
            case 5: return "Key";
        }
    }
    return {};
}

void DjTrackTableModel::loadDatabaseRows(const QVector<DjTrackRow>& rows) {
    beginResetModel();
    tracks_ = rows;
    endResetModel();
}
'''
with open('src/ui/dj/browser/DjTrackTableModel.cpp', 'w', encoding='utf-8') as f:
    f.write(cpp_content)

print("Model updated")
