import re
with open('src/ui/dj/browser/DjTrackTableModel.cpp', 'r', encoding='utf-8') as f:
    content = f.read()

new_logic = '''
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
    } else if (role == Qt::UserRole + 1) {
        return tracks_[index.row()].filePath;
    }
    return {};
}
'''
content = re.sub(r'QVariant DjTrackTableModel::data.*?\n\}', new_logic.strip(), content, flags=re.DOTALL)

with open('src/ui/dj/browser/DjTrackTableModel.cpp', 'w', encoding='utf-8') as f:
    f.write(content)
