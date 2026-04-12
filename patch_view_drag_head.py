import re
with open('src/ui/dj/browser/DjTrackTableView.h', 'r', encoding='utf-8') as f:
    content = f.read()

header = '''
class DjTrackTableView : public QTableView {
    Q_OBJECT
public:
    explicit DjTrackTableView(QWidget* parent = nullptr);
    ~DjTrackTableView() override;

    void applyFilter(const QString& filter);
    void loadDatabaseRows(const QVector<DjTrackRow>& rows);

protected:
    void mousePressEvent(QMouseEvent* event) override;
    void mouseMoveEvent(QMouseEvent* event) override;

private:
    DjTrackTableModel* sourceModel_;
    QSortFilterProxyModel* proxyModel_;
    QPoint dragStartPos_;
};
'''
content = re.sub(r'class DjTrackTableView : public QTableView \{.*?\};', header.strip(), content, flags=re.DOTALL)

with open('src/ui/dj/browser/DjTrackTableView.h', 'w', encoding='utf-8') as f:
    f.write(content)
