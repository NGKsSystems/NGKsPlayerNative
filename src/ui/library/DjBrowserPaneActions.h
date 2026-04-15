#pragma once

#include "ui/library/DjBrowserFileTableModel.h"
#include "ui/library/DjBrowserTrackOps.h"
#include "ui/library/DjBrowserUiFeedback.h"
#include "ui/library/DjLibraryDatabase.h"
#include "ui/library/TrackDragView.h"

#include <QDialog>
#include <QDialogButtonBox>
#include <QDir>
#include <QFormLayout>
#include <QLabel>
#include <QLineEdit>
#include <QString>
#include <QStringList>
#include <QVBoxLayout>
#include <QWidget>

#include <functional>

namespace DjBrowserPaneActions {

using FooterCallback = std::function<void(const QString&, const QString&)>;

struct BulkReplaceRequest {
    QString findText;
    QString replaceText;
    bool accepted{false};
};

inline void updateFooterMessage(QLabel* footerLabel, const QString& text, const QString& tone)
{
    if (!footerLabel) return;
    footerLabel->setText(text.isEmpty() ? QStringLiteral("Ready.") : text);
    footerLabel->setStyleSheet(DjBrowserUiFeedback::footerStyleForTone(tone));
}

inline bool renameVisibleFile(QWidget* parent,
                              DjBrowserFileTableModel* fileModel,
                              TrackDragView* fileView,
                              const QString& filePath,
                              const FooterCallback& updateFooter)
{
    if (!fileModel || !fileView) return false;

    for (int row = 0; row < fileModel->rowCount(); ++row) {
        if (fileModel->filePathAt(row) != filePath) continue;
        const QModelIndex index = fileModel->index(row, DjBrowserFileTableModel::NameColumn);
        fileView->setCurrentIndex(index);
        fileView->selectRow(row);
        fileView->edit(index);
        if (updateFooter) {
            updateFooter(
                QStringLiteral("Rename mode: type the replacement name directly in the song list."),
                QStringLiteral("info"));
        }
        return true;
    }

    if (!filePath.isEmpty()) {
        DjBrowserUiFeedback::themedWarning(
            parent,
            QStringLiteral("Rename Failed"),
            QStringLiteral("Could not locate the selected file in the browser."));
        if (updateFooter) {
            updateFooter(QStringLiteral("Rename failed: file is no longer visible."), QStringLiteral("error"));
        }
        return false;
    }

    return false;
}

inline BulkReplaceRequest promptBulkReplace(QWidget* parent,
                                            const QString& initialFindText,
                                            const QString& initialReplaceText)
{
    QDialog dialog(parent);
    dialog.setModal(true);
    dialog.setWindowTitle(QStringLiteral("Find and Replace File Names"));
    dialog.setStyleSheet(DjBrowserUiFeedback::dialogStyleSheet());

    auto* layout = new QVBoxLayout(&dialog);
    auto* intro = new QLabel(
        QStringLiteral("Rename files in the current folder by replacing matching text in each visible file name."),
        &dialog);
    intro->setWordWrap(true);
    layout->addWidget(intro);

    auto* form = new QFormLayout();
    auto* findBox = new QLineEdit(&dialog);
    auto* replaceBox = new QLineEdit(&dialog);
    findBox->setPlaceholderText(QStringLiteral("Text to find"));
    replaceBox->setPlaceholderText(QStringLiteral("Replacement text (can be empty)"));
    findBox->setText(initialFindText);
    replaceBox->setText(initialReplaceText);
    DjBrowserUiFeedback::applyInputChrome(findBox);
    DjBrowserUiFeedback::applyInputChrome(replaceBox);
    form->addRow(QStringLiteral("Find"), findBox);
    form->addRow(QStringLiteral("Replace"), replaceBox);
    layout->addLayout(form);

    auto* buttons = new QDialogButtonBox(QDialogButtonBox::Ok | QDialogButtonBox::Cancel, &dialog);
    layout->addWidget(buttons);

    QObject::connect(buttons, &QDialogButtonBox::accepted, &dialog, &QDialog::accept);
    QObject::connect(buttons, &QDialogButtonBox::rejected, &dialog, &QDialog::reject);

    findBox->setFocus();
    findBox->selectAll();

    BulkReplaceRequest request;
    if (dialog.exec() != QDialog::Accepted) return request;

    request.findText = findBox->text();
    request.replaceText = replaceBox->text();
    request.accepted = true;
    return request;
}

inline bool bulkReplaceFiles(QWidget* parent,
                             DjLibraryDatabase* db,
                             DjBrowserFileTableModel* fileModel,
                             const QString& findText,
                             const QString& replaceText,
                             const FooterCallback& updateFooter)
{
    if (!fileModel) return false;

    const QString findStr = findText.trimmed();
    if (findStr.isEmpty()) {
        DjBrowserUiFeedback::themedInformation(parent, QStringLiteral("Replace"), QStringLiteral("Enter text to find first."));
        if (updateFooter) {
            updateFooter(QStringLiteral("Replace skipped: find text is empty."), QStringLiteral("error"));
        }
        return false;
    }

    const QString dir = fileModel->folderPath();
    QDir folder(dir);
    const QStringList allFiles = folder.entryList(
        {QStringLiteral("*.mp3"), QStringLiteral("*.wav"), QStringLiteral("*.flac"), QStringLiteral("*.ogg")},
        QDir::Files | QDir::NoDotAndDotDot);

    QStringList targets;
    for (const QString& name : allFiles) {
        if (name.contains(findStr, Qt::CaseInsensitive)) targets << name;
    }

    if (targets.isEmpty()) {
        DjBrowserUiFeedback::themedInformation(
            parent,
            QStringLiteral("Replace"),
            QStringLiteral("No files found containing '%1'.").arg(findStr));
        if (updateFooter) {
            updateFooter(QStringLiteral("Replace found no matches for: %1").arg(findStr), QStringLiteral("info"));
        }
        return false;
    }

    QStringList preview;
    for (const QString& name : targets) {
        const QString newName = QString(name).replace(findStr, replaceText, Qt::CaseInsensitive);
        preview << (name + QStringLiteral(" -> ") + newName);
    }

    const auto result = DjBrowserUiFeedback::themedQuestion(
        parent,
        QStringLiteral("Replace in File Names"),
        QStringLiteral("Rename %1 file(s)?\n\n%2").arg(targets.size()).arg(preview.join(QLatin1Char('\n'))),
        QMessageBox::Yes | QMessageBox::No,
        QMessageBox::No);
    if (result != QMessageBox::Yes) return false;

    const auto renameResult = DjBrowserTrackOps::replaceFileNamesAndSyncTracks(
        db, folder, targets, findStr, replaceText);

    fileModel->refresh();
    if (renameResult.failedCount > 0) {
        DjBrowserUiFeedback::themedWarning(
            parent,
            QStringLiteral("Replace in File Names"),
            QStringLiteral("%1 file(s) could not be renamed.").arg(renameResult.failedCount));
        if (updateFooter) {
            updateFooter(
                QStringLiteral("Replace finished: %1 renamed, %2 failed.").arg(renameResult.renamedCount).arg(renameResult.failedCount),
                QStringLiteral("error"));
        }
        return false;
    }

    if (updateFooter) {
        updateFooter(
            QStringLiteral("Replace finished: %1 file(s) renamed.").arg(renameResult.renamedCount),
            QStringLiteral("success"));
    }
    return true;
}

} // namespace DjBrowserPaneActions