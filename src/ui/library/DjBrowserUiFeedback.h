#pragma once

#include <QColor>
#include <QLineEdit>
#include <QMessageBox>
#include <QPalette>
#include <QString>
#include <QWidget>

namespace DjBrowserUiFeedback {

inline QString inputStyleSheet()
{
    return QStringLiteral(
        "QLineEdit { background: #111826; color: #f4f7fb; border: 1px solid #355078; border-radius: 4px; padding: 6px 8px; selection-background-color: #f28c28; selection-color: #10141f; }"
        "QLineEdit:focus { border-color: #f28c28; }");
}

inline void applyInputChrome(QLineEdit* lineEdit)
{
    if (!lineEdit) return;

    QPalette palette = lineEdit->palette();
    palette.setColor(QPalette::Base, QColor(QStringLiteral("#111826")));
    palette.setColor(QPalette::Text, QColor(QStringLiteral("#f4f7fb")));
    palette.setColor(QPalette::PlaceholderText, QColor(QStringLiteral("#8da3be")));
    palette.setColor(QPalette::Highlight, QColor(QStringLiteral("#f28c28")));
    palette.setColor(QPalette::HighlightedText, QColor(QStringLiteral("#10141f")));
    lineEdit->setPalette(palette);
    lineEdit->setAutoFillBackground(true);
    lineEdit->setStyleSheet(inputStyleSheet());
}

inline QString dialogStyleSheet()
{
    return QStringLiteral(
        "QDialog, QMessageBox, QInputDialog { background: #0b0f17; color: #e6edf7; }"
        "QLabel { color: #e6edf7; font-size: 12px; }"
        )
        + inputStyleSheet()
        + QStringLiteral(
        "QPushButton { background: #16213e; color: #e6edf7; border: 1px solid #355078; border-radius: 4px; padding: 6px 14px; min-width: 72px; }"
        "QPushButton:hover { background: #1c2b4d; }"
        "QPushButton:pressed { background: #13213c; }"
        "QPushButton:default { border: 1px solid #f28c28; }"
        "QPlainTextEdit { background: #111826; color: #f4f7fb; border: 1px solid #355078; }");
}

inline QMessageBox::StandardButton themedQuestion(QWidget* parent,
                                                  const QString& title,
                                                  const QString& text,
                                                  QMessageBox::StandardButtons buttons,
                                                  QMessageBox::StandardButton defaultButton)
{
    QMessageBox box(parent);
    box.setIcon(QMessageBox::Question);
    box.setWindowTitle(title);
    box.setText(text);
    box.setStandardButtons(buttons);
    box.setDefaultButton(defaultButton);
    box.setStyleSheet(dialogStyleSheet());
    return static_cast<QMessageBox::StandardButton>(box.exec());
}

inline void themedInformation(QWidget* parent, const QString& title, const QString& text)
{
    QMessageBox box(parent);
    box.setIcon(QMessageBox::Information);
    box.setWindowTitle(title);
    box.setText(text);
    box.setStandardButtons(QMessageBox::Ok);
    box.setStyleSheet(dialogStyleSheet());
    box.exec();
}

inline void themedWarning(QWidget* parent, const QString& title, const QString& text)
{
    QMessageBox box(parent);
    box.setIcon(QMessageBox::Warning);
    box.setWindowTitle(title);
    box.setText(text);
    box.setStandardButtons(QMessageBox::Ok);
    box.setStyleSheet(dialogStyleSheet());
    box.exec();
}

inline QString footerStyleForTone(const QString& tone)
{
    QString color = QStringLiteral("#8da3be");
    if (tone == QStringLiteral("busy")) color = QStringLiteral("#f2c14e");
    else if (tone == QStringLiteral("success")) color = QStringLiteral("#75d38b");
    else if (tone == QStringLiteral("error")) color = QStringLiteral("#ff8a8a");

    return QStringLiteral(
        "QLabel { background: #0f1726; color: %1; border-top: 1px solid #1b2a3d; padding: 6px 10px; font-size: 11px; }")
        .arg(color);
}

} // namespace DjBrowserUiFeedback