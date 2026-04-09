#include "TagEditorView.h"
#include "AnalysisQualityFlag.h"
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QGridLayout>
#include <QGroupBox>
#include <QLabel>
#include <QLineEdit>
#include <QTextEdit>
#include <QPushButton>
#include <QComboBox>
#include <QFileDialog>
#include <QFrame>
#include <QScrollArea>
#include <QMessageBox>
#include <QDebug>

// ── Stylesheet ─────────────────────────────────────────────────────
static const char* kPageStyle = R"(
    /* ── Page canvas: dark charcoal-navy DJ surface ── */
    QWidget#tagEditorPage {
        background: #0c1420;
    }

    /* ── Force child containers transparent so page bg shows ── */
    QWidget#tagEditorPage > QWidget { background: transparent; }
    QScrollArea QWidget { background: transparent; }

    /* ── Typography ── */
    QLabel {
        color: #8898a8;
        font-size: 12px;
        font-family: "Segoe UI", sans-serif;
        background: transparent;
    }

    /* ── Text inputs: dark recessed fields ── */
    QLineEdit, QTextEdit {
        background: #0a1420;
        color: #d0dce8;
        border: 1px solid #1e2e42;
        border-radius: 5px;
        padding: 7px 10px;
        font-size: 13px;
        font-family: "Segoe UI", sans-serif;
        selection-background-color: #1e3858;
    }
    QLineEdit:focus, QTextEdit:focus {
        border: 1px solid #3890d0;
        background: #0c1624;
    }
    QLineEdit:hover, QTextEdit:hover {
        border-color: #283c54;
    }
    QLineEdit:disabled, QTextEdit:disabled {
        background: #080e18;
        color: #303c48;
        border-color: #141e2a;
    }

    /* ── Buttons: dark filled, product style ── */
    QPushButton {
        background: #141e2e;
        color: #90a0b8;
        border: 1px solid #1e2e42;
        border-radius: 5px;
        padding: 8px 18px;
        font-size: 12px;
        font-weight: 600;
        font-family: "Segoe UI", sans-serif;
    }
    QPushButton:hover {
        background: #1a283e;
        border-color: #2a3e58;
        color: #c0d0e4;
    }
    QPushButton:pressed {
        background: #101824;
        border-color: #3890d0;
    }
    QPushButton:disabled {
        background: #0c1018;
        color: #283040;
        border-color: #141c28;
    }

    /* ── Group boxes: dark card panels with warm accent rail ── */
    QGroupBox {
        background: #111c2a;
        border: 1px solid #1a2840;
        border-left: 2px solid #3a2818;
        border-radius: 8px;
        margin-top: 18px;
        padding: 24px 14px 14px 14px;
        font-size: 10px;
        font-weight: 700;
        font-family: "Segoe UI", sans-serif;
        letter-spacing: 1.5px;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        left: 14px;
        padding: 3px 10px;
        border-radius: 4px;
        color: #e88040;
        background: #0e1824;
        border: 1px solid #1a2840;
    }

    /* ── Combo boxes ── */
    QComboBox {
        background: #0a1420;
        color: #d0dce8;
        border: 1px solid #1e2e42;
        border-radius: 5px;
        padding: 7px 10px;
        font-size: 13px;
        font-family: "Segoe UI", sans-serif;
    }
    QComboBox:hover { border-color: #283c54; }
    QComboBox:focus { border: 1px solid #3890d0; }
    QComboBox::drop-down {
        border: none; width: 24px;
        subcontrol-position: right center;
    }
    QComboBox QAbstractItemView {
        background: #0e1824;
        color: #d0dce8;
        selection-background-color: #1e3858;
        border: 1px solid #1e2e42;
        border-radius: 4px;
        outline: none;
    }

    /* ── Scroll ── */
    QScrollArea { background: transparent; border: none; }
    QScrollBar:vertical {
        background: transparent; width: 6px;
        border: none; border-radius: 3px; margin: 2px;
    }
    QScrollBar::handle:vertical {
        background: #1e2e42; border-radius: 3px; min-height: 30px;
    }
    QScrollBar::handle:vertical:hover { background: #2a3e58; }
    QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
        height: 0; background: none;
    }
    QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
        background: none;
    }
)";

// ── Constructor ────────────────────────────────────────────────────

TagEditorView::TagEditorView(QWidget* parent)
    : QWidget(parent)
{
    setObjectName(QStringLiteral("tagEditorPage"));
    setAttribute(Qt::WA_StyledBackground, true);
    controller_ = new TagEditorController(this);

    connect(controller_, &TagEditorController::fileLoaded,
            this, &TagEditorView::onFileLoaded);
    connect(controller_, &TagEditorController::dirtyChanged,
            this, &TagEditorView::onDirtyChanged);
    connect(controller_, &TagEditorController::saveResult,
            this, &TagEditorView::onSaveResult);
    connect(controller_, &TagEditorController::analysisStarted, this, [this]() {
        analyzeBtn_->setEnabled(false);
        clearAnalysisBtn_->setEnabled(false);
        analyzeBtn_->setText(QStringLiteral("Analyzing..."));
        statusLabel_->setText(QStringLiteral("Analyzing..."));
        statusLabel_->setStyleSheet(QStringLiteral(
            "color:#5090c0; font-size:11px; font-weight:700;"
            " letter-spacing:0.5px; padding:0 14px;"));
    });
    connect(controller_, &TagEditorController::analysisFinished,
            this, [this](const AnalysisResult& result) {
        analyzeBtn_->setEnabled(controller_->hasFile());
        clearAnalysisBtn_->setEnabled(controller_->hasFile());
        analyzeBtn_->setText(QStringLiteral("Analyze"));
        if (result.valid) {
            statusLabel_->setText(QStringLiteral("Analysis complete"));
            statusLabel_->setStyleSheet(QStringLiteral(
                "color:#4a8a5a; font-size:11px; font-weight:700;"
                " letter-spacing:0.5px; padding:0 14px;"));
        } else {
            statusLabel_->setText(QStringLiteral("Analysis failed"));
            statusLabel_->setStyleSheet(QStringLiteral(
                "color:#cc4444; font-size:11px; font-weight:700;"
                " letter-spacing:0.5px; padding:0 14px;"));
        }
    });

    buildUi();
}

// ── Public API ─────────────────────────────────────────────────────

void TagEditorView::openFile(const QString& path)
{
    controller_->loadFile(path);
}

// ── Build UI ───────────────────────────────────────────────────────

void TagEditorView::buildUi()
{
    setStyleSheet(QString::fromLatin1(kPageStyle));

    auto* root = new QVBoxLayout(this);
    root->setContentsMargins(20, 12, 20, 12);
    root->setSpacing(8);

    // ── Top bar ──
    {
        auto* topBar = new QHBoxLayout();
        topBar->setSpacing(10);

        auto* backBtn = new QPushButton(QStringLiteral("<  Library"), this);
        backBtn->setCursor(Qt::PointingHandCursor);
        backBtn->setStyleSheet(QStringLiteral(
            "QPushButton { padding:8px 16px; font-size:12px;"
            " background:#141e2e; border:1px solid #1e2e42;"
            " border-radius:5px; color:#708090; }"
            "QPushButton:hover { color:#b0c0d4; border-color:#2a3e58;"
            " background:#1a283e; }"));
        connect(backBtn, &QPushButton::clicked, this, &TagEditorView::backRequested);
        topBar->addWidget(backBtn);

        auto* pageTitle = new QLabel(QStringLiteral("TAG EDITOR"), this);
        pageTitle->setStyleSheet(QStringLiteral(
            "font-size:20px; font-weight:700; color:#e88040;"
            " letter-spacing:2px; padding-left:4px;"));
        topBar->addWidget(pageTitle);
        topBar->addStretch(1);

        auto* singleBtn = new QPushButton(QStringLiteral("Single File"), this);
        singleBtn->setCheckable(true);
        singleBtn->setChecked(true);
        singleBtn->setStyleSheet(QStringLiteral(
            "QPushButton { background:#0e1e34;"
            " border:1px solid #1e3050; border-radius:5px;"
            " padding:7px 16px; font-size:11px; color:#5090c0; font-weight:700;"
            " letter-spacing:0.5px; }"
            "QPushButton:hover { background:#142640; border-color:#2a4060; }"
            "QPushButton:checked { background:#162e4a; border-color:#3080c0;"
            "  color:#70b8e8; }"));
        topBar->addWidget(singleBtn);

        auto* batchBtn = new QPushButton(QStringLiteral("Batch"), this);
        batchBtn->setEnabled(false);
        batchBtn->setToolTip(QStringLiteral("Batch editing coming soon"));
        batchBtn->setStyleSheet(QStringLiteral(
            "QPushButton { padding:7px 16px; font-size:11px; letter-spacing:0.5px; }"));
        topBar->addWidget(batchBtn);

        topBar->addSpacing(14);

        auto* browseBtn = new QPushButton(QStringLiteral("Open File..."), this);
        browseBtn->setCursor(Qt::PointingHandCursor);
        browseBtn->setStyleSheet(QStringLiteral(
            "QPushButton { padding:8px 20px; font-size:12px;"
            " background:#141e2e; border:1px solid #1e3050;"
            " color:#6098c0; font-weight:600; border-radius:5px; }"
            "QPushButton:hover { border-color:#2a4060; color:#90c0e0;"
            " background:#1a283e; }"));
        connect(browseBtn, &QPushButton::clicked, this, &TagEditorView::onBrowseFile);
        topBar->addWidget(browseBtn);
        root->addLayout(topBar);
    }

    // ── File path display ──
    filePathLabel_ = new QLabel(QStringLiteral("No file loaded"), this);
    filePathLabel_->setStyleSheet(QStringLiteral(
        "background:#0a1220; color:#506070; padding:8px 14px;"
        " border:1px solid #1a2838; border-radius:5px;"
        " font-size:12px; font-family:'Segoe UI',sans-serif;"));
    filePathLabel_->setWordWrap(true);
    root->addWidget(filePathLabel_);

    // ── Center: left (metadata + DJ) | right (art + analysis) ──
    auto* center = new QHBoxLayout();
    center->setSpacing(18);

    // ═══ LEFT COLUMN ═══
    {
        auto* leftScroll = new QScrollArea(this);
        leftScroll->setFrameShape(QFrame::NoFrame);
        leftScroll->setWidgetResizable(true);
        leftScroll->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
        leftScroll->viewport()->setStyleSheet(QStringLiteral("background:transparent;"));
        auto* leftWidget = new QWidget();
        leftWidget->setStyleSheet(QStringLiteral("background:transparent;"));
        auto* leftCol = new QVBoxLayout(leftWidget);
        leftCol->setContentsMargins(0, 0, 8, 0);
        leftCol->setSpacing(12);

        // ── Metadata group ──
        auto* metaGroup = new QGroupBox(QStringLiteral("METADATA"), leftWidget);
        auto* metaGrid = new QGridLayout(metaGroup);
        metaGrid->setSpacing(8);
        metaGrid->setContentsMargins(16, 24, 16, 14);

        int row = 0;
        auto addField = [&](const QString& label, QLineEdit*& edit) {
            auto* lbl = new QLabel(label, metaGroup);
            lbl->setFixedWidth(90);
            lbl->setStyleSheet(QStringLiteral(
                "font-size:11px; color:#6080a0; font-weight:600;"
                " letter-spacing:0.3px; text-transform:uppercase;"));
            edit = new QLineEdit(metaGroup);
            connect(edit, &QLineEdit::textEdited, this, &TagEditorView::onFieldEdited);
            metaGrid->addWidget(lbl, row, 0);
            metaGrid->addWidget(edit, row, 1);
            row++;
        };

        addField(QStringLiteral("Title"),        titleEdit_);
        addField(QStringLiteral("Artist"),       artistEdit_);
        addField(QStringLiteral("Album"),        albumEdit_);
        addField(QStringLiteral("Album Artist"), albumArtistEdit_);
        addField(QStringLiteral("Genre"),        genreEdit_);
        addField(QStringLiteral("Year"),         yearEdit_);
        addField(QStringLiteral("Track #"),      trackNumEdit_);
        addField(QStringLiteral("Disc #"),       discNumEdit_);
        addField(QStringLiteral("BPM"),          bpmEdit_);
        addField(QStringLiteral("Key"),          keyEdit_);

        auto* commLabel = new QLabel(QStringLiteral("Comments"), metaGroup);
        commLabel->setStyleSheet(QStringLiteral(
            "font-size:11px; color:#6080a0; font-weight:600;"
            " letter-spacing:0.3px;"));
        commentsEdit_ = new QTextEdit(metaGroup);
        commentsEdit_->setMaximumHeight(64);
        connect(commentsEdit_, &QTextEdit::textChanged, this, &TagEditorView::onFieldEdited);
        metaGrid->addWidget(commLabel, row, 0, Qt::AlignTop);
        metaGrid->addWidget(commentsEdit_, row, 1);
        row++;

        leftCol->addWidget(metaGroup);

        // ── DJ Workflow group ──
        auto* djGroup = new QGroupBox(QStringLiteral("DJ WORKFLOW"), leftWidget);
        auto* djGrid = new QGridLayout(djGroup);
        djGrid->setSpacing(8);
        djGrid->setContentsMargins(16, 24, 16, 14);
        int djRow = 0;

        auto addDjLabel = [&](const QString& text, QWidget* parent) -> QLabel* {
            auto* lbl = new QLabel(text, parent);
            lbl->setFixedWidth(90);
            lbl->setStyleSheet(QStringLiteral(
                "font-size:11px; color:#6080a0; font-weight:600;"
                " letter-spacing:0.3px;"));
            return lbl;
        };

        djGrid->addWidget(addDjLabel(QStringLiteral("Rating"), djGroup), djRow, 0);
        ratingCombo_ = new QComboBox(djGroup);
        ratingCombo_->addItems({QStringLiteral("--"),
            QStringLiteral("1"), QStringLiteral("2"),
            QStringLiteral("3"), QStringLiteral("4"), QStringLiteral("5")});
        connect(ratingCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged),
                this, &TagEditorView::onFieldEdited);
        djGrid->addWidget(ratingCombo_, djRow, 1);
        djRow++;

        djGrid->addWidget(addDjLabel(QStringLiteral("Color"), djGroup), djRow, 0);
        colorCombo_ = new QComboBox(djGroup);
        colorCombo_->addItems({QStringLiteral("None"), QStringLiteral("Red"),
            QStringLiteral("Orange"), QStringLiteral("Yellow"),
            QStringLiteral("Green"), QStringLiteral("Blue"),
            QStringLiteral("Purple"), QStringLiteral("Pink")});
        connect(colorCombo_, QOverload<int>::of(&QComboBox::currentIndexChanged),
                this, &TagEditorView::onFieldEdited);
        djGrid->addWidget(colorCombo_, djRow, 1);
        djRow++;

        djGrid->addWidget(addDjLabel(QStringLiteral("Labels"), djGroup), djRow, 0);
        labelsEdit_ = new QLineEdit(djGroup);
        labelsEdit_->setPlaceholderText(QStringLiteral("comma-separated labels"));
        connect(labelsEdit_, &QLineEdit::textEdited, this, &TagEditorView::onFieldEdited);
        djGrid->addWidget(labelsEdit_, djRow, 1);
        djRow++;

        djGrid->addWidget(addDjLabel(QStringLiteral("DJ Notes"), djGroup), djRow, 0,
                          Qt::AlignTop);
        djNotesEdit_ = new QTextEdit(djGroup);
        djNotesEdit_->setMaximumHeight(56);
        djNotesEdit_->setPlaceholderText(QStringLiteral("Mix notes, transition cues..."));
        connect(djNotesEdit_, &QTextEdit::textChanged, this, &TagEditorView::onFieldEdited);
        djGrid->addWidget(djNotesEdit_, djRow, 1);
        djRow++;

        leftCol->addWidget(djGroup);
        leftCol->addStretch(1);

        leftScroll->setWidget(leftWidget);
        center->addWidget(leftScroll, 5);
    }

    // ═══ RIGHT COLUMN ═══
    {
        auto* rightScroll = new QScrollArea(this);
        rightScroll->setFrameShape(QFrame::NoFrame);
        rightScroll->setWidgetResizable(true);
        rightScroll->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
        rightScroll->viewport()->setStyleSheet(QStringLiteral("background:transparent;"));
        auto* rightWidget = new QWidget();
        rightWidget->setStyleSheet(QStringLiteral("background:transparent;"));
        auto* rightCol = new QVBoxLayout(rightWidget);
        rightCol->setContentsMargins(8, 0, 0, 0);
        rightCol->setSpacing(12);

        // ── Album Art group ──
        auto* artGroup = new QGroupBox(QStringLiteral("ALBUM ART"), rightWidget);
        artGroup->setStyleSheet(QStringLiteral(
            "QGroupBox { border-left:2px solid #182840; }"
            "QGroupBox::title { color:#3890d0; }"));
        auto* artLayout = new QVBoxLayout(artGroup);
        artLayout->setContentsMargins(16, 24, 16, 14);
        artLayout->setSpacing(12);

        artPreview_ = new QLabel(artGroup);
        artPreview_->setFixedSize(240, 240);
        artPreview_->setAlignment(Qt::AlignCenter);
        artPreview_->setStyleSheet(QStringLiteral(
            "background:#080e1a;"
            " border:2px solid #1a2840; border-radius:8px;"
            " color:#304060; font-size:14px; font-weight:700;"));
        artPreview_->setText(QStringLiteral("No Art"));
        artLayout->addWidget(artPreview_, 0, Qt::AlignCenter);

        auto* artBtnRow = new QHBoxLayout();
        artBtnRow->setSpacing(8);
        replaceArtBtn_ = new QPushButton(QStringLiteral("Replace Art"), artGroup);
        removeArtBtn_ = new QPushButton(QStringLiteral("Remove Art"), artGroup);
        replaceArtBtn_->setCursor(Qt::PointingHandCursor);
        removeArtBtn_->setCursor(Qt::PointingHandCursor);
        connect(replaceArtBtn_, &QPushButton::clicked, this, &TagEditorView::onReplaceArt);
        connect(removeArtBtn_, &QPushButton::clicked, this, &TagEditorView::onRemoveArt);
        artBtnRow->addWidget(replaceArtBtn_);
        artBtnRow->addWidget(removeArtBtn_);
        artLayout->addLayout(artBtnRow);
        rightCol->addWidget(artGroup);

        // ── Analysis panel helper ──
        auto makeAnalRow = [](QGridLayout* g, int r,
                              const QString& l1, QLabel*& v1,
                              const QString& l2, QLabel*& v2,
                              QWidget* parent) {
            auto* la = new QLabel(l1, parent);
            la->setStyleSheet(QStringLiteral(
                "font-size:10px; color:#4080a8; font-weight:700;"
                " letter-spacing:0.5px; text-transform:uppercase;"));
            v1 = new QLabel(QStringLiteral("--"), parent);
            v1->setStyleSheet(QStringLiteral(
                "font-size:13px; color:#c8d4e4; font-weight:500;"));
            g->addWidget(la, r, 0);
            g->addWidget(v1, r, 1);
            if (!l2.isEmpty()) {
                auto* lb = new QLabel(l2, parent);
                lb->setStyleSheet(QStringLiteral(
                    "font-size:10px; color:#4080a8; font-weight:700;"
                    " letter-spacing:0.5px; text-transform:uppercase;"));
                v2 = new QLabel(QStringLiteral("--"), parent);
                v2->setStyleSheet(QStringLiteral(
                    "font-size:13px; color:#c8d4e4; font-weight:500;"));
                g->addWidget(lb, r, 2);
                g->addWidget(v2, r, 3);
            }
        };

        // ── Audio Analysis ──
        {
            auto* grp = new QGroupBox(QStringLiteral("AUDIO ANALYSIS"), rightWidget);
            grp->setStyleSheet(QStringLiteral(
                "QGroupBox { border-left:2px solid #182840; }"
                "QGroupBox::title { color:#3890d0; }"));
            auto* g = new QGridLayout(grp);
            g->setSpacing(8);
            g->setContentsMargins(16, 24, 16, 12);
            g->setColumnStretch(1, 1);
            g->setColumnStretch(3, 1);
            makeAnalRow(g, 0, QStringLiteral("Energy"),   analysisEnergyVal_,
                              QStringLiteral("Loudness"), analysisLoudnessVal_, grp);
            makeAnalRow(g, 1, QStringLiteral("Cue In"),   analysisCueInVal_,
                              QStringLiteral("Cue Out"),  analysisCueOutVal_, grp);

            // BPM diagnostics — spans full row
            {
                auto* bpmLbl = new QLabel(QStringLiteral("BPM Detail"), grp);
                bpmLbl->setStyleSheet(QStringLiteral(
                    "font-size:10px; color:#4080a8; font-weight:700;"
                    " letter-spacing:0.5px; text-transform:uppercase;"));
                analysisBpmDiagVal_ = new QLabel(QStringLiteral("--"), grp);
                analysisBpmDiagVal_->setStyleSheet(QStringLiteral(
                    "font-size:12px; color:#88c8a0; font-weight:500;"));
                g->addWidget(bpmLbl, 2, 0);
                g->addWidget(analysisBpmDiagVal_, 2, 1, 1, 3);
            }
            rightCol->addWidget(grp);
        }

        // ── Auto DJ Features ──
        {
            auto* grp = new QGroupBox(QStringLiteral("AUTO DJ FEATURES"), rightWidget);
            grp->setStyleSheet(QStringLiteral(
                "QGroupBox { border-left:2px solid #182840; }"
                "QGroupBox::title { color:#3890d0; }"));
            auto* g = new QGridLayout(grp);
            g->setSpacing(8);
            g->setContentsMargins(16, 24, 16, 12);
            g->setColumnStretch(1, 1);
            g->setColumnStretch(3, 1);
            makeAnalRow(g, 0, QStringLiteral("Dance"),     analysisDanceVal_,
                              QStringLiteral("Acoustic"),  analysisAcousticVal_, grp);
            makeAnalRow(g, 1, QStringLiteral("Instrmntl"), analysisInstrumVal_,
                              QStringLiteral("Liveness"),  analysisLivenessVal_, grp);
            rightCol->addWidget(grp);
        }

        // ── Pro Analysis ──
        {
            auto* grp = new QGroupBox(QStringLiteral("PRO ANALYSIS"), rightWidget);
            grp->setStyleSheet(QStringLiteral(
                "QGroupBox { border-left:2px solid #182840; }"
                "QGroupBox::title { color:#3890d0; }"));
            auto* g = new QGridLayout(grp);
            g->setSpacing(8);
            g->setContentsMargins(16, 24, 16, 12);
            g->setColumnStretch(1, 1);
            g->setColumnStretch(3, 1);
            makeAnalRow(g, 0, QStringLiteral("Camelot"), analysisCamelotVal_,
                              QStringLiteral("LRA"),     analysisLRAVal_, grp);
            {
                // Transition difficulty — single label in row 1
                QLabel* dummy = nullptr;
                makeAnalRow(g, 1, QStringLiteral("Trans.Diff"), analysisTransDiffVal_,
                                  QStringLiteral(""),           dummy, grp);
            }
            rightCol->addWidget(grp);
        }

        // ── Analysis Quality Badge ──
        {
            analysisQualityBadge_ = new QLabel(QStringLiteral(""), rightWidget);
            analysisQualityBadge_->setFixedHeight(24);
            analysisQualityBadge_->setContentsMargins(12, 0, 12, 0);
            analysisQualityBadge_->setStyleSheet(QStringLiteral(
                "font-size:11px; color:#607888; font-weight:600;"
                " letter-spacing:0.3px; padding:3px 10px;"
                " border-radius:10px; background:transparent;"));
            analysisQualityBadge_->setVisible(false);
            rightCol->addWidget(analysisQualityBadge_);
        }

        rightCol->addStretch(1);
        rightScroll->setWidget(rightWidget);
        center->addWidget(rightScroll, 3);
    }

    root->addLayout(center, 1);

    // ── Bottom bar ──
    {
        auto* sep = new QFrame(this);
        sep->setFrameShape(QFrame::HLine);
        sep->setFixedHeight(1);
        sep->setStyleSheet(QStringLiteral(
            "background:#1a2840; border:none;"));
        root->addWidget(sep);

        auto* bottomBar = new QHBoxLayout();
        bottomBar->setSpacing(10);

        saveBtn_ = new QPushButton(QStringLiteral("Save"), this);
        revertBtn_ = new QPushButton(QStringLiteral("Revert"), this);
        analyzeBtn_ = new QPushButton(QStringLiteral("Analyze"), this);
        clearAnalysisBtn_ = new QPushButton(QStringLiteral("Clear Analysis"), this);

        saveBtn_->setStyleSheet(QStringLiteral(
            "QPushButton { background:#14281e;"
            " border:1px solid #1e4030; color:#50b870;"
            " font-weight:700; padding:9px 22px; border-radius:5px;"
            " font-size:12px; letter-spacing:0.5px; }"
            "QPushButton:hover { background:#1a3428;"
            " border-color:#288048; color:#70d890; }"
            "QPushButton:disabled { background:#0c1018;"
            " color:#1e2e24; border-color:#141e1a; }"));
        revertBtn_->setStyleSheet(QStringLiteral(
            "QPushButton { background:#201418;"
            " border:1px solid #3a1e22; color:#a06060;"
            " font-weight:700; padding:9px 22px; border-radius:5px;"
            " font-size:12px; letter-spacing:0.5px; }"
            "QPushButton:hover { background:#2a1a1e;"
            " border-color:#4a2a2e; color:#c08080; }"
            "QPushButton:disabled { background:#0c1018;"
            " color:#2a1e1e; border-color:#1a1414; }"));
        analyzeBtn_->setStyleSheet(QStringLiteral(
            "QPushButton { background:#0e1e34;"
            " border:1px solid #1e3050; color:#5090c0;"
            " font-weight:700; padding:9px 22px; border-radius:5px;"
            " font-size:12px; letter-spacing:0.5px; }"
            "QPushButton:hover { background:#142840;"
            " border-color:#2a4060; color:#70b8e0; }"
            "QPushButton:disabled { background:#0c1018;"
            " color:#1e2a38; border-color:#141e2a; }"));
        clearAnalysisBtn_->setStyleSheet(QStringLiteral(
            "QPushButton { background:#2a1a0e;"
            " border:1px solid #4a3020; color:#c08040;"
            " font-weight:700; padding:9px 22px; border-radius:5px;"
            " font-size:12px; letter-spacing:0.5px; }"
            "QPushButton:hover { background:#3a2418;"
            " border-color:#5a4030; color:#e0a060; }"
            "QPushButton:disabled { background:#0c1018;"
            " color:#2a2218; border-color:#1a1410; }"));

        saveBtn_->setCursor(Qt::PointingHandCursor);
        revertBtn_->setCursor(Qt::PointingHandCursor);
        analyzeBtn_->setCursor(Qt::PointingHandCursor);
        clearAnalysisBtn_->setCursor(Qt::PointingHandCursor);
        saveBtn_->setEnabled(false);
        revertBtn_->setEnabled(false);
        analyzeBtn_->setEnabled(false);
        clearAnalysisBtn_->setEnabled(false);

        connect(saveBtn_, &QPushButton::clicked, this, [this]() {
            controller_->updateFields(
                titleEdit_->text(), artistEdit_->text(),
                albumEdit_->text(), albumArtistEdit_->text(),
                genreEdit_->text(), yearEdit_->text(),
                trackNumEdit_->text(), discNumEdit_->text(),
                bpmEdit_->text(), keyEdit_->text(),
                commentsEdit_->toPlainText());
            controller_->saveFile();
        });
        connect(revertBtn_, &QPushButton::clicked, this, [this]() {
            controller_->revertChanges();
        });
        connect(analyzeBtn_, &QPushButton::clicked, this, [this]() {
            controller_->runAnalysis();
        });
        connect(clearAnalysisBtn_, &QPushButton::clicked, this, [this]() {
            controller_->clearAnalysis();
        });

        bottomBar->addWidget(saveBtn_);
        bottomBar->addWidget(revertBtn_);
        bottomBar->addWidget(analyzeBtn_);
        bottomBar->addWidget(clearAnalysisBtn_);
        bottomBar->addStretch(1);

        statusLabel_ = new QLabel(QStringLiteral("Clean"), this);
        statusLabel_->setStyleSheet(QStringLiteral(
            "color:#4a8a5a; font-size:11px; font-weight:700;"
            " letter-spacing:0.5px; padding:0 14px;"));
        bottomBar->addWidget(statusLabel_);
        root->addLayout(bottomBar);
    }
}

// ── Populate fields from data ──────────────────────────────────────

void TagEditorView::populateFields(const TrackTagData& data)
{
    populatingFields_ = true;

    filePathLabel_->setText(data.sourceFilePath.isEmpty()
        ? QStringLiteral("No file loaded") : data.sourceFilePath);

    titleEdit_->setText(data.title);
    artistEdit_->setText(data.artist);
    albumEdit_->setText(data.album);
    albumArtistEdit_->setText(data.albumArtist);
    genreEdit_->setText(data.genre);
    yearEdit_->setText(data.year);
    trackNumEdit_->setText(data.trackNumber);
    discNumEdit_->setText(data.discNumber);
    bpmEdit_->setText(data.bpm);
    keyEdit_->setText(data.musicalKey);
    commentsEdit_->setPlainText(data.comments);

    // Album art preview
    if (data.hasAlbumArt && !data.albumArt.isNull()) {
        artPreview_->setPixmap(data.albumArt.scaled(
            artPreview_->size(), Qt::KeepAspectRatio, Qt::SmoothTransformation));
    } else {
        artPreview_->setPixmap(QPixmap());
        artPreview_->setText(QStringLiteral("No Art"));
    }

    // Reset DJ workflow fields
    ratingCombo_->setCurrentIndex(0);
    colorCombo_->setCurrentIndex(0);
    labelsEdit_->clear();
    djNotesEdit_->clear();

    // Reset analysis displays
    clearAnalysisDisplay();

    populatingFields_ = false;
}

void TagEditorView::clearAnalysisDisplay()
{
    const auto dash = QStringLiteral("--");
    analysisEnergyVal_->setText(dash);
    analysisLoudnessVal_->setText(dash);
    analysisBpmDiagVal_->setText(dash);
    analysisCueInVal_->setText(dash);
    analysisCueOutVal_->setText(dash);
    analysisDanceVal_->setText(dash);
    analysisAcousticVal_->setText(dash);
    analysisInstrumVal_->setText(dash);
    analysisLivenessVal_->setText(dash);
    analysisCamelotVal_->setText(dash);
    analysisLRAVal_->setText(dash);
    analysisTransDiffVal_->setText(dash);

    // Hide quality badge
    analysisQualityBadge_->setVisible(false);
    analysisQualityBadge_->setText(QString());
    analysisQualityBadge_->setToolTip(QString());
}

void TagEditorView::setExtraContext(int rating, const QString& colorLabel,
                                     const QString& labels,
                                     double energy, double loudnessLUFS,
                                     double loudnessRange,
                                     const QString& cueIn, const QString& cueOut,
                                     double danceability, double acousticness,
                                     double instrumentalness, double liveness,
                                     const QString& camelotKey,
                                     double transitionDifficulty,
                                     double rawBpm,
                                     double resolvedBpm,
                                     double bpmConfidence,
                                     const QString& bpmFamily)
{
    populatingFields_ = true;

    // DJ workflow
    ratingCombo_->setCurrentIndex(qBound(0, rating, 5));
    int ci = colorCombo_->findText(colorLabel, Qt::MatchFixedString);
    colorCombo_->setCurrentIndex(ci >= 0 ? ci : 0);
    labelsEdit_->setText(labels);

    // Analysis
    const auto dash = QStringLiteral("--");
    auto fmtPct = [&](double v) -> QString {
        return v >= 0 ? QString::number(v, 'f', 1) : dash;
    };

    analysisEnergyVal_->setText(fmtPct(energy));
    analysisLoudnessVal_->setText(loudnessLUFS != 0.0
        ? QString::number(loudnessLUFS, 'f', 1) + QStringLiteral(" LUFS") : dash);
    analysisCueInVal_->setText(cueIn.isEmpty() ? dash : cueIn);
    analysisCueOutVal_->setText(cueOut.isEmpty() ? dash : cueOut);
    analysisDanceVal_->setText(fmtPct(danceability));
    analysisAcousticVal_->setText(fmtPct(acousticness));
    analysisInstrumVal_->setText(fmtPct(instrumentalness));
    analysisLivenessVal_->setText(fmtPct(liveness));
    analysisCamelotVal_->setText(camelotKey.isEmpty() ? dash : camelotKey);
    analysisLRAVal_->setText(loudnessRange != 0.0
        ? QString::number(loudnessRange, 'f', 1) + QStringLiteral(" LU") : dash);
    analysisTransDiffVal_->setText(fmtPct(transitionDifficulty));

    // BPM diagnostics
    if (resolvedBpm > 0.0 && rawBpm > 0.0) {
        // Format: "170.4 (Raw: 85.2 | Conf: 0.82 | x2)"
        QString familyTag;
        if (bpmFamily == QLatin1String("HALF"))
            familyTag = QStringLiteral("/2");
        else if (bpmFamily == QLatin1String("DOUBLE"))
            familyTag = QStringLiteral("x2");
        else
            familyTag = QStringLiteral("=");

        QString diag = QString::number(resolvedBpm, 'f', 1)
            + QStringLiteral("  (Raw: ")
            + QString::number(rawBpm, 'f', 1)
            + QStringLiteral(" | Conf: ")
            + QString::number(bpmConfidence, 'f', 2)
            + QStringLiteral(" | ") + familyTag
            + QStringLiteral(")");
        analysisBpmDiagVal_->setText(diag);

        // Color by confidence
        if (bpmConfidence >= 0.8)
            analysisBpmDiagVal_->setStyleSheet(QStringLiteral(
                "font-size:12px; color:#60d890; font-weight:600;"));
        else if (bpmConfidence >= 0.6)
            analysisBpmDiagVal_->setStyleSheet(QStringLiteral(
                "font-size:12px; color:#c8b840; font-weight:500;"));
        else
            analysisBpmDiagVal_->setStyleSheet(QStringLiteral(
                "font-size:12px; color:#c86040; font-weight:500;"));
    } else {
        analysisBpmDiagVal_->setText(dash);
        analysisBpmDiagVal_->setStyleSheet(QStringLiteral(
            "font-size:12px; color:#88c8a0; font-weight:500;"));
    }

    populatingFields_ = false;
}

// ── Slots ──────────────────────────────────────────────────────────

void TagEditorView::onFileLoaded(const TrackTagData& data)
{
    populateFields(data);

    // Populate analysis + DJ workflow fields from merged data
    setExtraContext(data.rating, data.colorLabel, data.labels,
                    data.energy, data.loudnessLUFS, data.loudnessRange,
                    data.cueIn, data.cueOut,
                    data.danceability, data.acousticness,
                    data.instrumentalness, data.liveness,
                    data.camelotKey, data.transitionDifficulty,
                    data.rawBpm, data.resolvedBpm,
                    data.bpmConfidence, data.bpmFamily);

    // Enable art buttons based on state
    const bool hasFile = controller_->hasFile();
    replaceArtBtn_->setEnabled(hasFile);
    removeArtBtn_->setEnabled(hasFile && data.hasAlbumArt);
    analyzeBtn_->setEnabled(hasFile);
    clearAnalysisBtn_->setEnabled(hasFile);

    // Update analysis quality badge
    updateQualityBadge(data);
}

void TagEditorView::onDirtyChanged(bool dirty)
{
    saveBtn_->setEnabled(dirty && controller_->hasFile());
    revertBtn_->setEnabled(dirty && controller_->hasFile());
    statusLabel_->setText(dirty ? QStringLiteral("Modified") : QStringLiteral("Clean"));
    statusLabel_->setStyleSheet(dirty
        ? QStringLiteral("color:#cc9933; font-size:11px; font-weight:700;"
          " letter-spacing:0.5px; padding:0 14px;")
        : QStringLiteral("color:#4a8a5a; font-size:11px; font-weight:700;"
          " letter-spacing:0.5px; padding:0 14px;"));
}

void TagEditorView::onSaveResult(bool success, const QString& msg)
{
    statusLabel_->setText(success ? QStringLiteral("Saved") : QStringLiteral("FAILED"));
    statusLabel_->setStyleSheet(success
        ? QStringLiteral("color:#4a8a5a; font-size:11px; font-weight:700;"
          " letter-spacing:0.5px; padding:0 14px;")
        : QStringLiteral("color:#cc4444; font-size:11px; font-weight:700;"
          " letter-spacing:0.5px; padding:0 14px;"));
    if (!success) {
        QMessageBox::warning(this, QStringLiteral("Save Failed"), msg);
    }
}

void TagEditorView::onBrowseFile()
{
    const QString path = QFileDialog::getOpenFileName(
        this, QStringLiteral("Open Audio File"), QString(),
        QStringLiteral("Audio Files (*.mp3 *.flac *.wav *.m4a *.aac *.ogg);;MP3 (*.mp3);;All Files (*)"));
    if (!path.isEmpty())
        controller_->loadFile(path);
}

void TagEditorView::onReplaceArt()
{
    const QString path = QFileDialog::getOpenFileName(
        this, QStringLiteral("Select Album Art Image"), QString(),
        QStringLiteral("Images (*.jpg *.jpeg *.png *.bmp);;All Files (*)"));
    if (path.isEmpty()) return;

    QPixmap art;
    if (art.load(path))
        controller_->replaceAlbumArt(art);
}

void TagEditorView::onRemoveArt()
{
    controller_->removeAlbumArt();
}

void TagEditorView::onFieldEdited()
{
    if (!populatingFields_ && controller_->hasFile())
        controller_->markDirty();
}

// ── Analysis Quality Badge ─────────────────────────────────────────

void TagEditorView::updateQualityBadge(const TrackTagData& data)
{
    AnalysisQualityInput input;
    input.rawBpm            = data.rawBpm;
    input.resolvedBpm       = data.resolvedBpm;
    input.bpmConfidence     = data.bpmConfidence;
    input.bpmFamily         = data.bpmFamily;
    input.bpmCandidateCount = data.bpmCandidateCount;
    input.bpmCandidateGap   = data.bpmCandidateGap;
    input.keyConfidence     = data.keyConfidence;
    input.keyAmbiguous      = data.keyAmbiguous;
    input.keyRunnerUp       = data.keyRunnerUp;
    input.keyCorrectionReason = data.keyCorrectionReason;
    input.camelotKey        = data.camelotKey;
    input.hasBpm            = data.resolvedBpm > 0.0 || !data.bpm.isEmpty();
    input.hasKey            = !data.camelotKey.isEmpty();

    auto status = AnalysisQualityEvaluator::evaluate(input);

    if (!input.hasBpm && !input.hasKey) {
        // No analysis at all — hide badge
        analysisQualityBadge_->setVisible(false);
        return;
    }

    analysisQualityBadge_->setText(status.summaryText);
    analysisQualityBadge_->setToolTip(status.tooltipText);
    analysisQualityBadge_->setVisible(true);

    switch (status.overallState) {
    case AnalysisQualityState::Clean:
        analysisQualityBadge_->setStyleSheet(QStringLiteral(
            "font-size:11px; color:#58a870; font-weight:600;"
            " letter-spacing:0.3px; padding:3px 10px;"
            " border-radius:10px; background:#0c2018;"));
        break;
    case AnalysisQualityState::Review:
        analysisQualityBadge_->setStyleSheet(QStringLiteral(
            "font-size:11px; color:#c8a840; font-weight:600;"
            " letter-spacing:0.3px; padding:3px 10px;"
            " border-radius:10px; background:#1c1808;"));
        break;
    case AnalysisQualityState::Suspicious:
        analysisQualityBadge_->setStyleSheet(QStringLiteral(
            "font-size:11px; color:#d07040; font-weight:600;"
            " letter-spacing:0.3px; padding:3px 10px;"
            " border-radius:10px; background:#1c1008;"));
        break;
    }
}
