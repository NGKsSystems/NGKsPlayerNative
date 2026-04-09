#pragma once
#include <QString>
#include <QStringList>
#include <vector>
#include "AnalysisResult.h"

// ── BPM comparison verdict ─────────────────────────────────────────
enum class BpmVerdict { EXACT, CLOSE, FAMILY_MATCH, WRONG };

// ── Key comparison verdict ─────────────────────────────────────────
enum class KeyVerdict { EXACT, ENHARMONIC, RELATIVE_KEY, CAMELOT_ADJACENT, MODE_WRONG, ROOT_WRONG };

// ── Overall track verdict ──────────────────────────────────────────
enum class TrackVerdict { PASS, SOFT_PASS, REVIEW, FAIL };

// ── One row from the baseline CSV ──────────────────────────────────
struct BaselineRow
{
    QString filename;           // bare filename, e.g. "Brooks & Dunn - Ain't …"
    double  durationSec{0.0};
    double  bpm{0.0};
    QString keyText;            // raw CSV value, e.g. "C major (8B)"
    QString camelotKey;         // parsed, e.g. "8B"
    QString keyRoot;            // parsed, e.g. "C"
    QString keyMode;            // parsed, e.g. "major"
    double  energy{0.0};
    double  lufs{0.0};
    QString genre;
    double  confBpm{0.0};
    double  confKey{0.0};
};

// ── Per-track comparison result ────────────────────────────────────
struct ComparisonRow
{
    BaselineRow     baseline;
    AnalysisResult  current;
    QString         resolvedPath;   // full path that was analyzed

    BpmVerdict      bpmVerdict;
    double          bpmDelta{0.0};  // absolute difference
    KeyVerdict      keyVerdict;
    TrackVerdict    trackVerdict;
    QString         notes;
};

// ── Aggregate summary ──────────────────────────────────────────────
struct ComparatorSummary
{
    int total{0};
    int pass{0};
    int softPass{0};
    int review{0};
    int fail{0};
    int filesNotFound{0};
    int analysisErrors{0};
    double avgBpmDelta{0.0};
    int bpmExact{0};
    int bpmClose{0};
    int bpmFamily{0};
    int bpmWrong{0};
    int keyExact{0};
    int keyEnharmonic{0};
    int keyRelative{0};
    int keyAdjacent{0};
    int keyModeWrong{0};
    int keyRootWrong{0};
};

// ── Comparator engine ──────────────────────────────────────────────
class AnalysisComparator
{
public:
    // csvPath  — path to the Electron baseline CSV
    // libRoot  — directory to search for matching filenames
    // proofDir — directory where proof artifacts are written
    bool run(const QString& csvPath,
             const QString& libRoot,
             const QString& proofDir);

    const std::vector<ComparisonRow>& rows() const { return rows_; }
    const ComparatorSummary& summary()       const { return summary_; }

private:
    bool loadBaseline(const QString& csvPath);
    QString resolveFile(const BaselineRow& row, const QString& libRoot);
    void parseKeyText(BaselineRow& row);

    BpmVerdict  judgeBpm(double baseline, double current);
    KeyVerdict  judgeKey(const BaselineRow& baseline, const AnalysisResult& current);
    TrackVerdict judgeTrack(BpmVerdict bv, KeyVerdict kv);

    void computeSummary();
    void writeProofArtifacts(const QString& proofDir);

    // Key helpers
    static QString normalizeRoot(const QString& root);
    static int     camelotNumber(const QString& camelot);
    static QChar   camelotLetter(const QString& camelot);
    static QString camelotFromRootMode(const QString& root, const QString& mode);
    static QString relativeKey(const QString& camelot);

    std::vector<BaselineRow>    baseline_;
    std::vector<ComparisonRow>  rows_;
    ComparatorSummary           summary_;
};
