#include "AnalysisComparator.h"
#include "AudioAnalysisService.h"

#include <QDir>
#include <QDirIterator>
#include <QFile>
#include <QFileInfo>
#include <QTextStream>
#include <QDateTime>
#include <QDebug>

#include <cmath>
#include <fstream>
#include <algorithm>

// ════════════════════════════════════════════════════════════════════
// String helpers
// ════════════════════════════════════════════════════════════════════

static QString bpmVerdictStr(BpmVerdict v) {
    switch (v) {
    case BpmVerdict::EXACT:        return QStringLiteral("EXACT");
    case BpmVerdict::CLOSE:        return QStringLiteral("CLOSE");
    case BpmVerdict::FAMILY_MATCH: return QStringLiteral("FAMILY");
    case BpmVerdict::WRONG:        return QStringLiteral("WRONG");
    }
    return QStringLiteral("?");
}

static QString keyVerdictStr(KeyVerdict v) {
    switch (v) {
    case KeyVerdict::EXACT:            return QStringLiteral("EXACT");
    case KeyVerdict::ENHARMONIC:       return QStringLiteral("ENHARMONIC");
    case KeyVerdict::RELATIVE_KEY:     return QStringLiteral("RELATIVE");
    case KeyVerdict::CAMELOT_ADJACENT: return QStringLiteral("ADJACENT");
    case KeyVerdict::MODE_WRONG:       return QStringLiteral("MODE_WRONG");
    case KeyVerdict::ROOT_WRONG:       return QStringLiteral("ROOT_WRONG");
    }
    return QStringLiteral("?");
}

static QString trackVerdictStr(TrackVerdict v) {
    switch (v) {
    case TrackVerdict::PASS:      return QStringLiteral("PASS");
    case TrackVerdict::SOFT_PASS: return QStringLiteral("SOFT_PASS");
    case TrackVerdict::REVIEW:    return QStringLiteral("REVIEW");
    case TrackVerdict::FAIL:      return QStringLiteral("FAIL");
    }
    return QStringLiteral("?");
}

// ════════════════════════════════════════════════════════════════════
// CSV helpers — handle quoted fields with commas
// ════════════════════════════════════════════════════════════════════

static QStringList parseCsvLine(const QString& line)
{
    QStringList fields;
    QString current;
    bool inQuotes = false;

    for (int i = 0; i < line.size(); ++i) {
        const QChar c = line.at(i);
        if (c == '"') {
            if (inQuotes && i + 1 < line.size() && line.at(i + 1) == '"') {
                current += '"';
                ++i;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (c == ',' && !inQuotes) {
            fields.append(current.trimmed());
            current.clear();
        } else {
            current += c;
        }
    }
    fields.append(current.trimmed());
    return fields;
}

// ════════════════════════════════════════════════════════════════════
// Key normalisation tables
// ════════════════════════════════════════════════════════════════════

QString AnalysisComparator::normalizeRoot(const QString& root)
{
    // Map enharmonic equivalents to a canonical form.
    // The mapping matches our Camelot table below.
    const QString r = root.trimmed();
    if (r == "Db") return QStringLiteral("C#");
    if (r == "Eb") return QStringLiteral("D#");
    if (r == "Gb") return QStringLiteral("F#");
    if (r == "Ab") return QStringLiteral("G#");
    if (r == "Bb") return QStringLiteral("A#");
    if (r == "Cb") return QStringLiteral("B");
    if (r == "Fb") return QStringLiteral("E");
    return r;
}

int AnalysisComparator::camelotNumber(const QString& camelot)
{
    // "8B" → 8, "11A" → 11
    QString num;
    for (auto ch : camelot)
        if (ch.isDigit()) num += ch;
    return num.toInt();
}

QChar AnalysisComparator::camelotLetter(const QString& camelot)
{
    // "8B" → 'B', "11A" → 'A'
    for (int i = camelot.size() - 1; i >= 0; --i)
        if (camelot.at(i).isLetter()) return camelot.at(i).toUpper();
    return QChar('?');
}

// Camelot code from root + mode (canonical)
QString AnalysisComparator::camelotFromRootMode(const QString& root, const QString& mode)
{
    const QString nr = normalizeRoot(root);
    const bool isMajor = mode.toLower().startsWith("maj");

    // Major = B, Minor = A in Camelot
    struct Mapping { const char* root; int num; };
    static const Mapping majorMap[] = {
        {"B",  1}, {"F#", 2}, {"C#", 3}, {"G#", 4}, {"D#", 5}, {"A#", 6},
        {"F",  7}, {"C",  8}, {"G",  9}, {"D", 10}, {"A", 11}, {"E", 12}
    };
    static const Mapping minorMap[] = {
        {"G#", 1}, {"D#", 2}, {"A#", 3}, {"F",  4}, {"C",  5}, {"G",  6},
        {"D",  7}, {"A",  8}, {"E",  9}, {"B", 10}, {"F#",11}, {"C#",12}
    };

    const Mapping* map = isMajor ? majorMap : minorMap;
    const int count = 12;
    const QChar letter = isMajor ? QChar('B') : QChar('A');

    for (int i = 0; i < count; ++i) {
        if (nr == QLatin1String(map[i].root))
            return QString::number(map[i].num) + letter;
    }
    return QStringLiteral("??");
}

// Relative key: same Camelot number, flip A↔B
QString AnalysisComparator::relativeKey(const QString& camelot)
{
    const int num = camelotNumber(camelot);
    const QChar letter = camelotLetter(camelot);
    const QChar flipped = (letter == 'A') ? QChar('B') : QChar('A');
    return QString::number(num) + flipped;
}

// ════════════════════════════════════════════════════════════════════
// Baseline CSV parsing
// ════════════════════════════════════════════════════════════════════

void AnalysisComparator::parseKeyText(BaselineRow& row)
{
    // Expected format: "C major (8B)" or "C# minor (12A)"
    const QString& t = row.keyText;
    const int paren = t.indexOf('(');
    if (paren > 0) {
        row.camelotKey = t.mid(paren + 1).remove(')').trimmed();  // "8B"
        const QString before = t.left(paren).trimmed();           // "C major"
        const QStringList parts = before.split(' ', Qt::SkipEmptyParts);
        if (parts.size() >= 2) {
            row.keyRoot = parts[0];   // "C"
            row.keyMode = parts[1];   // "major"
        } else if (parts.size() == 1) {
            row.keyRoot = parts[0];
            row.keyMode = QStringLiteral("major");
        }
    } else {
        // Fallback: just the raw text
        row.camelotKey = t;
    }
}

bool AnalysisComparator::loadBaseline(const QString& csvPath)
{
    QFile f(csvPath);
    if (!f.open(QIODevice::ReadOnly | QIODevice::Text)) {
        qWarning() << "Cannot open baseline CSV:" << csvPath;
        return false;
    }

    QTextStream in(&f);
    const QString headerLine = in.readLine();
    if (headerLine.isEmpty()) return false;

    const QStringList headers = parseCsvLine(headerLine);

    // Find column indices
    const int iFilename  = headers.indexOf(QStringLiteral("Filename"));
    const int iDuration  = headers.indexOf(QStringLiteral("Duration_sec"));
    const int iBpm       = headers.indexOf(QStringLiteral("BPM"));
    const int iKey       = headers.indexOf(QStringLiteral("Key"));
    const int iEnergy    = headers.indexOf(QStringLiteral("Energy_0to10"));
    const int iLufs      = headers.indexOf(QStringLiteral("LUFS"));
    const int iGenre     = headers.indexOf(QStringLiteral("Genre"));
    const int iConfBpm   = headers.indexOf(QStringLiteral("Confidence_BPM"));
    const int iConfKey   = headers.indexOf(QStringLiteral("Confidence_Key"));

    if (iFilename < 0 || iBpm < 0 || iKey < 0) {
        qWarning() << "CSV missing required columns (Filename, BPM, Key)";
        return false;
    }

    baseline_.clear();
    while (!in.atEnd()) {
        const QString line = in.readLine();
        if (line.trimmed().isEmpty()) continue;
        const QStringList cols = parseCsvLine(line);

        BaselineRow row;
        auto col = [&](int idx) -> QString {
            return (idx >= 0 && idx < cols.size()) ? cols[idx] : QString();
        };

        row.filename    = col(iFilename);
        row.durationSec = col(iDuration).toDouble();
        row.bpm         = col(iBpm).toDouble();
        row.keyText     = col(iKey);
        row.energy      = col(iEnergy).toDouble();
        row.lufs        = col(iLufs).toDouble();
        row.genre       = col(iGenre);
        row.confBpm     = col(iConfBpm).toDouble();
        row.confKey     = col(iConfKey).toDouble();

        parseKeyText(row);
        baseline_.push_back(std::move(row));
    }

    qInfo().noquote() << QString("Loaded %1 baseline rows from CSV").arg(baseline_.size());
    return !baseline_.empty();
}

// ════════════════════════════════════════════════════════════════════
// File resolution
// ════════════════════════════════════════════════════════════════════

QString AnalysisComparator::resolveFile(const BaselineRow& row, const QString& libRoot)
{
    // Direct match in libRoot
    const QString direct = QDir(libRoot).filePath(row.filename);
    if (QFileInfo::exists(direct))
        return direct;

    // Recursive search (slower, fallback)
    QDirIterator it(libRoot, QStringList() << row.filename,
                    QDir::Files, QDirIterator::Subdirectories);
    if (it.hasNext()) {
        it.next();
        return it.filePath();
    }

    return {};
}

// ════════════════════════════════════════════════════════════════════
// Scoring
// ════════════════════════════════════════════════════════════════════

BpmVerdict AnalysisComparator::judgeBpm(double baseline, double current)
{
    const double diff = std::abs(baseline - current);
    if (diff <= 1.0) return BpmVerdict::EXACT;
    if (diff <= 3.0) return BpmVerdict::CLOSE;

    // Half/double family match
    const double half   = baseline / 2.0;
    const double dbl    = baseline * 2.0;
    if (std::abs(current - half) <= 2.0 || std::abs(current - dbl) <= 2.0)
        return BpmVerdict::FAMILY_MATCH;

    return BpmVerdict::WRONG;
}

KeyVerdict AnalysisComparator::judgeKey(const BaselineRow& baseline, const AnalysisResult& current)
{
    // current.camelotKey is e.g. "8B"
    const QString baseCamelot = baseline.camelotKey;
    const QString currCamelot = current.camelotKey;

    if (baseCamelot.isEmpty() || currCamelot.isEmpty())
        return KeyVerdict::ROOT_WRONG;

    // Exact Camelot match
    if (baseCamelot == currCamelot)
        return KeyVerdict::EXACT;

    // Same number, different letter → relative key (A↔B same number)
    const int baseNum = camelotNumber(baseCamelot);
    const int currNum = camelotNumber(currCamelot);
    const QChar baseLetter = camelotLetter(baseCamelot);
    const QChar currLetter = camelotLetter(currCamelot);

    // Enharmonic: same pitch class, so same Camelot should already
    // match above. But handle edge cases where root differs but
    // resolves to same Camelot:
    if (baseNum == currNum && baseLetter == currLetter)
        return KeyVerdict::ENHARMONIC;

    // Relative key: same number, A↔B
    if (baseNum == currNum && baseLetter != currLetter)
        return KeyVerdict::RELATIVE_KEY;

    // Camelot adjacent: ±1 on the wheel, same letter
    if (baseLetter == currLetter) {
        int diff = std::abs(baseNum - currNum);
        if (diff == 1 || diff == 11) // wrap 12→1
            return KeyVerdict::CAMELOT_ADJACENT;
    }

    // Mode wrong: same root, wrong mode (different letter but not adjacent)
    const QString baseRoot = normalizeRoot(baseline.keyRoot);
    // Parse current key root from camelot — we only have camelot for current
    // If roots match but camelot doesn't → mode wrong
    // We can't easily extract root from just camelot, so use letter comparison
    if (baseLetter != currLetter && baseNum != currNum) {
        // Check if current could be the parallel mode (same root, different mode)
        // Parallel mode shifts by 3 on Camelot wheel
        const int parallelDiff = std::abs(baseNum - currNum);
        if (parallelDiff == 3 || parallelDiff == 9)
            return KeyVerdict::MODE_WRONG;
    }

    return KeyVerdict::ROOT_WRONG;
}

TrackVerdict AnalysisComparator::judgeTrack(BpmVerdict bv, KeyVerdict kv)
{
    // PASS: both BPM and Key are exact/enharmonic
    if ((bv == BpmVerdict::EXACT || bv == BpmVerdict::CLOSE) &&
        (kv == KeyVerdict::EXACT || kv == KeyVerdict::ENHARMONIC))
        return TrackVerdict::PASS;

    // FAIL: both are wrong
    if (bv == BpmVerdict::WRONG &&
        (kv == KeyVerdict::ROOT_WRONG || kv == KeyVerdict::MODE_WRONG))
        return TrackVerdict::FAIL;

    // SOFT_PASS: BPM family match or key relative/adjacent
    if ((bv == BpmVerdict::EXACT || bv == BpmVerdict::CLOSE || bv == BpmVerdict::FAMILY_MATCH) &&
        (kv == KeyVerdict::EXACT || kv == KeyVerdict::ENHARMONIC ||
         kv == KeyVerdict::RELATIVE_KEY || kv == KeyVerdict::CAMELOT_ADJACENT))
        return TrackVerdict::SOFT_PASS;

    return TrackVerdict::REVIEW;
}

// ════════════════════════════════════════════════════════════════════
// Summary computation
// ════════════════════════════════════════════════════════════════════

void AnalysisComparator::computeSummary()
{
    summary_ = {};
    double totalBpmDelta = 0.0;
    int compared = 0;

    for (const auto& r : rows_) {
        summary_.total++;
        switch (r.trackVerdict) {
        case TrackVerdict::PASS:      summary_.pass++;      break;
        case TrackVerdict::SOFT_PASS: summary_.softPass++;   break;
        case TrackVerdict::REVIEW:    summary_.review++;     break;
        case TrackVerdict::FAIL:      summary_.fail++;       break;
        }
        switch (r.bpmVerdict) {
        case BpmVerdict::EXACT:        summary_.bpmExact++;  break;
        case BpmVerdict::CLOSE:        summary_.bpmClose++;  break;
        case BpmVerdict::FAMILY_MATCH: summary_.bpmFamily++; break;
        case BpmVerdict::WRONG:        summary_.bpmWrong++;  break;
        }
        switch (r.keyVerdict) {
        case KeyVerdict::EXACT:            summary_.keyExact++;     break;
        case KeyVerdict::ENHARMONIC:       summary_.keyEnharmonic++;break;
        case KeyVerdict::RELATIVE_KEY:     summary_.keyRelative++;  break;
        case KeyVerdict::CAMELOT_ADJACENT: summary_.keyAdjacent++;  break;
        case KeyVerdict::MODE_WRONG:       summary_.keyModeWrong++; break;
        case KeyVerdict::ROOT_WRONG:       summary_.keyRootWrong++; break;
        }
        totalBpmDelta += r.bpmDelta;
        compared++;
    }

    if (compared > 0)
        summary_.avgBpmDelta = totalBpmDelta / compared;
}

// ════════════════════════════════════════════════════════════════════
// Proof artifact writing
// ════════════════════════════════════════════════════════════════════

void AnalysisComparator::writeProofArtifacts(const QString& proofDir)
{
    QDir().mkpath(proofDir);

    // ── Per-track scorecard (TSV) ──────────────────────────────────
    {
        const QString path = QDir(proofDir).filePath(QStringLiteral("scorecard.tsv"));
        std::ofstream f(path.toStdString(), std::ios::trunc);
        f << "Filename\tBase_BPM\tCurr_BPM\tBPM_Delta\tBPM_Verdict\t"
             "Base_Key\tCurr_Key\tKey_Verdict\tTrack_Verdict\tNotes\n";
        for (const auto& r : rows_) {
            f << r.baseline.filename.toStdString() << '\t'
              << r.baseline.bpm << '\t'
              << r.current.bpm << '\t'
              << r.bpmDelta << '\t'
              << bpmVerdictStr(r.bpmVerdict).toStdString() << '\t'
              << r.baseline.camelotKey.toStdString() << '\t'
              << r.current.camelotKey.toStdString() << '\t'
              << keyVerdictStr(r.keyVerdict).toStdString() << '\t'
              << trackVerdictStr(r.trackVerdict).toStdString() << '\t'
              << r.notes.toStdString() << '\n';
        }
    }

    // ── Summary text ───────────────────────────────────────────────
    {
        const QString path = QDir(proofDir).filePath(QStringLiteral("summary.txt"));
        std::ofstream f(path.toStdString(), std::ios::trunc);
        f << "════════════════════════════════════════════════════════════════\n";
        f << "ANALYSIS_COMPARATOR_SCORING_LAYER_V1 — SUMMARY\n";
        f << "════════════════════════════════════════════════════════════════\n\n";
        f << "Total Tracks:       " << summary_.total       << "\n";
        f << "Files Not Found:    " << summary_.filesNotFound << "\n";
        f << "Analysis Errors:    " << summary_.analysisErrors << "\n\n";
        f << "── OVERALL VERDICT ──\n";
        f << "PASS:               " << summary_.pass       << "\n";
        f << "SOFT_PASS:          " << summary_.softPass    << "\n";
        f << "REVIEW:             " << summary_.review      << "\n";
        f << "FAIL:               " << summary_.fail        << "\n\n";
        f << "── BPM SCORING ──\n";
        f << "EXACT (<=1.0):      " << summary_.bpmExact   << "\n";
        f << "CLOSE (1-3):        " << summary_.bpmClose    << "\n";
        f << "FAMILY (half/dbl):  " << summary_.bpmFamily   << "\n";
        f << "WRONG:              " << summary_.bpmWrong    << "\n";
        f << "Avg BPM Delta:      " << summary_.avgBpmDelta << "\n\n";
        f << "── KEY SCORING ──\n";
        f << "EXACT:              " << summary_.keyExact      << "\n";
        f << "ENHARMONIC:         " << summary_.keyEnharmonic << "\n";
        f << "RELATIVE:           " << summary_.keyRelative   << "\n";
        f << "ADJACENT:           " << summary_.keyAdjacent   << "\n";
        f << "MODE_WRONG:         " << summary_.keyModeWrong  << "\n";
        f << "ROOT_WRONG:         " << summary_.keyRootWrong  << "\n";
        f << "════════════════════════════════════════════════════════════════\n";
    }

    // ── Per-track detail dump ──────────────────────────────────────
    {
        const QString path = QDir(proofDir).filePath(QStringLiteral("detail.txt"));
        std::ofstream f(path.toStdString(), std::ios::trunc);
        int idx = 0;
        for (const auto& r : rows_) {
            ++idx;
            f << "────────────────────────────────────────\n";
            f << "[" << idx << "] " << r.baseline.filename.toStdString() << "\n";
            f << "  Path:         " << r.resolvedPath.toStdString() << "\n";
            f << "  Base BPM:     " << r.baseline.bpm << "  Curr BPM: " << r.current.bpm
              << "  Delta: " << r.bpmDelta << "  Verdict: " << bpmVerdictStr(r.bpmVerdict).toStdString() << "\n";
            f << "  Base Key:     " << r.baseline.keyText.toStdString()
              << "  Curr Key: " << r.current.camelotKey.toStdString()
              << "  Verdict: " << keyVerdictStr(r.keyVerdict).toStdString() << "\n";
            f << "  Key Conf:     " << r.current.keyConfidence
              << "  Ambiguous: " << (r.current.keyAmbiguous ? "yes" : "no")
              << "  RunnerUp: " << r.current.keyRunnerUp.toStdString() << "\n";
            f << "  BPM Conf:     " << r.current.bpmConfidence
              << "  Family: " << r.current.bpmFamily.toStdString() << "\n";
            f << "  Track Verdict: " << trackVerdictStr(r.trackVerdict).toStdString() << "\n";
            if (!r.notes.isEmpty())
                f << "  Notes:        " << r.notes.toStdString() << "\n";
        }
    }

    qInfo().noquote() << QString("Proof artifacts written to: %1").arg(proofDir);
}

// ════════════════════════════════════════════════════════════════════
// Main entry
// ════════════════════════════════════════════════════════════════════

bool AnalysisComparator::run(const QString& csvPath,
                              const QString& libRoot,
                              const QString& proofDir)
{
    qInfo().noquote() << "════════════════════════════════════════════════════════════════";
    qInfo().noquote() << "PHASE: ANALYSIS_COMPARATOR_SCORING_LAYER_V1";
    qInfo().noquote() << "CSV:     " << csvPath;
    qInfo().noquote() << "Library: " << libRoot;
    qInfo().noquote() << "ProofDir:" << proofDir;
    qInfo().noquote() << "════════════════════════════════════════════════════════════════";

    if (!loadBaseline(csvPath))
        return false;

    AudioAnalysisService analysisSvc;
    rows_.clear();

    for (size_t i = 0; i < baseline_.size(); ++i) {
        const BaselineRow& b = baseline_[i];
        qInfo().noquote() << QString("[%1/%2] %3")
            .arg(i + 1).arg(baseline_.size()).arg(b.filename);

        ComparisonRow cr;
        cr.baseline = b;

        // Resolve file path
        cr.resolvedPath = resolveFile(b, libRoot);
        if (cr.resolvedPath.isEmpty()) {
            cr.notes = QStringLiteral("FILE_NOT_FOUND");
            cr.bpmVerdict   = BpmVerdict::WRONG;
            cr.keyVerdict   = KeyVerdict::ROOT_WRONG;
            cr.trackVerdict = TrackVerdict::FAIL;
            summary_.filesNotFound++;
            rows_.push_back(std::move(cr));
            qWarning().noquote() << "  → FILE NOT FOUND:" << b.filename;
            continue;
        }

        // Run fresh analysis
        cr.current = analysisSvc.analyzeFile(cr.resolvedPath, b.genre);
        if (!cr.current.valid) {
            cr.notes = QStringLiteral("ANALYSIS_ERROR: ") + cr.current.errorMsg;
            cr.bpmVerdict   = BpmVerdict::WRONG;
            cr.keyVerdict   = KeyVerdict::ROOT_WRONG;
            cr.trackVerdict = TrackVerdict::FAIL;
            summary_.analysisErrors++;
            rows_.push_back(std::move(cr));
            qWarning().noquote() << "  → ANALYSIS ERROR:" << cr.current.errorMsg;
            continue;
        }

        // Score
        cr.bpmDelta     = std::abs(b.bpm - cr.current.bpm);
        cr.bpmVerdict   = judgeBpm(b.bpm, cr.current.bpm);
        cr.keyVerdict   = judgeKey(b, cr.current);
        cr.trackVerdict = judgeTrack(cr.bpmVerdict, cr.keyVerdict);

        qInfo().noquote() << QString("  BPM: %1→%2 (%3)  Key: %4→%5 (%6)  → %7")
            .arg(b.bpm, 0, 'f', 1)
            .arg(cr.current.bpm, 0, 'f', 1)
            .arg(bpmVerdictStr(cr.bpmVerdict))
            .arg(b.camelotKey)
            .arg(cr.current.camelotKey)
            .arg(keyVerdictStr(cr.keyVerdict))
            .arg(trackVerdictStr(cr.trackVerdict));

        rows_.push_back(std::move(cr));
    }

    computeSummary();
    writeProofArtifacts(proofDir);

    // Print summary to stdout
    qInfo().noquote() << "════════════════════════════════════════════════════════════════";
    qInfo().noquote() << QString("SUMMARY: %1 tracks — PASS=%2 SOFT_PASS=%3 REVIEW=%4 FAIL=%5")
        .arg(summary_.total).arg(summary_.pass).arg(summary_.softPass)
        .arg(summary_.review).arg(summary_.fail);
    qInfo().noquote() << QString("BPM: exact=%1 close=%2 family=%3 wrong=%4  avgDelta=%5")
        .arg(summary_.bpmExact).arg(summary_.bpmClose)
        .arg(summary_.bpmFamily).arg(summary_.bpmWrong)
        .arg(summary_.avgBpmDelta, 0, 'f', 2);
    qInfo().noquote() << QString("KEY: exact=%1 enharmonic=%2 relative=%3 adjacent=%4 modeWrong=%5 rootWrong=%6")
        .arg(summary_.keyExact).arg(summary_.keyEnharmonic)
        .arg(summary_.keyRelative).arg(summary_.keyAdjacent)
        .arg(summary_.keyModeWrong).arg(summary_.keyRootWrong);
    qInfo().noquote() << "COMPARATOR=DONE";
    qInfo().noquote() << "════════════════════════════════════════════════════════════════";

    return true;
}
