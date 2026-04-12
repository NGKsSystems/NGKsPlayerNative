import os

path = "src/ui/DeckStrip.cpp"
with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

for i, l in enumerate(lines):
    if "if (!currentPath.isEmpty() && currentPath != waveformTrackPath_) {" in l:
        # Add DECK_LOAD_REQUEST and onTrackLoaded
        insert_idx = i + 1
        lines.insert(insert_idx, "        qInfo().noquote() << QStringLiteral(\"DECK_LOAD_REQUEST deck=%1 file='%2'\").arg(deckIndex_ == 0 ? 'A' : 'B').arg(currentPath);\n")
        lines.insert(insert_idx + 1, "        qDebug().noquote() << QStringLiteral(\"DECK_SNAPSHOT_PATH deck=%1 path='%2'\").arg(deckIndex_ == 0 ? 'A' : 'B').arg(currentPath);\n")
        lines.insert(insert_idx + 2, "        waveformCtrl_.onTrackLoaded(dur);\n")
        break

for i, l in enumerate(lines):
    if "const bool fullyDecoded = bridge_->isDeckFullyDecoded(deckIndex_);" in l:
        lines.insert(i + 1, "        qDebug().noquote() << QStringLiteral(\"DECK_DECODE_READY deck=%1 ready=%2\").arg(deckIndex_ == 0 ? 'A' : 'B').arg(fullyDecoded ? 1 : 0);\n")
        break

for i, l in enumerate(lines):
    if "auto t1 = std::chrono::steady_clock::now();" in l:
        lines.insert(i, "            qInfo().noquote() << QStringLiteral(\"DECK_WAVEFORM_FETCH deck=%1 bins=%2\").arg(deckIndex_ == 0 ? 'A' : 'B').arg(wfData.size());\n")
        break

for i, l in enumerate(lines):
    if "wf->setWaveformData(wfData);" in l:
        lines.insert(i + 1, "                qInfo().noquote() << QStringLiteral(\"DECK_WAVEFORM_BOUND deck=%1 success=%2\").arg(deckIndex_ == 0 ? 'A' : 'B').arg(!wfData.empty() ? 1 : 0);\n")
        break

with open(path, "w", encoding="utf-8") as f:
    f.writelines(lines)
print("Patched DeckStrip.cpp")
