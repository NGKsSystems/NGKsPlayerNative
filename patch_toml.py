import re

path = 'ngksgraph.toml'
with open(path, 'r', encoding='utf-8') as f:
    text = f.read()

bad = '''src_glob = ["src/ui/main.cpp", "src/ui/EqPanel.cpp",
"src/ui/DeckStrip.cpp", "src/ui/WaveformState.cpp",
"src/ui/TagReaderService.cpp", "src/ui/TagWriterService.cpp",
"src/ui/AlbumArtService.cpp", "src/ui/TagEditorController.cpp",
"src/ui/TagEditorView.cpp", "src/ui/TagDatabaseService.cpp",
"src/ui/AudioAnalysisService.cpp", "src/ui/KeyDetectionService.cpp",       
"src/ui/TransitionValidationService.cpp",
"src/ui/BpmResolverService.cpp", "src/ui/AnalysisQualityFlag.cpp",
"src/ui/AnalysisComparator.cpp"]'''

good = '''src_glob = ["src/ui/main.cpp", "src/ui/EqPanel.cpp",
"src/ui/DeckStrip.cpp", "src/ui/WaveformState.cpp",
"src/ui/TagReaderService.cpp", "src/ui/TagWriterService.cpp",
"src/ui/AlbumArtService.cpp", "src/ui/TagEditorController.cpp",
"src/ui/TagEditorView.cpp", "src/ui/TagDatabaseService.cpp",
"src/ui/AudioAnalysisService.cpp", "src/ui/KeyDetectionService.cpp",       
"src/ui/TransitionValidationService.cpp",
"src/ui/BpmResolverService.cpp", "src/ui/AnalysisQualityFlag.cpp",
"src/ui/AnalysisComparator.cpp",
"src/ui/library/dj/DjLibraryWidget.cpp", "src/ui/library/dj/DjLibraryModel.cpp", "src/ui/library/dj/DjLibraryDatabase.cpp"]'''

text = text.replace(bad, good)

with open(path, 'w', encoding='utf-8') as f:
    f.write(text)
print("TOML_PATCHED")
