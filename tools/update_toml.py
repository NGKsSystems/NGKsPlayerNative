with open('ngksgraph.toml', encoding='utf-8') as f:
    content = f.read()

old_glob = '"src/ui/AnalysisComparator.cpp"]'
new_glob = (
    '"src/ui/AnalysisComparator.cpp", '
    '"src/ui/diagnostics/RuntimeLogSupport.cpp", '
    '"src/ui/library/LibraryPersistence.cpp", '
    '"src/ui/library/LibraryScanner.cpp", '
    '"src/ui/library/LegacyLibraryImport.cpp", '
    '"src/ui/audio/AudioProfileStore.cpp", '
    '"src/ui/diagnostics/DiagnosticsDialog.cpp", '
    '"src/ui/widgets/VisualizerWidget.cpp"]'
)

if old_glob not in content:
    print("ERROR: Could not find insertion point")
else:
    content = content.replace(old_glob, new_glob)
    with open('ngksgraph.toml', 'w', encoding='utf-8', newline='\n') as f:
        f.write(content)
    print("Done. Verifying...")
    c2 = open('ngksgraph.toml', encoding='utf-8').read()
    if 'RuntimeLogSupport.cpp' in c2:
        print("PASS: RuntimeLogSupport.cpp found in toml")
    if 'VisualizerWidget.cpp' in c2:
        print("PASS: VisualizerWidget.cpp found in toml")
