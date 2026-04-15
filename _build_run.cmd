@echo off
call "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat"
set NGKS_ALLOW_DIRECT_BUILDCORE=1
"C:\Users\suppo\Desktop\NGKsSystems\NGKsDevFabEco\.venv\Scripts\python.exe" -m ngksbuildcore run --plan build_graph\release\ngksbuildcore_plan.json
