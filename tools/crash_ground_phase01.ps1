$ErrorActionPreference = "Stop"

# ═══════════════════════════════════════════════════════════════════
# NGKsPlayerNative — CRT fwrite nullptr crash ground-truth runner
# Option 4 Auditability: every step writes to _proof folder
# ═══════════════════════════════════════════════════════════════════

$repo = "C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
Set-Location $repo
if ((Get-Location).Path -ne $repo) {
    Write-Host "hey stupid Fucker, wrong window again"
    exit 1
}

# ── MSVC bootstrap helper ─────────────────────────────────────────
function Invoke-MsvcCmd {
    param([string]$Cmd)
    $full = "call `"C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat`" -arch=x64 >nul 2>nul & $Cmd"
    cmd /d /c $full
}

# ── Activate venv + vendored ngksgraph ────────────────────────────
$venvPython = Join-Path $repo ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    Write-Host "FATAL: .venv not found at $venvPython"
    exit 1
}
$env:PYTHONPATH = Join-Path $repo "tools\NGKsGraph"

# ═══════════════════════════════════════════════════════════════════
# PHASE 0 — ROOT GUARD + PROOF FOLDER
# ═══════════════════════════════════════════════════════════════════
Write-Host "`n====== PHASE 0: PROOF FOLDER ======"
$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$pf = Join-Path $repo "_proof\graph_crash_ground_$ts"
New-Item -ItemType Directory -Force $pf | Out-Null
Write-Host "PF=$pf"

git status | Out-File (Join-Path $pf "01_git_status.txt") -Encoding utf8
git log -1 --oneline | Out-File (Join-Path $pf "02_git_head.txt") -Encoding utf8
(Get-Location).Path | Out-File (Join-Path $pf "03_pwd.txt") -Encoding utf8
Copy-Item (Join-Path $repo "ngksgraph.toml") (Join-Path $pf "04_ngksgraph_toml.txt")
if (Test-Path (Join-Path $repo ".vscode\settings.json")) {
    Copy-Item (Join-Path $repo ".vscode\settings.json") (Join-Path $pf "05_vscode_settings.txt")
}

$toolVersions = @()
$toolVersions += "python: $(& $venvPython --version 2>&1)"
$toolVersions += "cl: $(Invoke-MsvcCmd 'cl 2>&1 | findstr /i version')"
$toolVersions += "dumpbin: $(Invoke-MsvcCmd 'dumpbin 2>&1 | findstr /i version')"
$toolVersions += "windeployqt: $(Invoke-MsvcCmd 'where windeployqt 2>nul')"
$toolVersions | Out-File (Join-Path $pf "06_tool_versions.txt") -Encoding utf8
Write-Host "Phase 0 DONE"

# ═══════════════════════════════════════════════════════════════════
# PHASE 1 — REPRO MATRIX (build both profiles, dumpbin)
# ═══════════════════════════════════════════════════════════════════
Write-Host "`n====== PHASE 1: REPRO MATRIX ======"

foreach ($profile in @("debug", "release")) {
    Write-Host "--- Building $profile ---"
    $buildCmd = "call `"C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat`" -arch=x64 >nul 2>nul & cd /d `"$repo`" & set PYTHONPATH=$($env:PYTHONPATH) & `"$venvPython`" -u -m ngksgraph build --project . --profile $profile --target native"
    $buildLog = cmd /d /c $buildCmd 2>&1
    $buildLog | Out-File (Join-Path $pf "1${profile}_build.txt") -Encoding utf8
    Write-Host "  Build RC=$LASTEXITCODE"

    # Find the EXE
    $exePath = Join-Path $repo "build_graph\$profile\bin\native.exe"
    if (-not (Test-Path $exePath)) {
        Write-Host "  WARNING: EXE not found at $exePath"
        "EXE_NOT_FOUND" | Out-File (Join-Path $pf "1${profile}_dependents.txt") -Encoding utf8
        continue
    }

    # dumpbin /DEPENDENTS
    $prefix = if ($profile -eq "debug") { "10" } else { "20" }
    $dumpOut = Invoke-MsvcCmd "dumpbin /DEPENDENTS `"$exePath`""
    $dumpOut | Out-File (Join-Path $pf "${prefix}_${profile}_dependents.txt") -Encoding utf8

    # CRT check
    $crtCheck = $dumpOut | Select-String -Pattern "ucrt|msvcp|vcruntime" -CaseSensitive:$false
    if ($crtCheck) {
        $crtCheck | Out-File (Join-Path $pf "${prefix}_${profile}_ucrt_check.txt") -Encoding utf8
    } else {
        "NO_CRT_MATCHES_FOUND" | Out-File (Join-Path $pf "${prefix}_${profile}_ucrt_check.txt") -Encoding utf8
    }
    Write-Host "  dumpbin done -> ${prefix}_${profile}_dependents.txt"
}

# ── HARD GATE: release must NOT depend on ucrtbased.dll ──
Write-Host "`n--- CRT CONTAMINATION CHECK ---"
$releaseDepFile = Join-Path $pf "20_release_dependents.txt"
$contaminated = $false
if (Test-Path $releaseDepFile) {
    $releaseContent = Get-Content $releaseDepFile -Raw
    if ($releaseContent -match "ucrtbased\.dll") {
        Write-Host "!! HARD GATE FAIL: Release EXE depends on ucrtbased.dll -> CRT CONTAMINATION"
        Write-Host "!! Must fix ngksgraph.toml before proceeding to deploy/run."
        "CRT_CONTAMINATION=YES  release depends on ucrtbased.dll" | Out-File (Join-Path $pf "15_crt_gate.txt") -Encoding utf8
        $contaminated = $true
    } else {
        Write-Host "CRT gate PASS: release does not depend on ucrtbased.dll"
        "CRT_CONTAMINATION=NO" | Out-File (Join-Path $pf "15_crt_gate.txt") -Encoding utf8
    }
} else {
    Write-Host "!! Release build failed - no dependents file. Checking if CRT contamination fix needed."
    $contaminated = $true
    "CRT_CONTAMINATION=UNKNOWN (release build failed)" | Out-File (Join-Path $pf "15_crt_gate.txt") -Encoding utf8
}

# Save contamination result and proof path for later phases
"PF=$pf`nCONTAMINATED=$contaminated" | Out-File (Join-Path $pf "19_phase1_result.txt") -Encoding utf8
Get-Content (Join-Path $pf "15_crt_gate.txt")
Write-Host "`nPhase 1 DONE. PF=$pf"
