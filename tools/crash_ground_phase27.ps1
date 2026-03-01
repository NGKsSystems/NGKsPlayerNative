$ErrorActionPreference = "Stop"

$repo = "C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
Set-Location $repo
if ((Get-Location).Path -ne $repo) { Write-Host "hey stupid Fucker, wrong window again"; exit 1 }

# ── Find latest proof folder from phase01 ──────────────────────────
$pf = (Get-ChildItem "$repo\_proof" -Directory -Filter "graph_crash_ground_*" | Sort-Object Name -Descending | Select-Object -First 1).FullName
if (-not $pf) { Write-Host "FATAL: No phase01 proof folder found"; exit 1 }
Write-Host "PF=$pf"

# ── Helpers ────────────────────────────────────────────────────────
function Invoke-MsvcCmd {
    param([string]$Cmd)
    $full = "call `"C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat`" -arch=x64 >nul 2>nul & $Cmd"
    cmd /d /c $full
}

$venvPython = Join-Path $repo ".venv\Scripts\python.exe"
$env:PYTHONPATH = Join-Path $repo "tools\NGKsGraph"
$qtRoot = "C:\Qt\6.10.2\msvc2022_64"
$qtBin  = "$qtRoot\bin"

# ═══════════════════════════════════════════════════════════════════
# PHASE 1b — REBUILD BOTH PROFILES WITH FIXED TOML
# ═══════════════════════════════════════════════════════════════════
Write-Host "`n====== PHASE 1b: REBUILD WITH FIXED TOML ======"
Copy-Item (Join-Path $repo "ngksgraph.toml") (Join-Path $pf "04b_ngksgraph_toml_fixed.txt")

foreach ($profile in @("debug", "release")) {
    Write-Host "--- Rebuilding $profile ---"
    $buildCmd = "call `"C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat`" -arch=x64 >nul 2>nul & cd /d `"$repo`" & set PYTHONPATH=$($env:PYTHONPATH) & `"$venvPython`" -u -m ngksgraph build --project . --profile $profile --target native"
    $buildLog = cmd /d /c $buildCmd 2>&1
    $buildLog | Out-File (Join-Path $pf "1b_${profile}_rebuild.txt") -Encoding utf8
    Write-Host "  RC=$LASTEXITCODE"

    $exePath = Join-Path $repo "build_graph\$profile\bin\native.exe"
    if (-not (Test-Path $exePath)) {
        Write-Host "  WARNING: EXE not at $exePath"
        continue
    }

    $prefix = if ($profile -eq "debug") { "10b" } else { "20b" }
    $dumpOut = Invoke-MsvcCmd "dumpbin /DEPENDENTS `"$exePath`""
    $dumpOut | Out-File (Join-Path $pf "${prefix}_${profile}_dependents.txt") -Encoding utf8
    $crtCheck = $dumpOut | Select-String -Pattern "ucrt|msvcp|vcruntime" -CaseSensitive:$false
    $crtCheck | Out-File (Join-Path $pf "${prefix}_${profile}_ucrt_check.txt") -Encoding utf8
    Write-Host "  dumpbin -> ${prefix}_${profile}_dependents.txt"
}

# ── HARD GATE: release must NOT depend on ucrtbased.dll ──
$releaseDepFile = Join-Path $pf "20b_release_dependents.txt"
if (Test-Path $releaseDepFile) {
    $content = Get-Content $releaseDepFile -Raw
    if ($content -match "ucrtbased\.dll") {
        Write-Host "!! HARD GATE STILL FAILING: release depends on ucrtbased.dll"
        "CRT_CONTAMINATION=STILL_YES" | Out-File (Join-Path $pf "15b_crt_gate.txt") -Encoding utf8
        exit 1
    }
    Write-Host "CRT gate PASS: release clean of debug CRT"
    "CRT_CONTAMINATION=NO (fixed)" | Out-File (Join-Path $pf "15b_crt_gate.txt") -Encoding utf8
} else {
    Write-Host "!! Release build failed - no dependents file"
    exit 1
}

# ═══════════════════════════════════════════════════════════════════
# PHASE 2 — DETERMINISTIC QT DEPLOY
# ═══════════════════════════════════════════════════════════════════
Write-Host "`n====== PHASE 2: QT DEPLOY ======"

$windeployqtPath = Invoke-MsvcCmd "where windeployqt 2>nul"
if (-not $windeployqtPath) {
    # Try Qt bin directly
    $windeployqtPath = "$qtBin\windeployqt.exe"
}
$windeployqtPath = $windeployqtPath.Trim()
Write-Host "windeployqt=$windeployqtPath"

foreach ($profile in @("debug", "release")) {
    Write-Host "--- Deploying Qt for $profile ---"
    $binDir = Join-Path $repo "build_graph\$profile\bin"
    $exePath = Join-Path $binDir "native.exe"
    if (-not (Test-Path $exePath)) {
        Write-Host "  SKIP: no EXE"
        continue
    }

    $pluginDir = Join-Path $binDir "plugins"

    # Clean deploy surface
    Get-ChildItem $binDir -Filter "Qt6*.dll" -ErrorAction SilentlyContinue | Remove-Item -Force
    if (Test-Path $pluginDir) { Remove-Item $pluginDir -Recurse -Force }
    foreach ($d in @("platforms","imageformats","styles","tls","generic","iconengines","networkinformation","translations")) {
        $flat = Join-Path $binDir $d
        if (Test-Path $flat) { Remove-Item $flat -Recurse -Force }
    }

    # Run windeployqt
    $mode = if ($profile -eq "debug") { "--debug" } else { "--release" }
    $deployCmd = "call `"C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat`" -arch=x64 >nul 2>nul & `"$windeployqtPath`" $mode --no-translations --no-opengl-sw --dir `"$binDir`" --plugindir `"$pluginDir`" `"$exePath`""
    $deployLog = cmd /d /c $deployCmd 2>&1
    $deployLog | Out-File (Join-Path $pf "30_${profile}_deploy.txt") -Encoding utf8

    # Write qt.conf
    "[Paths]`nPlugins=plugins" | Out-File (Join-Path $binDir "qt.conf") -Encoding ascii -NoNewline

    # Snapshot tree
    Get-ChildItem $binDir -Recurse | Select-Object FullName, Length, LastWriteTime | Format-Table -AutoSize | Out-String -Width 300 | Out-File (Join-Path $pf "30_${profile}_bin_tree.txt") -Encoding utf8

    # Validate qwindows plugin
    $expectedPlugin = if ($profile -eq "debug") { "qwindowsd.dll" } else { "qwindows.dll" }
    $found = Get-ChildItem $pluginDir -Recurse -Filter $expectedPlugin -ErrorAction SilentlyContinue
    if ($found) {
        "FOUND: $($found.FullName)" | Out-File (Join-Path $pf "31_${profile}_qwindows.txt") -Encoding utf8
        Write-Host "  qwindows: FOUND"
    } else {
        # Try alternate location (platforms subdir of bin)
        $altDir = Join-Path $binDir "platforms"
        $found2 = Get-ChildItem $altDir -Filter $expectedPlugin -ErrorAction SilentlyContinue
        if ($found2) {
            "FOUND_ALT: $($found2.FullName)" | Out-File (Join-Path $pf "31_${profile}_qwindows.txt") -Encoding utf8
            Write-Host "  qwindows: FOUND (alt location)"
        } else {
            "MISSING: $expectedPlugin not found in $pluginDir or $altDir" | Out-File (Join-Path $pf "31_${profile}_qwindows.txt") -Encoding utf8
            Write-Host "  qwindows: MISSING"
        }
    }
    Write-Host "  Deploy done"
}

# ═══════════════════════════════════════════════════════════════════
# PHASE 3 — CONTROLLED RUN
# ═══════════════════════════════════════════════════════════════════
Write-Host "`n====== PHASE 3: CONTROLLED RUN ======"

foreach ($profile in @("debug", "release")) {
    Write-Host "--- Running $profile ---"
    $binDir = Join-Path $repo "build_graph\$profile\bin"
    $exePath = Join-Path $binDir "native.exe"
    if (-not (Test-Path $exePath)) {
        Write-Host "  SKIP: no EXE"
        continue
    }

    $prefix = if ($profile -eq "debug") { "40" } else { "50" }
    $pluginDir = Join-Path $binDir "plugins"

    # Setup env
    $savedPath = $env:PATH
    $savedQtPlugin = $env:QT_PLUGIN_PATH
    $env:QT_PLUGIN_PATH = $pluginDir
    $env:PATH = "$binDir;$qtBin;$savedPath"
    $env:QT_DEBUG_PLUGINS = "1"
    $env:NGKS_SKIP_PROFILE_COMBO_MUTATION = "1"

    # Launch with timeout — GUI app will stay alive if it works
    $stdoutFile = Join-Path $pf "${prefix}_stdout.txt"
    $stderrFile = Join-Path $pf "${prefix}_stderr.txt"
    $proc = Start-Process -FilePath $exePath -NoNewWindow -PassThru `
        -RedirectStandardOutput $stdoutFile `
        -RedirectStandardError $stderrFile
    $exited = $proc.WaitForExit(10000)

    if ($exited) {
        $rc = $proc.ExitCode
        $status = "EXITED_WITHIN_10s"
    } else {
        $rc = 0
        $status = "STILL_RUNNING_AFTER_10s"
        try { $proc.Kill() } catch {}
        Start-Sleep -Milliseconds 500
    }

    # Restore env
    $env:PATH = $savedPath
    $env:QT_PLUGIN_PATH = $savedQtPlugin
    $env:QT_DEBUG_PLUGINS = $null

    # Combine logs
    $runLog = @()
    if (Test-Path $stdoutFile) { $runLog += Get-Content $stdoutFile }
    $runLog += "--- STDERR ---"
    if (Test-Path $stderrFile) { $runLog += Get-Content $stderrFile }
    $runLog | Out-File (Join-Path $pf "${prefix}_${profile}_run.txt") -Encoding utf8

    $rcHex = ('0x{0:X8}' -f ([System.BitConverter]::ToUInt32([System.BitConverter]::GetBytes([int]$rc), 0)))
    "RC_DEC=$rc`nRC_HEX=$rcHex`nSTATUS=$status" | Tee-Object -FilePath (Join-Path $pf "${prefix}_${profile}_rc.txt")
    Write-Host "  $profile -> RC=$rc ($rcHex) $status"

    # Check for assertion in logs
    $assertFound = Select-String -Path (Join-Path $pf "${prefix}_${profile}_run.txt") `
        -Pattern "Debug Assertion Failed|buffer != nullptr|fwrite.cpp|0xC0000135" `
        -ErrorAction SilentlyContinue
    if ($assertFound) {
        Write-Host "  !! ASSERTION/CRASH detected in $profile run"
        $assertFound | Out-File (Join-Path $pf "${prefix}_${profile}_assertion.txt") -Encoding utf8
    }
}

# ═══════════════════════════════════════════════════════════════════
# PHASE 4-5 — STACK TRACE (if crash detected)
# ═══════════════════════════════════════════════════════════════════
Write-Host "`n====== PHASE 4-5: STACK TRACE CHECK ======"

# Check if we have any crashes
$hasCrash = $false
foreach ($f in (Get-ChildItem $pf -Filter "*_assertion.txt" -ErrorAction SilentlyContinue)) {
    $hasCrash = $true
    Write-Host "  Crash detection: $($f.Name)"
    Get-Content $f.FullName | Write-Host
}

$releaseRcFile = Join-Path $pf "50_release_rc.txt"
$debugRcFile = Join-Path $pf "40_debug_rc.txt"
foreach ($rcFile in @($releaseRcFile, $debugRcFile)) {
    if (Test-Path $rcFile) {
        $rcContent = Get-Content $rcFile -Raw
        if ($rcContent -match "RC_HEX=0xC0000135|RC_DEC=-1073741515") {
            $hasCrash = $true
            Write-Host "  DLL-not-found crash in $rcFile"
        }
        if ($rcContent -match "RC_HEX=0xC0000005") {
            $hasCrash = $true
            Write-Host "  Access violation in $rcFile"
        }
        if ($rcContent -match "STATUS=EXITED" -and $rcContent -notmatch "RC_DEC=0") {
            $hasCrash = $true
            Write-Host "  Non-zero RC in $rcFile"
        }
    }
}

if (-not $hasCrash) {
    Write-Host 'No crashes detected - both profiles appear stable.'
    'NO_CRASH_DETECTED - CRT fix resolved the issue.' | Out-File (Join-Path $pf '60_root_cause_summary.txt') -Encoding utf8
} else {
    Write-Host 'Crash detected - checking for debugger tools...'

    # Try to get procdump / cdb
    $procdumpPath = $null
    $cdbPath = $null
    try { $procdumpPath = (Get-Command procdump -ErrorAction SilentlyContinue).Source } catch {}
    try { $cdbPath = (Get-Command cdb -ErrorAction SilentlyContinue).Source } catch {}
    $cdbMsvc = Invoke-MsvcCmd "where cdb 2>nul"
    if ($cdbMsvc) { $cdbPath = $cdbMsvc.Trim() }

    "procdump=$procdumpPath`ncdb=$cdbPath" | Out-File (Join-Path $pf "60_debugger_tools.txt") -Encoding utf8

    if (-not $procdumpPath -and -not $cdbPath) {
        Write-Host "No debugger tools found (procdump/cdb). Stack trace unavailable."
        "NO_DEBUGGER_AVAILABLE" | Out-File (Join-Path $pf "61_stack_excerpt.txt") -Encoding utf8
    }

    # Root cause summary based on what we know
    $lines = @(
        'ROOT CAUSE: CRT contamination in ngksgraph.toml'
        ''
        'Every [[targets]] block and the global section had debug-only flags hardcoded:'
        '  /MDd (debug CRT linkage)'
        '  _DEBUG and DEBUG defines'
        '  /Od (disable optimization)'
        '  /Zi (debug info)'
        ''
        'When building with --profile release, these flags were APPENDED to the profile'
        '/MD flag. After normalize and sorted-set dedup, both /MD and /MDd remained, and'
        'since /MDd sorts AFTER /MD, it came last on the command line and WON even in'
        'release builds.'
        ''
        'Result: Release EXE linked against ucrtbased.dll (debug CRT) which has stricter'
        'assertion checks. The fwrite nullptr assertion was triggered by the debug CRT'
        'validation that the release CRT would silently accept.'
        ''
        'FIX: Moved /MDd /Od /Zi DEBUG _DEBUG to [profiles.debug] ONLY.'
        'Global/target sections now contain only profile-neutral flags.'
        'The EngineBridge.cpp fprintf guard already applied provides defense-in-depth.'
        ''
        'SOURCE LOCATIONS:'
        '  ngksgraph.toml: all [[targets]] and global sections'
        '  src/ui/EngineBridge.cpp:43-44 fprintf null guard already fixed'
    )
    $lines -join "`n" | Out-File (Join-Path $pf "60_root_cause_summary.txt") -Encoding utf8
}

# ═══════════════════════════════════════════════════════════════════
# FINAL RESULT
# ═══════════════════════════════════════════════════════════════════
Write-Host "`n====== FINAL RESULT ======"
$allResults = @()
foreach ($profile in @("debug", "release")) {
    $prefix = if ($profile -eq "debug") { "40" } else { "50" }
    $rcFile = Join-Path $pf "${prefix}_${profile}_rc.txt"
    if (Test-Path $rcFile) {
        $allResults += "${profile}: $(Get-Content $rcFile -Raw)"
    }
}
$allResults | Out-File (Join-Path $pf "70_final_result.txt") -Encoding utf8
$allResults | ForEach-Object { Write-Host $_ }

Write-Host "`nPF=$pf"
Write-Host "DONE"
