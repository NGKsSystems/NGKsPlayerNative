[CmdletBinding()]
param(
    [switch]$Launch,
    [switch]$Clean,
    [switch]$RegeneratePlan
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = 'C:\Users\suppo\Desktop\NGKsSystems\NGKsDevFabEco\.venv\Scripts\python.exe'
$vcvarsPath = 'C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat'
$stateDb = Join-Path $repoRoot '.ngksbuildcore\state.sqlite'
$buildDriver = Join-Path $repoRoot '.ngksbuildcore\build_native_release.cmd'
$buildPlan = Join-Path $repoRoot 'build_graph\release\ngksbuildcore_plan.json'
$releaseDir = Join-Path $repoRoot 'build_graph\release\bin'
$currentExe = Join-Path $releaseDir 'native.exe'
$archiveDir = Join-Path $releaseDir '_archived_app_binaries'
$legacyNames = @(
    'NGKsPlayerNative.exe'
)

$blockingProcesses = Get-CimInstance Win32_Process | Where-Object {
    $_.ExecutablePath -and
    ([System.StringComparer]::OrdinalIgnoreCase.Equals($_.ExecutablePath, $currentExe))
}

if ($blockingProcesses) {
    $pidList = ($blockingProcesses | ForEach-Object { $_.ProcessId }) -join ', '
    throw "Release build blocked because native.exe is still running from $currentExe (PID: $pidList). Close the app and rerun the build."
}

if (-not (Test-Path $pythonExe)) {
    throw "Python executable not found: $pythonExe"
}

if (-not (Test-Path $vcvarsPath)) {
    throw "MSVC environment script not found: $vcvarsPath"
}

New-Item -ItemType Directory -Path (Split-Path -Parent $buildDriver) -Force | Out-Null

if ($Clean -and (Test-Path $stateDb)) {
    Remove-Item $stateDb -Force
}

$shouldGeneratePlan = $Clean -or $RegeneratePlan -or -not (Test-Path $buildPlan)

$buildScriptLines = [System.Collections.Generic.List[string]]::new()
$buildScriptLines.Add('@echo off')
$buildScriptLines.Add(('cd /d "{0}"' -f $repoRoot))
$buildScriptLines.Add(('call "{0}"' -f $vcvarsPath))
$buildScriptLines.Add('if errorlevel 1 exit /b %errorlevel%')
$buildScriptLines.Add('set NGKS_ALLOW_DIRECT_BUILDCORE=1')

if ($shouldGeneratePlan) {
    $buildScriptLines.Add(('"{0}" -m ngksgraph build --project ngksgraph.toml --profile release --target native' -f $pythonExe))
    $buildScriptLines.Add('if errorlevel 1 exit /b %errorlevel%')
}

$buildScriptLines.Add(('"{0}" -m ngksbuildcore run --plan build_graph\release\ngksbuildcore_plan.json' -f $pythonExe))
$buildScriptLines.Add('exit /b %errorlevel%')
Set-Content -Path $buildDriver -Value $buildScriptLines -Encoding ASCII

try {
    $buildProcess = Start-Process -FilePath 'cmd.exe' -ArgumentList '/d', '/c', ('"' + $buildDriver + '"') -Wait -NoNewWindow -PassThru
    if ($buildProcess.ExitCode -ne 0) {
        throw "Release build failed with exit code $($buildProcess.ExitCode)"
    }
}
finally {
    Remove-Item $buildDriver -ErrorAction SilentlyContinue
}

if (-not (Test-Path $currentExe)) {
    throw "Current release executable not found after build: $currentExe"
}

$currentItem = Get-Item $currentExe
New-Item -ItemType Directory -Path $archiveDir -Force | Out-Null

foreach ($legacyName in $legacyNames) {
    $legacyPath = Join-Path $releaseDir $legacyName
    if (-not (Test-Path $legacyPath)) {
        continue
    }

    $legacyItem = Get-Item $legacyPath
    if ($legacyItem.FullName -eq $currentItem.FullName) {
        continue
    }

    $stamp = $legacyItem.LastWriteTime.ToString('yyyyMMdd-HHmmss')
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($legacyItem.Name)
    $extension = [System.IO.Path]::GetExtension($legacyItem.Name)
    $destination = Join-Path $archiveDir ("{0}-stale-{1}{2}" -f $baseName, $stamp, $extension)
    $counter = 1

    while (Test-Path $destination) {
        $destination = Join-Path $archiveDir ("{0}-stale-{1}-{2}{3}" -f $baseName, $stamp, $counter, $extension)
        $counter += 1
    }

    Move-Item -Path $legacyItem.FullName -Destination $destination -Force
    Write-Host "Archived stale binary: $($legacyItem.Name) -> $destination"
}

if ($Launch) {
    $process = Start-Process -FilePath $currentExe -WorkingDirectory $releaseDir -PassThru
    Write-Host "Launched PID=$($process.Id) Path=$currentExe"
} else {
    Write-Host "Current release executable: $currentExe"
}