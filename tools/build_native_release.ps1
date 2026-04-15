[CmdletBinding()]
param(
    [switch]$Launch
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$releaseDir = Join-Path $repoRoot 'build_graph\release\bin'
$currentExe = Join-Path $releaseDir 'native.exe'
$archiveDir = Join-Path $releaseDir '_archived_app_binaries'
$legacyNames = @(
    'NGKsPlayerNative.exe'
)

if (-not (Test-Path $currentExe)) {
    throw "Current release executable not found: $currentExe"
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