# NGKsSystems
# NGKsPlayerNative
# Module: tools/Run-AndLog_Core.ps1
# Purpose: Shared utilities for Run-AndLog wrappers (logging, path checks, marker checklists).

Set-StrictMode -Version Latest

function Test-RequiredPath([string]$Path) {
  if (-not (Test-Path $Path)) {
    throw "Missing required path: $Path"
  }
}

function New-RunLogContext([string]$ProofDir, [string]$Prefix) {
  Test-RequiredPath '.git'
  New-Item -ItemType Directory -Force $ProofDir | Out-Null

  $ts = Get-Date -Format 'yyyyMMdd_HHmmss'
  $runLog = Join-Path $ProofDir ("{0}_{1}.txt" -f $Prefix, $ts)

  "RUN_TS=$ts" | Out-File $runLog -Encoding ascii
  ("PWD={0}" -f (Get-Location)) | Add-Content $runLog
  ("GIT_TOP={0}" -f (git rev-parse --show-toplevel)) | Add-Content $runLog
  ("GIT_BRANCH={0}" -f (git rev-parse --abbrev-ref HEAD)) | Add-Content $runLog
  '' | Add-Content $runLog

  return [pscustomobject]@{
    TimeStamp = $ts
    RunLog = $runLog
  }
}

function Write-MarkerChecklist(
  [string]$ChecklistPath,
  [string]$Header,
  [string]$TargetLog,
  [string[]]$Markers,
  [int]$TailLines = 0
) {
  $raw = Get-Content $TargetLog -Raw
  $allPass = $true

  $Header | Out-File $ChecklistPath -Encoding ascii
  "TARGET_LOG=$TargetLog" | Add-Content $ChecklistPath
  '' | Add-Content $ChecklistPath

  foreach ($m in $Markers) {
    $ok = $raw.Contains($m)
    if (-not $ok) {
      $allPass = $false
    }
    ("{0} -> {1}" -f $m, $(if ($ok) { 'PASS' } else { 'FAIL' })) | Add-Content $ChecklistPath
  }

  if ($TailLines -gt 0) {
    '' | Add-Content $ChecklistPath
    ("## Tail({0})" -f $TailLines) | Add-Content $ChecklistPath
    Get-Content $TargetLog -Tail $TailLines | Add-Content $ChecklistPath
  }

  return $allPass
}
