# NGKsSystems
# NGKsPlayerNative
# Module: tools/Run-AndLog_AE.ps1
# Purpose: Single-command runner for AE soak certification with auditable proof logging.
# Option 4: visible terminal only, all output to _proof/.

param(
  [switch]$BuildFirst,
  [int]$Seconds = 600,
  [int]$PollMs = 250,
  [int]$MaxXruns = 0,
  [UInt64]$MaxJitterNs = 20000000,
  [switch]$RequireNoRestarts,
  [int]$ToneHz = 440,
  [int]$ToneDb = -12
)

$ErrorActionPreference = 'Stop'

function Require-Path([string]$p){
  if(-not (Test-Path $p)){ throw "Missing required path: $p" }
}

Require-Path '.git'

$proofDir = '_proof\milestone_AE'
New-Item -ItemType Directory -Force $proofDir | Out-Null

$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
$runLog = Join-Path $proofDir ("12_run_and_log_AE_{0}.txt" -f $ts)
$headLog = Join-Path $proofDir '05_run_headless_AE.txt'
$checkLog = Join-Path $proofDir '10_runtime_marker_checklist_AE.txt'

"RUN_TS=$ts" | Out-File $runLog -Encoding ascii
("PWD={0}" -f (Get-Location)) | Add-Content $runLog
("GIT_TOP={0}" -f (git rev-parse --show-toplevel)) | Add-Content $runLog
("GIT_BRANCH={0}" -f (git rev-parse --abbrev-ref HEAD)) | Add-Content $runLog
'' | Add-Content $runLog

if($BuildFirst){
  '## BUILD_FIRST=TRUE' | Add-Content $runLog

  $vcvars = 'C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat'
  Require-Path $vcvars

  $cfgCmd = Join-Path $proofDir 'run_configure_AE.cmd'
  $bldCmd = Join-Path $proofDir 'run_build_AE.cmd'

@"
@echo off
call "$vcvars"
cmake -S . -B build -G Ninja
"@ | Set-Content -Path $cfgCmd -Encoding ascii

@"
@echo off
call "$vcvars"
cmake --build build --config RelWithDebInfo --target NGKsPlayerHeadless NGKsPlayerNative --parallel
"@ | Set-Content -Path $bldCmd -Encoding ascii

  '### CONFIGURE' | Add-Content $runLog
  cmd /c $cfgCmd 2>&1 | Tee-Object -FilePath (Join-Path $proofDir '03_configure_AE.txt') | Add-Content $runLog

  '### BUILD' | Add-Content $runLog
  cmd /c $bldCmd 2>&1 | Tee-Object -FilePath (Join-Path $proofDir '04_build_AE.txt') | Add-Content $runLog

  '' | Add-Content $runLog
}else{
  '## BUILD_FIRST=FALSE' | Add-Content $runLog
  '' | Add-Content $runLog
}

Require-Path '.\build\NGKsPlayerHeadless.exe'

'## AE SOAK' | Add-Content $runLog
$exe = '.\\build\\NGKsPlayerHeadless.exe'
$args = @(
  '--ae_soak',
  '--seconds', $Seconds,
  '--poll_ms', $PollMs,
  '--max_xruns', $MaxXruns,
  '--max_jitter_ns', $MaxJitterNs,
  '--tone_hz', $ToneHz,
  '--tone_db', $ToneDb
)
if($RequireNoRestarts){
  $args += '--require_no_restarts'
}

& $exe @args 2>&1 | Tee-Object -FilePath $headLog | Add-Content $runLog

'' | Add-Content $runLog
'## AE MARKER CHECKLIST' | Add-Content $runLog

$need = @(
  'RTAudioAE=BEGIN',
  'RTAudioAESeconds=',
  'RTAudioAECallbackProgress=',
  'RTAudioAEXRunsTotal=',
  'RTAudioAEXRunsCheck=',
  'RTAudioAEJitterMaxNs=',
  'RTAudioAEJitterCheck=',
  'RTAudioAERestarts=',
  'RTAudioAEWatchdogFinal=',
  'RTAudioAEWatchdogCheck=',
  'RTAudioAE='
)

"# AE runtime marker checklist" | Out-File $checkLog -Encoding ascii
"HEADLESS_LOG=$headLog" | Add-Content $checkLog
'' | Add-Content $checkLog
$raw = Get-Content $headLog -Raw
foreach($m in $need){
  $ok = $raw.Contains($m)
  ("{0} -> {1}" -f $m, $(if($ok){'PASS'} else {'FAIL'})) | Add-Content $checkLog
}
'' | Add-Content $checkLog
'## Tail(80)' | Add-Content $checkLog
Get-Content $headLog -Tail 80 | Add-Content $checkLog

Get-Content $checkLog | Add-Content $runLog

if((Get-Content $checkLog | Select-String -SimpleMatch '-> FAIL' -Quiet) -or (Get-Content $headLog | Select-String -SimpleMatch 'RTAudioAE=FAIL' -Quiet)){
  'AE_CERT=FAIL' | Add-Content $runLog
  throw "AE certification FAILED. See $headLog and $checkLog"
}

'AE_CERT=PASS' | Add-Content $runLog
"PROOF_RUNLOG=$runLog" | Add-Content $runLog
Write-Host "PASS: AE certification. Proof: $runLog"
