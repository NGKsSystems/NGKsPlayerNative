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
  [UInt64]$MaxJitterNs = 15000000,
  [switch]$StrictJitter,
  [switch]$RequireNoRestarts,
  [switch]$AllowStallTrips,
  [int]$SampleRate = 0,
  [int]$BufferFrames = 0,
  [int]$ChOut = 0,
  [int]$ToneHz = 440,
  [int]$ToneDb = -12
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\Run-AndLog_Core.ps1"

$proofDir = '_proof\milestone_AE'
$ctx = New-RunLogContext -ProofDir $proofDir -Prefix '12_run_and_log_AE'
$ts = $ctx.TimeStamp
$runLog = $ctx.RunLog
$headLog = Join-Path $proofDir '05_run_headless_AE.txt'
$checkLog = Join-Path $proofDir '10_runtime_marker_checklist_AE.txt'

if($BuildFirst){
  '## BUILD_FIRST=TRUE' | Add-Content $runLog

  $vcvars = 'C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat'
  Test-RequiredPath $vcvars

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

Test-RequiredPath '.\build\NGKsPlayerHeadless.exe'

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
if($SampleRate -gt 0){
  $args += @('--sr', $SampleRate)
}
if($BufferFrames -gt 0){
  $args += @('--buffer_frames', $BufferFrames)
}
if($ChOut -gt 0){
  $args += @('--ch_out', $ChOut)
}
if($RequireNoRestarts){
  $args += '--require_no_restarts'
}
if($StrictJitter){
  $args += '--strict_jitter'
}
if($AllowStallTrips){
  $args += '--allow_stall_trips'
}

& $exe @args 2>&1 | Tee-Object -FilePath $headLog | Add-Content $runLog

'' | Add-Content $runLog
'## AE MARKER CHECKLIST' | Add-Content $runLog

$need = @(
  'RTAudioAG=BEGIN',
  'RTAudioAGRequestedSR=',
  'RTAudioAGRequestedBufferFrames=',
  'RTAudioAGRequestedChOut=',
  'RTAudioAGAppliedSR=',
  'RTAudioAGAppliedBufferFrames=',
  'RTAudioAGAppliedChOut=',
  'RTAudioAGFallback=',
  'RTAudioAG=PASS',
  'RTAudioAE=BEGIN',
  'RTAudioAESeconds=',
  'RTAudioAEJitterLimitNs=',
  'RTAudioAEXRunLimit=',
  'RTAudioAERestartPolicy=',
  'RTAudioAECallbackProgress=PASS',
  'RTAudioAEXRunsTotal=',
  'RTAudioAEXRunsCheck=PASS',
  'RTAudioAEJitterMaxNs=',
  'RTAudioAEJitterCheck=PASS',
  'RTAudioAERestarts=',
  'RTAudioAEStallTrips=',
  'RTAudioAEStallTripCheck=PASS',
  'RTAudioAEWatchdogFinal=',
  'RTAudioAEWatchdogCheck=PASS',
  'RTAudioAE=PASS'
)

$allMarkersPass = Write-MarkerChecklist -ChecklistPath $checkLog -Header '# AE runtime marker checklist' -TargetLog $headLog -Markers $need -TailLines 80

Get-Content $checkLog | Add-Content $runLog

if((-not $allMarkersPass) -or (Get-Content $headLog | Select-String -SimpleMatch 'RTAudioAE=FAIL' -Quiet)){
  'AE_CERT=FAIL' | Add-Content $runLog
  throw "AE certification FAILED. See $headLog and $checkLog"
}

'AE_CERT=PASS' | Add-Content $runLog
"PROOF_RUNLOG=$runLog" | Add-Content $runLog
Write-Host "PASS: AE certification. Proof: $runLog"
