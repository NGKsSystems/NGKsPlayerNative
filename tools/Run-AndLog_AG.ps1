# NGKsSystems
# NGKsPlayerNative
# Module: tools/Run-AndLog_AG.ps1
# Purpose: Single-command runner for AG capability negotiation + fallback evidence.

param(
  [switch]$BuildFirst,
  [int]$Seconds = 20,
  [int]$Case1SampleRate = 48000,
  [int]$Case1BufferFrames = 128,
  [int]$Case1ChOut = 2,
  [int]$Case2SampleRate = 12345,
  [int]$Case2BufferFrames = 31,
  [int]$Case2ChOut = 9
)

$ErrorActionPreference = 'Stop'

function Require-Path([string]$p){
  if(-not (Test-Path $p)){ throw "Missing required path: $p" }
}

function Require-Markers([string]$LogPath,[string[]]$Markers,[string]$Title){
  $raw = Get-Content $LogPath -Raw
  foreach($m in $Markers){
    if(-not $raw.Contains($m)){
      throw "$Title missing marker: $m ($LogPath)"
    }
  }
}

Require-Path '.git'

$proofDir = '_proof\milestone_AG'
New-Item -ItemType Directory -Force $proofDir | Out-Null

$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
$runLog = Join-Path $proofDir ("12_run_and_log_AG_{0}.txt" -f $ts)
$listLog = Join-Path $proofDir '04_list_devices_AG.txt'
$case1Log = Join-Path $proofDir '05_case1_normal_AG.txt'
$case2Log = Join-Path $proofDir '06_case2_fallback_AG.txt'
$case3Log = Join-Path $proofDir '07_case3_preferred_AG.txt'
$checkLog = Join-Path $proofDir '10_runtime_marker_checklist_AG.txt'

"RUN_TS=$ts" | Out-File $runLog -Encoding ascii
("PWD={0}" -f (Get-Location)) | Add-Content $runLog
("GIT_TOP={0}" -f (git rev-parse --show-toplevel)) | Add-Content $runLog
("GIT_BRANCH={0}" -f (git rev-parse --abbrev-ref HEAD)) | Add-Content $runLog
'' | Add-Content $runLog

if($BuildFirst){
  $vcvars = 'C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat'
  Require-Path $vcvars

  @"
@echo off
call "$vcvars"
cmake -S . -B build -G Ninja
"@ | Set-Content -Encoding ascii _proof\milestone_AG\run_configure_AG.cmd

  @"
@echo off
call "$vcvars"
cmake --build build --config RelWithDebInfo --target NGKsPlayerHeadless NGKsPlayerNative --parallel
"@ | Set-Content -Encoding ascii _proof\milestone_AG\run_build_AG.cmd

  '### CONFIGURE' | Add-Content $runLog
  cmd /c _proof\milestone_AG\run_configure_AG.cmd 2>&1 | Tee-Object -FilePath _proof\milestone_AG\03_configure_AG.txt | Add-Content $runLog
  '### BUILD' | Add-Content $runLog
  cmd /c _proof\milestone_AG\run_build_AG.cmd 2>&1 | Tee-Object -FilePath _proof\milestone_AG\04_build_AG.txt | Add-Content $runLog
}

Require-Path '.\build\NGKsPlayerHeadless.exe'
$exe = '.\build\NGKsPlayerHeadless.exe'

'## LIST DEVICES' | Add-Content $runLog
& $exe --list_devices 2>&1 | Tee-Object -FilePath $listLog | Add-Content $runLog

$firstDeviceLine = Get-Content $listLog | Where-Object { $_ -like 'RTAudioDevice id=*' } | Select-Object -First 1
if([string]::IsNullOrWhiteSpace($firstDeviceLine)){
  throw "No audio device lines found in $listLog"
}
$firstDeviceId = [regex]::Match($firstDeviceLine,'id=([^\s]+)').Groups[1].Value
if([string]::IsNullOrWhiteSpace($firstDeviceId)){
  throw "Unable to parse first device id from: $firstDeviceLine"
}

$commonAe = @('--ae_soak','--seconds',$Seconds)

'## CASE1 NORMAL REQUEST' | Add-Content $runLog
$case1Args = @($commonAe + @('--sr',$Case1SampleRate,'--buffer_frames',$Case1BufferFrames,'--ch_out',$Case1ChOut))
& $exe @case1Args 2>&1 | Tee-Object -FilePath $case1Log | Add-Content $runLog

'## CASE2 FORCED FALLBACK' | Add-Content $runLog
$case2Args = @($commonAe + @('--sr',$Case2SampleRate,'--buffer_frames',$Case2BufferFrames,'--ch_out',$Case2ChOut))
& $exe @case2Args 2>&1 | Tee-Object -FilePath $case2Log | Add-Content $runLog

'## CASE3 PREFERRED DEVICE PATH' | Add-Content $runLog
& $exe --set_preferred_device_id $firstDeviceId --sr $Case1SampleRate --buffer_frames $Case1BufferFrames --ch_out $Case1ChOut 2>&1 | Tee-Object -FilePath (Join-Path $proofDir '06_set_preferred_device_AG.txt') | Add-Content $runLog
& $exe --ae_soak --seconds $Seconds 2>&1 | Tee-Object -FilePath $case3Log | Add-Content $runLog

"# AG runtime marker checklist" | Out-File $checkLog -Encoding ascii
"CASE1_LOG=$case1Log" | Add-Content $checkLog
"CASE2_LOG=$case2Log" | Add-Content $checkLog
"CASE3_LOG=$case3Log" | Add-Content $checkLog
'' | Add-Content $checkLog

$requiredAll = @(
  'RTAudioAG=BEGIN',
  'RTAudioAGRequestedSR=',
  'RTAudioAGRequestedBufferFrames=',
  'RTAudioAGRequestedChOut=',
  'RTAudioAGAppliedSR=',
  'RTAudioAGAppliedBufferFrames=',
  'RTAudioAGAppliedChOut=',
  'RTAudioAGFallback=',
  'RTAudioAG=PASS',
  'RTAudioAE=PASS'
)

foreach($marker in $requiredAll){
  $ok1 = (Get-Content $case1Log -Raw).Contains($marker)
  $ok2 = (Get-Content $case2Log -Raw).Contains($marker)
  $ok3 = (Get-Content $case3Log -Raw).Contains($marker)
  ("{0} -> case1:{1} case2:{2} case3:{3}" -f $marker, $(if($ok1){'PASS'}else{'FAIL'}), $(if($ok2){'PASS'}else{'FAIL'}), $(if($ok3){'PASS'}else{'FAIL'})) | Add-Content $checkLog
}

$case2Fallback = (Get-Content $case2Log -Raw).Contains('RTAudioAGFallback=TRUE')
("RTAudioAGFallback=TRUE (case2) -> {0}" -f $(if($case2Fallback){'PASS'}else{'FAIL'})) | Add-Content $checkLog

$checkRaw = Get-Content $checkLog -Raw
if($checkRaw.Contains('FAIL')){
  throw "AG checklist failed: $checkLog"
}

'AG_CERT=PASS' | Add-Content $runLog
"PROOF_RUNLOG=$runLog" | Add-Content $runLog
Write-Host "PASS: AG certification. Proof: $runLog"
