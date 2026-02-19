# NGKsSystems
# NGKsPlayerNative
# Module: tools/Run-AndLog_AF.ps1
# Purpose: Single-command runner for AF device matrix + persistence + AE verification.

param(
  [switch]$BuildFirst,
  [switch]$ListDevices,
  [string]$SetPreferredDeviceId,
  [string]$DeviceId,
  [string]$DeviceName,
  [int]$SampleRate = 0,
  [int]$BufferFrames = 0,
  [int]$ChOut = 0,
  [int]$Seconds = 30
)

$ErrorActionPreference = 'Stop'

function Require-Path([string]$p){
  if(-not (Test-Path $p)){ throw "Missing required path: $p" }
}

Require-Path '.git'

$proofDir = '_proof\milestone_AF'
New-Item -ItemType Directory -Force $proofDir | Out-Null
$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
$runLog = Join-Path $proofDir ("12_run_and_log_AF_{0}.txt" -f $ts)

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
"@ | Set-Content -Encoding ascii _proof\milestone_AF\run_configure_AF.cmd
  @"
@echo off
call "$vcvars"
cmake --build build --config RelWithDebInfo --target NGKsPlayerHeadless NGKsPlayerNative --parallel
"@ | Set-Content -Encoding ascii _proof\milestone_AF\run_build_AF.cmd

  cmd /c _proof\milestone_AF\run_configure_AF.cmd 2>&1 | Tee-Object -FilePath _proof\milestone_AF\03_configure_AF.txt | Add-Content $runLog
  cmd /c _proof\milestone_AF\run_build_AF.cmd 2>&1 | Tee-Object -FilePath _proof\milestone_AF\04_build_AF.txt | Add-Content $runLog
}

Require-Path '.\build\NGKsPlayerHeadless.exe'

if($ListDevices){
  .\build\NGKsPlayerHeadless.exe --list_devices 2>&1 | Tee-Object -FilePath _proof\milestone_AF\05_list_devices_AF.txt | Add-Content $runLog
}

if(-not [string]::IsNullOrWhiteSpace($SetPreferredDeviceId)){
  $setArgs = @('--set_preferred_device_id', $SetPreferredDeviceId)
  if($SampleRate -gt 0){
    $setArgs += @('--sr', $SampleRate)
  }
  if($BufferFrames -gt 0){
    $setArgs += @('--buffer_frames', $BufferFrames)
  }
  if($ChOut -gt 0){
    $setArgs += @('--ch_out', $ChOut)
  }
  .\build\NGKsPlayerHeadless.exe @setArgs 2>&1 | Tee-Object -FilePath _proof\milestone_AF\07_set_preferred_device_AF.txt | Add-Content $runLog
}

$aeArgs = @('--ae_soak','--seconds',$Seconds)
if(-not [string]::IsNullOrWhiteSpace($DeviceId)){
  $aeArgs += @('--device_id',$DeviceId)
}
if(-not [string]::IsNullOrWhiteSpace($DeviceName)){
  $aeArgs += @('--device_name',$DeviceName)
}
if($SampleRate -gt 0){
  $aeArgs += @('--sr', $SampleRate)
}
if($BufferFrames -gt 0){
  $aeArgs += @('--buffer_frames', $BufferFrames)
}
if($ChOut -gt 0){
  $aeArgs += @('--ch_out', $ChOut)
}

$targetLog = '_proof\milestone_AF\08_ae_30s_preferred_AF.txt'
if(-not [string]::IsNullOrWhiteSpace($DeviceId) -or -not [string]::IsNullOrWhiteSpace($DeviceName)){
  $targetLog = '_proof\milestone_AF\06_ae_30s_deviceid_AF.txt'
}

.\build\NGKsPlayerHeadless.exe @aeArgs 2>&1 | Tee-Object -FilePath $targetLog | Add-Content $runLog

$check = '_proof\milestone_AF\10_runtime_marker_checklist_AF.txt'
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
  'RTAudioDeviceSelect=PASS',
  'RTAudioDeviceId=',
  'RTAudioDeviceName=',
  'RTAudioSampleRate=',
  'RTAudioBufferFrames=',
  'RTAudioAE=PASS'
)

"# AF runtime marker checklist" | Out-File $check -Encoding ascii
"TARGET_LOG=$targetLog" | Add-Content $check
$raw = Get-Content $targetLog -Raw
foreach($m in $need){
  $ok = $raw.Contains($m)
  ("{0} -> {1}" -f $m, $(if($ok){'PASS'}else{'FAIL'})) | Add-Content $check
}

if((Get-Content $check | Select-String -SimpleMatch '-> FAIL' -Quiet) -or ($raw -notmatch 'RTAudioAE=PASS')){
  throw "AF checklist failed: $check"
}

Write-Host "PASS: AF runner. Proof: $runLog"
