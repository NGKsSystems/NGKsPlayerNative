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
. "$PSScriptRoot\Run-AndLog_Core.ps1"

$proofDir = '_proof\milestone_AF'
$ctx = New-RunLogContext -ProofDir $proofDir -Prefix '12_run_and_log_AF'
$runLog = $ctx.RunLog

"RUN_TS=$ts" | Out-File $runLog -Encoding ascii
("PWD={0}" -f (Get-Location)) | Add-Content $runLog
("GIT_TOP={0}" -f (git rev-parse --show-toplevel)) | Add-Content $runLog
("GIT_BRANCH={0}" -f (git rev-parse --abbrev-ref HEAD)) | Add-Content $runLog
'' | Add-Content $runLog

if($BuildFirst){
  $vcvars = 'C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat'
  Test-RequiredPath $vcvars
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

Test-RequiredPath '.\build\NGKsPlayerHeadless.exe'

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

$allMarkersPass = Write-MarkerChecklist -ChecklistPath $check -Header '# AF runtime marker checklist' -TargetLog $targetLog -Markers $need
$raw = Get-Content $targetLog -Raw

if((-not $allMarkersPass) -or ($raw -notmatch 'RTAudioAE=PASS')){
  throw "AF checklist failed: $check"
}

Write-Host "PASS: AF runner. Proof: $runLog"
