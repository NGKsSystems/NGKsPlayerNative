# NGKsSystems
# NGKsPlayerNative
# Module: tools/Run-AndLog_AJ.ps1
# Purpose: Automated matrix smoke runner across profiles and common format requests.

param(
  [string]$Profiles = '',
  [switch]$UseAllProfiles,
  [int]$Seconds = 10
)

$ErrorActionPreference = 'Stop'

function Require-Path([string]$p){
  if(-not (Test-Path $p)){ throw "Missing required path: $p" }
}

function Parse-ProfileRows([string]$text){
  $rows = @{}
  $active = ''
  foreach($line in ($text -split "`r?`n")){
    if($line -match '^RTAudioProfileActive=(.+)$'){
      $active = $Matches[1].Trim()
      continue
    }
    if($line -match '^RTAudioProfile name=(.+?)\s+device_id=(.*?)\s+device_name=(.*?)\s+sr=(\-?\d+)\s+buffer=(\-?\d+)\s+ch_out=(\-?\d+)\s*$'){
      $rows[$Matches[1]] = [pscustomobject]@{
        name      = $Matches[1]
        device_id = $Matches[2]
        device    = $Matches[3]
        sr        = [int]$Matches[4]
        buffer    = [int]$Matches[5]
        ch_out    = [int]$Matches[6]
      }
    }
  }
  return [pscustomobject]@{ rows=$rows; active=$active }
}

function Extract-Value([string]$text,[string]$prefix){
  $m = [regex]::Match($text, [regex]::Escape($prefix) + '([^\r\n]+)')
  if($m.Success){ return $m.Groups[1].Value.Trim() }
  return ''
}

Require-Path '.git'
Require-Path '.\build\NGKsPlayerHeadless.exe'

$proofDir = '_proof\milestone_AJ'
$matrixDir = Join-Path $proofDir '05_matrix_runs'
New-Item -ItemType Directory -Force $proofDir | Out-Null
New-Item -ItemType Directory -Force $matrixDir | Out-Null

$listLog = Join-Path $proofDir '04_profile_list_and_devices_AJ.txt'
$summary = Join-Path $proofDir '10_matrix_summary_AJ.txt'

$exe = '.\build\NGKsPlayerHeadless.exe'

"## list_devices" | Out-File $listLog -Encoding ascii
& $exe --list_devices 2>&1 | Add-Content $listLog
"" | Add-Content $listLog
"## profile_list" | Add-Content $listLog
$profileListText = (& $exe --profile_list 2>&1 | Out-String)
$profileListText | Add-Content $listLog

$parsed = Parse-ProfileRows $profileListText
$profileRows = $parsed.rows

$selectedProfiles = @()
if($UseAllProfiles){
  $selectedProfiles = @($profileRows.Keys | Sort-Object)
}else{
  if([string]::IsNullOrWhiteSpace($Profiles)){
    throw 'Provide -Profiles <comma list> or -UseAllProfiles'
  }
  $selectedProfiles = @($Profiles.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

if($selectedProfiles.Count -eq 0){
  throw 'No profiles selected for matrix run'
}

"profile | device_id | req sr/buf | applied sr/buf | fallback | AE pass/fail | notes" | Out-File $summary -Encoding ascii
"------- | --------- | ---------- | -------------- | -------- | ------------- | -----" | Add-Content $summary

$overallFail = $false

foreach($profile in $selectedProfiles){
  if(-not $profileRows.ContainsKey($profile)){
    "${profile} | <missing> | - | - | - | FAIL | profile_not_found" | Add-Content $summary
    $overallFail = $true
    continue
  }

  $p = $profileRows[$profile]
  $requestedSet = @(
    @{ sr=44100; buffer=128; ch_out=2; note='fixed_44100_128_2' },
    @{ sr=48000; buffer=256; ch_out=2; note='fixed_48000_256_2' },
    @{ sr=([int]$p.sr); buffer=([int]$p.buffer); ch_out=([int]$p.ch_out); note='profile_requested' }
  )

  $uniq = @{}
  $runSet = @()
  foreach($f in $requestedSet){
    $key = "{0}|{1}|{2}" -f $f.sr,$f.buffer,$f.ch_out
    if(-not $uniq.ContainsKey($key)){
      $uniq[$key] = $true
      $runSet += $f
    }
  }

  $i = 0
  foreach($fmt in $runSet){
    $i++
    $safeProfile = ($profile -replace '[^a-zA-Z0-9_\-]','_')
    $runLog = Join-Path $matrixDir ("{0}_{1:D2}.txt" -f $safeProfile,$i)

    $args = @(
      '--ae_soak',
      '--seconds', $Seconds,
      '--profile_use', $profile,
      '--sr', $fmt.sr,
      '--buffer_frames', $fmt.buffer,
      '--ch_out', $fmt.ch_out
    )

    & $exe @args 2>&1 | Tee-Object -FilePath $runLog | Out-Null

    $raw = Get-Content $runLog -Raw
    $appliedSr = Extract-Value $raw 'RTAudioAGAppliedSR='
    $appliedBuf = Extract-Value $raw 'RTAudioAGAppliedBufferFrames='
    $fallback = Extract-Value $raw 'RTAudioAGFallback='
    $aePass = if($raw.Contains('RTAudioAE=PASS')){'PASS'}else{'FAIL'}

    $notes = $fmt.note
    if($aePass -ne 'PASS'){
      $overallFail = $true
    }

    $row = "{0} | {1} | {2}/{3} | {4}/{5} | {6} | {7} | {8}" -f $profile, $p.device_id, $fmt.sr, $fmt.buffer, $(if([string]::IsNullOrWhiteSpace($appliedSr)){'?'}else{$appliedSr}), $(if([string]::IsNullOrWhiteSpace($appliedBuf)){'?'}else{$appliedBuf}), $(if([string]::IsNullOrWhiteSpace($fallback)){'?'}else{$fallback}), $aePass, $notes
    Add-Content -Path $summary -Value $row
  }
}

if($overallFail){
  Write-Host "AJ_MATRIX=FAIL summary=$summary"
  exit 1
}

Write-Host "AJ_MATRIX=PASS summary=$summary"