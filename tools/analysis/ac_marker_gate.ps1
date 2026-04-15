# NGKsSystems
# NGKsPlayerNative
# Module: tools/ac_marker_gate.ps1
# Purpose: AC runtime marker gate (headless + Qt) with deterministic proof output.
# Rule: visible terminal, logs in _proof/, hard-fail on missing markers.

param(
  [int]$HeadlessSeconds = 5,
  [int]$QtSeconds = 2,
  [int]$ToneHz = 440,
  [int]$ToneDb = -12
)

$ErrorActionPreference = 'Stop'

function Require-Path([string]$p){
  if(-not (Test-Path $p)){ throw "Missing required path: $p" }
}

# ---- Paths ----
$proofDir = Join-Path $PSScriptRoot "..\_proof\milestone_AC"
New-Item -ItemType Directory -Force $proofDir | Out-Null
$proofDir = (Resolve-Path $proofDir).Path

$headLog = Join-Path $proofDir "05_run_headless_AC.txt"
$qtLog   = Join-Path $proofDir "06_run_qt_AC.txt"
$outLog  = Join-Path $proofDir "10_runtime_marker_checklist_AC.txt"

$exeHead = Join-Path $PSScriptRoot "..\build\NGKsPlayerHeadless.exe"
$exeQt   = Join-Path $PSScriptRoot "..\build\NGKsPlayerNative.exe"

Require-Path $exeHead
Require-Path $exeQt

# ---- Run Headless (capture stdout+stderr) ----
& $exeHead --rt_audio_probe --seconds $HeadlessSeconds --tone_hz $ToneHz --tone_db $ToneDb 2>&1 | Tee-Object -FilePath $headLog | Out-Null
Require-Path $headLog

# ---- Run Qt (proven env-driven smoke for RTAudio* lines) ----
$qtBin = Join-Path $PSScriptRoot "..\_deps\qt6\bin"
if(Test-Path $qtBin){
  $env:PATH = "$qtBin;$env:PATH"
}

$env:NGKS_DIAG_AUTOSHOW = '1'
$env:NGKS_SELFTEST_AUTORUN = '1'
$env:NGKS_RT_AUDIO_AUTORUN = '1'
$env:NGKS_UI_SMOKE = '1'
$env:NGKS_UI_SMOKE_SECONDS = [string]$QtSeconds

$uiRuntimeLog = Join-Path $PSScriptRoot "..\data\runtime\ui_qt.log"
if(Test-Path $uiRuntimeLog){ Remove-Item $uiRuntimeLog -Force }
if(Test-Path $qtLog){ Remove-Item $qtLog -Force }

$proc = Start-Process -FilePath $exeQt -PassThru
try {
  Wait-Process -Id $proc.Id -Timeout ($QtSeconds + 8)
} catch {
  Stop-Process -Id $proc.Id -Force
}

Require-Path $uiRuntimeLog
Get-Content $uiRuntimeLog | Tee-Object -FilePath $qtLog | Out-Null

Require-Path $qtLog

# ---- Marker check ----
$needHead = @(
  'RTAudioProbe=BEGIN',
  'RTAudioDeviceOpen=PASS',
  'RTAudioCallbackTicks>=10=PASS',
  'RTAudioXRuns=0',
  'RTAudioWatchdog=PASS',
  'RTAudioProbe=PASS'
)

$needQt = @(
  'RTAudioPollTick=PASS',
  'RTAudioDeviceOpen=TRUE',
  'RTAudioCallbackCount',
  'RTAudioXRuns=0',
  'RTAudioPeakDb',
  'RTAudioWatchdog=TRUE'
)

function Has-Needle([string]$path, [string]$needle){
  $s = Get-Content $path -Raw
  return $s.Contains($needle)
}

"# AC runtime marker checklist" | Out-File $outLog -Encoding ascii
"HEADLESS_LOG=$headLog" | Add-Content $outLog
"QT_LOG=$qtLog" | Add-Content $outLog
"" | Add-Content $outLog

"## Headless markers" | Add-Content $outLog
foreach($n in $needHead){
  $ok = Has-Needle $headLog $n
  ("{0} -> {1}" -f $n, $(if($ok){'PASS'}else{'FAIL'})) | Add-Content $outLog
}

"" | Add-Content $outLog
"## Qt markers" | Add-Content $outLog
foreach($n in $needQt){
  $ok = Has-Needle $qtLog $n
  ("{0} -> {1}" -f $n, $(if($ok){'PASS'}else{'FAIL'})) | Add-Content $outLog
}

"" | Add-Content $outLog
"## Sample Qt marker lines (first 30)" | Add-Content $outLog
(Get-Content $qtLog | Select-String -Pattern 'RTAudio' | Select-Object -First 30 | ForEach-Object { $_.Line }) | Add-Content $outLog

# ---- Hard fail if any FAIL ----
if( (Get-Content $outLog | Select-String -SimpleMatch '-> FAIL') ){
  throw "AC runtime marker gate FAILED. See: $outLog"
}

Write-Host "AC runtime marker gate PASS. Proof: $outLog"