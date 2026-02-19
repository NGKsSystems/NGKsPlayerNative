# NGKsSystems
# NGKsPlayerNative
# Module: tools/Run-AndLog_AC.ps1
# Purpose: Single-command runner for AC marker gate with auditable proof logging.
# Option 4: visible terminal only, all output to _proof/.

param(
  [switch]$BuildFirst,
  [int]$HeadlessSeconds = 5,
  [int]$QtSeconds = 2,
  [int]$ToneHz = 440,
  [int]$ToneDb = -12
)

$ErrorActionPreference = 'Stop'

function Require-Path([string]$p){
  if(-not (Test-Path $p)){ throw "Missing required path: $p" }
}

# Repo root check
Require-Path ".git"

$proofDir = "_proof\milestone_AC"
New-Item -ItemType Directory -Force $proofDir | Out-Null

$ts = Get-Date -Format "yyyyMMdd_HHmmss"
$runLog = Join-Path $proofDir ("12_run_and_log_AC_{0}.txt" -f $ts)

"RUN_TS=$ts" | Out-File $runLog -Encoding ascii
("PWD={0}" -f (Get-Location)) | Add-Content $runLog
("GIT_TOP={0}" -f (git rev-parse --show-toplevel)) | Add-Content $runLog
("GIT_BRANCH={0}" -f (git rev-parse --abbrev-ref HEAD)) | Add-Content $runLog
"" | Add-Content $runLog

if($BuildFirst){
  "## BUILD_FIRST=TRUE" | Add-Content $runLog

  $vcvars = "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat"
  if(-not (Test-Path $vcvars)){ throw "Missing vcvars64.bat at: $vcvars" }

  $cfgCmd = Join-Path $proofDir "run_configure_AC.cmd"
  $bldCmd = Join-Path $proofDir "run_build_AC.cmd"

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

  "### CONFIGURE" | Add-Content $runLog
  cmd /c $cfgCmd 2>&1 | Tee-Object -FilePath (Join-Path $proofDir "13_configure_AC_$ts.txt") | Add-Content $runLog

  "### BUILD" | Add-Content $runLog
  cmd /c $bldCmd 2>&1 | Tee-Object -FilePath (Join-Path $proofDir "14_build_AC_$ts.txt") | Add-Content $runLog

  "" | Add-Content $runLog
}else{
  "## BUILD_FIRST=FALSE" | Add-Content $runLog
  "" | Add-Content $runLog
}

"## AC MARKER GATE" | Add-Content $runLog
$gateArgs = @(
  "-NoProfile",
  "-ExecutionPolicy","Bypass",
  "-File","tools\ac_marker_gate.ps1",
  "-HeadlessSeconds",$HeadlessSeconds,
  "-QtSeconds",$QtSeconds,
  "-ToneHz",$ToneHz,
  "-ToneDb",$ToneDb
)

# Run gate and capture into both transcript + a dedicated proof file
$gateRunProof = Join-Path $proofDir ("11_ac_marker_gate_run_{0}.txt" -f $ts)
powershell @gateArgs 2>&1 | Tee-Object -FilePath $gateRunProof
Get-Content $gateRunProof | Add-Content $runLog

"" | Add-Content $runLog
"## RESULT" | Add-Content $runLog

# Gate writes 10_runtime_marker_checklist_AC.txt; fail if it contains FAIL
$checklist = Join-Path $proofDir "10_runtime_marker_checklist_AC.txt"
Require-Path $checklist

if(Select-String -Path $checklist -SimpleMatch "FAIL" -Quiet){
  "AC_MARKER_GATE=FAIL" | Add-Content $runLog
  throw "AC marker gate FAILED. See $checklist and $gateRunProof"
}else{
  "AC_MARKER_GATE=PASS" | Add-Content $runLog
}

"PROOF_RUNLOG=$runLog" | Add-Content $runLog
Write-Host "PASS: AC marker gate. Proof: $runLog"
