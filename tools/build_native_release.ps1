param(
  [switch]$Launch
)

$ErrorActionPreference = "Stop"

$repo = "C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative"
Set-Location $repo
if ((Get-Location).Path -ne $repo) {
  Write-Output "hey stupid Fucker, wrong window again"
  exit 1
}

$graphPython = Join-Path $repo ".venv\Scripts\python.exe"
$buildCorePython = "C:\Users\suppo\Desktop\NGKsSystems\NGKsDevFabEco\.venv\Scripts\python.exe"
$planPath = Join-Path $repo "build_graph\release\ngksbuildcore_plan.json"
$nativeExe = Join-Path $repo "build_graph\release\bin\native.exe"
$shipExe = Join-Path $repo "build_graph\release\bin\NGKsPlayerNative.exe"
$releaseBin = Split-Path $nativeExe -Parent
$qtDeploy = "C:\Qt\6.10.2\msvc2022_64\bin\windeployqt.exe"

foreach ($requiredPath in @($graphPython, $buildCorePython, $qtDeploy)) {
  if (!(Test-Path $requiredPath)) {
    throw "Missing required tool: $requiredPath"
  }
}

$buildGraphCmd = "call tools\msvc_x64.cmd && `"$graphPython`" -u -m ngksgraph build --project ngksgraph.toml --profile release --target native"
$buildCoreCmd = "call tools\msvc_x64.cmd && set NGKS_ALLOW_DIRECT_BUILDCORE=1 && `"$buildCorePython`" -m ngksbuildcore run --plan build_graph\release\ngksbuildcore_plan.json"

cmd /d /c $buildGraphCmd
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

if (!(Test-Path $planPath)) {
  throw "Missing release plan: $planPath"
}

Remove-Item ".ngksbuildcore\state.sqlite" -ErrorAction SilentlyContinue
cmd /d /c $buildCoreCmd
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

if (!(Test-Path $nativeExe)) {
  throw "Missing built native exe: $nativeExe"
}

$debugResidue = @(
  "concrt140d.dll",
  "msvcp140d.dll",
  "msvcp140_1d.dll",
  "msvcp140_2d.dll",
  "Qt6Cored.dll",
  "Qt6Guid.dll",
  "Qt6Networkd.dll",
  "Qt6Sqld.dll",
  "Qt6Svgd.dll",
  "Qt6Widgetsd.dll",
  "vccorlib140d.dll",
  "vcruntime140d.dll",
  "vcruntime140_1d.dll",
  "vcruntime140_threadsd.dll"
)
foreach ($name in $debugResidue) {
  $path = Join-Path $releaseBin $name
  if (Test-Path $path) {
    Remove-Item $path -Force
  }
}

& $qtDeploy --release $nativeExe
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Copy-Item $nativeExe $shipExe -Force
if (!(Test-Path $shipExe)) {
  throw "Failed to materialize ship exe: $shipExe"
}

if ($Launch) {
  Push-Location (Split-Path $shipExe -Parent)
  try {
    $proc = Start-Process -FilePath $shipExe -PassThru
    Start-Sleep -Seconds 6
    if ($proc.HasExited) {
      Write-Output "FAIL exit=$($proc.ExitCode)"
      exit 2
    }
    Write-Output "PASS PID=$($proc.Id)"
    $proc.Kill()
    $proc.WaitForExit()
  }
  finally {
    Pop-Location
  }
}

Get-Item $shipExe | Select-Object FullName, LastWriteTime, Length