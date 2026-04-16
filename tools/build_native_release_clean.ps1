[CmdletBinding()]
param(
    [switch]$Launch
)

$scriptPath = Join-Path $PSScriptRoot 'build_native_release.ps1'
$invokeArgs = @{
    FilePath = 'powershell.exe'
    Wait = $true
    NoNewWindow = $true
    PassThru = $true
    ArgumentList = @(
        '-NoProfile',
        '-ExecutionPolicy', 'Bypass',
        '-File', $scriptPath,
        '-Clean',
        '-RegeneratePlan'
    )
}

if ($Launch) {
    $invokeArgs.ArgumentList += '-Launch'
}

$process = Start-Process @invokeArgs
exit $process.ExitCode