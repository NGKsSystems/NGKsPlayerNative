Set-Location "build_graph\release\bin"
$p = Start-Process -FilePath ".\NGKsPlayerNative.exe" -PassThru
Start-Sleep 6
if ($p.HasExited) { "FAIL exit=$($p.ExitCode)" } else { "PASS PID=$($p.Id)"; Stop-Process -Id $p.Id -Force }
