Set-Location "C:\Users\suppo\Desktop\NGKsSystems\NGKsPlayerNative\build_graph\release\bin"
$p = Start-Process -FilePath ".\NGKsPlayerNative.exe" -PassThru
Start-Sleep 6
if ($p.HasExited) { Write-Host "FAIL exit=$($p.ExitCode)" } else { Write-Host "PASS PID=$($p.Id)"; Stop-Process -Id $p.Id }
