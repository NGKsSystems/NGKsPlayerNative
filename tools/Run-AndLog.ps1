param(
  [Parameter(Mandatory=$true)][string]$Title,
  [Parameter(Mandatory=$true)][string]$Command,
  [string]$LogPath = ".\_proof\00_env.txt"
)

"=== $Title ===" | Tee-Object -FilePath $LogPath -Append
"CMD: $Command"  | Tee-Object -FilePath $LogPath -Append

# Run via cmd.exe so it works for both .bat and exe commands
cmd.exe /c $Command 2>&1 | Tee-Object -FilePath $LogPath -Append

"" | Tee-Object -FilePath $LogPath -Append
