# Stage 1 fills scan - detached launcher
# Runs independently of OpenCode, writes progress to log file

$logFile = "C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\data\stage1_launch.log"
$script = "C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER\2hl_Stage1_Filter.py"

"Launching Stage 1 scan at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File $logFile

# Run via python, redirect stdout+stderr to log
Start-Process -FilePath "python" -ArgumentList $script `
  -WorkingDirectory "C:\Users\wigmore\trading_stack\Hyperliquid scanner\WALLET FINDER" `
  -RedirectStandardOutput $logFile -RedirectStandardError "$logFile.err" `
  -NoNewWindow -PassThru | Select-Object Id, ProcessName, StartTime

Write-Output "Stage 1 launched. Monitor with: Get-Content '$logFile' -Tail 20"
Write-Output "Check passes so far: (Import-Csv 'data\hl_stage1_pass.csv').Count"
