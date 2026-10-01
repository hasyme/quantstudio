$log = "data\logs\daemon.log"
$out = "agent_workspace\_repair_progress.txt"
$deadline = (Get-Date).AddHours(6)
$seen = 0
while ((Get-Date) -lt $deadline) {
  $proc = Get-Process python -ErrorAction SilentlyContinue
  $lines = Get-Content $log -ErrorAction SilentlyContinue
  $batch = ($lines | Select-String -Pattern "export .* (\d+)/(\d+):" | Select-Object -Last 1)
  $errs  = ($lines | Select-String -Pattern "ValueError|fail-fast|ERROR" | Measure-Object).Count
  $ts = Get-Date -Format "HH:mm:ss"
  "$ts batches_last=[$($batch.Matches[0].Groups[1].Value)/$($batch.Matches[0].Groups[2].Value)] errors=$errs py_proc=$($proc.Count)" | Tee-Object -FilePath $out -Append
  if (-not $proc) { "PROCESS EXITED" | Tee-Object -FilePath $out -Append; break }
  Start-Sleep -Seconds 300
}
