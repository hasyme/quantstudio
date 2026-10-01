# 缺陷1 数据修复重跑（第二次尝试）—— 带硬超时看门狗
# 变更：sources_config.json 增 export_async=true（绕过服务端 60s 软时间预算）
# 防护：最大运行 120 分钟；日志 12 分钟无增长即判空转并终止。
$ErrorActionPreference = "Continue"
$log = "data\logs\daemon.log"
$out = "agent_workspace\_repair2_watchdog.txt"
$maxMinutes = 120
$staleLimitMin = 12
$wd = "D:\hasym\PycharmProjects\QuantStudio"

function W([string]$m) {
  $line = "$(Get-Date -Format 'HH:mm:ss') $m"
  Add-Content -Path (Join-Path $wd $out) -Value $line
}

$logPath = Join-Path $wd $log
$beforeLen = (Get-Item $logPath).Length
W "WATCHDOG START (log before=$beforeLen bytes)"

$py = "C:\Users\hasym\.conda\envs\quant310\python.exe"
$args = @("-m","quantstudio.pipeline.daemon","--mode","once",
          "--task","mcp_stock_minutes","--pull-mode","full_range",
          "--config-dir","config/profiles/mcp_only",
          "--quality-audit","none")
$proc = Start-Process -FilePath $py -ArgumentList $args -WorkingDirectory $wd `
  -RedirectStandardOutput (Join-Path $wd "agent_workspace\_repair2.out.log") `
  -RedirectStandardError  (Join-Path $wd "agent_workspace\_repair2.err.log") `
  -PassThru -NoNewWindow
W "spawned PID=$($proc.Id)"

$start = Get-Date
$lastLen = $beforeLen
$lastGrowth = Get-Date

while (-not $proc.HasExited) {
  Start-Sleep -Seconds 60
  $now = Get-Date
  $len = (Get-Item $logPath).Length
  if ($len -gt $lastLen) { $lastLen = $len; $lastGrowth = $now }
  $elapsedMin = [math]::Round(($now - $start).TotalMinutes, 1)
  $staleMin = [math]::Round(($now - $lastGrowth).TotalMinutes, 1)

  $batchTxt = "n/a"
  $b = Get-Content $logPath -Tail 500 -ErrorAction SilentlyContinue |
       Select-String -Pattern "export .* (\d+)/(\d+):" | Select-Object -Last 1
  if ($b) { $batchTxt = "$($b.Matches[0].Groups[1].Value)/$($b.Matches[0].Groups[2].Value)" }

  $fails = (Get-Content $logPath -Tail 3000 -ErrorAction SilentlyContinue |
            Select-String -Pattern "export_exceeds_time_budget" | Measure-Object).Count

  $msg = "elapsed={0}m stale={1}m batch={2} logMB={3} budgetErr={4}" -f `
         $elapsedMin, $staleMin, $batchTxt, [math]::Round($len / 1MB, 2), $fails
  W $msg

  if ($staleMin -ge $staleLimitMin) {
    W "!! STALL (no log growth ${staleMin}m) -> KILL"
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    break
  }
  if ($elapsedMin -ge $maxMinutes) {
    W "!! MAX RUNTIME ${maxMinutes}m -> KILL"
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    break
  }
}
W "WATCHDOG END (exited=$($proc.HasExited))"
