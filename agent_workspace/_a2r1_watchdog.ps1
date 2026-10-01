# A2 第 1 轮 —— 真实窗口，测写阶段吞吐
# 目标：拿到写阶段的稳定吞吐（行/分钟、片/分钟），据此重算 6 轮划分。
# 防护改进：优先「自然完成」；到点用 **优雅终止**（CloseMainWindow / 等待），
#           避免 Stop-Process -Force 绕过清理留下 .write_lock。
# 记录：每 60s 采样 累计写入行数 / 批次数 / CPU / 阶段，落盘供吞吐计算。
$ErrorActionPreference = "Continue"
$wd  = "D:\hasym\PycharmProjects\QuantStudio"
$log = Join-Path $wd "data\logs\daemon.log"
$out = Join-Path $wd "agent_workspace\_a2r1_watchdog.txt"
$maxMinutes = 150          # 单轮上限（留足写阶段观测）
$stallLimitMin = 25
$stallMinCpu = 2.0

function W([string]$m) { Add-Content -Path $out -Value "$(Get-Date -Format 'HH:mm:ss') $m" }

$start = "2026-01-01"; $end = "2026-02-15"
$before = (Get-Item $log).Length
W "A2R1 START window=$start..$end (log_before=$before)"

$proc = Start-Process -FilePath "C:\Users\hasym\.conda\envs\quant310\python.exe" `
  -ArgumentList @("-m","quantstudio.pipeline.daemon","--mode","once",
                  "--task","mcp_stock_minutes","--pull-mode","full_range",
                  "--start-date",$start,"--end-date",$end,
                  "--config-dir","config/profiles/mcp_only","--quality-audit","none") `
  -WorkingDirectory $wd `
  -RedirectStandardOutput (Join-Path $wd "agent_workspace\_a2r1.out.log") `
  -RedirectStandardError  (Join-Path $wd "agent_workspace\_a2r1.err.log") `
  -PassThru -NoNewWindow
W "spawned PID=$($proc.Id)"

$t0 = Get-Date; $lastLen = $before; $lastGrowth = Get-Date; $lastCpu = $proc.CPU
$fetchDoneAt = $null; $writeStartCount = 0

while (-not $proc.HasExited) {
  Start-Sleep -Seconds 60
  $now = Get-Date
  $len = (Get-Item $log).Length
  $cpu = $proc.CPU
  $cpuDelta = [math]::Round($cpu - $lastCpu, 1); $lastCpu = $cpu
  if ($len -gt $lastLen) { $lastLen = $len; $lastGrowth = $now }

  $el = [math]::Round(($now - $t0).TotalMinutes, 1)
  $st = [math]::Round(($now - $lastGrowth).TotalMinutes, 1)

  # 吞吐统计：从当前 daemon.log 抽取 wrote 行
  $lines = Get-Content $log -EA SilentlyContinue
  $wcnt = 0; $wrows = 0
  foreach ($l in $lines) { if ($l -match "wrote (\d+) rows") { $wrows += [int]$Matches[1]; $wcnt++ } }
  $batch = ($lines | Select-String -Pattern "export .* (\d+)/(\d+):" | Select-Object -Last 1)
  $bt = "n/a"
  if ($batch) {
    $g1 = $batch.Matches[0].Groups[1].Value
    $g2 = $batch.Matches[0].Groups[2].Value
    $bt = $g1 + "/" + $g2
  }
  $rate = 0
  if ($el -gt 0) { $rate = [math]::Round($wrows / $el, 0) }

  W ("elapsed={0}m logStale={1}m cpu+{2}s batch={3} wrBatches={4} wrRows={5} rowsPerMin={6}" -f `
     $el, $st, $cpuDelta, $bt, $wcnt, $wrows, $rate)

  if ($el -ge 10 -and $st -ge $stallLimitMin -and $cpuDelta -lt $stallMinCpu) {
    W "!! STALL -> graceful stop"
    $proc.CloseMainWindow() | Out-Null
    Start-Sleep -Seconds 10
    if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force -EA SilentlyContinue }
    break
  }
  if ($el -ge $maxMinutes) {
    W ("!! MAX " + $maxMinutes + "m -> graceful stop (avoid stale lock)")
    $proc.CloseMainWindow() | Out-Null
    Start-Sleep -Seconds 15
    if (-not $proc.HasExited) {
      W "graceful failed -> force kill; check .write_lock afterwards"
      Stop-Process -Id $proc.Id -Force -EA SilentlyContinue
    }
    break
  }
}
$elapsedFinal = [math]::Round(((Get-Date) - $t0).TotalMinutes, 1)
W ("A2R1 END exited=" + $proc.HasExited + " elapsed=" + $elapsedFinal + "m")
