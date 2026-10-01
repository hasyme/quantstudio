# A0/A1 重跑 —— 修正版看门狗
# 修正上一轮缺陷：fetch 阶段结束后写阶段日志稀疏，15min STALL 阈值误杀。
# 本版：STALL 判据 = 日志无增长 **且** 进程 CPU 无增长（双条件，避免误杀慢写）。
# 超时：单轮 90 分钟。
$ErrorActionPreference = "Continue"
$wd   = "D:\hasym\PycharmProjects\QuantStudio"
$log  = Join-Path $wd "data\logs\daemon.log"
$out  = Join-Path $wd "agent_workspace\_a1b_watchdog.txt"
$stallLimitMin = 25
$maxMinutes    = 90

function W([string]$m) { Add-Content -Path $out -Value "$(Get-Date -Format 'HH:mm:ss') $m" }

$start = $args[0]; $end = $args[1]; $tag = $args[2]
$before = (Get-Item $log).Length
W "WATCHDOG START tag=$tag window=$start..$end (log_before=$before)"

$proc = Start-Process -FilePath "C:\Users\hasym\.conda\envs\quant310\python.exe" `
  -ArgumentList @("-m","quantstudio.pipeline.daemon","--mode","once",
                  "--task","mcp_stock_minutes","--pull-mode","full_range",
                  "--start-date",$start,"--end-date",$end,
                  "--config-dir","config/profiles/mcp_only","--quality-audit","none") `
  -WorkingDirectory $wd `
  -RedirectStandardOutput (Join-Path $wd "agent_workspace\_a1b.out.log") `
  -RedirectStandardError  (Join-Path $wd "agent_workspace\_a1b.err.log") `
  -PassThru -NoNewWindow
W "spawned PID=$($proc.Id)"

$t0 = Get-Date; $lastLen = $before; $lastGrowth = Get-Date
$lastCpu = $proc.CPU

while (-not $proc.HasExited) {
  Start-Sleep -Seconds 60
  $now = Get-Date
  $len = (Get-Item $log).Length
  $cpu = $proc.CPU
  $cpuDelta = [math]::Round($cpu - $lastCpu, 1)
  $lastCpu = $cpu
  if ($len -gt $lastLen) { $lastLen = $len; $lastGrowth = $now }

  $el = [math]::Round(($now - $t0).TotalMinutes,1)
  $st = [math]::Round(($now - $lastGrowth).TotalMinutes,1)

  # 阶段判定：fetch 完成标志 = "流式 export 分批=" 出现
  $phase = "fetch"
  $tail = Get-Content $log -Tail 400 -EA SilentlyContinue
  if ($tail | Select-String -Pattern "流式 export 分批=" ) { $phase = "write" }
  $b = $tail | Select-String -Pattern "export .* (\d+)/(\d+):" | Select-Object -Last 1
  $bt = if ($b) { "$($b.Matches[0].Groups[1].Value)/$($b.Matches[0].Groups[2].Value)" } else { "n/a" }

  W ("elapsed={0}m logStale={1}m cpu+{2}s phase={3} batch={4}" -f $el,$st,$cpuDelta,$phase,$bt)

  # STALL 双条件：日志不增长 **且** CPU 几乎不动
  if ($st -ge $stallLimitMin -and $cpuDelta -lt 2.0) {
    W "!! STALL (logStale=${st}m && cpuDelta=${cpuDelta}s) -> KILL"
    Stop-Process -Id $proc.Id -Force -EA SilentlyContinue; break
  }
  if ($el -ge $maxMinutes) {
    W "!! MAX ${maxMinutes}m -> KILL"
    Stop-Process -Id $proc.Id -Force -EA SilentlyContinue; break
  }
}
W "WATCHDOG END exited=$($proc.HasExited) elapsed=$([math]::Round(((Get-Date)-$t0).TotalMinutes,1))m"
