#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""daemon 启动器（仓内正式入口，唯一人工 / 脚本入口）。

收敛自 `data/logs/launch_gen_main.py`（已标废弃，非版本管理目录），
三轮裁定 §3.3 #1：**hold 门唯一实现点 + 四验 + detached Popen**。

职责：
  1. **启动前 hold 门**（裁定闸门）——实现在
     `quantstudio/pipeline/daemon_hold_gate.py`；命中 ⇒ 非零退出 + 留痕；
     检查时点在写启动横幅与 `Popen` **之前**；
  2. 启动横幅四验：`sys.executable` / `sys.prefix` / `duckdb.__version__` / token，
     写入 `data/logs/daemon_bootstrap_<token>.log`（只捕获 basicConfig 之前的启动崩溃）；
  3. detached `Popen` 拉起 `python -m quantstudio.pipeline.daemon --mode forever`。

用法：
  py -3.11 scripts/launch_daemon.py [--config-dir config/profiles/mcp_only]
                                    [--max-iter N] [--wait-sec 15]

退出码：0 = 已拉起；3 = hold 门拒启；其他非零 = 启动失败。
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

QS_ROOT = Path(__file__).resolve().parent.parent
if str(QS_ROOT) not in sys.path:
    sys.path.insert(0, str(QS_ROOT))

from quantstudio.pipeline.daemon_hold_gate import (   # noqa: E402
    enforce_hold_or_exit,
    hold_marker_path,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="QuantStudio daemon 启动器（含 hold 门）")
    parser.add_argument("--config-dir", default="config/profiles/mcp_only",
                        help="配置目录（相对路径按仓库根解析）")
    parser.add_argument("--max-iter", type=int, default=None,
                        help="forever 模式下最大迭代次数（测试用）")
    parser.add_argument("--wait-sec", type=int, default=15,
                        help="拉起后等待并回显 status 的秒数；0=不等待")
    args = parser.parse_args()

    config_dir = Path(args.config_dir)
    if not config_dir.is_absolute():
        config_dir = QS_ROOT / config_dir

    # ① 启动前 hold 门（在写横幅与 Popen 之前；命中 ⇒ 非零退出 + 留痕）
    enforce_hold_or_exit("scripts/launch_daemon.py")

    log_dir = QS_ROOT / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    boot = log_dir / f"daemon_bootstrap_{token}.log"

    # ② 启动横幅四验（sys.executable / sys.prefix / duckdb.__version__ / token）
    import duckdb
    banner = ("=== DAEMON BOOTSTRAP BANNER ===\n"
              f"exe={sys.executable}\n"
              f"prefix={sys.prefix}\n"
              f"duckdb={duckdb.__version__}\n"
              f"token={token}\n"
              f"config_dir={config_dir}\n"
              f"hold_marker={hold_marker_path()}\n"
              "=== END BANNER ===\n")

    cmd = [sys.executable, "-m", "quantstudio.pipeline.daemon", "--mode", "forever",
           "--config-dir", str(config_dir), "--instance-token", token]
    if args.max_iter is not None:
        cmd += ["--max-iter", str(args.max_iter)]

    popen_kwargs = dict(
        stdin=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        cwd=str(QS_ROOT),
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
        close_fds=True,
    )
    if sys.platform == "win32":
        # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
        popen_kwargs["creationflags"] = 0x00000008 | 0x00000200
    else:
        popen_kwargs["start_new_session"] = True

    # ③ detached Popen（横幅 fd 由父进程写好后传入；父进程立即关闭自己的 fd）
    fd = os.open(str(boot), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, banner.encode("utf-8"))
        popen_kwargs["stdout"] = fd
        proc = subprocess.Popen(cmd, **popen_kwargs)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass

    print(f"launched pid={proc.pid} token={token[:8]}")
    print(f"banner: {boot.name}")

    if args.wait_sec > 0:
        time.sleep(args.wait_sec)
        from quantstudio.pipeline.daemon_lifecycle import read_daemon_status
        status = read_daemon_status()
        print("status:", json.dumps(status, ensure_ascii=False)[:200] if status else "None")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
