# -*- coding: utf-8 -*-
"""duckdb 版本闸（笔5，2026-09-23 A 案裁定①）。

背景
----
`pyproject.toml` 双处钉 `duckdb>=1.4.5,<1.6`（issue duckdb#23645 回归防护，1.5.x 已放开；H9 完整 daemon 副本 A/B 实证 1.5.6 修复 stock_daily 写停摆），但 09-23
定谳：**钉版从未覆盖实际运行环境** ——
  - 官方主 venv `_runtime\\venv_quant_studio` = duckdb **1.5.4**（`scripts/activate_venv.bat` 指向它）
  - daemon 实际所用 `venv_miniQMT` = duckdb **1.5.3**（曾以该版本写主库两天）
  - 合规落点只有 `Python311` 与 `python3.12.9`（均 1.4.5）

本闸**启动即校验**，不合规即**拒启**，覆盖三入口：
  1) GUI：`main_gui.py`
  2) daemon：`quantstudio/pipeline/daemon.py` 的 `-m` 与**直启**（`python daemon.py …`）两形态
  3) GUI 拉起的 daemon 子进程（继承 GUI 解释器，且子进程自身亦过闸）

逃生阀
------
`QS_DUCKDB_VERSION_GATE=0`：显式放行（仅限明确知情的运维场景），会打印醒目警告。
默认（未设置或非 0）一律**拒绝**。
"""
from __future__ import annotations

import logging
import os
from typing import Tuple

logger = logging.getLogger(__name__)

REQUIRED_SERIES = ("1.4", "1.5")  # 允许 1.4.x 与 1.5.x（钉版 >=1.4.5,<1.6；#23645 已评估接受）
GATE_ENV = "QS_DUCKDB_VERSION_GATE"

_REMEDY = (
    "修复方式（择一）：\n"
    '  ① 把当前解释器的 duckdb 锁到钉版：pip install "duckdb>=1.4.5,<1.6"\n'
    "  ② 改用已合规的解释器启动：\n"
    "     C:\\Users\\Administrator\\AppData\\Local\\Programs\\Python\\Python311\\python.exe（1.4.5）\n"
    "     C:\\python3.12.9\\python.exe（1.4.5）\n"
    "  ③ 官方主 venv（_runtime\\venv_quant_studio，当前 1.5.4）需先降级再使用；\n"
    f"  ④ 确需临时放行：设 {GATE_ENV}=0（风险自负，会在日志留醒目警告）。\n"
    "依据：pyproject 双处 duckdb>=1.4.5,<1.6（issue duckdb#23645 回归钉版已放开 1.5.x）。"
)


def check_duckdb_version() -> Tuple[bool, str]:
    """校验 duckdb 版本是否属允许系列（1.4.x / 1.5.x）。返回 `(ok, version_or_error)`。"""
    try:
        import duckdb
    except Exception as e:  # 无 duckdb 亦视为不合规（无法判定即拒绝）
        return False, f"无法导入 duckdb: {type(e).__name__}: {e}"
    ver = str(getattr(duckdb, "__version__", "unknown"))
    return any(ver.startswith(s + ".") for s in REQUIRED_SERIES), ver


def require_duckdb_version(component: str) -> None:
    """不合规即拒启（`SystemExit(3)`）；合规则静默通过（debug 级留痕）。"""
    ok, detail = check_duckdb_version()
    if ok:
        logger.debug(f"[version-gate] {component}: duckdb {detail} 合规（需 1.4.x/1.5.x）")
        return

    if os.environ.get(GATE_ENV, "1").strip() == "0":
        logger.warning(
            f"[version-gate] {component}: duckdb {detail} **不合规**（需 {REQUIRED_SERIES}.x），"
            f"但 {GATE_ENV}=0 已显式放行 —— 风险自负（钉版依据 pyproject: duckdb>=1.4.5,<1.6）")
        return

    msg = (
        f"[version-gate] 拒绝启动：{component} 当前 duckdb = **{detail}**，"
        f"不在钉版区间（需 1.4.x/1.5.x，pyproject: duckdb>=1.4.5,<1.6）。\n"
        f"原因：1.5.x 存在已知回归（issue duckdb#23645，ART index-delete fatal），"
        f"且混版读写同一主库已被 09-23 事故证实为真实风险。\n" + _REMEDY
    )
    logger.error(msg)
    print(msg, file=__import__("sys").stderr)
    raise SystemExit(3)
