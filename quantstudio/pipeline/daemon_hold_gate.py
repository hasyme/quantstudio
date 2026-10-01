# -*- coding: utf-8 -*-
"""daemon 启动前 hold 门（裁定闸门）——纯只读、无 DB / 网络依赖。

背景（2026-09-27 事件，见 docs/daemon-skip-weekdays-v3-design.md §3）：
总调度已裁定「daemon 重启=暂缓（附四就绪条件）」，但新 daemon（PID 8724）仍被拉起，
四就绪条件零满足。缺口性质：**裁定只存在于会话/文书层，启动路径上没有任何机器可读的
闸门** ⇒ 任何人（含自动化 / 新会话）都可能再次违裁重启。本模块把裁定落成机器可读的门。

标记文件：`data/daemon_hold.marker`（UTF-8 无 BOM，JSON）

    {"ruling_id": "<裁定标识>", "issued_at": "<ISO8601>", "expires_at": "<ISO8601>",
     "reason": "<短因>", "issued_by": "<签发方>"}

语义（六步②裁定 §7-4：**到期自动放行**）：
  - 文件存在且 `now < expires_at` ⇒ **hold 生效 ⇒ 拒启**；
  - `expires_at` 缺失 / 已过期 / JSON 损坏 ⇒ **失效（视为通行）+ 留痕告警**，绝不静默；
    —— 该取舍防「忘记撤裁导致长期停采」；签发方负 `expires_at` 显式设定的责任。

硬不变量（设计件 §3.4）：
  1. 无 marker 时行为逐位不变（不引入新启动失败面、不增加可感知延迟）；
  2. 检查**纯只读**（不改 marker 内容、**不自动删除**过期 marker）；
  3. 拒启路径必须**非零退出码 + 留痕**（不得静默 exit 0）；
  4. hold 判定**不得依赖网络 / 数据库**（DB 静止态、无网络时同样可判）；
  5. 三处启动入口共用本实现：`scripts/launch_daemon.py`（唯一人工/脚本入口）、
     `quantstudio/gui/daemon_process.start_daemon_subprocess`（GUI 入口）、
     `quantstudio/pipeline/daemon.py::main()`（最终兜底）。
"""
from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

HOLD_MARKER_NAME = "daemon_hold.marker"
HOLD_CHECK_LOG_NAME = "daemon_hold_check.log"
REJECTED_LOG_PREFIX = "daemon_launch_rejected_"
# 拒启退出码：与既有 1（启动被拒/异常）/ 2（目标库非主库）区分，便于审计直判"因 hold 门拒启"。
HOLD_REJECT_EXIT_CODE = 3


def _data_root() -> Path:
    from quantstudio._paths import DATA_ROOT
    return Path(DATA_ROOT)


def hold_marker_path() -> Path:
    return _data_root() / HOLD_MARKER_NAME


def hold_check_log_path() -> Path:
    return _data_root() / "logs" / HOLD_CHECK_LOG_NAME


@dataclass
class HoldDecision:
    """一次 hold 判定的结果（可审计）。"""
    hold: bool
    reason: str          # no_marker | hold_active | expired | missing_expires_at |
                         # unparsable_expires_at | malformed | unreadable
    detail: str
    checked_at: str = ""
    marker: dict = field(default_factory=dict)
    marker_text: str = ""
    expires_at: str = ""


class HoldActiveError(RuntimeError):
    """hold 门命中：调用方必须放弃启动（不得 Popen）。"""

    def __init__(self, decision: HoldDecision):
        super().__init__(decision.detail)
        self.decision = decision


# ---------------------------------------------------------------------------
# 判定（纯只读）
# ---------------------------------------------------------------------------

def _parse_iso(value: str) -> Optional[datetime]:
    """解析 ISO8601；带时区者折算为本地 naive。不可解析返回 None。"""
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone().replace(tzinfo=None)
    return dt


def evaluate_hold(marker_path: Optional[Path] = None,
                  now: Optional[datetime] = None) -> HoldDecision:
    """评估 hold 门（不写任何文件）。

    marker_path=None ⇒ data/daemon_hold.marker；now=None ⇒ datetime.now()。
    """
    path = Path(marker_path) if marker_path is not None else hold_marker_path()
    now = now or datetime.now()
    if now.tzinfo is not None:
        now = now.astimezone().replace(tzinfo=None)
    checked_at = now.isoformat(timespec="seconds")

    if not path.exists():
        return HoldDecision(False, "no_marker", "无 hold 标记文件", checked_at=checked_at)
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as e:
        # 读不到（权限/瞬时 IO）→ 按失效处理 + 留痕；绝不因门本身故障阻断启动。
        return HoldDecision(False, "unreadable", f"marker 读取失败（按失效处理）: {e}",
                            checked_at=checked_at)

    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        obj = None
    if not isinstance(obj, dict):
        return HoldDecision(False, "malformed",
                            "marker 非法 JSON / 非对象（按失效处理）",
                            marker_text=raw, checked_at=checked_at)

    raw_exp = obj.get("expires_at")
    if raw_exp in (None, ""):
        return HoldDecision(False, "missing_expires_at",
                            "marker 缺 expires_at（按失效处理；签发方须显式设定到期时刻）",
                            marker_text=raw, marker=obj, checked_at=checked_at)

    exp = _parse_iso(str(raw_exp))
    if exp is None:
        return HoldDecision(False, "unparsable_expires_at",
                            f"expires_at 无法解析：{raw_exp!r}（按失效处理）",
                            marker_text=raw, marker=obj, expires_at=str(raw_exp),
                            checked_at=checked_at)

    if now < exp:
        return HoldDecision(True, "hold_active", f"hold 生效至 {raw_exp}",
                            marker_text=raw, marker=obj, expires_at=str(raw_exp),
                            checked_at=checked_at)
    return HoldDecision(False, "expired", f"hold 已于 {raw_exp} 到期（自动放行）",
                        marker_text=raw, marker=obj, expires_at=str(raw_exp),
                        checked_at=checked_at)


# ---------------------------------------------------------------------------
# 留痕（无论通过 / 拒绝）
# ---------------------------------------------------------------------------

def _marker_summary(decision: HoldDecision) -> str:
    parts = [f"reason={decision.reason}"]
    for key in ("ruling_id", "issued_by", "issued_at", "expires_at"):
        value = (decision.marker or {}).get(key)
        if value not in (None, ""):
            parts.append(f"{key}={value}")
    return "; ".join(parts)


def append_hold_check_log(decision: HoldDecision, entry_point: str,
                          pid: Optional[int] = None,
                          log_path: Optional[Path] = None) -> Optional[Path]:
    """向 data/logs/daemon_hold_check.log 追加一行（通过/拒绝两态各留一行，可审计）。

    契约：本函数**永不抛异常**（留痕故障不得反噬判定），失败仅告警到 stderr。
    """
    path = Path(log_path) if log_path is not None else hold_check_log_path()
    ts = decision.checked_at or datetime.now().isoformat(timespec="seconds")
    line = (f"{ts}\t{'REJECT' if decision.hold else 'PASS'}\t"
            f"entry={entry_point}\tpid={os.getpid() if pid is None else pid}\t"
            f"{_marker_summary(decision)}\t{decision.detail}\n")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(line)
        return path
    except OSError as e:
        print(f"[hold-gate] 留痕写入失败 {path}: {e}", file=sys.stderr, flush=True)
        return None


def write_reject_trace(decision: HoldDecision, entry_point: str,
                       log_dir: Optional[Path] = None) -> Optional[Path]:
    """写拒启留痕 data/logs/daemon_launch_rejected_<YYYYmmdd_HHMMSS>.log（含 marker 全文）。"""
    directory = Path(log_dir) if log_dir is not None else (_data_root() / "logs")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = directory / f"{REJECTED_LOG_PREFIX}{stamp}.log"
    seq = 1
    while path.exists():          # 同秒重入防覆盖
        path = directory / f"{REJECTED_LOG_PREFIX}{stamp}_{seq}.log"
        seq += 1
    content = "\n".join([
        "=== DAEMON LAUNCH REJECTED (hold gate) ===",
        f"entry_point = {entry_point}",
        f"checked_at  = {decision.checked_at}",
        f"pid         = {os.getpid()}",
        f"reason      = {decision.reason}",
        f"detail      = {decision.detail}",
        f"expires_at  = {decision.expires_at or '(none)'}",
        "--- marker 全文 ---",
        decision.marker_text or "(empty)",
        "=== END ===",
    ]) + "\n"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path
    except OSError as e:
        print(f"[hold-gate] 拒启留痕写入失败 {path}: {e}", file=sys.stderr, flush=True)
        return None


# ---------------------------------------------------------------------------
# 三入口共用 API
# ---------------------------------------------------------------------------

def check_hold(entry_point: str, now: Optional[datetime] = None,
               marker_path: Optional[Path] = None,
               log_dir: Optional[Path] = None) -> HoldDecision:
    """评估 + 留痕（通过/拒绝都留一行）。命中**不**抛异常、**不**退出。"""
    decision = evaluate_hold(marker_path=marker_path, now=now)
    append_hold_check_log(
        decision, entry_point,
        log_path=(Path(log_dir) / HOLD_CHECK_LOG_NAME) if log_dir is not None else None)
    return decision


def ensure_not_held(entry_point: str, now: Optional[datetime] = None,
                    marker_path: Optional[Path] = None,
                    log_dir: Optional[Path] = None) -> HoldDecision:
    """hold 门强校验（进程内路径用，如 GUI 启动函数）。

    命中 ⇒ 写拒启留痕 + 抛 `HoldActiveError`（调用方必须放弃启动，不得 Popen）。
    """
    decision = check_hold(entry_point, now=now, marker_path=marker_path, log_dir=log_dir)
    if decision.hold:
        write_reject_trace(decision, entry_point, log_dir=log_dir)
        raise HoldActiveError(decision)
    return decision


def enforce_hold_or_exit(entry_point: str, now: Optional[datetime] = None,
                         marker_path: Optional[Path] = None,
                         log_dir: Optional[Path] = None) -> HoldDecision:
    """hold 门强校验（进程入口用）：命中 ⇒ 拒启留痕 + 非零退出码（本函数不返回）。

    硬不变量 3：拒启必须非零退出 + 留痕，绝不静默 exit 0。
    """
    try:
        return ensure_not_held(entry_point, now=now, marker_path=marker_path, log_dir=log_dir)
    except HoldActiveError as e:
        marker = marker_path if marker_path is not None else hold_marker_path()
        logger.error("[hold-gate] 拒启：%s（entry=%s, marker=%s）。"
                     "裁定未解除前不得启动 daemon；解除 = 删除 marker 或等待 expires_at 到期。",
                     e.decision.detail, entry_point, marker)
        raise SystemExit(HOLD_REJECT_EXIT_CODE) from None
