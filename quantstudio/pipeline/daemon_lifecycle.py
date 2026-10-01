"""DaemonLifecycle — v3 常驻采集进程的轻量调度核心。

替代旧 ResidentCollector.run_forever() 的常驻循环语义（旧版长期持有 DuckDB
共享连接，与 GUI 跨进程读库冲突）。本模块实现：

1. **.daemon.lock**：daemon 单实例锁，daemon 生命周期持有。
2. **daemon_status.json**：带 instance_token 的状态文件，原子写。
3. **psutil 五验**：身份校验（PID + create_time + exe + cmdline + token）。
4. **.collector_run.lock 封装**：采集/健康检查的 per-operation 互斥。
5. **daemon_run_state.json**：每日执行状态持久化（completed 严格语义）。
6. **daemon_stop.request 轮询**：优雅停止信号。
7. **轻量调度循环**：空闲期无 ResidentCollector、无 DuckDB 连接，GUI 可正常读库。

核心纪律（评审决议）：
- daemon 空闲时不得持有 DuckDB 连接；
- collector_run.lock 必须在 from_configs 之前；
- 每轮采集/审计/健康检查后 finally 调 collector.close()；
- 只有 daemon 自己写/清 status；GUI 只读（强制停止路径除外，且需五验通过）。
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import psutil
from filelock import FileLock, Timeout

logger = logging.getLogger(__name__)

# 运行时文件目录（与 .collector.lock 同惯例）
def _data_root() -> Path:
    from quantstudio._paths import DATA_ROOT
    return DATA_ROOT

# 状态/信号/锁文件路径
def daemon_lock_path() -> Path:
    return _data_root() / ".daemon.lock"

def collector_run_lock_path() -> Path:
    return _data_root() / ".collector_run.lock"

def daemon_status_path() -> Path:
    return _data_root() / "daemon_status.json"

def daemon_run_state_path() -> Path:
    return _data_root() / "daemon_run_state.json"

def daemon_stop_request_path() -> Path:
    return _data_root() / "daemon_stop.request"


# ---------------------------------------------------------------------------
# psutil 五验（评审 9）
# ---------------------------------------------------------------------------

def verify_daemon_identity(status: dict) -> str:
    """校验状态文件对应的进程身份。

    返回 'alive' | 'stale' | 'denied' | 'unknown'：
      - alive：五项全部通过，可安全 stop/clear。
      - stale：进程不存在或身份不匹配（PID 复用他进程），可清理 stale status。
      - denied：权限不足，**不清不杀**，只能提示用户。
      - unknown：内核查询瞬时失败（D2，2026-09-14 客户 macOS 案）——**不得**当作死亡；
        调用方应保持现状、继续观察，绝不清理 token/status（族谱第四例：「未知必须自成一态」）。

    五项校验：
      1. PID 存活；
      2. create_time ±1s；
      3. exe 路径（双侧 realpath 容错，不匹配仅告警不判死）；
      4. cmdline 含 'quantstudio.pipeline.daemon'；
      5. instance_token（仅当 status 的 cmdline 含 token 时，要求 p.cmdline() 亦含）。
    """
    if not status or "pid" not in status:
        return "stale"
    try:
        p = psutil.Process(status["pid"])
    except psutil.NoSuchProcess:
        return "stale"
    except psutil.AccessDenied:
        return "denied"
    try:
        if not p.is_running():
            return "stale"
        # create_time ±1s 浮点误差容忍
        if abs(p.create_time() - float(status.get("create_time", 0))) > 1.0:
            return "stale"
        # exe 路径：双侧 realpath 容错（D1，2026-09-14 客户 macOS 案）——
        # p.exe() 在 macOS/Linux 取内核真路径（symlink 已解引用），而 status["exe"]
        # 记的是启动调用路径；单侧 abspath 会让 symlink 安装场景必然不等 → 活进程被判死。
        # 不等时降级为「不匹配但非致命」：记 warning 交调用方去抖裁决，不在此单点判死
        # （PID 复用场景由 create_time 校验兜底，见上）。
        actual_exe = os.path.normcase(os.path.realpath(os.path.abspath(p.exe() or "")))
        stored_exe = os.path.normcase(os.path.realpath(os.path.abspath(str(status.get("exe", "") or ""))))
        if actual_exe != stored_exe:
            logger.warning(
                "[daemon-identity] exe 路径不一致（疑似 symlink 真路径漂移，不据此判死）："
                "actual=%r stored=%r", actual_exe, stored_exe)
        # cmdline 含 quantstudio.pipeline.daemon
        p_cmdline = p.cmdline()
        cmdline_str = " ".join(p_cmdline)
        if "quantstudio.pipeline.daemon" not in cmdline_str:
            return "stale"
        # instance_token：仅当 status 的 cmdline 记录含 token 时（GUI 启动场景）
        # 才要求 p.cmdline() 也含 token；CLI 手动启动场景不校验（评审 6 方案 B）。
        token = status.get("instance_token")
        stored_cmdline = status.get("cmdline", [])
        if token and token in stored_cmdline:
            if token not in p_cmdline:
                return "stale"
        return "alive"
    except psutil.NoSuchProcess:
        return "stale"
    except psutil.AccessDenied:
        return "denied"
    except (psutil.Error, OSError):
        # D2（2026-09-14 客户 macOS 案）：「查不出来」必须自成一态，不得塌缩成死亡；
        # 调用方语义：unknown = 无法确认 → 保持现状、继续观察，绝不清理 token/status。
        return "unknown"


# ---------------------------------------------------------------------------
# status 文件原子读写（评审 5 所有权）
# ---------------------------------------------------------------------------

def _atomic_write_json(path: Path, data: dict):
    """临时文件 + os.replace 原子写。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}.{uuid.uuid4().hex[:8]}")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def read_daemon_status() -> Optional[dict]:
    """读取 status 文件。损坏/缺失返回 None。"""
    path = daemon_status_path()
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def clear_daemon_status_if_owned(status: dict) -> bool:
    """清理 status 文件，但必须三验（pid+create_time+token）属于当前进程。

    返回 True 表示已清理；False 表示不是自己的（他进程持有），不删。
    daemon 正常退出和 GUI 强制停止都用此函数。
    """
    if not status:
        return False
    current_pid = os.getpid()
    try:
        current_create = psutil.Process(current_pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        current_create = 0.0
    token_match = (status.get("instance_token") == status.get("_owner_token")) or \
                  (status.get("pid") == current_pid)
    pid_match = status.get("pid") == current_pid
    create_match = abs(float(status.get("create_time", 0)) - current_create) <= 1.0
    # daemon 自己清理：pid + create_time 必匹配；token 比对由调用方保证
    if pid_match and create_match:
        try:
            daemon_status_path().unlink(missing_ok=True)
            return True
        except OSError:
            return False
    return False


def clear_stale_status() -> str:
    """清理陈旧 status（进程已不存在）。daemon 启动时调用。

    返回 'cleared' | 'alive_other' | 'denied' | 'none'：
      - cleared：原 status 对应进程已死，已清理；
      - alive_other：原 status 进程仍活（另一个 daemon 在跑），调用方应退出；
      - denied：权限不足无法确认，**不清**，调用方应退出；
      - none：无 status 文件。
    """
    status = read_daemon_status()
    if status is None:
        return "none"
    result = verify_daemon_identity(status)
    if result == "alive":
        return "alive_other"  # 另一个 daemon 在跑
    if result == "denied":
        return "denied"  # 不清不杀
    # stale：可清理
    try:
        daemon_status_path().unlink(missing_ok=True)
        logger.info("[DaemonLifecycle] 清理陈旧 status 文件（原进程已退出）")
        return "cleared"
    except OSError as e:
        logger.warning(f"[DaemonLifecycle] 清理陈旧 status 失败: {e}")
        return "denied"


# ---------------------------------------------------------------------------
# run_state 持久化（评审 7）
# ---------------------------------------------------------------------------

def read_run_state() -> Optional[dict]:
    path = daemon_run_state_path()
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def write_run_state(**fields):
    """原子写 run_state（合并已有字段）。"""
    path = daemon_run_state_path()
    current = read_run_state() or {}
    current.update(fields)
    _atomic_write_json(path, current)


def is_today_completed() -> bool:
    """判断今日 run_state 是否已 completed（重启不重跑）。"""
    state = read_run_state()
    if not state:
        return False
    today = datetime.now().strftime("%Y-%m-%d")
    return state.get("scheduled_date") == today and state.get("status") == "completed"


def is_today_pending_rerun() -> bool:
    """判断今日是否有未完成轮次（running/interrupted），需重跑。

    stale-running（进程已死但状态未清）也视为 pending rerun。
    """
    state = read_run_state()
    if not state:
        return False
    today = datetime.now().strftime("%Y-%m-%d")
    if state.get("scheduled_date") != today:
        return False
    if state.get("status") in ("running", "interrupted"):
        return True
    return False


# ---------------------------------------------------------------------------
# stop.request 轮询（评审 5/6）
# ---------------------------------------------------------------------------

def read_stop_request() -> Optional[dict]:
    path = daemon_stop_request_path()
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def consume_stop_request_if_matched(instance_token: str) -> bool:
    """daemon 检测 stop.request：token 匹配则删除并返回 True。"""
    req = read_stop_request()
    if req is None:
        return False
    if req.get("instance_token") != instance_token:
        return False  # 不是给我的
    try:
        daemon_stop_request_path().unlink(missing_ok=True)
        logger.info("[DaemonLifecycle] 收到停止请求，准备优雅退出")
        return True
    except OSError:
        return True


# ---------------------------------------------------------------------------
# collector_run.lock 封装（per-operation）
# ---------------------------------------------------------------------------

class CollectorRunLock:
    """collector_run.lock 的上下文管理器封装。

    timeout：
      - 整轮采集：5s（等待 GUI 手动释放）；
      - 健康检查：0s（非阻塞，拿不到即跳过）。
    """

    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout
        self._lock: Optional[FileLock] = None

    def __enter__(self) -> "CollectorRunLock":
        # T2（2026-09-20）：preserve_lock_file=True —— 锁文件常驻不删，
        # 消除释放删文件的跨进程竞态（POSIX flock 陷阱，计划 §0.3）
        self._lock = FileLock(str(collector_run_lock_path()), timeout=self.timeout,
                              preserve_lock_file=True)
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._lock is not None:
            try:
                self._lock.release()
            except Exception:
                pass
        return False

    def try_acquire(self) -> bool:
        """非阻塞尝试，失败返回 False（不抛）。"""
        self._lock = FileLock(str(collector_run_lock_path()), timeout=0,
                              preserve_lock_file=True)
        try:
            self._lock.acquire(timeout=0)
            return True
        except Timeout:
            self._lock = None
            return False


# ---------------------------------------------------------------------------
# DaemonLifecycle 主类（轻量调度循环）
# ---------------------------------------------------------------------------

class DaemonLifecycle:
    """daemon 子进程的轻量调度核心。

    持有 .daemon.lock 全程；空闲期不创建 ResidentCollector、不持有 DuckDB 连接；
    到执行点在 collector_run.lock 内临时 from_configs + 跑轮次 + close。
    """

    # 单实例锁（daemon 生命周期持有）
    _instance_lock: Optional[FileLock] = None

    def __init__(self, config_dir: Path, instance_token: Optional[str] = None,
                 max_iterations: Optional[int] = None):
        self.config_dir = Path(config_dir)
        self.instance_token = instance_token or uuid.uuid4().hex
        self.max_iterations = max_iterations
        self._running = True
        self._status: Optional[dict] = None  # 本进程的 status 快照

    # -- 启动握手 [1][2][3][4] --
    def acquire_instance_lock(self) -> bool:
        """[1] 获取 .daemon.lock（非阻塞）。失败=另一个 daemon 在跑。"""
        lock_path = daemon_lock_path()
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._instance_lock = FileLock(str(lock_path), timeout=0,
                                        preserve_lock_file=True)
        try:
            self._instance_lock.acquire(timeout=0)
            return True
        except Timeout:
            logger.warning("[DaemonLifecycle] .daemon.lock 已被占用（另一个 daemon 运行中），退出")
            return False

    def release_instance_lock(self):
        """释放 .daemon.lock（publish_status 失败或退出时调用）。"""
        if self._instance_lock is not None:
            try:
                self._instance_lock.release()
            except Exception:
                pass
            # T2（2026-09-20）步④：锁文件**常驻不删**——与九处 preserve_lock_file=True
            # 为同一决定。原显式 unlink（:368）是该决定的漏网点，且注释"FileLock 不会
            # 自动删"为加参前旧语义残留：
            #   POSIX：删锁文件 = flock inode 竞态本体（计划 §0.3 陷阱）；
            #   Windows：释放后立即删与并发 acquire 的重建竞态（msvcrt 句柄语义）。
            # 无调用方依赖"退出后文件消失"（acquire 自动建文件）。
            self._instance_lock = None

    def publish_status(self):
        """[3][4] 清陈旧 status + 原子写本进程 status。

        Review FIX-4：alive_other / denied 都必须**拒绝启动**，不得覆盖原 status。
        - alive_other：另一个 daemon 在跑；
        - denied：无法确认原 status 进程身份（AccessDenied），不清不杀也不覆盖。
        两种情况都 raise，调用方（main）负责释放 .daemon.lock 并退出。
        """
        clear_result = clear_stale_status()
        if clear_result == "alive_other":
            raise RuntimeError("检测到另一个 daemon 仍在运行，拒绝启动")
        if clear_result == "denied":
            raise RuntimeError(
                "无法确认陈旧 status 进程身份（AccessDenied），"
                "不清不杀也不覆盖原 status；请人工处理 data/daemon_status.json 后重启")
        # clear_result in ("cleared", "none") → 安全发布本进程 status
        p = psutil.Process(os.getpid())
        cmdline = sys.argv[:]
        self._status = {
            "pid": os.getpid(),
            "create_time": p.create_time(),
            "exe": sys.executable,
            "cmdline": cmdline,
            "instance_token": self.instance_token,
            "config_dir": str(self.config_dir),
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "status": "running",
        }
        _atomic_write_json(daemon_status_path(), self._status)
        logger.info(f"[DaemonLifecycle] status 已发布 (pid={self._status['pid']}, "
                    f"token={self.instance_token[:8]}...)")

    def update_status(self, **fields):
        """更新本进程 status（如 status: running → stop_requested）。"""
        if self._status is None:
            return
        self._status.update(fields)
        _atomic_write_json(daemon_status_path(), self._status)

    def clear_own_status(self):
        """退出时清理自己的 status（pid + create_time 双验）。"""
        if self._status is None:
            return
        if clear_daemon_status_if_owned(self._status):
            logger.info("[DaemonLifecycle] 已清理本进程 status 文件")
        else:
            logger.warning("[DaemonLifecycle] status 已被替换或非本进程所有，不清理")

    # -- 调度循环 [6] --
    def run_forever(self):
        """轻量调度循环：持 .daemon.lock，无 collector 无 conn。

        配置热加载：每轮迭代重新读 collector_tasks.json，用户在 GUI 配置编辑里
        改 daily_time / check_interval 后无需重启 daemon，下个 tick 即生效。
        """
        from quantstudio._paths import DATA_ROOT
        # 启动时读一次（仅用于首条日志，循环内每次重新读）
        tasks_cfg = self._read_tasks_cfg()
        sched_cfg = tasks_cfg.get("daemon_schedule", {}) if tasks_cfg else {}
        daily_time = sched_cfg.get("daily_time", "17:00")
        check_interval = sched_cfg.get("check_interval_sec", 300)
        skip_weekdays = list(sched_cfg.get("skip_weekdays", []) or [])

        logger.info(f"[DaemonLifecycle] 启动轻量调度循环。daily_time={daily_time}, "
                    f"check_interval={check_interval}s, skip_weekdays={skip_weekdays}, "
                    f"today_completed={is_today_completed()}, "
                    f"today_pending_rerun={is_today_pending_rerun()}")

        iteration = 0
        last_daily_time = daily_time
        last_check_interval = check_interval
        last_skip_weekdays = skip_weekdays
        while self._running:
            iteration += 1
            # 检测停止请求
            if consume_stop_request_if_matched(self.instance_token):
                self._graceful_exit()
                break
            # 配置热加载：每轮重新读调度配置（用户改 daily_time 后下个 tick 生效）
            tasks_cfg = self._read_tasks_cfg()
            sched_cfg = tasks_cfg.get("daemon_schedule", {}) if tasks_cfg else {}
            daily_time = sched_cfg.get("daily_time", "17:00")
            check_interval = sched_cfg.get("check_interval_sec", 300)
            skip_weekdays = list(sched_cfg.get("skip_weekdays", []) or [])
            # 配置变化时打日志（便于排查"为何触发时间变了"）
            if (daily_time != last_daily_time or check_interval != last_check_interval
                    or skip_weekdays != last_skip_weekdays):
                logger.info(f"[DaemonLifecycle] 检测到调度配置变化："
                            f"daily_time {last_daily_time}→{daily_time}, "
                            f"check_interval {last_check_interval}→{check_interval}")
                logger.info(f"[DaemonLifecycle] skip_weekdays "
                            f"{last_skip_weekdays}→{skip_weekdays}")
                last_daily_time = daily_time
                last_check_interval = check_interval
                last_skip_weekdays = skip_weekdays
            # 调度判定
            now = datetime.now()
            today_str = now.strftime("%Y-%m-%d")
            now_hm = now.strftime("%H:%M")
            should_run = (now_hm >= daily_time and
                          not is_today_completed() and
                          (not is_today_pending_rerun() or self._is_first_run_today(today_str)))
            # pending_rerun 场景：首 tick 立即补跑
            if is_today_pending_rerun() and not is_today_completed():
                should_run = True
            # §7-1（裁定 P1）：跳过日整体抑制 —— 含上面的 pending_rerun 补跑分支。
            # 星期口径沿用旧实现（daemon.py:2136 `now.weekday()`）：0=周一 … 6=周日。
            # 抑制事实写入 run_state（skip_reason=skip_weekday），使「当日为何未跑」可审计；
            # 每个跳过日只写/记一次，避免每 tick 刷盘刷日志。
            weekday = now.weekday()
            if should_run and weekday in skip_weekdays:
                should_run = False
                _skip_state = read_run_state() or {}
                if not (_skip_state.get("skip_reason") == "skip_weekday"
                        and _skip_state.get("skip_weekday_date") == today_str):
                    write_run_state(skip_reason="skip_weekday", skip_weekday=weekday,
                                    skip_weekday_date=today_str)
                    logger.info(f"[DaemonLifecycle] 今日 weekday={weekday} 属 "
                                f"skip_weekdays={skip_weekdays}，本日不发起采集轮次"
                                f"（skip_reason=skip_weekday, pending_rerun="
                                f"{is_today_pending_rerun()}）")
            if should_run:
                logger.info(f"[DaemonLifecycle] === 开始每日采集轮次 ({today_str}, daily_time={daily_time}) ===")
                try:
                    self.run_one_cycle(tasks_cfg)
                except Exception as e:
                    logger.exception(f"[DaemonLifecycle] 采集轮次异常: {e}")
                logger.info("[DaemonLifecycle] === 每日采集轮次结束 ===")
            # 健康检查 tick（非阻塞 try-lock，拿不到跳过）
            try:
                self.run_health_check(tasks_cfg)
            except Exception as e:
                logger.debug(f"[DaemonLifecycle] 健康检查跳过或异常: {e}")
            # 再次检测停止请求（健康检查后）
            if consume_stop_request_if_matched(self.instance_token):
                self._graceful_exit()
                break
            # 退出条件
            if self.max_iterations is not None and iteration >= self.max_iterations:
                logger.info(f"[DaemonLifecycle] 达到 max_iterations={self.max_iterations}，退出")
                break
            # 可中断睡眠
            self._interruptible_sleep(check_interval)

        self.clear_own_status()

    def _is_first_run_today(self, today_str: str) -> bool:
        """pending_rerun 时的首 tick 补跑判定（总是 True，靠水位线防重复）。"""
        return True

    def _read_tasks_cfg(self) -> dict:
        path = self.config_dir / "collector_tasks.json"
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"[DaemonLifecycle] 读取 collector_tasks.json 失败: {e}")
            return {}

    # -- 整轮采集（评审 7 + Review FIX-1/2/3） --
    def run_one_cycle(self, tasks_cfg: dict):
        """整轮采集：collector_run.lock 内 from_configs + 遍历 + 审计 + close。

        completed 严格条件（全部满足才写 completed）：
          1. lock 获取成功；
          2. 任务列表完整遍历（traversal_completed=True，未因 stop/异常 break）；
          3. 每个 eligible task 都有明确结果（success/failed，非 skipped-by-stop）；
          4. 质量审计执行（无论成败）；
          5. collector.close() 成功（close_ok=True）。

        任一不满足 → 写 interrupted（原因 stop_requested / cleanup_failed / exception）。

        stop.request 消费点（Review FIX-1）：
          - 每个 task 开始前；
          - 每个 task 完成后；
          - 质量审计开始前；
          - 质量审计完成后。
        """
        today = datetime.now().strftime("%Y-%m-%d")
        run_id = f"r_{uuid.uuid4().hex[:8]}"
        started_at = datetime.now().isoformat(timespec="seconds")

        # 阻塞获取 collector_run.lock（5s 等 GUI 手动释放）
        lock = CollectorRunLock(timeout=5)
        try:
            lock.__enter__()
        except Timeout:
            logger.info("[DaemonLifecycle] collector_run.lock 被占用（GUI 正在采集），本轮跳过")
            return  # 不写 run_state（lock 获取失败不写任何状态，评审 7）
        collector = None
        task_summary = []
        success_count = 0
        failed_count = 0
        quality_audit_ok = False
        # D 件（daemon-interruptible-quality-audit-design.md）：审计中断状态，
        # 供 run_state 区分「审计完整通过 / 审计被中断 / 审计失败」。
        qa_interrupted = False
        qa_skipped = 0
        # Review FIX-2：显式追踪遍历完整性
        eligible_task_count = 0
        attempted_task_count = 0
        traversal_completed = False
        stop_requested = False
        close_ok = False
        # Review FIX-1：任务边界消费 stop.request
        def _check_stop_at_boundary(point_name: str):
            nonlocal stop_requested
            if consume_stop_request_if_matched(self.instance_token):
                self._running = False
                stop_requested = True
                # 同步通知 collector 内部循环（per_stock/per_date 已支持 _running 检查）
                if collector is not None:
                    try:
                        collector._running = False
                    except Exception:
                        pass
                logger.info(f"[DaemonLifecycle] 任务边界({point_name})收到停止请求，"
                            f"中断遍历")
                return True
            return False

        try:
            # 拿到 lock，写 running
            write_run_state(
                scheduled_date=today, run_id=run_id, status="running",
                skip_reason=None, skip_weekday=None, skip_weekday_date=None,
                started_at=started_at, finished_at=None,
                success_count=0, failed_count=0, quality_audit_ok=False,
                interrupted=False, skipped_after_stop=0,
                task_summary=[], eligible_task_count=0, attempted_task_count=0,
                traversal_completed=False, stop_requested=False,
            )
            # 锁内首次 from_configs（_init_tables 安全）
            from quantstudio.pipeline.daemon import ResidentCollector
            collector = ResidentCollector.from_configs(
                self.config_dir / "data_config.json",
                self.config_dir / "sources_config.json",
                self.config_dir / "collector_tasks.json",
                self.config_dir / "alignment_rules.json",
            )
            # TD-D2 统一路由：把 qfq_aux_paths.json 路径交给 collector
            # （released 释放门开关在该文件；缺失 → fail-secure legacy）
            _aux_cfg = self.config_dir / "qfq_aux_paths.json"
            collector.qfq_aux_paths_config = _aux_cfg if _aux_cfg.exists() else None
            # 增量开始前消费 stop（用户可能在 from_configs 期间点了停止）
            _check_stop_at_boundary("pre_cycle")
            # 工作包 D 防线 3：采集轮启动黄金行冒烟自检（不匹配仅告警，不阻断）。
            # getattr 防御：兼容旧版/mock collector（观测缺位不阻断采集轮）。
            try:
                getattr(collector, "qfq_golden_row_smoke", lambda: None)()
            except Exception as e:
                logger.warning(f"[DaemonLifecycle] QFQ 黄金行冒烟异常（不阻断）: {e}")
            # —— QFQ 协调周期开启（resident orchestrator v2）——
            # enabled=false（默认）→ qfq_begin_cycle 返回 None，不写任何 qfq 字段，
            # 后续水位推进走旧路径，本函数行为与接入前逐位一致（紧急回退开关）。
            # enabled=true → 先开周期再跑增量任务，任务内四价格表水位改为延迟提交；
            # 开启失败则 fail-closed：任务照跑，但价格表水位保持不动（绝不提前推进）。
            qfq_cycle_id = None
            if not stop_requested:
                try:
                    qfq_cycle_id = collector.qfq_begin_cycle()
                    if qfq_cycle_id:
                        write_run_state(qfq_phase="cycle_started",
                                        qfq_cycle_id=qfq_cycle_id, qfq_error=None)
                except Exception as e:
                    logger.error(f"[DaemonLifecycle] QFQ 协调周期开启失败"
                                 f"（fail-closed，价格表水位本轮保持）: {e}",
                                 exc_info=True)
                    write_run_state(qfq_phase="begin_failed",
                                    qfq_cycle_id=None,
                                    qfq_error=f"{type(e).__name__}: {e}")
            tasks = tasks_cfg.get("tasks", [])
            for task in tasks:
                # Review FIX-1：每个 task 开始前消费 stop；
                # 也检查 self._running（stop 可能已在 pre_cycle 被消费，文件已删）
                if stop_requested or not self._running:
                    break
                if _check_stop_at_boundary(f"pre_task_{task.get('name','?')}"):
                    break
                if not task.get("enabled", True):
                    continue
                if task.get("mode", "incremental") != "incremental":
                    continue
                eligible_task_count += 1
                task_name = task.get("name", "")
                result_entry = {"name": task_name, "result": "skipped"}
                attempted_task_count += 1
                try:
                    ok = collector.execute_task(task, mode="incremental",
                                                run_quality_audit=False)
                    if ok:
                        result_entry["result"] = "success"
                        success_count += 1
                    else:
                        result_entry["result"] = "failed"
                        result_entry["error"] = "execute_task 返回 False"
                        failed_count += 1
                except Exception as e:
                    result_entry["result"] = "failed"
                    result_entry["error"] = f"{type(e).__name__}: {e}"
                    failed_count += 1
                    logger.error(f"[DaemonLifecycle] 任务 {task_name} 失败: {e}", exc_info=True)
                task_summary.append(result_entry)
                # 每任务后更新（便于崩溃排查）
                write_run_state(success_count=success_count, failed_count=failed_count,
                                task_summary=task_summary,
                                eligible_task_count=eligible_task_count,
                                attempted_task_count=attempted_task_count)
                # Review FIX-1：每个 task 完成后消费 stop
                if _check_stop_at_boundary(f"post_task_{task_name}"):
                    break
            else:
                # for 循环正常结束（未 break）→ 遍历完成
                traversal_completed = True
            # —— QFQ post-ingest 协调阶段（增量任务后、质量审计前）——
            # 完整闭环：recover → discover → claim/merge → reanchor → gate →
            # commit/hold watermarks（四价格表水位在此统一提交或保持）。
            # stop 中断 → 跳过本阶段，本轮 defer 的 intent 保持 pending，
            # 水位不推进（安全），下轮 begin_cycle 自动 supersede 清障。
            if qfq_cycle_id is not None:
                if stop_requested:
                    write_run_state(qfq_phase="interrupted",
                                    qfq_error="stop_requested，post-ingest 未执行，"
                                              "水位保持（下轮清障重算）")
                else:
                    write_run_state(qfq_phase="post_ingest_running")
                    try:
                        qfq_summary = collector.qfq_run_post_ingest(run_id)
                        if qfq_summary is not None:
                            write_run_state(
                                qfq_phase=qfq_summary.status,
                                qfq_error=qfq_summary.error,
                                qfq_summary={
                                    "cycle_id": qfq_summary.cycle_id,
                                    "triggers_found": qfq_summary.triggers_found,
                                    "claimed": qfq_summary.claimed,
                                    "committed": qfq_summary.committed,
                                    "retryable_failed": qfq_summary.retryable_failed,
                                    "dead_letter": qfq_summary.dead_letter,
                                    "watermarks_committed":
                                        qfq_summary.watermarks_committed,
                                    "watermarks_held": qfq_summary.watermarks_held,
                                    "bootstrap_required":
                                        qfq_summary.bootstrap_required,
                                })
                            if qfq_summary.status != "finalized":
                                logger.warning(
                                    f"[DaemonLifecycle] QFQ post-ingest 未完全通过："
                                    f"status={qfq_summary.status} "
                                    f"error={qfq_summary.error} "
                                    f"held={qfq_summary.watermarks_held}"
                                    f"（水位按 hold_until_consistent 保持）")
                    except Exception as e:
                        logger.error(f"[DaemonLifecycle] QFQ post-ingest 异常"
                                     f"（水位保持，下轮重试）: {e}", exc_info=True)
                        write_run_state(qfq_phase="failed",
                                        qfq_error=f"{type(e).__name__}: {e}")
            # Review FIX-1：质量审计开始前消费 stop
            if not stop_requested:
                _check_stop_at_boundary("pre_quality_audit")
            # finally 收尾：质量审计——stop 已消费时按检查点分段中止，避免不可中断
            # 长尾阻塞优雅停；**已执行项结论保留、未执行项如实标记**（设计 §6 收窄照准）。
            # 判据只用已缓存标志：stop.request 文件已在上方被消费删除，严禁重读
            # （设计 §2.4-3 硬不变量）。
            try:
                quality_audit_ok = collector._run_full_quality_audit(
                    should_stop=lambda: stop_requested or not self._running)
            except Exception as e:
                logger.error(f"[DaemonLifecycle] 质量审计失败: {e}", exc_info=True)
                quality_audit_ok = False
            _qa_state = getattr(collector, "_last_quality_audit", None) or {}
            qa_interrupted = bool(_qa_state.get("interrupted", False))
            qa_skipped = int(_qa_state.get("skipped_after_stop", 0))
            # 工作包 D 防线 2.1（补充 A）：因子完整性扫描挂必然执行点——与
            # _run_full_quality_audit 并列（finally 必跑），不挂 qfq_run_post_ingest
            # （编排器 disabled 时 post_ingest 是 no-op，因子监测不应依赖编排器开关）。
            # getattr 防御：兼容旧版/mock collector（观测缺位不阻断审计收尾）。
            try:
                getattr(collector, "_audit_qfq_factor_integrity", lambda: None)()
            except Exception as e:
                logger.warning(f"[DaemonLifecycle] QFQ 因子完整性扫描异常（不阻断）: {e}",
                               exc_info=True)
            # FM 代答两工件导出（2026-09-22，线调度 FM 需求规格 v1；落裁②）：
            # 与 _run_full_quality_audit 并列的**必然执行点**（finally 必跑）。
            # 关键：导出在 daemon 进程内、复用既有 writer.shared_conn() ⇒ **零新增
            # 读连接、零读窗依赖、不触 DuckDB 单写者锁**（这正是落裁②的目的）。
            # 失败必告警（禁静默）：run_fm_export 内部消化异常并写 _status.json + ERROR，
            # 此处再兜一层，**一律不阻断轮次收尾**。
            try:
                from .fm_export import run_fm_export
                _fm = run_fm_export(collector.writer.shared_conn())
                if not _fm.get("ok"):
                    logger.error("[DaemonLifecycle] FM 导出未成功（旧件保留，_status.json 已记）: %s",
                                 _fm.get("error"))
                elif not _fm.get("skipped"):
                    logger.info("[DaemonLifecycle] FM 导出完成 as_of=%s health=%s",
                                _fm.get("as_of"), _fm.get("health"))
            except Exception as e:
                logger.error(f"[DaemonLifecycle] FM 导出异常（不阻断轮次收尾）: {e}",
                             exc_info=True)
            # Review FIX-1：质量审计完成后消费 stop
            _check_stop_at_boundary("post_quality_audit")
        except Exception as e:
            # 可处理异常 → interrupted（评审 7）
            write_run_state(status="interrupted",
                            finished_at=datetime.now().isoformat(timespec="seconds"),
                            error=f"{type(e).__name__}: {e}",
                            success_count=success_count, failed_count=failed_count,
                            quality_audit_ok=quality_audit_ok, task_summary=task_summary,
                            interrupted=qa_interrupted, skipped_after_stop=qa_skipped,
                            eligible_task_count=eligible_task_count,
                            attempted_task_count=attempted_task_count,
                            traversal_completed=False,
                            stop_requested=stop_requested)
            logger.exception(f"[DaemonLifecycle] 轮次异常（标 interrupted）: {e}")
            return
        finally:
            # Review FIX-3：collector.close() 失败不得 completed
            if collector is not None:
                try:
                    collector.close()
                    close_ok = True
                except Exception as e:
                    close_ok = False
                    logger.error(f"[DaemonLifecycle] collector.close() 失败: {e}",
                                 exc_info=True)
                    # Review FIX-3：检查 DuckDB 可打开性，必要时阻止下轮
                    try:
                        import duckdb
                        from quantstudio._paths import db_path
                        test_conn = duckdb.connect(str(db_path()), read_only=True)
                        test_conn.close()
                    except Exception as dbe:
                        logger.error(f"[DaemonLifecycle] DuckDB 不可打开，"
                                     f"可能残留连接: {dbe}")
            else:
                close_ok = True  # 无 collector 无需 close
            try:
                lock.__exit__(None, None, None)
            except Exception:
                pass
        # Review FIX-2/3：严格判定 completed vs interrupted
        can_complete = (traversal_completed and not stop_requested and close_ok)
        if can_complete:
            write_run_state(
                status="completed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                success_count=success_count, failed_count=failed_count,
                quality_audit_ok=quality_audit_ok, task_summary=task_summary,
                interrupted=qa_interrupted, skipped_after_stop=qa_skipped,
                eligible_task_count=eligible_task_count,
                attempted_task_count=attempted_task_count,
                traversal_completed=traversal_completed,
                stop_requested=stop_requested,
            )
            if failed_count > 0:
                logger.warning(f"[DaemonLifecycle] 今日轮次完成但有 {failed_count} 个任务失败，"
                               f"请检查 task_summary 和日志")
            else:
                logger.info(f"[DaemonLifecycle] 今日轮次全部成功 ({success_count} 任务)")
        else:
            # 中断：stop_requested 或 close 失败或异常
            reason = []
            if stop_requested:
                reason.append("stop_requested")
            if not close_ok:
                reason.append("cleanup_failed")
            if not traversal_completed and not stop_requested:
                reason.append("traversal_incomplete")
            interrupt_reason = ",".join(reason) if reason else "unknown"
            interrupt_status = "interrupted" if close_ok else "failed_cleanup"
            write_run_state(
                status=interrupt_status,
                finished_at=datetime.now().isoformat(timespec="seconds"),
                reason=interrupt_reason,
                success_count=success_count, failed_count=failed_count,
                quality_audit_ok=quality_audit_ok, task_summary=task_summary,
                interrupted=qa_interrupted, skipped_after_stop=qa_skipped,
                eligible_task_count=eligible_task_count,
                attempted_task_count=attempted_task_count,
                traversal_completed=traversal_completed,
                stop_requested=stop_requested,
            )
            logger.warning(f"[DaemonLifecycle] 今日轮次未完成（{interrupt_status}, "
                           f"reason={interrupt_reason}），attempted={attempted_task_count}/"
                           f"eligible={eligible_task_count}")

    # -- 健康检查（评审 3：try-lock 非阻塞） --
    def run_health_check(self, tasks_cfg: dict):
        """健康检查：collector_run.lock 非阻塞，拿不到跳过。"""
        lock = CollectorRunLock(timeout=0)
        if not lock.try_acquire():
            logger.debug("[DaemonLifecycle] 健康检查跳过（collector_run.lock 忙）")
            return
        collector = None
        try:
            from quantstudio.pipeline.daemon import ResidentCollector
            collector = ResidentCollector.from_configs(
                self.config_dir / "data_config.json",
                self.config_dir / "sources_config.json",
                self.config_dir / "collector_tasks.json",
                self.config_dir / "alignment_rules.json",
            )
            # TD-D2 统一路由：qfq_aux_paths.json 路径交给 collector（缺省 fail-secure）
            _aux_cfg2 = self.config_dir / "qfq_aux_paths.json"
            collector.qfq_aux_paths_config = _aux_cfg2 if _aux_cfg2.exists() else None
            collector._health_check()
        except Exception as e:
            logger.debug(f"[DaemonLifecycle] 健康检查异常: {e}")
        finally:
            if collector is not None:
                try:
                    collector.close()
                except Exception:
                    pass
            try:
                lock.__exit__(None, None, None)
            except Exception:
                pass

    # -- 可中断睡眠 --
    def _interruptible_sleep(self, seconds: int):
        """可中断睡眠：每秒检测停止请求。"""
        elapsed = 0
        while elapsed < seconds and self._running:
            if consume_stop_request_if_matched(self.instance_token):
                self._running = False
                return
            time.sleep(1)
            elapsed += 1

    # -- 优雅退出 --
    def _graceful_exit(self):
        """停止请求触发：标记状态，等当前安全点结束。"""
        self._running = False
        try:
            self.update_status(status="stopping")
        except Exception:
            pass
        logger.info("[DaemonLifecycle] 优雅退出流程启动，等待当前安全步骤结束")
        # 当前如果在 run_one_cycle 内，循环会在下个任务边界 break；
        # _interruptible_sleep 也会在 1s 内响应。
