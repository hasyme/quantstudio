"""口径金标准复核：用 amount/volume 反推真实成交价（口径无关）。

原理：
  amount（成交额，元）/ volume（成交量，股）= 真实成交价（raw，元/股）
  该值**不依赖任何复权处理**，是判定云端口径的独立基准。

判定：
  · 若 cloud_close ≈ amount/volume            → 云端 = RAW
  · 若 cloud_close × (adj_latest/adj_i) ≈ amount/volume → 云端 = QFQ

本脚本对四表抽样验证，输出判定与置信度。
"""
from __future__ import annotations
import io
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from quantstudio.pipeline.mcp.client import MCPClient

CST = timezone(timedelta(hours=8))
OUT = ROOT / "agent_workspace" / "_caliber_gold.txt"

TABLES = [
    ("stock_daily", "stock_daily", "adj_factor", "2018-06-15", "2018-06-16"),
    ("etf_daily", "etf_daily", "fund_adj", "2020-06-15", "2020-06-16"),
    ("stock_minutes", "stock_minutes", "adj_factor", "2020-06-15", "2020-06-16"),
    ("etf_minutes", "etf_minutes", "fund_adj", "2020-06-15", "2020-06-16"),
]


def main() -> int:
    out = []
    client = MCPClient(endpoint="https://124.223.159.234/mcp", tls_verify=False)
    client.handshake()
    aux = sqlite3.connect(str(ROOT / "data" / "qfq_aux.db"))
    aux.execute("PRAGMA query_only=ON")

    out.append("=== 口径金标准复核（amount/volume 反推真实成交价）===")
    out.append("")

    for table, ds, ftbl, d0, d1 in TABLES:
        out.append(f"--- {table} ({d0}) ---")
        try:
            arts = client.export_dataset(ds, page_size=5_000_000, row_limit=5_000_000,
                                         time_start=d0, time_end=d1, async_mode=False)
        except Exception as e:
            out.append(f"  取数失败: {type(e).__name__}: {str(e)[:80]}")
            out.append("")
            continue
        if not arts:
            out.append("  空返回")
            out.append("")
            continue
        df = pd.concat([pd.read_parquet(io.BytesIO(a.parquet_bytes))
                        for a in arts], ignore_index=True)
        # 统一列名
        ccol = "close" if "close" in df.columns else None
        vcol = "vol" if "vol" in df.columns else ("volume" if "volume" in df.columns else None)
        acol = "amount" if "amount" in df.columns else None
        codecol = "ts_code" if "ts_code" in df.columns else "code"
        if not (ccol and vcol and acol):
            out.append(f"  缺列（close={bool(ccol)}, vol={bool(vcol)}, amount={bool(acol)}）")
            out.append(f"  实际列: {list(df.columns)[:14]}")
            out.append("")
            continue
        df["bare"] = df[codecol].astype(str).str.split(".").str[0]
        df["c"] = pd.to_numeric(df[ccol], errors="coerce")
        df["v"] = pd.to_numeric(df[vcol], errors="coerce")
        df["a"] = pd.to_numeric(df[acol], errors="coerce")
        df = df[(df["v"] > 0) & (df["a"] > 0) & (df["c"] > 0)].copy()
        df["implied"] = df["a"] / df["v"] * 10.0
        # 单位说明（实测 2026-09-30）：
        #   云端 amount 单位 = **千元**、vol 单位 = **手**；
        #   本地 canonical（经 aligner unit_convert）= 元 / 股。
        #   故云端真实成交价 = (amount×1000)/(vol×100) = amount/vol × 10。
        #   （该常数不影响 RAW/QFQ 判定，但使误差量级正确、判定力显著提升。）
        # 取该 code 的**真实末行**（注意：不可用 groupby.last()——
        # 那是「按列取最后非空值」，会混合不同行的字段）
        if "trade_time" in df.columns:
            df["_t"] = df["trade_time"].astype(str)
        elif "trade_date" in df.columns:
            df["_t"] = df["trade_date"].astype(str)
        else:
            df["_t"] = pd.to_datetime(df["time"] if "time" in df.columns else df.index,
                                      unit="ms", utc=True, errors="coerce").astype(str)
        df = df.sort_values(["bare", "_t"]).groupby("bare", as_index=False).tail(1)
        df["ratio_raw"] = df["c"] / df["implied"]
        # 因子
        def _af(code):
            r = aux.execute(f"SELECT adj_factor FROM {ftbl} WHERE code=? ORDER BY time DESC LIMIT 1",
                            (code,)).fetchone()
            return r[0] if r else None
        latest = {c: _af(c) for c in df["bare"].unique()}
        # 当日因子
        t0ms = int(datetime.strptime(d0, "%Y-%m-%d").replace(tzinfo=CST).timestamp() * 1000)
        def _ai(code):
            r = aux.execute(f"SELECT adj_factor FROM {ftbl} WHERE code=? AND time<=? "
                            f"ORDER BY time DESC LIMIT 1", (code, t0ms)).fetchone()
            return r[0] if r else None
        cur = {c: _ai(c) for c in df["bare"].unique()}
        df["al"] = df["bare"].map(latest)
        df["ai"] = df["bare"].map(cur)
        df = df.dropna(subset=["al", "ai"])
        df = df[df["ai"] > 0]
        if df.empty:
            out.append("  无有效因子配对")
            out.append("")
            continue
        df["qfq_pred"] = df["c"] * df["al"] / df["ai"]
        df["err_raw"] = (df["c"] - df["implied"]).abs() / df["implied"]
        df["err_qfq"] = (df["qfq_pred"] - df["implied"]).abs() / df["implied"]
        n = len(df)
        mr = df["err_raw"].median()
        mq = df["err_qfq"].median()
        verdict = "RAW" if mr < mq else "QFQ"
        out.append(f"  样本数: {n}")
        out.append(f"  err_raw 中位: {mr:.6f}")
        out.append(f"  err_qfq 中位: {mq:.6f}")
        out.append(f"  **判定: {verdict}**  (raw/qfq 误差比 = {mq/mr if mr>0 else float('inf'):.2f})")
        # 只在因子差异显著时才有判定力
        big = df[(df["al"] / df["ai"] - 1).abs() > 0.05]
        if len(big) >= 5:
            br = big["err_raw"].median()
            bq = big["err_qfq"].median()
            out.append(f"  [大幅度子集 |al/ai-1|>5%] n={len(big)}  "
                       f"err_raw={br:.6f} err_qfq={bq:.6f}  -> "
                       f"{'RAW' if br < bq else 'QFQ'}（判定力强）")
        out.append("")
    aux.close()
    OUT.write_text("\n".join(out), encoding="utf-8")
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
