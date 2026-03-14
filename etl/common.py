"""etl/common.py — ETL 公共工具，供所有拉取脚本复用。"""

import os
import time
import logging
from pathlib import Path

import tushare as ts
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

_ROOT = Path(__file__).parent.parent

# ── 环境 & 配置 ───────────────────────────────────────────────────────────────

def load_envs() -> None:
    """加载 .env.postgres 和 .env.tushare 到环境变量。"""
    load_dotenv(_ROOT / ".env.postgres")
    load_dotenv(_ROOT / ".env.tushare")


def get_pg_dsn() -> dict:
    return {
        "host":     os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "port":     int(os.getenv("POSTGRES_PORT", 5432)),
        "dbname":   os.environ["POSTGRES_DB"],
        "user":     os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


def make_pro() -> object:
    """创建并返回已配置自定义 endpoint 的 Tushare pro 实例。"""
    token = os.environ["TUSHARE_TOKEN"]
    http_url = os.getenv("TUSHARE_HTTP_URL", "").strip()
    pro = ts.pro_api(token)
    if http_url:
        pro._DataApi__token = token
        pro._DataApi__http_url = http_url
    return pro


# ── 数据处理工具 ──────────────────────────────────────────────────────────────

def to_date(val) -> str | None:
    """YYYYMMDD 字符串 → YYYY-MM-DD；无效值 → None。"""
    if isinstance(val, str) and len(val) == 8:
        return f"{val[:4]}-{val[4:6]}-{val[6:8]}"
    return None


def clean_df(df):
    """将 DataFrame 中的 NaN/NaT 替换为 Python None，兼容 psycopg2。

    df.where(..., other=None) 对 float64 列无法可靠地将 NaN 转为 None
    （pandas 会把 None 强制转回 NaN 以保持 dtype），因此逐列处理。
    """
    import numpy as np
    result = df.copy()
    for col in result.columns:
        if result[col].dtype == object:
            # object 列：None 已正确，无需处理
            continue
        # 数值/日期列：用 numpy where 强制转换为 object 再替换
        result[col] = result[col].astype(object).where(result[col].notna(), other=None)
    return result


# ── 数据库 ────────────────────────────────────────────────────────────────────

def upsert(pg_dsn: dict, sql: str, rows: list[tuple]) -> int:
    """执行 upsert，返回写入行数。"""
    with psycopg2.connect(**pg_dsn) as conn, conn.cursor() as cur:
        execute_values(cur, sql, rows)
        conn.commit()
    return len(rows)


# ── 重试 ──────────────────────────────────────────────────────────────────────

def with_retry(fn, label: str, log: logging.Logger,
               retry_limit: int = 3, retry_delay: int = 5):
    """
    调用 fn()，失败时自动重试最多 retry_limit 次。
    超过限制返回空列表。
    """
    for attempt in range(1, retry_limit + 1):
        try:
            return fn()
        except Exception as e:
            log.warning(f"[{label}] 第 {attempt} 次失败：{e}")
            if attempt < retry_limit:
                time.sleep(retry_delay)
    log.error(f"[{label}] 超过重试次数，跳过")
    return []
