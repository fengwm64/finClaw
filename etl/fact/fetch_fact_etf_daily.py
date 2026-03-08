"""
etl/fact/fetch_fact_etf_daily.py — 按交易日期拉取 ETF 日行情，upsert 到 fact_etf_daily。

拉取策略：
  - 每次 API 调用传入 trade_date，返回当日全市场 ETF 数据
  - ThreadPoolExecutor 并发拉取，每批 BATCH_SIZE 个日期完成后统一写库
  - 增量模式自动跳过已存在日期；--force 强制全量重跑
"""

import sys
import time
import logging
import argparse
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import load_envs, get_pg_dsn, make_pro, to_date, clean_df, upsert, with_retry

load_envs()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PG_DSN      = get_pg_dsn()
MAX_WORKERS = 1      # fund_daily 限速 500次/分，单线程避免触发
BATCH_SIZE  = 15
REQ_INTERVAL = 0.15  # 请求间隔（秒），约 6.7次/秒 = 400次/分

FIELDS = "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
COLS   = ["ts_code", "trade_date", "open", "high", "low",
          "close", "pre_close", "change", "pct_chg", "vol", "amount"]

UPSERT_SQL = """
INSERT INTO fact_etf_daily (
    ts_code, trade_date, open, high, low, close,
    pre_close, change, pct_chg, vol, amount
) VALUES %s
ON CONFLICT (trade_date, ts_code) DO UPDATE SET
    open      = EXCLUDED.open,
    high      = EXCLUDED.high,
    low       = EXCLUDED.low,
    close     = EXCLUDED.close,
    pre_close = EXCLUDED.pre_close,
    change    = EXCLUDED.change,
    pct_chg   = EXCLUDED.pct_chg,
    vol       = EXCLUDED.vol,
    amount    = EXCLUDED.amount;
"""


def get_pending_dates(force: bool) -> tuple[list[str], int]:
    with psycopg2.connect(**PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT to_char(cal_date, 'YYYYMMDD')
            FROM dim_trade_cal
            WHERE exchange = 'SSE' AND is_open = TRUE AND cal_date <= CURRENT_DATE
            ORDER BY cal_date
        """)
        all_dates = [r[0] for r in cur.fetchall()]

        if force:
            return all_dates, 0

        cur.execute("SELECT DISTINCT to_char(trade_date, 'YYYYMMDD') FROM fact_etf_daily")
        loaded = {r[0] for r in cur.fetchall()}

    return [d for d in all_dates if d not in loaded], len(loaded)


def fetch_one_date(trade_date: str) -> list[tuple]:
    pro = make_pro()

    def _call():
        df = pro.fund_daily(trade_date=trade_date, fields=FIELDS)
        time.sleep(REQ_INTERVAL)
        if df is None or df.empty:
            return []
        df["trade_date"] = df["trade_date"].apply(to_date)
        df = clean_df(df)
        return list(df[COLS].itertuples(index=False, name=None))

    return with_retry(_call, label=trade_date, log=log)


def run(force: bool = False) -> None:
    pending, loaded_count = get_pending_dates(force)
    total = len(pending)
    log.info(f"待拉取：{total}，已加载：{loaded_count}")
    if not total:
        log.info("无需更新")
        return

    done = 0
    for batch_start in range(0, total, BATCH_SIZE):
        batch      = pending[batch_start: batch_start + BATCH_SIZE]
        batch_rows: list[tuple] = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_one_date, d): d for d in batch}
            for future in as_completed(futures):
                d    = futures[future]
                rows = future.result()
                batch_rows.extend(rows)
                done += 1
                log.info(f"[{done}/{total}] {d}：{len(rows)} 条")

        if batch_rows:
            n = upsert(PG_DSN, UPSERT_SQL, batch_rows)
            log.info(f"批次写库完成，本批 {n} 条")

    log.info("全部完成")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="忽略已加载日期，强制全量重跑")
    args = parser.parse_args()
    run(force=args.force)
