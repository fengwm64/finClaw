"""
etl/fact/fetch_fact_adj_factor.py — 按股票拉取复权因子，upsert 到 fact_adj_factor。

拉取策略：
  - 从 dim_stock 获取全量股票列表
  - 查询每支股票在 DB 中已有的最新日期，增量拉取新增部分
  - 新股（DB 无记录）拉取全量历史
  - 分批提交 futures，避免一次性创建 6000 个 future
"""

import sys
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

PG_DSN       = get_pg_dsn()
MAX_WORKERS  = 3       # 并发线程数
SUBMIT_CHUNK = 300     # 每批提交的股票数，防止 futures 队列暴涨
FLUSH_ROWS   = 10_000  # 累积超过此行数就写库

UPSERT_SQL = """
INSERT INTO fact_adj_factor (ts_code, trade_date, adj_factor)
VALUES %s
ON CONFLICT (trade_date, ts_code) DO UPDATE SET
    adj_factor = EXCLUDED.adj_factor;
"""


def get_stocks_with_latest(force: bool) -> tuple[list[str], dict[str, str]]:
    """一次查询同时返回全量股票列表和各股票最新日期。"""
    with psycopg2.connect(**PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT ts_code FROM dim_stock ORDER BY ts_code")
        all_stocks = [r[0] for r in cur.fetchall()]

        if force:
            return all_stocks, {}

        cur.execute("""
            SELECT ts_code, to_char(MAX(trade_date) + INTERVAL '1 day', 'YYYYMMDD')
            FROM fact_adj_factor
            GROUP BY ts_code
        """)
        latest_dates = dict(cur.fetchall())

    return all_stocks, latest_dates


def fetch_one_stock(ts_code: str, start_date: str | None) -> list[tuple]:
    """拉取单支股票复权因子，start_date=None 时拉全量。"""
    pro    = make_pro()
    kwargs = dict(ts_code=ts_code, fields="ts_code,trade_date,adj_factor")
    if start_date:
        kwargs["start_date"] = start_date

    def _call():
        df = pro.adj_factor(**kwargs)
        if df is None or df.empty:
            return []
        df["trade_date"] = df["trade_date"].apply(to_date)
        df = clean_df(df)
        return list(df[["ts_code", "trade_date", "adj_factor"]].itertuples(index=False, name=None))

    return with_retry(_call, label=ts_code, log=log)


def run(force: bool = False) -> None:
    all_stocks, latest_dates = get_stocks_with_latest(force)
    total = len(all_stocks)
    log.info(f"股票总数：{total}，DB 中已有记录：{len(latest_dates)} 支")

    done       = 0
    batch_rows: list[tuple] = []

    # 分批提交，避免 6000 个 future 同时入队
    for chunk_start in range(0, total, SUBMIT_CHUNK):
        chunk = all_stocks[chunk_start: chunk_start + SUBMIT_CHUNK]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_one_stock, code, latest_dates.get(code)): code
                for code in chunk
            }
            for future in as_completed(futures):
                code = futures[future]
                rows = future.result()
                batch_rows.extend(rows)
                done += 1

                mode = "增量" if latest_dates.get(code) else "全量"
                log.info(f"[{done}/{total}] {code} ({mode})：{len(rows)} 条")

                if len(batch_rows) >= FLUSH_ROWS:
                    n = upsert(PG_DSN, UPSERT_SQL, batch_rows)
                    log.info(f"批次写库：{n} 条")
                    batch_rows = []

    if batch_rows:
        n = upsert(PG_DSN, UPSERT_SQL, batch_rows)
        log.info(f"最终写库：{n} 条")

    log.info("全部完成")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="忽略已有记录，强制全量重跑")
    args = parser.parse_args()
    run(force=args.force)
