"""
etl/fact/fetch_fact_index_weight.py — 按指数拉取月度成分权重，upsert 到 fact_index_weight。

拉取策略：
  - 从 dim_index 获取全量指数列表
  - 查询每个指数在 DB 中已有的最新日期，增量拉取新增月份
  - 新指数（DB 无记录）拉取全量历史
  - 分批提交 futures，控制并发
"""

import sys
import logging
import argparse
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import load_envs, get_pg_dsn, make_pro, to_date, clean_df, upsert, with_retry

load_envs()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PG_DSN       = get_pg_dsn()
MAX_WORKERS  = 3
SUBMIT_CHUNK = 100
FLUSH_ROWS   = 50_000

COLS = ["index_code", "con_code", "trade_date", "weight"]

UPSERT_SQL = """
INSERT INTO fact_index_weight (index_code, con_code, trade_date, weight)
VALUES %s
ON CONFLICT (trade_date, index_code, con_code) DO UPDATE SET
    weight = EXCLUDED.weight;
"""


def get_indexes_with_latest(force: bool) -> tuple[list[str], dict[str, str]]:
    with psycopg2.connect(**PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT ts_code FROM dim_index ORDER BY ts_code")
        all_indexes = [r[0] for r in cur.fetchall()]

        if force:
            return all_indexes, {}

        cur.execute("""
            SELECT index_code, to_char(MAX(trade_date) + INTERVAL '1 day', 'YYYYMMDD')
            FROM fact_index_weight
            GROUP BY index_code
        """)
        latest_dates = dict(cur.fetchall())

    return all_indexes, latest_dates


def fetch_one_index(index_code: str, start_date: str | None) -> list[tuple]:
    pro = make_pro()
    kwargs = dict(index_code=index_code)
    if start_date:
        kwargs["start_date"] = start_date

    def _call():
        df = pro.index_weight(**kwargs)
        if df is None or df.empty:
            return []
        df["trade_date"] = df["trade_date"].apply(to_date)
        df = clean_df(df)
        return list(df[COLS].itertuples(index=False, name=None))

    return with_retry(_call, label=index_code, log=log)


def run(force: bool = False) -> None:
    all_indexes, latest_dates = get_indexes_with_latest(force)
    total = len(all_indexes)
    log.info(f"指数总数：{total}，DB 中已有记录：{len(latest_dates)} 个")

    done       = 0
    batch_rows: list[tuple] = []

    for chunk_start in range(0, total, SUBMIT_CHUNK):
        chunk = all_indexes[chunk_start: chunk_start + SUBMIT_CHUNK]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(fetch_one_index, code, latest_dates.get(code)): code
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
