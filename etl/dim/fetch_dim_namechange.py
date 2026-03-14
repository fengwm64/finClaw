"""etl/dim/fetch_dim_namechange.py — 按股票拉取曾用名记录，upsert 到 dim_namechange。

拉取策略：
  - 逐支股票调用 namechange(ts_code=xxx)，汇总后 upsert
  - 单次全量拉取上限 10000 条且含重复行，按股票拉取可完整覆盖所有记录
"""

import sys
import logging
import time
import threading
import psycopg2
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import load_envs, get_pg_dsn, make_pro, to_date, clean_df, upsert, with_retry

load_envs()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PG_DSN       = get_pg_dsn()
MAX_WORKERS  = 3
SUBMIT_CHUNK = 500
FLUSH_ROWS   = 20_000
RATE_LIMIT_PER_MIN = 200
RATE_WINDOW_SECONDS = 60.0

FIELDS = "ts_code,name,start_date,end_date,ann_date,change_reason"
COLS   = ["ts_code", "name", "start_date", "end_date", "ann_date", "change_reason"]

UPSERT_SQL = """
INSERT INTO dim_namechange (ts_code, name, start_date, end_date, ann_date, change_reason)
VALUES %s
ON CONFLICT (ts_code, start_date) DO UPDATE SET
    name          = EXCLUDED.name,
    end_date      = EXCLUDED.end_date,
    ann_date      = EXCLUDED.ann_date,
    change_reason = EXCLUDED.change_reason;
"""


_api_call_times = deque()
_api_call_lock = threading.Lock()


def throttle_namechange_calls() -> None:
    """全局限流：所有线程合计每分钟最多调用 namechange 200 次。"""
    while True:
        now = time.monotonic()
        with _api_call_lock:
            while _api_call_times and now - _api_call_times[0] >= RATE_WINDOW_SECONDS:
                _api_call_times.popleft()

            if len(_api_call_times) < RATE_LIMIT_PER_MIN:
                _api_call_times.append(now)
                return

            wait_seconds = RATE_WINDOW_SECONDS - (now - _api_call_times[0])

        time.sleep(max(wait_seconds, 0.01))


def get_all_stocks() -> list[str]:
    with psycopg2.connect(**PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT ts_code FROM dim_stock ORDER BY ts_code")
        return [r[0] for r in cur.fetchall()]


def fetch_one_stock(ts_code: str) -> list[tuple]:
    pro = make_pro()

    def _call():
        throttle_namechange_calls()
        df = pro.namechange(ts_code=ts_code, fields=FIELDS)
        if df is None or df.empty:
            return []
        for col in ("start_date", "end_date", "ann_date"):
            df[col] = df[col].apply(to_date)
        df = clean_df(df)
        # 去重：同一股票可能有重复的 (ts_code, start_date)，保留最后一条
        df = df.drop_duplicates(subset=["ts_code", "start_date"], keep="last")
        return list(df[COLS].itertuples(index=False, name=None))

    return with_retry(_call, label=ts_code, log=log)


def run() -> None:
    all_stocks = get_all_stocks()
    total = len(all_stocks)
    log.info(f"股票总数：{total}，开始按股票拉取曾用名...")

    done       = 0
    batch_rows: list[tuple] = []

    for chunk_start in range(0, total, SUBMIT_CHUNK):
        chunk = all_stocks[chunk_start: chunk_start + SUBMIT_CHUNK]

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_one_stock, code): code for code in chunk}
            for future in as_completed(futures):
                code = futures[future]
                rows = future.result()
                batch_rows.extend(rows)
                done += 1
                log.info(f"[{done}/{total}] {code}：{len(rows)} 条")

                if len(batch_rows) >= FLUSH_ROWS:
                    n = upsert(PG_DSN, UPSERT_SQL, batch_rows)
                    log.info(f"批次写库：{n} 条")
                    batch_rows = []

    if batch_rows:
        n = upsert(PG_DSN, UPSERT_SQL, batch_rows)
        log.info(f"最终写库：{n} 条")

    log.info("全部完成")


if __name__ == "__main__":
    run()
