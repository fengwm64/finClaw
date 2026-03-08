"""
etl/fact/fetch_fact_daily_basic.py — 按交易日期拉取每日指标，upsert 到 fact_daily_basic。

拉取策略与 fetch_fact_daily 一致：
  - 每次 API 调用传入 trade_date，返回当日全市场数据
  - ThreadPoolExecutor 并发拉取，每批 BATCH_SIZE 个日期写库
  - 增量模式自动跳过已存在日期；--force 强制全量重跑
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

PG_DSN      = get_pg_dsn()
MAX_WORKERS = 3
BATCH_SIZE  = 15

FIELDS = (
    "ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,"
    "pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,"
    "total_share,float_share,free_share,total_mv,circ_mv"
)

COLS = [
    "ts_code", "trade_date", "close",
    "turnover_rate", "turnover_rate_f", "volume_ratio",
    "pe", "pe_ttm", "pb", "ps", "ps_ttm",
    "dv_ratio", "dv_ttm",
    "total_share", "float_share", "free_share",
    "total_mv", "circ_mv",
]

UPSERT_SQL = """
INSERT INTO fact_daily_basic (
    ts_code, trade_date, close,
    turnover_rate, turnover_rate_f, volume_ratio,
    pe, pe_ttm, pb, ps, ps_ttm,
    dv_ratio, dv_ttm,
    total_share, float_share, free_share,
    total_mv, circ_mv
) VALUES %s
ON CONFLICT (trade_date, ts_code) DO UPDATE SET
    close            = EXCLUDED.close,
    turnover_rate    = EXCLUDED.turnover_rate,
    turnover_rate_f  = EXCLUDED.turnover_rate_f,
    volume_ratio     = EXCLUDED.volume_ratio,
    pe               = EXCLUDED.pe,
    pe_ttm           = EXCLUDED.pe_ttm,
    pb               = EXCLUDED.pb,
    ps               = EXCLUDED.ps,
    ps_ttm           = EXCLUDED.ps_ttm,
    dv_ratio         = EXCLUDED.dv_ratio,
    dv_ttm           = EXCLUDED.dv_ttm,
    total_share      = EXCLUDED.total_share,
    float_share      = EXCLUDED.float_share,
    free_share       = EXCLUDED.free_share,
    total_mv         = EXCLUDED.total_mv,
    circ_mv          = EXCLUDED.circ_mv;
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

        cur.execute("SELECT DISTINCT to_char(trade_date, 'YYYYMMDD') FROM fact_daily_basic")
        loaded = {r[0] for r in cur.fetchall()}

    return [d for d in all_dates if d not in loaded], len(loaded)


def fetch_one_date(trade_date: str) -> list[tuple]:
    pro = make_pro()

    def _call():
        df = pro.daily_basic(trade_date=trade_date, fields=FIELDS)
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
