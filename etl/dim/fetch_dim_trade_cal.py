"""etl/dim/fetch_dim_trade_cal.py — 拉取交易日历，upsert 到 dim_trade_cal。"""

import sys
import logging
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import load_envs, get_pg_dsn, make_pro, to_date, clean_df, upsert

load_envs()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PG_DSN     = get_pg_dsn()
EXCHANGES  = ["SSE", "SZSE"]
START_DATE = "19900101"

UPSERT_SQL = """
INSERT INTO dim_trade_cal (
    exchange, cal_date, is_open, pretrade_date
) VALUES %s
ON CONFLICT (exchange, cal_date) DO UPDATE SET
    is_open       = EXCLUDED.is_open,
    pretrade_date = EXCLUDED.pretrade_date;
"""


def fetch_from_tushare() -> list[tuple]:
    pro      = make_pro()
    end_date = f"{date.today().year}1231"
    rows: list[tuple] = []

    for exchange in EXCHANGES:
        log.info(f"正在拉取 {exchange} 交易日历 {START_DATE} ~ {end_date} ...")
        df = pro.trade_cal(
            exchange=exchange,
            start_date=START_DATE,
            end_date=end_date,
            fields="exchange,cal_date,is_open,pretrade_date",
        )
        df["cal_date"]      = df["cal_date"].apply(to_date)
        df["pretrade_date"] = df["pretrade_date"].apply(to_date)
        df["is_open"]       = df["is_open"].astype(bool)
        df = clean_df(df)

        rows.extend(df[["exchange", "cal_date", "is_open", "pretrade_date"]].itertuples(index=False, name=None))
        log.info(f"  {exchange}：{len(df)} 条")

    log.info(f"共获取 {len(rows)} 条记录")
    return rows


if __name__ == "__main__":
    rows = fetch_from_tushare()
    n = upsert(PG_DSN, UPSERT_SQL, rows)
    log.info(f"upsert 完成，共写入 {n} 条")
