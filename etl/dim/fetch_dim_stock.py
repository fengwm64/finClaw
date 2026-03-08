"""etl/dim/fetch_dim_stock.py — 拉取股票基本信息，upsert 到 dim_stock。"""

import sys
import logging
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import load_envs, get_pg_dsn, make_pro, to_date, clean_df, upsert

load_envs()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PG_DSN = get_pg_dsn()

COLUMNS = [
    "ts_code", "symbol", "name", "fullname", "enname", "cnspell",
    "area", "industry", "market", "exchange", "curr_type",
    "list_status", "list_date", "delist_date", "is_hs",
    "act_name", "act_ent_type",
]

UPSERT_SQL = """
INSERT INTO dim_stock (
    ts_code, symbol, name, fullname, enname, cnspell,
    area, industry, market, exchange, curr_type,
    list_status, list_date, delist_date, is_hs,
    act_name, act_ent_type
) VALUES %s
ON CONFLICT (ts_code) DO UPDATE SET
    symbol       = EXCLUDED.symbol,
    name         = EXCLUDED.name,
    fullname     = EXCLUDED.fullname,
    enname       = EXCLUDED.enname,
    cnspell      = EXCLUDED.cnspell,
    area         = EXCLUDED.area,
    industry     = EXCLUDED.industry,
    market       = EXCLUDED.market,
    exchange     = EXCLUDED.exchange,
    curr_type    = EXCLUDED.curr_type,
    list_status  = EXCLUDED.list_status,
    list_date    = EXCLUDED.list_date,
    delist_date  = EXCLUDED.delist_date,
    is_hs        = EXCLUDED.is_hs,
    act_name     = EXCLUDED.act_name,
    act_ent_type = EXCLUDED.act_ent_type,
    updated_at   = NOW();
"""


def fetch_from_tushare() -> list[tuple]:
    pro = make_pro()
    log.info("正在从 Tushare 拉取 stock_basic（全量）...")

    df = pd.concat(
        [pro.stock_basic(fields=",".join(COLUMNS), list_status=s) for s in ("L", "D", "P")],
        ignore_index=True,
    )
    log.info(f"共获取 {len(df)} 条记录")

    for col in ("list_date", "delist_date"):
        df[col] = df[col].apply(to_date)

    df = clean_df(df)
    return list(df[COLUMNS].itertuples(index=False, name=None))


if __name__ == "__main__":
    rows = fetch_from_tushare()
    n = upsert(PG_DSN, UPSERT_SQL, rows)
    log.info(f"upsert 完成，共写入 {n} 条")
