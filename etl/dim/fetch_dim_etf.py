"""etl/dim/fetch_dim_etf.py — 拉取 ETF 基础信息，upsert 到 dim_etf。"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import load_envs, get_pg_dsn, make_pro, to_date, clean_df, upsert

load_envs()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PG_DSN = get_pg_dsn()

FIELDS = "ts_code,csname,extname,cname,index_code,index_name,setup_date,list_date,list_status,exchange,mgr_name,custod_name,mgt_fee,etf_type"
COLS   = ["ts_code", "csname", "extname", "cname", "index_code", "index_name",
          "setup_date", "list_date", "list_status", "exchange",
          "mgr_name", "custod_name", "mgt_fee", "etf_type"]

UPSERT_SQL = """
INSERT INTO dim_etf (ts_code, csname, extname, cname, index_code, index_name,
                     setup_date, list_date, list_status, exchange,
                     mgr_name, custod_name, mgt_fee, etf_type)
VALUES %s
ON CONFLICT (ts_code) DO UPDATE SET
    csname       = EXCLUDED.csname,
    extname      = EXCLUDED.extname,
    cname        = EXCLUDED.cname,
    index_code   = EXCLUDED.index_code,
    index_name   = EXCLUDED.index_name,
    setup_date   = EXCLUDED.setup_date,
    list_date    = EXCLUDED.list_date,
    list_status  = EXCLUDED.list_status,
    exchange     = EXCLUDED.exchange,
    mgr_name     = EXCLUDED.mgr_name,
    custod_name  = EXCLUDED.custod_name,
    mgt_fee      = EXCLUDED.mgt_fee,
    etf_type     = EXCLUDED.etf_type;
"""


def fetch_from_tushare() -> list[tuple]:
    pro = make_pro()
    rows = []

    for status in ("L", "D", "P"):
        log.info(f"正在拉取 ETF 基础信息（list_status={status}）...")
        df = pro.etf_basic(list_status=status, fields=FIELDS)
        log.info(f"  获取 {len(df)} 条")
        for col in ("setup_date", "list_date"):
            df[col] = df[col].apply(to_date)
        df = clean_df(df)
        rows.extend(df[COLS].itertuples(index=False, name=None))

    return rows


if __name__ == "__main__":
    rows = fetch_from_tushare()
    n = upsert(PG_DSN, UPSERT_SQL, rows)
    log.info(f"upsert 完成，共写入 {n} 条")
