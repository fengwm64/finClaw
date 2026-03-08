"""etl/dim/fetch_dim_index.py — 拉取指数基本信息，upsert 到 dim_index。"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import load_envs, get_pg_dsn, make_pro, to_date, clean_df, upsert

load_envs()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PG_DSN = get_pg_dsn()

FIELDS = "ts_code,indx_name,indx_csname,pub_party_name,pub_date,base_date,bp,adj_circle"
COLS   = ["ts_code", "indx_name", "indx_csname", "pub_party_name", "pub_date", "base_date", "bp", "adj_circle"]

UPSERT_SQL = """
INSERT INTO dim_index (ts_code, indx_name, indx_csname, pub_party_name, pub_date, base_date, bp, adj_circle)
VALUES %s
ON CONFLICT (ts_code) DO UPDATE SET
    indx_name      = EXCLUDED.indx_name,
    indx_csname    = EXCLUDED.indx_csname,
    pub_party_name = EXCLUDED.pub_party_name,
    pub_date       = EXCLUDED.pub_date,
    base_date      = EXCLUDED.base_date,
    bp             = EXCLUDED.bp,
    adj_circle     = EXCLUDED.adj_circle;
"""


def fetch_from_tushare() -> list[tuple]:
    pro = make_pro()
    log.info("正在拉取指数基本信息...")

    df = pro.etf_index(fields=FIELDS)
    log.info(f"共获取 {len(df)} 条记录")

    for col in ("pub_date", "base_date"):
        df[col] = df[col].apply(to_date)

    df = clean_df(df)
    return list(df[COLS].itertuples(index=False, name=None))


if __name__ == "__main__":
    rows = fetch_from_tushare()
    n = upsert(PG_DSN, UPSERT_SQL, rows)
    log.info(f"upsert 完成，共写入 {n} 条")
