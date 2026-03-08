"""etl/dim/fetch_dim_company.py — 拉取上市公司基本信息，upsert 到 dim_company。"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from common import load_envs, get_pg_dsn, make_pro, to_date, clean_df, upsert

load_envs()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

PG_DSN = get_pg_dsn()

FIELDS = (
    "ts_code,com_name,com_id,exchange,chairman,manager,secretary,"
    "reg_capital,setup_date,province,city,introduction,"
    "website,email,office,employees,main_business,business_scope"
)

COLS = [
    "ts_code", "com_name", "com_id", "exchange",
    "chairman", "manager", "secretary", "reg_capital",
    "setup_date", "province", "city", "introduction",
    "website", "email", "office", "employees",
    "main_business", "business_scope",
]

UPSERT_SQL = """
INSERT INTO dim_company (
    ts_code, com_name, com_id, exchange,
    chairman, manager, secretary, reg_capital,
    setup_date, province, city, introduction,
    website, email, office, employees,
    main_business, business_scope
) VALUES %s
ON CONFLICT (ts_code) DO UPDATE SET
    com_name       = EXCLUDED.com_name,
    com_id         = EXCLUDED.com_id,
    exchange       = EXCLUDED.exchange,
    chairman       = EXCLUDED.chairman,
    manager        = EXCLUDED.manager,
    secretary      = EXCLUDED.secretary,
    reg_capital    = EXCLUDED.reg_capital,
    setup_date     = EXCLUDED.setup_date,
    province       = EXCLUDED.province,
    city           = EXCLUDED.city,
    introduction   = EXCLUDED.introduction,
    website        = EXCLUDED.website,
    email          = EXCLUDED.email,
    office         = EXCLUDED.office,
    employees      = EXCLUDED.employees,
    main_business  = EXCLUDED.main_business,
    business_scope = EXCLUDED.business_scope,
    updated_at     = NOW();
"""


def fetch_from_tushare() -> list[tuple]:
    pro = make_pro()
    log.info("正在拉取上市公司基本信息（全量）...")

    # API 按交易所分批拉取（单次返回有上限）
    import pandas as pd
    dfs = []
    for exchange in ("SSE", "SZSE", "BSE"):
        df = pro.stock_company(exchange=exchange, fields=FIELDS)
        if df is not None and not df.empty:
            dfs.append(df)
            log.info(f"  {exchange}：{len(df)} 条")

    df = pd.concat(dfs, ignore_index=True)
    log.info(f"共获取 {len(df)} 条记录")

    df["setup_date"] = df["setup_date"].apply(to_date)
    # employees 从 API 返回为 float64（含 NaN），显式转为 Python int 或 None
    df["employees"] = df["employees"].apply(
        lambda v: int(v) if v is not None and str(v) != "nan" else None
    )
    df = clean_df(df)

    return list(df[COLS].itertuples(index=False, name=None))


if __name__ == "__main__":
    rows = fetch_from_tushare()
    n = upsert(PG_DSN, UPSERT_SQL, rows)
    log.info(f"upsert 完成，共写入 {n} 条")
