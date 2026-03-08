-- ETF 基础信息维表
CREATE TABLE IF NOT EXISTS dim_etf (
    ts_code      VARCHAR(16)  NOT NULL PRIMARY KEY,  -- ETF 交易代码
    csname       VARCHAR(64),                         -- ETF 中文简称
    extname      VARCHAR(128),                        -- ETF 扩展简称（交易所专用）
    cname        VARCHAR(128),                        -- ETF 中文全称
    index_code   VARCHAR(32),                         -- 跟踪指数代码
    index_name   VARCHAR(128),                        -- 跟踪指数名称
    setup_date   DATE,                                -- 成立日期
    list_date    DATE,                                -- 上市日期
    list_status  VARCHAR(2),                          -- 上市状态：L=上市 D=退市 P=暂停
    exchange     VARCHAR(8),                          -- 交易所：SH / SZ
    mgr_name     VARCHAR(64),                         -- 基金管理人简称
    custod_name  VARCHAR(64),                         -- 基金托管人名称
    mgt_fee      NUMERIC(8, 4),                       -- 管理费率（%）
    etf_type     VARCHAR(32)                          -- 基金渠道类型（境内 / QDII）
);

COMMENT ON TABLE dim_etf IS 'ETF 基础信息维表，含沪深两市上市及退市 ETF（包含 QDII）';

COMMENT ON COLUMN dim_etf.ts_code     IS 'ETF 交易代码，格式如 159526.SZ';
COMMENT ON COLUMN dim_etf.csname      IS 'ETF 中文简称';
COMMENT ON COLUMN dim_etf.extname     IS 'ETF 扩展简称（交易所专属字段）';
COMMENT ON COLUMN dim_etf.cname       IS 'ETF 中文全称';
COMMENT ON COLUMN dim_etf.index_code  IS '跟踪基准指数代码';
COMMENT ON COLUMN dim_etf.index_name  IS '跟踪基准指数名称';
COMMENT ON COLUMN dim_etf.setup_date  IS 'ETF 成立日期';
COMMENT ON COLUMN dim_etf.list_date   IS 'ETF 上市日期';
COMMENT ON COLUMN dim_etf.list_status IS '上市状态：L=上市中，D=已退市，P=暂停上市';
COMMENT ON COLUMN dim_etf.exchange    IS '交易所：SH=上交所，SZ=深交所';
COMMENT ON COLUMN dim_etf.mgr_name    IS '基金管理人简称';
COMMENT ON COLUMN dim_etf.custod_name IS '基金托管人名称';
COMMENT ON COLUMN dim_etf.mgt_fee     IS '管理费率（%）';
COMMENT ON COLUMN dim_etf.etf_type    IS '基金渠道类型，如境内、QDII';
