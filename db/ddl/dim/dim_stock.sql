-- 股票基本信息维表
CREATE TABLE IF NOT EXISTS dim_stock (
    ts_code        VARCHAR(16) PRIMARY KEY,  -- TS代码，格式如 000001.SZ
    symbol         VARCHAR(8),               -- 股票代码，如 000001
    name           VARCHAR(64),              -- 股票简称
    fullname       VARCHAR(128),             -- 股票全称
    enname         VARCHAR(256),             -- 英文全称
    cnspell        VARCHAR(32),              -- 拼音缩写
    area           VARCHAR(64),              -- 所在地域
    industry       VARCHAR(64),              -- 所属行业
    market         VARCHAR(32),              -- 市场类型（主板/创业板/科创板/CDR）
    exchange       VARCHAR(16),              -- 交易所代码（SSE/SZSE/BSE）
    curr_type      VARCHAR(8),               -- 交易货币
    list_status    VARCHAR(2),               -- 上市状态：L上市 D退市 G过会未交易 P暂停上市
    list_date      DATE,                     -- 上市日期
    delist_date    DATE,                     -- 退市日期
    is_hs          VARCHAR(2),               -- 是否沪深港通标的：N否 H沪股通 S深股通
    act_name       VARCHAR(128),             -- 实控人名称
    act_ent_type   VARCHAR(64),              -- 实控人企业性质
    updated_at     TIMESTAMP DEFAULT NOW()   -- 记录最后更新时间
);

COMMENT ON TABLE dim_stock IS '股票基本信息维表';

COMMENT ON COLUMN dim_stock.ts_code      IS 'Tushare 股票代码，格式为 {symbol}.{exchange}，如 000001.SZ';
COMMENT ON COLUMN dim_stock.symbol       IS '纯数字股票代码，如 000001';
COMMENT ON COLUMN dim_stock.name         IS '股票简称';
COMMENT ON COLUMN dim_stock.fullname     IS '股票全称';
COMMENT ON COLUMN dim_stock.enname       IS '英文全称';
COMMENT ON COLUMN dim_stock.cnspell      IS '拼音缩写';
COMMENT ON COLUMN dim_stock.area         IS '注册地域，如 广东、北京';
COMMENT ON COLUMN dim_stock.industry     IS '所属申万一级行业';
COMMENT ON COLUMN dim_stock.market       IS '市场类型，如 主板、创业板、科创板、CDR';
COMMENT ON COLUMN dim_stock.exchange     IS '交易所代码：SSE（上交所）/ SZSE（深交所）/ BSE（北交所）';
COMMENT ON COLUMN dim_stock.curr_type    IS '交易货币，如 CNY';
COMMENT ON COLUMN dim_stock.list_status  IS '上市状态：L=上市中，D=已退市，G=过会未交易，P=暂停上市';
COMMENT ON COLUMN dim_stock.list_date    IS '上市日期';
COMMENT ON COLUMN dim_stock.delist_date  IS '退市日期，上市中的股票为 NULL';
COMMENT ON COLUMN dim_stock.is_hs        IS '沪深港通标的：N=否，H=沪股通，S=深股通';
COMMENT ON COLUMN dim_stock.act_name     IS '实控人名称';
COMMENT ON COLUMN dim_stock.act_ent_type IS '实控人企业性质';
COMMENT ON COLUMN dim_stock.updated_at   IS '本条记录最后同步/更新时间';
