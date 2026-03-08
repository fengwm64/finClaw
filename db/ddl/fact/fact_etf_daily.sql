-- ETF 日行情事实表（按年分区）
CREATE TABLE IF NOT EXISTS fact_etf_daily (
    ts_code    VARCHAR(16)    NOT NULL,  -- ETF 代码
    trade_date DATE           NOT NULL,  -- 交易日期
    open       NUMERIC(12, 4),           -- 开盘价（元）
    high       NUMERIC(12, 4),           -- 最高价（元）
    low        NUMERIC(12, 4),           -- 最低价（元）
    close      NUMERIC(12, 4),           -- 收盘价（元）
    pre_close  NUMERIC(12, 4),           -- 昨收价（元）
    change     NUMERIC(12, 4),           -- 涨跌额（元）
    pct_chg    NUMERIC(12, 4),           -- 涨跌幅（%）
    vol        NUMERIC(20, 2),           -- 成交量（份）
    amount     NUMERIC(20, 4),           -- 成交额（千元）
    PRIMARY KEY (trade_date, ts_code)
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE fact_etf_daily IS 'ETF 日行情事实表，不复权';

COMMENT ON COLUMN fact_etf_daily.ts_code    IS 'ETF 交易代码';
COMMENT ON COLUMN fact_etf_daily.trade_date IS '交易日期';
COMMENT ON COLUMN fact_etf_daily.open       IS '开盘价（元）';
COMMENT ON COLUMN fact_etf_daily.high       IS '最高价（元）';
COMMENT ON COLUMN fact_etf_daily.low        IS '最低价（元）';
COMMENT ON COLUMN fact_etf_daily.close      IS '收盘价（元）';
COMMENT ON COLUMN fact_etf_daily.pre_close  IS '昨收价（元）';
COMMENT ON COLUMN fact_etf_daily.change     IS '涨跌额（元）';
COMMENT ON COLUMN fact_etf_daily.pct_chg   IS '涨跌幅（%）';
COMMENT ON COLUMN fact_etf_daily.vol        IS '成交量（份）';
COMMENT ON COLUMN fact_etf_daily.amount     IS '成交额（千元）';

-- 按年动态创建分区（1990 ~ 2040）
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 1990..2040 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS fact_etf_daily_%s
             PARTITION OF fact_etf_daily
             FOR VALUES FROM (%L) TO (%L)',
            y,
            format('%s-01-01', y),
            format('%s-01-01', y + 1)
        );
    END LOOP;
END;
$$;
