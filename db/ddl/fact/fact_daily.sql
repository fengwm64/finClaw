-- A股日行情事实表（按年分区）
CREATE TABLE IF NOT EXISTS fact_daily (
    ts_code    VARCHAR(16)    NOT NULL,  -- 股票代码
    trade_date DATE           NOT NULL,  -- 交易日期
    open       NUMERIC(12, 4),           -- 开盘价
    high       NUMERIC(12, 4),           -- 最高价
    low        NUMERIC(12, 4),           -- 最低价
    close      NUMERIC(12, 4),           -- 收盘价
    pre_close  NUMERIC(12, 4),           -- 昨收价（除权）
    change     NUMERIC(12, 4),           -- 涨跌额
    pct_chg    NUMERIC(12, 4),           -- 涨跌幅（%）
    vol        NUMERIC(20, 2),           -- 成交量（手）
    amount     NUMERIC(20, 4),           -- 成交额（千元）
    PRIMARY KEY (trade_date, ts_code)
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE fact_daily IS 'A股日行情事实表，不复权';

COMMENT ON COLUMN fact_daily.ts_code    IS 'Tushare 股票代码';
COMMENT ON COLUMN fact_daily.trade_date IS '交易日期';
COMMENT ON COLUMN fact_daily.open       IS '开盘价';
COMMENT ON COLUMN fact_daily.high       IS '最高价';
COMMENT ON COLUMN fact_daily.low        IS '最低价';
COMMENT ON COLUMN fact_daily.close      IS '收盘价';
COMMENT ON COLUMN fact_daily.pre_close  IS '昨收价（前复权调整）';
COMMENT ON COLUMN fact_daily.change     IS '涨跌额';
COMMENT ON COLUMN fact_daily.pct_chg   IS '涨跌幅（基于除权后昨收，百分比）';
COMMENT ON COLUMN fact_daily.vol        IS '成交量（手）';
COMMENT ON COLUMN fact_daily.amount     IS '成交额（千元）';

-- 按年动态创建分区（1990 ~ 2040）
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 1990..2040 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS fact_daily_%s
             PARTITION OF fact_daily
             FOR VALUES FROM (%L) TO (%L)',
            y,
            format('%s-01-01', y),
            format('%s-01-01', y + 1)
        );
    END LOOP;
END;
$$;
