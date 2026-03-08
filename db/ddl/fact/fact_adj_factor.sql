-- 复权因子表（按年分区）
CREATE TABLE IF NOT EXISTS fact_adj_factor (
    ts_code     VARCHAR(16) NOT NULL,  -- 股票代码
    trade_date  DATE        NOT NULL,  -- 交易日期
    adj_factor  NUMERIC(20, 6)         -- 复权因子
        CONSTRAINT adj_factor_positive CHECK (adj_factor > 0),
    PRIMARY KEY (trade_date, ts_code)
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE fact_adj_factor IS '复权因子表，每日盘前更新';

COMMENT ON COLUMN fact_adj_factor.ts_code    IS 'Tushare 股票代码';
COMMENT ON COLUMN fact_adj_factor.trade_date IS '交易日期';
COMMENT ON COLUMN fact_adj_factor.adj_factor IS '复权因子，前复权价 = 收盘价 × 复权因子 / 当日复权因子';

-- 按年动态创建分区（1990 ~ 2040）
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 1990..2040 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS fact_adj_factor_%s
             PARTITION OF fact_adj_factor
             FOR VALUES FROM (%L) TO (%L)',
            y,
            format('%s-01-01', y),
            format('%s-01-01', y + 1)
        );
    END LOOP;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_adj_factor_ts_code ON fact_adj_factor (ts_code);
