-- 迁移：fact_adj_factor 改为按年分区表
-- 原表数据完整保留

BEGIN;

-- 1. 创建新的分区表（临时命名）
CREATE TABLE fact_adj_factor_new (
    ts_code     VARCHAR(16) NOT NULL,
    trade_date  DATE        NOT NULL,
    adj_factor  NUMERIC(20, 6)
        CONSTRAINT adj_factor_new_positive CHECK (adj_factor > 0),
    PRIMARY KEY (trade_date, ts_code)
) PARTITION BY RANGE (trade_date);

-- 2. 创建年份分区（1990 ~ 2040）
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 1990..2040 LOOP
        EXECUTE format(
            'CREATE TABLE fact_adj_factor_new_%s
             PARTITION OF fact_adj_factor_new
             FOR VALUES FROM (%L) TO (%L)',
            y,
            format('%s-01-01', y),
            format('%s-01-01', y + 1)
        );
    END LOOP;
END;
$$;

-- 3. 复制数据
INSERT INTO fact_adj_factor_new (ts_code, trade_date, adj_factor)
SELECT ts_code, trade_date, adj_factor FROM fact_adj_factor;

-- 4. 替换旧表
DROP TABLE fact_adj_factor;
ALTER TABLE fact_adj_factor_new RENAME TO fact_adj_factor;

-- 5. 重命名子分区（去掉 _new 后缀）
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 1990..2040 LOOP
        EXECUTE format(
            'ALTER TABLE fact_adj_factor_new_%s RENAME TO fact_adj_factor_%s',
            y, y
        );
    END LOOP;
END;
$$;

-- 6. 按股票代码建索引（分区表上的全局索引）
CREATE INDEX idx_adj_factor_ts_code ON fact_adj_factor (ts_code);

-- 7. 补充注释
COMMENT ON TABLE fact_adj_factor IS '复权因子表，每日盘前更新';
COMMENT ON COLUMN fact_adj_factor.ts_code    IS 'Tushare 股票代码';
COMMENT ON COLUMN fact_adj_factor.trade_date IS '交易日期';
COMMENT ON COLUMN fact_adj_factor.adj_factor IS '复权因子，前复权价 = 收盘价 × 复权因子 / 当日复权因子';

COMMIT;
