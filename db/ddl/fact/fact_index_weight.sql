-- 指数成分及权重事实表（月度，按年分区）
CREATE TABLE IF NOT EXISTS fact_index_weight (
    index_code VARCHAR(32)    NOT NULL,  -- 指数代码
    con_code   VARCHAR(32)    NOT NULL,  -- 成分证券代码
    trade_date DATE           NOT NULL,  -- 交易日期（月度快照日）
    weight     NUMERIC(10, 6),           -- 成分权重（%）
    PRIMARY KEY (trade_date, index_code, con_code)
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE fact_index_weight IS '指数成分及权重月度快照，每月更新一次';

COMMENT ON COLUMN fact_index_weight.index_code IS '指数代码';
COMMENT ON COLUMN fact_index_weight.con_code   IS '成分证券代码';
COMMENT ON COLUMN fact_index_weight.trade_date IS '权重快照日期（通常为月末交易日）';
COMMENT ON COLUMN fact_index_weight.weight     IS '该成分在指数中的权重（%）';

-- 按年动态创建分区（2000 ~ 2040）
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 2000..2040 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS fact_index_weight_%s
             PARTITION OF fact_index_weight
             FOR VALUES FROM (%L) TO (%L)',
            y,
            format('%s-01-01', y),
            format('%s-01-01', y + 1)
        );
    END LOOP;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_index_weight_index_code ON fact_index_weight (index_code);
CREATE INDEX IF NOT EXISTS idx_index_weight_con_code   ON fact_index_weight (con_code);
