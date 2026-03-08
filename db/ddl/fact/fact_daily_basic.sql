-- 股票每日指标事实表（按年分区）
CREATE TABLE IF NOT EXISTS fact_daily_basic (
    ts_code          VARCHAR(16)    NOT NULL,  -- 股票代码
    trade_date       DATE           NOT NULL,  -- 交易日期
    close            NUMERIC(12, 4),           -- 当日收盘价
    turnover_rate    NUMERIC(10, 4),           -- 换手率（%）
    turnover_rate_f  NUMERIC(10, 4),           -- 换手率（自由流通股）
    volume_ratio     NUMERIC(10, 4),           -- 量比
    pe               NUMERIC(16, 4),           -- 市盈率（动态），亏损为空
    pe_ttm           NUMERIC(16, 4),           -- 市盈率 TTM，亏损为空
    pb               NUMERIC(16, 4),           -- 市净率
    ps               NUMERIC(16, 4),           -- 市销率
    ps_ttm           NUMERIC(16, 4),           -- 市销率 TTM
    dv_ratio         NUMERIC(10, 4),           -- 股息率（%）
    dv_ttm           NUMERIC(10, 4),           -- 股息率 TTM（%）
    total_share      NUMERIC(20, 4),           -- 总股本（万股）
    float_share      NUMERIC(20, 4),           -- 流通股本（万股）
    free_share       NUMERIC(20, 4),           -- 自由流通股本（万股）
    total_mv         NUMERIC(20, 4),           -- 总市值（万元）
    circ_mv          NUMERIC(20, 4),           -- 流通市值（万元）
    PRIMARY KEY (trade_date, ts_code)
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE fact_daily_basic IS '股票每日指标事实表，每日15:00~17:00更新';

COMMENT ON COLUMN fact_daily_basic.ts_code         IS 'Tushare 股票代码';
COMMENT ON COLUMN fact_daily_basic.trade_date      IS '交易日期';
COMMENT ON COLUMN fact_daily_basic.close           IS '当日收盘价';
COMMENT ON COLUMN fact_daily_basic.turnover_rate   IS '换手率（%，基于总股本）';
COMMENT ON COLUMN fact_daily_basic.turnover_rate_f IS '换手率（%，基于自由流通股）';
COMMENT ON COLUMN fact_daily_basic.volume_ratio    IS '量比';
COMMENT ON COLUMN fact_daily_basic.pe              IS '市盈率（动态），亏损时为 NULL';
COMMENT ON COLUMN fact_daily_basic.pe_ttm          IS '滚动市盈率 TTM，亏损时为 NULL';
COMMENT ON COLUMN fact_daily_basic.pb              IS '市净率';
COMMENT ON COLUMN fact_daily_basic.ps              IS '市销率';
COMMENT ON COLUMN fact_daily_basic.ps_ttm          IS '滚动市销率 TTM';
COMMENT ON COLUMN fact_daily_basic.dv_ratio        IS '股息率（%）';
COMMENT ON COLUMN fact_daily_basic.dv_ttm          IS '滚动股息率 TTM（%）';
COMMENT ON COLUMN fact_daily_basic.total_share     IS '总股本（万股）';
COMMENT ON COLUMN fact_daily_basic.float_share     IS '流通股本（万股）';
COMMENT ON COLUMN fact_daily_basic.free_share      IS '自由流通股本（万股）';
COMMENT ON COLUMN fact_daily_basic.total_mv        IS '总市值（万元）';
COMMENT ON COLUMN fact_daily_basic.circ_mv         IS '流通市值（万元）';

-- 按年动态创建分区（1990 ~ 2040）
DO $$
DECLARE
    y INT;
BEGIN
    FOR y IN 1990..2040 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS fact_daily_basic_%s
             PARTITION OF fact_daily_basic
             FOR VALUES FROM (%L) TO (%L)',
            y,
            format('%s-01-01', y),
            format('%s-01-01', y + 1)
        );
    END LOOP;
END;
$$;
