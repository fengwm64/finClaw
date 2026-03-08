-- 前复权日线行情视图
-- 公式：复权价 = 原始价 × adj_factor / 最新adj_factor
-- 效果：历史价格向当前价格基准对齐，当日价格 = 原始价
CREATE OR REPLACE VIEW v_daily_qfq AS
WITH latest_factor AS (
    SELECT DISTINCT ON (ts_code)
        ts_code,
        adj_factor AS latest_adj
    FROM fact_adj_factor
    ORDER BY ts_code, trade_date DESC
)
SELECT
    d.ts_code,
    d.trade_date,
    ROUND((d.open      * f.adj_factor / lf.latest_adj)::numeric, 4) AS open,
    ROUND((d.high      * f.adj_factor / lf.latest_adj)::numeric, 4) AS high,
    ROUND((d.low       * f.adj_factor / lf.latest_adj)::numeric, 4) AS low,
    ROUND((d.close     * f.adj_factor / lf.latest_adj)::numeric, 4) AS close,
    ROUND((d.pre_close * f.adj_factor / lf.latest_adj)::numeric, 4) AS pre_close,
    d.change,
    d.pct_chg,
    d.vol,
    d.amount
FROM fact_daily d
JOIN fact_adj_factor f USING (ts_code, trade_date)
JOIN latest_factor lf USING (ts_code);

COMMENT ON VIEW v_daily_qfq IS '前复权日线行情（历史价格以当前价为基准对齐，复权价 = 原始价 × adj_factor / 最新adj_factor）';
