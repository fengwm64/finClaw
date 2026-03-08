-- 后复权日线行情视图
-- 公式：复权价 = 原始价 × adj_factor
CREATE OR REPLACE VIEW v_daily_hfq AS
SELECT
    d.ts_code,
    d.trade_date,
    ROUND((d.open   * f.adj_factor)::numeric, 4) AS open,
    ROUND((d.high   * f.adj_factor)::numeric, 4) AS high,
    ROUND((d.low    * f.adj_factor)::numeric, 4) AS low,
    ROUND((d.close  * f.adj_factor)::numeric, 4) AS close,
    ROUND((d.pre_close * f.adj_factor)::numeric, 4) AS pre_close,
    d.change,
    d.pct_chg,
    d.vol,
    d.amount
FROM fact_daily d
JOIN fact_adj_factor f USING (ts_code, trade_date);

COMMENT ON VIEW v_daily_hfq IS '后复权日线行情（复权价 = 原始价 × adj_factor）';
