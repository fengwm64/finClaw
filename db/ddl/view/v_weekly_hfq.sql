-- 后复权周线行情视图（复用 v_daily_hfq）
CREATE OR REPLACE VIEW v_weekly_hfq AS
WITH ranked AS (
    SELECT *,
        date_trunc('week', trade_date)::date AS week_key,
        ROW_NUMBER() OVER (PARTITION BY ts_code, date_trunc('week', trade_date) ORDER BY trade_date ASC)  AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY ts_code, date_trunc('week', trade_date) ORDER BY trade_date DESC) AS rn_desc
    FROM v_daily_hfq
)
SELECT
    ts_code,
    week_key,
    MAX(CASE WHEN rn_asc  = 1 THEN trade_date END) AS week_start,
    MAX(CASE WHEN rn_desc = 1 THEN trade_date END) AS week_end,
    MAX(CASE WHEN rn_asc  = 1 THEN open      END) AS open,
    MAX(high)                                       AS high,
    MIN(low)                                        AS low,
    MAX(CASE WHEN rn_desc = 1 THEN close     END) AS close,
    MAX(CASE WHEN rn_asc  = 1 THEN pre_close END) AS pre_close,
    ROUND(
        ((MAX(CASE WHEN rn_desc = 1 THEN close     END)
        - MAX(CASE WHEN rn_asc  = 1 THEN pre_close END))
        / NULLIF(MAX(CASE WHEN rn_asc = 1 THEN pre_close END), 0) * 100)::numeric, 4
    ) AS pct_chg,
    SUM(vol)    AS vol,
    SUM(amount) AS amount
FROM ranked
GROUP BY ts_code, week_key;

COMMENT ON VIEW v_weekly_hfq IS '后复权周线行情，适合历史回测';
