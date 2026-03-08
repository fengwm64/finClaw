-- 原始月线行情视图（未复权）
CREATE OR REPLACE VIEW v_monthly AS
WITH ranked AS (
    SELECT *,
        date_trunc('month', trade_date)::date AS month_key,
        ROW_NUMBER() OVER (PARTITION BY ts_code, date_trunc('month', trade_date) ORDER BY trade_date ASC)  AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY ts_code, date_trunc('month', trade_date) ORDER BY trade_date DESC) AS rn_desc
    FROM fact_daily
)
SELECT
    ts_code,
    month_key,
    MAX(CASE WHEN rn_asc  = 1 THEN trade_date END) AS month_start,
    MAX(CASE WHEN rn_desc = 1 THEN trade_date END) AS month_end,
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
GROUP BY ts_code, month_key;

COMMENT ON VIEW v_monthly IS '原始（未复权）月线行情，以自然月为分组键';
