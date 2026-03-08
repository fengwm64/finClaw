-- 交易日历维表
-- 数据来源：tushare trade_cal 接口
CREATE TABLE IF NOT EXISTS dim_trade_cal (
    exchange       VARCHAR(16) NOT NULL,  -- 交易所代码
    cal_date       DATE        NOT NULL,  -- 日历日期
    is_open        BOOLEAN     NOT NULL,  -- 是否交易日
    pretrade_date  DATE,                  -- 上一个交易日
    PRIMARY KEY (exchange, cal_date)
);

COMMENT ON TABLE dim_trade_cal IS '交易日历维表';

COMMENT ON COLUMN dim_trade_cal.exchange      IS '交易所代码：SSE（上交所）/ SZSE（深交所）';
COMMENT ON COLUMN dim_trade_cal.cal_date      IS '日历日期，含节假日和周末';
COMMENT ON COLUMN dim_trade_cal.is_open       IS '是否为交易日：true=交易日，false=非交易日';
COMMENT ON COLUMN dim_trade_cal.pretrade_date IS '上一个交易日日期，非交易日该字段也有值';
