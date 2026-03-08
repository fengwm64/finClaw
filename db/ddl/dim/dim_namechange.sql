-- 股票曾用名维表
CREATE TABLE IF NOT EXISTS dim_namechange (
    ts_code       VARCHAR(16) NOT NULL,  -- 股票代码
    name          VARCHAR(64) NOT NULL,  -- 证券名称
    start_date    DATE        NOT NULL,  -- 名称生效日期
    end_date      DATE,                  -- 名称失效日期（NULL 表示当前仍在使用）
    ann_date      DATE,                  -- 公告日期
    change_reason VARCHAR(256),          -- 更名原因
    PRIMARY KEY (ts_code, start_date)
);

COMMENT ON TABLE dim_namechange IS '股票曾用名及更名历史';

COMMENT ON COLUMN dim_namechange.ts_code       IS 'Tushare 股票代码';
COMMENT ON COLUMN dim_namechange.name          IS '证券名称（该段时间内有效的名称）';
COMMENT ON COLUMN dim_namechange.start_date    IS '名称生效日期';
COMMENT ON COLUMN dim_namechange.end_date      IS '名称失效日期，NULL 表示当前仍使用该名称';
COMMENT ON COLUMN dim_namechange.ann_date      IS '更名公告日期';
COMMENT ON COLUMN dim_namechange.change_reason IS '更名原因';
