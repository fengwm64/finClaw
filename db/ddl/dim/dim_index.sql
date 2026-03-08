-- 指数基本信息维表
CREATE TABLE IF NOT EXISTS dim_index (
    ts_code        VARCHAR(32)   NOT NULL PRIMARY KEY,  -- 指数代码
    indx_name      VARCHAR(128),                        -- 指数全称
    indx_csname    VARCHAR(64),                         -- 指数简称
    pub_party_name VARCHAR(128),                        -- 发布机构
    pub_date       DATE,                                -- 指数发布日期
    base_date      DATE,                                -- 指数基日
    bp             NUMERIC(16, 4),                      -- 基点
    adj_circle     VARCHAR(64)                          -- 成分证券调整周期
);

COMMENT ON TABLE dim_index IS '指数基本信息维表';

COMMENT ON COLUMN dim_index.ts_code        IS '指数代码';
COMMENT ON COLUMN dim_index.indx_name      IS '指数全称';
COMMENT ON COLUMN dim_index.indx_csname    IS '指数简称';
COMMENT ON COLUMN dim_index.pub_party_name IS '指数发布机构';
COMMENT ON COLUMN dim_index.pub_date       IS '指数发布日期';
COMMENT ON COLUMN dim_index.base_date      IS '指数基日（计算基点的起始日期）';
COMMENT ON COLUMN dim_index.bp             IS '指数基点';
COMMENT ON COLUMN dim_index.adj_circle     IS '成分证券调整周期，如季度、半年度';
