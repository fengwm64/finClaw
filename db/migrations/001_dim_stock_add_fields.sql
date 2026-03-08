-- 迁移：dim_stock 补充 API 字段
ALTER TABLE dim_stock
    ADD COLUMN IF NOT EXISTS fullname     VARCHAR(128),
    ADD COLUMN IF NOT EXISTS enname       VARCHAR(256),
    ADD COLUMN IF NOT EXISTS cnspell      VARCHAR(32),
    ADD COLUMN IF NOT EXISTS curr_type    VARCHAR(8),
    ADD COLUMN IF NOT EXISTS act_name     VARCHAR(128),
    ADD COLUMN IF NOT EXISTS act_ent_type VARCHAR(64);

-- 更新 list_status 注释（新增 G 状态）
COMMENT ON COLUMN dim_stock.list_status  IS '上市状态：L=上市中，D=已退市，G=过会未交易，P=暂停上市';
COMMENT ON COLUMN dim_stock.fullname     IS '股票全称';
COMMENT ON COLUMN dim_stock.enname       IS '英文全称';
COMMENT ON COLUMN dim_stock.cnspell      IS '拼音缩写';
COMMENT ON COLUMN dim_stock.curr_type    IS '交易货币，如 CNY';
COMMENT ON COLUMN dim_stock.act_name     IS '实控人名称';
COMMENT ON COLUMN dim_stock.act_ent_type IS '实控人企业性质';
