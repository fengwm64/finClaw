-- 上市公司基本信息维表
CREATE TABLE IF NOT EXISTS dim_company (
    ts_code        VARCHAR(16)   PRIMARY KEY,  -- 股票代码
    com_name       VARCHAR(128),               -- 公司全称
    com_id         VARCHAR(32),                -- 统一社会信用代码
    exchange       VARCHAR(16),                -- 交易所代码
    chairman       VARCHAR(64),                -- 法人代表
    manager        VARCHAR(64),                -- 总经理
    secretary      VARCHAR(64),                -- 董秘
    reg_capital    NUMERIC(20, 4),             -- 注册资本（万元）
    setup_date     DATE,                       -- 注册日期
    province       VARCHAR(32),                -- 所在省份
    city           VARCHAR(32),                -- 所在城市
    introduction   TEXT,                       -- 公司介绍
    website        VARCHAR(128),               -- 公司主页
    email          VARCHAR(128),               -- 电子邮件
    office         VARCHAR(256),               -- 办公地址
    employees      BIGINT,                     -- 员工人数（源数据存在异常大值，用 BIGINT 兼容）
    main_business  TEXT,                       -- 主要业务及产品
    business_scope TEXT,                       -- 经营范围
    updated_at     TIMESTAMP DEFAULT NOW()     -- 记录最后更新时间
);

COMMENT ON TABLE dim_company IS '上市公司基本信息维表';

COMMENT ON COLUMN dim_company.ts_code        IS 'Tushare 股票代码';
COMMENT ON COLUMN dim_company.com_name       IS '公司全称';
COMMENT ON COLUMN dim_company.com_id         IS '统一社会信用代码';
COMMENT ON COLUMN dim_company.exchange       IS '交易所代码：SSE / SZSE / BSE';
COMMENT ON COLUMN dim_company.chairman       IS '法人代表';
COMMENT ON COLUMN dim_company.manager        IS '总经理';
COMMENT ON COLUMN dim_company.secretary      IS '董事会秘书';
COMMENT ON COLUMN dim_company.reg_capital    IS '注册资本（万元）';
COMMENT ON COLUMN dim_company.setup_date     IS '公司注册日期';
COMMENT ON COLUMN dim_company.province       IS '注册所在省份';
COMMENT ON COLUMN dim_company.city           IS '注册所在城市';
COMMENT ON COLUMN dim_company.introduction   IS '公司简介';
COMMENT ON COLUMN dim_company.website        IS '官方网站';
COMMENT ON COLUMN dim_company.email          IS '信息披露联系邮箱';
COMMENT ON COLUMN dim_company.office         IS '办公室地址';
COMMENT ON COLUMN dim_company.employees      IS '员工总人数';
COMMENT ON COLUMN dim_company.main_business  IS '主要业务及产品描述';
COMMENT ON COLUMN dim_company.business_scope IS '工商登记经营范围';
COMMENT ON COLUMN dim_company.updated_at     IS '本条记录最后同步时间';
