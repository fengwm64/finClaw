# FinClaw 数据库文档

**数据库**：PostgreSQL 17（Docker）
**初始化**：`bash db/init.sh`
**数据同步**：`bash etl/run_dim.sh` / `bash etl/run_fact.sh`

---

## 整体结构

```
├── 维表（dim_*）   — 相对静态的参考数据，全量 upsert
├── 事实表（fact_*）— 按年 RANGE 分区，增量追加
└── 视图（v_*）     — 派生计算，不占存储，查询时实时计算
```

---

## 维表

### dim_stock — 股票基本信息

主键：`ts_code`

| 列 | 类型 | 说明 |
|---|---|---|
| ts_code | VARCHAR(16) | 股票代码，格式 `000001.SZ` |
| symbol | VARCHAR(8) | 纯数字代码，如 `000001` |
| name | VARCHAR(64) | 股票简称 |
| fullname | VARCHAR(128) | 股票全称 |
| enname | VARCHAR(256) | 英文全称 |
| cnspell | VARCHAR(32) | 拼音缩写 |
| area | VARCHAR(64) | 注册地域 |
| industry | VARCHAR(64) | 所属申万一级行业 |
| market | VARCHAR(32) | 市场类型（主板 / 创业板 / 科创板 / CDR） |
| exchange | VARCHAR(16) | 交易所：SSE / SZSE / BSE |
| curr_type | VARCHAR(8) | 交易货币 |
| list_status | VARCHAR(2) | 上市状态：`L`=上市 `D`=退市 `P`=暂停 `G`=过会未交易 |
| list_date | DATE | 上市日期 |
| delist_date | DATE | 退市日期，NULL 表示仍在市 |
| is_hs | VARCHAR(2) | 沪深港通：`N`=否 `H`=沪股通 `S`=深股通 |
| act_name | VARCHAR(128) | 实控人名称 |
| act_ent_type | VARCHAR(64) | 实控人企业性质 |
| updated_at | TIMESTAMP | 最后同步时间 |

---

### dim_trade_cal — 交易日历

主键：`(exchange, cal_date)`

| 列 | 类型 | 说明 |
|---|---|---|
| exchange | VARCHAR(16) | 交易所：SSE / SZSE |
| cal_date | DATE | 日历日期（含节假日、周末） |
| is_open | BOOLEAN | 是否为交易日 |
| pretrade_date | DATE | 上一个交易日日期 |

常用查询：
```sql
-- 某年所有交易日
SELECT cal_date FROM dim_trade_cal
WHERE exchange = 'SSE' AND is_open AND cal_date BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY cal_date;
```

---

### dim_company — 上市公司基本信息

主键：`ts_code`

| 列 | 类型 | 说明 |
|---|---|---|
| ts_code | VARCHAR(16) | 股票代码 |
| com_name | VARCHAR(128) | 公司全称 |
| com_id | VARCHAR(32) | 统一社会信用代码 |
| exchange | VARCHAR(16) | 交易所：SSE / SZSE / BSE |
| chairman | VARCHAR(64) | 法人代表 |
| manager | VARCHAR(64) | 总经理 |
| secretary | VARCHAR(64) | 董事会秘书 |
| reg_capital | NUMERIC(20,4) | 注册资本（万元） |
| setup_date | DATE | 公司注册日期 |
| province | VARCHAR(32) | 注册省份 |
| city | VARCHAR(32) | 注册城市 |
| introduction | TEXT | 公司简介 |
| website | VARCHAR(128) | 官网 |
| email | VARCHAR(128) | 信息披露联系邮箱 |
| office | VARCHAR(256) | 办公室地址 |
| employees | BIGINT | 员工总人数 |
| main_business | TEXT | 主要业务及产品 |
| business_scope | TEXT | 工商登记经营范围 |
| updated_at | TIMESTAMP | 最后同步时间 |

---

### dim_index — 指数基本信息

主键：`ts_code`

| 列 | 类型 | 说明 |
|---|---|---|
| ts_code | VARCHAR(32) | 指数代码（PK） |
| indx_name | VARCHAR(128) | 指数全称 |
| indx_csname | VARCHAR(64) | 指数简称 |
| pub_party_name | VARCHAR(128) | 发布机构（如中证、国证、沪深） |
| pub_date | DATE | 指数发布日期 |
| base_date | DATE | 指数基日（计算基点的起始日期） |
| bp | NUMERIC(16,4) | 基点 |
| adj_circle | VARCHAR(64) | 成分证券调整周期（如季度、半年度） |

---

### dim_namechange — 股票曾用名

主键：`(ts_code, start_date)`

| 列 | 类型 | 说明 |
|---|---|---|
| ts_code | VARCHAR(16) | 股票代码 |
| name | VARCHAR(64) | 该段时间内的证券名称 |
| start_date | DATE | 名称生效日期 |
| end_date | DATE | 名称失效日期，NULL 表示当前名称 |
| ann_date | DATE | 更名公告日期 |
| change_reason | VARCHAR(256) | 更名原因 |

---

## 事实表

所有事实表均按 `trade_date` **年度 RANGE 分区**（1990–2040），每年一个子表，如 `fact_daily_2024`。

> **注意**：事实表之间无外键约束（数据仓库设计惯例），`ts_code` 含义与 `dim_stock.ts_code` 一致，但不强制引用完整性。

---

### fact_daily — 日行情（不复权）

主键：`(trade_date, ts_code)`

| 列 | 类型 | 说明 |
|---|---|---|
| ts_code | VARCHAR(16) | 股票代码 |
| trade_date | DATE | 交易日期 |
| open | NUMERIC(12,4) | 开盘价 |
| high | NUMERIC(12,4) | 最高价 |
| low | NUMERIC(12,4) | 最低价 |
| close | NUMERIC(12,4) | 收盘价 |
| pre_close | NUMERIC(12,4) | 昨收价（除权） |
| change | NUMERIC(12,4) | 涨跌额 |
| pct_chg | NUMERIC(12,4) | 涨跌幅（%，基于除权昨收） |
| vol | NUMERIC(20,2) | 成交量（手） |
| amount | NUMERIC(20,4) | 成交额（千元） |

---

### fact_adj_factor — 复权因子

主键：`(trade_date, ts_code)`；附加索引：`ts_code`

| 列 | 类型 | 说明 |
|---|---|---|
| ts_code | VARCHAR(16) | 股票代码 |
| trade_date | DATE | 交易日期 |
| adj_factor | NUMERIC(20,6) | 累计复权因子，恒 > 0 |

复权因子含义：
- **后复权价** = 原始价 × adj_factor
- **前复权价** = 原始价 × adj_factor / 最新adj_factor

---

### fact_daily_basic — 每日指标

主键：`(trade_date, ts_code)`

| 列 | 类型 | 说明 |
|---|---|---|
| ts_code | VARCHAR(16) | 股票代码 |
| trade_date | DATE | 交易日期 |
| close | NUMERIC(12,4) | 收盘价 |
| turnover_rate | NUMERIC(10,4) | 换手率（%，总股本） |
| turnover_rate_f | NUMERIC(10,4) | 换手率（%，自由流通股） |
| volume_ratio | NUMERIC(10,4) | 量比 |
| pe | NUMERIC(16,4) | 市盈率（动态），亏损为 NULL |
| pe_ttm | NUMERIC(16,4) | 滚动市盈率 TTM |
| pb | NUMERIC(16,4) | 市净率 |
| ps | NUMERIC(16,4) | 市销率 |
| ps_ttm | NUMERIC(16,4) | 滚动市销率 TTM |
| dv_ratio | NUMERIC(10,4) | 股息率（%） |
| dv_ttm | NUMERIC(10,4) | 滚动股息率 TTM（%） |
| total_share | NUMERIC(20,4) | 总股本（万股） |
| float_share | NUMERIC(20,4) | 流通股本（万股） |
| free_share | NUMERIC(20,4) | 自由流通股本（万股） |
| total_mv | NUMERIC(20,4) | 总市值（万元） |
| circ_mv | NUMERIC(20,4) | 流通市值（万元） |

---

## 视图

视图实时计算，不存储数据。

| 视图 | 基于 | 说明 |
|---|---|---|
| `v_daily_hfq` | fact_daily + fact_adj_factor | 后复权日线 |
| `v_daily_qfq` | fact_daily + fact_adj_factor | 前复权日线（⚠️ 含未来函数，见下方说明） |
| `v_weekly` | fact_daily | 原始周线（含 week_start / week_end） |
| `v_monthly` | fact_daily | 原始月线（含 month_start / month_end） |
| `v_weekly_hfq` | v_daily_hfq | 后复权周线 |
| `v_monthly_hfq` | v_daily_hfq | 后复权月线 |

**周/月线聚合规则**：

| 字段 | 取值 |
|---|---|
| open | 周期内第一个交易日开盘价 |
| high | 周期内最高价 |
| low | 周期内最低价 |
| close | 周期内最后一个交易日收盘价 |
| pre_close | 周期内第一个交易日的前收（即上期末收盘） |
| pct_chg | `(close - pre_close) / pre_close × 100` |
| vol / amount | 求和 |

**⚠️ 前复权的未来函数问题**：
`v_daily_qfq` 和基于它的视图以**当前最新复权因子**为基准计算历史价格。每次股票分红送股后，历史复权价会整体平移，即查询时"看到"的历史价格会随未来事件而变化。适合日常展示和趋势分析，**不适合严格历史回测**。回测请使用 `v_*_hfq` 系列（后复权），或直接使用 `pct_chg` 计算收益率。

---

## 常用查询示例

```sql
-- 1. 查某股票最近 20 个交易日后复权日线
SELECT trade_date, open, high, low, close, vol
FROM v_daily_hfq
WHERE ts_code = '000001.SZ'
ORDER BY trade_date DESC
LIMIT 20;

-- 2. 查某股票后复权周线（近一年）
SELECT week_start, week_end, open, high, low, close, pct_chg, vol
FROM v_weekly_hfq
WHERE ts_code = '000001.SZ'
  AND week_key >= date_trunc('year', CURRENT_DATE)
ORDER BY week_key;

-- 3. 全市场某日 PE < 15 且换手率 > 3% 的股票
SELECT b.ts_code, s.name, b.pe, b.turnover_rate, b.circ_mv
FROM fact_daily_basic b
JOIN dim_stock s USING (ts_code)
WHERE b.trade_date = '2024-12-31'
  AND b.pe BETWEEN 0 AND 15
  AND b.turnover_rate > 3
ORDER BY b.circ_mv DESC;

-- 4. 查某股票历史曾用名
SELECT name, start_date, end_date, change_reason
FROM dim_namechange
WHERE ts_code = '000001.SZ'
ORDER BY start_date;
```

---

## 迁移记录

| 文件 | 说明 |
|---|---|
| `db/migrations/001_dim_stock_add_fields.sql` | dim_stock 补充 fullname / enname / cnspell / curr_type / act_name / act_ent_type 字段 |
| `db/migrations/002_fact_adj_factor_add_partition.sql` | fact_adj_factor 从非分区表迁移至按年分区表 |
