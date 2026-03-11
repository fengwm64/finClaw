#!/usr/bin/env bash
# FinClaw 数据库初始化脚本
# 用法：bash db/init.sh
# 可重复执行（所有 DDL 使用 IF NOT EXISTS）

set -e

CONTAINER="finclaw-db-postgres-1"
DB_USER="finclaw"
DB_NAME="finclaw"

run_sql() {
    echo "  -> $1"
    docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" < "$1"
}

echo "=== FinClaw DB 初始化开始 ==="

echo "[1/3] 维表"
run_sql db/ddl/dim/dim_stock.sql
run_sql db/ddl/dim/dim_trade_cal.sql
run_sql db/ddl/dim/dim_company.sql
run_sql db/ddl/dim/dim_namechange.sql
run_sql db/ddl/dim/dim_index.sql
run_sql db/ddl/dim/dim_etf.sql

echo "[2/3] 事实表"
run_sql db/ddl/fact/fact_daily.sql
run_sql db/ddl/fact/fact_adj_factor.sql
run_sql db/ddl/fact/fact_daily_basic.sql
run_sql db/ddl/fact/fact_etf_daily.sql
run_sql db/ddl/fact/fact_index_weight.sql

echo "[3/3] 视图"
run_sql db/ddl/view/v_daily_hfq.sql
run_sql db/ddl/view/v_daily_qfq.sql
run_sql db/ddl/view/v_weekly.sql
run_sql db/ddl/view/v_monthly.sql
run_sql db/ddl/view/v_weekly_hfq.sql
run_sql db/ddl/view/v_monthly_hfq.sql

echo "=== 初始化完成 ==="
