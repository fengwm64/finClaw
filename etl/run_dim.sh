#!/usr/bin/env bash
# 维表全量更新入口
# 用法：bash etl/run_dim.sh
# 可重复执行

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="${PYTHON:-python}"

run() {
    echo "  -> $1"
    "$PYTHON" "$SCRIPT_DIR/$1"
}

echo "=== FinClaw 维表更新开始 ==="

run dim/fetch_dim_stock.py
run dim/fetch_dim_trade_cal.py
run dim/fetch_dim_company.py
run dim/fetch_dim_namechange.py
run dim/fetch_dim_index.py
run dim/fetch_dim_etf.py

echo "=== 维表更新完成 ==="
