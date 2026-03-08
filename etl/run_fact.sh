#!/usr/bin/env bash
# 事实表更新入口
# 用法：
#   bash etl/run_fact.sh           # 增量模式（跳过已有日期）
#   bash etl/run_fact.sh --force   # 强制全量重跑

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="${PYTHON:-python}"

echo "=== FinClaw 事实表更新开始 ==="

"$PYTHON" "$SCRIPT_DIR/fact/fetch_fact_daily.py" "$@"
"$PYTHON" "$SCRIPT_DIR/fact/fetch_fact_adj_factor.py" "$@"
"$PYTHON" "$SCRIPT_DIR/fact/fetch_fact_daily_basic.py" "$@"
"$PYTHON" "$SCRIPT_DIR/fact/fetch_fact_etf_daily.py" "$@"
"$PYTHON" "$SCRIPT_DIR/fact/fetch_fact_index_weight.py" "$@"

echo "=== 事实表更新完成 ==="
