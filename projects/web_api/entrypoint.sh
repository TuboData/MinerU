#!/usr/bin/env bash
set -euo pipefail

# 激活虚拟环境
. /app/venv/bin/activate

# 设置配置文件路径
export CONFIG_PATH="/app/config.yaml"

# 确保输出目录存在
mkdir -p /app/output

# 启动应用
exec uvicorn app:app "$@"
