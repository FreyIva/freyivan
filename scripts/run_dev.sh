#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -d "venv" ]]; then
  echo "Не найдено виртуальное окружение ./venv"
  echo "Создайте его: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
  exit 1
fi

source "venv/bin/activate"
export FLASK_APP="app.py"
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-5002}"

echo "Запуск StroyControl на ${HOST}:${PORT}"
python "app.py"
