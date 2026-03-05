#!/usr/bin/env python3
"""
Скрипт для первоначальной настройки amoCRM.
Запуск: python scripts/seed_amocrm.py --subdomain sherwoodhome --token "JWT_токен"
Или: python scripts/seed_amocrm.py (интерактивно)
"""
import argparse
import sqlite3
import sys
from pathlib import Path

# Добавить корень проекта в path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DATABASE = ROOT / "database.db"


def main():
    parser = argparse.ArgumentParser(description="Сохранить настройки amoCRM в БД")
    parser.add_argument("--subdomain", default="sherwoodhome", help="Subdomain amoCRM")
    parser.add_argument("--token", help="Долгосрочный JWT-токен")
    args = parser.parse_args()

    subdomain = args.subdomain.strip()
    token = (args.token or "").strip()

    if not token:
        print("Укажите --token с JWT-токеном из amoCRM.")
        print("Или введите токен вручную на странице /admin/amocrm/settings")
        sys.exit(1)

    if not DATABASE.exists():
        print(f"БД не найдена: {DATABASE}. Сначала запустите приложение для создания БД.")
        sys.exit(1)

    conn = sqlite3.connect(DATABASE)
    conn.execute(
        "INSERT OR REPLACE INTO amocrm_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
        ("subdomain", subdomain),
    )
    conn.execute(
        "INSERT OR REPLACE INTO amocrm_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
        ("access_token", token),
    )
    conn.commit()
    conn.close()
    print(f"Готово. Subdomain: {subdomain}. Токен сохранён.")
    print("Проверьте подключение на /admin/amocrm/settings")


if __name__ == "__main__":
    main()
