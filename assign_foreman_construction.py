#!/usr/bin/env python3
"""
Назначить прораба «Алексей Прорабов» на все проекты каркас, газоблок, пенополистиролбетон.

Проекты типа frame, gasblock, penopolistirol должны вести прорабы, а не мастера.
Скрипт добавляет назначение Алексея Прорабова во foreman_project_access.

Запуск: ./venv/bin/python assign_foreman_construction.py
"""

import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).parent
DATABASE = BASE_DIR / "database.db"
CONSTRUCTION_TYPES = ("frame", "gasblock", "penopolistirol")
FOREMAN_NAME = "Алексей Прорабов"


def main() -> None:
    if not DATABASE.exists():
        print("database.db не найден.")
        return

    conn = sqlite3.connect(str(DATABASE))
    conn.row_factory = sqlite3.Row

    # Найти прораба Алексея
    foreman = conn.execute(
        "SELECT id FROM users WHERE full_name = ? AND role = 'foreman'",
        (FOREMAN_NAME,),
    ).fetchone()
    if not foreman:
        print(f"Прораб «{FOREMAN_NAME}» не найден в базе.")
        conn.close()
        return

    foreman_id = foreman["id"]

    # Найти любого админа для assigned_by_id
    admin = conn.execute(
        "SELECT id FROM users WHERE role IN ('admin', 'super') LIMIT 1"
    ).fetchone()
    assigned_by_id = admin["id"] if admin else None

    # Все строительные проекты
    projects = conn.execute(
        "SELECT id, name, type FROM projects WHERE type IN (?, ?, ?) ORDER BY id",
        CONSTRUCTION_TYPES,
    ).fetchall()

    if not projects:
        print("Строительных проектов (каркас/газоблок/пенополистирол) не найдено.")
        conn.close()
        return

    added = 0
    already = 0
    for p in projects:
        exists = conn.execute(
            "SELECT 1 FROM foreman_project_access WHERE foreman_id = ? AND project_id = ?",
            (foreman_id, p["id"]),
        ).fetchone()
        if exists:
            already += 1
            continue
        conn.execute(
            """INSERT INTO foreman_project_access (foreman_id, project_id, assigned_by_id)
               VALUES (?, ?, ?)""",
            (foreman_id, p["id"], assigned_by_id),
        )
        added += 1
        print(f"  + {p['name']} ({p['type']})")

    conn.commit()
    conn.close()

    print(f"\nПрораб «{FOREMAN_NAME}» назначен на {added} проектов.")
    if already:
        print(f"Уже был назначен на {already} проектов.")


if __name__ == "__main__":
    main()
