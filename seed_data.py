#!/usr/bin/env python3
"""
Скрипт заполнения тестовыми данными: Отрада, Муранка, Новосемейкино, Золотые пески.
Фотографии загружаются с picsum.photos. При ошибке — placeholder.
Для первых 5 проектов — рандомные даты этапов: просрочка, вовремя, досрочно.
Также создаются прорабы, их назначения на объекты и тестовые отчёты.
Запуск: python seed_data.py
"""
import random
import sqlite3
import warnings
from datetime import date, timedelta
from pathlib import Path

from werkzeug.security import generate_password_hash

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

DATABASE = Path(__file__).parent / "database.db"
UPLOAD_FOLDER = Path(__file__).parent / "static" / "uploads"
UPLOAD_FOLDER.mkdir(parents=True, exist_ok=True)

# Минимальный валидный серый JPEG 2x2 (base64)
_PLACEHOLDER_JPEG = (
    b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06'
    b'\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\x0d\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a\x1f'
    b'\x1e\x1d\x1a\x1c\x1c $.\' ",#\x1c\x1c(7),01444\x1f\'9=82<.342\xff\xc0\x00\x0b\x08\x00\x02\x00'
    b'\x02\x01\x01\x11\x00\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00'
    b'\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\xff\xda\x00\x08\x01\x01\x00\x00\x00?\x00\xfc'
    b'\xbf\xff\xd9'
)

BUILDING_TYPES = ["frame", "module", "gasblock", "penopolistirol"]

PROJECTS_DATA = [
    ("Отрада", "module", "Самарская обл., Отрада"),
    ("Муранка", "frame", "Самарская обл., Муранка"),
    ("Новосемейкино", "frame", "Самарская обл., пос. Новосемейкино"),
    ("Золотые пески", "module", "Самарская обл., Золотые пески"),
]

STAGES_DATA = [
    ("Фундамент", 1),
    ("Стены", 2),
    ("Кровля", 3),
    ("Окна", 4),
    ("Инженерка", 5),
    ("Отделка", 6),
]

CHAT_MESSAGES = [
    ("Добрый день! Этап фундамента завершён, жду подтверждения.", "chat"),
    ("Проверил фото — всё в порядке, можно переходить к стенам.", "chat"),
    ("Есть вопросы по срокам — когда планируется сдача?", "chat"),
]


def create_placeholder(filepath: Path) -> bool:
    """Создать placeholder (минимальный валидный JPEG)."""
    try:
        filepath.write_bytes(_PLACEHOLDER_JPEG)
        return True
    except Exception:
        return False


def download_image(url: str, filepath: Path) -> bool:
    """Скачать изображение по URL. При ошибке — placeholder."""
    if HAS_REQUESTS:
        try:
            r = requests.get(url, timeout=15, verify=False)
            if r.ok and len(r.content) > 100:
                filepath.write_bytes(r.content)
                return True
        except Exception:
            pass
    return create_placeholder(filepath)


def main():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Очистить старые проекты (для чистого seed)
    cur.execute("DELETE FROM edit_request_photos")
    cur.execute("DELETE FROM edit_requests")
    cur.execute("DELETE FROM project_chat_read")
    cur.execute("DELETE FROM project_chat")
    cur.execute("DELETE FROM reports")
    cur.execute("DELETE FROM stages")
    cur.execute("DELETE FROM projects")
    conn.commit()

    master_id = cur.execute("SELECT id FROM users WHERE role = 'master' LIMIT 1").fetchone()
    client_id = cur.execute("SELECT id FROM users WHERE role = 'client' LIMIT 1").fetchone()
    admin_id = cur.execute("SELECT id FROM users WHERE role = 'admin' LIMIT 1").fetchone()

    if not master_id or not client_id or not admin_id:
        print("Нужны пользователи: admin, master, client. Запустите приложение для init_db.")
        conn.close()
        return

    master_id = master_id[0]
    client_id = client_id[0]
    admin_id = admin_id[0]
    manager_op_row = cur.execute("SELECT id FROM users WHERE role = 'manager_op' LIMIT 1").fetchone()
    manager_op_id = manager_op_row[0] if manager_op_row else admin_id

    # Сценарии выполнения: просрочка, вовремя, досрочно
    GANTT_SCENARIOS = ["overdue", "on_time", "early"]
    GANTT_SCENARIOS_ON_TIME = ["on_time", "early"]  # только вовремя или досрочно

    # Все 4 проекта — этапы сданы в срок/досрочно
    ON_TIME_PROJECT_INDICES = {0, 1, 2, 3}
    COMPLETION_PATTERN = [6, 6, 6, 6]

    stage_cols = [r[1] for r in cur.execute("PRAGMA table_info(stages)").fetchall()]
    has_start_end = "planned_start_date" in stage_cols and "planned_end_date" in stage_cols

    print("Создание тестовых проектов...")
    for i, (name, btype, address) in enumerate(PROJECTS_DATA):
        # Скачать фото проекта
        photo_path = None
        img_url = f"https://picsum.photos/seed/proj{i+100}/400/300"
        filename = f"project_seed_{i+1}.jpg"
        filepath = UPLOAD_FOLDER / filename
        if download_image(img_url, filepath):
            photo_path = f"uploads/{filename}"
        n_to_complete = COMPLETION_PATTERN[i] if i < len(COMPLETION_PATTERN) else 3
        on_time_label = " (в срок/досрочно)" if i in ON_TIME_PROJECT_INDICES else ""
        print(f"  Проект {i+1}: {name} — выполнено этапов: {n_to_complete}/6{on_time_label}")

        cur.execute(
            """INSERT INTO projects (name, type, address, client_id, master_id, photo_path, created_by_id, responsible_manager_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, btype, address, client_id, master_id, photo_path, admin_id, manager_op_id),
        )
        project_id = cur.lastrowid

        rnd = random.Random(project_id * 1000 + i)
        base_date = date.today() - timedelta(days=90)

        # Этапы с planned_start_date и planned_end_date для всех проектов (для Ганта)
        stage_ids_with_planned = []
        prev_planned_end = base_date
        for si, (stage_name, order_num) in enumerate(STAGES_DATA):
            planned_start = prev_planned_end
            planned_end = prev_planned_end + timedelta(days=10 + rnd.randint(0, 5))
            prev_planned_end = planned_end
            if has_start_end:
                cur.execute(
                    """INSERT INTO stages (project_id, name, order_num, planned_date, planned_start_date, planned_end_date)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (project_id, stage_name, order_num, planned_end.isoformat(), planned_start.isoformat(), planned_end.isoformat()),
                )
            else:
                cur.execute(
                    "INSERT INTO stages (project_id, name, order_num, planned_date) VALUES (?, ?, ?, ?)",
                    (project_id, stage_name, order_num, planned_end.isoformat()),
                )
            stage_id = cur.lastrowid
            stage_ids_with_planned.append((stage_id, stage_name, planned_start, planned_end))

        # Выполненные этапы: первые n_to_complete этапов
        n_stages = len(stage_ids_with_planned)
        stages_to_complete = set(range(min(n_to_complete, n_stages)))

        scenarios_pool = GANTT_SCENARIOS_ON_TIME if i in ON_TIME_PROJECT_INDICES else GANTT_SCENARIOS
        for idx, (stage_id, stage_name, planned_start, planned_end) in enumerate(stage_ids_with_planned):
            if idx not in stages_to_complete:
                continue  # этап не выполнен — без отчётов
            scenario = rnd.choice(scenarios_pool)
            if scenario == "overdue":
                actual = planned_end + timedelta(days=rnd.randint(3, 15))
            elif scenario == "on_time":
                delta = (planned_end - planned_start).days
                actual = planned_start + timedelta(days=rnd.randint(0, max(0, delta)))
            else:
                actual = planned_start - timedelta(days=rnd.randint(5, 14))
            created_at = f"{actual.isoformat()} 12:00:00"
            rep_url = f"https://picsum.photos/seed/stage{project_id}{stage_id}/300/200"
            rep_fn = f"report_seed_{project_id}_{stage_id}_0.jpg"
            rep_fp = UPLOAD_FOLDER / rep_fn
            if download_image(rep_url, rep_fp):
                cur.execute(
                    "INSERT INTO reports (stage_id, photo_path, comment) VALUES (?, ?, ?)",
                    (stage_id, f"uploads/{rep_fn}", f"Фото этапа «{stage_name}»"),
                )
                cur.execute("UPDATE reports SET created_at = ? WHERE id = ?", (created_at, cur.lastrowid))

        # Заявки на редактирование (для проектов с выполненными этапами — по 1–2 заявки)
        if n_to_complete > 0:
            stage_ids = [r[0] for r in cur.execute("SELECT id FROM stages WHERE project_id = ? ORDER BY order_num", (project_id,)).fetchall()]
            reports = cur.execute("SELECT stage_id, photo_path FROM reports WHERE stage_id IN ({})".format(",".join("?" * len(stage_ids))), stage_ids).fetchall()
            photo_by_stage = {r[0]: r[1] for r in reports}
            for idx in range(1 if i % 2 == 0 else 2):
                sid = stage_ids[(project_id + i + idx) % len(stage_ids)]
                photo = photo_by_stage.get(sid)
                if not photo:
                    for s in stage_ids:
                        if photo_by_stage.get(s):
                            photo = photo_by_stage[s]
                            break
                if photo:
                    cur.execute(
                        "INSERT INTO edit_requests (stage_id, master_id, status) VALUES (?, ?, 'pending')",
                        (sid, master_id),
                    )
                    cur.execute(
                        "INSERT INTO edit_request_photos (edit_request_id, photo_path, comment) VALUES (?, ?, ?)",
                        (cur.lastrowid, photo, "Заявка на замену фото"),
                    )

        # Сообщения в чат
        first_stage_id = cur.execute("SELECT id FROM stages WHERE project_id = ? ORDER BY order_num LIMIT 1", (project_id,)).fetchone()[0]
        for j, (msg_text, msg_type) in enumerate(CHAT_MESSAGES[:2] if i % 2 == 0 else CHAT_MESSAGES[:3]):
            author = admin_id if j % 2 == 0 else master_id
            cur.execute(
                """INSERT INTO project_chat (project_id, user_id, message, msg_type, stage_id)
                   VALUES (?, ?, ?, ?, ?)""",
                (project_id, author, msg_text, msg_type, first_stage_id),
            )

    # Прорабы и тестовые отчёты
    print("Создание прорабов и тестовых отчётов...")
    cons_projects = [
        r[0] for r in cur.execute(
            "SELECT id FROM projects WHERE type IN ('frame', 'gasblock', 'penopolistirol') ORDER BY id"
        ).fetchall()
    ]
    if cons_projects:
        # Директор по строительству
        dc_row = cur.execute("SELECT id FROM users WHERE role = 'director_construction' LIMIT 1").fetchone()
        if not dc_row:
            cur.execute(
                """INSERT INTO users (username, password, role, full_name, phone)
                   VALUES (?, ?, 'director_construction', ?, '')""",
                ("director_cons", generate_password_hash("director123"), "Сергей Директоров"),
            )
            dc_id = cur.lastrowid
        else:
            dc_id = dc_row[0]

        # Прорабы
        foremen_data = [
            ("foreman1", "foreman123", "Алексей Прорабов"),
            ("foreman2", "foreman123", "Михаил Строев"),
        ]
        foreman_ids = []
        for username, pwd, full_name in foremen_data:
            row = cur.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if not row:
                cur.execute(
                    """INSERT INTO users (username, password, role, full_name, phone, reports_to_construction_id)
                       VALUES (?, ?, 'foreman', ?, '', ?)""",
                    (username, generate_password_hash(pwd), full_name, dc_id),
                )
                foreman_ids.append(cur.lastrowid)
            else:
                foreman_ids.append(row[0])
                cur.execute(
                    "UPDATE users SET reports_to_construction_id = ? WHERE id = ?",
                    (dc_id, row[0]),
                )

        # Назначение прорабов на объекты
        for fid in foreman_ids:
            for pid in cons_projects[:8]:  # каждый прораб на первые 8 строительных проектов
                cur.execute(
                    """INSERT OR IGNORE INTO foreman_project_access (foreman_id, project_id, assigned_by_id)
                       VALUES (?, ?, ?)""",
                    (fid, pid, admin_id),
                )

        # Тестовые отчёты прорабов
        work_item_ids = [r[0] for r in cur.execute(
            "SELECT id FROM work_items WHERE active = 1 ORDER BY id LIMIT 8"
        ).fetchall()]
        allowed_percents = [30.0, 60.0, 80.0, 100.0]
        today = date.today()
        rnd = random.Random(42)
        reports_created = 0
        for fid in foreman_ids:
            my_projects = [
                r[0] for r in cur.execute(
                    "SELECT project_id FROM foreman_project_access WHERE foreman_id = ?",
                    (fid,),
                ).fetchall()
            ]
            for pid in my_projects[:5]:
                for d in range(7):
                    report_date = today - timedelta(days=d)
                    cur.execute(
                        """INSERT OR IGNORE INTO worker_daily_reports (worker_id, project_id, report_date)
                           VALUES (?, ?, ?)""",
                        (fid, pid, report_date.isoformat()),
                    )
                    if cur.rowcount:
                        reports_created += 1
                    dr_id = cur.execute(
                        "SELECT id FROM worker_daily_reports WHERE worker_id = ? AND project_id = ? AND report_date = ?",
                        (fid, pid, report_date.isoformat()),
                    ).fetchone()[0]
                    items_count = rnd.randint(2, min(5, len(work_item_ids)))
                    for wi_id in rnd.sample(work_item_ids, items_count):
                        pct = rnd.choice(allowed_percents)
                        cur.execute(
                            """INSERT OR IGNORE INTO worker_daily_report_items
                               (daily_report_id, work_item_id, percent, comment, approved_status)
                               VALUES (?, ?, ?, ?, 'approved')""",
                            (dr_id, wi_id, pct, "Тестовый отчёт" if d == 0 else None),
                        )
        print(f"  Создано отчётов прорабов: {reports_created}")

    # Привязка директора по производству bocharov: все мастера и работники
    user_cols = [r[1] for r in cur.execute("PRAGMA table_info(users)").fetchall()]
    if "reports_to_production_id" in user_cols:
        bocharov = cur.execute("SELECT id FROM users WHERE username = 'bocharov'").fetchone()
        if not bocharov:
            cur.execute(
                """INSERT INTO users (username, password, role, full_name, phone)
                   VALUES (?, ?, 'director_production', ?, '')""",
                ("bocharov", generate_password_hash("bocharov123"), "Бочаров (дир. производства)"),
            )
            bocharov_id = cur.lastrowid
        else:
            bocharov_id = bocharov[0]
        cur.execute(
            "UPDATE users SET reports_to_production_id = ? WHERE role IN ('master', 'worker')",
            (bocharov_id,),
        )
        print("  Директор по производству bocharov: мастера и работники привязаны")

    # Ответственный ОП Александр: все проекты назначаются на него
    proj_cols = [r[1] for r in cur.execute("PRAGMA table_info(projects)").fetchall()]
    if "responsible_manager_id" in proj_cols:
        alexander = cur.execute(
            """SELECT id FROM users WHERE role = 'manager_op'
               AND (username LIKE '%alex%' OR full_name LIKE '%лександр%' OR full_name LIKE '%Alexander%')
               ORDER BY id LIMIT 1"""
        ).fetchone()
        if not alexander:
            alexander = cur.execute(
                "SELECT id FROM users WHERE username = 'alexander' AND role = 'manager_op'"
            ).fetchone()
        if not alexander:
            cur.execute(
                """INSERT INTO users (username, password, role, full_name, phone)
                   VALUES (?, ?, 'manager_op', ?, '')""",
                ("alexander", generate_password_hash("alexander123"), "Александр (менеджер ОП)"),
            )
            alexander_id = cur.lastrowid
        else:
            alexander_id = alexander[0]
        cur.execute("UPDATE projects SET responsible_manager_id = ?", (alexander_id,))
        print("  Ответственный ОП: все проекты назначены на Александра")

    conn.commit()
    conn.close()
    print("Готово! 15 проектов созданы с этапами, фото, чатом и отчётами прорабов.")


if __name__ == "__main__":
    main()
