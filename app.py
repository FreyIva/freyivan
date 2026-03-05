"""
StroyControl — Строительный ERP
Основной файл приложения Flask
Версия: 1.5.10
"""

__version__ = "1.5.10"

import os
import sqlite3
import csv
import io
import hashlib
import mimetypes
import secrets
import shutil
import json
import socket
import zipfile
import random
from datetime import datetime, date, timedelta, timezone
from functools import wraps
from pathlib import Path
from xml.sax.saxutils import escape as xml_escape

from flask import (
    Flask,
    Response,
    abort,
    redirect,
    render_template,
    request,
    session,
    send_file,
    url_for,
    flash,
)
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


def normalize_phone(phone: str | None) -> str:
    """Приводит телефон к формату хранения: 79033019009 (только цифры, 11 знаков)."""
    if not phone:
        return ""
    digits = "".join(c for c in str(phone).strip() if c.isdigit())
    if not digits:
        return ""
    if len(digits) == 10 and digits[0] in "789":
        digits = "7" + digits
    elif len(digits) == 11 and digits[0] == "8":
        digits = "7" + digits[1:]
    elif len(digits) == 11 and digits[0] != "7":
        return ""
    return digits if len(digits) == 11 and digits[0] == "7" else ""


def format_phone(phone: str | None) -> str:
    """Форматирует телефон для отображения: +7 (903) 301-90-09."""
    normalized = normalize_phone(phone)
    if not normalized or len(normalized) != 11:
        return str(phone or "").strip() or ""
    return f"+7 ({normalized[1:4]}) {normalized[4:7]}-{normalized[7:9]}-{normalized[9:11]}"


def format_date_dmy(value) -> str:
    """Формат даты по проекту: dd.mm.yy (например 01.02.26)."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%d.%m.%y")
    if isinstance(value, date):
        return value.strftime("%d.%m.%y")
    s = str(value).strip()
    if not s:
        return ""
    # поддержка ISO 'YYYY-MM-DD' и 'YYYY-MM-DD HH:MM:SS'
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            y = int(s[0:4])
            m = int(s[5:7])
            d = int(s[8:10])
            return date(y, m, d).strftime("%d.%m.%y")
    except Exception:
        pass
    return s


# Проектное время: Самара (UTC+4)
PROJECT_TZ = timezone(timedelta(hours=4), name="Samara")


def project_now() -> datetime:
    """Текущее время проекта в часовом поясе Самары."""
    return datetime.now(timezone.utc).astimezone(PROJECT_TZ)


def format_project_dt(value) -> str:
    """Форматирует дату/время в часовом поясе Самары."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if not s:
            return ""
        dt = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except Exception:
                continue
        if dt is None:
            return s
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PROJECT_TZ).strftime("%d.%m.%Y %H:%M:%S")


def _first_name_from_full_name(full_name: str | None) -> str:
    s = (full_name or "").strip()
    if not s:
        return "коллега"
    return s.split()[0]


def _ru_holidays_for_day(d: date) -> list[dict]:
    """Возвращает список праздников РФ на дату (official / informal)."""
    y = int(d.year)
    m = int(d.month)
    day = int(d.day)

    res: list[dict] = []

    # Официальные (основные государственные)
    if m == 1 and 1 <= day <= 8:
        res.append({"name": "Новогодние каникулы", "kind": "official"})
    if m == 1 and day == 7:
        res.append({"name": "Рождество Христово", "kind": "official"})
    if m == 2 and day == 23:
        res.append({"name": "День защитника Отечества", "kind": "official"})
    if m == 3 and day == 8:
        res.append({"name": "Международный женский день", "kind": "official"})
    if m == 5 and day == 1:
        res.append({"name": "Праздник Весны и Труда", "kind": "official"})
    if m == 5 and day == 9:
        res.append({"name": "День Победы", "kind": "official"})
    if m == 6 and day == 12:
        res.append({"name": "День России", "kind": "official"})
    if m == 11 and day == 4:
        res.append({"name": "День народного единства", "kind": "official"})

    # Популярные неофициальные (без претензии на полный календарь)
    fixed_informal = {
        (1, 25): "День студента (Татьянин день)",
        (2, 14): "День всех влюблённых",
        (3, 1): "День кошек в России",
        (4, 12): "День космонавтики",
        (7, 8): "День семьи, любви и верности",
        (9, 1): "День знаний",
        (10, 5): "День учителя",
    }
    nm = fixed_informal.get((m, day))
    if nm:
        res.append({"name": nm, "kind": "informal"})

    # День программиста — 256-й день года
    if d == (date(y, 1, 1) + timedelta(days=255)):
        res.append({"name": "День программиста", "kind": "informal"})

    # День строителя — 2-е воскресенье августа
    if m == 8:
        first = date(y, 8, 1)
        first_sun = first + timedelta(days=(6 - first.weekday()) % 7)
        second_sun = first_sun + timedelta(days=7)
        if d == second_sun:
            res.append({"name": "День строителя", "kind": "informal"})

    # День медицинского работника — 3-е воскресенье июня
    if m == 6:
        first = date(y, 6, 1)
        first_sun = first + timedelta(days=(6 - first.weekday()) % 7)
        third_sun = first_sun + timedelta(days=14)
        if d == third_sun:
            res.append({"name": "День медицинского работника", "kind": "informal"})

    # День матери — последнее воскресенье ноября (в РФ)
    if m == 11:
        last = date(y, 11, 30)
        last_sun = last - timedelta(days=(last.weekday() - 6) % 7)
        if d == last_sun:
            res.append({"name": "День матери", "kind": "informal"})

    # День отца — 3-е воскресенье октября (в РФ)
    if m == 10:
        first = date(y, 10, 1)
        first_sun = first + timedelta(days=(6 - first.weekday()) % 7)
        third_sun = first_sun + timedelta(days=14)
        if d == third_sun:
            res.append({"name": "День отца", "kind": "informal"})

    # убрать дубли по имени
    seen = set()
    out = []
    for it in res:
        key = (it.get("kind"), it.get("name"))
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def _holiday_congrats(d: date) -> str | None:
    items = _ru_holidays_for_day(d)
    if not items:
        return None
    official = [x["name"] for x in items if x.get("kind") == "official"]
    informal = [x["name"] for x in items if x.get("kind") == "informal"]

    parts = []
    if official:
        parts.append(f"С государственным праздником: {', '.join(official)}!")
    if informal:
        parts.append(f"Также сегодня {', '.join(informal)} — поздравляем!")
    return " ".join(parts).strip() or None


def _time_bucket(dt: datetime) -> str:
    h = int(dt.hour)
    if 5 <= h <= 11:
        return "morning"
    if 12 <= h <= 16:
        return "day"
    if 17 <= h <= 22:
        return "evening"
    return "night"


def _generate_greeting(*, name: str, dt: datetime, role: str | None) -> tuple[str, str]:
    """Возвращает (id, message) для анти-повтора подряд."""
    bucket = _time_bucket(dt)
    salutations = {
        "morning": ["Доброе утро", "С добрым утром", "Прекрасного утра"],
        "day": ["Добрый день", "Здравствуйте", "Рады вас видеть", "Хорошего дня"],
        "evening": ["Добрый вечер", "Здравствуйте", "Рады вас видеть"],
        "night": ["Здравствуйте", "Доброй ночи", "Рады вас видеть"],
    }[bucket]

    sal = secrets.choice(salutations)
    is_client = (role or "").strip() == "client"
    if is_client:
        msg = f"{sal}, {name}!"
        hid = f"{bucket}|{sal}|client"
    else:
        official_warm = [
            "Желаем продуктивной и спокойной работы.",
            "Пусть сегодня всё складывается ровно и по плану.",
            "Пусть задачи решаются быстро и без лишней суеты.",
            "Пусть день будет результативным и приятным.",
            "Спасибо за вашу работу — будем на связи.",
            "Пусть будет больше ясности и меньше срочных правок.",
        ]
        micro = [
            "Если понадобится помощь — пишите.",
            "Хорошего настроя и уверенного темпа.",
            "Пусть всё получится с первого раза.",
            "Пусть будет хороший прогресс по объектам.",
            "Пусть на стройке и в цифрах будет порядок.",
        ]
        w1 = secrets.choice(official_warm)
        w2 = secrets.choice(micro)
        msg = f"{sal}, {name}! {w1} {w2}"
        hid = f"{bucket}|{sal}|{w1}|{w2}"

    congrats = _holiday_congrats(dt.date())
    if congrats:
        msg = f"{msg} {congrats}"
        hid = f"{hid}|{congrats}"
    return hid, msg


def inject_dynamic_greeting():
    """Приветствие на каждой странице для авторизованного пользователя."""
    if "user_id" not in session:
        return {}

    # Показываем приветствие только один раз после входа (на первую страницу сессии)
    if session.get("greet_shown"):
        return {}

    # Имя: из сессии (быстро) или из БД (фолбэк)
    full_name = session.get("full_name")
    if not full_name:
        conn = get_db()
        try:
            row = conn.execute("SELECT full_name FROM users WHERE id = ?", (session["user_id"],)).fetchone()
            full_name = row["full_name"] if row else None
        finally:
            conn.close()
        if full_name:
            session["full_name"] = full_name

    name = _first_name_from_full_name(full_name)
    dt = project_now()
    role = session.get("role")

    last_id = session.get("greet_last_id")
    gid = None
    msg = None
    for _ in range(12):
        gid, msg = _generate_greeting(name=name, dt=dt, role=role)
        if gid != last_id:
            break
    if gid and msg:
        session["greet_last_id"] = gid
        session["greet_shown"] = True
        return {"header_greeting": msg}
    return {}

# Диапазоны выполнения работ (сохраняем в БД верхнюю границу)
PERCENT_RANGES = [
    (10, 20),
    (30, 40),
    (50, 60),
    (70, 80),
    (90, 100),
]
ALLOWED_PERCENT_ENDS = [end for _, end in PERCENT_RANGES]

# Дней без активности ответственного — после этого проект можно запросить на взятие
TAKEOVER_INACTIVE_DAYS = 7

# Конфигурация приложения
app = Flask(__name__)
app.secret_key = os.urandom(24)

# Регистрируем контекст-процессоры, определённые выше
app.context_processor(inject_dynamic_greeting)
app.add_template_filter(format_date_dmy, name="dmy")
app.add_template_filter(format_phone, name="phone")


@app.context_processor
def inject_current_role():
    """Глобальные данные о текущей роли для шаблонов."""
    role_key = session.get("role")
    role_label = ROLES.get(role_key, role_key) if role_key else None
    # Человекочитаемый сегмент "виртуального" пути для хлебных крошек/заголовков
    role_slug_map = {
        "admin": "admin",
        "manager_op": "sales",
        "marketer": "marketing",
        "rop": "ROP",
        "director_production": "production",
        "director_construction": "construction",
        "master": "master",
        "foreman": "foreman",
        "worker": "worker",
        "client": "client",
    }
    role_slug = role_slug_map.get(role_key or "", role_key or "guest")
    return {
        "current_role": role_key,
        "current_role_label": role_label,
        "current_role_slug": role_slug,
    }

app.config["UPLOAD_FOLDER"] = Path(__file__).parent / "static" / "uploads"
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10 MB
ALLOWED_EXTENSIONS = {"jpg", "jpeg", "png"}

# Создаём папку для загрузок если её нет
app.config["UPLOAD_FOLDER"].mkdir(parents=True, exist_ok=True)

# Корень защищённого хранения медиа (внутри проекта, вне static/)
STORAGE_ROOT = Path(__file__).parent / "storage"
STORAGE_ROOT.mkdir(parents=True, exist_ok=True)

# Путь к базе данных
DATABASE = Path(__file__).parent / "database.db"
MEDIA_DB = Path(__file__).parent / "media.db"

# Виды справочника работ
WORK_ITEM_TYPES = {
    "production": "Производство",
    "construction": "Участок",
}

# Типы строений для выпадающих списков
BUILDING_TYPES = {
    "frame": "Каркасный",
    "module": "Модульный",
    "gasblock": "Газоблок",
    "penopolistirol": "Пенополистиролбетон",
}

# Роли пользователей: админ видит всё; директор по производству — заявки от мастера;
# директор по строительству — заявки и отчёты от прораба; работник отчитывается перед мастером
ROLES = {
    "admin": "Администратор",
    "manager_op": "Менеджер ОП (отдел продаж)",
    "rop": "РОП (руководитель отдела продаж)",
    "marketer": "Маркетолог",
    "director_construction": "Директор по строительству",
    "director_production": "Директор по производству",
    "foreman": "Прораб",
    "master": "Мастер",
    "worker": "Работник",
    "client": "Заказчик",
}

ACCESS_MODULES = [
    {"key": "reports", "label": "Отчеты", "sort_order": 10},
    {"key": "amocrm", "label": "amoCRM отчеты", "sort_order": 20},
    {"key": "worker_reports", "label": "Отчеты работников", "sort_order": 30},
    {"key": "foreman_reports", "label": "Отчеты прорабов", "sort_order": 40},
    {"key": "tv_dashboard", "label": "TV дашборд", "sort_order": 50},
]

ACCESS_PAGES = [
    {"key": "worker_reports", "module_key": "worker_reports", "label": "Отчеты работников", "route_name": "worker_reports", "sort_order": 10},
    {"key": "foreman_reports", "module_key": "foreman_reports", "label": "Отчеты прорабов", "route_name": "foreman_reports", "sort_order": 20},
    {"key": "amocrm_leads", "module_key": "amocrm", "label": "amoCRM: Потенциальные клиенты", "route_name": "amocrm_leads_report", "sort_order": 30},
    {"key": "amocrm_projects", "module_key": "amocrm", "label": "amoCRM: Проекты Sherwood Home", "route_name": "amocrm_projects_report", "sort_order": 40},
    {"key": "amocrm_sources", "module_key": "amocrm", "label": "amoCRM: Источники лидов", "route_name": "amocrm_sources_report", "sort_order": 50},
    {"key": "amocrm_tv", "module_key": "tv_dashboard", "label": "amoCRM: Единый TV дашборд", "route_name": "amocrm_sources_dashboard", "sort_order": 60},
]

ACCESS_MODULE_KEYS = {m["key"] for m in ACCESS_MODULES}
ACCESS_PAGE_KEYS = {p["key"] for p in ACCESS_PAGES}
ACCESS_PAGE_BY_KEY = {p["key"]: p for p in ACCESS_PAGES}
ACCESS_PAGES_REQUIRE_REPORTS = ACCESS_PAGE_KEYS.copy()

ROLE_DEFAULT_MODULE_ACCESS = {
    "admin": {"reports", "amocrm", "worker_reports", "foreman_reports", "tv_dashboard"},
    "manager_op": {"reports", "amocrm", "tv_dashboard"},
    "rop": {"reports", "amocrm", "tv_dashboard"},
    "marketer": {"reports", "amocrm", "tv_dashboard"},
    "director_production": {"reports", "worker_reports"},
    "master": {"reports", "worker_reports"},
    "director_construction": {"reports", "foreman_reports"},
    "foreman": set(),
    "worker": set(),
    "client": set(),
}

ROLE_DEFAULT_PAGE_ACCESS = {
    "admin": {"worker_reports", "foreman_reports", "amocrm_leads", "amocrm_projects", "amocrm_sources", "amocrm_tv"},
    "manager_op": {"amocrm_leads", "amocrm_projects", "amocrm_sources", "amocrm_tv"},
    "rop": {"amocrm_leads", "amocrm_projects", "amocrm_sources", "amocrm_tv"},
    "marketer": {"amocrm_leads", "amocrm_projects", "amocrm_sources", "amocrm_tv"},
    "director_production": {"worker_reports"},
    "master": {"worker_reports"},
    "director_construction": {"foreman_reports"},
    "foreman": set(),
    "worker": set(),
    "client": set(),
}

# Справочник работ (seed из предоставленного прайс-листа; можно расширять в БД)
WORK_ITEMS_SEED = [
    "Изготовление балок длиннее 6м (8-9м) из фанеры, за 1шт",
    "Сборка основание и установка по уровню",
    "Сборка силового каркаса, с укосинами",
    "Монтаж допонительной перегородки",
    "Утепление основание, покрытие пленками",
    "Изготовление обрешетки и покрытие черновых полов",
    "Формирование уколна под слив в парной",
    "Монтаж оцинк поддона в парной, 2 листа",
    "Монтаж шпунтованной половой доски, 1 м2",
    "Монтаж внутренних перегородок после полов, 1 пог.м",
    "Мона внутреи пленок с проклейко",
    "Утепление каркаса",
    "Обшивка пленками наружняя с проклейкой",
    "Монтаж контр абрешеток внутри",
    "Монтаж усиления полков бани в парной",
    "Монтаж контр обрешеток снаружи, 4 стороны",
    "Монтаж усиления панорамных окно, стена 3м пог",
    "Скрытый монтаж эл. кабелей без гофры (с гофр 2596)",
    "Скрытый монтаж водопровода с закладными под болер",
    "Изготовление заготовок имм бруса на внешнюю отделку (одна стена 6м)",
    "Покраска имм бруса на внешнюю отделку 1 слой (одна стена 6м)",
    "Монтаж внешней иммитации бруса (одна стена 6м) с вырезом проемов",
    "Покраска имм бруса на внешнюю отделку 2 слой (одна стена 6м)",
    "Монтаж профлиста на стену с подрезкой 3 пог.м.",
    "Монтаж профлиста на стену",
    "Монтаж профлиста на кровлю",
    "Монтаж угловых элементов",
    "Изготовление заготовок имм бруса на внутренюю отделку",
    "Покраска имм бруса на внутренюю отделку 1 слой",
    "Монтаж иммитации бруса внутри дома с вырезкой проемов",
    "Покраска имм бруса на внутренюю отделку 2 слой",
    "Установка печи в парную с обшивкой минеритом",
    "Установка дымохода с противопожарным порталом",
    "Изготовления заготовок на каркас полков, 2,5м пог 2 уровня",
    "Монтаж каркаса полков, 2,5м пог 2 уровня",
    "Монтаж полков 2,5м пог 2 уровня",
    "Обшивка зоны душа листовым материалом",
    "Наклейка пластиковых панелей в зоне душа",
    "Установка входной двери",
    "Установка окон ПВХ 1800х2000",
    "Установка окон ПВХ 900х2000",
    "Установка окон ПВХ 1000х600, 600х600",
    "Установка крепежа маскит сеток, 1 проем",
    "Монтаж отливов окон, 1 окно",
    "Изготовление заготовок доборов и наличников окон и двери с внешней стороны, 1 окно/дверь",
    "Покраска доборов и наличников окон и двери с внешней стороны, 1 окно/дверь",
    "Монтаж доборов и наличников окон и двери с внешней стороны, 1 окно/дверь",
    "Установка межкомнатной двери с врезкой замков, 1 шт",
    "Изготовление заготовок внутренних доборов, 1 окно/дверь",
    "Покраска внутренних доборов, 1 окно/дверь",
    "Монтаж внутренних доборов, 1 окно/дверь",
    "Устройство пленочного теплого пола со штраблением, 1 комн",
    "Монтаж подложки под ламинат и кварцвинил, 1 мод",
    "Монтаж ламината и кварцвинила, 1 мод",
    "Покраска наличников внутренних, 1 окно/дверь",
    "Монтаж наличников внутренних, 1 окно/дверь",
    "Монтаж плинтусов, 1 комн",
    "Покраска элементов внутренних углов, 1 мод",
    "Монтаж элементов внутренних углов, 1 мод",
    "Монтаж светильников, розеток, терморег и щитка",
    "Устройство вытяжной вентиляции, за 1 шт",
    "Устройство отверстий канализации и ввода воды",
    "Сборка труб канализации",
    "Упаковка модуля",
    "Перемещение на хранение",
    "Изготовение элементов лестницы 6 ступ, 1,5м шир",
    "Сборка лестницы 6 ступ, 1,5м шир",
    "Изготовление элементов открытой террасы, 6м2",
    "Покраска элементов открытой террасы, 5м2",
    "Изготовление элементов закрытой террасы, 6м2",
    "Покраска и обработка элементов закрытой террасы, 6м2",
    "Монтаж элементов закрытой террасы, 6м2",
    "Изготовление и покраска реек и перил ограждения, 1м2",
    "Монтаж реек ограждения, 1 м2",
    "Монтаж модуля на участке со сваями и лестницей",
    "Дополнительные работы",
]

# Прайс-лист (ч/ч, стоимость часа, стоимость работы). Стоимость работы = ч/ч * стоимость часа.
WORK_ITEMS_PRICE_LIST = [
    ("Изготовление заготовок на силовой поддон и антисептирование", 3, 350, 1050),
    ("Изготовление балок длиннее 6м (8-9м) из фанеры, за 1шт", 3, 350, 1050),
    ("Сборка основание и установка по уровню", 7, 400, 2800),
    ("Изготовление заготовок на силовой каркас и антисептирование", 9, 350, 3150),
    ("Сборка силового каркаса, с укосинами", 17, 400, 6800),
    ("Монтаж допонительной перегородки", 3, 400, 1200),
    ("Утепление основание, покрытие пленками", 3, 350, 1050),
    ("Изготовление обрешетки и покрытие черновых полов", 9, 350, 3150),
    ("Формирование уколна под слив в парной", 3, 400, 1200),
    ("Монтаж оцинк поддона в парной, 2 листа", 6, 400, 2400),
    ("Монтаж шпунтованной половой доски, 1 м2", 1, 400, 400),
    ("Монтаж внутренних перегородок после полов, 1 пог.м", 2, 400, 800),
    ("Монтаж внутренних пленок с проклейкой", 8, 350, 2800),
    ("Утепление каркаса", 12, 350, 4200),
    ("Обшивка пленками наружняя с проклейкой", 6, 350, 2100),
    ("Монтаж контр обрешеток внутри", 6, 350, 2100),
    ("Монтаж усиления полков бани в парной", 3, 400, 1200),
    ("Монтаж контр обрешеток снаружи, 4 стороны", 6, 350, 2100),
    ("Монтаж усиления панорамных окно, стена 3м пог", 6, 350, 2100),
    ("Скрытый монтаж эл. кабелей без гофры (с гофр 25%)", 6, 400, 2400),
    ("Скрытый монтаж водопровода с закладными под болер", 6, 400, 2400),
    ("Изготовление заготовок имм бруса на внешнюю отделку (одна стена 6м)", 2, 350, 700),
    ("Покраска имм бруса на внешнюю отделку 1 слой (одна стена 6м)", 4, 300, 1200),
    ("Монтаж внешней иммитации бруса (одна стена 6м) с вырезом поемов", 6, 350, 2100),
    ("Покраска имм бруса на внешнюю отделку 2 слой (одна стена 6м)", 3, 300, 900),
    ("Монтаж профлиста на стену с подрезкой 3 пог.м.", 3, 350, 1050),
    ("Монтаж профлиста на стену", 3, 350, 1050),
    ("Монтаж профлиста на кровлю", 4, 350, 1400),
    ("Монтаж угловых элементов", 3, 350, 1050),
    ("Изготовление заготовок имм бруса на внутренюю отделку", 8, 350, 2800),
    ("Покраска имм бруса на внутренюю отделку 1 слой", 12, 300, 3600),
    ("Монтаж иммитации бруса внутри дома с вырезкой проемов", 16, 350, 5600),
    ("Покраска имм бруса на внутренюю отделку 2 слой", 6, 300, 1800),
    ("Установка печи в парную с обшивкой минеритом", 8, 400, 3200),
    ("Установка дымохода с противопожарным порталом", 6, 400, 2400),
    ("Изготовления заготовок на каркас полков, 2,5м пог 2 уровня", 2, 400, 800),
    ("Монтаж каркаса полков, 2,5м пог 2 уровня", 4, 400, 1600),
    ("Монтаж полков  2,5м пог 2 уровня", 5, 400, 2000),
    ("Обшивка зоны душа листовым материалом", 4, 400, 1600),
    ("Наклейка пластиковых панелей в зоне душа", 4, 400, 1600),
    ("Установка входной двери", 3, 400, 1200),
    ("Установка окон ПВХ 1800х2000", 4.5, 400, 1800),
    ("Установка окон ПВХ 900х2000", 3, 400, 1200),
    ("Установка окон ПВХ 1000х600, 600х600", 1.5, 400, 600),
    ("Установка крепежа маскит сеток, 1 проем", 0.5, 350, 175),
    ("Монтаж отливов окон, 1 окно", 0.5, 350, 175),
    ("Изготовление заготовок доборов и наличников окон и двери с внешней стороны, 1 окно/дверь", 1, 200, 200),
    ("Покраска доборов и наличников окон и двери с внешней стороны, 1 окно/дверь", 0.5, 400, 200),
    ("Монтаж доборов и наличников окон и двери с внешней стороны, 1 окно/дверь", 0.5, 400, 200),
    ("Установка межкомнатной двери с врезкой замков 1 шт", 6, 400, 2400),
    ("Изготовление заготовок внутренних доборов, 1 окно/дверь", 1, 350, 350),
    ("Покраска внутренних доборов, 1 окно/дверь", 0.8, 300, 240),
    ("Монтаж внутренних доборов, 1 окно/дверь", 1, 350, 350),
    ("Устойство пленочного теплого пола со штраблением, 1 комн", 3, 400, 1200),
    ("Монтаж подложки под ламинат и кварцвинил, 1 мод", 1, 300, 300),
    ("Монтаж ламината и кварцвинила, 1 мод", 12, 350, 4200),
    ("Покраска наличников внутренних, 1 окно/дверь", 1, 300, 300),
    ("Монтаж наличников внутренних, 1 окно/дверь", 0.5, 350, 175),
    ("Монтаж плитусов, 1 комн", 1.5, 300, 450),
    ("Покраска элементов внутренних углов, 1 мод", 2, 300, 600),
    ("Монтаж элементов внутренних углов, 1 мод", 3, 350, 1050),
    ("Монтаж светильников, розеток, терморег и щитка", 8, 400, 3200),
    ("Устройство вытяжной вентиляции, за 1 шт", 3, 400, 1200),
    ("Устройство отверстий канализации и ввода воды", 1.5, 350, 525),
    ("Сборка труб канализации", 2.5, 350, 875),
    ("Упаковка модуля", 8, 350, 2800),
    ("Перемещение на хранение", 4, 350, 1400),
    ("Изготовение элеменов лестницы 6 ступ 1,5м шир", 2, 400, 800),
    ("Покраска элементов лестницы 6 ступ 1,5м шир", 2, 300, 600),
    ("Сборка лестницы 6 ступ 1,5м шир", 2, 350, 700),
    ("Изготовление элементов открытой террасы, 6м2", 3, 350, 1050),
    ("Покраска элементов открытой террасы, 6м2", 4, 300, 1200),
    ("Изготовление элементов закрытой террасы, 6м2", 8, 400, 3200),
    ("Покраска и обработка элементов закрытой террасы, 6м2", 6, 300, 1800),
    ("Монтаж элементов закрытой террасы, 6м2", 36, 400, 14400),
    ("Изготовление и покраска реек и перил ограждения, 1м2", 3, 300, 900),
    ("Монтаж реек ограждения, 1 м2", 2, 400, 800),
    ("Монтаж модуля на участке со стандарт терр и лестницей", 24, 400, 9600),
]


def get_db():
    """Получить соединение с БД (для каждого запроса своё)"""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # Результаты как словари
    return conn


def get_media_db():
    """Получить соединение с media.db (для каждого запроса своё)."""
    conn = sqlite3.connect(MEDIA_DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_media_db():
    """Инициализация media.db: таблица метаданных файлов."""
    conn = get_media_db()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS media_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            stage_id INTEGER,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            stored_relpath TEXT NOT NULL,
            original_filename TEXT,
            mime_type TEXT,
            size_bytes INTEGER,
            sha256 TEXT,
            uploaded_by_id INTEGER,
            uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            public_token TEXT NOT NULL UNIQUE,
            is_archived INTEGER NOT NULL DEFAULT 0 CHECK (is_archived IN (0, 1)),
            archived_at DATETIME,
            archived_by_id INTEGER,
            archive_reason TEXT
        )
    """)

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_files_project_id ON media_files(project_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_files_stage_id ON media_files(stage_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_files_entity ON media_files(entity_type, entity_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_files_archived ON media_files(is_archived)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_files_uploaded_at ON media_files(uploaded_at)"
    )

    conn.commit()
    conn.close()


def _posix_relpath(path: Path) -> str:
    # stored_relpath должен быть одинаковым на всех ОС
    return path.as_posix().lstrip("/")


def _storage_rel_dir_for(
    entity_type: str,
    *,
    project_id: int,
    stage_id: int | None = None,
    edit_request_id: int | None = None,
) -> Path:
    if entity_type == "project_photo":
        return Path("projects") / str(int(project_id)) / "project"
    if entity_type == "project_contract":
        return Path("projects") / str(int(project_id)) / "contract"
    if entity_type == "project_estimate":
        return Path("projects") / str(int(project_id)) / "estimate"
    if entity_type == "stage_report":
        if not stage_id:
            raise ValueError("stage_id is required for stage_report")
        return (
            Path("projects")
            / str(int(project_id))
            / "stages"
            / str(int(stage_id))
            / "reports"
        )
    if entity_type == "edit_request_photo":
        if not stage_id or not edit_request_id:
            raise ValueError("stage_id and edit_request_id are required for edit_request_photo")
        return (
            Path("projects")
            / str(int(project_id))
            / "stages"
            / str(int(stage_id))
            / "edit_requests"
            / str(int(edit_request_id))
        )
    raise ValueError(f"Unknown entity_type: {entity_type}")


def _internal_media_filename(*, uploaded_by_id: int | None, original_filename: str) -> str:
    ext = ""
    if original_filename and "." in original_filename:
        ext = "." + original_filename.rsplit(".", 1)[1].lower()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rand8 = secrets.token_hex(4)
    uid = int(uploaded_by_id) if uploaded_by_id else 0
    return f"u{uid}_{ts}_{rand8}{ext}"


def _write_stream_to_file_and_hash(stream, dst_path: Path) -> tuple[str, int]:
    sha = hashlib.sha256()
    size = 0
    with open(dst_path, "wb") as f:
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
            sha.update(chunk)
            size += len(chunk)
    return sha.hexdigest(), size


def save_media_file(
    file_storage,
    *,
    project_id: int,
    stage_id: int | None,
    entity_type: str,
    entity_id: int,
    uploaded_by_id: int | None,
    rel_dir: Path,
) -> str:
    """Сохраняет файл в STORAGE_ROOT и создаёт запись в media.db. Возвращает public_token."""
    rel_dir = Path(rel_dir)
    dest_dir = STORAGE_ROOT / rel_dir
    dest_dir.mkdir(parents=True, exist_ok=True)

    original_filename = (getattr(file_storage, "filename", None) or "").strip()
    internal_name = _internal_media_filename(
        uploaded_by_id=uploaded_by_id, original_filename=original_filename
    )
    dest_path = dest_dir / internal_name

    # читаем поток и считаем хеш/размер
    stream = getattr(file_storage, "stream", None)
    if stream is None:
        raise ValueError("file_storage.stream is required")
    try:
        stream.seek(0)
    except Exception:
        pass
    sha256, size_bytes = _write_stream_to_file_and_hash(stream, dest_path)
    try:
        stream.seek(0)
    except Exception:
        pass

    mime_type = (getattr(file_storage, "mimetype", None) or "").strip() or None
    if not mime_type:
        guessed, _ = mimetypes.guess_type(dest_path.name)
        mime_type = guessed or None

    stored_relpath = _posix_relpath(rel_dir / internal_name)

    media_conn = get_media_db()
    try:
        for _ in range(8):
            token = secrets.token_urlsafe(32)
            try:
                media_conn.execute(
                    """INSERT INTO media_files
                       (project_id, stage_id, entity_type, entity_id, stored_relpath,
                        original_filename, mime_type, size_bytes, sha256, uploaded_by_id, public_token)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        int(project_id),
                        int(stage_id) if stage_id else None,
                        entity_type,
                        int(entity_id),
                        stored_relpath,
                        original_filename or None,
                        mime_type,
                        int(size_bytes),
                        sha256,
                        int(uploaded_by_id) if uploaded_by_id else None,
                        token,
                    ),
                )
                media_conn.commit()
                return token
            except sqlite3.IntegrityError:
                # collision по UNIQUE token — пробуем другой
                continue
        raise RuntimeError("Failed to generate unique media token")
    finally:
        media_conn.close()


def save_media_file_from_path(
    src_path: Path,
    *,
    project_id: int,
    stage_id: int | None,
    entity_type: str,
    entity_id: int,
    uploaded_by_id: int | None,
    rel_dir: Path,
    original_filename: str | None = None,
    mime_type: str | None = None,
) -> str:
    """Копирует существующий файл в STORAGE_ROOT и создаёт запись в media.db."""
    src_path = Path(src_path)
    if not src_path.exists() or not src_path.is_file():
        raise FileNotFoundError(str(src_path))

    rel_dir = Path(rel_dir)
    dest_dir = STORAGE_ROOT / rel_dir
    dest_dir.mkdir(parents=True, exist_ok=True)

    original_filename = (original_filename or src_path.name).strip()
    internal_name = _internal_media_filename(
        uploaded_by_id=uploaded_by_id, original_filename=original_filename
    )
    dest_path = dest_dir / internal_name

    # copy2 сохранит mtime; потом пересчитаем хеш/размер
    shutil.copy2(src_path, dest_path)
    with open(dest_path, "rb") as f:
        sha256 = hashlib.sha256(f.read()).hexdigest()
    size_bytes = int(dest_path.stat().st_size)

    if not mime_type:
        guessed, _ = mimetypes.guess_type(dest_path.name)
        mime_type = guessed or None

    stored_relpath = _posix_relpath(rel_dir / internal_name)

    media_conn = get_media_db()
    try:
        for _ in range(8):
            token = secrets.token_urlsafe(32)
            try:
                media_conn.execute(
                    """INSERT INTO media_files
                       (project_id, stage_id, entity_type, entity_id, stored_relpath,
                        original_filename, mime_type, size_bytes, sha256, uploaded_by_id, public_token)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        int(project_id),
                        int(stage_id) if stage_id else None,
                        entity_type,
                        int(entity_id),
                        stored_relpath,
                        original_filename or None,
                        mime_type,
                        int(size_bytes),
                        sha256,
                        int(uploaded_by_id) if uploaded_by_id else None,
                        token,
                    ),
                )
                media_conn.commit()
                return token
            except sqlite3.IntegrityError:
                continue
        raise RuntimeError("Failed to generate unique media token")
    finally:
        media_conn.close()


def archive_media(entity_type: str, entity_id: int, *, by_user_id: int | None, reason: str | None):
    media_conn = get_media_db()
    try:
        media_conn.execute(
            """UPDATE media_files
               SET is_archived = 1,
                   archived_at = CURRENT_TIMESTAMP,
                   archived_by_id = ?,
                   archive_reason = ?
               WHERE entity_type = ? AND entity_id = ? AND is_archived = 0""",
            (
                int(by_user_id) if by_user_id else None,
                (reason or "").strip() or None,
                entity_type,
                int(entity_id),
            ),
        )
        media_conn.commit()
    finally:
        media_conn.close()


def archive_media_by_project(project_id: int, *, by_user_id: int | None, reason: str | None):
    media_conn = get_media_db()
    try:
        media_conn.execute(
            """UPDATE media_files
               SET is_archived = 1,
                   archived_at = CURRENT_TIMESTAMP,
                   archived_by_id = ?,
                   archive_reason = ?
               WHERE project_id = ? AND is_archived = 0""",
            (
                int(by_user_id) if by_user_id else None,
                (reason or "").strip() or None,
                int(project_id),
            ),
        )
        media_conn.commit()
    finally:
        media_conn.close()


def archive_media_by_stage(stage_id: int, *, by_user_id: int | None, reason: str | None):
    media_conn = get_media_db()
    try:
        media_conn.execute(
            """UPDATE media_files
               SET is_archived = 1,
                   archived_at = CURRENT_TIMESTAMP,
                   archived_by_id = ?,
                   archive_reason = ?
               WHERE stage_id = ? AND is_archived = 0""",
            (
                int(by_user_id) if by_user_id else None,
                (reason or "").strip() or None,
                int(stage_id),
            ),
        )
        media_conn.commit()
    finally:
        media_conn.close()


def get_active_media_tokens_map(entity_type: str, entity_ids: list[int]) -> dict[int, str]:
    ids = [int(x) for x in (entity_ids or []) if x]
    if not ids:
        return {}
    media_conn = get_media_db()
    try:
        q = "SELECT entity_id, public_token FROM media_files WHERE entity_type = ? AND is_archived = 0 AND entity_id IN ({})".format(
            ",".join(["?"] * len(ids))
        )
        rows = media_conn.execute(q, (entity_type, *ids)).fetchall()
        return {int(r["entity_id"]): str(r["public_token"]) for r in rows}
    finally:
        media_conn.close()


def _current_user_role(conn, user_id: int) -> str | None:
    row = conn.execute("SELECT role FROM users WHERE id = ?", (int(user_id),)).fetchone()
    return row["role"] if row else None


def can_user_access_project(conn, *, user_id: int, role: str, project_id: int) -> bool:
    pid = int(project_id)
    uid = int(user_id)
    if role == "admin":
        return True
    if role == "master":
        return (
            conn.execute("SELECT 1 FROM projects WHERE id = ? AND master_id = ?", (pid, uid)).fetchone()
            is not None
        )
    if role == "client":
        return (
            conn.execute("SELECT 1 FROM projects WHERE id = ? AND client_id = ?", (pid, uid)).fetchone()
            is not None
        )
    if role == "worker":
        return (
            conn.execute(
                "SELECT 1 FROM worker_project_access WHERE worker_id = ? AND project_id = ?",
                (uid, pid),
            ).fetchone()
            is not None
        )
    if role == "foreman":
        return (
            conn.execute(
                "SELECT 1 FROM foreman_project_access WHERE foreman_id = ? AND project_id = ?",
                (uid, pid),
            ).fetchone()
            is not None
        )
    if role == "director_production":
        # проекты мастеров направления (как в director_production_dashboard)
        return (
            conn.execute(
                """SELECT 1
                   FROM projects p
                   JOIN users m ON m.id = p.master_id
                   WHERE p.id = ? AND m.role = 'master' AND m.reports_to_production_id = ?""",
                (pid, uid),
            ).fetchone()
            is not None
        )
    if role == "director_construction":
        # проекты, где назначен этот директор ИЛИ есть прораб его направления
        proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
        if "director_construction_id" in proj_cols:
            proj = conn.execute(
                "SELECT director_construction_id FROM projects WHERE id = ?", (pid,)
            ).fetchone()
            if proj and proj.get("director_construction_id") == uid:
                return True
        return (
            conn.execute(
                """SELECT 1
                   FROM foreman_project_access fpa
                   JOIN users u ON u.id = fpa.foreman_id
                   WHERE fpa.project_id = ?
                     AND u.role = 'foreman'
                     AND u.reports_to_construction_id = ?""",
                (pid, uid),
            ).fetchone()
            is not None
        )
    return False


def _touch_project_responsible_activity(conn, project_id: int, user_id: int):
    """Обновить last_responsible_activity_at, если user_id — ответственный менеджер проекта"""
    proj = conn.execute(
        "SELECT responsible_manager_id FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    if proj and proj["responsible_manager_id"] == user_id:
        conn.execute(
            "UPDATE projects SET last_responsible_activity_at = CURRENT_TIMESTAMP WHERE id = ?",
            (project_id,),
        )


@app.route("/media/<token>")
def media_file(token):
    """Защищённая выдача файла по public_token."""
    media_conn = get_media_db()
    try:
        mf = media_conn.execute(
            "SELECT * FROM media_files WHERE public_token = ?",
            ((token or "").strip(),),
        ).fetchone()
    finally:
        media_conn.close()

    if not mf:
        abort(404)

    user_id = session.get("user_id")
    if not user_id:
        abort(403)

    conn = get_db()
    try:
        role = session.get("role") or _current_user_role(conn, int(user_id))
        if not role:
            abort(403)

        if int(mf["is_archived"] or 0) == 1 and role != "admin":
            abort(404)

        if not can_user_access_project(
            conn, user_id=int(user_id), role=str(role), project_id=int(mf["project_id"])
        ):
            abort(403)
    finally:
        conn.close()

    stored_relpath = (mf["stored_relpath"] or "").strip()
    if not stored_relpath or stored_relpath.startswith(("/", "\\")) or ".." in stored_relpath:
        abort(404)
    fpath = (STORAGE_ROOT / stored_relpath).resolve()
    if not str(fpath).startswith(str(STORAGE_ROOT.resolve())) or not fpath.exists():
        abort(404)

    mime_type = (mf["mime_type"] or "").strip() or None
    return send_file(fpath, mimetype=mime_type, conditional=True)


def _csv_response_utf8(content_text: str, filename: str):
    """Отдаёт CSV в UTF-8 (с BOM для корректного открытия в Excel)."""
    payload = (content_text or "").encode("utf-8-sig")
    return Response(
        payload,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def _xlsx_col_letter(idx: int) -> str:
    out = ""
    n = int(idx)
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out or "A"


def _xlsx_cell_xml(row_idx: int, col_idx: int, value) -> str:
    ref = f"{_xlsx_col_letter(col_idx)}{row_idx}"
    if value is None:
        return f'<c r="{ref}"/>'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"><v>{value}</v></c>'
    text = xml_escape(str(value))
    return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>'


def _xlsx_response(rows: list[list], filename: str):
    """Отдаёт простой XLSX (1 лист, без формул/стилей)."""
    sheet_rows = []
    for r_idx, row in enumerate(rows, start=1):
        cells = "".join(_xlsx_cell_xml(r_idx, c_idx, val) for c_idx, val in enumerate(row, start=1))
        sheet_rows.append(f'<row r="{r_idx}">{cells}</row>')
    sheet_data = "".join(sheet_rows)

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Источники лидов" sheetId="1" r:id="rId1"/></sheets></workbook>'
    )
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f"<sheetData>{sheet_data}</sheetData></worksheet>"
    )
    rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/></Relationships>'
    )
    wb_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/></Relationships>'
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>"
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types_xml)
        zf.writestr("_rels/.rels", rels_xml)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", wb_rels_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)

    payload = buf.getvalue()
    return Response(
        payload,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def _normalize_work_item_name(name: str) -> str:
    name = (name or "").strip()
    name = " ".join(name.split())
    return name.casefold()


def _rebuild_work_item_codes(conn):
    rows = conn.execute(
        "SELECT id FROM work_items ORDER BY name COLLATE NOCASE ASC, id ASC"
    ).fetchall()
    for idx, r in enumerate(rows, start=1):
        conn.execute("UPDATE work_items SET code = ? WHERE id = ?", (idx, int(r["id"])))


def _dedupe_work_items(conn):
    """Удаляет дубли по нормализованному названию, сохраняя один элемент и перенося ссылки."""
    items = [dict(r) for r in conn.execute("SELECT id, name FROM work_items").fetchall()]
    groups = {}
    for it in items:
        key = _normalize_work_item_name(it.get("name") or "")
        if not key:
            continue
        groups.setdefault(key, []).append(it)

    deleted = 0
    merged = 0
    for key, g in groups.items():
        if len(g) <= 1:
            continue
        g.sort(key=lambda x: int(x["id"]))
        keep_id = int(g[0]["id"])
        for dup in g[1:]:
            dup_id = int(dup["id"])
            # переносим ссылки в закрытиях дней
            conn.execute(
                "UPDATE worker_daily_report_items SET work_item_id = ? WHERE work_item_id = ?",
                (keep_id, dup_id),
            )
            conn.execute("DELETE FROM work_items WHERE id = ?", (dup_id,))
            deleted += 1
            merged += 1
    return {"deleted": deleted, "merged": merged}


def _seed_access_catalog(conn):
    for mod in ACCESS_MODULES:
        conn.execute(
            """INSERT OR IGNORE INTO access_modules_catalog
               (module_key, label, sort_order, is_active)
               VALUES (?, ?, ?, 1)""",
            (mod["key"], mod["label"], int(mod.get("sort_order") or 0)),
        )
    for page in ACCESS_PAGES:
        conn.execute(
            """INSERT OR IGNORE INTO access_pages_catalog
               (page_key, module_key, label, route_name, sort_order, is_active)
               VALUES (?, ?, ?, ?, ?, 1)""",
            (
                page["key"],
                page["module_key"],
                page["label"],
                page["route_name"],
                int(page.get("sort_order") or 0),
            ),
        )


def _catalog_modules(conn):
    try:
        rows = conn.execute(
            """SELECT module_key, label, sort_order, is_active
               FROM access_modules_catalog
               ORDER BY sort_order, label"""
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return [
            {
                "module_key": m["key"],
                "label": m["label"],
                "sort_order": int(m.get("sort_order") or 0),
                "is_active": 1,
            }
            for m in ACCESS_MODULES
        ]


def _catalog_pages(conn):
    try:
        rows = conn.execute(
            """SELECT page_key, module_key, label, route_name, sort_order, is_active
               FROM access_pages_catalog
               ORDER BY sort_order, label"""
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return [
            {
                "page_key": p["key"],
                "module_key": p["module_key"],
                "label": p["label"],
                "route_name": p.get("route_name"),
                "sort_order": int(p.get("sort_order") or 0),
                "is_active": 1,
            }
            for p in ACCESS_PAGES
        ]


def _role_default_permissions(role: str | None):
    r = str(role or "").strip()
    modules_allowed = set(ROLE_DEFAULT_MODULE_ACCESS.get(r, set()))
    pages_allowed = set(ROLE_DEFAULT_PAGE_ACCESS.get(r, set()))
    modules_map = {k: (k in modules_allowed) for k in ACCESS_MODULE_KEYS}
    pages_map = {k: (k in pages_allowed) for k in ACCESS_PAGE_KEYS}
    return modules_map, pages_map


def _effective_user_permissions(conn, user_id: int, role: str | None):
    modules_map, pages_map = _role_default_permissions(role)

    try:
        for r in conn.execute(
            "SELECT module_key, is_allowed FROM user_module_access WHERE user_id = ?",
            (int(user_id),),
        ).fetchall():
            k = str(r["module_key"] or "").strip()
            if k in modules_map:
                modules_map[k] = bool(int(r["is_allowed"] or 0))

        for r in conn.execute(
            "SELECT page_key, is_allowed FROM user_page_access WHERE user_id = ?",
            (int(user_id),),
        ).fetchall():
            k = str(r["page_key"] or "").strip()
            if k in pages_map:
                pages_map[k] = bool(int(r["is_allowed"] or 0))
    except sqlite3.OperationalError:
        # Если БД ещё не мигрирована под модульные права, работаем на ролевых дефолтах.
        pass

    return {"modules": modules_map, "pages": pages_map}


def _is_page_access_allowed(permissions: dict, page_key: str) -> bool:
    page_def = ACCESS_PAGE_BY_KEY.get(page_key)
    if not page_def:
        return False
    modules = permissions.get("modules") or {}
    pages = permissions.get("pages") or {}
    if page_key in ACCESS_PAGES_REQUIRE_REPORTS and not modules.get("reports", False):
        return False
    if not modules.get(page_def["module_key"], False):
        return False
    return bool(pages.get(page_key, False))


def init_db():
    """Инициализация БД: создание таблиц и тестовых данных"""
    conn = get_db()
    cursor = conn.cursor()

    # Таблица пользователей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            full_name TEXT NOT NULL,
            phone TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Таблица проектов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            address TEXT NOT NULL,
            client_id INTEGER NOT NULL,
            master_id INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES users (id),
            FOREIGN KEY (master_id) REFERENCES users (id)
        )
    """)

    # Таблица этапов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            order_num INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
        )
    """)

    # Таблица отчётов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stage_id INTEGER NOT NULL,
            photo_path TEXT NOT NULL,
            comment TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (stage_id) REFERENCES stages (id) ON DELETE CASCADE
        )
    """)

    # Таблица заявок на редактирование этапа
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edit_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stage_id INTEGER NOT NULL,
            master_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (stage_id) REFERENCES stages (id) ON DELETE CASCADE,
            FOREIGN KEY (master_id) REFERENCES users (id)
        )
    """)

    # Фото в заявке на редактирование (новые, которые заменят старые после одобрения)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS edit_request_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            edit_request_id INTEGER NOT NULL,
            photo_path TEXT NOT NULL,
            comment TEXT,
            FOREIGN KEY (edit_request_id) REFERENCES edit_requests (id) ON DELETE CASCADE
        )
    """)

    # Доступ работников к проектам (мастер открывает конкретные объекты работникам)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS worker_project_access (
            worker_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (worker_id, project_id),
            FOREIGN KEY (worker_id) REFERENCES users (id) ON DELETE CASCADE,
            FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
        )
    """)

    # Назначения объектов прорабам (директор по строительству)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS foreman_project_access (
            foreman_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            assigned_by_id INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (foreman_id, project_id),
            FOREIGN KEY (foreman_id) REFERENCES users (id) ON DELETE CASCADE,
            FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
            FOREIGN KEY (assigned_by_id) REFERENCES users (id)
        )
    """)

    # Справочник работ
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS work_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code INTEGER,
            name TEXT UNIQUE NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            labor_hours REAL NOT NULL DEFAULT 0,
            hour_price REAL NOT NULL DEFAULT 0,
            work_cost REAL NOT NULL DEFAULT 0,
            unit_price REAL NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Закрытие рабочего дня: шапка (1 запись на день на проект)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS worker_daily_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            worker_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            report_date DATE NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(worker_id, project_id, report_date),
            FOREIGN KEY (worker_id) REFERENCES users (id) ON DELETE CASCADE,
            FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE
        )
    """)

    # Строки закрытия дня: какие работы и какой процент закрыт
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS worker_daily_report_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            daily_report_id INTEGER NOT NULL,
            work_item_id INTEGER NOT NULL,
            percent REAL NOT NULL,
            comment TEXT,
            approved_status TEXT NOT NULL DEFAULT 'approved',
            approved_by_id INTEGER,
            approved_at DATETIME,
            UNIQUE(daily_report_id, work_item_id),
            FOREIGN KEY (daily_report_id) REFERENCES worker_daily_reports (id) ON DELETE CASCADE,
            FOREIGN KEY (work_item_id) REFERENCES work_items (id),
            FOREIGN KEY (approved_by_id) REFERENCES users (id)
        )
    """)

    # Чат проекта (только админ и мастер, клиент не имеет доступа)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS project_chat (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            msg_type TEXT DEFAULT 'chat',
            stage_id INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (stage_id) REFERENCES stages (id) ON DELETE SET NULL
        )
    """)
    # Миграция: добавить stage_id если таблица создана раньше без него
    cursor.execute("PRAGMA table_info(project_chat)")
    cols = [row[1] for row in cursor.fetchall()]
    if "stage_id" not in cols:
        try:
            cursor.execute("ALTER TABLE project_chat ADD COLUMN stage_id INTEGER REFERENCES stages(id)")
        except sqlite3.OperationalError:
            pass

    # Миграция: добавить photo_path в projects
    cursor.execute("PRAGMA table_info(projects)")
    cols = [row[1] for row in cursor.fetchall()]
    if "photo_path" not in cols:
        try:
            cursor.execute("ALTER TABLE projects ADD COLUMN photo_path TEXT")
        except sqlite3.OperationalError:
            pass
    if "created_by_id" not in cols:
        try:
            cursor.execute("ALTER TABLE projects ADD COLUMN created_by_id INTEGER REFERENCES users(id)")
        except sqlite3.OperationalError:
            pass
    if "responsible_manager_id" not in cols:
        try:
            cursor.execute("ALTER TABLE projects ADD COLUMN responsible_manager_id INTEGER REFERENCES users(id)")
        except sqlite3.OperationalError:
            pass
    # Назначить ответственного ОП для проектов без него
    first_op = cursor.execute(
        "SELECT id FROM users WHERE role = 'manager_op' ORDER BY id LIMIT 1"
    ).fetchone()
    if first_op:
        cursor.execute(
            "UPDATE projects SET responsible_manager_id = ? WHERE responsible_manager_id IS NULL",
            (first_op[0],),
        )
    cursor.execute("PRAGMA table_info(projects)")
    proj_cols2 = [r[1] for r in cursor.fetchall()]
    if "last_responsible_activity_at" not in proj_cols2:
        try:
            cursor.execute("ALTER TABLE projects ADD COLUMN last_responsible_activity_at DATETIME")
        except sqlite3.OperationalError:
            pass

    # Таблица заявок на взятие проекта (делегирование)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS project_takeover_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            requester_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            reviewed_by_id INTEGER,
            reviewed_at DATETIME,
            admin_comment TEXT,
            FOREIGN KEY (project_id) REFERENCES projects (id) ON DELETE CASCADE,
            FOREIGN KEY (requester_id) REFERENCES users (id),
            FOREIGN KEY (reviewed_by_id) REFERENCES users (id)
        )
    """)

    # Миграция: добавить planned_date в stages (плановая дата этапа, видна только админу)
    cursor.execute("PRAGMA table_info(stages)")
    stage_cols = [row[1] for row in cursor.fetchall()]
    if "planned_date" not in stage_cols:
        try:
            cursor.execute("ALTER TABLE stages ADD COLUMN planned_date DATE")
        except sqlite3.OperationalError:
            pass
    if "planned_start_date" not in stage_cols:
        try:
            cursor.execute("ALTER TABLE stages ADD COLUMN planned_start_date DATE")
        except sqlite3.OperationalError:
            pass
    if "planned_end_date" not in stage_cols:
        try:
            cursor.execute("ALTER TABLE stages ADD COLUMN planned_end_date DATE")
        except sqlite3.OperationalError:
            pass
    if "stage_confirmed_by_id" not in stage_cols:
        try:
            cursor.execute("ALTER TABLE stages ADD COLUMN stage_confirmed_by_id INTEGER REFERENCES users(id)")
        except sqlite3.OperationalError:
            pass
    if "stage_confirmed_at" not in stage_cols:
        try:
            cursor.execute("ALTER TABLE stages ADD COLUMN stage_confirmed_at DATETIME")
        except sqlite3.OperationalError:
            pass

    # Заполнить planned_start_date и planned_end_date для этапов без них (тестовые данные)
    def _parse_d(s):
        if not s:
            return None
        try:
            return date(*[int(x) for x in str(s)[:10].split("-")])
        except (ValueError, TypeError):
            return None
    cursor.execute(
        """SELECT s.id, s.project_id, s.order_num, s.planned_date, s.planned_start_date, s.planned_end_date
           FROM stages s ORDER BY s.project_id, s.order_num"""
    )
    all_stages = cursor.fetchall()
    base_date = date.today() - timedelta(days=60)
    prev_end_by_project = {}
    for row in all_stages:
        pid = row["project_id"]
        prev_end = prev_end_by_project.get(pid, base_date)
        need_update = row["planned_start_date"] is None or row["planned_end_date"] is None
        if need_update:
            planned_end = _parse_d(row["planned_date"]) or (prev_end + timedelta(days=10))
            planned_start = prev_end
            cursor.execute(
                "UPDATE stages SET planned_start_date = ?, planned_end_date = ? WHERE id = ?",
                (planned_start.isoformat(), planned_end.isoformat(), row["id"]),
            )
            prev_end_by_project[pid] = planned_end
        else:
            pe = _parse_d(row["planned_end_date"])
            if pe:
                prev_end_by_project[pid] = pe

    # Миграция: договор и смета проекта (подтверждение клиентом)
    cursor.execute("PRAGMA table_info(projects)")
    proj_cols = [row[1] for row in cursor.fetchall()]
    if "contract_confirmed_at" not in proj_cols:
        try:
            cursor.execute("ALTER TABLE projects ADD COLUMN contract_confirmed_at DATETIME")
        except sqlite3.OperationalError:
            pass
    if "estimate_confirmed_at" not in proj_cols:
        try:
            cursor.execute("ALTER TABLE projects ADD COLUMN estimate_confirmed_at DATETIME")
        except sqlite3.OperationalError:
            pass
    if "contract_admin_approved_at" not in proj_cols:
        try:
            cursor.execute("ALTER TABLE projects ADD COLUMN contract_admin_approved_at DATETIME")
        except sqlite3.OperationalError:
            pass
    if "estimate_admin_approved_at" not in proj_cols:
        try:
            cursor.execute("ALTER TABLE projects ADD COLUMN estimate_admin_approved_at DATETIME")
        except sqlite3.OperationalError:
            pass
    # Миграция: для уже подтверждённых заказчиком — считать одобренными админом
    cursor.execute(
        """UPDATE projects SET contract_admin_approved_at = contract_confirmed_at
           WHERE contract_confirmed_at IS NOT NULL AND contract_admin_approved_at IS NULL"""
    )
    cursor.execute(
        """UPDATE projects SET estimate_admin_approved_at = estimate_confirmed_at
           WHERE estimate_confirmed_at IS NOT NULL AND estimate_admin_approved_at IS NULL"""
    )
    # Миграция: директор по строительству для проектов frame, gasblock, penopolistirol
    proj_cols_dc = [r[1] for r in cursor.execute("PRAGMA table_info(projects)").fetchall()]
    if "director_construction_id" not in proj_cols_dc:
        try:
            cursor.execute(
                "ALTER TABLE projects ADD COLUMN director_construction_id INTEGER REFERENCES users(id)"
            )
        except sqlite3.OperationalError:
            pass
    # Миграция: завершение проекта (скрывает с дашборда, остаётся в списке с пометкой)
    proj_cols_completed = [r[1] for r in cursor.execute("PRAGMA table_info(projects)").fetchall()]
    if "completed_at" not in proj_cols_completed:
        try:
            cursor.execute(
                "ALTER TABLE projects ADD COLUMN completed_at DATETIME"
            )
        except sqlite3.OperationalError:
            pass

    # Таблица уведомлений (согласование, заявки и т.п.)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            notification_type TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT,
            link_url TEXT,
            project_id INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            read_at DATETIME,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (project_id) REFERENCES projects (id)
        )
    """)

    # Миграция: добавить unit_price в work_items (ставка за 100% выполнения)
    cursor.execute("PRAGMA table_info(work_items)")
    cols = [row[1] for row in cursor.fetchall()]
    if "code" not in cols:
        try:
            cursor.execute("ALTER TABLE work_items ADD COLUMN code INTEGER")
        except sqlite3.OperationalError:
            pass
    if "unit_price" not in cols:
        try:
            cursor.execute("ALTER TABLE work_items ADD COLUMN unit_price REAL NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
    if "labor_hours" not in cols:
        try:
            cursor.execute("ALTER TABLE work_items ADD COLUMN labor_hours REAL NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
    if "hour_price" not in cols:
        try:
            cursor.execute("ALTER TABLE work_items ADD COLUMN hour_price REAL NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
    if "work_cost" not in cols:
        try:
            cursor.execute("ALTER TABLE work_items ADD COLUMN work_cost REAL NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
    if "work_item_type" not in cols:
        try:
            cursor.execute("ALTER TABLE work_items ADD COLUMN work_item_type TEXT NOT NULL DEFAULT 'production'")
            cursor.execute("UPDATE work_items SET work_item_type = 'production' WHERE work_item_type IS NULL OR work_item_type = ''")
        except sqlite3.OperationalError:
            pass

    # amoCRM: настройки интеграции (subdomain, access_token и т.д.)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amocrm_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amocrm_leads_cache (
            cache_key TEXT PRIMARY KEY,
            data_json TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amocrm_sources_weekly_manual (
            week_start TEXT PRIMARY KEY,
            meetings_total INTEGER,
            comment_text TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amocrm_sources_monthly_manual (
            month_start TEXT PRIMARY KEY,
            plan_budget REAL,
            fact_budget REAL,
            plan_leads INTEGER,
            plan_qual_leads INTEGER,
            plan_cpl REAL,
            plan_cpql REAL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amocrm_sources_monthly_manual_by_source (
            month_start TEXT NOT NULL,
            source_name TEXT NOT NULL,
            plan_budget REAL,
            fact_budget REAL,
            plan_leads INTEGER,
            plan_qual_leads INTEGER,
            plan_cpl REAL,
            plan_cpql REAL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (month_start, source_name)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amocrm_sales_monthly_plan (
            month_start TEXT PRIMARY KEY,
            plan_deals INTEGER,
            plan_amount REAL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amocrm_modules_yearly_plan (
            year INTEGER NOT NULL,
            category_key TEXT NOT NULL,
            plan_units REAL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (year, category_key)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS access_modules_catalog (
            module_key TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS access_pages_catalog (
            page_key TEXT PRIMARY KEY,
            module_key TEXT NOT NULL,
            label TEXT NOT NULL,
            route_name TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (module_key) REFERENCES access_modules_catalog (module_key) ON DELETE CASCADE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_module_access (
            user_id INTEGER NOT NULL,
            module_key TEXT NOT NULL,
            is_allowed INTEGER NOT NULL DEFAULT 1,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, module_key),
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
            FOREIGN KEY (module_key) REFERENCES access_modules_catalog (module_key) ON DELETE CASCADE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_page_access (
            user_id INTEGER NOT NULL,
            page_key TEXT NOT NULL,
            is_allowed INTEGER NOT NULL DEFAULT 1,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, page_key),
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
            FOREIGN KEY (page_key) REFERENCES access_pages_catalog (page_key) ON DELETE CASCADE
        )
    """)
    _seed_access_catalog(conn)

    # Заполнить code если пусто (по алфавиту)
    try:
        cursor.execute("SELECT COUNT(*) FROM work_items WHERE code IS NULL OR code = 0")
        need = int(cursor.fetchone()[0] or 0)
        if need > 0:
            _rebuild_work_item_codes(conn)
    except sqlite3.OperationalError:
        pass

    # Миграция: is_superadmin — главный админ (1) или обычный админ (0)
    cursor.execute("PRAGMA table_info(users)")
    cols = [row[1] for row in cursor.fetchall()]
    if "is_superadmin" not in cols:
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN is_superadmin INTEGER DEFAULT 0")
            cursor.execute("UPDATE users SET is_superadmin = 1 WHERE role = 'admin' LIMIT 1")
        except sqlite3.OperationalError:
            pass
    if "reports_to_id" not in cols:
        try:
            cursor.execute("ALTER TABLE users ADD COLUMN reports_to_id INTEGER REFERENCES users(id)")
        except sqlite3.OperationalError:
            pass

    # Миграции: подчинение по направлениям
    if "reports_to_production_id" not in cols:
        try:
            cursor.execute(
                "ALTER TABLE users ADD COLUMN reports_to_production_id INTEGER REFERENCES users(id)"
            )
        except sqlite3.OperationalError:
            pass
    if "reports_to_construction_id" not in cols:
        try:
            cursor.execute(
                "ALTER TABLE users ADD COLUMN reports_to_construction_id INTEGER REFERENCES users(id)"
            )
        except sqlite3.OperationalError:
            pass

    # Миграции: кто назначил доступ рабочему к проекту
    cursor.execute("PRAGMA table_info(worker_project_access)")
    wpa_cols = [row[1] for row in cursor.fetchall()]
    if "assigned_by_id" not in wpa_cols:
        try:
            cursor.execute(
                "ALTER TABLE worker_project_access ADD COLUMN assigned_by_id INTEGER REFERENCES users(id)"
            )
        except sqlite3.OperationalError:
            pass
    if "assigned_by_role" not in wpa_cols:
        try:
            cursor.execute("ALTER TABLE worker_project_access ADD COLUMN assigned_by_role TEXT")
        except sqlite3.OperationalError:
            pass

    # Миграции: подтверждение выполнения работ (закрытие дня)
    cursor.execute("PRAGMA table_info(worker_daily_report_items)")
    wdri_cols = [row[1] for row in cursor.fetchall()]
    if "approved_status" not in wdri_cols:
        try:
            cursor.execute("ALTER TABLE worker_daily_report_items ADD COLUMN approved_status TEXT NOT NULL DEFAULT 'approved'")
            # старые записи считаем подтверждёнными
            cursor.execute("UPDATE worker_daily_report_items SET approved_status = 'approved' WHERE approved_status IS NULL OR approved_status = ''")
        except sqlite3.OperationalError:
            pass
    if "approved_by_id" not in wdri_cols:
        try:
            cursor.execute("ALTER TABLE worker_daily_report_items ADD COLUMN approved_by_id INTEGER REFERENCES users(id)")
        except sqlite3.OperationalError:
            pass
    if "approved_at" not in wdri_cols:
        try:
            cursor.execute("ALTER TABLE worker_daily_report_items ADD COLUMN approved_at DATETIME")
        except sqlite3.OperationalError:
            pass

    # Настройки пользователей (сохраняются между сессиями)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INTEGER NOT NULL,
            pref_key TEXT NOT NULL,
            pref_value TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, pref_key),
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    """)

    # Таблица для отслеживания прочитанных сообщений (мастер)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS project_chat_read (
            user_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            last_read_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, project_id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (project_id) REFERENCES projects (id)
        )
    """)

    # Миграция: привести телефоны к формату 79033019009
    for row in cursor.execute("SELECT id, phone FROM users WHERE phone IS NOT NULL AND phone != ''").fetchall():
        uid, old_phone = row[0], row[1]
        new_phone = normalize_phone(old_phone)
        if new_phone and new_phone != old_phone:
            cursor.execute("UPDATE users SET phone = ? WHERE id = ?", (new_phone, uid))

    # Проверяем, есть ли уже пользователи
    cursor.execute("SELECT COUNT(*) FROM users")
    if cursor.fetchone()[0] == 0:
        # Создаём тестовых пользователей
        users_data = [
            ("admin", generate_password_hash("admin123"), "admin", "Главный администратор", ""),
            ("master", generate_password_hash("master123"), "master", "Иван Мастеров", "79991112233"),
            ("client", generate_password_hash("client123"), "client", "Пётр Клиентов", "79994445566"),
            ("manager_op", generate_password_hash("manager123"), "manager_op", "Ольга Менеджерова", "79001234567"),
        ]
        for u in users_data:
            cursor.execute(
                "INSERT INTO users (username, password, role, full_name, phone) VALUES (?, ?, ?, ?, ?)",
                u,
            )
        cursor.execute("UPDATE users SET is_superadmin = 1 WHERE username = 'admin'")

        # Получаем ID мастера, клиента и менеджера ОП
        cursor.execute("SELECT id FROM users WHERE username = 'master'")
        master_id = cursor.fetchone()[0]
        cursor.execute("SELECT id FROM users WHERE username = 'client'")
        client_id = cursor.fetchone()[0]
        cursor.execute("SELECT id FROM users WHERE username = 'manager_op'")
        manager_op_id = cursor.fetchone()[0]

        # Создаём тестовый проект
        cursor.execute(
            """INSERT INTO projects (name, type, address, client_id, master_id, created_by_id, responsible_manager_id) 
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("Дом на Пушкина, 10", "frame", "г. Москва, ул. Пушкина, д. 10", client_id, master_id, manager_op_id, manager_op_id),
        )
        project_id = cursor.lastrowid

        # Создаём этапы для тестового проекта
        stages_data = [
            ("Фундамент", 1),
            ("Стены", 2),
            ("Кровля", 3),
            ("Окна", 4),
            ("Инженерка", 5),
            ("Отделка", 6),
        ]
        for name, order_num in stages_data:
            cursor.execute(
                "INSERT INTO stages (project_id, name, order_num) VALUES (?, ?, ?)",
                (project_id, name, order_num),
            )

    # Супер-аккаунт: роль админа подчиняется суперу, в интерфейсе не отображается
    super_row = cursor.execute("SELECT id FROM users WHERE username = 'super'").fetchone()
    if not super_row:
        cursor.execute(
            """INSERT INTO users (username, password, role, full_name, phone, is_superadmin)
               VALUES (?, ?, 'admin', ?, '', 1)""",
            ("super", generate_password_hash("super123"), "Администратор"),
        )
        cursor.execute("UPDATE users SET is_superadmin = 0 WHERE username = 'admin'")
    else:
        # Миграция: если super есть — admin подчиняется
        cursor.execute("UPDATE users SET is_superadmin = 0 WHERE username = 'admin'")
        cursor.execute("UPDATE users SET is_superadmin = 1 WHERE username = 'super'")
        cursor.execute("UPDATE users SET full_name = 'Администратор' WHERE username = 'super'")

    # Seed справочника работ (один раз)
    cursor.execute("SELECT COUNT(*) FROM work_items")
    if cursor.fetchone()[0] == 0:
        # Сначала — прайс-лист с нормированными значениями
        for name, labor, hour_price, work_cost in WORK_ITEMS_PRICE_LIST:
            try:
                cursor.execute(
                    """INSERT INTO work_items (name, labor_hours, hour_price, work_cost, unit_price, active)
                       VALUES (?, ?, ?, ?, ?, 1)""",
                    (name, float(labor), float(hour_price), float(work_cost), float(work_cost)),
                )
            except sqlite3.IntegrityError:
                pass
        # Затем — остальные работы (если есть) без нормирования
        for name in WORK_ITEMS_SEED:
            try:
                cursor.execute(
                    "INSERT INTO work_items (name, active) VALUES (?, 1)",
                    (name,),
                )
            except sqlite3.IntegrityError:
                pass

    # Ответственный ОП Александр: назначить все проекты на него
    proj_cols = [r[1] for r in cursor.execute("PRAGMA table_info(projects)").fetchall()]
    if "responsible_manager_id" in proj_cols:
        alexander = cursor.execute(
            """SELECT id FROM users WHERE role = 'manager_op'
               AND (username LIKE '%alex%' OR full_name LIKE '%лександр%' OR full_name LIKE '%Alexander%')
               ORDER BY id LIMIT 1"""
        ).fetchone()
        if not alexander:
            alexander = cursor.execute(
                "SELECT id FROM users WHERE username = 'alexander' AND role = 'manager_op'"
            ).fetchone()
        if not alexander:
            cursor.execute(
                """INSERT INTO users (username, password, role, full_name, phone)
                   VALUES (?, ?, 'manager_op', ?, '')""",
                ("alexander", generate_password_hash("alexander123"), "Александр (менеджер ОП)"),
            )
            alexander_id = cursor.lastrowid
        else:
            alexander_id = alexander[0]
        cursor.execute("UPDATE projects SET responsible_manager_id = ?", (alexander_id,))

    conn.commit()
    conn.close()


def login_required(f):
    """Декоратор: требуется авторизация"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated


def admin_required(f):
    """Декоратор: требуется роль администратора"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] != "admin":
            flash("Доступ запрещён.", "error")
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated


def projects_manager_required(f):
    """Декоратор: админ, менеджер ОП, РОП, маркетолог или супер-админ — доступ к проектам и amoCRM-отчётам"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        real_id = session.get("super_admin_id") or session["user_id"]
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user:
            flash("Доступ запрещён.", "error")
            return redirect(url_for("index"))
        role_ok = user["role"] in ("admin", "manager_op", "rop", "marketer")
        superadmin_ok = is_superadmin(real_id)
        if not (role_ok or superadmin_ok):
            flash("Доступ запрещён.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return decorated


def master_required(f):
    """Декоратор: требуется роль мастера"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] != "master":
            flash("Доступ запрещён.", "error")
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated


def foreman_required(f):
    """Декоратор: требуется роль прораба"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] != "foreman":
            flash("Доступ запрещён.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return decorated


def director_production_required(f):
    """Декоратор: требуется роль директора по производству"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] != "director_production":
            flash("Доступ запрещён.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return decorated


def work_items_manager_required(f):
    """Доступ к справочнику работ: админ или директор по производству."""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] not in ("admin", "director_production"):
            flash("Доступ запрещён.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return decorated


def director_construction_required(f):
    """Декоратор: требуется роль директора по строительству"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] != "director_construction":
            flash("Доступ запрещён.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return decorated


def client_required(f):
    """Декоратор: требуется роль клиента"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] != "client":
            flash("Доступ запрещён.", "error")
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated


def director_or_admin_edit_requests(f):
    """Доступ к заявкам: админ видит всё; директор по производству — только от мастера; директор по строительству — только от прораба"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] not in ("admin", "director_production", "director_construction"):
            flash("Доступ запрещён.", "error")
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated


def worker_required(f):
    """Декоратор: требуется роль работника"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] != "worker":
            flash("Доступ запрещён.", "error")
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated


def reports_viewer_required(f):
    """Декоратор: доступ к отчётам работников (админ/директора/мастер/прораб)."""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute(
            "SELECT role FROM users WHERE id = ?", (session["user_id"],)
        ).fetchone()
        conn.close()
        if not user or user["role"] not in (
            "admin",
            "director_production",
            "director_construction",
            "master",
            "foreman",
        ):
            flash("Доступ запрещён.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return decorated


def worker_reports_viewer_required(f):
    """Доступ к отчётам работников: админ, директор по производству, мастер."""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] not in ("admin", "director_production", "master"):
            flash("Доступ запрещён.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return decorated


def foreman_reports_viewer_required(f):
    """Доступ к отчётам прорабов: админ, директор по строительству."""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        conn = get_db()
        user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
        conn.close()
        if not user or user["role"] not in ("admin", "director_construction"):
            flash("Доступ запрещён.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return decorated


def permission_required(page_key: str):
    """Декоратор: доступ к странице по модульным правам пользователя."""

    def outer(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if "user_id" not in session:
                flash("Войдите в систему.", "warning")
                return redirect(url_for("login"))

            user_id = int(session["user_id"])
            conn = get_db()
            user = conn.execute(
                "SELECT role FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if not user:
                conn.close()
                flash("Доступ запрещён.", "error")
                return redirect(url_for("index"))

            permissions = _effective_user_permissions(conn, user_id, user["role"])
            conn.close()
            if not _is_page_access_allowed(permissions, page_key):
                flash("Нет доступа к выбранному отчёту.", "error")
                return redirect(url_for("index"))
            return f(*args, **kwargs)

        return decorated

    return outer


def allowed_file(filename):
    """Проверка расширения файла (фото)"""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


DOCUMENT_EXTENSIONS = {"pdf", "doc", "docx", "xls", "xlsx"}


def allowed_document_file(filename):
    """Проверка расширения для договоров и смет"""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in DOCUMENT_EXTENSIONS


def get_user_pref(user_id, key, default=None):
    """Получить настройку пользователя"""
    conn = get_db()
    row = conn.execute(
        "SELECT pref_value FROM user_preferences WHERE user_id = ? AND pref_key = ?",
        (user_id, key),
    ).fetchone()
    conn.close()
    return row["pref_value"] if row else default


def set_user_pref(user_id, key, value):
    """Сохранить настройку пользователя"""
    conn = get_db()
    conn.execute(
        "REPLACE INTO user_preferences (user_id, pref_key, pref_value, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
        (user_id, key, value),
    )
    conn.commit()
    conn.close()


def _get_json_pref(user_id: int, key: str, default):
    raw = get_user_pref(user_id, key, default=None)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _set_json_pref(user_id: int, key: str, value) -> None:
    try:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        payload = "{}"
    set_user_pref(user_id, key, payload)


@app.route("/prefs/report-columns", methods=["POST"])
@login_required
def save_report_column_widths():
    """Сохранить ширины колонок отчёта (персонально)."""
    user_id = int(session["user_id"])
    data = request.get_json(silent=True) or {}
    report = (data.get("report") or "").strip()
    widths = data.get("widths") or {}

    allowed_reports = {"worker_reports_v1"}
    if report not in allowed_reports or not isinstance(widths, dict):
        return {"ok": False}, 400

    allowed_cols = {
        "date",
        "project",
        "worker",
        "code",
        "work",
        "pct",
        "rate",
        "amount",
        "comment",
        "actions",
    }

    cleaned = {}
    for k, v in widths.items():
        if k not in allowed_cols:
            continue
        try:
            n = int(v)
        except Exception:
            continue
        n = max(70, min(520, n))
        cleaned[k] = n

    _set_json_pref(user_id, f"report_colwidths:{report}", cleaned)
    return {"ok": True}


def get_pending_edit_requests_count():
    """Количество заявок на редактирование, ожидающих рассмотрения"""
    conn = get_db()
    count = conn.execute(
        "SELECT COUNT(*) FROM edit_requests WHERE status = 'pending'"
    ).fetchone()[0]
    conn.close()
    return count


def get_pending_edit_requests_count_for_role(role):
    """Количество заявок для роли: админ — все; директор по производству — от мастера; директор по строительству — от прораба"""
    conn = get_db()
    if role == "admin":
        count = conn.execute(
            "SELECT COUNT(*) FROM edit_requests WHERE status = 'pending'"
        ).fetchone()[0]
    elif role == "director_production":
        count = conn.execute(
            """SELECT COUNT(*) FROM edit_requests er
               JOIN users u ON er.master_id = u.id
               WHERE er.status = 'pending' AND u.role = 'master'"""
        ).fetchone()[0]
    elif role == "director_construction":
        count = conn.execute(
            """SELECT COUNT(*) FROM edit_requests er
               JOIN users u ON er.master_id = u.id
               WHERE er.status = 'pending' AND u.role = 'foreman'"""
        ).fetchone()[0]
    else:
        count = 0
    conn.close()
    return count


def is_superadmin(user_id):
    """Является ли пользователь главным администратором"""
    conn = get_db()
    row = conn.execute(
        "SELECT is_superadmin FROM users WHERE id = ? AND role = 'admin'",
        (user_id,),
    ).fetchone()
    conn.close()
    return bool(row and row["is_superadmin"])


def super_admin_required(f):
    """Декоратор: требуется супер-админ (главный администратор)"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            flash("Войдите в систему.", "warning")
            return redirect(url_for("login"))
        real_id = session.get("super_admin_id") or session["user_id"]
        if not is_superadmin(real_id):
            abort(404)
        return f(*args, **kwargs)

    return decorated


def get_edit_requests_count_by_project():
    """Количество заявок по проектам (project_id -> count) для админа и мастера"""
    conn = get_db()
    rows = conn.execute(
        """SELECT s.project_id, COUNT(*) as cnt
           FROM edit_requests er
           JOIN stages s ON er.stage_id = s.id
           WHERE er.status = 'pending'
           GROUP BY s.project_id"""
    ).fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows}


def get_unread_chat_count_for_master(user_id):
    """Количество непрочитанных сообщений в чатах проектов мастера (ответы от админа)"""
    conn = get_db()
    projects = conn.execute(
        "SELECT id FROM projects WHERE master_id = ?", (user_id,)
    ).fetchall()
    total = 0
    for row in projects:
        pid = row[0]
        last_read = conn.execute(
            "SELECT last_read_at FROM project_chat_read WHERE user_id = ? AND project_id = ?",
            (user_id, pid),
        ).fetchone()
        last_read_at = last_read["last_read_at"] if last_read else None
        if last_read_at:
            cnt = conn.execute(
                """SELECT COUNT(*) FROM project_chat
                   WHERE project_id = ? AND user_id != ? AND created_at > ?""",
                (pid, user_id, last_read_at),
            ).fetchone()[0]
        else:
            cnt = conn.execute(
                """SELECT COUNT(*) FROM project_chat
                   WHERE project_id = ? AND user_id != ?""",
                (pid, user_id),
            ).fetchone()[0]
        total += cnt
    conn.close()
    return total


def get_unread_chat_count_by_project_for_master(user_id):
    """Непрочитанные сообщения по проектам мастера: project_id -> count"""
    conn = get_db()
    projects = conn.execute(
        "SELECT id FROM projects WHERE master_id = ?", (user_id,)
    ).fetchall()
    result = {}
    for row in projects:
        pid = row[0]
        last_read = conn.execute(
            "SELECT last_read_at FROM project_chat_read WHERE user_id = ? AND project_id = ?",
            (user_id, pid),
        ).fetchone()
        last_read_at = last_read["last_read_at"] if last_read else None
        if last_read_at:
            cnt = conn.execute(
                """SELECT COUNT(*) FROM project_chat
                   WHERE project_id = ? AND user_id != ? AND created_at > ?""",
                (pid, user_id, last_read_at),
            ).fetchone()[0]
        else:
            cnt = conn.execute(
                """SELECT COUNT(*) FROM project_chat
                   WHERE project_id = ? AND user_id != ?""",
                (pid, user_id),
            ).fetchone()[0]
        result[pid] = cnt
    conn.close()
    return result


def mark_chat_read(user_id, project_id):
    """Отметить чат проекта как прочитанный мастером"""
    conn = get_db()
    conn.execute(
        "REPLACE INTO project_chat_read (user_id, project_id, last_read_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
        (user_id, project_id),
    )
    conn.commit()
    conn.close()


def add_notification(user_id, notification_type, title, message=None, link_url=None, project_id=None):
    """Создать уведомление для пользователя"""
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO notifications (user_id, notification_type, title, message, link_url, project_id)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, notification_type, title, message or "", link_url or "", project_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_pending_takeover_requests_count():
    """Количество заявок на взятие проекта, ожидающих одобрения админа"""
    conn = get_db()
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='project_takeover_requests'")
        if not cursor.fetchone():
            return 0
        count = conn.execute(
            "SELECT COUNT(*) FROM project_takeover_requests WHERE status = 'pending'"
        ).fetchone()[0]
        return count
    finally:
        conn.close()


def get_pending_document_approvals_count_for_admin():
    """Количество документов (договор/смета), ожидающих одобрения админом"""
    conn = get_db()
    cursor = conn.execute("PRAGMA table_info(projects)")
    cols = [r[1] for r in cursor.fetchall()]
    conn.close()
    if "contract_admin_approved_at" not in cols:
        return 0
    conn = get_db()
    # Проекты с договором, ожидающие одобрения
    contract_ids = [
        r[0] for r in conn.execute(
            "SELECT id FROM projects WHERE contract_admin_approved_at IS NULL"
        ).fetchall()
    ]
    estimate_ids = [
        r[0] for r in conn.execute(
            "SELECT id FROM projects WHERE estimate_admin_approved_at IS NULL"
        ).fetchall()
    ]
    conn.close()
    contract_tokens = get_active_media_tokens_map("project_contract", contract_ids)
    estimate_tokens = get_active_media_tokens_map("project_estimate", estimate_ids)
    return len(contract_tokens) + len(estimate_tokens)


def get_unread_notifications_count(user_id):
    """Количество непрочитанных уведомлений пользователя"""
    conn = get_db()
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='notifications'")
        if not cursor.fetchone():
            return 0
        count = conn.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id = ? AND read_at IS NULL",
            (user_id,),
        ).fetchone()[0]
        return count
    finally:
        conn.close()


def get_unread_notifications(user_id, limit=20):
    """Получить непрочитанные уведомления пользователя"""
    conn = get_db()
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='notifications'")
        if not cursor.fetchone():
            return []
        rows = conn.execute(
            """SELECT n.*, p.name as project_name FROM notifications n
               LEFT JOIN projects p ON n.project_id = p.id
               WHERE n.user_id = ? AND n.read_at IS NULL
               ORDER BY n.created_at DESC LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_notification_read(notification_id, user_id):
    """Отметить уведомление как прочитанное"""
    conn = get_db()
    conn.execute(
        "UPDATE notifications SET read_at = CURRENT_TIMESTAMP WHERE id = ? AND user_id = ?",
        (notification_id, user_id),
    )
    conn.commit()
    conn.close()


@app.context_processor
def inject_notifications():
    """Добавить счётчики уведомлений в контекст шаблонов"""
    if "user_id" not in session:
        return {}
    role = session.get("role")
    result = {}
    if role in ("admin", "director_production", "director_construction"):
        result["pending_edit_requests_count"] = get_pending_edit_requests_count_for_role(role)
    if role == "admin":
        result["pending_document_approvals_count"] = get_pending_document_approvals_count_for_admin()
        result["pending_takeover_requests_count"] = get_pending_takeover_requests_count()
    elif role == "master":
        result["unread_chat_count"] = get_unread_chat_count_for_master(session["user_id"])
    if role in ("admin", "manager_op", "client"):
        result["unread_notifications_count"] = get_unread_notifications_count(session["user_id"])
    return result


@app.context_processor
def inject_impersonation():
    """Данные для выпадающего списка «Войти как» (супер-админ)."""
    if "user_id" not in session:
        return {}
    real_id = session.get("super_admin_id") or session["user_id"]
    if not is_superadmin(real_id):
        return {}
    conn = get_db()
    rows = conn.execute(
        "SELECT id, username, full_name, role FROM users ORDER BY role, full_name"
    ).fetchall()
    conn.close()
    users = []
    for r in rows:
        u = dict(r)
        u["role_label"] = ROLES.get(r["role"], r["role"])
        users.append(u)
    return {
        "can_impersonate": True,
        "impersonation_users": users,
        "is_impersonating": "super_admin_id" in session,
        "super_admin_id": real_id,
    }


@app.context_processor
def inject_access_flags():
    """Флаги модульных доступов для отображения пунктов меню."""
    if "user_id" not in session:
        return {}

    user_id = int(session["user_id"])
    conn = get_db()
    user = conn.execute("SELECT role FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return {}

    permissions = _effective_user_permissions(conn, user_id, user["role"])
    conn.close()
    modules = permissions.get("modules") or {}

    def can_access_page(page_key: str) -> bool:
        return _is_page_access_allowed(permissions, page_key)

    return {
        "can_access_page": can_access_page,
        "can_module_reports": bool(modules.get("reports")),
        "can_page_worker_reports": can_access_page("worker_reports"),
        "can_page_foreman_reports": can_access_page("foreman_reports"),
        "can_page_amocrm_leads": can_access_page("amocrm_leads"),
        "can_page_amocrm_projects": can_access_page("amocrm_projects"),
        "can_page_amocrm_sources": can_access_page("amocrm_sources"),
        "can_page_amocrm_tv": can_access_page("amocrm_tv"),
    }


# ============== РОУТЫ ==============


@app.route("/")
def index():
    """Главная: редирект на логин или на дашборд по роли"""
    if "user_id" not in session:
        return redirect(url_for("login"))
    conn = get_db()
    user = conn.execute("SELECT role FROM users WHERE id = ?", (session["user_id"],)).fetchone()
    conn.close()
    role = user["role"]
    if role == "admin":
        return redirect(url_for("admin_dashboard"))
    if role in ("rop", "marketer"):
        # РОП и маркетолог сразу попадают в отчёт "Источники лидов (недели)"
        return redirect(url_for("amocrm_sources_report"))
    if role == "manager_op":
        return redirect(url_for("manager_op_dashboard"))
    if role in ("director_production", "director_construction"):
        if role == "director_production":
            return redirect(url_for("director_production_dashboard"))
        return redirect(url_for("director_construction_dashboard"))
    if role in ("master", "foreman"):
        if role == "foreman":
            return redirect(url_for("foreman_dashboard"))
        return redirect(url_for("master_dashboard"))
    if role == "worker":
        return redirect(url_for("worker_dashboard"))
    if role == "client":
        return redirect(url_for("client_dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    """Страница входа"""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if not username or not password:
            flash("Введите логин и пароль.", "error")
            return render_template("login.html")
        conn = get_db()
        user = conn.execute(
            "SELECT id, username, password, role, full_name FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        conn.close()
        if user and check_password_hash(user["password"], password):
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]
            session["full_name"] = user["full_name"]
            session["greet_shown"] = False
            session.pop("greet_last_id", None)
            return redirect(url_for("index"))
        flash("Неверный логин или пароль.", "error")
    return render_template("login.html")


@app.route("/logout")
def logout():
    """Выход из системы"""
    session.clear()
    flash("Вы вышли из системы.", "info")
    return redirect(url_for("login"))


@app.route("/admin/approvals")
@login_required
def admin_document_approvals():
    """Страница согласования: проекты с договором/сметой, ожидающие одобрения админа"""
    if session.get("role") != "admin":
        flash("Доступ запрещён.", "error")
        return redirect(url_for("index"))
    conn = get_db()
    cursor = conn.execute("PRAGMA table_info(projects)")
    cols = [r[1] for r in cursor.fetchall()]
    conn.close()
    if "contract_admin_approved_at" not in cols:
        return render_template("admin/approvals.html", projects_contract=[], projects_estimate=[])
    conn = get_db()
    # Проекты с договором без одобрения
    contract_ids = [r[0] for r in conn.execute(
        "SELECT id FROM projects WHERE contract_admin_approved_at IS NULL"
    ).fetchall()]
    estimate_ids = [r[0] for r in conn.execute(
        "SELECT id FROM projects WHERE estimate_admin_approved_at IS NULL"
    ).fetchall()]
    contract_tokens = get_active_media_tokens_map("project_contract", contract_ids)
    estimate_tokens = get_active_media_tokens_map("project_estimate", estimate_ids)
    pid_contract = list(contract_tokens.keys())
    pid_estimate = list(estimate_tokens.keys())
    projects_contract = []
    projects_estimate = []
    if pid_contract:
        projects_contract = [dict(r) for r in conn.execute(
            "SELECT id, name, address FROM projects WHERE id IN ({})".format(",".join(["?"] * len(pid_contract))),
            tuple(pid_contract),
        ).fetchall()]
    if pid_estimate:
        projects_estimate = [dict(r) for r in conn.execute(
            "SELECT id, name, address FROM projects WHERE id IN ({})".format(",".join(["?"] * len(pid_estimate))),
            tuple(pid_estimate),
        ).fetchall()]
    conn.close()
    return render_template(
        "admin/approvals.html",
        projects_contract=projects_contract,
        projects_estimate=projects_estimate,
    )


@app.route("/notifications")
@login_required
def notifications_page():
    """Страница уведомлений (admin, manager_op, client)"""
    role = session.get("role")
    if role not in ("admin", "manager_op", "client"):
        flash("Уведомления недоступны для вашей роли.", "warning")
        return redirect(url_for("index"))
    items = get_unread_notifications(session["user_id"], limit=50)
    return render_template("notifications.html", notifications=items)


@app.route("/notifications/<int:notification_id>/read")
@login_required
def notification_read(notification_id):
    """Отметить уведомление прочитанным и перейти по ссылке"""
    conn = get_db()
    row = conn.execute(
        "SELECT id, user_id, link_url FROM notifications WHERE id = ? AND user_id = ?",
        (notification_id, session["user_id"]),
    ).fetchone()
    conn.close()
    if not row:
        flash("Уведомление не найдено.", "warning")
        return redirect(url_for("index"))
    mark_notification_read(notification_id, session["user_id"])
    url = (row["link_url"] or "").strip()
    if url and url.startswith("/"):
        return redirect(url)
    return redirect(url_for("notifications_page"))


@app.route("/switch-user", methods=["POST"])
def switch_user():
    """Войти под выбранным пользователем (только для супер-админа)."""
    if "user_id" not in session:
        flash("Войдите в систему.", "warning")
        return redirect(url_for("login"))
    real_id = session.get("super_admin_id") or session["user_id"]
    if not is_superadmin(real_id):
        flash("Недостаточно прав.", "error")
        return redirect(url_for("index"))
    target_id = request.form.get("user_id", type=int)
    if not target_id:
        flash("Укажите пользователя.", "error")
        return redirect(request.referrer or url_for("index"))
    conn = get_db()
    target = conn.execute(
        "SELECT id, username, role, full_name FROM users WHERE id = ?",
        (target_id,),
    ).fetchone()
    conn.close()
    if not target:
        flash("Пользователь не найден.", "error")
        return redirect(request.referrer or url_for("index"))
    if target_id == real_id:
        # Вернуться к себе
        session.pop("super_admin_id", None)
        session["user_id"] = real_id
        conn = get_db()
        u = conn.execute(
            "SELECT username, role, full_name FROM users WHERE id = ?",
            (real_id,),
        ).fetchone()
        conn.close()
        if u:
            session["username"] = u["username"]
            session["role"] = u["role"]
            session["full_name"] = u["full_name"]
        session["greet_shown"] = False
        session.pop("greet_last_id", None)
        flash("Вы вернулись к своему аккаунту.", "info")
    else:
        # Войти под выбранным
        if "super_admin_id" not in session:
            session["super_admin_id"] = real_id
        session["user_id"] = target_id
        session["username"] = target["username"]
        session["role"] = target["role"]
        session["full_name"] = target["full_name"]
        session["greet_shown"] = False
        session.pop("greet_last_id", None)
        flash(f"Вход под пользователем: {target['full_name']} ({target['username']})", "info")
    return redirect(url_for("index"))


# ============== АДМИН ==============


@app.route("/admin/dashboard-pref", methods=["POST"])
@admin_required
def admin_dashboard_pref():
    """Сохранить настройку отображения дашборда (без перезагрузки)"""
    display = request.form.get("recent_display") or (request.get_json(silent=True) or {}).get("recent_display")
    if display in ("1", "2", "3", "4"):
        set_user_pref(session["user_id"], "admin_recent_display", display)
    return "", 204


@app.route("/admin")
@admin_required
def admin_dashboard():
    """Дашборд администратора"""
    display = request.args.get("recent_display", type=str)
    if display and display in ("1", "2", "3", "4"):
        set_user_pref(session["user_id"], "admin_recent_display", display)
        return redirect(url_for("admin_dashboard"))
    recent_display = get_user_pref(session["user_id"], "admin_recent_display", "1")
    conn = get_db()
    proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
    if "completed_at" in proj_cols:
        projects = [dict(r) for r in conn.execute(
            """SELECT p.*, 
               u1.full_name as client_name, u2.full_name as master_name
               FROM projects p
               LEFT JOIN users u1 ON p.client_id = u1.id
               LEFT JOIN users u2 ON p.master_id = u2.id
               WHERE p.completed_at IS NULL
               ORDER BY p.created_at DESC"""
        ).fetchall()]
    else:
        projects = [dict(r) for r in conn.execute(
            """SELECT p.*, 
               u1.full_name as client_name, u2.full_name as master_name
               FROM projects p
               LEFT JOIN users u1 ON p.client_id = u1.id
               LEFT JOIN users u2 ON p.master_id = u2.id
               ORDER BY p.created_at DESC"""
        ).fetchall()]
    tokens = get_active_media_tokens_map("project_photo", [int(p["id"]) for p in projects])
    for p in projects:
        p["photo_token"] = tokens.get(int(p["id"]))
    counts = get_edit_requests_count_by_project()
    for p in projects:
        p["edit_requests_count"] = counts.get(p["id"], 0)
    users = conn.execute(
        "SELECT id, username, full_name, role, phone FROM users ORDER BY role, full_name"
    ).fetchall()
    conn.close()
    return render_template(
        "admin/dashboard.html",
        projects=projects,
        users=users,
        building_types=BUILDING_TYPES,
        recent_display=recent_display,
    )


# Связи ролей: подчинение, отчётность, обязанности (для страницы супер-админа)
ROLES_HIERARCHY = {
    "super": {
        "label": "Супер-админ",
        "reports_to": None,
        "reports_via": None,
        "duties": [
            "Полный доступ ко всей системе",
            "Вход под любым пользователем (impersonation)",
            "Создание и удаление администраторов",
            "Управление всеми пользователями",
        ],
    },
    "admin": {
        "label": "Администратор",
        "reports_to": "Супер-админ",
        "reports_via": "Подчиняется супер-админу (не видно в интерфейсе)",
        "duties": [
            "Проекты, пользователи, этапы, заявки на редактирование",
            "Назначение ролей и руководителей: reports_to_id / reports_to_production_id / reports_to_construction_id",
            "Настройка модульных доступов к отчётам по каждому пользователю (модули + страницы)",
            "Производственный календарь, отчёты работников и прорабов, amoCRM-отчёты",
            "Справочник работ, чат проектов",
            "Не может создавать других администраторов",
        ],
    },
    "manager_op": {
        "label": "Менеджер ОП (отдел продаж)",
        "reports_to": "Администратор",
        "reports_via": "Назначается администратором",
        "duties": [
            "Создание новых проектов",
            "Загрузка фото, договоров, смет",
            "Прописание этапов работ с датами начала и окончания",
            "Производственный календарь",
            "Отчёты amoCRM по назначенным модульным правам (лиды, проекты, источники, TV)",
            "Заявки на взятие проекта при неактивности ответственного ОП",
        ],
    },
    "rop": {
        "label": "РОП (руководитель отдела продаж)",
        "reports_to": "Администратор",
        "reports_via": "Назначается администратором",
        "duties": [
            "Управление отделом продаж",
            "Контроль работы менеджеров ОП",
            "Просмотр и контроль amoCRM-отчётов по назначенным правам",
            "Анализ воронки продаж и источников лидов",
        ],
    },
    "marketer": {
        "label": "Маркетолог",
        "reports_to": "Администратор",
        "reports_via": "Назначается администратором",
        "duties": [
            "Аналитика источников лидов и маркетинговых кампаний",
            "Работа с недельным/месячным отчётом по источникам лидов",
            "Просмотр amoCRM-отчётов по назначенным правам",
            "Подготовка маркетинговых KPI и предложений по оптимизации",
        ],
    },
    "director_production": {
        "label": "Директор по производству",
        "reports_to": "Администратор",
        "reports_via": "Назначается в карточке пользователя",
        "duties": [
            "Заявки на редактирование от мастеров",
            "Подтверждение % выполнения работ работников направления",
            "Подтверждение этапов модульных домов (мастер отчитался)",
            "Назначение объектов работникам (мастера и работники направления)",
            "Просмотр отчётов работников по доступу модуля worker_reports",
        ],
        "subordinates": "Мастера (reports_to_production_id), работники (reports_to_production_id)",
    },
    "director_construction": {
        "label": "Директор строительства на участке",
        "reports_to": "Администратор",
        "reports_via": "Назначается в карточке пользователя",
        "duties": [
            "Заявки на редактирование и отчёты от прорабов",
            "Подтверждение этапов каркасных/газобетонных/пенополистиролбетонных объектов",
            "Назначение объектов прорабам направления",
            "Просмотр отчётов прорабов по доступу модуля foreman_reports",
        ],
        "subordinates": "Прорабы (reports_to_construction_id)",
    },
    "foreman": {
        "label": "Прораб",
        "reports_to": "Директор по строительству",
        "reports_via": "reports_to_construction_id",
        "duties": [
            "Объекты, назначенные директором по строительству",
            "Закрытие рабочего дня (работы + % + комментарий)",
            "Заявки на редактирование этапов",
            "Отчёты попадают в календарь и отчёт foreman_reports",
        ],
    },
    "master": {
        "label": "Мастер",
        "reports_to": "Директор по производству",
        "reports_via": "reports_to_production_id",
        "duties": [
            "Свои объекты, добавление отчётов по этапам",
            "Заявки на редактирование этапов",
            "Подтверждение этапов модульных домов (после отчёта)",
            "Назначение объектов работникам, чат проекта",
            "Просмотр отчётов работников по доступу модуля worker_reports",
        ],
        "subordinates": "Работники (reports_to_id)",
    },
    "worker": {
        "label": "Работник",
        "reports_to": "Мастер",
        "reports_via": "reports_to_id",
        "duties": [
            "Закрытие дня: работа + % выполнения + комментарий",
            "Видит объекты мастера, отчитывается перед ним",
            "Подтверждение % — мастер или директор по производству",
        ],
    },
    "client": {
        "label": "Заказчик",
        "reports_to": None,
        "reports_via": None,
        "duties": [
            "Просмотр своих объектов и прогресса этапов",
            "Подтверждение договора и сметы (после загрузки менеджером ОП)",
        ],
    },
}


@app.route("/admin/roles")
@super_admin_required
def admin_roles():
    """Страница связей ролей — только для супер-админа"""
    return render_template(
        "admin/roles.html",
        roles_hierarchy=ROLES_HIERARCHY,
        roles=ROLES,
    )


@app.route("/reports/roles/presentation")
@super_admin_required
def roles_presentation_report():
    """Презентационный отчёт по связям ролей и развитию проекта (для супер-админа)."""
    return render_template(
        "reports/roles_presentation.html",
        roles_hierarchy=ROLES_HIERARCHY,
        roles=ROLES,
    )


@app.route("/admin/users", methods=["GET", "POST"])
@admin_required
def admin_users():
    """Управление пользователями"""
    if request.method == "POST":
        action = request.form.get("action")
        if action == "create_user":
            role = request.form.get("role", "").strip()
            if role == "admin" and not is_superadmin(session["user_id"]):
                flash("Создание администраторов недоступно.", "error")
            elif role not in ROLES:
                flash("Выберите роль.", "error")
            else:
                username = request.form.get("username", "").strip()
                password = request.form.get("password", "")
                full_name = request.form.get("full_name", "").strip()
                phone = normalize_phone(request.form.get("phone", ""))
                if username and password and full_name:
                    conn = get_db()
                    try:
                        cursor = conn.cursor()
                        cursor.execute("PRAGMA table_info(users)")
                        cols = [r[1] for r in cursor.fetchall()]
                        if role == "admin" and "is_superadmin" in cols:
                            conn.execute(
                                "INSERT INTO users (username, password, role, full_name, phone, is_superadmin) VALUES (?, ?, 'admin', ?, ?, 0)",
                                (username, generate_password_hash(password), full_name, phone),
                            )
                        else:
                            conn.execute(
                                "INSERT INTO users (username, password, role, full_name, phone) VALUES (?, ?, ?, ?, ?)",
                                (username, generate_password_hash(password), role, full_name, phone),
                            )
                        conn.commit()
                        flash("Пользователь создан.", "success")
                    except sqlite3.IntegrityError:
                        flash("Пользователь с таким логином уже существует.", "error")
                    conn.close()
        elif action == "delete":
            user_id = request.form.get("user_id", type=int)
            if user_id:
                conn = get_db()
                target = conn.execute(
                    "SELECT id, role, COALESCE(is_superadmin, 0) as is_superadmin FROM users WHERE id = ?",
                    (user_id,),
                ).fetchone()
                if not target:
                    flash("Пользователь не найден.", "error")
                elif target["id"] == session["user_id"]:
                    flash("Нельзя удалить самого себя.", "error")
                elif target["is_superadmin"]:
                    flash("Нельзя удалить этого пользователя.", "error")
                else:
                    try:
                        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
                        conn.commit()
                        flash("Пользователь удалён.", "success")
                    except sqlite3.IntegrityError:
                        flash("Нельзя удалить: пользователь привязан к проектам.", "error")
                conn.close()
        return redirect(url_for("admin_users"))

    conn = get_db()
    q = request.args.get("q", "").strip()
    role_filter = request.args.get("role", "").strip()

    sql = """SELECT id, username, full_name, role, phone, COALESCE(is_superadmin, 0) as is_superadmin
             FROM users WHERE username != 'super'"""
    params = []
    if q:
        sql += " AND (full_name LIKE ? OR phone LIKE ?)"
        q_like = f"%{q}%"
        params.extend([q_like, q_like])
    if role_filter and role_filter in ROLES:
        sql += " AND role = ?"
        params.append(role_filter)
    sql += " ORDER BY role, full_name"
    users = conn.execute(sql, params).fetchall()
    conn.close()
    users = [dict(u) for u in users]
    return render_template(
        "admin/users.html",
        users=users,
        roles=ROLES,
        is_superadmin=is_superadmin(session["user_id"]),
        search_q=q,
        filter_role=role_filter,
    )


@app.route("/admin/user/<int:user_id>/edit", methods=["GET", "POST"])
@admin_required
def admin_user_edit(user_id):
    """Редактирование пользователя"""
    conn = get_db()
    user = conn.execute(
        """SELECT id, username, full_name, role, phone, COALESCE(is_superadmin, 0) as is_superadmin,
                  reports_to_id, reports_to_production_id, reports_to_construction_id
                  FROM users WHERE id = ?""",
        (user_id,),
    ).fetchone()
    if not user:
        conn.close()
        flash("Пользователь не найден.", "error")
        return redirect(url_for("admin_users"))
    user = dict(user)
    modules_catalog = _catalog_modules(conn)
    pages_catalog = _catalog_pages(conn)
    effective_permissions = _effective_user_permissions(conn, user_id, user["role"])
    selected_module_keys = {k for k, v in (effective_permissions.get("modules") or {}).items() if v}
    selected_page_keys = {k for k, v in (effective_permissions.get("pages") or {}).items() if v}

    if user["is_superadmin"] and not is_superadmin(session["user_id"]):
        conn.close()
        flash("Недостаточно прав для редактирования этого пользователя.", "error")
        return redirect(url_for("admin_users"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        full_name = request.form.get("full_name", "").strip()
        phone = normalize_phone(request.form.get("phone", ""))
        new_password = request.form.get("new_password", "")
        role = request.form.get("role", "").strip()
        selected_module_keys = {
            k
            for k in request.form.getlist("perm_modules")
            if k in ACCESS_MODULE_KEYS
        }
        selected_page_keys = {
            k
            for k in request.form.getlist("perm_pages")
            if k in ACCESS_PAGE_KEYS
        }

        if not username or not full_name:
            flash("Логин и имя обязательны.", "error")
            masters = [dict(m) for m in conn.execute("SELECT id, full_name FROM users WHERE role = 'master' ORDER BY full_name").fetchall()]
            prod_directors = [dict(r) for r in conn.execute(
                "SELECT id, full_name FROM users WHERE role = 'director_production' ORDER BY full_name"
            ).fetchall()]
            cons_directors = [dict(r) for r in conn.execute(
                "SELECT id, full_name FROM users WHERE role = 'director_construction' ORDER BY full_name"
            ).fetchall()]
            conn.close()
            return render_template(
                "admin/user_edit.html",
                user=user,
                roles=ROLES,
                masters=masters,
                prod_directors=prod_directors,
                cons_directors=cons_directors,
                is_superadmin=is_superadmin(session["user_id"]),
                modules_catalog=modules_catalog,
                pages_catalog=pages_catalog,
                selected_module_keys=selected_module_keys,
                selected_page_keys=selected_page_keys,
            )

        # Роль можно менять только главному админу; нельзя понижать главного админа
        can_change_role = is_superadmin(session["user_id"]) and not user["is_superadmin"]
        if can_change_role and role in ROLES:
            pass  # role будет обновлён
        else:
            role = user["role"]

        # Подчинение:
        # - worker -> master (reports_to_id) + director_production (reports_to_production_id)
        # - master -> director_production (reports_to_production_id)
        # - foreman -> director_construction (reports_to_construction_id)
        reports_to_id = request.form.get("reports_to_id", type=int) if role == "worker" else None
        reports_to_production_id = (
            request.form.get("reports_to_production_id", type=int)
            if role in ("worker", "master")
            else None
        )
        reports_to_construction_id = (
            request.form.get("reports_to_construction_id", type=int)
            if role == "foreman"
            else None
        )
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(users)")
        cols = [r[1] for r in cursor.fetchall()]
        has_reports_to = "reports_to_id" in cols
        has_prod = "reports_to_production_id" in cols
        has_cons = "reports_to_construction_id" in cols

        try:
            if new_password:
                if has_reports_to or has_prod or has_cons:
                    sets = ["username=?", "full_name=?", "phone=?", "role=?", "password=?"]
                    args = [username, full_name, phone, role, generate_password_hash(new_password)]
                    if has_reports_to:
                        sets.append("reports_to_id=?")
                        args.append(reports_to_id)
                    if has_prod:
                        sets.append("reports_to_production_id=?")
                        args.append(reports_to_production_id)
                    if has_cons:
                        sets.append("reports_to_construction_id=?")
                        args.append(reports_to_construction_id)
                    args.append(user_id)
                    conn.execute(
                        f"UPDATE users SET {', '.join(sets)} WHERE id=?",
                        tuple(args),
                    )
                else:
                    conn.execute(
                        "UPDATE users SET username=?, full_name=?, phone=?, role=?, password=? WHERE id=?",
                        (username, full_name, phone, role, generate_password_hash(new_password), user_id),
                    )
            else:
                if has_reports_to or has_prod or has_cons:
                    sets = ["username=?", "full_name=?", "phone=?", "role=?"]
                    args = [username, full_name, phone, role]
                    if has_reports_to:
                        sets.append("reports_to_id=?")
                        args.append(reports_to_id)
                    if has_prod:
                        sets.append("reports_to_production_id=?")
                        args.append(reports_to_production_id)
                    if has_cons:
                        sets.append("reports_to_construction_id=?")
                        args.append(reports_to_construction_id)
                    args.append(user_id)
                    conn.execute(
                        f"UPDATE users SET {', '.join(sets)} WHERE id=?",
                        tuple(args),
                    )
                else:
                    conn.execute(
                        "UPDATE users SET username=?, full_name=?, phone=?, role=? WHERE id=?",
                        (username, full_name, phone, role, user_id),
                    )
            try:
                conn.execute("DELETE FROM user_module_access WHERE user_id = ?", (int(user_id),))
                conn.execute("DELETE FROM user_page_access WHERE user_id = ?", (int(user_id),))
                for m in modules_catalog:
                    mk = str(m.get("module_key") or "")
                    if not mk:
                        continue
                    conn.execute(
                        """INSERT INTO user_module_access (user_id, module_key, is_allowed, updated_at)
                           VALUES (?, ?, ?, datetime('now'))""",
                        (int(user_id), mk, 1 if mk in selected_module_keys else 0),
                    )
                for p in pages_catalog:
                    pk = str(p.get("page_key") or "")
                    if not pk:
                        continue
                    conn.execute(
                        """INSERT INTO user_page_access (user_id, page_key, is_allowed, updated_at)
                           VALUES (?, ?, ?, datetime('now'))""",
                        (int(user_id), pk, 1 if pk in selected_page_keys else 0),
                    )
            except sqlite3.OperationalError:
                # База без новых таблиц прав — сохраняем профиль/роль без падения.
                pass
            conn.commit()
            flash("Пользователь обновлён.", "success")
        except sqlite3.IntegrityError:
            flash("Пользователь с таким логином уже существует.", "error")
        conn.close()
        return redirect(url_for("admin_users"))

    masters = [dict(m) for m in conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'master' ORDER BY full_name"
    ).fetchall()]
    prod_directors = [dict(r) for r in conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'director_production' ORDER BY full_name"
    ).fetchall()]
    cons_directors = [dict(r) for r in conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'director_construction' ORDER BY full_name"
    ).fetchall()]
    conn.close()
    return render_template(
        "admin/user_edit.html",
        user=user,
        roles=ROLES,
        masters=masters,
        prod_directors=prod_directors,
        cons_directors=cons_directors,
        is_superadmin=is_superadmin(session["user_id"]),
        modules_catalog=modules_catalog,
        pages_catalog=pages_catalog,
        selected_module_keys=selected_module_keys,
        selected_page_keys=selected_page_keys,
    )


@app.route("/manager-op")
@projects_manager_required
def manager_op_dashboard():
    """Дашборд менеджера ОП: проекты и производственный календарь"""
    return redirect(url_for("admin_projects"))


@app.route("/manager-op/takeover", methods=["GET", "POST"])
@projects_manager_required
def manager_op_takeover():
    """Запрос на взятие проекта: менеджер ОП видит проекты без активного ответственного > N дней"""
    if session.get("role") != "manager_op":
        flash("Доступ только для менеджера ОП.", "error")
        return redirect(url_for("index"))
    conn = get_db()
    uid = session["user_id"]
    cutoff = (date.today() - timedelta(days=TAKEOVER_INACTIVE_DAYS)).isoformat()

    if request.method == "POST":
        action = request.form.get("action")
        project_id = request.form.get("project_id", type=int)
        if action == "request" and project_id:
            # Проверка: проект не свой, нет активной заявки от этого пользователя
            existing = conn.execute(
                """SELECT 1 FROM project_takeover_requests
                   WHERE project_id = ? AND requester_id = ? AND status = 'pending'""",
                (project_id, uid),
            ).fetchone()
            if existing:
                flash("Вы уже подали заявку на этот проект.", "warning")
            else:
                proj = conn.execute(
                    "SELECT id, responsible_manager_id FROM projects WHERE id = ?",
                    (project_id,),
                ).fetchone()
                if proj and proj["responsible_manager_id"] != uid:
                    conn.execute(
                        "INSERT INTO project_takeover_requests (project_id, requester_id, status) VALUES (?, ?, 'pending')",
                        (project_id, uid),
                    )
                    conn.commit()
                    proj_name = conn.execute("SELECT name FROM projects WHERE id = ?", (project_id,)).fetchone()
                    proj_name = proj_name["name"] if proj_name else ""
                    for admin_row in conn.execute("SELECT id FROM users WHERE role = 'admin'").fetchall():
                        add_notification(
                            admin_row[0],
                            "takeover_request",
                            "Заявка на взятие проекта",
                            f"Менеджер ОП запросил права на проект «{proj_name}». Требуется одобрение.",
                            url_for("admin_takeover_requests"),
                            project_id,
                        )
                    flash("Заявка отправлена. Администратор рассмотрит её.", "success")
                else:
                    flash("Нельзя запросить этот проект.", "error")
        conn.close()
        return redirect(url_for("manager_op_takeover"))

    # Проекты, доступные для запроса: не свои + ответственный неактивен > N дней (или NULL)
    cursor = conn.execute("PRAGMA table_info(projects)")
    proj_cols = [r[1] for r in cursor.fetchall()]
    if "last_responsible_activity_at" not in proj_cols:
        available = [dict(r) for r in conn.execute(
            """SELECT p.id, p.name, p.address, p.type, u.full_name as responsible_name,
                      NULL as last_responsible_activity_at,
                      (SELECT 1 FROM project_takeover_requests ptr
                       WHERE ptr.project_id = p.id AND ptr.requester_id = ? AND ptr.status = 'pending') as my_pending
               FROM projects p
               LEFT JOIN users u ON u.id = p.responsible_manager_id
               WHERE p.responsible_manager_id IS NULL OR p.responsible_manager_id != ?
               ORDER BY p.name""",
            (uid, uid),
        ).fetchall()]
    else:
        available = [dict(r) for r in conn.execute(
            """SELECT p.id, p.name, p.address, p.type, u.full_name as responsible_name,
                      p.last_responsible_activity_at,
                      (SELECT 1 FROM project_takeover_requests ptr
                       WHERE ptr.project_id = p.id AND ptr.requester_id = ? AND ptr.status = 'pending') as my_pending
               FROM projects p
               LEFT JOIN users u ON u.id = p.responsible_manager_id
               WHERE (p.responsible_manager_id IS NULL OR p.responsible_manager_id != ?)
                 AND (p.last_responsible_activity_at IS NULL OR p.last_responsible_activity_at <= ?)
               ORDER BY p.name""",
            (uid, uid, cutoff + " 23:59:59"),
        ).fetchall()]

    my_requests = [dict(r) for r in conn.execute(
        """SELECT ptr.id, ptr.project_id, ptr.status, ptr.created_at, p.name as project_name
           FROM project_takeover_requests ptr
           JOIN projects p ON p.id = ptr.project_id
           WHERE ptr.requester_id = ?
           ORDER BY ptr.created_at DESC""",
        (uid,),
    ).fetchall()]
    conn.close()
    return render_template(
        "manager_op/takeover.html",
        available=available,
        my_requests=my_requests,
        inactive_days=TAKEOVER_INACTIVE_DAYS,
        building_types=BUILDING_TYPES,
    )


@app.route("/admin/takeover-requests", methods=["GET", "POST"])
@admin_required
def admin_takeover_requests():
    """Заявки на взятие проекта: одобрение/отклонение"""
    conn = get_db()
    if request.method == "POST":
        action = request.form.get("action")
        request_id = request.form.get("request_id", type=int)
        if action in ("approve", "reject") and request_id:
            row = conn.execute(
                """SELECT ptr.id, ptr.project_id, ptr.requester_id, p.name as project_name, p.responsible_manager_id
                   FROM project_takeover_requests ptr
                   JOIN projects p ON p.id = ptr.project_id
                   WHERE ptr.id = ? AND ptr.status = 'pending'""",
                (request_id,),
            ).fetchone()
            if row:
                if action == "approve":
                    conn.execute(
                        "UPDATE projects SET responsible_manager_id = ?, last_responsible_activity_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (row["requester_id"], row["project_id"]),
                    )
                    conn.execute(
                        "UPDATE project_takeover_requests SET status = 'approved', reviewed_by_id = ?, reviewed_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (session["user_id"], request_id),
                    )
                    conn.commit()
                    add_notification(
                        row["requester_id"],
                        "takeover_approved",
                        "Проект передан вам",
                        f"Администратор одобрил заявку на проект «{row['project_name']}». Вы теперь ответственный ОП.",
                        url_for("admin_project_stages", project_id=row["project_id"]),
                        row["project_id"],
                    )
                    if row["responsible_manager_id"]:
                        add_notification(
                            row["responsible_manager_id"],
                            "takeover_approved",
                            "Проект передан другому менеджеру",
                            f"Проект «{row['project_name']}» передан другому менеджеру ОП по заявке.",
                            url_for("admin_projects"),
                            row["project_id"],
                        )
                    flash("Заявка одобрена. Менеджер ОП получит уведомление.", "success")
                else:
                    admin_comment = request.form.get("admin_comment", "").strip()
                    conn.execute(
                        "UPDATE project_takeover_requests SET status = 'rejected', reviewed_by_id = ?, reviewed_at = CURRENT_TIMESTAMP, admin_comment = ? WHERE id = ?",
                        (session["user_id"], admin_comment or None, request_id),
                    )
                    conn.commit()
                    add_notification(
                        row["requester_id"],
                        "takeover_rejected",
                        "Заявка отклонена",
                        f"Администратор отклонил заявку на проект «{row['project_name']}»." + (f" Комментарий: {admin_comment}" if admin_comment else ""),
                        url_for("manager_op_takeover"),
                        row["project_id"],
                    )
                    flash("Заявка отклонена.", "info")
        conn.close()
        return redirect(url_for("admin_takeover_requests"))

    pending = [dict(r) for r in conn.execute(
        """SELECT ptr.id, ptr.project_id, ptr.requester_id, ptr.created_at,
                  p.name as project_name, u.full_name as requester_name,
                  resp.full_name as current_responsible_name
           FROM project_takeover_requests ptr
           JOIN projects p ON p.id = ptr.project_id
           JOIN users u ON u.id = ptr.requester_id
           LEFT JOIN users resp ON resp.id = p.responsible_manager_id
           WHERE ptr.status = 'pending'
           ORDER BY ptr.created_at ASC"""
    ).fetchall()]
    conn.close()
    return render_template("admin/takeover_requests.html", pending=pending)


@app.route("/admin/projects", methods=["GET", "POST"])
@projects_manager_required
def admin_projects():
    """Управление проектами"""
    conn = get_db()
    if request.method == "POST":
        action = request.form.get("action")
        if action == "create":
            name = request.form.get("name", "").strip()
            building_type = request.form.get("type", "")
            address = request.form.get("address", "").strip()
            client_id = request.form.get("client_id", type=int)
            master_id = request.form.get("master_id", type=int)
            responsible_manager_id = request.form.get("responsible_manager_id", type=int)
            director_construction_id = request.form.get("director_construction_id", type=int)
            user_id = session["user_id"]
            role = session.get("role")
            # Менеджер ОП — сам ответственный; админ — выбирает ответственного
            if role == "manager_op":
                responsible_manager_id = user_id
            if name and building_type and address and client_id and master_id and responsible_manager_id:
                photo = request.files.get("photo")
                proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
                if "director_construction_id" in proj_cols:
                    dc_id = director_construction_id if building_type in ("frame", "gasblock", "penopolistirol") else None
                    conn.execute(
                        """INSERT INTO projects (name, type, address, client_id, master_id, photo_path, created_by_id, responsible_manager_id, director_construction_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (name, building_type, address, client_id, master_id, None, user_id, responsible_manager_id, dc_id),
                    )
                else:
                    conn.execute(
                        """INSERT INTO projects (name, type, address, client_id, master_id, photo_path, created_by_id, responsible_manager_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (name, building_type, address, client_id, master_id, None, user_id, responsible_manager_id),
                    )
                project_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                # Для каркас/газоблок/пенополистирол: master_id — это прораб, добавляем в foreman_project_access
                if building_type in ("frame", "gasblock", "penopolistirol") and master_id:
                    conn.execute(
                        """INSERT OR IGNORE INTO foreman_project_access (foreman_id, project_id, assigned_by_id)
                           VALUES (?, ?, ?)""",
                        (master_id, project_id, user_id if role == "admin" else None),
                    )
                conn.execute(
                    "UPDATE projects SET last_responsible_activity_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (project_id,),
                )
                conn.commit()
                if photo and photo.filename and allowed_file(photo.filename):
                    try:
                        rel_dir = _storage_rel_dir_for(
                            "project_photo", project_id=int(project_id)
                        )
                        save_media_file(
                            photo,
                            project_id=int(project_id),
                            stage_id=None,
                            entity_type="project_photo",
                            entity_id=int(project_id),
                            uploaded_by_id=session.get("user_id"),
                            rel_dir=rel_dir,
                        )
                    except Exception:
                        flash("Проект создан, но фото не удалось сохранить в защищённое хранилище.", "warning")
                contract = request.files.get("contract")
                if contract and contract.filename and allowed_document_file(contract.filename):
                    try:
                        rel_dir = _storage_rel_dir_for("project_contract", project_id=int(project_id))
                        save_media_file(
                            contract,
                            project_id=int(project_id),
                            stage_id=None,
                            entity_type="project_contract",
                            entity_id=int(project_id),
                            uploaded_by_id=session.get("user_id"),
                            rel_dir=rel_dir,
                        )
                        proj_name = name
                        for ar in conn.execute("SELECT id FROM users WHERE role = 'admin'").fetchall():
                            add_notification(
                                ar[0],
                                "document_upload",
                                "Договор загружен",
                                f"Менеджер ОП загрузил договор по проекту «{proj_name}». Требуется одобрение.",
                                url_for("admin_project_edit", project_id=project_id),
                                project_id,
                            )
                    except Exception:
                        flash("Договор не удалось сохранить.", "warning")
                estimate = request.files.get("estimate")
                if estimate and estimate.filename and allowed_document_file(estimate.filename):
                    try:
                        rel_dir = _storage_rel_dir_for("project_estimate", project_id=int(project_id))
                        save_media_file(
                            estimate,
                            project_id=int(project_id),
                            stage_id=None,
                            entity_type="project_estimate",
                            entity_id=int(project_id),
                            uploaded_by_id=session.get("user_id"),
                            rel_dir=rel_dir,
                        )
                        proj_name = name
                        for ar in conn.execute("SELECT id FROM users WHERE role = 'admin'").fetchall():
                            add_notification(
                                ar[0],
                                "document_upload",
                                "Смета загружена",
                                f"Менеджер ОП загрузил смету по проекту «{proj_name}». Требуется одобрение.",
                                url_for("admin_project_edit", project_id=project_id),
                                project_id,
                            )
                    except Exception:
                        flash("Смету не удалось сохранить.", "warning")
                flash("Проект создан.", "success")
                conn.close()
                return redirect(url_for("admin_project_stages", project_id=project_id))

    projects = [dict(r) for r in conn.execute(
        """SELECT p.*, u1.full_name as client_name,
                  u3.full_name as responsible_manager_name,
                  CASE
                    WHEN p.type IN ('frame', 'gasblock', 'penopolistirol') THEN
                      (SELECT u2.full_name FROM foreman_project_access fpa
                       JOIN users u2 ON u2.id = fpa.foreman_id
                       WHERE fpa.project_id = p.id LIMIT 1)
                    ELSE u2m.full_name
                  END as master_name
           FROM projects p
           LEFT JOIN users u1 ON p.client_id = u1.id
           LEFT JOIN users u2m ON p.master_id = u2m.id
           LEFT JOIN users u3 ON p.responsible_manager_id = u3.id
           ORDER BY p.created_at DESC"""
    ).fetchall()]
    tokens = get_active_media_tokens_map("project_photo", [int(p["id"]) for p in projects])
    for p in projects:
        p["photo_token"] = tokens.get(int(p["id"]))
    counts = get_edit_requests_count_by_project()
    for p in projects:
        p["edit_requests_count"] = counts.get(p["id"], 0)
    masters = [dict(r) for r in conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'master' ORDER BY full_name"
    ).fetchall()]
    foremen = [dict(r) for r in conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'foreman' ORDER BY full_name"
    ).fetchall()]
    clients = conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'client' ORDER BY full_name"
    ).fetchall()
    managers_op = conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'manager_op' ORDER BY full_name"
    ).fetchall()
    directors_construction = conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'director_construction' ORDER BY full_name"
    ).fetchall()
    # Менеджер ОП видит только свои проекты (создал или назначен ответственным)
    role = session.get("role")
    if role == "manager_op":
        uid = session["user_id"]
        projects = [p for p in projects if p.get("created_by_id") == uid or p.get("responsible_manager_id") == uid]
    conn.close()
    return render_template(
        "admin/projects.html",
        projects=projects,
        masters=masters,
        foremen=foremen,
        clients=clients,
        managers_op=managers_op,
        directors_construction=directors_construction,
        building_types=BUILDING_TYPES,
    )


@app.route("/admin/project/<int:project_id>/edit", methods=["GET", "POST"])
@projects_manager_required
def admin_project_edit(project_id):
    """Редактирование проекта"""
    conn = get_db()
    proj = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    if not proj:
        conn.close()
        flash("Проект не найден.", "error")
        return redirect(url_for("admin_projects"))
    project = dict(proj)

    # Менеджер ОП может редактировать только свои проекты
    role = session.get("role")
    if role == "manager_op":
        uid = session["user_id"]
        if project.get("created_by_id") != uid and project.get("responsible_manager_id") != uid:
            conn.close()
            flash("Доступ запрещён: это не ваш проект.", "error")
            return redirect(url_for("admin_projects"))

    if request.method == "POST":
        action = request.form.get("action", "")
        # Одобрение договора/сметы админом (только admin, не manager_op)
        if action == "approve_contract" and role == "admin":
            proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
            if "contract_admin_approved_at" in proj_cols:
                conn.execute(
                    "UPDATE projects SET contract_admin_approved_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (project_id,),
                )
                conn.commit()
                client_id = project.get("client_id")
                if client_id:
                    add_notification(
                        client_id,
                        "document_approval",
                        "Договор одобрен",
                        f"Администратор одобрил договор по проекту «{project.get('name', '')}». Подтвердите в личном кабинете.",
                        url_for("client_project", project_id=project_id),
                        project_id,
                    )
                flash("Договор одобрен. Заказчик получит уведомление.", "success")
            conn.close()
            return redirect(url_for("admin_project_edit", project_id=project_id))
        if action == "approve_estimate" and role == "admin":
            proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
            if "estimate_admin_approved_at" in proj_cols:
                conn.execute(
                    "UPDATE projects SET estimate_admin_approved_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (project_id,),
                )
                conn.commit()
                client_id = project.get("client_id")
                if client_id:
                    add_notification(
                        client_id,
                        "document_approval",
                        "Смета одобрена",
                        f"Администратор одобрил смету по проекту «{project.get('name', '')}». Подтвердите в личном кабинете.",
                        url_for("client_project", project_id=project_id),
                        project_id,
                    )
                flash("Смета одобрена. Заказчик получит уведомление.", "success")
            conn.close()
            return redirect(url_for("admin_project_edit", project_id=project_id))

        name = request.form.get("name", "").strip()
        building_type = request.form.get("type", "")
        address = request.form.get("address", "").strip()
        client_id = request.form.get("client_id", type=int)
        master_id = request.form.get("master_id", type=int)
        responsible_manager_id = request.form.get("responsible_manager_id", type=int)
        director_construction_id = request.form.get("director_construction_id", type=int)
        if role == "manager_op":
            responsible_manager_id = project.get("responsible_manager_id") or session["user_id"]
        if name and building_type and address and client_id and master_id and responsible_manager_id:
            photo_path = project.get("photo_path")
            photo = request.files.get("photo")
            if photo and photo.filename and allowed_file(photo.filename):
                archive_media(
                    "project_photo",
                    int(project_id),
                    by_user_id=session.get("user_id"),
                    reason="project_photo_replaced",
                )
                rel_dir = _storage_rel_dir_for("project_photo", project_id=int(project_id))
                try:
                    save_media_file(
                        photo,
                        project_id=int(project_id),
                        stage_id=None,
                        entity_type="project_photo",
                        entity_id=int(project_id),
                        uploaded_by_id=session.get("user_id"),
                        rel_dir=rel_dir,
                    )
                except Exception:
                    flash("Фото не удалось сохранить в защищённое хранилище.", "error")
            contract = request.files.get("contract")
            if contract and contract.filename and allowed_document_file(contract.filename):
                archive_media(
                    "project_contract",
                    int(project_id),
                    by_user_id=session.get("user_id"),
                    reason="project_contract_replaced",
                )
                rel_dir = _storage_rel_dir_for("project_contract", project_id=int(project_id))
                try:
                    save_media_file(
                        contract,
                        project_id=int(project_id),
                        stage_id=None,
                        entity_type="project_contract",
                        entity_id=int(project_id),
                        uploaded_by_id=session.get("user_id"),
                        rel_dir=rel_dir,
                    )
                    # Сброс согласования: менеджер загрузил — ждём одобрения админа
                    conn.execute(
                        "UPDATE projects SET contract_admin_approved_at = NULL, contract_confirmed_at = NULL WHERE id = ?",
                        (project_id,),
                    )
                    # Уведомление админам
                    for admin_row in conn.execute("SELECT id FROM users WHERE role = 'admin'").fetchall():
                        add_notification(
                            admin_row[0],
                            "document_upload",
                            "Договор загружен",
                            f"Менеджер ОП загрузил договор по проекту «{project.get('name', '')}». Требуется одобрение.",
                            url_for("admin_project_edit", project_id=project_id),
                            project_id,
                        )
                except Exception:
                    flash("Договор не удалось сохранить.", "error")
            estimate = request.files.get("estimate")
            if estimate and estimate.filename and allowed_document_file(estimate.filename):
                archive_media(
                    "project_estimate",
                    int(project_id),
                    by_user_id=session.get("user_id"),
                    reason="project_estimate_replaced",
                )
                rel_dir = _storage_rel_dir_for("project_estimate", project_id=int(project_id))
                try:
                    save_media_file(
                        estimate,
                        project_id=int(project_id),
                        stage_id=None,
                        entity_type="project_estimate",
                        entity_id=int(project_id),
                        uploaded_by_id=session.get("user_id"),
                        rel_dir=rel_dir,
                    )
                    conn.execute(
                        "UPDATE projects SET estimate_admin_approved_at = NULL, estimate_confirmed_at = NULL WHERE id = ?",
                        (project_id,),
                    )
                    for admin_row in conn.execute("SELECT id FROM users WHERE role = 'admin'").fetchall():
                        add_notification(
                            admin_row[0],
                            "document_upload",
                            "Смета загружена",
                            f"Менеджер ОП загрузил смету по проекту «{project.get('name', '')}». Требуется одобрение.",
                            url_for("admin_project_edit", project_id=project_id),
                            project_id,
                        )
                except Exception:
                    flash("Смету не удалось сохранить.", "error")
            proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
            if "director_construction_id" in proj_cols:
                dc_id = director_construction_id if building_type in ("frame", "gasblock", "penopolistirol") else None
                conn.execute(
                    """UPDATE projects SET name=?, type=?, address=?, client_id=?, master_id=?, photo_path=?,
                       responsible_manager_id=?, director_construction_id=?
                       WHERE id=?""",
                    (name, building_type, address, client_id, master_id, photo_path, responsible_manager_id, dc_id, project_id),
                )
            else:
                conn.execute(
                    """UPDATE projects SET name=?, type=?, address=?, client_id=?, master_id=?, photo_path=?, responsible_manager_id=?
                       WHERE id=?""",
                    (name, building_type, address, client_id, master_id, photo_path, responsible_manager_id, project_id),
                )
            # Для каркас/газоблок/пенополистирол: master_id — это прораб, синхронизируем foreman_project_access
            if building_type in ("frame", "gasblock", "penopolistirol") and master_id:
                conn.execute(
                    """INSERT OR IGNORE INTO foreman_project_access (foreman_id, project_id, assigned_by_id)
                       VALUES (?, ?, ?)""",
                    (master_id, project_id, session["user_id"] if role == "admin" else None),
                )
            _touch_project_responsible_activity(conn, int(project_id), session["user_id"])
            conn.commit()
            flash("Проект обновлён.", "success")
        conn.close()
        return redirect(url_for("admin_projects"))

    masters = [dict(r) for r in conn.execute("SELECT id, full_name FROM users WHERE role = 'master' ORDER BY full_name").fetchall()]
    foremen = [dict(r) for r in conn.execute("SELECT id, full_name FROM users WHERE role = 'foreman' ORDER BY full_name").fetchall()]
    clients = conn.execute("SELECT id, full_name FROM users WHERE role = 'client' ORDER BY full_name").fetchall()
    managers_op = conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'manager_op' ORDER BY full_name"
    ).fetchall()
    directors_construction = conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'director_construction' ORDER BY full_name"
    ).fetchall()
    conn.close()
    tokens = get_active_media_tokens_map("project_photo", [int(project_id)])
    project["photo_token"] = tokens.get(int(project_id))
    contract_tokens = get_active_media_tokens_map("project_contract", [int(project_id)])
    project["contract_token"] = contract_tokens.get(int(project_id))
    estimate_tokens = get_active_media_tokens_map("project_estimate", [int(project_id)])
    project["estimate_token"] = estimate_tokens.get(int(project_id))
    return render_template(
        "admin/project_edit.html",
        project=project,
        masters=masters,
        foremen=foremen,
        clients=clients,
        managers_op=managers_op,
        directors_construction=directors_construction,
        building_types=BUILDING_TYPES,
    )


@app.route("/admin/project/<int:project_id>/delete", methods=["POST"])
@projects_manager_required
def admin_project_delete(project_id):
    """Удаление проекта"""
    conn = get_db()
    proj = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    if not proj:
        conn.close()
        flash("Проект не найден.", "error")
        return redirect(url_for("admin_projects"))
    if session.get("role") == "manager_op":
        uid = session["user_id"]
        if proj.get("created_by_id") != uid and proj.get("responsible_manager_id") != uid:
            conn.close()
            flash("Доступ запрещён: это не ваш проект.", "error")
            return redirect(url_for("admin_projects"))
    project = dict(proj)

    # Медиа физически не удаляем — только архивируем
    archive_media_by_project(
        int(project_id),
        by_user_id=session.get("user_id"),
        reason="project_deleted",
    )

    stage_ids = [
        r[0]
        for r in conn.execute(
            "SELECT id FROM stages WHERE project_id = ?", (project_id,)
        ).fetchall()
    ]
    conn.execute("DELETE FROM edit_request_photos WHERE edit_request_id IN (SELECT id FROM edit_requests WHERE stage_id IN (SELECT id FROM stages WHERE project_id = ?))", (project_id,))
    conn.execute("DELETE FROM edit_requests WHERE stage_id IN (SELECT id FROM stages WHERE project_id = ?)", (project_id,))
    conn.execute("DELETE FROM project_chat_read WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM project_chat WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM project_takeover_requests WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM reports WHERE stage_id IN (SELECT id FROM stages WHERE project_id = ?)", (project_id,))
    conn.execute("DELETE FROM stages WHERE project_id = ?", (project_id,))
    conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
    conn.commit()
    conn.close()
    flash("Проект удалён.", "info")
    return redirect(url_for("admin_projects"))


@app.route("/admin/project/<int:project_id>/stages", methods=["GET", "POST"])
@projects_manager_required
def admin_project_stages(project_id):
    """Настройка этапов проекта"""
    conn = get_db()
    proj = conn.execute(
        """SELECT p.*, u.full_name as responsible_manager_name
           FROM projects p
           LEFT JOIN users u ON p.responsible_manager_id = u.id
           WHERE p.id = ?""",
        (project_id,),
    ).fetchone()
    if not proj:
        conn.close()
        flash("Проект не найден.", "error")
        return redirect(url_for("admin_projects"))
    if session.get("role") == "manager_op":
        uid = session["user_id"]
        if proj.get("created_by_id") != uid and proj.get("responsible_manager_id") != uid:
            conn.close()
            flash("Доступ запрещён: это не ваш проект.", "error")
            return redirect(url_for("admin_projects"))
    project = dict(proj)
    try:
        project["photo_token"] = get_active_media_tokens_map("project_photo", [int(project_id)]).get(int(project_id))
    except Exception:
        project["photo_token"] = None

    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            name = request.form.get("stage_name", "").strip()
            planned_date = request.form.get("stage_planned_date", "").strip() or None
            planned_start = request.form.get("stage_planned_start", "").strip() or None
            planned_end = request.form.get("stage_planned_end", "").strip() or None
            if name:
                max_order = conn.execute(
                    "SELECT COALESCE(MAX(order_num), 0) FROM stages WHERE project_id = ?",
                    (project_id,),
                ).fetchone()[0]
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(stages)")
                stage_cols = [r[1] for r in cursor.fetchall()]
                if "planned_start_date" in stage_cols and "planned_end_date" in stage_cols:
                    conn.execute(
                        """INSERT INTO stages (project_id, name, order_num, planned_date, planned_start_date, planned_end_date)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (project_id, name, max_order + 1, planned_date, planned_start or None, planned_end or None),
                    )
                else:
                    conn.execute(
                        "INSERT INTO stages (project_id, name, order_num, planned_date) VALUES (?, ?, ?, ?)",
                        (project_id, name, max_order + 1, planned_date),
                    )
                _touch_project_responsible_activity(conn, int(project_id), session["user_id"])
                conn.commit()
                flash("Этап добавлен.", "success")
        elif action == "delete":
            stage_id = request.form.get("stage_id", type=int)
            if stage_id:
                archive_media_by_stage(
                    int(stage_id),
                    by_user_id=session.get("user_id"),
                    reason="stage_deleted",
                )
                conn.execute("DELETE FROM stages WHERE id = ? AND project_id = ?", (stage_id, project_id))
                _touch_project_responsible_activity(conn, int(project_id), session["user_id"])
                conn.commit()
                flash("Этап удалён.", "success")
            conn.close()
            return redirect(url_for("admin_project_stages", project_id=project_id))
        elif action == "save_all":
            updated = 0
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(stages)")
            stage_cols = [r[1] for r in cursor.fetchall()]
            has_start_end = "planned_start_date" in stage_cols and "planned_end_date" in stage_cols
            for key in request.form:
                if key.startswith("stage_") and not key.startswith("stage_planned_"):
                    try:
                        stage_id = int(key[6:])
                        new_name = request.form.get(key, "").strip()
                        planned_date = request.form.get(f"stage_planned_{stage_id}", "").strip() or None
                        planned_start = request.form.get(f"stage_planned_start_{stage_id}", "").strip() or None
                        planned_end = request.form.get(f"stage_planned_end_{stage_id}", "").strip() or None
                        if new_name:
                            if has_start_end:
                                cur = conn.execute(
                                    """UPDATE stages SET name = ?, planned_date = ?, planned_start_date = ?, planned_end_date = ?
                                       WHERE id = ? AND project_id = ?""",
                                    (new_name, planned_date, planned_start or None, planned_end or None, stage_id, project_id),
                                )
                            else:
                                cur = conn.execute(
                                    "UPDATE stages SET name = ?, planned_date = ? WHERE id = ? AND project_id = ?",
                                    (new_name, planned_date, stage_id, project_id),
                                )
                            if cur.rowcount:
                                updated += 1
                    except (ValueError, TypeError):
                        pass
            _touch_project_responsible_activity(conn, int(project_id), session["user_id"])
            conn.commit()
            flash(f"Изменения сохранены. Обновлено этапов: {updated}.", "success")
            conn.close()
            return redirect(url_for("admin_project_stages", project_id=project_id))
        elif action == "edit":
            stage_id = request.form.get("stage_id", type=int)
            new_name = request.form.get("stage_name", "").strip()
            if stage_id and new_name:
                conn.execute(
                    "UPDATE stages SET name = ? WHERE id = ? AND project_id = ?",
                    (new_name, stage_id, project_id),
                )
                _touch_project_responsible_activity(conn, int(project_id), session["user_id"])
                conn.commit()
                flash("Этап обновлён.", "success")
        elif action == "edit_photo":
            photo = request.files.get("photo")
            if photo and photo.filename and allowed_file(photo.filename):
                archive_media(
                    "project_photo",
                    int(project_id),
                    by_user_id=session.get("user_id"),
                    reason="project_photo_replaced",
                )
                rel_dir = _storage_rel_dir_for("project_photo", project_id=int(project_id))
                try:
                    save_media_file(
                        photo,
                        project_id=int(project_id),
                        stage_id=None,
                        entity_type="project_photo",
                        entity_id=int(project_id),
                        uploaded_by_id=session.get("user_id"),
                        rel_dir=rel_dir,
                    )
                    _touch_project_responsible_activity(conn, int(project_id), session["user_id"])
                    conn.commit()
                    flash("Фото проекта обновлено.", "success")
                except Exception:
                    flash("Не удалось сохранить фото в защищённое хранилище.", "error")
        elif action == "complete" and session.get("role") == "admin":
            proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
            if "completed_at" in proj_cols:
                conn.execute(
                    "UPDATE projects SET completed_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (project_id,),
                )
                conn.commit()
                flash("Проект отмечен как завершённый. Он скрыт с дашборда.", "success")
        elif action == "reopen" and session.get("role") == "admin":
            proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
            if "completed_at" in proj_cols:
                conn.execute(
                    "UPDATE projects SET completed_at = NULL WHERE id = ?",
                    (project_id,),
                )
                conn.commit()
                flash("Проект снова отображается на дашборде.", "success")
        conn.close()
        return redirect(url_for("admin_project_stages", project_id=project_id))

    stages_raw = conn.execute(
        "SELECT * FROM stages WHERE project_id = ? ORDER BY order_num", (project_id,)
    ).fetchall()
    stages = [dict(s) for s in stages_raw]
    stage_chat = {}
    for s in stages:
        msgs = conn.execute(
            """SELECT pc.*, u.full_name as author_name
               FROM project_chat pc
               JOIN users u ON pc.user_id = u.id
               WHERE pc.project_id = ? AND pc.stage_id = ?
               ORDER BY pc.created_at ASC""",
            (project_id, s["id"]),
        ).fetchall()
        stage_chat[s["id"]] = [dict(m) for m in msgs]
    conn.close()
    return render_template("admin/stages.html", project=project, stages=stages, stage_chat=stage_chat, building_types=BUILDING_TYPES)


def _parse_date(s):
    """Парсинг даты YYYY-MM-DD в date или None"""
    if not s:
        return None
    try:
        return date(*[int(x) for x in str(s)[:10].split("-")])
    except (ValueError, TypeError):
        return None


def _default_month_range(today=None):
    """Диапазон дат по умолчанию: с 1 числа текущего месяца по сегодня."""
    today = today or date.today()
    start = today.replace(day=1)
    end = today
    return start, end


def _week_start(d: date) -> date:
    """Начало недели (понедельник) для даты."""
    return d - timedelta(days=d.weekday())


def _format_money(x):
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def _percent_end_to_label(p):
    """Преобразовать percent (верхняя граница) в строку диапазона."""
    try:
        p = float(p)
    except (TypeError, ValueError):
        return ""
    for start, end in PERCENT_RANGES:
        if abs(p - end) < 0.0001:
            return f"{start}-{end}"
    # fallback для старых/нестандартных данных
    if p.is_integer():
        return f"{int(p)}"
    return f"{p:.1f}".replace(".", ",")


def _get_max_percent_by_work_item(conn, project_id, user_role, exclude_daily_report_id=None, include_pending=True):
    """Максимальный прогресс по работе в проекте.

    По умолчанию учитывает pending+approved (чтобы «резервировать» диапазоны).
    rejected не учитывается.
    """
    where = ["dr.project_id = ?", "u.role = ?"]
    params = [project_id, user_role]
    if include_pending:
        where.append("COALESCE(it.approved_status, 'approved') IN ('pending','approved')")
    else:
        where.append("COALESCE(it.approved_status, 'approved') = 'approved'")
    if exclude_daily_report_id:
        where.append("dr.id != ?")
        params.append(exclude_daily_report_id)
    rows = conn.execute(
        f"""SELECT it.work_item_id as work_item_id, COALESCE(MAX(it.percent), 0) as max_percent
            FROM worker_daily_report_items it
            JOIN worker_daily_reports dr ON dr.id = it.daily_report_id
            JOIN users u ON u.id = dr.worker_id
            WHERE {' AND '.join(where)}
            GROUP BY it.work_item_id""",
        tuple(params),
    ).fetchall()
    return {int(r["work_item_id"]): float(r["max_percent"] or 0) for r in rows}


def _build_production_calendar_projects(conn, project_filter="all", director_user_id=None, manager_op_user_id=None):
    """Строит список проектов для производственного календаря.
    project_filter: 'all' | 'module' | 'construction' (frame/gasblock/penopolistirol)
    director_user_id: для фильтра module — ID директора по производству; для construction — ID директора по строительству
    manager_op_user_id: для менеджера ОП — только проекты, которые он создал или где он назначен ответственным
    """
    from datetime import date, timedelta
    if manager_op_user_id:
        projects_raw = conn.execute(
            """SELECT p.id, p.name, p.address, p.type, p.created_at, u.full_name as master_name
               FROM projects p
               LEFT JOIN users u ON p.master_id = u.id
               WHERE (p.created_by_id = ? OR p.responsible_manager_id = ?)
               ORDER BY p.name""",
            (manager_op_user_id, manager_op_user_id),
        ).fetchall()
    elif project_filter == "module" and director_user_id:
        projects_raw = conn.execute(
            """SELECT p.id, p.name, p.address, p.type, p.created_at, u.full_name as master_name
               FROM projects p
               LEFT JOIN users u ON p.master_id = u.id
               WHERE p.type = 'module' AND u.reports_to_production_id = ?
               ORDER BY p.name""",
            (director_user_id,),
        ).fetchall()
    elif project_filter == "construction" and director_user_id:
        proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
        if "director_construction_id" in proj_cols:
            projects_raw = conn.execute(
                """SELECT p.id, p.name, p.address, p.type, p.created_at,
                      (SELECT u2.full_name FROM foreman_project_access fpa2
                       JOIN users u2 ON u2.id = fpa2.foreman_id
                       WHERE fpa2.project_id = p.id LIMIT 1) as master_name
                   FROM projects p
                   WHERE p.type IN ('frame', 'gasblock', 'penopolistirol')
                     AND (p.director_construction_id = ?
                          OR EXISTS (
                              SELECT 1 FROM foreman_project_access fpa
                              JOIN users u ON u.id = fpa.foreman_id
                              WHERE fpa.project_id = p.id AND u.reports_to_construction_id = ?
                          ))
                   ORDER BY p.name""",
                (director_user_id, director_user_id),
            ).fetchall()
        else:
            projects_raw = conn.execute(
                """SELECT DISTINCT p.id, p.name, p.address, p.type, p.created_at,
                      (SELECT u2.full_name FROM foreman_project_access fpa2
                       JOIN users u2 ON u2.id = fpa2.foreman_id
                       WHERE fpa2.project_id = p.id LIMIT 1) as master_name
                   FROM projects p
                   JOIN foreman_project_access fpa ON fpa.project_id = p.id
                   JOIN users u ON u.id = fpa.foreman_id
                   WHERE p.type IN ('frame', 'gasblock', 'penopolistirol')
                     AND u.reports_to_construction_id = ?
                   ORDER BY p.name""",
                (director_user_id,),
            ).fetchall()
    else:
        projects_raw = conn.execute(
            """SELECT p.id, p.name, p.address, p.type, p.created_at, u.full_name as master_name
               FROM projects p
               LEFT JOIN users u ON p.master_id = u.id
               ORDER BY p.name"""
        ).fetchall()
    today = date.today()
    projects = []
    all_dates = [today]
    for proj in projects_raw:
        p = dict(proj)
        proj_start = _parse_date(p.get("created_at")) or today - timedelta(days=30)
        all_dates.append(proj_start)
        stages_raw = conn.execute(
            """SELECT id, name, order_num, planned_date, planned_start_date, planned_end_date
               FROM stages WHERE project_id = ? ORDER BY order_num""",
            (p["id"],),
        ).fetchall()
        stages = []
        prev_planned_end = proj_start
        for s in stages_raw:
            first_report = conn.execute(
                "SELECT MIN(created_at) as completed_at FROM reports WHERE stage_id = ?",
                (s["id"],),
            ).fetchone()
            completed_at_raw = first_report["completed_at"] if first_report and first_report["completed_at"] else None
            planned_start = _parse_date(s["planned_start_date"]) or prev_planned_end
            planned_end = _parse_date(s["planned_end_date"]) or _parse_date(s["planned_date"])
            if not planned_end:
                planned_end = (planned_start or date.today()) + timedelta(days=7)
            actual_end = _parse_date(completed_at_raw[:10] if completed_at_raw else None)
            if planned_start:
                all_dates.append(planned_start)
            if planned_end:
                all_dates.append(planned_end)
                prev_planned_end = planned_end
            else:
                prev_planned_end = (prev_planned_end or date.today()) + timedelta(days=7)
            if actual_end:
                all_dates.append(actual_end)
            overdue = (actual_end and planned_end and actual_end > planned_end) or (
                not actual_end and planned_end and planned_end < today
            )
            days_diff = None
            if actual_end and planned_end:
                days_diff = (planned_end - actual_end).days  # >0 раннее, <0 просрочка
            stage_overdue_days = 0
            if actual_end and planned_end and actual_end > planned_end:
                stage_overdue_days = (actual_end - planned_end).days
            elif not actual_end and planned_end and planned_end < today:
                stage_overdue_days = (today - planned_end).days
            stages.append({
                "stage_id": s["id"],
                "stage_name": s["name"],
                "order_num": s["order_num"],
                "planned_date": s["planned_date"],
                "completed_at": completed_at_raw[:10] if completed_at_raw else None,
                "planned_start": planned_start,
                "planned_end": planned_end,
                "actual_end": actual_end,
                "overdue": overdue,
                "days_diff": days_diff,
                "overdue_days": stage_overdue_days,
            })
        p["stages"] = stages
        p["type_label"] = BUILDING_TYPES.get(p.get("type", ""), p.get("type", ""))
        p["overdue_days"] = max([s["overdue_days"] for s in stages], default=0)
        projects.append(p)
    date_min = min(all_dates) if all_dates else date.today()
    date_max = max(all_dates) if all_dates else date.today()
    if date_max - date_min < timedelta(days=30):
        date_max = date_min + timedelta(days=60)
    date_range_days = max(1, (date_max - date_min).days)

    def _pos(d, dmin=date_min, drange=date_range_days):
        if not d:
            return 0
        return max(0, min(100, (d - dmin).days / drange * 100))

    for p in projects:
        for s in p["stages"]:
            ps = s.get("planned_start") or date_min
            pe = s.get("planned_end") or (ps + timedelta(days=7))
            ae = s.get("actual_end")
            s["left_pct"] = _pos(ps)
            s["green_width_pct"] = max(1, _pos(pe) - _pos(ps))
            s["red_width_pct"] = 0
            s["red_left_pct"] = 0
            s["actual_left_pct"] = 0
            s["actual_width_pct"] = 0
            s["show_actual_bar"] = False
            if ae:
                s["show_actual_bar"] = True
                s["actual_left_pct"] = _pos(ae)
                s["actual_width_pct"] = max(2, 100 / date_range_days * 1.5)
                if pe and ae > pe:
                    s["red_left_pct"] = _pos(pe)
                    s["red_width_pct"] = max(1, _pos(ae) - _pos(pe))
            elif not ae and pe and pe < today:
                # Просрочка «не сдан»: красная полоса от planned_end до сегодня
                s["red_left_pct"] = _pos(pe)
                s["red_width_pct"] = max(1, _pos(today) - _pos(pe))
    return projects, date_min, date_max, date_range_days


def _working_days_between(d1, d2):
    """Количество рабочих дней (пн–пт) между датами включительно."""
    if not d1 or not d2 or d1 > d2:
        return 0
    days = 0
    d = d1
    while d <= d2:
        if d.weekday() < 5:
            days += 1
        d += timedelta(days=1)
    return days


def _build_production_analytics_full(conn):
    """Полная аналитика по направлениям: модульные и строительство.
    Возвращает stats_module, stats_construction, by_direction.
    """
    from datetime import date, timedelta

    def _calc_stats(projects_raw):
        total_projects = len(projects_raw)
        total_stages = stages_completed = stages_ontime = stages_early = stages_late = 0
        total_delay_days = total_early_days = 0
        total_planned_wd = total_actual_wd = 0
        projects_with_delays = set()
        by_person = {}
        for proj in projects_raw:
            p = dict(proj)
            resp_name = p.get("resp_name") or "—"
            if resp_name not in by_person:
                by_person[resp_name] = {"total": 0, "completed": 0, "ontime": 0, "late": 0, "early": 0, "delay_days": 0, "early_days": 0}
            stages_raw = conn.execute(
                """SELECT id, planned_start_date, planned_end_date, planned_date
                   FROM stages WHERE project_id = ? ORDER BY order_num""",
                (p["id"],),
            ).fetchall()
            for s in stages_raw:
                total_stages += 1
                by_person[resp_name]["total"] += 1
                first_report = conn.execute(
                    "SELECT MIN(created_at) as completed_at FROM reports WHERE stage_id = ?",
                    (s["id"],),
                ).fetchone()
                completed_at_raw = first_report["completed_at"] if first_report and first_report["completed_at"] else None
                planned_start = _parse_date(s["planned_start_date"])
                planned_end = _parse_date(s["planned_end_date"]) or _parse_date(s["planned_date"])
                if not planned_end:
                    planned_end = (planned_start or date.today()) + timedelta(days=7)
                actual_end = _parse_date(completed_at_raw[:10] if completed_at_raw else None)
                if planned_start and planned_end:
                    total_planned_wd += _working_days_between(planned_start, planned_end)
                if actual_end and (planned_start or planned_end):
                    start = planned_start or (planned_end - timedelta(days=7))
                    total_actual_wd += _working_days_between(start, actual_end)
                if actual_end:
                    stages_completed += 1
                    by_person[resp_name]["completed"] += 1
                    if planned_end:
                        days_diff = (planned_end - actual_end).days
                        if days_diff >= 0:
                            stages_ontime += 1
                            by_person[resp_name]["ontime"] += 1
                            if days_diff > 0:
                                stages_early += 1
                                total_early_days += days_diff
                                by_person[resp_name]["early"] += 1
                                by_person[resp_name]["early_days"] += days_diff
                        else:
                            stages_late += 1
                            total_delay_days += -days_diff
                            projects_with_delays.add(p["id"])
                            by_person[resp_name]["late"] += 1
                            by_person[resp_name]["delay_days"] += -days_diff
        pct_ontime = round(stages_ontime / stages_completed * 100, 1) if stages_completed else 0
        avg_delay = round(total_delay_days / stages_late, 1) if stages_late else 0
        avg_early = round(total_early_days / stages_early, 1) if stages_early else 0
        eff_ratio = round(total_planned_wd / total_actual_wd, 2) if total_actual_wd else 0
        return {
            "total_projects": total_projects,
            "total_stages": total_stages,
            "stages_completed": stages_completed,
            "stages_in_progress": total_stages - stages_completed,
            "stages_ontime": stages_ontime,
            "stages_early": stages_early,
            "stages_late": stages_late,
            "pct_ontime": pct_ontime,
            "avg_delay_days": avg_delay,
            "avg_early_days": avg_early,
            "total_delay_days": total_delay_days,
            "total_early_days": total_early_days,
            "projects_with_delays": len(projects_with_delays),
            "planned_working_days": total_planned_wd,
            "actual_working_days": total_actual_wd,
            "efficiency_ratio": eff_ratio,
            "by_person": by_person,
        }

    module_raw = conn.execute(
        """SELECT p.id, p.name, u.full_name as resp_name
           FROM projects p LEFT JOIN users u ON p.master_id = u.id
           WHERE p.type = 'module' ORDER BY p.name"""
    ).fetchall()
    construction_raw = conn.execute(
        """SELECT DISTINCT p.id, p.name,
              (SELECT u2.full_name FROM foreman_project_access fpa2
               JOIN users u2 ON u2.id = fpa2.foreman_id
               WHERE fpa2.project_id = p.id LIMIT 1) as resp_name
           FROM projects p
           WHERE p.type IN ('frame', 'gasblock', 'penopolistirol')
           ORDER BY p.name"""
    ).fetchall()
    stats_module = _calc_stats(module_raw)
    stats_construction = _calc_stats(construction_raw)
    stats_all = {
        "total_projects": stats_module["total_projects"] + stats_construction["total_projects"],
        "total_stages": stats_module["total_stages"] + stats_construction["total_stages"],
        "stages_completed": stats_module["stages_completed"] + stats_construction["stages_completed"],
        "stages_ontime": stats_module["stages_ontime"] + stats_construction["stages_ontime"],
        "stages_early": stats_module["stages_early"] + stats_construction["stages_early"],
        "stages_late": stats_module["stages_late"] + stats_construction["stages_late"],
        "total_delay_days": stats_module["total_delay_days"] + stats_construction["total_delay_days"],
        "total_early_days": stats_module["total_early_days"] + stats_construction["total_early_days"],
        "projects_with_delays": stats_module["projects_with_delays"] + stats_construction["projects_with_delays"],
    }
    tc = stats_all["stages_completed"] or 1
    stats_all["pct_ontime"] = round(stats_all["stages_ontime"] / tc * 100, 1)
    return {
        "module": stats_module,
        "construction": stats_construction,
        "all": stats_all,
    }


def _build_module_production_analytics(conn, director_user_id=None):
    """Аналитика цеха модульных домов: соблюдение сроков, эффективность.
    director_user_id=None — все модульные (админ); иначе — только проекты мастеров этого директора.
    """
    from datetime import date, timedelta
    if director_user_id:
        projects_raw = conn.execute(
            """SELECT p.id, p.name, u.full_name as master_name
               FROM projects p
               LEFT JOIN users u ON p.master_id = u.id
               WHERE p.type = 'module' AND u.reports_to_production_id = ?
               ORDER BY p.name""",
            (director_user_id,),
        ).fetchall()
    else:
        projects_raw = conn.execute(
            """SELECT p.id, p.name, u.full_name as master_name
               FROM projects p
               LEFT JOIN users u ON p.master_id = u.id
               WHERE p.type = 'module'
               ORDER BY p.name""",
        ).fetchall()
    total_projects = len(projects_raw)
    total_stages = 0
    stages_completed = 0
    stages_ontime = 0  # вовремя или раньше
    stages_early = 0
    stages_late = 0
    total_delay_days = 0
    total_early_days = 0
    projects_with_delays = set()
    by_master = {}
    for proj in projects_raw:
        p = dict(proj)
        master_name = p.get("master_name") or "—"
        if master_name not in by_master:
            by_master[master_name] = {"total": 0, "completed": 0, "ontime": 0, "late": 0, "early": 0, "delay_days": 0, "early_days": 0}
        stages_raw = conn.execute(
            """SELECT id, name, order_num, planned_start_date, planned_end_date, planned_date
               FROM stages WHERE project_id = ? ORDER BY order_num""",
            (p["id"],),
        ).fetchall()
        for s in stages_raw:
            total_stages += 1
            by_master[master_name]["total"] += 1
            first_report = conn.execute(
                "SELECT MIN(created_at) as completed_at FROM reports WHERE stage_id = ?",
                (s["id"],),
            ).fetchone()
            completed_at_raw = first_report["completed_at"] if first_report and first_report["completed_at"] else None
            planned_end = _parse_date(s["planned_end_date"]) or _parse_date(s["planned_date"])
            actual_end = _parse_date(completed_at_raw[:10] if completed_at_raw else None)
            if actual_end:
                stages_completed += 1
                by_master[master_name]["completed"] += 1
                if planned_end:
                    days_diff = (planned_end - actual_end).days
                    if days_diff >= 0:
                        stages_ontime += 1
                        by_master[master_name]["ontime"] += 1
                        if days_diff > 0:
                            stages_early += 1
                            total_early_days += days_diff
                            by_master[master_name]["early"] += 1
                            by_master[master_name]["early_days"] += days_diff
                    else:
                        stages_late += 1
                        total_delay_days += -days_diff
                        projects_with_delays.add(p["id"])
                        by_master[master_name]["late"] += 1
                        by_master[master_name]["delay_days"] += -days_diff
    pct_ontime = round(stages_ontime / stages_completed * 100, 1) if stages_completed else 0
    avg_delay = round(total_delay_days / stages_late, 1) if stages_late else 0
    avg_early = round(total_early_days / stages_early, 1) if stages_early else 0
    return {
        "total_projects": total_projects,
        "total_stages": total_stages,
        "stages_completed": stages_completed,
        "stages_in_progress": total_stages - stages_completed,
        "stages_ontime": stages_ontime,
        "stages_early": stages_early,
        "stages_late": stages_late,
        "pct_ontime": pct_ontime,
        "avg_delay_days": avg_delay,
        "avg_early_days": avg_early,
        "total_delay_days": total_delay_days,
        "total_early_days": total_early_days,
        "projects_with_delays": len(projects_with_delays),
        "by_master": by_master,
    }


@app.route("/admin/calendar")
@projects_manager_required
def admin_production_calendar():
    """Производственный календарь: все проекты (админ) или только свои (менеджер ОП)"""
    conn = get_db()
    role = session.get("role")
    manager_op_id = session["user_id"] if role == "manager_op" else None
    projects, date_min, date_max, date_range_days = _build_production_calendar_projects(
        conn, "all", manager_op_user_id=manager_op_id
    )
    conn.close()
    return render_template(
        "admin/production_calendar.html",
        projects=projects,
        date_min=date_min.isoformat(),
        date_max=date_max.isoformat(),
        date_range_days=date_range_days,
        calendar_role="admin",
    )


@app.route("/director/production/calendar")
@director_production_required
def director_production_calendar():
    """Производственный календарь директора по производству: только модульные дома"""
    conn = get_db()
    projects, date_min, date_max, date_range_days = _build_production_calendar_projects(
        conn, "module", session["user_id"]
    )
    conn.close()
    return render_template(
        "director/production_calendar.html",
        projects=projects,
        date_min=date_min.isoformat(),
        date_max=date_max.isoformat(),
        date_range_days=date_range_days,
    )


@app.route("/director/production/analytics")
@director_production_required
def director_production_analytics():
    """Аналитика цеха модульных домов: соблюдение сроков, эффективность"""
    conn = get_db()
    stats = _build_module_production_analytics(conn, session["user_id"])
    conn.close()
    return render_template(
        "director/production_analytics.html",
        stats=stats,
        scope="director",
    )


@app.route("/admin/analytics/production-report")
@admin_required
def admin_production_report():
    """Отчёт по производственному календарю — только админ и супер-админ.
    Эффективность направлений, соблюдение сроков, рабочие дни.
    """
    conn = get_db()
    data = _build_production_analytics_full(conn)
    conn.close()
    return render_template(
        "admin/production_report.html",
        data=data,
    )


@app.route("/director/construction/calendar")
@director_construction_required
def director_construction_calendar():
    """Производственный календарь директора по строительству: каркас, газобетон, пенополистиролбетон"""
    conn = get_db()
    projects, date_min, date_max, date_range_days = _build_production_calendar_projects(
        conn, "construction", session["user_id"]
    )
    conn.close()
    return render_template(
        "director/construction_calendar.html",
        projects=projects,
        date_min=date_min.isoformat(),
        date_max=date_max.isoformat(),
        date_range_days=date_range_days,
    )


@app.route("/admin/amocrm/settings", methods=["GET", "POST"])
@admin_required
def admin_amocrm_settings():
    """Настройки интеграции amoCRM: subdomain и долгосрочный токен."""
    conn = get_db()
    if request.method == "POST":
        action = request.form.get("action")
        subdomain = (request.form.get("subdomain") or "").strip()
        token = (request.form.get("access_token") or "").strip()
        if action == "save" and subdomain and token:
            conn.execute(
                "INSERT OR REPLACE INTO amocrm_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
                ("subdomain", subdomain),
            )
            conn.execute(
                "INSERT OR REPLACE INTO amocrm_settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
                ("access_token", token),
            )
            conn.commit()
            flash("Настройки amoCRM сохранены.", "success")
        conn.close()
        return redirect(url_for("admin_amocrm_settings"))

    rows = conn.execute("SELECT key, value FROM amocrm_settings").fetchall()
    settings = {r["key"]: r["value"] for r in rows}
    # Не показываем токен полностью — только первые/последние символы
    token_display = ""
    if settings.get("access_token"):
        t = settings["access_token"]
        token_display = f"{t[:12]}…{t[-4:]}" if len(t) > 20 else "••••••••"
    conn.close()
    return render_template(
        "admin/amocrm_settings.html",
        subdomain=settings.get("subdomain", ""),
        token_display=token_display,
        has_token=bool(settings.get("access_token")),
    )


@app.route("/admin/amocrm/test", methods=["POST"])
@admin_required
def admin_amocrm_test():
    """Проверить подключение к amoCRM."""
    import amocrm_api
    ok, msg = amocrm_api.amocrm_test_connection()
    flash(msg, "success" if ok else "error")
    return redirect(url_for("admin_amocrm_settings"))


def _build_amocrm_potential_clients_rows():
    """Собрать строки отчёта потенциальных клиентов из amoCRM."""
    import amocrm_api

    leads, err = amocrm_api.amocrm_get_all_leads()
    if err:
        return None, err

    pipelines, _pipe_err = amocrm_api.amocrm_get_pipelines()
    if not pipelines:
        pipelines = []
    users_list, users_err = amocrm_api.amocrm_get_users()
    if users_err:
        users_list = []
    users_map = {u["id"]: u["name"] for u in (users_list or [])}

    def _parse_num(v):
        if v is None:
            return None
        s = str(v).strip().replace(" ", "").replace(",", ".")
        if not s:
            return None
        try:
            f = float(s)
            return int(f) if abs(f - int(f)) < 1e-9 else f
        except Exception:
            return None

    def _fmt_num(v):
        n = _parse_num(v)
        if n is None:
            return "null"
        if isinstance(n, int):
            return f"{n:,}".replace(",", " ")
        return f"{n}".replace(".", ",")

    # Метаданные полей сделок amoCRM (по именам, чтобы не зависеть только от hardcoded ID)
    lead_fields_data, _cf_status, _cf_err = amocrm_api.amocrm_request("GET", "leads/custom_fields")
    lead_fields = (lead_fields_data or {}).get("_embedded", {}).get("custom_fields", [])

    def _norm(s):
        return (s or "").strip().lower()

    def _resolve_field_id(exact_names=None, contains=None):
        exact = {_norm(n) for n in (exact_names or [])}
        contains = [_norm(c) for c in (contains or [])]
        # 1) exact
        for f in lead_fields:
            nm = _norm(f.get("name"))
            if nm and nm in exact:
                return f.get("id")
        # 2) contains
        for f in lead_fields:
            nm = _norm(f.get("name"))
            if not nm:
                continue
            for c in contains:
                if c and c in nm:
                    return f.get("id")
        return None

    def _cf_value(lead, field_id):
        if not field_id:
            return None
        for cf in (lead.get("custom_fields_values") or []):
            if int(cf.get("field_id") or 0) != int(field_id):
                continue
            vals = cf.get("values") or []
            if not vals:
                return None
            first = vals[0]
            if isinstance(first, dict):
                return first.get("value")
            return first
        return None

    def _cf_by_names(lead, names):
        wanted = {_norm(n) for n in names}
        for cf in (lead.get("custom_fields_values") or []):
            nm = _norm(cf.get("field_name"))
            if nm in wanted:
                vals = cf.get("values") or []
                if not vals:
                    return None
                first = vals[0]
                if isinstance(first, dict):
                    return first.get("value")
                return first
        return None

    # Ищем реальные поля по именам в amoCRM
    CF_PROJECT_NAME = _resolve_field_id(
        exact_names=["Название прогнозируемого проект", "Название прогнозируемого проекта"],
        contains=["название прогнозируемого проект"],
    )
    CF_PAYMENT_METHOD = _resolve_field_id(
        exact_names=["Способ оплаты"],
        contains=["способ оплаты", "форма оплаты"],
    )
    CF_TOTAL_PRICE = _resolve_field_id(
        exact_names=["Итоговая стоимость договора"],
        contains=["итоговая стоимость договора"],
    )
    CF_MODULES_COUNT = _resolve_field_id(
        exact_names=["Количество модулей в проекте", "Количество модулей"],
        contains=["количество модул"],
    )
    CF_BUILD_AREA = _resolve_field_id(
        exact_names=["Площадь застройки"],
        contains=["площадь застройки"],
    )
    CF_HOUSE_AREA = _resolve_field_id(
        exact_names=["Площадь дома"],
        contains=["площадь дома", "планируемая площадь"],
    )
    CF_EXTRAS = _resolve_field_id(
        exact_names=["Допы"],
        contains=["допы"],
    )
    # В вашем amoCRM рабочее поле комментария — 1640257.
    # Оставляем fallback по имени на случай миграции/переименования.
    CF_COMMENT = 1640257 if any(int(f.get("id") or 0) == 1640257 for f in lead_fields) else _resolve_field_id(
        exact_names=["Комментарий"],
        contains=["комментарий"],
    )

    # Бизнес-правило для отчёта "Потенциальные клиенты":
    # - только воронка "Продажи"
    # - статусы: "Потенциальный клиент" и "проект согласован"
    # - не включаем "договор подписан" и этапы после него
    sales_pipeline = None
    for p in pipelines:
        if (p.get("name") or "").strip().lower() == "продажи":
            sales_pipeline = p
            break

    allowed_status_ids = set()
    if sales_pipeline:
        statuses = sales_pipeline.get("statuses", [])
        allowed_status_names = {"потенциальный клиент", "проект согласован"}
        for s in statuses:
            s_name = (s.get("name") or "").strip().lower()
            s_id = s.get("id")
            if not s_id:
                continue
            if s_name in allowed_status_names:
                allowed_status_ids.add(s_id)

    leads_for_dashboard = []
    for lead in leads:
        if not sales_pipeline:
            continue
        if int(lead.get("pipeline_id") or 0) != int(sales_pipeline.get("id") or 0):
            continue
        if int(lead.get("status_id") or 0) not in allowed_status_ids:
            continue
        leads_for_dashboard.append(lead)

    rows = []
    for lead in leads_for_dashboard:
        responsible = users_map.get(lead.get("responsible_user_id"), "null")
        if responsible not in (None, "", "null"):
            responsible = " ".join(str(responsible).split())
        project_name = _cf_value(lead, CF_PROJECT_NAME)
        if project_name is None:
            project_name = _cf_by_names(lead, ["Название прогнозируемого проекта", "Название прогнозируемого проект"])

        payment_method = _cf_value(lead, CF_PAYMENT_METHOD)
        total_price = _cf_value(lead, CF_TOTAL_PRICE)
        modules_count = _cf_value(lead, CF_MODULES_COUNT)
        build_area = _cf_value(lead, CF_BUILD_AREA)
        house_area = _cf_value(lead, CF_HOUSE_AREA)
        extras = _cf_value(lead, CF_EXTRAS)
        comment = _cf_value(lead, CF_COMMENT)
        if house_area is None:
            house_area = _cf_by_names(lead, ["Площадь дома", "Планируемая площадь:"])
        if extras is None:
            extras = _cf_by_names(lead, ["Допы", "Долы"])
        if comment is None:
            comment = _cf_by_names(lead, ["Комментарий"])

        modules_area_sqm = _amocrm_modules_to_sqm(modules_count)
        crm_house_area_num = _amocrm_parse_num(house_area)
        display_area_sqm = crm_house_area_num if (crm_house_area_num and crm_house_area_num > 0) else modules_area_sqm
        needs_house_area_fill = bool(modules_area_sqm and (not crm_house_area_num or crm_house_area_num <= 0))

        # В вашем аккаунте поле "Допы" имеет тип url и может приходить как "http://252000"
        if isinstance(extras, str) and extras.strip().lower().startswith("http://"):
            raw = extras.strip()[7:]
            if raw.replace(" ", "").isdigit():
                extras = raw

        payment_value = (str(payment_method).strip() if payment_method not in (None, "") else "null")
        if payment_value != "null":
            payment_value = " ".join(payment_value.split())
        row = {
            "lead_id": lead.get("id"),
            "project_name": (str(project_name).strip() if project_name not in (None, "") else "null"),
            "responsible": (str(responsible).strip() if responsible not in (None, "") else "null"),
            "payment_method": payment_value,
            "total_price": _fmt_num(total_price),
            "area_sqm": _amocrm_fmt_sqm(display_area_sqm),
            "build_area": _fmt_num(build_area),
            "house_area": (str(house_area).strip() if house_area not in (None, "") else "null"),
            "needs_house_area_fill": needs_house_area_fill,
            "extras": (str(extras).strip() if extras not in (None, "") else "null"),
            "comment": (str(comment).strip() if comment not in (None, "") else "null"),
        }
        rows.append(row)

    payload = {
        "rows": rows,
        "generated_at": project_now().strftime("%d.%m.%Y %H:%M:%S"),
        "version": 5,
    }
    return payload, None


AMOCRM_SOURCE_TAG_ALIASES = {
    "Тильда": ["tilda"],
    "Чат АМО": ["Чат"],
    "Сарафан": ["Сарафан"],
    "8800": ["8-800"],
    "Соцсети": ["TG_Chanel", "VK"],
    "Домклик": ["Домклик"],
    "Вячеслав": ["Вячеслав газобетон", "Вячеслав каркасные", "Flexbe"],
    "Авито": ["Авито"],
    "Каркас Тайги": ["Каркас Тайги"],
    "Ассет": ["Ассет"],
    "СРА": ["СРА"],
    "ВК таргет": ["ВК таргет"],
}

AMOCRM_SOURCE_TAGS = list(AMOCRM_SOURCE_TAG_ALIASES.keys())

MODULE_PLAN_CATEGORIES = [
    ("modular", "Модульное строительство"),
    ("frame_site", "Каркасное строительство на участке"),
    ("gazoblock_site", "Газоблочное строительство на участке"),
    ("prefab_beton_site", "Префаб/бетон на участке"),
]

MODULE_TO_SQM = 18.0


def _amocrm_norm_tag_name(v):
    return " ".join(str(v or "").strip().lower().split())


def _amocrm_week_start_tuesday(d: date) -> date:
    # Tuesday=1 in Python's weekday() where Monday=0.
    shift = (d.weekday() - 1) % 7
    return d - timedelta(days=shift)


def _amocrm_sources_week_starts(base_day: date | None = None) -> list[date]:
    """Окно отчёта: прошлая неделя, текущая и 3 следующие."""
    base = base_day or date.today()
    current = _amocrm_week_start_tuesday(base)
    return [current + timedelta(days=7 * i) for i in (-1, 0, 1, 2, 3)]


def _amocrm_week_label(week_start_iso: str) -> str:
    try:
        ws = date.fromisoformat(str(week_start_iso))
        we = ws + timedelta(days=6)
        return f"{ws.strftime('%d.%m')}-{we.strftime('%d.%m')}"
    except Exception:
        return str(week_start_iso or "—")


def _build_amocrm_sources_weekly_rows():
    """Собрать недельный отчёт по источникам из тегов сделок amoCRM."""
    import amocrm_api

    leads, err = amocrm_api.amocrm_get_all_leads()
    if err:
        return None, err
    pipelines, pipes_err = amocrm_api.amocrm_get_pipelines()
    if pipes_err:
        return None, pipes_err

    def _lead_has_phone(lead_obj) -> bool:
        # В отчёте источник "Авито" считаем только для лидов с телефоном в сделке.
        cfs = lead_obj.get("custom_fields_values") or []
        for cf in cfs:
            field_name = str(cf.get("field_name") or cf.get("field_code") or "").strip().lower()
            if ("тел" not in field_name) and ("phone" not in field_name):
                continue
            for v in (cf.get("values") or []):
                raw = v.get("value")
                if raw in (None, ""):
                    continue
                digits = "".join(ch for ch in str(raw) if ch.isdigit())
                if len(digits) >= 10:
                    return True
        return False

    def _lead_is_qual(lead_obj) -> bool:
        # Квал считаем только по полю квалификации сделки (Квал/Неквал).
        cfs = lead_obj.get("custom_fields_values") or []
        for cf in cfs:
            field_name = _amocrm_norm_tag_name(cf.get("field_name") or cf.get("field_code") or "")
            if ("квалиф" not in field_name) and ("квали" not in field_name):
                continue
            for v in (cf.get("values") or []):
                val_norm = _amocrm_norm_tag_name(v.get("value"))
                if not val_norm:
                    continue
                if val_norm.startswith("неквал"):
                    return False
                if val_norm == "квал" or val_norm.startswith("квал "):
                    return True
        return False

    week_starts = _amocrm_sources_week_starts()
    week_keys = [d.isoformat() for d in week_starts]
    current_month = project_now().date().replace(day=1)
    month_starts = []
    for i in (-1, 0, 1, 2, 3):
        m = current_month.month + i
        y = current_month.year + (m - 1) // 12
        mm = ((m - 1) % 12) + 1
        month_starts.append(date(y, mm, 1))
    month_keys = [m.isoformat() for m in month_starts]
    source_aliases_norm = {
        col: {_amocrm_norm_tag_name(a) for a in aliases if _amocrm_norm_tag_name(a)}
        for col, aliases in AMOCRM_SOURCE_TAG_ALIASES.items()
    }
    avito_norm = _amocrm_norm_tag_name("Авито")
    sales_pipeline_ids = {
        int(p.get("id"))
        for p in (pipelines or [])
        if _amocrm_norm_tag_name(p.get("name")) == "продажи"
    }
    if not sales_pipeline_ids:
        return None, "Воронка 'Продажи' не найдена в amoCRM."

    by_week = {
        wk: {
            "week_start": wk,
            "total_leads": 0,
            "sources": {tag: {"leads": 0, "qual": 0} for tag in AMOCRM_SOURCE_TAGS},
        }
        for wk in week_keys
    }
    by_month = {
        mk: {
            "month_start": mk,
            "actual_leads": 0,
            "actual_qual_leads": 0,
        }
        for mk in month_keys
    }

    for lead in leads or []:
        try:
            pipeline_id = int(lead.get("pipeline_id") or 0)
        except Exception:
            pipeline_id = 0
        if pipeline_id not in sales_pipeline_ids:
            continue
        created_at = lead.get("created_at")
        try:
            created_day = datetime.fromtimestamp(int(created_at), tz=timezone.utc).astimezone(PROJECT_TZ).date()
        except Exception:
            continue
        week_key = _amocrm_week_start_tuesday(created_day).isoformat()
        if week_key not in by_week:
            continue
        month_key = created_day.replace(day=1).isoformat()
        if month_key not in by_month:
            continue

        tags = ((lead.get("_embedded") or {}).get("tags") or [])
        lead_tags_norm = {_amocrm_norm_tag_name(t.get("name")) for t in tags if t.get("name")}
        has_phone = _lead_has_phone(lead)
        is_qual = _lead_is_qual(lead)

        matched_cols = []
        for col_name, aliases_norm in source_aliases_norm.items():
            if col_name and _amocrm_norm_tag_name(col_name) == avito_norm and not has_phone:
                continue
            if aliases_norm.intersection(lead_tags_norm):
                matched_cols.append(col_name)

        # Приоритет тегов: если есть одновременно 8-800 и Авито, считаем только Авито.
        if "Авито" in matched_cols and "8800" in matched_cols:
            matched_cols = [c for c in matched_cols if c != "8800"]

        for col_name in matched_cols:
            by_week[week_key]["sources"][col_name]["leads"] += 1
            if is_qual:
                by_week[week_key]["sources"][col_name]["qual"] += 1

        if matched_cols:
            by_week[week_key]["total_leads"] += 1
            by_month[month_key]["actual_leads"] += 1
            if is_qual:
                by_month[month_key]["actual_qual_leads"] += 1

    payload = {
        "weeks": [by_week[wk] for wk in week_keys],
        "months": [by_month[mk] for mk in month_keys],
        "generated_at": project_now().strftime("%d.%m.%Y %H:%M:%S"),
        "version": 5,
    }
    return payload, None


def _amocrm_sources_manual_map(conn):
    try:
        rows = conn.execute(
            "SELECT week_start, meetings_total, comment_text FROM amocrm_sources_weekly_manual"
        ).fetchall()
    except sqlite3.OperationalError:
        # На случай запуска через `flask run`, когда init_db миграции не выполнялись.
        conn.execute(
            """CREATE TABLE IF NOT EXISTS amocrm_sources_weekly_manual (
                week_start TEXT PRIMARY KEY,
                meetings_total INTEGER,
                comment_text TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        conn.commit()
        rows = conn.execute(
            "SELECT week_start, meetings_total, comment_text FROM amocrm_sources_weekly_manual"
        ).fetchall()
    return {
        r["week_start"]: {
            "meetings_total": r["meetings_total"],
            "comment_text": (r["comment_text"] or ""),
        }
        for r in rows
    }


def _amocrm_sources_manual_upsert(conn, week_start: str, meetings_total, comment_text: str):
    try:
        conn.execute(
            """INSERT INTO amocrm_sources_weekly_manual (week_start, meetings_total, comment_text, updated_at)
               VALUES (?, ?, ?, datetime('now'))
               ON CONFLICT(week_start) DO UPDATE SET
                 meetings_total=excluded.meetings_total,
                 comment_text=excluded.comment_text,
                 updated_at=datetime('now')""",
            (week_start, meetings_total, comment_text),
        )
    except sqlite3.OperationalError:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS amocrm_sources_weekly_manual (
                week_start TEXT PRIMARY KEY,
                meetings_total INTEGER,
                comment_text TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        conn.execute(
            """INSERT INTO amocrm_sources_weekly_manual (week_start, meetings_total, comment_text, updated_at)
               VALUES (?, ?, ?, datetime('now'))
               ON CONFLICT(week_start) DO UPDATE SET
                 meetings_total=excluded.meetings_total,
                 comment_text=excluded.comment_text,
                 updated_at=datetime('now')""",
            (week_start, meetings_total, comment_text),
        )


def _amocrm_sources_monthly_manual_map(conn):
    try:
        rows = conn.execute(
            """SELECT month_start, source_name, plan_budget, fact_budget, plan_leads, plan_qual_leads, plan_cpl, plan_cpql
               FROM amocrm_sources_monthly_manual_by_source"""
        ).fetchall()
    except sqlite3.OperationalError:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS amocrm_sources_monthly_manual_by_source (
                month_start TEXT NOT NULL,
                source_name TEXT NOT NULL,
                plan_budget REAL,
                fact_budget REAL,
                plan_leads INTEGER,
                plan_qual_leads INTEGER,
                plan_cpl REAL,
                plan_cpql REAL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (month_start, source_name)
            )"""
        )
        conn.commit()
        rows = conn.execute(
            """SELECT month_start, source_name, plan_budget, fact_budget, plan_leads, plan_qual_leads, plan_cpl, plan_cpql
               FROM amocrm_sources_monthly_manual_by_source"""
        ).fetchall()
    return {(r["month_start"], r["source_name"]): dict(r) for r in rows}


def _amocrm_sources_monthly_manual_upsert(conn, month_start: str, source_name: str, data: dict):
    conn.execute(
        """INSERT INTO amocrm_sources_monthly_manual_by_source
           (month_start, source_name, plan_budget, fact_budget, plan_leads, plan_qual_leads, plan_cpl, plan_cpql, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(month_start, source_name) DO UPDATE SET
             plan_budget=excluded.plan_budget,
             fact_budget=excluded.fact_budget,
             plan_leads=excluded.plan_leads,
             plan_qual_leads=excluded.plan_qual_leads,
             plan_cpl=excluded.plan_cpl,
             plan_cpql=excluded.plan_cpql,
             updated_at=datetime('now')""",
        (
            month_start,
            source_name,
            data.get("plan_budget"),
            data.get("fact_budget"),
            data.get("plan_leads"),
            data.get("plan_qual_leads"),
            data.get("plan_cpl"),
            data.get("plan_cpql"),
        ),
    )


def _amocrm_sources_monthly_manual_agg(conn):
    """Агрегированные месячные ручные KPI (сумма по всем источникам)."""
    try:
        rows = conn.execute(
            """SELECT month_start,
                      SUM(COALESCE(plan_budget, 0)) as plan_budget,
                      SUM(COALESCE(fact_budget, 0)) as fact_budget,
                      SUM(COALESCE(plan_leads, 0)) as plan_leads,
                      SUM(COALESCE(plan_qual_leads, 0)) as plan_qual_leads
               FROM amocrm_sources_monthly_manual_by_source
               GROUP BY month_start
               ORDER BY month_start"""
        ).fetchall()
    except sqlite3.OperationalError:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS amocrm_sources_monthly_manual_by_source (
                month_start TEXT NOT NULL,
                source_name TEXT NOT NULL,
                plan_budget REAL,
                fact_budget REAL,
                plan_leads INTEGER,
                plan_qual_leads INTEGER,
                plan_cpl REAL,
                plan_cpql REAL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (month_start, source_name)
            )"""
        )
        conn.commit()
        rows = []
    return {r["month_start"]: dict(r) for r in rows}


def _amocrm_sales_monthly_plan_map(conn):
    try:
        rows = conn.execute(
            """SELECT month_start, plan_deals, plan_amount
               FROM amocrm_sales_monthly_plan
               ORDER BY month_start"""
        ).fetchall()
    except sqlite3.OperationalError:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS amocrm_sales_monthly_plan (
                month_start TEXT PRIMARY KEY,
                plan_deals INTEGER,
                plan_amount REAL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        conn.commit()
        rows = []
    return {r["month_start"]: dict(r) for r in rows}


def _amocrm_sales_monthly_plan_upsert(conn, month_start: str, plan_deals, plan_amount):
    conn.execute(
        """INSERT INTO amocrm_sales_monthly_plan (month_start, plan_deals, plan_amount, updated_at)
           VALUES (?, ?, ?, datetime('now'))
           ON CONFLICT(month_start) DO UPDATE SET
             plan_deals=excluded.plan_deals,
             plan_amount=excluded.plan_amount,
             updated_at=datetime('now')""",
        (month_start, plan_deals, plan_amount),
    )


def _amocrm_modules_yearly_plan_map(conn):
    try:
        rows = conn.execute(
            """SELECT year, category_key, plan_units
               FROM amocrm_modules_yearly_plan"""
        ).fetchall()
    except sqlite3.OperationalError:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS amocrm_modules_yearly_plan (
                year INTEGER NOT NULL,
                category_key TEXT NOT NULL,
                plan_units REAL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (year, category_key)
            )"""
        )
        conn.commit()
        rows = []
    return {(int(r["year"]), str(r["category_key"])): r["plan_units"] for r in rows}


def _amocrm_modules_yearly_plan_upsert(conn, year: int, category_key: str, plan_units):
    conn.execute(
        """INSERT INTO amocrm_modules_yearly_plan (year, category_key, plan_units, updated_at)
           VALUES (?, ?, ?, datetime('now'))
           ON CONFLICT(year, category_key) DO UPDATE SET
             plan_units=excluded.plan_units,
             updated_at=datetime('now')""",
        (int(year), str(category_key), plan_units),
    )


def _module_category_from_project_type(project_type: str) -> str | None:
    s = _amocrm_norm_tag_name(project_type)
    if not s or s == "null":
        return None
    if "модул" in s:
        return "modular"
    if "каркас" in s:
        return "frame_site"
    if ("газоблок" in s) or ("газоблоч" in s) or ("газобетон" in s):
        return "gazoblock_site"
    if ("префаб" in s) or ("prefab" in s) or ("бетон" in s):
        return "prefab_beton_site"
    return None


def _amocrm_modules_fact_yearly_map(conn):
    projects_cache = _amocrm_cache_get(conn, "projects_sherwood_home")
    if (not projects_cache) or int(projects_cache.get("version") or 0) < 3:
        pr_payload, pr_err = _build_amocrm_projects_rows()
        if not pr_err and pr_payload:
            _amocrm_cache_set(conn, "projects_sherwood_home", pr_payload)
            conn.commit()
            projects_cache = pr_payload
    sales_rows = list((projects_cache or {}).get("rows") or [])

    def _parse_year_dmy(s):
        txt = str(s or "").strip()
        if not txt or txt == "null":
            return None
        try:
            if len(txt) >= 10 and txt[2] == "." and txt[5] == ".":
                return int(txt[6:10])
        except Exception:
            return None
        return None

    def _to_num(v):
        s = str(v or "").strip()
        if not s or s == "null":
            return 0.0
        cleaned = []
        for ch in s:
            if ch.isdigit() or ch in ".,":  # игнорируем "м²", "руб.", текст и т.п.
                cleaned.append(ch)
        s2 = "".join(cleaned).replace(",", ".")
        if not s2:
            return 0.0
        try:
            return float(s2)
        except Exception:
            return 0.0

    out = {}
    for r in sales_rows:
        cat = _module_category_from_project_type(r.get("project_type"))
        if not cat:
            continue
        # Для модульных домов год считаем по дате предоплаты; для остальных — по дате договора
        if cat == "modular":
            date_src = r.get("prepay_date") or r.get("contract_date")
        else:
            date_src = r.get("contract_date") or r.get("prepay_date")
        year = _parse_year_dmy(date_src)
        if not year:
            continue
        k = (year, cat)
        out[k] = out.get(k, 0.0) + float(_amocrm_row_area_sqm(r) or 0.0)
    return out


def _ensure_demo_yearly_plan_data(conn):
    """Заполнить тестовые плановые данные, не перетирая уже введённые вручную."""
    changed = False
    years = (2024, 2025, 2026)

    monthly_manual = _amocrm_sources_monthly_manual_map(conn)
    for y in years:
        for m in range(1, 13):
            mk = date(y, m, 1).isoformat()
            for idx, src in enumerate(AMOCRM_SOURCE_TAGS):
                key = (mk, src)
                if key in monthly_manual:
                    continue
                leads = max(6, 10 + ((idx * 3 + m + (y - 2024) * 2) % 18))
                quals = max(2, int(round(leads * (0.33 + (idx % 4) * 0.03))))
                budget = float(leads * (4200 + (idx % 5) * 350))
                _amocrm_sources_monthly_manual_upsert(
                    conn,
                    mk,
                    src,
                    {
                        "plan_budget": budget,
                        "fact_budget": None,
                        "plan_leads": leads,
                        "plan_qual_leads": min(quals, leads),
                        "plan_cpl": None,
                        "plan_cpql": None,
                    },
                )
                changed = True

    sales_plan = _amocrm_sales_monthly_plan_map(conn)
    for y in years:
        for m in range(1, 13):
            mk = date(y, m, 1).isoformat()
            if mk in sales_plan:
                continue
            season = 1.0 if m in (3, 4, 5, 9, 10, 11) else 0.75
            deals = int(round((8 + (y - 2024) * 3) * season))
            amount = float(deals * (3_500_000 + (y - 2024) * 450_000))
            _amocrm_sales_monthly_plan_upsert(conn, mk, deals, amount)
            changed = True

    module_plan = _amocrm_modules_yearly_plan_map(conn)
    for y in years:
        for cat_key, _cat_label in MODULE_PLAN_CATEGORIES:
            if (y, cat_key) in module_plan:
                continue
            if y in (2024, 2025):
                plan_units = 160.0 if cat_key == "modular" else 0.0
            else:
                defaults_2026 = {
                    "modular": 220.0,
                    "frame_site": 80.0,
                    "gazoblock_site": 70.0,
                    "prefab_beton_site": 60.0,
                }
                plan_units = defaults_2026.get(cat_key, 0.0)
            _amocrm_modules_yearly_plan_upsert(conn, y, cat_key, plan_units)
            changed = True

    return changed


def _ensure_demo_worker_reports(conn):
    """Создать фейковые закрытия дней для всех работников, если данных ещё нет (для презентаций)."""

    cur = conn.cursor()
    try:
        existing = cur.execute(
            "SELECT COUNT(1) FROM worker_daily_report_items"
        ).fetchone()[0]
    except sqlite3.OperationalError:
        # Таблица ещё не создана — ничего не делаем
        return

    if existing:
        # Уже есть реальные / тестовые отчёты — не трогаем
        return

    workers = [
        dict(r)
        for r in cur.execute(
            "SELECT id, full_name FROM users WHERE role = 'worker' ORDER BY id"
        ).fetchall()
    ]
    if not workers:
        return

    projects = [
        dict(r)
        for r in cur.execute(
            "SELECT id, name FROM projects ORDER BY id"
        ).fetchall()
    ]
    if not projects:
        return

    work_items = [
        dict(r)
        for r in cur.execute(
            "SELECT id, code, name FROM work_items WHERE active = 1 ORDER BY id LIMIT 20"
        ).fetchall()
    ]
    if not work_items:
        return

    today = project_now().date()
    # Для фейковых отчётов стараемся держаться в диапазоне 50–70% по контрактам
    perc_choices = [
        p for p in ALLOWED_PERCENT_ENDS if 50 <= p <= 80
    ] or ALLOWED_PERCENT_ENDS or [20, 40, 60, 80, 100]
    comments = [
        "Работы выполнены по плану.",
        "Часть объёма перенесена на следующий день.",
        "Выполнено с опережением графика.",
        "Сложности по погоде, но план закрыт.",
    ]

    # Детеминированный генератор, чтобы данные были стабильны между запусками
    rnd = random.Random(42)

    for w in workers:
        worker_id = int(w["id"])
        # Для каждого работника используем 1–2 проекта
        worker_projects = rnd.sample(
            projects, k=min(len(projects), rnd.randint(1, 2))
        )
        for proj in worker_projects:
            project_id = int(proj["id"])
            # 5 последних дней текущего месяца
            for offset in range(0, 5):
                report_date = (today - timedelta(days=offset)).isoformat()
                # Шапка отчёта (если уже есть — не создаём повторно)
                cur.execute(
                    """
                    INSERT OR IGNORE INTO worker_daily_reports (worker_id, project_id, report_date)
                    VALUES (?, ?, ?)
                    """,
                    (worker_id, project_id, report_date),
                )
                daily_row = cur.execute(
                    """
                    SELECT id FROM worker_daily_reports
                    WHERE worker_id = ? AND project_id = ? AND report_date = ?
                    """,
                    (worker_id, project_id, report_date),
                ).fetchone()
                if not daily_row:
                    continue
                # SQLite может вернуть либо Row, либо кортеж
                daily_id = (
                    int(daily_row["id"])
                    if isinstance(daily_row, sqlite3.Row)
                    else int(daily_row[0])
                )

                # 1–3 строк по разным работам
                items_sample = rnd.sample(
                    work_items, k=min(len(work_items), rnd.randint(1, 3))
                )
                for wi in items_sample:
                    wid = int(wi["id"])
                    pct = float(rnd.choice(perc_choices))
                    cmt = rnd.choice(comments)
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO worker_daily_report_items
                        (daily_report_id, work_item_id, percent, comment, approved_status, approved_by_id, approved_at)
                        VALUES (?, ?, ?, ?, 'approved', NULL, CURRENT_TIMESTAMP)
                        """,
                        (daily_id, wid, pct, cmt),
                    )

    conn.commit()


def _ensure_demo_foreman_reports(conn):
    """Создать фейковые закрытия дней для прорабов, если данных ещё нет (для презентаций)."""

    cur = conn.cursor()
    try:
        existing = cur.execute(
            """
            SELECT COUNT(1)
            FROM worker_daily_reports dr
            JOIN users u ON u.id = dr.worker_id
            WHERE u.role = 'foreman'
            """
        ).fetchone()[0]
    except sqlite3.OperationalError:
        # Таблицы ещё не созданы — выходим
        return

    if existing:
        # Уже есть реальные / тестовые отчёты прорабов — не трогаем
        return

    foremen = [
        dict(r)
        for r in cur.execute(
            "SELECT id, full_name FROM users WHERE role = 'foreman' ORDER BY id"
        ).fetchall()
    ]
    if not foremen:
        return

    work_items = [
        dict(r)
        for r in cur.execute(
            "SELECT id, code, name FROM work_items WHERE active = 1 AND work_item_type = 'construction' ORDER BY id LIMIT 20"
        ).fetchall()
    ]
    if not work_items:
        return

    today = project_now().date()
    perc_choices = [
        p for p in ALLOWED_PERCENT_ENDS if 50 <= p <= 80
    ] or ALLOWED_PERCENT_ENDS or [20, 40, 60, 80, 100]
    comments = [
        "Этап идёт по графику.",
        "Часть работ перенесена на завтра.",
        "Работа выполнена с небольшим опережением.",
        "Есть замечания по погоде/логистике, но критичных задержек нет.",
    ]

    rnd = random.Random(99)

    for f in foremen:
        foreman_id = int(f["id"])
        projects = [
            dict(r)
            for r in cur.execute(
                """
                SELECT DISTINCT p.id, p.name
                FROM foreman_project_access fpa
                JOIN projects p ON p.id = fpa.project_id
                WHERE fpa.foreman_id = ?
                ORDER BY p.id
                """,
                (foreman_id,),
            ).fetchall()
        ]
        if not projects:
            continue

        # Для каждого прораба 1–3 активных объекта
        foreman_projects = rnd.sample(
            projects, k=min(len(projects), rnd.randint(1, min(3, len(projects))))
        )
        for proj in foreman_projects:
            project_id = int(proj["id"])
            # 5 последних дней
            for offset in range(0, 5):
                report_date = (today - timedelta(days=offset)).isoformat()
                cur.execute(
                    """
                    INSERT OR IGNORE INTO worker_daily_reports (worker_id, project_id, report_date)
                    VALUES (?, ?, ?)
                    """,
                    (foreman_id, project_id, report_date),
                )
                daily_row = cur.execute(
                    """
                    SELECT id FROM worker_daily_reports
                    WHERE worker_id = ? AND project_id = ? AND report_date = ?
                    """,
                    (foreman_id, project_id, report_date),
                ).fetchone()
                if not daily_row:
                    continue
                daily_id = (
                    int(daily_row["id"])
                    if isinstance(daily_row, sqlite3.Row)
                    else int(daily_row[0])
                )

                items_sample = rnd.sample(
                    work_items, k=min(len(work_items), rnd.randint(1, 3))
                )
                for wi in items_sample:
                    wid = int(wi["id"])
                    pct = float(rnd.choice(perc_choices))
                    cmt = rnd.choice(comments)
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO worker_daily_report_items
                        (daily_report_id, work_item_id, percent, comment, approved_status, approved_by_id, approved_at)
                        VALUES (?, ?, ?, ?, 'approved', NULL, CURRENT_TIMESTAMP)
                        """,
                        (daily_id, wid, pct, cmt),
                    )

    conn.commit()


def _amocrm_sales_fact_month_map(conn):
    projects_cache = _amocrm_cache_get(conn, "projects_sherwood_home")
    if (not projects_cache) or int(projects_cache.get("version") or 0) < 3:
        pr_payload, pr_err = _build_amocrm_projects_rows()
        if not pr_err and pr_payload:
            _amocrm_cache_set(conn, "projects_sherwood_home", pr_payload)
            conn.commit()
            projects_cache = pr_payload
    sales_rows = list((projects_cache or {}).get("rows") or [])

    def _to_num(v):
        s = str(v or "").strip()
        if not s or s == "null":
            return 0.0
        # Разрешаем в строке любые символы, но берём только цифры, точки и запятые.
        cleaned = []
        for ch in s:
            if ch.isdigit() or ch in ".,":  # игнорируем "м²", "руб." и т.п.
                cleaned.append(ch)
        s2 = "".join(cleaned).replace(",", ".")
        if not s2:
            return 0.0
        try:
            return float(s2)
        except Exception:
            return 0.0

    def _parse_dmy_to_month(s):
        txt = str(s or "").strip()
        if not txt or txt == "null":
            return None
        try:
            if len(txt) >= 10 and txt[2] == "." and txt[5] == ".":
                m = int(txt[3:5]); y = int(txt[6:10])
                return date(y, m, 1).isoformat()
        except Exception:
            return None
        return None

    fact = {}
    for r in sales_rows:
        # По модульным домам месяц продажи считаем по дате предоплаты,
        # по остальным типам — по дате заключения договора.
        cat = _module_category_from_project_type(r.get("project_type"))
        if cat == "modular":
            date_src = r.get("prepay_date") or r.get("contract_date")
        else:
            date_src = r.get("contract_date") or r.get("prepay_date")
        mk = _parse_dmy_to_month(date_src)
        if not mk:
            continue
        rec = fact.setdefault(mk, {"sold_count": 0.0, "sold_deals": 0, "sold_area": 0.0, "sold_amount": 0.0})
        rec["sold_deals"] += 1
        rec["sold_count"] += 1.0
        rec["sold_area"] += float(_amocrm_row_area_sqm(r) or 0.0)
        rec["sold_amount"] += _to_num(r.get("contract_total"))
    return fact


def _month_seq(start_month: date, end_month: date) -> list[date]:
    out = []
    y, m = start_month.year, start_month.month
    while (y < end_month.year) or (y == end_month.year and m <= end_month.month):
        out.append(date(y, m, 1))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def _build_amocrm_sources_dashboard_payload(conn):
    """Полноэкранный объединенный TV-дашборд: маркетинг + продажи."""
    import amocrm_api

    _ensure_demo_yearly_plan_data(conn)

    leads, err = amocrm_api.amocrm_get_all_leads(conn=conn)
    if err:
        return None, err
    pipelines, pipes_err = amocrm_api.amocrm_get_pipelines(conn=conn)
    if pipes_err:
        return None, pipes_err

    def _lead_has_phone(lead_obj) -> bool:
        cfs = lead_obj.get("custom_fields_values") or []
        for cf in cfs:
            field_name = str(cf.get("field_name") or cf.get("field_code") or "").strip().lower()
            if ("тел" not in field_name) and ("phone" not in field_name):
                continue
            for v in (cf.get("values") or []):
                raw = v.get("value")
                if raw in (None, ""):
                    continue
                digits = "".join(ch for ch in str(raw) if ch.isdigit())
                if len(digits) >= 10:
                    return True
        return False

    def _lead_is_qual(lead_obj) -> bool:
        cfs = lead_obj.get("custom_fields_values") or []
        for cf in cfs:
            field_name = _amocrm_norm_tag_name(cf.get("field_name") or cf.get("field_code") or "")
            if ("квалиф" not in field_name) and ("квали" not in field_name):
                continue
            for v in (cf.get("values") or []):
                val_norm = _amocrm_norm_tag_name(v.get("value"))
                if not val_norm:
                    continue
                if val_norm.startswith("неквал"):
                    return False
                if val_norm == "квал" or val_norm.startswith("квал "):
                    return True
        return False

    source_aliases_norm = {
        col: {_amocrm_norm_tag_name(a) for a in aliases if _amocrm_norm_tag_name(a)}
        for col, aliases in AMOCRM_SOURCE_TAG_ALIASES.items()
    }
    avito_norm = _amocrm_norm_tag_name("Авито")
    sales_pipeline_ids = {
        int(p.get("id"))
        for p in (pipelines or [])
        if _amocrm_norm_tag_name(p.get("name")) == "продажи"
    }
    if not sales_pipeline_ids:
        return None, "Воронка 'Продажи' не найдена в amoCRM."

    month_map = {}
    min_month = None
    max_month = None
    for lead in leads or []:
        try:
            if int(lead.get("pipeline_id") or 0) not in sales_pipeline_ids:
                continue
            created_day = datetime.fromtimestamp(int(lead.get("created_at")), tz=timezone.utc).astimezone(PROJECT_TZ).date()
        except Exception:
            continue

        tags = ((lead.get("_embedded") or {}).get("tags") or [])
        lead_tags_norm = {_amocrm_norm_tag_name(t.get("name")) for t in tags if t.get("name")}
        has_phone = _lead_has_phone(lead)
        is_qual = _lead_is_qual(lead)

        matched_cols = []
        for col_name, aliases_norm in source_aliases_norm.items():
            if col_name and _amocrm_norm_tag_name(col_name) == avito_norm and not has_phone:
                continue
            if aliases_norm.intersection(lead_tags_norm):
                matched_cols.append(col_name)
        if "Авито" in matched_cols and "8800" in matched_cols:
            matched_cols = [c for c in matched_cols if c != "8800"]
        if not matched_cols:
            continue

        month_start = created_day.replace(day=1).isoformat()
        if month_start not in month_map:
            month_map[month_start] = {
                "month_start": month_start,
                "leads": 0,
                "qual": 0,
                "by_source": {s: {"leads": 0, "qual": 0} for s in AMOCRM_SOURCE_TAGS},
            }
        month_map[month_start]["leads"] += 1
        if is_qual:
            month_map[month_start]["qual"] += 1
        for src in matched_cols:
            month_map[month_start]["by_source"][src]["leads"] += 1
            if is_qual:
                month_map[month_start]["by_source"][src]["qual"] += 1

        dmonth = created_day.replace(day=1)
        if min_month is None or dmonth < min_month:
            min_month = dmonth
        if max_month is None or dmonth > max_month:
            max_month = dmonth

    if min_month is None or max_month is None:
        months = []
    else:
        months = []
        for m in _month_seq(min_month, max_month):
            key = m.isoformat()
            row = month_map.get(key) or {
                "month_start": key,
                "leads": 0,
                "qual": 0,
                "by_source": {s: {"leads": 0, "qual": 0} for s in AMOCRM_SOURCE_TAGS},
            }
            row["label"] = m.strftime("%m.%Y")
            row["year"] = m.year
            months.append(row)

    manual_agg = _amocrm_sources_monthly_manual_agg(conn)
    for m in months:
        mm = manual_agg.get(m["month_start"], {})
        m["plan_budget"] = float(mm.get("plan_budget") or 0)
        m["fact_budget"] = float(mm.get("fact_budget") or 0)
        m["plan_leads"] = int(mm.get("plan_leads") or 0)
        m["plan_qual_leads"] = int(mm.get("plan_qual_leads") or 0)

    years = sorted({m["year"] for m in months})
    totals = {
        "leads": sum(int(m.get("leads") or 0) for m in months),
        "qual": sum(int(m.get("qual") or 0) for m in months),
    }
    totals["qual_rate"] = (totals["qual"] * 100.0 / totals["leads"]) if totals["leads"] else 0.0

    # --- Sales block (fact + manual monthly plan) ---
    projects_cache = _amocrm_cache_get(conn, "projects_sherwood_home")
    if (not projects_cache) or int(projects_cache.get("version") or 0) < 3:
        pr_payload, pr_err = _build_amocrm_projects_rows()
        if not pr_err and pr_payload:
            _amocrm_cache_set(conn, "projects_sherwood_home", pr_payload)
            conn.commit()
            projects_cache = pr_payload
    sales_rows = list((projects_cache or {}).get("rows") or [])

    def _to_num(v):
        s = str(v or "").strip().replace(" ", "").replace(",", ".")
        if (not s) or s == "null":
            return 0.0
        try:
            return float(s)
        except Exception:
            return 0.0

    def _parse_dmy_to_month(s):
        txt = str(s or "").strip()
        if not txt or txt == "null":
            return None
        try:
            if len(txt) >= 10 and txt[2] == "." and txt[5] == ".":
                m = int(txt[3:5]); y = int(txt[6:10])
                return date(y, m, 1)
        except Exception:
            return None
        return None

    payment_totals = {}
    sales_by_month = {}
    plan_by_month = {}
    manual_sales_plan = _amocrm_sales_monthly_plan_map(conn)
    min_sales_month = None
    max_sales_month = None
    today = project_now().date()
    cur_month = today.replace(day=1)
    for r in sales_rows:
        amount = _to_num(r.get("contract_total"))
        pm_raw = str(r.get("payment_method") or "null").strip()
        payment_totals[pm_raw or "null"] = payment_totals.get(pm_raw or "null", 0.0) + amount

        # По модульным домам месяц продажи считаем по дате предоплаты,
        # по остальным типам — по дате заключения договора.
        cat = _module_category_from_project_type(r.get("project_type"))
        if cat == "modular":
            date_src = r.get("prepay_date") or r.get("contract_date")
        else:
            date_src = r.get("contract_date") or r.get("prepay_date")
        sold_m = _parse_dmy_to_month(date_src)
        if sold_m:
            key = sold_m.isoformat()
            rec = sales_by_month.setdefault(
                key,
                {"month_start": key, "sold_count": 0.0, "sold_deals": 0, "sold_amount": 0.0, "sold_area": 0.0},
            )
            row_area_sqm = float(_amocrm_row_area_sqm(r) or 0.0)
            rec["sold_deals"] += 1
            rec["sold_count"] += row_area_sqm
            rec["sold_amount"] += amount
            rec["sold_area"] += row_area_sqm

            if min_sales_month is None or sold_m < min_sales_month:
                min_sales_month = sold_m
            if max_sales_month is None or sold_m > max_sales_month:
                max_sales_month = sold_m

        # fallback plan from projected delivery dates if manual monthly sales plan is empty
        plan_m = _parse_dmy_to_month(r.get("delivery_date")) or _parse_dmy_to_month(r.get("prod_end"))
        if plan_m and plan_m >= cur_month:
            keyp = plan_m.isoformat()
            recp = plan_by_month.setdefault(keyp, {"month_start": keyp, "plan_count": 0, "plan_area": 0.0, "plan_amount": 0.0})
            recp["plan_count"] += 1
            recp["plan_area"] += float(_amocrm_row_area_sqm(r) or 0.0)
            recp["plan_amount"] += amount
            if max_sales_month is None or plan_m > max_sales_month:
                max_sales_month = plan_m
            if min_sales_month is None or plan_m < min_sales_month:
                min_sales_month = plan_m

    for mk in manual_sales_plan.keys():
        try:
            dm = date.fromisoformat(str(mk))
        except Exception:
            continue
        if min_sales_month is None or dm < min_sales_month:
            min_sales_month = dm
        if max_sales_month is None or dm > max_sales_month:
            max_sales_month = dm

    sales_months = []
    if min_sales_month and max_sales_month:
        for m in _month_seq(min_sales_month, max_sales_month):
            k = m.isoformat()
            sold = sales_by_month.get(k, {})
            plan = plan_by_month.get(k, {})
            manual_plan = manual_sales_plan.get(k, {})
            plan_count = manual_plan.get("plan_deals")
            plan_amount = manual_plan.get("plan_amount")
            try:
                plan_count = int(plan_count) if plan_count not in (None, "") else None
            except Exception:
                plan_count = None
            try:
                plan_amount = float(plan_amount) if plan_amount not in (None, "") else None
            except Exception:
                plan_amount = None
            sales_months.append(
                {
                    "month_start": k,
                    "label": m.strftime("%m.%Y"),
                    "year": m.year,
                    "sold_count": float(sold.get("sold_count") or 0.0),
                    "sold_deals": int(sold.get("sold_deals") or 0),
                    "sold_amount": float(sold.get("sold_amount") or 0.0),
                    "sold_area": float(sold.get("sold_area") or 0.0),
                    "plan_count": int(plan_count if plan_count is not None else (plan.get("plan_count") or 0)),
                    "plan_area": float(
                        (
                            (float(plan_count) * MODULE_TO_SQM)
                            if plan_count is not None
                            else (plan.get("plan_area") or 0.0)
                        )
                    ),
                    "plan_amount": float(plan_amount if plan_amount is not None else (plan.get("plan_amount") or 0.0)),
                }
            )

    sales_totals = {
        "sold_count": sum(float(m.get("sold_count") or 0.0) for m in sales_months),
        "sold_deals": sum(int(m.get("sold_deals") or 0) for m in sales_months),
        "sold_amount": sum(float(m.get("sold_amount") or 0.0) for m in sales_months),
        "sold_area": sum(float(m.get("sold_area") or 0.0) for m in sales_months),
        "plan_count": sum(int(m.get("plan_count") or 0) for m in sales_months),
        "plan_area": sum(float(m.get("plan_area") or 0.0) for m in sales_months),
        "plan_amount": sum(float(m.get("plan_amount") or 0.0) for m in sales_months),
        "by_payment": {k: float(v) for k, v in payment_totals.items()},
    }

    modules_plan_map = _amocrm_modules_yearly_plan_map(conn)
    modules_fact_map = _amocrm_modules_fact_yearly_map(conn)
    category_labels = {k: v for k, v in MODULE_PLAN_CATEGORIES}
    module_years = sorted({y for (y, _c) in modules_plan_map.keys()} | {y for (y, _c) in modules_fact_map.keys()})
    modules_yearly = []
    for y in module_years:
        cats = []
        for cat_key, cat_label in MODULE_PLAN_CATEGORIES:
            plan_units = float(modules_plan_map.get((y, cat_key)) or 0.0) * MODULE_TO_SQM
            fact_units = float(modules_fact_map.get((y, cat_key)) or 0.0)
            cats.append(
                {
                    "key": cat_key,
                    "label": cat_label,
                    "plan_units": plan_units,
                    "fact_units": fact_units,
                }
            )
        modules_yearly.append({"year": y, "categories": cats})

    all_years = sorted(set(years) | {m["year"] for m in sales_months} | set(module_years))
    payload = {
        "months": months,  # marketing monthly
        "sales_months": sales_months,
        "years": all_years,
        "sources": AMOCRM_SOURCE_TAGS,
        "totals": totals,  # marketing totals
        "sales_totals": sales_totals,
        "module_categories": [{"key": k, "label": category_labels.get(k, k)} for k, _ in MODULE_PLAN_CATEGORIES],
        "modules_yearly": modules_yearly,
        "generated_at": project_now().strftime("%d.%m.%Y %H:%M:%S"),
        "version": 7,
    }
    return payload, None


def _amocrm_sources_prepare_rows(conn, selected_year=None):
    cache = _amocrm_cache_get(conn, "sources_weekly_tags")
    error = None
    if (not cache) or int(cache.get("version") or 0) < 5:
        payload, err = _build_amocrm_sources_weekly_rows()
        if err:
            error = err
            payload = {"weeks": [], "months": [], "generated_at": None, "version": 5}
        else:
            _amocrm_cache_set(conn, "sources_weekly_tags", payload)
            conn.commit()
        cache = payload

    manual = _amocrm_sources_manual_map(conn)
    rows = []
    for w in (cache.get("weeks") or []):
        wk = w.get("week_start")
        row_manual = manual.get(wk, {})
        rows.append(
            {
                "week_start": wk,
                "week_label": _amocrm_week_label(wk),
                "sources": w.get("sources") or {tag: {"leads": 0, "qual": 0} for tag in AMOCRM_SOURCE_TAGS},
                "total_leads": int(w.get("total_leads") or 0),
                "meetings_total": row_manual.get("meetings_total"),
                "comment_text": row_manual.get("comment_text", ""),
            }
        )

    monthly_manual = _amocrm_sources_monthly_manual_map(conn)
    monthly_rows = []
    today = project_now().date()
    current_month_start = today.replace(day=1).isoformat()
    weekly_rows_sorted = sorted(rows, key=lambda r: r.get("week_start") or "")
    recent_weeks = [w for w in weekly_rows_sorted if (w.get("week_start") or "") <= today.isoformat()]
    recent_weeks = recent_weeks[-4:]
    source_totals = {s: {"leads": 0, "qual": 0} for s in AMOCRM_SOURCE_TAGS}
    for w in recent_weeks:
        wsources = w.get("sources") or {}
        for s in AMOCRM_SOURCE_TAGS:
            src = wsources.get(s) or {}
            source_totals[s]["leads"] += int(src.get("leads") or 0)
            source_totals[s]["qual"] += int(src.get("qual") or 0)

    for idx, source_name in enumerate(AMOCRM_SOURCE_TAGS, start=1):
        manual = monthly_manual.get((current_month_start, source_name), {})
        actual_leads = int((source_totals.get(source_name) or {}).get("leads") or 0)
        actual_qual = int((source_totals.get(source_name) or {}).get("qual") or 0)
        plan_budget = manual.get("plan_budget")
        fact_budget = manual.get("fact_budget")
        plan_leads = manual.get("plan_leads")
        plan_qual_leads = manual.get("plan_qual_leads")
        plan_cpl = manual.get("plan_cpl")
        plan_cpql = manual.get("plan_cpql")

        def _to_float(v):
            try:
                if v in (None, ""):
                    return None
                return float(v)
            except Exception:
                return None

        pb = _to_float(plan_budget)
        fb = _to_float(fact_budget)
        pl = _to_float(plan_leads)
        pql = _to_float(plan_qual_leads)
        pcpl = _to_float(plan_cpl)
        pcpql = _to_float(plan_cpql)

        qual_pct = (actual_qual * 100.0 / actual_leads) if actual_leads else None
        fact_cpl = (fb / actual_leads) if (fb is not None and actual_leads) else None
        fact_cpql = (fb / actual_qual) if (fb is not None and actual_qual) else None
        lead_plan_pct = (actual_leads * 100.0 / pl) if pl else None
        qual_plan_pct = (actual_qual * 100.0 / pql) if pql else None
        cpl_plan_pct = (fact_cpl * 100.0 / pcpl) if (fact_cpl is not None and pcpl) else None
        cpql_plan_pct = (fact_cpql * 100.0 / pcpql) if (fact_cpql is not None and pcpql) else None
        efficiency_pct = None
        parts = [v for v in (lead_plan_pct, qual_plan_pct) if v is not None]
        if parts:
            efficiency_pct = sum(parts) / len(parts)

        try:
            ms = date.fromisoformat(str(current_month_start))
            month_label = ms.strftime("%m.%Y")
        except Exception:
            month_label = str(current_month_start or "—")

        monthly_rows.append(
            {
                "row_key": str(idx),
                "month_start": current_month_start,
                "month_label": month_label,
                "source_name": source_name,
                "plan_budget": plan_budget,
                "fact_budget": fact_budget,
                "plan_leads": plan_leads,
                "plan_qual_leads": plan_qual_leads,
                "plan_cpl": plan_cpl,
                "plan_cpql": plan_cpql,
                "actual_leads": actual_leads,
                "actual_qual_leads": actual_qual,
                "qual_pct": qual_pct,
                "fact_cpl": fact_cpl,
                "fact_cpql": fact_cpql,
                "lead_plan_pct": lead_plan_pct,
                "qual_plan_pct": qual_plan_pct,
                "cpl_plan_pct": cpl_plan_pct,
                "cpql_plan_pct": cpql_plan_pct,
                "efficiency_pct": efficiency_pct,
            }
        )

    try:
        selected_year = int(selected_year) if selected_year is not None else project_now().year
    except Exception:
        selected_year = project_now().year
    years_from_cache = set()
    for mm in (cache.get("months") or []):
        try:
            years_from_cache.add(int(str(mm.get("month_start") or "0000")[:4]))
        except Exception:
            continue
    year_options = sorted(
        {
            project_now().year - 1,
            project_now().year,
            project_now().year + 1,
            *years_from_cache,
        }
    )
    year_options = [y for y in year_options if y >= 2020]
    if selected_year not in year_options:
        year_options.append(selected_year)
        year_options = sorted(year_options)

    monthly_manual_all = _amocrm_sources_monthly_manual_map(conn)
    yearly_source_rows = []
    rk = 0
    for m in range(1, 13):
        mk = date(selected_year, m, 1).isoformat()
        month_label = f"{m:02d}.{selected_year}"
        for src in AMOCRM_SOURCE_TAGS:
            rk += 1
            mm = monthly_manual_all.get((mk, src), {})
            yearly_source_rows.append(
                {
                    "row_key": str(rk),
                    "month_start": mk,
                    "month_label": month_label,
                    "source_name": src,
                    "plan_budget": mm.get("plan_budget"),
                    "plan_leads": mm.get("plan_leads"),
                    "plan_qual_leads": mm.get("plan_qual_leads"),
                }
            )

    sales_plan_map = _amocrm_sales_monthly_plan_map(conn)
    sales_fact_map = _amocrm_sales_fact_month_map(conn)
    yearly_sales_rows = []
    for m in range(1, 13):
        mk = date(selected_year, m, 1).isoformat()
        month_label = f"{m:02d}.{selected_year}"
        sp = sales_plan_map.get(mk, {})
        sf = sales_fact_map.get(mk, {})
        yearly_sales_rows.append(
            {
                "month_start": mk,
                "month_label": month_label,
                "plan_deals": sp.get("plan_deals"),
                "plan_amount": sp.get("plan_amount"),
                "fact_deals": int(sf.get("sold_deals") or sf.get("sold_count") or 0),
                "fact_amount": float(sf.get("sold_amount") or 0.0),
            }
        )

    return {
        "rows": rows,
        "monthly_rows": monthly_rows,
        "yearly_source_rows": yearly_source_rows,
        "yearly_sales_rows": yearly_sales_rows,
        "year_options": year_options,
        "selected_year": selected_year,
        "error": error,
        "cache_updated_at": cache.get("_cache_updated_at") or format_project_dt(cache.get("generated_at")),
    }


def _amocrm_cache_get(conn, cache_key):
    row = conn.execute(
        "SELECT data_json, updated_at FROM amocrm_leads_cache WHERE cache_key = ?",
        (cache_key,),
    ).fetchone()
    if not row:
        return None
    try:
        data = json.loads(row["data_json"] or "{}")
    except Exception:
        return None
    data["_cache_updated_at"] = format_project_dt(row["updated_at"])
    return data


def _amocrm_cache_set(conn, cache_key, payload):
    data_json = json.dumps(payload, ensure_ascii=False)
    conn.execute(
        """INSERT INTO amocrm_leads_cache (cache_key, data_json, updated_at)
           VALUES (?, ?, datetime('now'))
           ON CONFLICT(cache_key) DO UPDATE SET
             data_json=excluded.data_json,
             updated_at=datetime('now')""",
        (cache_key, data_json),
    )


@app.route("/admin/amocrm/sources/sync", methods=["POST"])
@projects_manager_required
@permission_required("amocrm_sources")
def amocrm_sources_sync():
    """Синхронизация недельного отчёта по источникам (теги в сделках amoCRM)."""
    conn = get_db()
    payload, err = _build_amocrm_sources_weekly_rows()
    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    if err:
        conn.close()
        if is_ajax:
            return Response(
                json.dumps({"ok": False, "error": err}, ensure_ascii=False),
                status=500,
                mimetype="application/json",
            )
        flash(f"Ошибка синхронизации: {err}", "error")
        return redirect(url_for("amocrm_sources_report"))
    _amocrm_cache_set(conn, "sources_weekly_tags", payload)
    conn.commit()
    conn.close()
    if is_ajax:
        return Response(
            json.dumps(
                {
                    "ok": True,
                    "updated_at": payload.get("generated_at"),
                    "weeks_count": len(payload.get("weeks") or []),
                },
                ensure_ascii=False,
            ),
            status=200,
            mimetype="application/json",
        )
    flash("Отчёт по источникам синхронизирован.", "success")
    return redirect(url_for("amocrm_sources_report"))


@app.route("/admin/amocrm/sources/data", methods=["GET"])
@projects_manager_required
@permission_required("amocrm_sources")
def amocrm_sources_data():
    """JSON данные отчёта по источникам (из кэша, для ajax-обновления)."""
    conn = get_db()
    prepared = _amocrm_sources_prepare_rows(conn)
    rows = prepared["rows"]
    monthly_rows = prepared["monthly_rows"]
    error = prepared["error"]
    cache_updated_at = prepared["cache_updated_at"]
    rows_html = render_template("reports/_amocrm_sources_weekly_rows.html", rows=rows, source_tags=AMOCRM_SOURCE_TAGS)
    monthly_html = render_template("reports/_amocrm_sources_monthly_rows.html", rows=monthly_rows)
    conn.close()
    if error:
        return Response(
            json.dumps({"ok": False, "error": error}, ensure_ascii=False),
            status=500,
            mimetype="application/json",
        )
    return Response(
        json.dumps(
            {
                "ok": True,
                "rows_html": rows_html,
                "monthly_html": monthly_html,
                "cache_updated_at": cache_updated_at,
                "rows_count": len(rows),
            },
            ensure_ascii=False,
        ),
        status=200,
        mimetype="application/json",
    )


@app.route("/admin/amocrm/sources", methods=["GET", "POST"])
@projects_manager_required
@permission_required("amocrm_sources")
def amocrm_sources_report():
    """Недельный отчёт по источникам лидов из тегов сделок amoCRM."""
    conn = get_db()
    _ensure_demo_yearly_plan_data(conn)
    if request.method == "POST":
        selected_year_raw = (request.form.get("plan_year") or request.args.get("year") or "").strip()
        try:
            selected_year = int(selected_year_raw) if selected_year_raw else project_now().year
        except Exception:
            selected_year = project_now().year

        week_starts = request.form.getlist("week_start")
        for wk in week_starts:
            wk = (wk or "").strip()
            if not wk:
                continue
            meetings_raw = (request.form.get(f"meetings_{wk}") or "").strip()
            comment_text = (request.form.get(f"comment_{wk}") or "").strip()
            meetings_total = None
            if meetings_raw:
                try:
                    meetings_total = int(float(meetings_raw.replace(",", ".")))
                except Exception:
                    meetings_total = None
            _amocrm_sources_manual_upsert(conn, wk, meetings_total, comment_text)

        row_keys = request.form.getlist("monthly_row_key")
        for rk in row_keys:
            rk = (rk or "").strip()
            if not rk:
                continue
            mk = (request.form.get(f"month_start_{rk}") or "").strip()
            source_name = (request.form.get(f"source_name_{rk}") or "").strip()
            if not mk or not source_name:
                continue

            def _read_num(name, as_int=False):
                raw = (request.form.get(name) or "").strip()
                if not raw:
                    return None
                try:
                    v = float(raw.replace(",", "."))
                    return int(v) if as_int else v
                except Exception:
                    return None

            payload = {
                "plan_budget": _read_num(f"plan_budget_{rk}", as_int=False),
                "fact_budget": _read_num(f"fact_budget_{rk}", as_int=False),
                "plan_leads": _read_num(f"plan_leads_{rk}", as_int=True),
                "plan_qual_leads": _read_num(f"plan_qual_leads_{rk}", as_int=True),
                "plan_cpl": _read_num(f"plan_cpl_{rk}", as_int=False),
                "plan_cpql": _read_num(f"plan_cpql_{rk}", as_int=False),
            }
            _amocrm_sources_monthly_manual_upsert(conn, mk, source_name, payload)

        yearly_row_keys = request.form.getlist("yearly_plan_row_key")
        monthly_manual_map = _amocrm_sources_monthly_manual_map(conn)
        for rk in yearly_row_keys:
            rk = (rk or "").strip()
            if not rk:
                continue
            mk = (request.form.get(f"yearly_month_start_{rk}") or "").strip()
            source_name = (request.form.get(f"yearly_source_name_{rk}") or "").strip()
            if not mk or not source_name:
                continue

            def _read_num(name, as_int=False):
                raw = (request.form.get(name) or "").strip()
                if raw == "":
                    return None
                try:
                    v = float(raw.replace(",", "."))
                    return int(v) if as_int else v
                except Exception:
                    return None

            existing = monthly_manual_map.get((mk, source_name), {})
            merged = {
                "plan_budget": _read_num(f"yearly_plan_budget_{rk}", as_int=False),
                "fact_budget": existing.get("fact_budget"),
                "plan_leads": _read_num(f"yearly_plan_leads_{rk}", as_int=True),
                "plan_qual_leads": _read_num(f"yearly_plan_qual_leads_{rk}", as_int=True),
                "plan_cpl": existing.get("plan_cpl"),
                "plan_cpql": existing.get("plan_cpql"),
            }
            _amocrm_sources_monthly_manual_upsert(conn, mk, source_name, merged)

        for mk in request.form.getlist("sales_plan_month_start"):
            mk = (mk or "").strip()
            if not mk:
                continue
            deals_raw = (request.form.get(f"sales_plan_deals_{mk}") or "").strip()
            amount_raw = (request.form.get(f"sales_plan_amount_{mk}") or "").strip()
            try:
                plan_deals = int(float(deals_raw.replace(",", "."))) if deals_raw else None
            except Exception:
                plan_deals = None
            try:
                plan_amount = float(amount_raw.replace(",", ".")) if amount_raw else None
            except Exception:
                plan_amount = None
            _amocrm_sales_monthly_plan_upsert(conn, mk, plan_deals, plan_amount)

        for rk in request.form.getlist("modules_plan_row_key"):
            rk = (rk or "").strip()
            if not rk:
                continue
            year_raw = (request.form.get(f"modules_plan_year_{rk}") or "").strip()
            cat_key = (request.form.get(f"modules_plan_cat_{rk}") or "").strip()
            units_raw = (request.form.get(f"modules_plan_units_{rk}") or "").strip()
            if not year_raw or not cat_key:
                continue
            try:
                year_val = int(year_raw)
            except Exception:
                continue
            try:
                sqm_val = float(units_raw.replace(",", ".")) if units_raw else None
            except Exception:
                sqm_val = None
            units_val = (sqm_val / MODULE_TO_SQM) if sqm_val is not None else None
            _amocrm_modules_yearly_plan_upsert(conn, year_val, cat_key, units_val)

        conn.commit()
        _amocrm_cache_set(conn, "sources_dashboard_all_years", {"version": 0})
        conn.close()
        flash("Данные недельного/месячного/годового планов сохранены.", "success")
        return redirect(url_for("amocrm_sources_report", year=selected_year))

    selected_year_arg = (request.args.get("year") or "").strip()
    try:
        selected_year = int(selected_year_arg) if selected_year_arg else project_now().year
    except Exception:
        selected_year = project_now().year

    prepared = _amocrm_sources_prepare_rows(conn, selected_year=selected_year)
    rows = prepared["rows"]
    monthly_rows = prepared["monthly_rows"]
    yearly_source_rows = prepared["yearly_source_rows"]
    yearly_sales_rows = prepared["yearly_sales_rows"]
    module_plan_map = _amocrm_modules_yearly_plan_map(conn)
    module_fact_map = _amocrm_modules_fact_yearly_map(conn)
    module_plan_rows = []
    rr = 0
    for y in (2024, 2025, 2026):
        for cat_key, cat_label in MODULE_PLAN_CATEGORIES:
            rr += 1
            module_plan_rows.append(
                {
                    "row_key": str(rr),
                    "year": y,
                    "category_key": cat_key,
                    "category_label": cat_label,
                    "plan_units": (
                        float(module_plan_map.get((y, cat_key)) or 0.0) * MODULE_TO_SQM
                    ),
                    "fact_units": float(module_fact_map.get((y, cat_key)) or 0.0),
                }
            )
    year_options = prepared["year_options"]
    selected_year = prepared["selected_year"]
    error = prepared["error"]
    cache_updated_at = prepared["cache_updated_at"]

    fmt = (request.args.get("format") or "").strip().lower()
    if fmt in ("csv", "xlsx"):
        header = ["Неделя", *AMOCRM_SOURCE_TAGS, "Всего лидов", "Всего встреч", "Комментарии"]
        export_rows = [header]
        for r in rows:
            export_rows.append(
                [
                    r["week_label"],
                    *[
                        f"{int(((r.get('sources') or {}).get(tag) or {}).get('leads', 0) or 0)}/"
                        f"{int(((r.get('sources') or {}).get(tag) or {}).get('qual', 0) or 0)}"
                        for tag in AMOCRM_SOURCE_TAGS
                    ],
                    int(r.get("total_leads") or 0),
                    ("" if r.get("meetings_total") is None else int(r.get("meetings_total") or 0)),
                    r.get("comment_text") or "",
                ]
            )
        # Добавим второй лист-like блок (с разделителем) по месячному KPI по источникам.
        export_rows.append([])
        export_rows.append(["Месячный KPI по источникам (сумма последних 4 недель)"])
        export_rows.append(
            [
                "Месяц",
                "Источник",
                "План бюджет",
                "Факт бюджет",
                "План лидов",
                "План квалов",
                "Факт лидов",
                "Факт квалов",
                "% квалов факт",
                "План CPL",
                "Факт CPL",
                "% к плану CPL",
                "План CPQL",
                "Факт CPQL",
                "% к плану CPQL",
                "% плана лидов",
                "% плана квалов",
                "Эффективность",
            ]
        )
        for r in prepared["monthly_rows"]:
            export_rows.append(
                [
                    r.get("month_label"),
                    r.get("source_name"),
                    r.get("plan_budget"),
                    r.get("fact_budget"),
                    r.get("plan_leads"),
                    r.get("plan_qual_leads"),
                    r.get("actual_leads"),
                    r.get("actual_qual_leads"),
                    (round(r["qual_pct"], 2) if r.get("qual_pct") is not None else ""),
                    r.get("plan_cpl"),
                    (round(r["fact_cpl"], 2) if r.get("fact_cpl") is not None else ""),
                    (round(r["cpl_plan_pct"], 2) if r.get("cpl_plan_pct") is not None else ""),
                    r.get("plan_cpql"),
                    (round(r["fact_cpql"], 2) if r.get("fact_cpql") is not None else ""),
                    (round(r["cpql_plan_pct"], 2) if r.get("cpql_plan_pct") is not None else ""),
                    (round(r["lead_plan_pct"], 2) if r.get("lead_plan_pct") is not None else ""),
                    (round(r["qual_plan_pct"], 2) if r.get("qual_plan_pct") is not None else ""),
                    (round(r["efficiency_pct"], 2) if r.get("efficiency_pct") is not None else ""),
                ]
            )
        conn.close()
        today = date.today().isoformat()
        if fmt == "csv":
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=";")
            writer.writerows(export_rows)
            return _csv_response_utf8(buf.getvalue(), f"amocrm_sources_weekly_{today}.csv")
        return _xlsx_response(export_rows, f"amocrm_sources_weekly_{today}.xlsx")

    conn.close()
    return render_template(
        "reports/amocrm_sources_weekly.html",
        error=error,
        source_tags=AMOCRM_SOURCE_TAGS,
        rows=rows,
        monthly_rows=monthly_rows,
        yearly_source_rows=yearly_source_rows,
        yearly_sales_rows=yearly_sales_rows,
        module_plan_rows=module_plan_rows,
        year_options=year_options,
        selected_year=selected_year,
        now=project_now(),
        cache_updated_at=cache_updated_at,
    )


@app.route("/admin/amocrm/sources/dashboard/data", methods=["GET"])
@projects_manager_required
@permission_required("amocrm_tv")
def amocrm_sources_dashboard_data():
    conn = get_db()
    cache = _amocrm_cache_get(conn, "sources_dashboard_all_years")
    if (not cache) or int(cache.get("version") or 0) < 7:
        payload, err = _build_amocrm_sources_dashboard_payload(conn)
        if err:
            conn.close()
            return Response(json.dumps({"ok": False, "error": err}, ensure_ascii=False), status=500, mimetype="application/json")
        _amocrm_cache_set(conn, "sources_dashboard_all_years", payload)
        conn.commit()
        cache = payload
    conn.close()
    return Response(
        json.dumps(
            {
                "ok": True,
                "payload": cache,
                "cache_updated_at": cache.get("_cache_updated_at") or cache.get("generated_at"),
            },
            ensure_ascii=False,
        ),
        status=200,
        mimetype="application/json",
    )


@app.route("/admin/amocrm/sources/dashboard/sync", methods=["POST"])
@projects_manager_required
@permission_required("amocrm_tv")
def amocrm_sources_dashboard_sync():
    conn = get_db()
    payload, err = _build_amocrm_sources_dashboard_payload(conn)
    if err:
        conn.close()
        return Response(json.dumps({"ok": False, "error": err}, ensure_ascii=False), status=500, mimetype="application/json")
    _amocrm_cache_set(conn, "sources_dashboard_all_years", payload)
    conn.commit()
    conn.close()
    return Response(
        json.dumps(
            {"ok": True, "updated_at": payload.get("generated_at")},
            ensure_ascii=False,
        ),
        status=200,
        mimetype="application/json",
    )


@app.route("/admin/amocrm/sources/dashboard", methods=["GET"])
@projects_manager_required
@permission_required("amocrm_tv")
def amocrm_sources_dashboard():
    """Полноэкранный объединенный TV-дашборд без шапки."""
    conn = get_db()
    cache = _amocrm_cache_get(conn, "sources_dashboard_all_years")
    error = None
    if (not cache) or int(cache.get("version") or 0) < 7:
        payload, err = _build_amocrm_sources_dashboard_payload(conn)
        if err:
            error = err
            payload = {
                "months": [],
                "sales_months": [],
                "years": [],
                "sources": AMOCRM_SOURCE_TAGS,
                "totals": {},
                "sales_totals": {},
                "module_categories": [],
                "modules_yearly": [],
                "generated_at": None,
                "version": 7,
            }
        else:
            _amocrm_cache_set(conn, "sources_dashboard_all_years", payload)
            conn.commit()
        cache = payload
    conn.close()
    return render_template(
        "reports/amocrm_sources_dashboard.html",
        error=error,
        payload_json=json.dumps(cache, ensure_ascii=False),
        cache_updated_at=cache.get("_cache_updated_at") or cache.get("generated_at"),
    )


def _amocrm_report_view_data(cache, req_args):
    rows = list((cache or {}).get("rows") or [])

    # В отчёт выводим только содержательные сделки:
    # когда заполнено хотя бы одно ключевое поле отчёта (кроме Ответственного/Площади дома).
    key_fields = ("project_name", "payment_method", "total_price", "area_sqm", "build_area", "extras", "comment")
    rows = [r for r in rows if any(r.get(k) not in (None, "", "null") for k in key_fields)]

    responsible_options = sorted({r.get("responsible") for r in rows if r.get("responsible") not in (None, "", "null")})
    payment_options = sorted({r.get("payment_method") for r in rows if r.get("payment_method") not in (None, "", "null")})

    selected_responsible = (req_args.get("responsible") or "").strip()
    selected_payment = (req_args.get("payment_method") or "").strip()

    # Логика фильтрации: по всем активным фильтрам сразу (AND).
    if selected_responsible:
        rows = [r for r in rows if r.get("responsible") == selected_responsible]
    if selected_payment:
        rows = [r for r in rows if r.get("payment_method") == selected_payment]

    def _to_amount(v):
        s = str(v or "").strip()
        if not s or s == "null":
            return None
        s = s.replace(" ", "").replace(",", ".")
        try:
            return float(s)
        except Exception:
            return None

    amounts = [_to_amount(r.get("total_price")) for r in rows]
    amounts = [a for a in amounts if a is not None]
    total_amount = sum(amounts) if amounts else 0.0
    avg_amount = (total_amount / len(amounts)) if amounts else 0.0

    by_payment = {}
    by_responsible = {}
    by_responsible_amount = {}
    for r in rows:
        pm = r.get("payment_method") or "null"
        resp = r.get("responsible") or "null"
        by_payment[pm] = by_payment.get(pm, 0) + 1
        by_responsible[resp] = by_responsible.get(resp, 0) + 1
        by_responsible_amount[resp] = by_responsible_amount.get(resp, 0.0) + (_to_amount(r.get("total_price")) or 0.0)

    top_by_amount = sorted(
        [r for r in rows if _to_amount(r.get("total_price")) is not None],
        key=lambda r: _to_amount(r.get("total_price")) or 0,
        reverse=True,
    )[:5]
    missing_house_area_lead_ids = sorted(
        {
            int(r.get("lead_id"))
            for r in rows
            if r.get("needs_house_area_fill") and str(r.get("lead_id") or "").isdigit()
        }
    )

    return {
        "rows": rows,
        "responsible_options": responsible_options,
        "payment_options": payment_options,
        "selected_responsible": selected_responsible,
        "selected_payment": selected_payment,
        "summary": {
            "count": len(rows),
            "count_with_amount": len(amounts),
            "total_amount": int(total_amount),
            "avg_amount": int(avg_amount),
            "by_payment": by_payment,
            "by_responsible": by_responsible,
            "by_responsible_amount": {k: int(round(v)) for k, v in by_responsible_amount.items()},
            "top_by_amount": top_by_amount,
            "missing_house_area_lead_ids": missing_house_area_lead_ids,
        },
    }


@app.route("/admin/amocrm/leads/sync", methods=["POST"])
@projects_manager_required
@permission_required("amocrm_leads")
def amocrm_leads_sync():
    """Синхронизировать отчёт потенциальных клиентов и перезаписать кэш."""
    conn = get_db()
    payload, err = _build_amocrm_potential_clients_rows()
    if err:
        conn.close()
        return Response(
            json.dumps({"ok": False, "error": err}, ensure_ascii=False),
            status=500,
            mimetype="application/json",
        )
    _amocrm_cache_set(conn, "potential_clients", payload)
    conn.commit()
    conn.close()
    return Response(
        json.dumps(
            {
                "ok": True,
                "updated_at": payload.get("generated_at"),
                "rows_count": len(payload.get("rows") or []),
            },
            ensure_ascii=False,
        ),
        status=200,
        mimetype="application/json",
    )


@app.route("/admin/amocrm/leads/data", methods=["GET"])
@projects_manager_required
@permission_required("amocrm_leads")
def amocrm_leads_data():
    """JSON данные отчёта потенциальных клиентов из кэша (для ajax-обновления)."""
    conn = get_db()
    cache = _amocrm_cache_get(conn, "potential_clients") or {"rows": [], "generated_at": None}
    data = _amocrm_report_view_data(cache, request.args)
    rows_html = render_template("reports/_amocrm_leads_rows.html", rows=data["rows"])
    cards_html = render_template("reports/_amocrm_leads_kanban.html", rows=data["rows"])
    grouped_html = render_template("reports/_amocrm_leads_grouped.html", rows=data["rows"])
    list_html = render_template("reports/_amocrm_leads_summary.html", summary=data["summary"])
    totals_html = render_template("reports/_amocrm_responsible_totals.html", totals=data["summary"].get("by_responsible_amount") or {}, title="Итоговая стоимость по ответственным")
    conn.close()
    return Response(
        json.dumps(
            {
                "ok": True,
                "rows_html": rows_html,
                "cards_html": cards_html,
                "grouped_html": grouped_html,
                "list_html": list_html,
                "totals_html": totals_html,
                "rows_count": len(data["rows"]),
                "cache_updated_at": cache.get("_cache_updated_at") or format_project_dt(cache.get("generated_at")),
                "responsible_options": data["responsible_options"],
                "payment_options": data["payment_options"],
                "selected_responsible": data["selected_responsible"],
                "selected_payment": data["selected_payment"],
                "summary": data["summary"],
            },
            ensure_ascii=False,
        ),
        status=200,
        mimetype="application/json",
    )


@app.route("/admin/amocrm/leads", methods=["GET"])
@projects_manager_required
@permission_required("amocrm_leads")
def amocrm_leads_report():
    """Отчёт «Потенциальные клиенты»: быстрое открытие из кэша, синхронизация отдельно."""
    conn = get_db()
    cache = _amocrm_cache_get(conn, "potential_clients")
    error = None
    if (not cache) or int(cache.get("version") or 0) < 5:
        payload, err = _build_amocrm_potential_clients_rows()
        if err:
            error = err
            payload = {"rows": [], "generated_at": None}
        else:
            _amocrm_cache_set(conn, "potential_clients", payload)
            conn.commit()
        cache = payload

    data = _amocrm_report_view_data(cache, request.args)
    conn.close()

    return render_template(
        "reports/amocrm_leads.html",
        error=error,
        leads=[],
        pipelines=[],
        users_map={},
        status_map={},
        rows=data["rows"],
        responsible_options=data["responsible_options"],
        payment_options=data["payment_options"],
        selected_responsible=data["selected_responsible"],
        selected_payment=data["selected_payment"],
        summary=data["summary"],
        now=project_now(),
        cache_updated_at=cache.get("_cache_updated_at") or format_project_dt(cache.get("generated_at")),
    )


def _amocrm_fmt_money(v):
    s = str(v or "").strip()
    if not s or s == "null":
        return "null"
    s = s.replace(" ", "").replace(",", ".")
    try:
        n = float(s)
        return f"{int(round(n)):,}".replace(",", " ")
    except Exception:
        return "null"


def _amocrm_parse_num(v):
    s = str(v or "").strip()
    if not s or s == "null":
        return None
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _amocrm_modules_to_sqm(modules_value, fallback_modules=0.0):
    modules_num = _amocrm_parse_num(modules_value)
    if (modules_num is None) or (modules_num <= 0):
        modules_num = float(fallback_modules or 0.0)
    if modules_num <= 0:
        return None
    return modules_num * MODULE_TO_SQM


def _amocrm_row_area_sqm(row):
    house_area_num = _amocrm_parse_num(row.get("house_area"))
    if house_area_num and house_area_num > 0:
        return house_area_num
    modules_fallback = 1.0 if _module_category_from_project_type(row.get("project_type")) == "modular" else 0.0
    return _amocrm_modules_to_sqm(row.get("modules"), fallback_modules=modules_fallback)


def _amocrm_fmt_sqm(v):
    n = _amocrm_parse_num(v)
    if n is None:
        return "null"
    return f"{n:.2f}".replace(".", ",")


def _amocrm_fmt_date(v):
    if v in (None, "", "null"):
        return "null"
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(int(v)).strftime("%d.%m.%Y")
        s = str(v).strip()
        if s.isdigit():
            return datetime.fromtimestamp(int(s)).strftime("%d.%m.%Y")
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            y = int(s[0:4])
            m = int(s[5:7])
            d = int(s[8:10])
            return date(y, m, d).strftime("%d.%m.%Y")
        return s
    except Exception:
        return "null"


def _build_amocrm_projects_rows():
    """Собрать строки отчёта 'Проекты Sherwood Home' из воронки 'Сопровождение'."""
    import amocrm_api

    leads, err = amocrm_api.amocrm_get_all_leads()
    if err:
        return None, err
    pipelines, _ = amocrm_api.amocrm_get_pipelines()
    users_list, users_err = amocrm_api.amocrm_get_users()
    users_map = {u["id"]: " ".join((u.get("name") or "").split()) for u in (users_list or [])} if not users_err else {}

    # pipeline "Сопровождение"
    support_pipeline = None
    for p in (pipelines or []):
        if (p.get("name") or "").strip().lower() == "сопровождение":
            support_pipeline = p
            break
    if not support_pipeline:
        return {"rows": [], "generated_at": project_now().strftime("%d.%m.%Y %H:%M:%S"), "version": 3}, None

    # field ids (validated on reference deal 28524885)
    CF = {
        "project_name": 1638389,     # Название прогнозируемого проект
        "client_name": 1629943,      # Имя
        "legal_entity": 1638395,     # ЮЛ договора
        "project_type": 1638397,     # Проект
        "modules": 1638399,          # Количество модулей в проекте
        "prepay": 1638401,           # Сумма предоплаты
        "postpay": 1638405,          # Сумма постоплаты
        "extras": 1638409,           # Допы (затраты на подрядчиков суммой)
        "full_cost": 1638411,        # Полная стоимость дома (= модули + допы)
        "payment_method": 1638413,   # Способ оплаты
        "contract_total": 1638415,   # Итоговая стоимость договора
        "prepay_date": 1638403,      # Дата предоплаты
        "prod_start": 1638417,       # Дата начала производства
        "prod_end": 1638419,         # Дата окончания производства
        "contract_date": 1638393,    # Дата заключения договора.
        "delivery_date": 1638421,    # Дата сдачи по договору
        "contract_number": 1638391,  # Номер договора
    }

    def cf_value(lead, fid):
        for cf in (lead.get("custom_fields_values") or []):
            if int(cf.get("field_id") or 0) != int(fid):
                continue
            vals = cf.get("values") or []
            if not vals:
                return None
            v = vals[0]
            return v.get("value") if isinstance(v, dict) else v
        return None

    def cf_by_names(lead, names):
        """Поиск значения кастомного поля по имени (без привязки к ID)."""
        wanted = {str(n or "").strip().lower() for n in names if n}
        for cf in (lead.get("custom_fields_values") or []):
            nm = str(cf.get("field_name") or cf.get("field_code") or "").strip().lower()
            if nm not in wanted:
                continue
            vals = cf.get("values") or []
            if not vals:
                return None
            first = vals[0]
            return first.get("value") if isinstance(first, dict) else first
        return None

    rows = []
    for lead in leads:
        if int(lead.get("pipeline_id") or 0) != int(support_pipeline.get("id") or 0):
            continue

        raw_name = cf_value(lead, CF["project_name"])
        project_name = (str(raw_name).strip() if raw_name not in (None, "") else "").strip()
        if not project_name:
            # Если название проекта не заполнено, всё равно берём сделку в отчёт,
            # чтобы не терять продажи для TV-дашборда. Присваиваем техническое имя.
            project_name = f"Без названия (сделка {lead.get('id')})"

        responsible = users_map.get(lead.get("responsible_user_id"), "null")
        house_area = cf_by_names(lead, ["площадь дома", "площадь дома ", "планируемая площадь", "планируемая площадь:"])
        modules_raw = cf_value(lead, CF["modules"])
        modules_sqm = _amocrm_modules_to_sqm(modules_raw, fallback_modules=1.0)
        house_area_num = _amocrm_parse_num(house_area)
        area_sqm = house_area_num if (house_area_num and house_area_num > 0) else modules_sqm
        rows.append({
            "lead_id": lead.get("id"),
            "project_name": project_name or "null",
            "client_name": str(cf_value(lead, CF["client_name"]) or "null").strip() or "null",
            "legal_entity": str(cf_value(lead, CF["legal_entity"]) or "null").strip() or "null",
            "project_type": str(cf_value(lead, CF["project_type"]) or "null").strip() or "null",
            "responsible": responsible if responsible else "null",
            "modules": _amocrm_fmt_sqm(modules_sqm),
            "area_sqm": _amocrm_fmt_sqm(area_sqm),
            "prepay": _amocrm_fmt_money(cf_value(lead, CF["prepay"])),
            "postpay": _amocrm_fmt_money(cf_value(lead, CF["postpay"])),
            "extras": _amocrm_fmt_money(cf_value(lead, CF["extras"])),
            "full_cost": _amocrm_fmt_money(cf_value(lead, CF["full_cost"])),
            "payment_method": str(cf_value(lead, CF["payment_method"]) or "null").strip() or "null",
            "contract_total": _amocrm_fmt_money(cf_value(lead, CF["contract_total"])),
            "prepay_date": _amocrm_fmt_date(cf_value(lead, CF["prepay_date"])),
            "prod_start": _amocrm_fmt_date(cf_value(lead, CF["prod_start"])),
            "prod_end": _amocrm_fmt_date(cf_value(lead, CF["prod_end"])),
            "contract_date": _amocrm_fmt_date(cf_value(lead, CF["contract_date"])),
            "delivery_date": _amocrm_fmt_date(cf_value(lead, CF["delivery_date"])),
            "contract_number": str(cf_value(lead, CF["contract_number"]) or "null").strip() or "null",
            "house_area": str(house_area).strip() if house_area not in (None, "") else "null",
            "needs_house_area_fill": bool(modules_sqm and (not house_area_num or house_area_num <= 0)),
        })

    payload = {
        "rows": rows,
        "generated_at": project_now().strftime("%d.%m.%Y %H:%M:%S"),
        "version": 3,
    }
    return payload, None


def _amocrm_projects_view_data(cache, req_args):
    rows = list((cache or {}).get("rows") or [])

    # options
    def opts(key):
        return sorted({r.get(key) for r in rows if r.get(key) not in (None, "", "null")})

    options = {
        "contract_date": opts("contract_date"),
        "project_type": opts("project_type"),
        "prod_start": opts("prod_start"),
        "prod_end": opts("prod_end"),
        "delivery_date": opts("delivery_date"),
        "responsible": opts("responsible"),
        "legal_entity": opts("legal_entity"),
        "payment_method": opts("payment_method"),
    }

    selected = {
        "contract_date": (req_args.get("contract_date") or "").strip(),
        "project_type": (req_args.get("project_type") or "").strip(),
        "prod_start": (req_args.get("prod_start") or "").strip(),
        "prod_end": (req_args.get("prod_end") or "").strip(),
        "delivery_date": (req_args.get("delivery_date") or "").strip(),
        "responsible": (req_args.get("responsible") or "").strip(),
        "legal_entity": (req_args.get("legal_entity") or "").strip(),
        "payment_method": (req_args.get("payment_method") or "").strip(),
    }

    # apply all active filters (AND)
    for k, v in selected.items():
        if v:
            rows = [r for r in rows if r.get(k) == v]

    def to_num(v):
        s = str(v or "").strip().replace(" ", "").replace(",", ".")
        if not s or s == "null":
            return 0.0
        try:
            return float(s)
        except Exception:
            return 0.0

    total_contract = sum(to_num(r.get("contract_total")) for r in rows)
    total_full = sum(to_num(r.get("full_cost")) for r in rows)
    total_area_sqm = sum(to_num(r.get("area_sqm")) for r in rows)
    total_contractors = sum(to_num(r.get("extras")) for r in rows)

    by_payment = {}
    by_responsible = {}
    by_responsible_amount = {}
    for r in rows:
        pm = r.get("payment_method") or "null"
        resp = r.get("responsible") or "null"
        by_payment[pm] = by_payment.get(pm, 0) + 1
        by_responsible[resp] = by_responsible.get(resp, 0) + 1
        by_responsible_amount[resp] = by_responsible_amount.get(resp, 0.0) + to_num(r.get("contract_total"))

    top_by_contract = sorted(
        [r for r in rows if to_num(r.get("contract_total")) > 0],
        key=lambda r: to_num(r.get("contract_total")),
        reverse=True,
    )[:7]

    missing_house_area_lead_ids = sorted(
        {
            int(r.get("lead_id"))
            for r in rows
            if r.get("needs_house_area_fill") and str(r.get("lead_id") or "").isdigit()
        }
    )

    summary = {
        "total_contract": f"{int(round(total_contract)):,}".replace(",", " "),
        "total_full": f"{int(round(total_full)):,}".replace(",", " "),
        "total_area_sqm": f"{total_area_sqm:.2f}".replace(".", ","),
        "total_contractors": f"{int(round(total_contractors)):,}".replace(",", " "),
        "count": len(rows),
        "by_payment": by_payment,
        "by_responsible": by_responsible,
        "by_responsible_amount": {k: int(round(v)) for k, v in by_responsible_amount.items()},
        "top_by_contract": top_by_contract,
        "missing_house_area_lead_ids": missing_house_area_lead_ids,
    }

    return {"rows": rows, "options": options, "selected": selected, "summary": summary}


@app.route("/admin/amocrm/projects/sync", methods=["POST"])
@admin_required
@permission_required("amocrm_projects")
def amocrm_projects_sync():
    """Синхронизация отчёта Проекты Sherwood Home (воронка Сопровождение)."""
    conn = get_db()
    payload, err = _build_amocrm_projects_rows()
    if err:
        conn.close()
        return Response(json.dumps({"ok": False, "error": err}, ensure_ascii=False), status=500, mimetype="application/json")
    _amocrm_cache_set(conn, "projects_sherwood_home", payload)
    conn.commit()
    conn.close()
    return Response(
        json.dumps({"ok": True, "updated_at": payload.get("generated_at"), "rows_count": len(payload.get("rows") or [])}, ensure_ascii=False),
        status=200,
        mimetype="application/json",
    )


@app.route("/admin/amocrm/projects/data", methods=["GET"])
@admin_required
@permission_required("amocrm_projects")
def amocrm_projects_data():
    """JSON данные отчёта Проекты Sherwood Home из кэша."""
    conn = get_db()
    cache = _amocrm_cache_get(conn, "projects_sherwood_home") or {"rows": [], "generated_at": None, "version": 3}
    data = _amocrm_projects_view_data(cache, request.args)
    rows_html = render_template("reports/_amocrm_projects_rows.html", rows=data["rows"])
    kpis_html = render_template("reports/_amocrm_projects_kpis.html", summary=data["summary"])
    cards_html = render_template("reports/_amocrm_projects_cards.html", rows=data["rows"])
    grouped_html = render_template("reports/_amocrm_projects_grouped.html", rows=data["rows"])
    list_html = render_template("reports/_amocrm_projects_summary.html", summary=data["summary"])
    totals_html = render_template("reports/_amocrm_responsible_totals.html", totals=data["summary"].get("by_responsible_amount") or {}, title="Итоговая стоимость по ответственным")
    conn.close()
    return Response(
        json.dumps(
            {
                "ok": True,
                "rows_html": rows_html,
                "kpis_html": kpis_html,
                "cards_html": cards_html,
                "grouped_html": grouped_html,
                "list_html": list_html,
                "totals_html": totals_html,
                "rows_count": len(data["rows"]),
                "cache_updated_at": cache.get("_cache_updated_at") or format_project_dt(cache.get("generated_at")),
                "options": data["options"],
                "selected": data["selected"],
                "summary": data["summary"],
            },
            ensure_ascii=False,
        ),
        status=200,
        mimetype="application/json",
    )


@app.route("/admin/amocrm/projects", methods=["GET"])
@admin_required
@permission_required("amocrm_projects")
def amocrm_projects_report():
    """Отчёт Проекты Sherwood Home (воронка Сопровождение)."""
    conn = get_db()
    cache = _amocrm_cache_get(conn, "projects_sherwood_home")
    error = None
    if (not cache) or int(cache.get("version") or 0) < 3:
        payload, err = _build_amocrm_projects_rows()
        if err:
            error = err
            payload = {"rows": [], "generated_at": None, "version": 3}
        else:
            _amocrm_cache_set(conn, "projects_sherwood_home", payload)
            conn.commit()
        cache = payload

    data = _amocrm_projects_view_data(cache, request.args)
    conn.close()
    return render_template(
        "reports/amocrm_projects.html",
        error=error,
        rows=data["rows"],
        summary=data["summary"],
        options=data["options"],
        selected=data["selected"],
        now=project_now(),
        cache_updated_at=cache.get("_cache_updated_at") or format_project_dt(cache.get("generated_at")),
    )


@app.route("/admin/work-items", methods=["GET", "POST"])
@work_items_manager_required
def admin_work_items():
    """Справочник работ: ч/ч, стоимость часа, стоимость работы и активность."""
    conn = get_db()
    if request.method == "POST":
        action = request.form.get("action")
        def _pf(v):
            v = (v or "").strip()
            if not v:
                return 0.0
            try:
                return float(v.replace(",", "."))
            except ValueError:
                return 0.0

        if action == "save":
            items = conn.execute("SELECT id FROM work_items").fetchall()
            updated = 0
            name_errors = 0
            for r in items:
                wid = r["id"]
                name = (request.form.get(f"name_{wid}", "") or "").strip()
                labor_raw = request.form.get(f"labor_{wid}", "")
                hour_raw = request.form.get(f"hour_{wid}", "")
                cost_raw = request.form.get(f"cost_{wid}", "")
                price_raw = request.form.get(f"price_{wid}", "")
                active_raw = request.form.get(f"active_{wid}")
                active = 1 if active_raw in ("1", "on", "true", "yes") else 0
                labor = _pf(labor_raw)
                hour_price = _pf(hour_raw)
                work_cost = _pf(cost_raw)
                # unit_price оставляем для совместимости с отчётами (ставка за 100% = стоимость работы)
                unit_price = _pf(price_raw) or work_cost
                work_item_type = (request.form.get(f"type_{wid}", "") or "production").strip()
                if work_item_type not in ("production", "construction"):
                    work_item_type = "production"
                try:
                    if name:
                        conn.execute(
                            "UPDATE work_items SET name = ?, work_item_type = ?, labor_hours = ?, hour_price = ?, work_cost = ?, unit_price = ?, active = ? WHERE id = ?",
                            (name, work_item_type, labor, hour_price, work_cost, unit_price, active, wid),
                        )
                    else:
                        conn.execute(
                            "UPDATE work_items SET work_item_type = ?, labor_hours = ?, hour_price = ?, work_cost = ?, unit_price = ?, active = ? WHERE id = ?",
                            (work_item_type, labor, hour_price, work_cost, unit_price, active, wid),
                        )
                except sqlite3.IntegrityError:
                    name_errors += 1
                updated += 1
            conn.commit()
            try:
                _rebuild_work_item_codes(conn)
                conn.commit()
            except Exception:
                pass
            if name_errors:
                flash(f"Сохранено. Обновлено строк: {updated}. Ошибок по названию (дубликаты): {name_errors}.", "warning")
            else:
                flash(f"Сохранено. Обновлено строк: {updated}.", "success")
        elif action == "add":
            name = (request.form.get("name", "") or "").strip()
            work_item_type = (request.form.get("work_item_type", "") or "production").strip()
            if work_item_type not in ("production", "construction"):
                work_item_type = "production"
            labor = _pf(request.form.get("labor_hours"))
            hour_price = _pf(request.form.get("hour_price"))
            work_cost = _pf(request.form.get("work_cost"))
            unit_price = _pf(request.form.get("unit_price")) or work_cost
            if not name:
                flash("Введите название работы.", "error")
            else:
                try:
                    conn.execute(
                        "INSERT INTO work_items (name, work_item_type, labor_hours, hour_price, work_cost, unit_price, active) VALUES (?, ?, ?, ?, ?, ?, 1)",
                        (name, work_item_type, labor, hour_price, work_cost, unit_price),
                    )
                    conn.commit()
                    try:
                        _rebuild_work_item_codes(conn)
                        conn.commit()
                    except Exception:
                        pass
                    flash("Работа добавлена.", "success")
                except sqlite3.IntegrityError:
                    flash("Такая работа уже существует.", "error")
        elif action == "delete":
            wid = request.form.get("work_item_id", type=int)
            if not wid:
                flash("Не выбрана работа.", "error")
            else:
                used = conn.execute(
                    "SELECT 1 FROM worker_daily_report_items WHERE work_item_id = ? LIMIT 1",
                    (wid,),
                ).fetchone()
                if used:
                    conn.execute("UPDATE work_items SET active = 0 WHERE id = ?", (wid,))
                    conn.commit()
                    flash("Работа уже использовалась в отчётах, поэтому она скрыта (деактивирована).", "info")
                else:
                    conn.execute("DELETE FROM work_items WHERE id = ?", (wid,))
                    conn.commit()
                    flash("Работа удалена.", "info")
                try:
                    _rebuild_work_item_codes(conn)
                    conn.commit()
                except Exception:
                    pass
        elif action == "import_price_list":
            upserted = 0
            for name, labor, hour_price, work_cost in WORK_ITEMS_PRICE_LIST:
                # подстраховка: если work_cost не совпадает — считаем
                try:
                    calc = float(labor) * float(hour_price)
                except Exception:
                    calc = float(work_cost or 0)
                final_cost = float(work_cost) if work_cost is not None else calc
                if abs(final_cost - calc) > 0.01 and calc > 0:
                    # берём присланное значение, но unit_price = work_cost
                    pass
                conn.execute(
                    """INSERT INTO work_items (name, work_item_type, labor_hours, hour_price, work_cost, unit_price, active)
                       VALUES (?, 'production', ?, ?, ?, ?, 1)
                       ON CONFLICT(name) DO UPDATE SET
                         labor_hours=excluded.labor_hours,
                         hour_price=excluded.hour_price,
                         work_cost=excluded.work_cost,
                         unit_price=excluded.unit_price,
                         active=1""",
                    (name, float(labor), float(hour_price), float(final_cost), float(final_cost)),
                )
                upserted += 1
            conn.commit()
            # после импорта обновим нумерацию (code) по алфавиту
            try:
                _rebuild_work_item_codes(conn)
                conn.commit()
            except Exception:
                pass
            flash(f"Импортировано/обновлено работ: {upserted}.", "success")
        elif action == "dedupe_and_renumber":
            try:
                res = _dedupe_work_items(conn)
                _rebuild_work_item_codes(conn)
                conn.commit()
                flash(
                    f"Готово. Удалено дублей: {res.get('deleted', 0)}. Нумерация обновлена.",
                    "success",
                )
            except Exception:
                conn.rollback()
                flash("Не удалось выполнить очистку дублей.", "error")
        conn.close()
        return redirect(url_for("admin_work_items"))

    sort = (request.args.get("sort") or "name").strip().lower()
    direction = (request.args.get("dir") or "asc").strip().lower()
    filter_type = (request.args.get("type") or "").strip().lower()
    if filter_type not in ("production", "construction"):
        filter_type = None
    if sort not in ("code", "id", "name", "labor_hours", "hour_price", "work_cost", "active"):
        sort = "name"
    if direction not in ("asc", "desc"):
        direction = "asc"

    if sort == "code":
        order_sql = f"COALESCE(code, id) {direction.upper()}, name COLLATE NOCASE ASC, id ASC"
    elif sort == "id":
        order_sql = f"id {direction.upper()}"
    elif sort == "labor_hours":
        order_sql = f"COALESCE(labor_hours, 0) {direction.upper()}, name COLLATE NOCASE ASC, id ASC"
    elif sort == "hour_price":
        order_sql = f"COALESCE(hour_price, 0) {direction.upper()}, name COLLATE NOCASE ASC, id ASC"
    elif sort == "work_cost":
        order_sql = f"COALESCE(work_cost, unit_price, 0) {direction.upper()}, name COLLATE NOCASE ASC, id ASC"
    elif sort == "active":
        order_sql = f"active {direction.upper()}, name COLLATE NOCASE ASC, id ASC"
    else:
        order_sql = f"name COLLATE NOCASE {direction.upper()}, id ASC"

    where_clause = ""
    if filter_type:
        where_clause = f" WHERE work_item_type = '{filter_type}' "
    items = [dict(r) for r in conn.execute(
        f"""SELECT id, code, name, active,
                  COALESCE(work_item_type, 'production') as work_item_type,
                  COALESCE(labor_hours, 0) as labor_hours,
                  COALESCE(hour_price, 0) as hour_price,
                  COALESCE(work_cost, 0) as work_cost,
                  COALESCE(work_cost, unit_price, 0) as unit_price
           FROM work_items
           {where_clause}
           ORDER BY CASE WHEN work_item_type = 'production' THEN 0 ELSE 1 END, {order_sql}"""
    ).fetchall()]
    conn.close()
    return render_template(
        "admin/work_items.html",
        items=items,
        work_item_types=WORK_ITEM_TYPES,
        current_sort=sort,
        current_dir=direction,
        filter_type=filter_type,
        current_role=session.get("role"),
    )


@app.route("/admin/daily-report/<int:daily_id>/delete", methods=["POST"])
@admin_required
def admin_daily_report_delete(daily_id):
    """Удалить закрытие дня целиком (шапка + строки)."""
    conn = get_db()
    conn.execute("DELETE FROM worker_daily_report_items WHERE daily_report_id = ?", (daily_id,))
    conn.execute("DELETE FROM worker_daily_reports WHERE id = ?", (daily_id,))
    conn.commit()
    conn.close()
    flash("Закрытие дня удалено.", "info")
    return redirect(request.referrer or url_for("worker_reports"))


@app.route("/admin/daily-report/<int:daily_id>/edit", methods=["GET", "POST"])
@admin_required
def admin_daily_report_edit(daily_id):
    """Редактировать закрытие дня (проценты/комментарии/удаление строк)."""
    conn = get_db()
    header = conn.execute(
        """SELECT dr.id, dr.report_date, dr.created_at,
                  u.full_name as user_name, u.role as user_role,
                  p.name as project_name, p.id as project_id
           FROM worker_daily_reports dr
           JOIN users u ON u.id = dr.worker_id
           JOIN projects p ON p.id = dr.project_id
           WHERE dr.id = ?""",
        (daily_id,),
    ).fetchone()
    if not header:
        conn.close()
        flash("Закрытие дня не найдено.", "error")
        return redirect(url_for("worker_reports"))
    header = dict(header)

    if request.method == "POST":
        item_ids = request.form.getlist("item_id")
        percents = request.form.getlist("percent")
        comments = request.form.getlist("comment")
        deletes = set(request.form.getlist("delete_item"))
        updated = 0
        deleted = 0
        for idx, iid in enumerate(item_ids):
            iid = (iid or "").strip()
            if not iid:
                continue
            if iid in deletes:
                conn.execute(
                    "DELETE FROM worker_daily_report_items WHERE id = ? AND daily_report_id = ?",
                    (int(iid), daily_id),
                )
                deleted += 1
                continue
            pct_raw = (percents[idx] if idx < len(percents) else "").strip()
            cmt = (comments[idx] if idx < len(comments) else "").strip()
            try:
                pct = float(pct_raw.replace(",", ".")) if pct_raw else 0.0
            except ValueError:
                pct = 0.0
            if pct < 0:
                pct = 0.0
            if pct > 100:
                pct = 100.0
            conn.execute(
                "UPDATE worker_daily_report_items SET percent = ?, comment = ? WHERE id = ? AND daily_report_id = ?",
                (pct, cmt, int(iid), daily_id),
            )
            updated += 1
        conn.commit()
        conn.close()
        flash(f"Сохранено. Обновлено: {updated}, удалено строк: {deleted}.", "success")
        return redirect(url_for("admin_daily_report_edit", daily_id=daily_id))

    items = [dict(r) for r in conn.execute(
        """SELECT i.id, w.name as work_name, i.percent, i.comment
           FROM worker_daily_report_items i
           JOIN work_items w ON w.id = i.work_item_id
           WHERE i.daily_report_id = ?
           ORDER BY w.name""",
        (daily_id,),
    ).fetchall()]
    conn.close()
    return render_template("admin/daily_report_edit.html", header=header, items=items)


def _render_worker_reports_template(template_path: str, *, report_key: str | None = None):
    """Общий рендер отчётов работников (HTML/CSV) для разных шаблонов."""
    role = session.get("role")
    user_id = session.get("user_id")

    group = (request.args.get("group") or "day").strip().lower()
    if group not in ("day", "week"):
        group = "day"

    d_from_raw = (request.args.get("date_from") or "").strip()
    d_to_raw = (request.args.get("date_to") or "").strip()
    d_from = _parse_date(d_from_raw)
    d_to = _parse_date(d_to_raw)
    if not d_from or not d_to:
        d_from, d_to = _default_month_range()
    if d_to < d_from:
        d_from, d_to = d_to, d_from

    project_id = request.args.get("project_id", type=int)
    worker_id = request.args.get("worker_id", type=int)
    date_eq_raw = (request.args.get("date_eq") or "").strip()
    date_eq = _parse_date(date_eq_raw)
    work_item_id = request.args.get("work_item_id", type=int)
    pct_end_raw = (request.args.get("pct_end") or "").strip()
    amount_bucket = (request.args.get("amount_bucket") or "").strip()
    comment_state = (request.args.get("comment_state") or "").strip().lower()
    out_format = (request.args.get("format") or "html").strip().lower()

    conn = get_db()

    # Для презентаций: если ещё нет данных по закрытиям дней, заполняем фейковыми отчётами работников.
    _ensure_demo_worker_reports(conn)

    # Данные для фильтров
    if role == "master":
        projects = [dict(r) for r in conn.execute(
            "SELECT id, name FROM projects WHERE master_id = ? ORDER BY name",
            (user_id,),
        ).fetchall()]
        module_projects = [dict(r) for r in conn.execute(
            "SELECT id, name FROM projects WHERE master_id = ? AND type = 'module' ORDER BY name",
            (user_id,),
        ).fetchall()]
        workers = [dict(r) for r in conn.execute(
            "SELECT id, full_name FROM users WHERE role = 'worker' AND reports_to_id = ? ORDER BY full_name",
            (user_id,),
        ).fetchall()]
        masters = []
        prod_directors = []
    elif role == "director_production":
        projects = [dict(r) for r in conn.execute(
            "SELECT id, name FROM projects ORDER BY name"
        ).fetchall()]
        workers = [dict(r) for r in conn.execute(
            "SELECT id, full_name FROM users WHERE role = 'worker' AND reports_to_production_id = ? ORDER BY full_name",
            (user_id,),
        ).fetchall()]
        masters = [dict(r) for r in conn.execute(
            "SELECT id, full_name FROM users WHERE role = 'master' AND reports_to_production_id = ? ORDER BY full_name",
            (user_id,),
        ).fetchall()]
        prod_directors = []
        module_projects = [dict(r) for r in conn.execute(
            "SELECT id, name FROM projects WHERE type = 'module' ORDER BY name"
        ).fetchall()]
    else:
        projects = [dict(r) for r in conn.execute(
            "SELECT id, name FROM projects ORDER BY name"
        ).fetchall()]
        workers = [dict(r) for r in conn.execute(
            "SELECT id, full_name FROM users WHERE role = 'worker' ORDER BY full_name"
        ).fetchall()]
        masters = [dict(r) for r in conn.execute(
            "SELECT id, full_name FROM users WHERE role = 'master' ORDER BY full_name"
        ).fetchall()]
        prod_directors = []
        module_projects = [dict(r) for r in conn.execute(
            "SELECT id, name FROM projects WHERE type = 'module' ORDER BY name"
        ).fetchall()]

    where = ["dr.report_date BETWEEN ? AND ?"]
    params = [d_from.isoformat(), d_to.isoformat()]
    # Только подтверждённые строки (мастером или директором по производству)
    where.append("COALESCE(it.approved_status, 'approved') = 'approved'")
    if project_id:
        where.append("p.id = ?")
        params.append(project_id)
    if worker_id:
        where.append("u.id = ?")
        params.append(worker_id)
    if role == "master":
        where.append("p.master_id = ?")
        params.append(user_id)
        where.append("u.reports_to_id = ?")
        params.append(user_id)
        where.append("u.role = 'worker'")
    elif role == "director_production":
        where.append("u.role = 'worker'")
        where.append("u.reports_to_production_id = ?")
        params.append(user_id)
    else:
        where.append("u.role = 'worker'")

    # Фильтры в строке отчёта (выпадающие списки)
    if date_eq:
        where.append("dr.report_date = ?")
        params.append(date_eq.isoformat())
    if work_item_id:
        where.append("wi.id = ?")
        params.append(int(work_item_id))

    def _pf_float(x: str):
        x = (x or "").strip()
        if not x:
            return None
        try:
            return float(x.replace(",", "."))
        except (TypeError, ValueError):
            return None

    pct_end = _pf_float(pct_end_raw)
    if pct_end is not None:
        where.append("COALESCE(it.percent, 0) = ?")
        params.append(pct_end)

    amount_expr = "(COALESCE(wi.work_cost, wi.unit_price, 0) * (COALESCE(it.percent, 0) / 100.0))"
    if amount_bucket:
        # формат: "A-B" или "A+"
        b = amount_bucket.replace(" ", "")
        if b.endswith("+"):
            lo = _pf_float(b[:-1])
            if lo is not None:
                where.append(f"{amount_expr} >= ?")
                params.append(lo)
        elif "-" in b:
            a_s, b_s = b.split("-", 1)
            lo = _pf_float(a_s)
            hi = _pf_float(b_s)
            if lo is not None:
                where.append(f"{amount_expr} >= ?")
                params.append(lo)
            if hi is not None:
                where.append(f"{amount_expr} <= ?")
                params.append(hi)

    if comment_state == "has":
        where.append("TRIM(COALESCE(it.comment, '')) <> ''")
    elif comment_state == "empty":
        where.append("TRIM(COALESCE(it.comment, '')) = ''")

    sql = f"""
        SELECT
            dr.id as daily_report_id,
            dr.report_date,
            p.id as project_id,
            p.name as project_name,
            u.id as worker_id,
            u.full_name as worker_name,
            wi.id as work_item_id,
            wi.code as work_code,
            wi.name as work_name,
            COALESCE(wi.work_cost, wi.unit_price, 0) as unit_price,
            it.percent as percent,
            {amount_expr} as amount_calc,
            it.comment as comment
        FROM worker_daily_reports dr
        JOIN worker_daily_report_items it ON it.daily_report_id = dr.id
        JOIN work_items wi ON wi.id = it.work_item_id
        JOIN users u ON u.id = dr.worker_id
        JOIN projects p ON p.id = dr.project_id
        WHERE {" AND ".join(where)}
        ORDER BY dr.report_date DESC, p.name ASC, u.full_name ASC, wi.name ASC
    """
    raw = conn.execute(sql, params).fetchall()

    rows = []
    for r in raw:
        rd = _parse_date(r["report_date"])
        unit_price = _format_money(r["unit_price"])
        pct = _format_money(r["percent"])
        amount = _format_money(r["amount_calc"])
        if group == "week" and rd:
            ws = _week_start(rd)
            key = ws.isoformat()
            label = f"{ws.isoformat()} — {(ws + timedelta(days=6)).isoformat()}"
        else:
            key = r["report_date"]
            label = r["report_date"]
        rows.append(
            {
                "group_key": key,
                "group_label": label,
                "daily_report_id": r["daily_report_id"],
                "report_date": r["report_date"],
                "project_id": r["project_id"],
                "project_name": r["project_name"],
                "worker_id": r["worker_id"],
                "worker_name": r["worker_name"],
                "work_item_id": r["work_item_id"],
                "work_code": r["work_code"],
                "work_name": r["work_name"],
                "percent": pct,
                "percent_label": _percent_end_to_label(pct),
                "unit_price": unit_price,
                "amount": amount,
                "comment": r["comment"] or "",
            }
        )
    conn.close()

    # Группировка для UI
    groups_map = {}
    for row in rows:
        gk = row["group_key"]
        if gk not in groups_map:
            groups_map[gk] = {"label": row["group_label"], "rows": [], "total_amount": 0.0}
        groups_map[gk]["rows"].append(row)
        groups_map[gk]["total_amount"] += row["amount"]
    groups = [
        {"key": k, "label": v["label"], "rows": v["rows"], "total_amount": v["total_amount"]}
        for k, v in groups_map.items()
    ]
    groups.sort(key=lambda x: x["key"], reverse=True)

    # Итоги по работникам (для периода)
    by_worker = {}
    for row in rows:
        wk = row["worker_name"]
        by_worker.setdefault(wk, 0.0)
        by_worker[wk] += row["amount"]
    worker_totals = [{"worker_name": k, "total_amount": v} for k, v in by_worker.items()]
    worker_totals.sort(key=lambda x: x["total_amount"], reverse=True)

    # CSV экспорт
    if out_format == "csv":
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=";")
        w.writerow(
            [
                "Период",
                "Дата",
                "Проект",
                "Работник",
                "Код_работы",
                "Работа",
                "Процент_выполнения",
                "Ставка_за_100",
                "Сумма",
                "Комментарий",
            ]
        )
        for row in rows:
            w.writerow(
                [
                    row["group_label"],
                    row["report_date"],
                    row["project_name"],
                    row["worker_name"],
                    row.get("work_code") or row["work_item_id"],
                    row["work_name"],
                    _percent_end_to_label(row["percent"]),
                    f"{row['unit_price']:.2f}",
                    f"{row['amount']:.2f}",
                    row["comment"],
                ]
            )
        content = buf.getvalue()
        filename = f"worker_reports_{d_from.isoformat()}_{d_to.isoformat()}_{group}.csv"
        return _csv_response_utf8(content, filename)

    query_args = dict(request.args)
    query_args.pop("format", None)
    query_args.pop("view", None)
    export_args = {k: v for k, v in query_args.items() if v not in (None, "", "None")}
    export_args["format"] = "csv"
    export_url = url_for("worker_reports", **export_args)
    view_style3_url = url_for("worker_reports", **query_args, view="style3")
    view_table_url = url_for("worker_reports", **query_args, view="table")
    col_widths = (
        _get_json_pref(int(user_id), f"report_colwidths:{report_key}", {})
        if user_id and report_key
        else {}
    )
    view = (request.args.get("view") or "style3").strip().lower()
    if view not in ("style3", "table"):
        view = "style3"

    # Данные для выпадающих фильтров — только значения, присутствующие в отчёте
    dates_list = []
    cur = d_to
    while cur >= d_from and len(dates_list) < 370:
        dates_list.append(cur.isoformat())
        cur -= timedelta(days=1)

    # Уникальные значения из строк отчёта
    projects_in_report = []
    workers_in_report = []
    work_items_in_report = []
    pct_labels_seen = set()
    amount_buckets_seen = set()
    has_comment = False
    has_empty_comment = False

    AMOUNT_BUCKETS = [
        ("", "Сумма"),
        ("0-1000", "0–1 000"),
        ("1000-5000", "1 000–5 000"),
        ("5000-15000", "5 000–15 000"),
        ("15000+", "15 000+"),
    ]

    def _amount_in_bucket(amt: float, bucket: str) -> bool:
        if not bucket:
            return True
        try:
            amt = float(amt or 0)
        except (TypeError, ValueError):
            return False
        if bucket.endswith("+"):
            lo = float(bucket[:-1].replace(" ", ""))
            return amt >= lo
        if "-" in bucket:
            a, b = bucket.split("-", 1)
            lo = float(a.strip())
            hi = float(b.strip())
            return lo <= amt <= hi
        return False

    seen_projects = {}
    seen_workers = {}
    seen_work_items = {}
    if rows:
        amount_buckets_seen.add("")
    for row in rows:
        pid, pname = row["project_id"], row["project_name"]
        if pid not in seen_projects:
            seen_projects[pid] = {"id": pid, "name": pname}
        wid, wname = row["worker_id"], row["worker_name"]
        if wid not in seen_workers:
            seen_workers[wid] = {"id": wid, "full_name": wname}
        wiid, wcode, wname = row["work_item_id"], row["work_code"], row["work_name"]
        if wiid not in seen_work_items:
            seen_work_items[wiid] = {"id": wiid, "code": wcode, "name": wname}
        pl = row.get("percent_label") or ""
        if pl:
            pct_labels_seen.add(pl)
        amt = row.get("amount") or 0
        for bval, _ in AMOUNT_BUCKETS:
            if bval and _amount_in_bucket(amt, bval):
                amount_buckets_seen.add(bval)
                break
        if (row.get("comment") or "").strip():
            has_comment = True
        else:
            has_empty_comment = True

    filter_projects = sorted(seen_projects.values(), key=lambda x: (x["name"] or ""))
    filter_workers = sorted(seen_workers.values(), key=lambda x: (x["full_name"] or ""))
    filter_work_items = sorted(seen_work_items.values(), key=lambda x: (x["name"] or ""))
    filter_pct_options = [
        {"value": str(end), "label": f"{start}-{end}"}
        for start, end in PERCENT_RANGES
        if f"{start}-{end}" in pct_labels_seen
    ]
    for pl in sorted(pct_labels_seen):
        if not any(f"{s}-{e}" == pl for s, e in PERCENT_RANGES):
            filter_pct_options.append({"value": pl, "label": pl})
    filter_amount_options = [
        {"value": bval, "label": blabel}
        for bval, blabel in AMOUNT_BUCKETS
        if bval in amount_buckets_seen or (not bval and amount_buckets_seen)
    ]
    if not filter_amount_options:
        filter_amount_options = [{"value": "", "label": "Сумма"}]
    filter_comment_options = []
    if has_comment:
        filter_comment_options.append({"value": "has", "label": "Есть"})
    if has_empty_comment:
        filter_comment_options.append({"value": "empty", "label": "Пусто"})

    return render_template(
        template_path,
        current_role=role,
        view=view,
        group=group,
        date_from=d_from.isoformat(),
        date_to=d_to.isoformat(),
        project_id=project_id,
        worker_id=worker_id,
        date_eq=(date_eq.isoformat() if date_eq else ""),
        work_item_id=work_item_id,
        pct_end=pct_end_raw,
        amount_bucket=amount_bucket,
        comment_state=comment_state,
        dates_list=dates_list,
        filter_projects=filter_projects,
        filter_workers=filter_workers,
        filter_work_items=filter_work_items,
        filter_pct_options=filter_pct_options,
        filter_amount_options=filter_amount_options,
        filter_comment_options=filter_comment_options,
        projects=projects,
        workers=workers,
        masters=masters,
        module_projects=module_projects,
        groups=groups,
        worker_totals=worker_totals,
        query_args=query_args,
        export_url=export_url,
        view_style3_url=view_style3_url,
        view_table_url=view_table_url,
        col_widths=col_widths,
        report_key=report_key,
    )


@app.route("/worker-reports")
@worker_reports_viewer_required
@permission_required("worker_reports")
def worker_reports():
    """Отчёты работников (закрытия дней) с фильтрами и экспортом CSV.

    В отчёты попадают только подтверждённые строки (approved).
    """
    return _render_worker_reports_template(
        "reports/worker_reports.html",
        report_key="worker_reports_v1",
    )


@app.route("/foreman-reports")
@foreman_reports_viewer_required
@permission_required("foreman_reports")
def foreman_reports():
    """Отчёты прорабов (закрытия дней) с фильтрами и экспортом CSV."""
    role = session.get("role")
    user_id = session.get("user_id")

    group = (request.args.get("group") or "day").strip().lower()
    if group not in ("day", "week"):
        group = "day"

    d_from_raw = (request.args.get("date_from") or "").strip()
    d_to_raw = (request.args.get("date_to") or "").strip()
    d_from = _parse_date(d_from_raw)
    d_to = _parse_date(d_to_raw)
    if not d_from or not d_to:
        d_from, d_to = _default_month_range()
    if d_to < d_from:
        d_from, d_to = d_to, d_from

    project_id = request.args.get("project_id", type=int)
    foreman_id = request.args.get("foreman_id", type=int)
    cons_director_id = request.args.get("cons_director_id", type=int)
    out_format = (request.args.get("format") or "html").strip().lower()

    conn = get_db()

    # Для презентаций: если ещё нет данных по закрытиям дней, заполняем фейковыми отчётами прорабов.
    _ensure_demo_foreman_reports(conn)

    # Данные для фильтров
    if role == "director_construction":
        foremen = [dict(r) for r in conn.execute(
            "SELECT id, full_name FROM users WHERE role = 'foreman' AND reports_to_construction_id = ? ORDER BY full_name",
            (user_id,),
        ).fetchall()]
        projects = [dict(r) for r in conn.execute(
            """SELECT DISTINCT p.id, p.name
               FROM foreman_project_access fpa
               JOIN projects p ON p.id = fpa.project_id
               JOIN users u ON u.id = fpa.foreman_id
               WHERE u.role = 'foreman' AND u.reports_to_construction_id = ?
                 AND p.type IN ('frame', 'gasblock', 'penopolistirol')
               ORDER BY p.name""",
            (user_id,),
        ).fetchall()]
        cons_directors = []
    else:
        foremen = [dict(r) for r in conn.execute(
            "SELECT id, full_name FROM users WHERE role = 'foreman' ORDER BY full_name"
        ).fetchall()]
        projects = [dict(r) for r in conn.execute(
            """SELECT id, name FROM projects 
               WHERE type IN ('frame', 'gasblock', 'penopolistirol') 
               ORDER BY name"""
        ).fetchall()]
        cons_directors = [dict(r) for r in conn.execute(
            "SELECT id, full_name FROM users WHERE role = 'director_construction' ORDER BY full_name"
        ).fetchall()]

    where = ["dr.report_date BETWEEN ? AND ?"]
    params = [d_from.isoformat(), d_to.isoformat()]
    if project_id:
        where.append("p.id = ?")
        params.append(project_id)
    if foreman_id:
        where.append("u.id = ?")
        params.append(foreman_id)
    if cons_director_id and role == "admin":
        where.append("u.reports_to_construction_id = ?")
        params.append(cons_director_id)
    if role == "director_construction":
        where.append("u.reports_to_construction_id = ?")
        params.append(user_id)
    where.append("u.role = 'foreman'")

    sql = f"""
        SELECT
            dr.id as daily_report_id,
            dr.report_date,
            p.id as project_id,
            p.name as project_name,
            u.id as foreman_id,
            u.full_name as foreman_name,
            wi.id as work_item_id,
            wi.code as work_code,
            wi.name as work_name,
            COALESCE(wi.work_cost, wi.unit_price, 0) as unit_price,
            it.percent as percent,
            it.comment as comment
        FROM worker_daily_reports dr
        JOIN worker_daily_report_items it ON it.daily_report_id = dr.id
        JOIN work_items wi ON wi.id = it.work_item_id
        JOIN users u ON u.id = dr.worker_id
        JOIN projects p ON p.id = dr.project_id
        JOIN foreman_project_access fpa ON fpa.project_id = p.id AND fpa.foreman_id = u.id
        WHERE {" AND ".join(where)}
        ORDER BY dr.report_date DESC, p.name ASC, u.full_name ASC, wi.name ASC
    """
    raw = conn.execute(sql, params).fetchall()
    conn.close()

    rows = []
    for r in raw:
        rd = _parse_date(r["report_date"])
        unit_price = _format_money(r["unit_price"])
        pct = _format_money(r["percent"])
        amount = unit_price * (pct / 100.0)
        if group == "week" and rd:
            ws = _week_start(rd)
            key = ws.isoformat()
            label = f"{ws.isoformat()} — {(ws + timedelta(days=6)).isoformat()}"
        else:
            key = r["report_date"]
            label = r["report_date"]
        rows.append(
            {
                "group_key": key,
                "group_label": label,
                "daily_report_id": r["daily_report_id"],
                "report_date": r["report_date"],
                "project_id": r["project_id"],
                "project_name": r["project_name"],
                "foreman_id": r["foreman_id"],
                "foreman_name": r["foreman_name"],
                "work_item_id": r["work_item_id"],
                "work_code": r["work_code"],
                "work_name": r["work_name"],
                "percent": pct,
                "unit_price": unit_price,
                "amount": amount,
                "comment": r["comment"] or "",
            }
        )

    groups_map = {}
    for row in rows:
        gk = row["group_key"]
        if gk not in groups_map:
            groups_map[gk] = {"label": row["group_label"], "rows": [], "total_amount": 0.0}
        groups_map[gk]["rows"].append(row)
        groups_map[gk]["total_amount"] += row["amount"]
    groups = [
        {"key": k, "label": v["label"], "rows": v["rows"], "total_amount": v["total_amount"]}
        for k, v in groups_map.items()
    ]
    groups.sort(key=lambda x: x["key"], reverse=True)

    by_foreman = {}
    for row in rows:
        wk = row["foreman_name"]
        by_foreman.setdefault(wk, 0.0)
        by_foreman[wk] += row["amount"]
    foreman_totals = [{"foreman_name": k, "total_amount": v} for k, v in by_foreman.items()]
    foreman_totals.sort(key=lambda x: x["total_amount"], reverse=True)

    if out_format == "csv":
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=";")
        w.writerow(
            [
                "Период",
                "Дата",
                "Проект",
                "Прораб",
                "Код_работы",
                "Работа",
                "Процент_выполнения",
                "Ставка_за_100",
                "Сумма",
                "Комментарий",
            ]
        )
        for row in rows:
            w.writerow(
                [
                    row["group_label"],
                    row["report_date"],
                    row["project_name"],
                    row["foreman_name"],
                    row.get("work_code") or row["work_item_id"],
                    row["work_name"],
                    _percent_end_to_label(row["percent"]),
                    f"{row['unit_price']:.2f}",
                    f"{row['amount']:.2f}",
                    row["comment"],
                ]
            )
        content = buf.getvalue()
        filename = f"foreman_reports_{d_from.isoformat()}_{d_to.isoformat()}_{group}.csv"
        return _csv_response_utf8(content, filename)

    query_args = dict(request.args)
    query_args.pop("format", None)
    export_args = {k: v for k, v in query_args.items() if v not in (None, "", "None")}
    export_args["format"] = "csv"
    export_url = url_for("foreman_reports", **export_args)
    return render_template(
        "reports/foreman_reports.html",
        current_role=role,
        group=group,
        date_from=d_from.isoformat(),
        date_to=d_to.isoformat(),
        project_id=project_id,
        foreman_id=foreman_id,
        cons_director_id=cons_director_id,
        projects=projects,
        foremen=foremen,
        cons_directors=cons_directors,
        groups=groups,
        foreman_totals=foreman_totals,
        export_url=export_url,
    )


@app.route("/admin/edit-requests", methods=["GET", "POST"])
@director_or_admin_edit_requests
def admin_edit_requests():
    """Заявки на редактирование: админ видит всё; директор по производству — от мастера; директор по строительству — от прораба"""
    current_role = session.get("role")
    conn = get_db()
    if request.method == "POST":
        action = request.form.get("action")
        req_id = request.form.get("request_id", type=int)
        if req_id:
            req = conn.execute(
                "SELECT * FROM edit_requests WHERE id = ? AND status = 'pending'",
                (req_id,),
            ).fetchone()
            if req:
                creator = conn.execute(
                    "SELECT role FROM users WHERE id = ?", (req["master_id"],)
                ).fetchone()
                can_approve = (
                    current_role == "admin"
                    or (current_role == "director_production" and creator and creator["role"] == "master")
                    or (current_role == "director_construction" and creator and creator["role"] == "foreman")
                )
                if not can_approve:
                    flash("Эта заявка не в вашей компетенции.", "error")
                else:
                    admin_comment = request.form.get("admin_comment", "").strip()
                    project_id = conn.execute(
                        "SELECT project_id FROM stages WHERE id = ?", (req["stage_id"],)
                    ).fetchone()["project_id"]

                    if action == "approve":
                        # Архивируем текущие отчёты этапа (токены остаются доступны админу)
                        old_report_ids = [
                            int(r["id"])
                            for r in conn.execute(
                                "SELECT id FROM reports WHERE stage_id = ?",
                                (req["stage_id"],),
                            ).fetchall()
                        ]
                        for rid in old_report_ids:
                            archive_media(
                                "stage_report",
                                rid,
                                by_user_id=session.get("user_id"),
                                reason="stage_edit_approved_replaced",
                            )

                        conn.execute("DELETE FROM reports WHERE stage_id = ?", (req["stage_id"],))
                        photos = conn.execute(
                            "SELECT id, photo_path, comment FROM edit_request_photos WHERE edit_request_id = ?",
                            (req_id,),
                        ).fetchall()
                        for p in photos:
                            # Создаём новый report и переносим файл в reports/
                            conn.execute(
                                "INSERT INTO reports (stage_id, photo_path, comment) VALUES (?, ?, ?)",
                                (req["stage_id"], "", p["comment"]),
                            )
                            new_report_id = int(
                                conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                            )

                            # источник: сначала storage (если есть media для edit_request_photo), иначе static/uploads (старый режим)
                            src_path = None
                            media_conn = get_media_db()
                            try:
                                mf = media_conn.execute(
                                    """SELECT stored_relpath
                                       FROM media_files
                                       WHERE entity_type = 'edit_request_photo'
                                         AND entity_id = ?
                                         AND is_archived = 0
                                       ORDER BY uploaded_at DESC
                                       LIMIT 1""",
                                    (int(p["id"]),),
                                ).fetchone()
                            finally:
                                media_conn.close()
                            if mf and mf["stored_relpath"]:
                                src_path = (STORAGE_ROOT / str(mf["stored_relpath"])).resolve()
                            else:
                                pp = (p["photo_path"] or "").strip()
                                if pp.startswith("uploads/"):
                                    src_path = (app.config["UPLOAD_FOLDER"] / pp.replace("uploads/", "")).resolve()

                            if src_path and src_path.exists():
                                rel_dir = _storage_rel_dir_for(
                                    "stage_report",
                                    project_id=int(project_id),
                                    stage_id=int(req["stage_id"]),
                                )
                                try:
                                    save_media_file_from_path(
                                        src_path,
                                        project_id=int(project_id),
                                        stage_id=int(req["stage_id"]),
                                        entity_type="stage_report",
                                        entity_id=new_report_id,
                                        uploaded_by_id=session.get("user_id"),
                                        rel_dir=rel_dir,
                                        original_filename=src_path.name,
                                    )
                                except Exception:
                                    # если не удалось перенести — удаляем только новую строку отчёта
                                    conn.execute("DELETE FROM reports WHERE id = ?", (new_report_id,))

                            archive_media(
                                "edit_request_photo",
                                int(p["id"]),
                                by_user_id=session.get("user_id"),
                                reason="stage_edit_approved",
                            )
                        conn.execute("UPDATE edit_requests SET status = 'approved' WHERE id = ?", (req_id,))
                        if admin_comment:
                            conn.execute(
                                "INSERT INTO project_chat (project_id, user_id, message, msg_type, stage_id) VALUES (?, ?, ?, 'edit_approval', ?)",
                                (project_id, session["user_id"], admin_comment, req["stage_id"]),
                            )
                        conn.commit()
                        flash("Заявка одобрена. Этап обновлён.", "success")
                    elif action == "reject":
                        photos = conn.execute(
                            "SELECT id, photo_path FROM edit_request_photos WHERE edit_request_id = ?",
                            (req_id,),
                        ).fetchall()
                        for p in photos:
                            archive_media(
                                "edit_request_photo",
                                int(p["id"]),
                                by_user_id=session.get("user_id"),
                                reason="stage_edit_rejected",
                            )
                        conn.execute("DELETE FROM edit_request_photos WHERE edit_request_id = ?", (req_id,))
                        conn.execute("UPDATE edit_requests SET status = 'rejected' WHERE id = ?", (req_id,))
                        if admin_comment:
                            conn.execute(
                                "INSERT INTO project_chat (project_id, user_id, message, msg_type, stage_id) VALUES (?, ?, ?, 'edit_reject', ?)",
                                (project_id, session["user_id"], admin_comment, req["stage_id"]),
                            )
                        conn.commit()
                        flash("Заявка отменена.", "info")
        conn.close()
        return redirect(url_for("admin_edit_requests"))

    raw = conn.execute(
        """SELECT er.*, s.name as stage_name, s.project_id, p.name as project_name, u.full_name as master_name, u.role as creator_role
           FROM edit_requests er
           JOIN stages s ON er.stage_id = s.id
           JOIN projects p ON s.project_id = p.id
           JOIN users u ON er.master_id = u.id
           WHERE er.status = 'pending'
           ORDER BY er.created_at DESC"""
    ).fetchall()
    # Фильтр по роли: директор по производству — только от мастера; директор по строительству — только от прораба
    if current_role == "director_production":
        raw = [r for r in raw if r["creator_role"] == "master"]
    elif current_role == "director_construction":
        raw = [r for r in raw if r["creator_role"] == "foreman"]
    edit_requests = []
    for r in raw:
        req = dict(r)
        req["current_photos"] = [dict(p) for p in conn.execute(
            "SELECT * FROM reports WHERE stage_id = ?", (req["stage_id"],)
        ).fetchall()]
        req["new_photos"] = [dict(p) for p in conn.execute(
            "SELECT * FROM edit_request_photos WHERE edit_request_id = ?", (req["id"],)
        ).fetchall()]

        cur_ids = [int(p["id"]) for p in req["current_photos"]]
        new_ids = [int(p["id"]) for p in req["new_photos"]]
        cur_tokens = get_active_media_tokens_map("stage_report", cur_ids)
        new_tokens = get_active_media_tokens_map("edit_request_photo", new_ids)
        for p in req["current_photos"]:
            p["photo_token"] = cur_tokens.get(int(p["id"]))
        for p in req["new_photos"]:
            p["photo_token"] = new_tokens.get(int(p["id"]))

        edit_requests.append(req)
    conn.close()
    is_director_only = current_role in ("director_production", "director_construction")
    return render_template(
        "admin/edit_requests.html",
        edit_requests=edit_requests,
        is_director_only=is_director_only,
        current_role=current_role,
    )


@app.route("/admin/project/<int:project_id>/chat", methods=["GET", "POST"])
@projects_manager_required
def admin_project_chat(project_id):
    """Чат проекта — admin, manager_op (свои проекты)"""
    conn = get_db()
    project = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    if not project:
        conn.close()
        flash("Проект не найден.", "error")
        return redirect(url_for("admin_dashboard"))
    if session.get("role") == "manager_op":
        uid = session["user_id"]
        if project.get("created_by_id") != uid and project.get("responsible_manager_id") != uid:
            conn.close()
            flash("Доступ запрещён: это не ваш проект.", "error")
            return redirect(url_for("admin_projects"))

    if request.method == "POST":
        msg = request.form.get("message", "").strip()
        stage_id = request.form.get("stage_id", type=int) or None
        from_page = request.form.get("from_page")
        if msg:
            conn.execute(
                "INSERT INTO project_chat (project_id, user_id, message, stage_id) VALUES (?, ?, ?, ?)",
                (project_id, session["user_id"], msg, stage_id),
            )
            conn.commit()
            flash("Сообщение отправлено.", "success")
        conn.close()
        if from_page == "stages":
            return redirect(url_for("admin_project_stages", project_id=project_id))
        return redirect(url_for("admin_project_chat", project_id=project_id))

    stages = [dict(s) for s in conn.execute(
        "SELECT id, name FROM stages WHERE project_id = ? ORDER BY order_num", (project_id,)
    ).fetchall()]
    messages = [dict(row) for row in conn.execute(
        """SELECT pc.*, u.full_name as author_name, s.name as stage_name
           FROM project_chat pc
           JOIN users u ON pc.user_id = u.id
           LEFT JOIN stages s ON pc.stage_id = s.id
           WHERE pc.project_id = ?
           ORDER BY pc.created_at ASC""",
        (project_id,),
    ).fetchall()]
    conn.close()
    return render_template(
        "admin/project_chat.html",
        project=project,
        messages=messages,
        stages=stages,
        building_types=BUILDING_TYPES,
    )


# ============== ДИРЕКТОРА ==============


@app.route("/director/production", methods=["GET", "POST"])
@director_production_required
def director_production_dashboard():
    """Дашборд директора по производству: работники/мастера направления и назначения объектов работникам."""
    conn = get_db()

    # Мастера и работники направления
    masters = [dict(r) for r in conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'master' AND reports_to_production_id = ? ORDER BY full_name",
        (session["user_id"],),
    ).fetchall()]
    workers = [dict(r) for r in conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'worker' AND reports_to_production_id = ? ORDER BY full_name",
        (session["user_id"],),
    ).fetchall()]

    # Проекты только мастеров направления
    projects = [dict(r) for r in conn.execute(
        """SELECT p.id, p.name
           FROM projects p
           JOIN users m ON m.id = p.master_id
           WHERE m.role = 'master' AND m.reports_to_production_id = ?
           ORDER BY p.name""",
        (session["user_id"],),
    ).fetchall()]

    if request.method == "POST":
        action = request.form.get("action")
        worker_id = request.form.get("worker_id", type=int)
        project_id = request.form.get("project_id", type=int)
        if action == "assign" and worker_id and project_id:
            ok_worker = conn.execute(
                "SELECT 1 FROM users WHERE id = ? AND role = 'worker' AND reports_to_production_id = ?",
                (worker_id, session["user_id"]),
            ).fetchone()
            ok_project = conn.execute(
                """SELECT 1 FROM projects p
                   JOIN users m ON m.id = p.master_id
                   WHERE p.id = ? AND m.role = 'master' AND m.reports_to_production_id = ?""",
                (project_id, session["user_id"]),
            ).fetchone()
            if not ok_worker or not ok_project:
                flash("Нельзя назначить: проверьте рабочего и объект в вашем направлении.", "error")
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO worker_project_access
                       (worker_id, project_id, assigned_by_id, assigned_by_role)
                       VALUES (?, ?, ?, ?)""",
                    (worker_id, project_id, session["user_id"], "director_production"),
                )
                conn.commit()
                flash("Объект назначен рабочему.", "success")
        elif action == "unassign" and worker_id and project_id:
            conn.execute(
                """DELETE FROM worker_project_access
                   WHERE worker_id = ? AND project_id = ?
                     AND assigned_by_id = ? AND assigned_by_role = 'director_production'""",
                (worker_id, project_id, session["user_id"]),
            )
            conn.commit()
            flash("Назначение снято.", "info")
        conn.close()
        return redirect(url_for("director_production_dashboard"))

    assignments = [dict(r) for r in conn.execute(
        """SELECT wpa.worker_id, u.full_name as worker_name, wpa.project_id, p.name as project_name, wpa.created_at
           FROM worker_project_access wpa
           JOIN users u ON u.id = wpa.worker_id
           JOIN projects p ON p.id = wpa.project_id
           WHERE wpa.assigned_by_id = ? AND wpa.assigned_by_role = 'director_production'
           ORDER BY wpa.created_at DESC""",
        (session["user_id"],),
    ).fetchall()]
    conn.close()
    return render_template(
        "director/production_dashboard.html",
        masters=masters,
        workers=workers,
        projects=projects,
        assignments=assignments,
    )


@app.route("/master/work-approvals", methods=["GET", "POST"])
@master_required
def master_work_approvals():
    """Подтверждение % выполнения работ работников мастера."""
    conn = get_db()
    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        item_id = request.form.get("item_id", type=int)
        if action in ("approve", "reject") and item_id:
            # проверяем доступ: работник должен быть подчинён мастеру
            row = conn.execute(
                """SELECT it.id
                   FROM worker_daily_report_items it
                   JOIN worker_daily_reports dr ON dr.id = it.daily_report_id
                   JOIN users u ON u.id = dr.worker_id
                   WHERE it.id = ? AND u.role = 'worker' AND u.reports_to_id = ?""",
                (item_id, session["user_id"]),
            ).fetchone()
            if row:
                status = "approved" if action == "approve" else "rejected"
                conn.execute(
                    """UPDATE worker_daily_report_items
                       SET approved_status = ?, approved_by_id = ?, approved_at = CURRENT_TIMESTAMP
                       WHERE id = ?""",
                    (status, session["user_id"], item_id),
                )
                conn.commit()
                flash("Сохранено.", "success")
            else:
                flash("Элемент не найден или доступ запрещён.", "error")
        conn.close()
        return redirect(url_for("master_work_approvals"))

    pending = [dict(r) for r in conn.execute(
        """SELECT it.id as item_id, dr.report_date, dr.created_at,
                  p.name as project_name, p.id as project_id,
                  u.full_name as worker_name, u.id as worker_id,
                  wi.id as work_item_id, wi.code as work_code, wi.name as work_name,
                  it.percent, it.comment
           FROM worker_daily_report_items it
           JOIN worker_daily_reports dr ON dr.id = it.daily_report_id
           JOIN users u ON u.id = dr.worker_id
           JOIN projects p ON p.id = dr.project_id
           JOIN work_items wi ON wi.id = it.work_item_id
           WHERE u.role = 'worker'
             AND u.reports_to_id = ?
             AND COALESCE(it.approved_status, 'approved') = 'pending'
           ORDER BY dr.report_date DESC, dr.created_at DESC""",
        (session["user_id"],),
    ).fetchall()]
    for r in pending:
        r["percent_label"] = _percent_end_to_label(r.get("percent"))
    conn.close()
    return render_template("master/work_approvals.html", items=pending)


@app.route("/director/production/approvals", methods=["GET", "POST"])
@director_production_required
def director_production_work_approvals():
    """Подтверждение % выполнения работ работников направления директора по производству."""
    conn = get_db()
    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        item_id = request.form.get("item_id", type=int)
        if action in ("approve", "reject") and item_id:
            row = conn.execute(
                """SELECT it.id
                   FROM worker_daily_report_items it
                   JOIN worker_daily_reports dr ON dr.id = it.daily_report_id
                   JOIN users u ON u.id = dr.worker_id
                   WHERE it.id = ? AND u.role = 'worker' AND u.reports_to_production_id = ?""",
                (item_id, session["user_id"]),
            ).fetchone()
            if row:
                status = "approved" if action == "approve" else "rejected"
                conn.execute(
                    """UPDATE worker_daily_report_items
                       SET approved_status = ?, approved_by_id = ?, approved_at = CURRENT_TIMESTAMP
                       WHERE id = ?""",
                    (status, session["user_id"], item_id),
                )
                conn.commit()
                flash("Сохранено.", "success")
            else:
                flash("Элемент не найден или доступ запрещён.", "error")
        conn.close()
        return redirect(url_for("director_production_work_approvals"))

    pending = [dict(r) for r in conn.execute(
        """SELECT it.id as item_id, dr.report_date, dr.created_at,
                  p.name as project_name, p.id as project_id,
                  u.full_name as worker_name, u.id as worker_id,
                  wi.id as work_item_id, wi.code as work_code, wi.name as work_name,
                  it.percent, it.comment
           FROM worker_daily_report_items it
           JOIN worker_daily_reports dr ON dr.id = it.daily_report_id
           JOIN users u ON u.id = dr.worker_id
           JOIN projects p ON p.id = dr.project_id
           JOIN work_items wi ON wi.id = it.work_item_id
           WHERE u.role = 'worker'
             AND u.reports_to_production_id = ?
             AND COALESCE(it.approved_status, 'approved') = 'pending'
           ORDER BY dr.report_date DESC, dr.created_at DESC""",
        (session["user_id"],),
    ).fetchall()]
    for r in pending:
        r["percent_label"] = _percent_end_to_label(r.get("percent"))
    conn.close()
    return render_template("director/production_approvals.html", items=pending)


@app.route("/director/construction", methods=["GET", "POST"])
@director_construction_required
def director_construction_dashboard():
    """Дашборд директора по строительству: прорабы направления и назначения объектов прорабам."""
    conn = get_db()

    foremen = [dict(r) for r in conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'foreman' AND reports_to_construction_id = ? ORDER BY full_name",
        (session["user_id"],),
    ).fetchall()]
    projects = [dict(r) for r in conn.execute(
        "SELECT id, name FROM projects ORDER BY name"
    ).fetchall()]

    if request.method == "POST":
        action = request.form.get("action")
        foreman_id = request.form.get("foreman_id", type=int)
        project_id = request.form.get("project_id", type=int)
        if action == "assign" and foreman_id and project_id:
            ok_foreman = conn.execute(
                "SELECT 1 FROM users WHERE id = ? AND role = 'foreman' AND reports_to_construction_id = ?",
                (foreman_id, session["user_id"]),
            ).fetchone()
            ok_project = conn.execute("SELECT 1 FROM projects WHERE id = ?", (project_id,)).fetchone()
            if not ok_foreman or not ok_project:
                flash("Нельзя назначить: проверьте прораба и объект.", "error")
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO foreman_project_access (foreman_id, project_id, assigned_by_id)
                       VALUES (?, ?, ?)""",
                    (foreman_id, project_id, session["user_id"]),
                )
                conn.commit()
                flash("Объект назначен прорабу.", "success")
        elif action == "unassign" and foreman_id and project_id:
            conn.execute(
                "DELETE FROM foreman_project_access WHERE foreman_id = ? AND project_id = ? AND assigned_by_id = ?",
                (foreman_id, project_id, session["user_id"]),
            )
            conn.commit()
            flash("Назначение снято.", "info")
        conn.close()
        return redirect(url_for("director_construction_dashboard"))

    assignments = [dict(r) for r in conn.execute(
        """SELECT fpa.foreman_id, u.full_name as foreman_name, fpa.project_id, p.name as project_name, fpa.created_at
           FROM foreman_project_access fpa
           JOIN users u ON u.id = fpa.foreman_id
           JOIN projects p ON p.id = fpa.project_id
           WHERE fpa.assigned_by_id = ?
           ORDER BY fpa.created_at DESC""",
        (session["user_id"],),
    ).fetchall()]
    conn.close()
    return render_template(
        "director/construction_dashboard.html",
        foremen=foremen,
        projects=projects,
        assignments=assignments,
    )


@app.route("/director/stage-confirmations")
@login_required
def director_stage_confirmations():
    """Этапы, ожидающие подтверждения: дир.производства (модуль), дир.строительства (каркас/газобетон/пенополистирол)"""
    if session.get("role") not in ("director_production", "director_construction"):
        flash("Доступ запрещён.", "error")
        return redirect(url_for("index"))
    conn = get_db()
    role = session.get("role")
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(stages)")
    stage_cols = [r[1] for r in cursor.fetchall()]
    if "stage_confirmed_at" not in stage_cols:
        conn.close()
        return render_template("director/stage_confirmations.html", stages=[], building_types=BUILDING_TYPES)

    if role == "director_production":
        stages = [dict(r) for r in conn.execute(
            """SELECT s.id, s.name, s.stage_confirmed_at, p.id as project_id, p.name as project_name, p.type
               FROM stages s
               JOIN projects p ON p.id = s.project_id
               JOIN users m ON m.id = p.master_id
               WHERE p.type = 'module'
                 AND m.reports_to_production_id = ?
                 AND (SELECT COUNT(*) FROM reports WHERE stage_id = s.id) > 0
                 AND s.stage_confirmed_at IS NULL
               ORDER BY p.name, s.order_num""",
            (session["user_id"],),
        ).fetchall()]
    elif role == "director_construction":
        proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
        uid = session["user_id"]
        if "director_construction_id" in proj_cols:
            stages = [dict(r) for r in conn.execute(
                """SELECT s.id, s.name, s.stage_confirmed_at, p.id as project_id, p.name as project_name, p.type
                   FROM stages s
                   JOIN projects p ON p.id = s.project_id
                   WHERE p.type IN ('frame', 'gasblock', 'penopolistirol')
                     AND (p.director_construction_id = ?
                          OR EXISTS (
                            SELECT 1 FROM foreman_project_access fpa
                            JOIN users u ON u.id = fpa.foreman_id
                            WHERE fpa.project_id = p.id AND u.reports_to_construction_id = ?
                          ))
                     AND (SELECT COUNT(*) FROM reports WHERE stage_id = s.id) > 0
                     AND s.stage_confirmed_at IS NULL
                   ORDER BY p.name, s.order_num""",
                (uid, uid),
            ).fetchall()]
        else:
            stages = [dict(r) for r in conn.execute(
                """SELECT s.id, s.name, s.stage_confirmed_at, p.id as project_id, p.name as project_name, p.type
                   FROM stages s
                   JOIN projects p ON p.id = s.project_id
                   WHERE p.type IN ('frame', 'gasblock', 'penopolistirol')
                     AND EXISTS (
                       SELECT 1 FROM foreman_project_access fpa
                       JOIN users u ON u.id = fpa.foreman_id
                       WHERE fpa.project_id = p.id AND u.reports_to_construction_id = ?
                     )
                     AND (SELECT COUNT(*) FROM reports WHERE stage_id = s.id) > 0
                     AND s.stage_confirmed_at IS NULL
                   ORDER BY p.name, s.order_num""",
                (uid,),
            ).fetchall()]
    else:
        stages = []
    conn.close()
    return render_template("director/stage_confirmations.html", stages=stages, building_types=BUILDING_TYPES)


@app.route("/director/project/<int:project_id>/chat", methods=["GET", "POST"])
@login_required
def director_project_chat(project_id):
    """Чат проекта — директор по производству (модуль) или по строительству (каркас/газобетон/пенополистирол)"""
    role = session.get("role")
    if role not in ("director_production", "director_construction"):
        flash("Доступ запрещён.", "error")
        return redirect(url_for("index"))
    conn = get_db()
    project = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    if not project:
        conn.close()
        flash("Проект не найден.", "error")
        return redirect(url_for("director_production_dashboard" if role == "director_production" else "director_construction_dashboard"))
    project = dict(project)
    uid = session["user_id"]
    if role == "director_production":
        ok = conn.execute(
            """SELECT 1 FROM projects p
               JOIN users m ON m.id = p.master_id
               WHERE p.id = ? AND p.type = 'module' AND m.reports_to_production_id = ?""",
            (project_id, uid),
        ).fetchone()
    else:
        proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
        if "director_construction_id" in proj_cols:
            ok = conn.execute(
                """SELECT 1 FROM projects p
                   WHERE p.id = ? AND p.type IN ('frame', 'gasblock', 'penopolistirol')
                   AND (p.director_construction_id = ?
                        OR EXISTS (
                          SELECT 1 FROM foreman_project_access fpa
                          JOIN users u ON u.id = fpa.foreman_id
                          WHERE fpa.project_id = p.id AND u.reports_to_construction_id = ?
                        ))""",
                (project_id, uid, uid),
            ).fetchone()
        else:
            ok = conn.execute(
                """SELECT 1 FROM projects p
                   WHERE p.id = ? AND p.type IN ('frame', 'gasblock', 'penopolistirol')
                   AND EXISTS (
                     SELECT 1 FROM foreman_project_access fpa
                     JOIN users u ON u.id = fpa.foreman_id
                     WHERE fpa.project_id = p.id AND u.reports_to_construction_id = ?
                   )""",
                (project_id, uid),
            ).fetchone()
    if not ok:
        conn.close()
        flash("Доступ запрещён: проект не в вашем направлении.", "error")
        return redirect(url_for("director_production_dashboard" if role == "director_production" else "director_construction_dashboard"))

    if request.method == "POST":
        msg = request.form.get("message", "").strip()
        stage_id = request.form.get("stage_id", type=int) or None
        from_page = request.form.get("from_page")
        if msg:
            conn.execute(
                "INSERT INTO project_chat (project_id, user_id, message, stage_id) VALUES (?, ?, ?, ?)",
                (project_id, session["user_id"], msg, stage_id),
            )
            conn.commit()
            flash("Сообщение отправлено.", "success")
        conn.close()
        if from_page == "confirmations":
            return redirect(url_for("director_stage_confirmations"))
        return redirect(url_for("director_project_chat", project_id=project_id))

    stages = [dict(s) for s in conn.execute(
        "SELECT id, name FROM stages WHERE project_id = ? ORDER BY order_num", (project_id,)
    ).fetchall()]
    messages = [dict(row) for row in conn.execute(
        """SELECT pc.*, u.full_name as author_name, s.name as stage_name
           FROM project_chat pc
           JOIN users u ON pc.user_id = u.id
           LEFT JOIN stages s ON pc.stage_id = s.id
           WHERE pc.project_id = ?
           ORDER BY pc.created_at ASC""",
        (project_id,),
    ).fetchall()]
    conn.close()
    return render_template(
        "director/project_chat.html",
        project=project,
        messages=messages,
        stages=stages,
        building_types=BUILDING_TYPES,
    )


# ============== ПРОРАБ ==============


@app.route("/foreman")
@foreman_required
def foreman_dashboard():
    """Дашборд прораба: список назначенных объектов."""
    conn = get_db()
    projects = [dict(r) for r in conn.execute(
        """SELECT p.*, dc.full_name as director_name
           FROM foreman_project_access fpa
           JOIN projects p ON p.id = fpa.project_id
           LEFT JOIN users dc ON dc.id = (SELECT reports_to_construction_id FROM users WHERE id = ?)
           WHERE fpa.foreman_id = ?
           ORDER BY p.created_at DESC""",
        (session["user_id"], session["user_id"]),
    ).fetchall()]
    tokens = get_active_media_tokens_map("project_photo", [int(p["id"]) for p in projects])
    for p in projects:
        p["photo_token"] = tokens.get(int(p["id"]))
    conn.close()
    return render_template("foreman/dashboard.html", projects=projects)


@app.route("/foreman/project/<int:project_id>", methods=["GET", "POST"])
@foreman_required
def foreman_project(project_id):
    """Объект для прораба: закрытие рабочего дня (работы + %)."""
    conn = get_db()
    access = conn.execute(
        """SELECT 1 FROM foreman_project_access
           WHERE foreman_id = ? AND project_id = ?""",
        (session["user_id"], project_id),
    ).fetchone()
    if not access:
        conn.close()
        flash("Объект не найден или доступ запрещён.", "error")
        return redirect(url_for("foreman_dashboard"))

    project = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    if not project:
        conn.close()
        flash("Объект не найден.", "error")
        return redirect(url_for("foreman_dashboard"))
    project = dict(project)

    if request.method == "POST":
        action = request.form.get("action")
        if action == "close_day":
            report_date = (request.form.get("report_date", "") or "").strip()
            work_item_ids = request.form.getlist("work_item_id")
            percents = request.form.getlist("percent")
            comments = request.form.getlist("work_comment")

            rows = []
            for idx, wid in enumerate(work_item_ids):
                wid = (wid or "").strip()
                pct_raw = (percents[idx] if idx < len(percents) else "").strip()
                cmt = (comments[idx] if idx < len(comments) else "").strip()
                if not wid or not pct_raw:
                    continue
                try:
                    pct = float(pct_raw.replace(",", "."))
                except ValueError:
                    continue
                if pct not in ALLOWED_PERCENT_ENDS:
                    continue
                rows.append((int(wid), float(pct), cmt))

            if not report_date:
                flash("Укажите дату закрытия дня.", "error")
            elif not rows:
                flash("Выберите хотя бы одну работу и укажите % выполнения.", "error")
            else:
                existing = conn.execute(
                    "SELECT id FROM worker_daily_reports WHERE worker_id = ? AND project_id = ? AND report_date = ?",
                    (session["user_id"], project_id, report_date),
                ).fetchone()
                if existing:
                    daily_id = existing["id"]
                    conn.execute(
                        "DELETE FROM worker_daily_report_items WHERE daily_report_id = ?",
                        (daily_id,),
                    )
                else:
                    conn.execute(
                        "INSERT INTO worker_daily_reports (worker_id, project_id, report_date) VALUES (?, ?, ?)",
                        (session["user_id"], project_id, report_date),
                    )
                    daily_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

                # Проверяем остатки выполнения по проекту/работе (по всем прорабам)
                max_by = _get_max_percent_by_work_item(
                    conn,
                    project_id=project_id,
                    user_role="foreman",
                    exclude_daily_report_id=daily_id,
                )
                seen = set()
                invalid = []
                for wid, pct, _cmt in rows:
                    if wid in seen:
                        invalid.append(f"Работа №{wid} выбрана несколько раз.")
                        continue
                    seen.add(wid)
                    current_max = float(max_by.get(wid, 0) or 0)
                    if pct <= current_max:
                        invalid.append(f"Работа №{wid}: доступно только выше {int(current_max)}%.")
                if invalid:
                    conn.rollback()
                    flash("Нельзя закрыть день: " + " ".join(invalid), "error")
                    conn.close()
                    return redirect(url_for("foreman_project", project_id=project_id))

                for wid, pct, cmt in rows:
                    conn.execute(
                        """INSERT OR REPLACE INTO worker_daily_report_items
                           (daily_report_id, work_item_id, percent, comment, approved_status, approved_by_id, approved_at)
                           VALUES (?, ?, ?, ?, 'pending', NULL, NULL)""",
                        (daily_id, wid, pct, cmt),
                    )
                conn.commit()
                flash("Рабочий день закрыт.", "success")

        conn.close()
        return redirect(url_for("foreman_project", project_id=project_id))

    work_items = [dict(r) for r in conn.execute(
        "SELECT id, code, name FROM work_items WHERE active = 1 AND work_item_type = 'construction' ORDER BY name COLLATE NOCASE ASC, id ASC"
    ).fetchall()]
    max_by = _get_max_percent_by_work_item(conn, project_id=project_id, user_role="foreman")
    for w in work_items:
        w["max_percent"] = float(max_by.get(int(w["id"]), 0) or 0)

    daily_reports_raw = conn.execute(
        """SELECT id, report_date, created_at
           FROM worker_daily_reports
           WHERE worker_id = ? AND project_id = ?
           ORDER BY report_date DESC
           LIMIT 10""",
        (session["user_id"], project_id),
    ).fetchall()
    daily_reports = []
    for dr in daily_reports_raw:
        d = dict(dr)
        d["work_rows"] = [dict(r) for r in conn.execute(
            """SELECT w.id as work_item_id, w.code as work_code, w.name as work_name, i.percent, i.comment
               FROM worker_daily_report_items i
               JOIN work_items w ON w.id = i.work_item_id
               WHERE i.daily_report_id = ?
               ORDER BY w.name""",
            (d["id"],),
        ).fetchall()]
        for it in d["work_rows"]:
            it["percent_label"] = _percent_end_to_label(it.get("percent"))
        daily_reports.append(d)

    stages_raw = conn.execute(
        "SELECT id, name, order_num, planned_start_date, planned_end_date FROM stages WHERE project_id = ? ORDER BY order_num",
        (project_id,),
    ).fetchall()
    stages = [dict(r) for r in stages_raw]

    conn.close()
    return render_template(
        "foreman/project.html",
        project=project,
        work_items=work_items,
        daily_reports=daily_reports,
        stages=stages,
    )


@app.route("/foreman/project/<int:project_id>/chat", methods=["GET", "POST"])
@foreman_required
def foreman_project_chat(project_id):
    """Чат проекта — прораб (назначенные объекты)"""
    conn = get_db()
    access = conn.execute(
        "SELECT 1 FROM foreman_project_access WHERE foreman_id = ? AND project_id = ?",
        (session["user_id"], project_id),
    ).fetchone()
    if not access:
        conn.close()
        flash("Объект не найден или доступ запрещён.", "error")
        return redirect(url_for("foreman_dashboard"))
    project = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
    if not project:
        conn.close()
        flash("Объект не найден.", "error")
        return redirect(url_for("foreman_dashboard"))
    project = dict(project)

    if request.method == "POST":
        msg = request.form.get("message", "").strip()
        stage_id = request.form.get("stage_id", type=int) or None
        from_page = request.form.get("from_page")
        if msg:
            conn.execute(
                "INSERT INTO project_chat (project_id, user_id, message, stage_id) VALUES (?, ?, ?, ?)",
                (project_id, session["user_id"], msg, stage_id),
            )
            conn.commit()
            flash("Сообщение отправлено.", "success")
        conn.close()
        if from_page == "project":
            return redirect(url_for("foreman_project", project_id=project_id))
        return redirect(url_for("foreman_project_chat", project_id=project_id))

    stages = [dict(s) for s in conn.execute(
        "SELECT id, name FROM stages WHERE project_id = ? ORDER BY order_num", (project_id,)
    ).fetchall()]
    messages = [dict(row) for row in conn.execute(
        """SELECT pc.*, u.full_name as author_name, s.name as stage_name
           FROM project_chat pc
           JOIN users u ON pc.user_id = u.id
           LEFT JOIN stages s ON pc.stage_id = s.id
           WHERE pc.project_id = ?
           ORDER BY pc.created_at ASC""",
        (project_id,),
    ).fetchall()]
    conn.close()
    return render_template(
        "foreman/project_chat.html",
        project=project,
        messages=messages,
        stages=stages,
        building_types=BUILDING_TYPES,
    )


# ============== МАСТЕР ==============


@app.route("/master/dashboard-pref", methods=["POST"])
@master_required
def master_dashboard_pref():
    """Сохранить настройку отображения дашборда мастера (без перезагрузки)"""
    display = request.form.get("projects_display") or (request.get_json(silent=True) or {}).get("projects_display")
    if display in ("1", "2", "3", "4"):
        set_user_pref(session["user_id"], "master_projects_display", display)
    return "", 204


@app.route("/master")
@master_required
def master_dashboard():
    """Дашборд мастера: список назначенных проектов"""
    display = request.args.get("projects_display", type=str)
    if display and display in ("1", "2", "3", "4"):
        set_user_pref(session["user_id"], "master_projects_display", display)
        return redirect(url_for("master_dashboard"))
    projects_display = get_user_pref(session["user_id"], "master_projects_display", "2")
    conn = get_db()
    projects = [dict(r) for r in conn.execute(
        """SELECT p.*, u.full_name as client_name
           FROM projects p
           LEFT JOIN users u ON p.client_id = u.id
           WHERE p.master_id = ?
           ORDER BY p.created_at DESC""",
        (session["user_id"],),
    ).fetchall()]
    tokens = get_active_media_tokens_map("project_photo", [int(p["id"]) for p in projects])
    for p in projects:
        p["photo_token"] = tokens.get(int(p["id"]))
    edit_counts = get_edit_requests_count_by_project()
    unread_counts = get_unread_chat_count_by_project_for_master(session["user_id"])
    for p in projects:
        p["edit_requests_count"] = edit_counts.get(p["id"], 0)
        p["unread_tasks_count"] = unread_counts.get(p["id"], 0)
    conn.close()
    return render_template("master/dashboard.html", projects=projects, building_types=BUILDING_TYPES, projects_display=projects_display)


@app.route("/master/project/<int:project_id>", methods=["GET", "POST"])
@master_required
def master_project(project_id):
    """Страница объекта для мастера"""
    conn = get_db()
    project = conn.execute(
        "SELECT * FROM projects WHERE id = ? AND master_id = ?",
        (project_id, session["user_id"]),
    ).fetchone()
    if not project:
        conn.close()
        flash("Объект не найден или доступ запрещён.", "error")
        return redirect(url_for("master_dashboard"))

    project = dict(project)
    project["photo_token"] = get_active_media_tokens_map("project_photo", [int(project_id)]).get(int(project_id))

    # Обработка действий мастера
    if request.method == "POST":
        action = request.form.get("action")
        if action == "assign_worker":
            worker_id = request.form.get("worker_id", type=int)
            if worker_id:
                worker = conn.execute(
                    "SELECT id FROM users WHERE id = ? AND role = 'worker' AND reports_to_id = ?",
                    (worker_id, session["user_id"]),
                ).fetchone()
                if not worker:
                    flash("Работник не найден или не относится к вам.", "error")
                else:
                    try:
                        conn.execute(
                            """INSERT OR IGNORE INTO worker_project_access
                               (worker_id, project_id, assigned_by_id, assigned_by_role)
                               VALUES (?, ?, ?, ?)""",
                            (worker_id, project_id, session["user_id"], "master"),
                        )
                        conn.commit()
                        flash("Доступ работнику выдан.", "success")
                    except sqlite3.IntegrityError:
                        flash("Не удалось выдать доступ.", "error")
        elif action == "unassign_worker":
            worker_id = request.form.get("worker_id", type=int)
            if worker_id:
                conn.execute(
                    """DELETE FROM worker_project_access
                       WHERE worker_id = ? AND project_id = ?
                         AND (assigned_by_id IS NULL OR (assigned_by_id = ? AND assigned_by_role = 'master'))""",
                    (worker_id, project_id, session["user_id"]),
                )
                conn.commit()
                flash("Доступ работнику снят.", "info")
        if action == "add_report":
            stage_id = request.form.get("stage_id", type=int)
            comment = request.form.get("comment", "").strip()
            # Поддержка нескольких фото: getlist для multiple input
            photos = request.files.getlist("photo")
            if stage_id:
                stage = conn.execute(
                    "SELECT id FROM stages WHERE id = ? AND project_id = ?",
                    (stage_id, project_id),
                ).fetchone()
                if stage:
                    saved = 0
                    for photo in photos:
                        if photo and photo.filename and allowed_file(photo.filename):
                            # создаём запись отчёта в database.db (photo_path пустой — выдача через media.db)
                            conn.execute(
                                "INSERT INTO reports (stage_id, photo_path, comment) VALUES (?, ?, ?)",
                                (stage_id, "", comment),
                            )
                            report_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                            try:
                                rel_dir = _storage_rel_dir_for(
                                    "stage_report",
                                    project_id=int(project_id),
                                    stage_id=int(stage_id),
                                )
                                save_media_file(
                                    photo,
                                    project_id=int(project_id),
                                    stage_id=int(stage_id),
                                    entity_type="stage_report",
                                    entity_id=int(report_id),
                                    uploaded_by_id=session.get("user_id"),
                                    rel_dir=rel_dir,
                                )
                                saved += 1
                            except Exception:
                                conn.execute("DELETE FROM reports WHERE id = ?", (int(report_id),))
                    if saved == 0:
                        flash("Добавьте хотя бы одну фотографию (JPG или PNG).", "error")
                        conn.close()
                        return redirect(url_for("master_project", project_id=project_id))
                    conn.commit()
                    flash("Отчёт добавлен." if saved == 1 else f"Добавлено фото: {saved}.", "success")
                else:
                    flash("Этап не найден.", "error")

        elif action == "create_edit_request":
            stage_id = request.form.get("stage_id", type=int)
            comment = request.form.get("comment", "").strip()
            photos = request.files.getlist("photo_edit")
            if stage_id:
                stage = conn.execute(
                    "SELECT id FROM stages WHERE id = ? AND project_id = ?",
                    (stage_id, project_id),
                ).fetchone()
                if stage:
                    # Проверяем, нет ли уже pending заявки
                    existing = conn.execute(
                        "SELECT id FROM edit_requests WHERE stage_id = ? AND status = 'pending'",
                        (stage_id,),
                    ).fetchone()
                    if existing:
                        flash("Заявка на редактирование уже отправлена и ожидает рассмотрения.", "warning")
                    elif not photos or not any(p.filename for p in photos):
                        flash("Добавьте хотя бы одну новую фотографию.", "error")
                    else:
                        conn.execute(
                            "INSERT INTO edit_requests (stage_id, master_id, status) VALUES (?, ?, 'pending')",
                            (stage_id, session["user_id"]),
                        )
                        req_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                        saved = 0
                        for photo in photos:
                            if photo and photo.filename and allowed_file(photo.filename):
                                conn.execute(
                                    "INSERT INTO edit_request_photos (edit_request_id, photo_path, comment) VALUES (?, ?, ?)",
                                    (req_id, "", comment),
                                )
                                erp_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                                try:
                                    pid_row = conn.execute(
                                        "SELECT project_id FROM stages WHERE id = ?",
                                        (int(stage_id),),
                                    ).fetchone()
                                    pid = int(pid_row["project_id"]) if pid_row else int(project_id)
                                    rel_dir = _storage_rel_dir_for(
                                        "edit_request_photo",
                                        project_id=pid,
                                        stage_id=int(stage_id),
                                        edit_request_id=int(req_id),
                                    )
                                    save_media_file(
                                        photo,
                                        project_id=pid,
                                        stage_id=int(stage_id),
                                        entity_type="edit_request_photo",
                                        entity_id=int(erp_id),
                                        uploaded_by_id=session.get("user_id"),
                                        rel_dir=rel_dir,
                                    )
                                    saved += 1
                                except Exception:
                                    conn.execute("DELETE FROM edit_request_photos WHERE id = ?", (int(erp_id),))
                        # Комментарий мастера к заявке сохраняем в чат проекта (видно админу)
                        if comment:
                            project_id = conn.execute(
                                "SELECT project_id FROM stages WHERE id = ?", (stage_id,)
                            ).fetchone()[0]
                            conn.execute(
                                "INSERT INTO project_chat (project_id, user_id, message, msg_type, stage_id) VALUES (?, ?, ?, 'edit_request', ?)",
                                (project_id, session["user_id"], comment, stage_id),
                            )
                        conn.commit()
                        flash("Заявка на редактирование отправлена на рассмотрение администратору.", "success")
                else:
                    flash("Этап не найден.", "error")

        return redirect(url_for("master_project", project_id=project_id))

    stages_raw = conn.execute(
        "SELECT * FROM stages WHERE project_id = ? ORDER BY order_num", (project_id,)
    ).fetchall()
    stages = []
    for row in stages_raw:
        s = dict(row)
        reports_rows = conn.execute(
            "SELECT * FROM reports WHERE stage_id = ? ORDER BY created_at DESC",
            (s["id"],),
        ).fetchall()
        s["reports"] = [dict(r) for r in reports_rows]
        if s["reports"]:
            rep_tokens = get_active_media_tokens_map(
                "stage_report", [int(r["id"]) for r in s["reports"]]
            )
            for r in s["reports"]:
                r["photo_token"] = rep_tokens.get(int(r["id"]))
        s["reports_count"] = len(s["reports"])
        s["can_confirm_stage"] = (
            project["type"] == "module"
            and project["master_id"] == session["user_id"]
            and s["reports_count"] > 0
            and not s.get("stage_confirmed_at")
        )
        pending = conn.execute(
            "SELECT id FROM edit_requests WHERE stage_id = ? AND status = 'pending'",
            (s["id"],),
        ).fetchone()
        s["edit_request_pending"] = pending is not None
        stages.append(s)

    # Отметить чат как прочитанный (для уведомлений о новых ответах)
    mark_chat_read(session["user_id"], project_id)

    # Сообщения чата по этапам (корректировки)
    stage_chat = {}
    for s in stages:
        msgs = conn.execute(
            """SELECT pc.*, u.full_name as author_name
               FROM project_chat pc
               JOIN users u ON pc.user_id = u.id
               WHERE pc.project_id = ? AND pc.stage_id = ?
               ORDER BY pc.created_at ASC""",
            (project_id, s["id"]),
        ).fetchall()
        stage_chat[s["id"]] = [dict(m) for m in msgs]
    workers = [dict(r) for r in conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'worker' AND reports_to_id = ? ORDER BY full_name",
        (session["user_id"],),
    ).fetchall()]
    assigned_workers = [dict(r) for r in conn.execute(
        """SELECT u.id, u.full_name
           FROM worker_project_access wpa
           JOIN users u ON u.id = wpa.worker_id
           WHERE wpa.project_id = ? AND u.reports_to_id = ?
           ORDER BY u.full_name""",
        (project_id, session["user_id"]),
    ).fetchall()]
    conn.close()
    return render_template(
        "master/project.html",
        project=project,
        stages=stages,
        stage_chat=stage_chat,
        building_types=BUILDING_TYPES,
        workers=workers,
        assigned_workers=assigned_workers,
    )


@app.route("/stage/<int:stage_id>/confirm", methods=["POST"])
@login_required
def stage_confirm(stage_id):
    """Подтверждение этапа: мастер/дир.производства (модуль), дир.строительства (каркас/газобетон/пенополистирол)"""
    conn = get_db()
    stage = conn.execute(
        "SELECT s.*, p.type, p.master_id FROM stages s JOIN projects p ON p.id = s.project_id WHERE s.id = ?",
        (stage_id,),
    ).fetchone()
    if not stage:
        conn.close()
        flash("Этап не найден.", "error")
        return redirect(url_for("index"))
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(stages)")
    stage_cols = [r[1] for r in cursor.fetchall()]
    if "stage_confirmed_by_id" not in stage_cols or stage.get("stage_confirmed_at"):
        conn.close()
        return redirect(request.referrer or url_for("index"))
    role = session.get("role")
    can_confirm = False
    if stage["type"] == "module":
        if role == "master" and stage["master_id"] == session["user_id"]:
            can_confirm = True
        elif role == "director_production":
            m = conn.execute(
                "SELECT reports_to_production_id FROM users WHERE id = ?",
                (stage["master_id"],),
            ).fetchone()
            if m and m["reports_to_production_id"] == session["user_id"]:
                can_confirm = True
    elif stage["type"] in ("frame", "gasblock", "penopolistirol") and role == "director_construction":
        proj_cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
        if "director_construction_id" in proj_cols:
            proj = conn.execute(
                "SELECT director_construction_id FROM projects WHERE id = ?",
                (stage["project_id"],),
            ).fetchone()
            if proj and proj.get("director_construction_id") == session["user_id"]:
                can_confirm = True
        if not can_confirm:
            has_foreman = conn.execute(
                """SELECT 1 FROM foreman_project_access fpa
                   JOIN users u ON u.id = fpa.foreman_id
                   WHERE fpa.project_id = ? AND u.reports_to_construction_id = ?""",
                (stage["project_id"], session["user_id"]),
            ).fetchone()
            if has_foreman:
                can_confirm = True
    if can_confirm:
        reports_count = conn.execute("SELECT COUNT(*) FROM reports WHERE stage_id = ?", (stage_id,)).fetchone()[0]
        if reports_count > 0:
            conn.execute(
                "UPDATE stages SET stage_confirmed_by_id = ?, stage_confirmed_at = CURRENT_TIMESTAMP WHERE id = ?",
                (session["user_id"], stage_id),
            )
            conn.commit()
            flash("Этап подтверждён.", "success")
    conn.close()
    return redirect(request.referrer or url_for("index"))


@app.route("/master/project/<int:project_id>/chat", methods=["GET", "POST"])
@master_required
def master_project_chat(project_id):
    """Чат проекта — только админ и мастер, клиент не имеет доступа"""
    conn = get_db()
    project = conn.execute(
        "SELECT * FROM projects WHERE id = ? AND master_id = ?",
        (project_id, session["user_id"]),
    ).fetchone()
    if not project:
        conn.close()
        flash("Проект не найден или доступ запрещён.", "error")
        return redirect(url_for("master_dashboard"))

    if request.method == "POST":
        msg = request.form.get("message", "").strip()
        stage_id = request.form.get("stage_id", type=int) or None
        from_page = request.form.get("from_page")
        if msg:
            conn.execute(
                "INSERT INTO project_chat (project_id, user_id, message, stage_id) VALUES (?, ?, ?, ?)",
                (project_id, session["user_id"], msg, stage_id),
            )
            conn.commit()
            flash("Сообщение отправлено.", "success")
        conn.close()
        if from_page == "project":
            return redirect(url_for("master_project", project_id=project_id))
        return redirect(url_for("master_project_chat", project_id=project_id))

    mark_chat_read(session["user_id"], project_id)

    stages = [dict(s) for s in conn.execute(
        "SELECT id, name FROM stages WHERE project_id = ? ORDER BY order_num", (project_id,)
    ).fetchall()]
    messages = [dict(row) for row in conn.execute(
        """SELECT pc.*, u.full_name as author_name, s.name as stage_name
           FROM project_chat pc
           JOIN users u ON pc.user_id = u.id
           LEFT JOIN stages s ON pc.stage_id = s.id
           WHERE pc.project_id = ?
           ORDER BY pc.created_at ASC""",
        (project_id,),
    ).fetchall()]
    conn.close()
    return render_template(
        "master/project_chat.html",
        project=project,
        messages=messages,
        stages=stages,
        building_types=BUILDING_TYPES,
    )


# ============== РАБОЧИЙ ==============


@app.route("/worker")
@worker_required
def worker_dashboard():
    """Дашборд рабочего: объекты мастера, перед которым отчитывается"""
    conn = get_db()
    row = conn.execute(
        "SELECT reports_to_id FROM users WHERE id = ?",
        (session["user_id"],),
    ).fetchone()
    reports_to_id = row["reports_to_id"] if row and row["reports_to_id"] else None
    if not reports_to_id:
        conn.close()
        return render_template("worker/dashboard.html", projects=[], no_master=True)
    projects = [dict(r) for r in conn.execute(
        """SELECT p.*, u.full_name as master_name
           FROM worker_project_access wpa
           JOIN projects p ON p.id = wpa.project_id
           LEFT JOIN users u ON p.master_id = u.id
           WHERE wpa.worker_id = ?
           ORDER BY p.created_at DESC""",
        (session["user_id"],),
    ).fetchall()]
    tokens = get_active_media_tokens_map("project_photo", [int(p["id"]) for p in projects])
    for p in projects:
        p["photo_token"] = tokens.get(int(p["id"]))
    conn.close()
    return render_template("worker/dashboard.html", projects=projects, no_master=False)


@app.route("/worker/project/<int:project_id>", methods=["GET", "POST"])
@worker_required
def worker_project(project_id):
    """Объект для рабочего: отчёт по этапам перед мастером"""
    conn = get_db()
    row = conn.execute(
        "SELECT reports_to_id FROM users WHERE id = ?",
        (session["user_id"],),
    ).fetchone()
    reports_to_id = row["reports_to_id"] if row and row["reports_to_id"] else None
    if not reports_to_id:
        conn.close()
        flash("Вам не назначен мастер.", "error")
        return redirect(url_for("worker_dashboard"))
    project = conn.execute(
        """SELECT p.*
           FROM worker_project_access wpa
           JOIN projects p ON p.id = wpa.project_id
           WHERE wpa.worker_id = ? AND p.id = ?""",
        (session["user_id"], project_id),
    ).fetchone()
    if not project:
        conn.close()
        flash("Объект не найден или доступ запрещён.", "error")
        return redirect(url_for("worker_dashboard"))
    project = dict(project)

    if request.method == "POST":
        action = request.form.get("action")
        if action == "add_report":
            stage_id = request.form.get("stage_id", type=int)
            comment = request.form.get("comment", "").strip()
            photos = request.files.getlist("photo")
            if stage_id:
                stage = conn.execute(
                    "SELECT id FROM stages WHERE id = ? AND project_id = ?",
                    (stage_id, project_id),
                ).fetchone()
                if stage:
                    saved = 0
                    for photo in photos:
                        if photo and photo.filename and allowed_file(photo.filename):
                            conn.execute(
                                "INSERT INTO reports (stage_id, photo_path, comment) VALUES (?, ?, ?)",
                                (stage_id, "", comment),
                            )
                            report_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                            try:
                                rel_dir = _storage_rel_dir_for(
                                    "stage_report",
                                    project_id=int(project_id),
                                    stage_id=int(stage_id),
                                )
                                save_media_file(
                                    photo,
                                    project_id=int(project_id),
                                    stage_id=int(stage_id),
                                    entity_type="stage_report",
                                    entity_id=int(report_id),
                                    uploaded_by_id=session.get("user_id"),
                                    rel_dir=rel_dir,
                                )
                                saved += 1
                            except Exception:
                                conn.execute("DELETE FROM reports WHERE id = ?", (int(report_id),))
                    if saved > 0:
                        conn.commit()
                        flash("Отчёт добавлен." if saved == 1 else f"Добавлено фото: {saved}.", "success")
                    else:
                        flash("Добавьте хотя бы одну фотографию (JPG или PNG).", "error")
        elif action == "close_day":
            report_date = (request.form.get("report_date", "") or "").strip()
            work_item_ids = request.form.getlist("work_item_id")
            percents = request.form.getlist("percent")
            comments = request.form.getlist("work_comment")

            rows = []
            for idx, wid in enumerate(work_item_ids):
                wid = (wid or "").strip()
                pct_raw = (percents[idx] if idx < len(percents) else "").strip()
                cmt = (comments[idx] if idx < len(comments) else "").strip()
                if not wid or not pct_raw:
                    continue
                try:
                    pct = float(pct_raw.replace(",", "."))
                except ValueError:
                    continue
                if pct not in ALLOWED_PERCENT_ENDS:
                    continue
                rows.append((int(wid), float(pct), cmt))

            if not report_date:
                flash("Укажите дату закрытия дня.", "error")
            elif not rows:
                flash("Выберите хотя бы одну работу и укажите % выполнения.", "error")
            else:
                existing = conn.execute(
                    "SELECT id FROM worker_daily_reports WHERE worker_id = ? AND project_id = ? AND report_date = ?",
                    (session["user_id"], project_id, report_date),
                ).fetchone()
                if existing:
                    daily_id = existing["id"]
                    conn.execute(
                        "DELETE FROM worker_daily_report_items WHERE daily_report_id = ?",
                        (daily_id,),
                    )
                else:
                    conn.execute(
                        "INSERT INTO worker_daily_reports (worker_id, project_id, report_date) VALUES (?, ?, ?)",
                        (session["user_id"], project_id, report_date),
                    )
                    daily_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

                # Проверяем остатки выполнения по проекту/работе (по всем рабочим)
                max_by = _get_max_percent_by_work_item(
                    conn,
                    project_id=project_id,
                    user_role="worker",
                    exclude_daily_report_id=daily_id,
                )
                # коды работ для сообщений
                code_by_id = {}
                if rows:
                    ids = [wid for wid, _pct, _cmt in rows]
                    q = "SELECT id, code FROM work_items WHERE id IN ({})".format(",".join(["?"] * len(ids)))
                    for r in conn.execute(q, tuple(ids)).fetchall():
                        code_by_id[int(r["id"])] = r["code"] or r["id"]
                seen = set()
                invalid = []
                for wid, pct, _cmt in rows:
                    if wid in seen:
                        code = code_by_id.get(wid, wid)
                        invalid.append(f"Работа №{code} выбрана несколько раз.")
                        continue
                    seen.add(wid)
                    current_max = float(max_by.get(wid, 0) or 0)
                    if pct <= current_max:
                        code = code_by_id.get(wid, wid)
                        invalid.append(
                            f"Работа №{code}: доступно только выше {int(current_max)}%."
                        )
                if invalid:
                    conn.rollback()
                    flash("Нельзя закрыть день: " + " ".join(invalid), "error")
                    conn.close()
                    return redirect(url_for("worker_project", project_id=project_id))

                for wid, pct, cmt in rows:
                    conn.execute(
                        """INSERT OR REPLACE INTO worker_daily_report_items
                           (daily_report_id, work_item_id, percent, comment, approved_status, approved_by_id, approved_at)
                           VALUES (?, ?, ?, ?, 'pending', NULL, NULL)""",
                        (daily_id, wid, pct, cmt),
                    )
                conn.commit()
                flash("Рабочий день закрыт.", "success")

        conn.close()
        return redirect(url_for("worker_project", project_id=project_id))

    stages_raw = conn.execute(
        """SELECT s.*,
           (SELECT COUNT(*) FROM reports WHERE stage_id = s.id) as reports_count
           FROM stages s WHERE s.project_id = ? ORDER BY s.order_num""",
        (project_id,),
    ).fetchall()
    stages = []
    for row in stages_raw:
        s = dict(row)
        s["reports"] = [dict(r) for r in conn.execute(
            "SELECT * FROM reports WHERE stage_id = ? ORDER BY created_at DESC",
            (s["id"],),
        ).fetchall()]
        stages.append(s)

    work_items = [dict(r) for r in conn.execute(
        "SELECT id, code, name FROM work_items WHERE active = 1 AND work_item_type = 'production' ORDER BY name COLLATE NOCASE ASC, id ASC"
    ).fetchall()]
    # текущий максимальный прогресс по работам этого проекта (все работники)
    max_by = _get_max_percent_by_work_item(conn, project_id=project_id, user_role="worker")
    for w in work_items:
        w["max_percent"] = float(max_by.get(int(w["id"]), 0) or 0)

    daily_reports_raw = conn.execute(
        """SELECT id, report_date, created_at
           FROM worker_daily_reports
           WHERE worker_id = ? AND project_id = ?
           ORDER BY report_date DESC
           LIMIT 10""",
        (session["user_id"], project_id),
    ).fetchall()
    daily_reports = []
    for dr in daily_reports_raw:
        d = dict(dr)
        d["work_rows"] = [dict(r) for r in conn.execute(
            """SELECT w.id as work_item_id, w.name as work_name, i.percent, i.comment
               FROM worker_daily_report_items i
               JOIN work_items w ON w.id = i.work_item_id
               WHERE i.daily_report_id = ?
               ORDER BY w.name""",
            (d["id"],),
        ).fetchall()]
        for it in d["work_rows"]:
            it["percent_label"] = _percent_end_to_label(it.get("percent"))
        daily_reports.append(d)

    conn.close()
    return render_template(
        "worker/project.html",
        project=project,
        stages=stages,
        work_items=work_items,
        daily_reports=daily_reports,
    )


# ============== КЛИЕНТ ==============


@app.route("/client/dashboard-pref", methods=["POST"])
@client_required
def client_dashboard_pref():
    """Сохранить настройку отображения дашборда клиента (без перезагрузки)"""
    display = request.form.get("projects_display") or (request.get_json(silent=True) or {}).get("projects_display")
    if display in ("1", "2", "3", "4"):
        set_user_pref(session["user_id"], "client_projects_display", display)
    return "", 204


@app.route("/client")
@client_required
def client_dashboard():
    """Дашборд клиента: список его проектов"""
    display = request.args.get("projects_display", type=str)
    if display and display in ("1", "2", "3", "4"):
        set_user_pref(session["user_id"], "client_projects_display", display)
        return redirect(url_for("client_dashboard"))
    projects_display = get_user_pref(session["user_id"], "client_projects_display", "4")
    conn = get_db()
    projects = [dict(r) for r in conn.execute(
        """SELECT p.*, u.full_name as client_name
           FROM projects p
           LEFT JOIN users u ON p.client_id = u.id
           WHERE p.client_id = ?
           ORDER BY p.created_at DESC""",
        (session["user_id"],),
    ).fetchall()]
    tokens = get_active_media_tokens_map("project_photo", [int(p["id"]) for p in projects])
    for p in projects:
        p["photo_token"] = tokens.get(int(p["id"]))
    conn.close()
    return render_template("client/dashboard.html", projects=projects, building_types=BUILDING_TYPES, projects_display=projects_display)


@app.route("/client/project/<int:project_id>", methods=["GET", "POST"])
@client_required
def client_project(project_id):
    """Страница объекта для клиента: просмотр и подтверждение договора/сметы"""
    conn = get_db()
    project = conn.execute(
        "SELECT * FROM projects WHERE id = ? AND client_id = ?",
        (project_id, session["user_id"]),
    ).fetchone()
    if not project:
        conn.close()
        flash("Объект не найден или доступ запрещён.", "error")
        return redirect(url_for("client_dashboard"))

    project = dict(project)

    if request.method == "POST":
        action = request.form.get("action")
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(projects)")
        cols = [r[1] for r in cursor.fetchall()]
        if action == "confirm_contract" and "contract_confirmed_at" in cols:
            if project.get("contract_admin_approved_at"):
                conn.execute(
                    "UPDATE projects SET contract_confirmed_at = CURRENT_TIMESTAMP WHERE id = ? AND client_id = ?",
                    (project_id, session["user_id"]),
                )
                conn.commit()
                flash("Договор подтверждён.", "success")
            else:
                flash("Договор ещё не одобрен администратором.", "warning")
        elif action == "confirm_estimate" and "estimate_confirmed_at" in cols:
            if project.get("estimate_admin_approved_at"):
                conn.execute(
                    "UPDATE projects SET estimate_confirmed_at = CURRENT_TIMESTAMP WHERE id = ? AND client_id = ?",
                    (project_id, session["user_id"]),
                )
                conn.commit()
                flash("Смета подтверждена.", "success")
            else:
                flash("Смета ещё не одобрена администратором.", "warning")
        conn.close()
        return redirect(url_for("client_project", project_id=project_id))

    project["photo_token"] = get_active_media_tokens_map("project_photo", [int(project_id)]).get(int(project_id))
    project["contract_token"] = get_active_media_tokens_map("project_contract", [int(project_id)]).get(int(project_id))
    project["estimate_token"] = get_active_media_tokens_map("project_estimate", [int(project_id)]).get(int(project_id))

    stages_raw = conn.execute(
        "SELECT * FROM stages WHERE project_id = ? ORDER BY order_num", (project_id,)
    ).fetchall()
    stages = []
    completed = 0
    for row in stages_raw:
        s = dict(row)
        reports_rows = conn.execute(
            "SELECT * FROM reports WHERE stage_id = ? ORDER BY created_at DESC",
            (s["id"],),
        ).fetchall()
        s["reports"] = [dict(r) for r in reports_rows]
        if s["reports"]:
            rep_tokens = get_active_media_tokens_map(
                "stage_report", [int(r["id"]) for r in s["reports"]]
            )
            for r in s["reports"]:
                r["photo_token"] = rep_tokens.get(int(r["id"]))
        if s["reports"]:
            completed += 1
        stages.append(s)
    total = len(stages)
    conn.close()
    progress_pct = int((completed / total * 100)) if total else 0
    return render_template(
        "client/project.html",
        project=project,
        stages=stages,
        building_types=BUILDING_TYPES,
        total=total,
        completed=completed,
        progress_pct=progress_pct,
    )


def _detect_local_ipv4() -> list[str]:
    """Возвращает локальные IPv4 адреса (без loopback)."""
    ips: set[str] = set()
    try:
        _, _, host_ips = socket.gethostbyname_ex(socket.gethostname())
        for ip in host_ips:
            if ip and not ip.startswith("127."):
                ips.add(ip)
    except Exception:
        pass
    try:
        # Получаем "основной" адрес интерфейса без реального сетевого запроса.
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            if ip and not ip.startswith("127."):
                ips.add(ip)
    except Exception:
        pass
    return sorted(ips)


def _print_startup_urls(host: str, port: int) -> None:
    """Печатает удобные URL для входа с этого и других устройств."""
    print("\n=== StroyControl ===")
    print(f"Локально: http://127.0.0.1:{port}")
    if host == "0.0.0.0":
        ips = _detect_local_ipv4()
        if ips:
            for ip in ips:
                print(f"В сети:  http://{ip}:{port}")
        else:
            print("В сети:  IP не определён (проверьте подключение к Wi-Fi/LAN).")
    else:
        print("Для доступа с других устройств запустите на host=0.0.0.0")
    print("====================\n")


if __name__ == "__main__":
    init_media_db()
    init_db()
    port = int(os.environ.get("PORT", 5002))
    host = os.environ.get("HOST", "0.0.0.0")
    debug_mode = True
    # При debug+reloader печатаем только в рабочем процессе.
    if (not debug_mode) or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        _print_startup_urls(host, port)
    app.run(host=host, port=port, debug=debug_mode)
