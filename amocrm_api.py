"""
amoCRM API v4 — модуль для работы с API сделок.
Использует долгосрочный токен (Bearer).
"""

import os
from typing import Any

import requests

# Базовый URL API — используется subdomain из настроек
AMOCRM_API_VERSION = "v4"


def get_amocrm_config(conn=None) -> dict | None:
    """
    Получить настройки amoCRM из БД.
    Возвращает dict с ключами: subdomain, access_token.
    Если conn не передан — создаётся временное соединение.
    """
    if conn is None:
        from app import get_db
        conn = get_db()
        own_conn = True
    else:
        own_conn = False

    try:
        rows = conn.execute(
            "SELECT key, value FROM amocrm_settings WHERE key IN ('subdomain', 'access_token')"
        ).fetchall()
        cfg = {r["key"]: r["value"] for r in rows if r["value"]}
        if own_conn:
            conn.close()

        # Приоритет: env vars > БД (для production)
        subdomain = os.environ.get("AMOCRM_SUBDOMAIN") or cfg.get("subdomain")
        token = os.environ.get("AMOCRM_ACCESS_TOKEN") or cfg.get("access_token")

        if subdomain and token:
            return {"subdomain": subdomain.strip(), "access_token": token.strip()}
        return None
    except Exception:
        if own_conn and conn:
            conn.close()
        return None


def amocrm_request(
    method: str,
    path: str,
    config: dict | None = None,
    params: dict | None = None,
    conn=None,
) -> tuple[dict | None, int | None, str | None]:
    """
    Выполнить запрос к amoCRM API v4.
    path — путь без ведущего слэша, например "leads".
    Возвращает (data, status_code, error_message).
    """
    cfg = config or get_amocrm_config(conn)
    if not cfg:
        return None, None, "amoCRM не настроен: укажите subdomain и access_token"

    subdomain = cfg["subdomain"]
    token = cfg["access_token"]
    base_url = f"https://{subdomain}.amocrm.ru/api/{AMOCRM_API_VERSION}"

    url = f"{base_url}/{path.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        if method.upper() == "GET":
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        elif method.upper() == "POST":
            resp = requests.post(url, headers=headers, json=params or {}, timeout=30)
        else:
            return None, None, f"Метод {method} не поддерживается"

        try:
            data = resp.json() if resp.text else {}
        except Exception:
            data = {"_raw": resp.text}

        if resp.status_code >= 400:
            detail = data.get("detail", data.get("title", resp.text))
            if isinstance(detail, dict):
                detail = str(detail)
            return data, resp.status_code, detail or resp.reason

        return data, resp.status_code, None
    except requests.RequestException as e:
        return None, None, str(e)


def amocrm_get_account(conn=None) -> tuple[dict | None, str | None]:
    """Получить данные аккаунта (проверка подключения)."""
    data, status, err = amocrm_request("GET", "account", conn=conn)
    if err:
        return None, err
    return data, None


def amocrm_get_leads(
    page: int = 1,
    limit: int = 50,
    order: dict | None = None,
    conn=None,
) -> tuple[list[dict] | None, dict | None, str | None]:
    """
    Получить список сделок.
    Возвращает (leads_list, _embedded_info, error_message).
    """
    params = {"page": page, "limit": min(limit, 250)}
    if order:
        for k, v in order.items():
            params[f"order[{k}]"] = v

    data, status, err = amocrm_request("GET", "leads", params=params, conn=conn)
    if err:
        return None, None, err

    embedded = data.get("_embedded", {})
    leads = embedded.get("leads", [])
    return leads, data, None


def amocrm_get_pipelines(conn=None) -> tuple[list[dict] | None, str | None]:
    """
    Получить воронки и этапы (pipelines + statuses).
    Возвращает список: [{"id", "name", "statuses": [{"id","name","color","sort_order"}]}, ...]
    """
    data, status, err = amocrm_request("GET", "leads/pipelines", conn=conn)
    if err:
        return None, err
    pipelines = data.get("_embedded", {}).get("pipelines", [])
    result = []
    for p in pipelines:
        statuses = p.get("_embedded", {}).get("statuses", [])
        result.append({
            "id": p.get("id"),
            "name": p.get("name", "—"),
            "statuses": [
                {"id": s.get("id"), "name": s.get("name", "—"), "color": s.get("color"), "sort_order": s.get("sort_order")}
                for s in statuses
            ],
        })
    return result, None


def amocrm_get_users(conn=None) -> tuple[list[dict] | None, str | None]:
    """Получить список пользователей (для responsible_user_id)."""
    data, status, err = amocrm_request("GET", "users", conn=conn)
    if err:
        return None, err
    users = data.get("_embedded", {}).get("users", [])
    return [{"id": u.get("id"), "name": u.get("name", "—")} for u in users], None


def amocrm_get_all_leads(conn=None, limit_per_page: int = 250) -> tuple[list[dict] | None, str | None]:
    """
    Получить все сделки (с пагинацией).
    Возвращает (leads_list, error_message).
    """
    all_leads = []
    page = 1
    while True:
        leads, _, err = amocrm_get_leads(
            page=page, limit=limit_per_page, order={"created_at": "desc"}, conn=conn
        )
        if err:
            return None, err
        if not leads:
            break
        all_leads.extend(leads)
        if len(leads) < limit_per_page:
            break
        page += 1
    return all_leads, None


def amocrm_test_connection(conn=None) -> tuple[bool, str]:
    """
    Проверить подключение к amoCRM.
    Возвращает (success, message).
    """
    acc, err = amocrm_get_account(conn)
    if err:
        return False, err
    if not acc:
        return False, "Нет ответа от API"
    name = acc.get("name", "—")
    return True, f"Подключено: {name}"
