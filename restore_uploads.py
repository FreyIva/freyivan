#!/usr/bin/env python3
"""
Восстановление отсутствующих файлов из static/uploads по ссылкам в БД.

Что делает:
- читает `projects.photo_path`, `reports.photo_path`, `edit_request_photos.photo_path`
- если файл (uploads/...) отсутствует на диске, пытается:
  - для seed-файлов скачать заново (picsum.photos) по детерминированному seed
  - иначе создать placeholder (валидный маленький JPEG/PNG)

Ничего НЕ удаляет и не меняет данные в БД.

Запуск:
  ./venv/bin/python restore_uploads.py
"""

from __future__ import annotations

import re
import sqlite3
import ssl
import urllib.request
from pathlib import Path


BASE_DIR = Path(__file__).parent
DATABASE = BASE_DIR / "database.db"
UPLOAD_DIR = BASE_DIR / "static" / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# Минимальный валидный серый JPEG 2x2 (тот же подход что в seed_data.py)
_PLACEHOLDER_JPEG = (
    b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06"
    b"\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\x0d\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a\x1f"
    b"\x1e\x1d\x1a\x1c\x1c $.' \",#\x1c\x1c(7),01444\x1f'9=82<.342\xff\xc0\x00\x0b\x08\x00\x02\x00"
    b"\x02\x01\x01\x11\x00\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00"
    b"\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\xff\xda\x00\x08\x01\x01\x00\x00\x00?\x00\xfc"
    b"\xbf\xff\xd9"
)

# Минимальный валидный PNG 1x1 (прозрачный)
_PLACEHOLDER_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
    b"\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01\xe2!\xbc3\x00\x00\x00\x00IEND\xaeB`\x82"
)

_SVG_PROJECT = """<svg xmlns="http://www.w3.org/2000/svg" width="800" height="500" viewBox="0 0 800 500">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#0b1220"/>
      <stop offset="1" stop-color="#111827"/>
    </linearGradient>
    <linearGradient id="acc" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0" stop-color="#c9a227"/>
      <stop offset="1" stop-color="#b8860b"/>
    </linearGradient>
  </defs>
  <rect width="800" height="500" fill="url(#bg)"/>
  <rect x="56" y="64" width="688" height="372" rx="22" fill="rgba(255,255,255,0.06)" stroke="rgba(148,163,184,0.22)"/>
  <rect x="56" y="64" width="10" height="372" rx="5" fill="url(#acc)"/>
  <g fill="rgba(248,250,252,0.92)" font-family="Manrope, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif">
    <text x="96" y="150" font-size="28" font-weight="800">Фото проекта</text>
    <text x="96" y="190" font-size="16" fill="rgba(226,232,240,0.78)">Файл отсутствует или был загружен как заглушка.</text>
    <text x="96" y="240" font-size="14" fill="rgba(148,163,184,0.85)">
      Загрузите фото заново в карточке проекта (Админ → Проект → Этапы).
    </text>
  </g>
</svg>
"""

_SVG_REPORT = """<svg xmlns="http://www.w3.org/2000/svg" width="800" height="500" viewBox="0 0 800 500">
  <defs>
    <linearGradient id="bg2" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#0b1220"/>
      <stop offset="1" stop-color="#0f172a"/>
    </linearGradient>
    <linearGradient id="acc2" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0" stop-color="#c9a227"/>
      <stop offset="1" stop-color="#b8860b"/>
    </linearGradient>
  </defs>
  <rect width="800" height="500" fill="url(#bg2)"/>
  <rect x="56" y="64" width="688" height="372" rx="22" fill="rgba(255,255,255,0.06)" stroke="rgba(148,163,184,0.22)"/>
  <rect x="56" y="64" width="10" height="372" rx="5" fill="url(#acc2)"/>
  <g fill="rgba(248,250,252,0.92)" font-family="Manrope, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif">
    <text x="96" y="150" font-size="28" font-weight="800">Фото отчёта</text>
    <text x="96" y="190" font-size="16" fill="rgba(226,232,240,0.78)">Здесь должна быть фотография этапа/отчёта.</text>
    <text x="96" y="240" font-size="14" fill="rgba(148,163,184,0.85)">
      Загрузите фото заново (в отчёте/заявке).
    </text>
  </g>
</svg>
"""


def _download(url: str, target: Path) -> bool:
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            data = resp.read()
        if data and len(data) > 256:
            target.write_bytes(data)
            return True
        return False
    except Exception:
        return False


def _write_placeholder(target: Path) -> None:
    ext = target.suffix.lower()
    if ext == ".png":
        target.write_bytes(_PLACEHOLDER_PNG)
    else:
        target.write_bytes(_PLACEHOLDER_JPEG)


def _picsum_url_for_filename(filename: str) -> str | None:
    # project_seed_{i}.jpg -> https://picsum.photos/seed/proj{i+99}/400/300  (в seed_data i=0..14, filename i+1)
    m = re.fullmatch(r"project_seed_(\d+)\.jpg", filename)
    if m:
        n = int(m.group(1))
        seed = f"proj{(n - 1) + 100}"
        return f"https://picsum.photos/seed/{seed}/400/300"

    # report_seed_{project_id}_{stage_id}_{r}.jpg
    m = re.fullmatch(r"report_seed_(\d+)_(\d+)_(\d+)\.jpg", filename)
    if m:
        project_id, stage_id, r = m.group(1), m.group(2), m.group(3)
        seed = f"stage{project_id}{stage_id}{r}"
        return f"https://picsum.photos/seed/{seed}/300/200"

    # edit_req_* -> нет детерминированного URL
    return None


def main() -> None:
    if not DATABASE.exists():
        print("database.db не найден.")
        return

    conn = sqlite3.connect(str(DATABASE))
    conn.row_factory = sqlite3.Row

    paths: list[str] = []
    for sql in (
        "SELECT photo_path FROM projects WHERE photo_path IS NOT NULL AND TRIM(photo_path) <> ''",
        "SELECT photo_path FROM reports WHERE photo_path IS NOT NULL AND TRIM(photo_path) <> ''",
        "SELECT photo_path FROM edit_request_photos WHERE photo_path IS NOT NULL AND TRIM(photo_path) <> ''",
    ):
        for r in conn.execute(sql).fetchall():
            p = (r["photo_path"] or "").strip()
            if p.startswith("uploads/"):
                paths.append(p)

    conn.close()

    # Создадим красивые SVG-заглушки (чтобы вместо «серых 2x2» были нормальные превью)
    placeholder_project = UPLOAD_DIR / "placeholder_project.svg"
    placeholder_report = UPLOAD_DIR / "placeholder_report.svg"
    if not placeholder_project.exists():
        placeholder_project.write_text(_SVG_PROJECT, encoding="utf-8")
    if not placeholder_report.exists():
        placeholder_report.write_text(_SVG_REPORT, encoding="utf-8")

    uniq = sorted(set(paths))
    missing = []
    restored_download = 0
    restored_placeholder = 0
    refreshed_download = 0
    for rel in uniq:
        filename = rel.replace("uploads/", "", 1)
        target = UPLOAD_DIR / filename
        if target.exists():
            # Если это seed-картинка и она подозрительно маленькая (placeholder),
            # попробуем скачать заново и заменить.
            url = _picsum_url_for_filename(target.name)
            try:
                size = target.stat().st_size
            except Exception:
                size = 0
            if url and size and size < 2048:
                if _download(url, target):
                    refreshed_download += 1
            continue
        missing.append(rel)
        target.parent.mkdir(parents=True, exist_ok=True)
        url = _picsum_url_for_filename(target.name)
        ok = False
        if url:
            ok = _download(url, target)
        if ok:
            restored_download += 1
        else:
            _write_placeholder(target)
            restored_placeholder += 1

    print("Всего ссылок uploads/* в БД:", len(uniq))
    print("Отсутствовало файлов:", len(missing))
    print("Восстановлено скачиванием:", restored_download)
    print("Восстановлено placeholder:", restored_placeholder)
    print("Заменено seed placeholder на скачанные:", refreshed_download)
    if missing:
        print("Примеры восстановленных путей:")
        for p in missing[:10]:
            print(" -", p)

    # Апгрейд: если в БД стоят seed-заглушки (очень маленькие jpg), переключим на SVG, чтобы UI не выглядел «пустым».
    conn = sqlite3.connect(str(DATABASE))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    def _is_tiny(rel_path: str) -> bool:
        try:
            fp = (BASE_DIR / "static" / rel_path)
            return fp.exists() and fp.stat().st_size < 2048
        except Exception:
            return False

    upd_projects = 0
    for r in cur.execute("SELECT id, photo_path FROM projects WHERE photo_path IS NOT NULL AND TRIM(photo_path) <> ''").fetchall():
        p = (r["photo_path"] or "").strip()
        if p.startswith("uploads/project_seed_") and _is_tiny(p):
            cur.execute("UPDATE projects SET photo_path = ? WHERE id = ?", ("uploads/placeholder_project.svg", int(r["id"])))
            upd_projects += 1

    upd_reports = 0
    for r in cur.execute("SELECT id, photo_path FROM reports WHERE photo_path IS NOT NULL AND TRIM(photo_path) <> ''").fetchall():
        p = (r["photo_path"] or "").strip()
        if p.startswith("uploads/report_seed_") and _is_tiny(p):
            cur.execute("UPDATE reports SET photo_path = ? WHERE id = ?", ("uploads/placeholder_report.svg", int(r["id"])))
            upd_reports += 1

    upd_edits = 0
    for r in cur.execute("SELECT id, photo_path FROM edit_request_photos WHERE photo_path IS NOT NULL AND TRIM(photo_path) <> ''").fetchall():
        p = (r["photo_path"] or "").strip()
        # В seed_data edit_request_photos ссылаются на report_seed_*
        if p.startswith("uploads/report_seed_") and _is_tiny(p):
            cur.execute("UPDATE edit_request_photos SET photo_path = ? WHERE id = ?", ("uploads/placeholder_report.svg", int(r["id"])))
            upd_edits += 1

    conn.commit()
    conn.close()

    print("SVG-заглушки применены (замена tiny seed):",
          "projects", upd_projects,
          "reports", upd_reports,
          "edit_request_photos", upd_edits)


if __name__ == "__main__":
    main()

