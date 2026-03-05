import hashlib
import mimetypes
import secrets
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).parent
MAIN_DB = BASE_DIR / "database.db"
MEDIA_DB = BASE_DIR / "media.db"
UPLOADS_DIR = BASE_DIR / "static" / "uploads"
STORAGE_ROOT = BASE_DIR / "storage"


def init_media_db():
    STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(MEDIA_DB)
    try:
        cur = conn.cursor()
        cur.execute("""
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_media_files_project_id ON media_files(project_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_media_files_stage_id ON media_files(stage_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_media_files_entity ON media_files(entity_type, entity_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_media_files_archived ON media_files(is_archived)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_media_files_uploaded_at ON media_files(uploaded_at)")
        conn.commit()
    finally:
        conn.close()


def _posix_relpath(path: Path) -> str:
    return path.as_posix().lstrip("/")


def _storage_rel_dir_for(entity_type: str, *, project_id: int, stage_id: int | None, edit_request_id: int | None):
    if entity_type == "project_photo":
        return Path("projects") / str(int(project_id)) / "project"
    if entity_type == "stage_report":
        if not stage_id:
            raise ValueError("stage_id required")
        return Path("projects") / str(int(project_id)) / "stages" / str(int(stage_id)) / "reports"
    if entity_type == "edit_request_photo":
        if not stage_id or not edit_request_id:
            raise ValueError("stage_id and edit_request_id required")
        return (
            Path("projects")
            / str(int(project_id))
            / "stages"
            / str(int(stage_id))
            / "edit_requests"
            / str(int(edit_request_id))
        )
    raise ValueError(entity_type)


def _internal_media_filename(*, uploaded_by_id: int | None, original_filename: str) -> str:
    ext = ""
    if original_filename and "." in original_filename:
        ext = "." + original_filename.rsplit(".", 1)[1].lower()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    rand8 = secrets.token_hex(4)
    uid = int(uploaded_by_id) if uploaded_by_id else 0
    return f"u{uid}_{ts}_{rand8}{ext}"


def _sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _has_active_media(media_conn: sqlite3.Connection, *, entity_type: str, entity_id: int) -> bool:
    row = media_conn.execute(
        "SELECT 1 FROM media_files WHERE entity_type = ? AND entity_id = ? AND is_archived = 0 LIMIT 1",
        (entity_type, int(entity_id)),
    ).fetchone()
    return row is not None


def _insert_media_row(
    media_conn: sqlite3.Connection,
    *,
    project_id: int,
    stage_id: int | None,
    entity_type: str,
    entity_id: int,
    stored_relpath: str,
    original_filename: str | None,
    mime_type: str | None,
    size_bytes: int,
    sha256: str,
):
    for _ in range(10):
        token = secrets.token_urlsafe(32)
        try:
            media_conn.execute(
                """INSERT INTO media_files
                   (project_id, stage_id, entity_type, entity_id, stored_relpath,
                    original_filename, mime_type, size_bytes, sha256, uploaded_by_id, public_token)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)""",
                (
                    int(project_id),
                    int(stage_id) if stage_id else None,
                    entity_type,
                    int(entity_id),
                    stored_relpath,
                    original_filename,
                    mime_type,
                    int(size_bytes),
                    sha256,
                    token,
                ),
            )
            return token
        except sqlite3.IntegrityError:
            continue
    raise RuntimeError("Failed to generate unique token")


def _migrate_one(
    media_conn: sqlite3.Connection,
    *,
    src_path: Path,
    project_id: int,
    stage_id: int | None,
    entity_type: str,
    entity_id: int,
    rel_dir: Path,
):
    rel_dir = Path(rel_dir)
    dest_dir = STORAGE_ROOT / rel_dir
    dest_dir.mkdir(parents=True, exist_ok=True)

    internal_name = _internal_media_filename(uploaded_by_id=None, original_filename=src_path.name)
    dest_path = dest_dir / internal_name
    shutil.copy2(src_path, dest_path)

    sha256 = _sha256_of(dest_path)
    size_bytes = int(dest_path.stat().st_size)
    mime_type, _ = mimetypes.guess_type(dest_path.name)
    stored_relpath = _posix_relpath(rel_dir / internal_name)

    _insert_media_row(
        media_conn,
        project_id=project_id,
        stage_id=stage_id,
        entity_type=entity_type,
        entity_id=entity_id,
        stored_relpath=stored_relpath,
        original_filename=src_path.name,
        mime_type=mime_type or None,
        size_bytes=size_bytes,
        sha256=sha256,
    )


def migrate():
    if not MAIN_DB.exists():
        raise FileNotFoundError(f"Main DB not found: {MAIN_DB}")
    init_media_db()

    main = sqlite3.connect(MAIN_DB)
    main.row_factory = sqlite3.Row
    media = sqlite3.connect(MEDIA_DB)
    media.row_factory = sqlite3.Row
    try:
        migrated = {"project_photo": 0, "stage_report": 0, "edit_request_photo": 0}
        skipped = 0
        missing = 0

        # projects.photo_path
        for r in main.execute(
            "SELECT id, photo_path FROM projects WHERE photo_path IS NOT NULL AND TRIM(photo_path) <> ''"
        ).fetchall():
            pid = int(r["id"])
            pp = (r["photo_path"] or "").strip()
            if not pp.startswith("uploads/"):
                skipped += 1
                continue
            if _has_active_media(media, entity_type="project_photo", entity_id=pid):
                skipped += 1
                continue
            src = UPLOADS_DIR / pp.replace("uploads/", "")
            if not src.exists():
                missing += 1
                continue
            rel_dir = _storage_rel_dir_for("project_photo", project_id=pid, stage_id=None, edit_request_id=None)
            _migrate_one(
                media,
                src_path=src,
                project_id=pid,
                stage_id=None,
                entity_type="project_photo",
                entity_id=pid,
                rel_dir=rel_dir,
            )
            migrated["project_photo"] += 1

        # reports.photo_path
        for r in main.execute(
            """SELECT r.id as report_id, r.photo_path, r.stage_id, s.project_id
               FROM reports r
               JOIN stages s ON s.id = r.stage_id
               WHERE r.photo_path IS NOT NULL AND TRIM(r.photo_path) <> ''"""
        ).fetchall():
            report_id = int(r["report_id"])
            sid = int(r["stage_id"])
            pid = int(r["project_id"])
            pp = (r["photo_path"] or "").strip()
            if not pp.startswith("uploads/"):
                skipped += 1
                continue
            if _has_active_media(media, entity_type="stage_report", entity_id=report_id):
                skipped += 1
                continue
            src = UPLOADS_DIR / pp.replace("uploads/", "")
            if not src.exists():
                missing += 1
                continue
            rel_dir = _storage_rel_dir_for("stage_report", project_id=pid, stage_id=sid, edit_request_id=None)
            _migrate_one(
                media,
                src_path=src,
                project_id=pid,
                stage_id=sid,
                entity_type="stage_report",
                entity_id=report_id,
                rel_dir=rel_dir,
            )
            migrated["stage_report"] += 1

        # edit_request_photos.photo_path
        for r in main.execute(
            """SELECT erp.id as erp_id, erp.photo_path, er.id as edit_request_id, er.stage_id, s.project_id
               FROM edit_request_photos erp
               JOIN edit_requests er ON er.id = erp.edit_request_id
               JOIN stages s ON s.id = er.stage_id
               WHERE erp.photo_path IS NOT NULL AND TRIM(erp.photo_path) <> ''"""
        ).fetchall():
            erp_id = int(r["erp_id"])
            req_id = int(r["edit_request_id"])
            sid = int(r["stage_id"])
            pid = int(r["project_id"])
            pp = (r["photo_path"] or "").strip()
            if not pp.startswith("uploads/"):
                skipped += 1
                continue
            if _has_active_media(media, entity_type="edit_request_photo", entity_id=erp_id):
                skipped += 1
                continue
            src = UPLOADS_DIR / pp.replace("uploads/", "")
            if not src.exists():
                missing += 1
                continue
            rel_dir = _storage_rel_dir_for("edit_request_photo", project_id=pid, stage_id=sid, edit_request_id=req_id)
            _migrate_one(
                media,
                src_path=src,
                project_id=pid,
                stage_id=sid,
                entity_type="edit_request_photo",
                entity_id=erp_id,
                rel_dir=rel_dir,
            )
            migrated["edit_request_photo"] += 1

        media.commit()
        print("Migration complete.")
        print("Migrated:", migrated)
        print("Skipped:", skipped)
        print("Missing files:", missing)
        print("Storage root:", STORAGE_ROOT)
        print("Media DB:", MEDIA_DB)
    finally:
        main.close()
        media.close()


if __name__ == "__main__":
    migrate()

