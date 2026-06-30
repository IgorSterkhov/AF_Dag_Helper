"""Local feedback persistence for the web UI."""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import sqlite3
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class AnalysisFeedbackContext:
    source_type: str
    dag_id: str
    repo_name: Optional[str]
    dag_path: Optional[str]
    repo_commit: Optional[str]
    original_filename: Optional[str]
    source_text: str
    analysis_options: Dict[str, Any]
    analysis_summary: Dict[str, Any]
    generated_text: str
    difference_text: str
    warnings_text: str = ""
    source_filename: Optional[str] = None


class FeedbackStore:
    """Stores feedback metadata in SQLite and analysis snapshots as files."""

    VALID_TYPES = {"global", "analysis_issue"}
    VALID_MODES = {"all", "new"}

    def __init__(self, root_dir: Path):
        self.root_dir = Path(root_dir).resolve()
        self.db_path = self.root_dir / "feedback.sqlite3"
        self.attachments_dir = self.root_dir / "attachments"
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.attachments_dir.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def create_global_feedback(self, message: str) -> Dict[str, Any]:
        message = self._clean_message(message)
        created_at = self._now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO feedback (
                    created_at, type, status, message
                )
                VALUES (?, 'global', 'new', ?)
                """,
                (created_at, message),
            )
            feedback_id = int(cursor.lastrowid)
        return self.get_feedback(feedback_id)

    def create_analysis_issue_feedback(
        self,
        message: str,
        context: AnalysisFeedbackContext,
    ) -> Dict[str, Any]:
        message = self._clean_message(message)
        created_at = self._now()
        source_filename = self._source_filename(context)
        attachment_dir: Optional[Path] = None

        with self._connect() as conn:
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO feedback (
                        created_at,
                        type,
                        status,
                        message,
                        source_type,
                        dag_id,
                        repo_name,
                        dag_path,
                        repo_commit,
                        original_filename,
                        analysis_options_json,
                        analysis_summary_json
                    )
                    VALUES (?, 'analysis_issue', 'new', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        created_at,
                        message,
                        context.source_type,
                        context.dag_id,
                        context.repo_name,
                        context.dag_path,
                        context.repo_commit,
                        context.original_filename,
                        json.dumps(context.analysis_options, sort_keys=True, ensure_ascii=False),
                        json.dumps(context.analysis_summary, sort_keys=True, ensure_ascii=False),
                    ),
                )
                feedback_id = int(cursor.lastrowid)
                attachment_dir = self._attachment_dir(feedback_id, created_at)
                attachment_dir.mkdir(parents=True, exist_ok=False)

                metadata = {
                    "feedback_id": feedback_id,
                    "created_at": created_at,
                    "source_type": context.source_type,
                    "dag_id": context.dag_id,
                    "repo_name": context.repo_name,
                    "dag_path": context.dag_path,
                    "repo_commit": context.repo_commit,
                    "original_filename": context.original_filename,
                    "analysis_options": context.analysis_options,
                    "analysis_summary": context.analysis_summary,
                }
                attachments = [
                    ("dag_source", f"dag_source__{source_filename}", context.source_text, "text/x-python"),
                    ("generated_omentity", "generated_omentity.py", context.generated_text, "text/x-python"),
                    ("difference", "difference.md", context.difference_text, "text/markdown"),
                    ("metadata", "metadata.json", json.dumps(metadata, indent=2, sort_keys=True, ensure_ascii=False), "application/json"),
                ]
                if context.warnings_text.strip() and context.warnings_text.strip() != "No warnings":
                    attachments.append(("warnings", "warnings.md", context.warnings_text, "text/markdown"))

                for kind, filename, text, content_type in attachments:
                    self._write_attachment(
                        conn,
                        feedback_id=feedback_id,
                        created_at=created_at,
                        attachment_dir=attachment_dir,
                        kind=kind,
                        filename=filename,
                        text=text,
                        content_type=content_type,
                    )
            except Exception:
                conn.rollback()
                if attachment_dir is not None:
                    shutil.rmtree(attachment_dir, ignore_errors=True)
                raise

        return self.get_feedback(feedback_id)

    def get_feedback(self, feedback_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM feedback WHERE id = ?", (feedback_id,)).fetchone()
            if row is None:
                raise KeyError(f"Feedback does not exist: {feedback_id}")
            return self._record_from_row(conn, row)

    def list_feedback(self, feedback_type: str, mode: str = "all") -> List[Dict[str, Any]]:
        self._validate_feedback_type(feedback_type)
        self._validate_mode(mode)
        query = "SELECT * FROM feedback WHERE type = ?"
        params: List[Any] = [feedback_type]
        if mode == "new":
            query += " AND status = 'new'"
        query += " ORDER BY id"
        with self._connect() as conn:
            return [self._record_from_row(conn, row) for row in conn.execute(query, params).fetchall()]

    def mark_exported(self, feedback_ids: Iterable[int]) -> None:
        ids = [int(feedback_id) for feedback_id in feedback_ids]
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE feedback SET status = 'exported', exported_at = ? WHERE id IN ({placeholders})",
                [self._now(), *ids],
            )

    def build_dag_issue_archive(self, records: List[Dict[str, Any]]) -> bytes:
        archive_buffer = io.BytesIO()
        manifest = {"items": records}
        with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
            self._add_bytes_to_archive(
                archive,
                "feedback.json",
                json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False).encode("utf-8"),
            )
            for record in records:
                feedback_dir = f"feedback-{int(record['id']):06d}"
                for attachment in record.get("attachments", []):
                    attachment_path = (self.root_dir / attachment["relative_path"]).resolve()
                    if not self._is_within(attachment_path, self.root_dir) or not attachment_path.is_file():
                        raise FileNotFoundError(f"Feedback attachment is missing: {attachment['relative_path']}")
                    archive.add(
                        attachment_path,
                        arcname=f"attachments/{feedback_dir}/{attachment['filename']}",
                        recursive=False,
                    )
        return archive_buffer.getvalue()

    def _initialize_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS feedback (
                    id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT NOT NULL,
                    source_type TEXT,
                    dag_id TEXT,
                    repo_name TEXT,
                    dag_path TEXT,
                    repo_commit TEXT,
                    original_filename TEXT,
                    analysis_options_json TEXT,
                    analysis_summary_json TEXT,
                    exported_at TEXT
                );
                CREATE TABLE IF NOT EXISTS feedback_attachments (
                    id INTEGER PRIMARY KEY,
                    feedback_id INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(feedback_id) REFERENCES feedback(id)
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _record_from_row(self, conn: sqlite3.Connection, row: sqlite3.Row) -> Dict[str, Any]:
        record = dict(row)
        record["analysis_options"] = self._json_or_empty(record.pop("analysis_options_json"))
        record["analysis_summary"] = self._json_or_empty(record.pop("analysis_summary_json"))
        record["attachments"] = [
            dict(attachment)
            for attachment in conn.execute(
                "SELECT * FROM feedback_attachments WHERE feedback_id = ? ORDER BY id",
                (record["id"],),
            ).fetchall()
        ]
        return record

    def _write_attachment(
        self,
        conn: sqlite3.Connection,
        *,
        feedback_id: int,
        created_at: str,
        attachment_dir: Path,
        kind: str,
        filename: str,
        text: str,
        content_type: str,
    ) -> None:
        safe_filename = self._safe_basename(filename)
        path = attachment_dir / safe_filename
        content = text.encode("utf-8")
        path.write_bytes(content)
        conn.execute(
            """
            INSERT INTO feedback_attachments (
                feedback_id,
                kind,
                filename,
                relative_path,
                content_type,
                sha256,
                size_bytes,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                feedback_id,
                kind,
                safe_filename,
                path.relative_to(self.root_dir).as_posix(),
                content_type,
                hashlib.sha256(content).hexdigest(),
                len(content),
                created_at,
            ),
        )

    def _attachment_dir(self, feedback_id: int, created_at: str) -> Path:
        date = datetime.fromisoformat(created_at)
        return (
            self.attachments_dir
            / f"{date.year:04d}"
            / f"{date.month:02d}"
            / f"{date.day:02d}"
            / f"feedback-{feedback_id:06d}"
        )

    def _source_filename(self, context: AnalysisFeedbackContext) -> str:
        if context.source_filename:
            return self._safe_python_basename(context.source_filename)
        if context.original_filename:
            return self._safe_python_basename(context.original_filename)
        if context.dag_id and context.dag_id != "Unknown":
            return self._safe_python_basename(context.dag_id)
        return "pasted_dag.py"

    def _safe_python_basename(self, name: str) -> str:
        safe = self._safe_basename(name)
        if safe.endswith(".py"):
            return safe
        if "." in safe:
            safe = safe.rsplit(".", 1)[0]
        return f"{safe}.py"

    def _safe_basename(self, name: str) -> str:
        basename = Path(name or "").name
        safe = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in basename)
        safe = safe.strip("._")
        return safe or "attachment"

    def _validate_feedback_type(self, feedback_type: str) -> None:
        if feedback_type not in self.VALID_TYPES:
            raise ValueError("feedback_type must be 'global' or 'analysis_issue'")

    def _validate_mode(self, mode: str) -> None:
        if mode not in self.VALID_MODES:
            raise ValueError("mode must be 'all' or 'new'")

    def _clean_message(self, message: str) -> str:
        message = (message or "").strip()
        if not message:
            raise ValueError("Feedback message is required")
        return message

    def _json_or_empty(self, value: Optional[str]) -> Dict[str, Any]:
        if not value:
            return {}
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _add_bytes_to_archive(self, archive: tarfile.TarFile, name: str, content: bytes) -> None:
        info = tarfile.TarInfo(name=name)
        info.size = len(content)
        archive.addfile(info, io.BytesIO(content))

    def _is_within(self, path: Path, base: Path) -> bool:
        try:
            path.relative_to(base)
        except ValueError:
            return False
        return True
