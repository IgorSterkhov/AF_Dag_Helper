"""Lightweight SQL table/dictionary extractor for diagnostics and tests."""

from dataclasses import dataclass
import re
from typing import List


_IMPORT_LINE_RE = re.compile(r"^\s*(?:from\s+\S+\s+import\s+.+|import\s+\S+.*)$")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--.*?$", re.MULTILINE)
_TABLE_RE = re.compile(
    r"\b(?:from|join|insert\s+into|truncate(?:\s+table)?)\s+"
    r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+)",
    re.IGNORECASE,
)
_ALTER_TABLE_RE = re.compile(
    r"\balter\s+table\s+"
    r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+)"
    r".*?\bfrom\s+"
    r"([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)+)",
    re.IGNORECASE | re.DOTALL,
)
_DICT_RE = re.compile(
    r"\bdictGet[A-Za-z0-9_]*\s*\(\s*(['\"])([^'\"]*\.[^'\"]*)\1",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SQLReferences:
    tables: List[str]
    dicts: List[str]


def extract_sql_references(text: str) -> SQLReferences:
    """Extract table-like references from SQL or Python/DAG text."""
    prepared = _strip_comments(_strip_python_import_lines(text))
    tables = set()
    dicts = set()

    for match in _TABLE_RE.finditer(prepared):
        tables.add(match.group(1))

    for match in _ALTER_TABLE_RE.finditer(prepared):
        tables.add(match.group(1))
        tables.add(match.group(2))

    for match in _DICT_RE.finditer(prepared):
        dict_name = match.group(2)
        if "." in dict_name:
            dicts.add(dict_name)

    return SQLReferences(tables=sorted(tables), dicts=sorted(dicts))


def _strip_python_import_lines(text: str) -> str:
    lines = [line for line in text.splitlines() if not _IMPORT_LINE_RE.match(line)]
    return "\n".join(lines)


def _strip_comments(text: str) -> str:
    without_blocks = _BLOCK_COMMENT_RE.sub(" ", text)
    return _LINE_COMMENT_RE.sub("", without_blocks)
