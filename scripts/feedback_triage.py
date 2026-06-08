#!/usr/bin/env python3
"""Fetch and analyze AF DAGs Helper feedback archives."""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import shlex
import subprocess
import tarfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlencode


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_INBOX_DIR = ROOT_DIR / ".runtime" / "feedback_inbox"
DEFAULT_HOST = os.environ.get("AF_DAGS_HELPER_DEPLOY_HOST", "ivm-1")
DEFAULT_APP_DIR = os.environ.get("AF_DAGS_HELPER_APP_DIR", "/home/igor.sterhov/dev/af_dags_helper")
DEFAULT_PORT = int(os.environ.get("AF_DAGS_HELPER_PORT", "8000"))


def fetch_feedback_archive(
    *,
    inbox_dir: Path = DEFAULT_INBOX_DIR,
    host: str = DEFAULT_HOST,
    app_dir: str = DEFAULT_APP_DIR,
    port: int = DEFAULT_PORT,
    mode: str = "new",
    mark_exported: bool = False,
    ssh_command: Optional[str] = None,
) -> Path:
    if mode not in {"all", "new"}:
        raise ValueError("mode must be 'all' or 'new'")

    run_dir = _create_run_dir(Path(inbox_dir))
    query = urlencode({"mode": mode, "mark_exported": str(mark_exported).lower()})
    remote_script = _remote_archive_script(query)
    command = _ssh_command(ssh_command) + [host, "bash", "-s", "--", app_dir, str(port)]
    result = subprocess.run(
        command,
        input=remote_script,
        capture_output=True,
        check=True,
    )

    archive_path = run_dir / "dag-issues-feedback.tar.gz"
    archive_path.write_bytes(result.stdout)
    _safe_extract_archive(archive_path, run_dir)

    feedback_json = run_dir / "feedback.json"
    item_count = 0
    if feedback_json.exists():
        data = json.loads(feedback_json.read_text(encoding="utf-8"))
        item_count = len(data.get("items", [])) if isinstance(data, dict) else 0

    fetch_info = {
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "host": host,
        "app_dir": app_dir,
        "port": port,
        "mode": mode,
        "mark_exported": mark_exported,
        "item_count": item_count,
        "archive": archive_path.name,
    }
    (run_dir / "fetch.json").write_text(
        json.dumps(fetch_info, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    return run_dir


def analyze_feedback_run(run_dir: Path) -> Path:
    run_dir = Path(run_dir)
    feedback_path = run_dir / "feedback.json"
    if not feedback_path.exists():
        raise FileNotFoundError(f"Missing feedback.json in {run_dir}")

    data = json.loads(feedback_path.read_text(encoding="utf-8"))
    items = data.get("items", []) if isinstance(data, dict) else []
    lines = [
        "# Feedback Triage Review",
        "",
        f"Run directory: `{run_dir}`",
        f"Feedback items: {len(items)}",
        "",
    ]

    for item in items:
        lines.extend(_review_feedback_item(run_dir, item))
        lines.append("")

    review_path = run_dir / "review.md"
    review_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return review_path


def _review_feedback_item(run_dir: Path, item: Dict) -> List[str]:
    feedback_id = int(item["id"])
    attachments = _attachments_by_kind(run_dir, item)
    lines = [
        f"## Feedback #{feedback_id}",
        "",
        f"- Created: `{item.get('created_at') or '-'}`",
        f"- Source: `{item.get('source_type') or '-'}`",
        f"- DAG: `{item.get('dag_id') or '-'}`",
        f"- Repo: `{item.get('repo_name') or '-'}`",
        f"- DAG path: `{item.get('dag_path') or '-'}`",
        f"- Repo commit: `{item.get('repo_commit') or '-'}`",
        "",
        "### User Comment",
        "",
        _quote_block(item.get("message") or ""),
        "",
        "### Attachments",
        "",
    ]
    for kind, path in sorted(attachments.items()):
        lines.append(f"- `{kind}`: `{path.relative_to(run_dir).as_posix()}`")
    lines.append("")

    dag_source = attachments.get("dag_source")
    generated_path = attachments.get("generated_omentity")
    difference_path = attachments.get("difference")
    warnings_path = attachments.get("warnings")

    if not dag_source or not generated_path:
        lines.extend([
            "### Diagnosis",
            "",
            "- Не хватает DAG source или generated OMEntity attachment; сравнение невозможно.",
        ])
        return lines

    existing = extract_existing_omentity(str(dag_source))
    generated = parse_generated_omentity(generated_path.read_text(encoding="utf-8"))
    results = compare_omentity(existing, generated)
    mappings = suggest_mappings(results)
    ref_corrections = suggest_reference_corrections(results)
    comparison = format_report(str(dag_source), results, mappings, ref_corrections)
    diagnosis = _diagnose_results(results, mappings, bool(warnings_path))

    lines.extend([
        "### Diagnosis",
        "",
        *[f"- {line}" for line in diagnosis],
        "",
        "### OMEntity Comparison",
        "",
        "```text",
        comparison,
        "```",
    ])

    if difference_path:
        lines.extend([
            "",
            "### Difference Attachment",
            "",
            "```text",
            difference_path.read_text(encoding="utf-8").strip(),
            "```",
        ])
    if warnings_path:
        lines.extend([
            "",
            "### Warnings Attachment",
            "",
            "```text",
            warnings_path.read_text(encoding="utf-8").strip(),
            "```",
        ])
    return lines


def _diagnose_results(results, mappings: Dict[str, str], has_warnings: bool) -> List[str]:
    lines: List[str] = []
    if mappings:
        mapping_text = ", ".join(f"{source} -> {target}" for source, target in sorted(mappings.items()))
        lines.append(
            f"Вероятная причина: server mapping отличается от reference ({mapping_text}); проверьте config/server_mapping.yaml."
        )

    for result in results:
        lines.extend(_cross_server_schema_diagnosis(result))
        if result.key_mismatches:
            lines.append(f"Task `{result.task_id}`: key assignment differs between existing and generated OMEntity.")
        if (result.existing_inlets or result.existing_outlets) and not (result.generated_inlets or result.generated_outlets):
            lines.append(
                f"Task `{result.task_id}`: generated OMEntity is empty; parser likely did not associate SQL/API with this task."
            )
        for suspicious in result.suspicious_fqns:
            lines.append(
                f"Task `{result.task_id}`: existing FQN `{suspicious.fqn}` looks suspicious: {suspicious.reason}."
            )

    if has_warnings:
        lines.append("Parser warnings were attached; inspect Warnings Attachment before changing analyzer code.")

    if not lines:
        lines.append("No obvious automated root cause found; inspect user comment, DAG source, and difference manually.")
    return lines


def _cross_server_schema_diagnosis(result: ComparisonResult) -> List[str]:
    lines: List[str] = []
    seen: Set[Tuple[str, str, str]] = set()
    for missing in result.missing_inlets | result.missing_outlets:
        missing_parts = missing.fqn.split(".")
        if len(missing_parts) < 4:
            continue
        missing_tail = ".".join(missing_parts[-2:])
        missing_source = ".".join(missing_parts[:-2])
        for extra in result.extra_inlets | result.extra_outlets:
            extra_parts = extra.fqn.split(".")
            if missing.entity_type != extra.entity_type or len(extra_parts) < 3:
                continue
            extra_tail = ".".join(extra_parts[-2:])
            if missing_tail != extra_tail:
                continue
            generated_server = extra_parts[0]
            identity = (missing_tail, missing_source, generated_server)
            if identity in seen:
                continue
            seen.add(identity)
            lines.append(
                f"Task `{result.task_id}`: likely cross-server source schema `{missing_tail}`; "
                f"generated server is `{generated_server}`, but existing OMEntity points to `{missing_source}`."
            )
    return lines


@dataclass(frozen=True)
class OMEntityInfo:
    entity_type: str
    fqn: str
    key: Optional[str] = None

    def __hash__(self):
        return hash((self.entity_type, normalize_fqn(self.fqn)))

    def __eq__(self, other):
        return isinstance(other, OMEntityInfo) and (
            self.entity_type,
            normalize_fqn(self.fqn),
        ) == (
            other.entity_type,
            normalize_fqn(other.fqn),
        )


@dataclass
class TaskOMEntity:
    task_id: str
    inlets: List[OMEntityInfo] = field(default_factory=list)
    outlets: List[OMEntityInfo] = field(default_factory=list)


@dataclass
class SuspiciousFQN:
    fqn: str
    reason: str
    suggested_correction: Optional[str] = None


@dataclass
class ComparisonResult:
    task_id: str
    existing_inlets: Set[OMEntityInfo] = field(default_factory=set)
    existing_outlets: Set[OMEntityInfo] = field(default_factory=set)
    generated_inlets: Set[OMEntityInfo] = field(default_factory=set)
    generated_outlets: Set[OMEntityInfo] = field(default_factory=set)
    key_mismatches: List[Tuple[str, str, Optional[str], Optional[str]]] = field(default_factory=list)
    suspicious_fqns: List[SuspiciousFQN] = field(default_factory=list)

    @property
    def missing_inlets(self) -> Set[OMEntityInfo]:
        return self.existing_inlets - self.generated_inlets

    @property
    def extra_inlets(self) -> Set[OMEntityInfo]:
        return self.generated_inlets - self.existing_inlets

    @property
    def missing_outlets(self) -> Set[OMEntityInfo]:
        return self.existing_outlets - self.generated_outlets

    @property
    def extra_outlets(self) -> Set[OMEntityInfo]:
        return self.generated_outlets - self.existing_outlets

    @property
    def is_match(self) -> bool:
        return not (
            self.missing_inlets
            or self.extra_inlets
            or self.missing_outlets
            or self.extra_outlets
            or self.key_mismatches
        )


def normalize_fqn(fqn: str) -> str:
    parts = fqn.rsplit(".", 1)
    if len(parts) == 2 and parts[1].endswith("_d"):
        return f"{parts[0]}.{parts[1][:-2]}"
    return fqn


def extract_existing_omentity(dag_path: str) -> Dict[str, TaskOMEntity]:
    tree = ast.parse(Path(dag_path).read_text(encoding="utf-8"))
    tasks: Dict[str, TaskOMEntity] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node) in {"PythonOperator", "ShortCircuitOperator"}:
            task = _parse_operator_omentity(node)
            if task and task.task_id:
                tasks[task.task_id] = task
    return tasks


def parse_generated_omentity(generated_text: str) -> Dict[str, TaskOMEntity]:
    tasks: Dict[str, TaskOMEntity] = {}
    current_task: Optional[TaskOMEntity] = None
    current_section = ""

    for raw_line in generated_text.splitlines():
        line = raw_line.strip()
        if line.startswith("# Task:"):
            if current_task:
                tasks[current_task.task_id] = current_task
            current_task = TaskOMEntity(task_id=line.replace("# Task:", "", 1).strip())
            current_section = ""
        elif line.startswith("inlets=["):
            current_section = "inlets"
        elif line.startswith("outlets=["):
            current_section = "outlets"
        elif line in {"]", "],"}:
            current_section = ""
        elif "OMEntity(" in line and current_task:
            entity = _parse_omentity_line(line)
            if entity and current_section == "inlets":
                current_task.inlets.append(entity)
            elif entity and current_section == "outlets":
                current_task.outlets.append(entity)

    if current_task:
        tasks[current_task.task_id] = current_task
    return tasks


def compare_omentity(existing: Dict[str, TaskOMEntity], generated: Dict[str, TaskOMEntity]) -> List[ComparisonResult]:
    results: List[ComparisonResult] = []
    for task_id in sorted(set(existing) | set(generated)):
        result = ComparisonResult(task_id=task_id)
        if task_id in existing:
            result.existing_inlets = set(existing[task_id].inlets)
            result.existing_outlets = set(existing[task_id].outlets)
            for item in result.existing_inlets | result.existing_outlets:
                suspicious = _detect_suspicious_fqn(item.fqn)
                if suspicious:
                    result.suspicious_fqns.append(suspicious)
        if task_id in generated:
            result.generated_inlets = set(generated[task_id].inlets)
            result.generated_outlets = set(generated[task_id].outlets)
        if task_id in existing and task_id in generated:
            result.key_mismatches = _key_mismatches(existing[task_id], generated[task_id])
        results.append(result)
    return results


def suggest_mappings(results: List[ComparisonResult]) -> Dict[str, str]:
    suggestions: Dict[str, str] = {}
    for result in results:
        for extra in result.extra_inlets | result.extra_outlets:
            for missing in result.missing_inlets | result.missing_outlets:
                if extra.entity_type != missing.entity_type:
                    continue
                extra_parts = extra.fqn.split(".")
                missing_parts = missing.fqn.split(".")
                if len(extra_parts) >= 2 and len(missing_parts) >= 2 and ".".join(extra_parts[1:]) == ".".join(missing_parts[1:]):
                    if extra_parts[0] != missing_parts[0]:
                        suggestions[extra_parts[0]] = missing_parts[0]
    return suggestions


def suggest_reference_corrections(results: List[ComparisonResult]) -> Dict[str, str]:
    suggestions: Dict[str, str] = {}
    for result in results:
        generated_items = result.generated_inlets | result.generated_outlets
        for suspicious in result.suspicious_fqns:
            if not suspicious.suggested_correction:
                continue
            for generated in generated_items:
                if normalize_fqn(generated.fqn) == normalize_fqn(suspicious.suggested_correction):
                    suggestions[suspicious.fqn] = suspicious.suggested_correction
    return suggestions


def format_report(
    dag_path: str,
    results: List[ComparisonResult],
    mappings: Dict[str, str],
    ref_corrections: Optional[Dict[str, str]] = None,
) -> str:
    lines = [
        "=" * 70,
        f"DAG: {Path(dag_path).name}",
        "=" * 70,
        "",
    ]
    lines.append(f"Tasks compared: {len(results)}")
    lines.append(f"Matches: {sum(1 for result in results if result.is_match)}")
    lines.append("")

    for result in results:
        status = "OK" if result.is_match else "MISMATCH"
        lines.append(f"[{status}] {result.task_id}")
        if not result.is_match:
            _append_entity_group(lines, "Existing inlets", result.existing_inlets, result.missing_inlets, "missing in generated")
            _append_entity_group(lines, "Existing outlets", result.existing_outlets, result.missing_outlets, "missing in generated")
            _append_entity_group(lines, "Generated inlets", result.generated_inlets, result.extra_inlets, "extra")
            _append_entity_group(lines, "Generated outlets", result.generated_outlets, result.extra_outlets, "extra")
        if result.key_mismatches:
            lines.append("  Key mismatches:")
            for entity_type, fqn, existing_key, generated_key in result.key_mismatches:
                lines.append(f"    - {entity_type} {fqn}: existing={existing_key!r}, generated={generated_key!r}")
        if result.suspicious_fqns:
            lines.append("  Suspicious FQN:")
            for suspicious in result.suspicious_fqns:
                lines.append(f"    - {suspicious.fqn}: {suspicious.reason}")
        lines.append("")

    if ref_corrections:
        lines.append("Reference correction suggestions:")
        for incorrect, correct in sorted(ref_corrections.items()):
            lines.append(f"  {incorrect} -> {correct}")
        lines.append("")

    if mappings:
        lines.append("Mapping suggestions for config/server_mapping.yaml:")
        for source, target in sorted(mappings.items()):
            lines.append(f"  {source}: {target}")
        lines.append("")
    return "\n".join(lines).rstrip()


def _append_entity_group(lines: List[str], title: str, values: Set[OMEntityInfo], flagged: Set[OMEntityInfo], flag_text: str) -> None:
    lines.append(f"  {title}:")
    if not values:
        lines.append("    (empty)")
        return
    for item in sorted(values, key=lambda value: (value.entity_type, value.fqn)):
        marker = f" <- {flag_text}" if item in flagged else ""
        key = f", key={item.key}" if item.key else ""
        lines.append(f"    - {item.entity_type}: {item.fqn}{key}{marker}")


def _call_name(call: ast.Call) -> str:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ""


def _parse_operator_omentity(call: ast.Call) -> Optional[TaskOMEntity]:
    task = TaskOMEntity(task_id="")
    for keyword in call.keywords:
        if keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant):
            task.task_id = str(keyword.value.value)
        elif keyword.arg == "inlets":
            task.inlets = _extract_omentity_list(keyword.value)
        elif keyword.arg == "outlets":
            task.outlets = _extract_omentity_list(keyword.value)
    return task


def _extract_omentity_list(node: ast.expr) -> List[OMEntityInfo]:
    if not isinstance(node, ast.List):
        return []
    entities: List[OMEntityInfo] = []
    for item in node.elts:
        if isinstance(item, ast.Call):
            entity = _parse_omentity_call(item)
            if entity:
                entities.append(entity)
    return entities


def _parse_omentity_call(call: ast.Call) -> Optional[OMEntityInfo]:
    entity_type = ""
    fqn = ""
    key = None
    for keyword in call.keywords:
        if keyword.arg == "entity" and isinstance(keyword.value, ast.Attribute):
            entity_type = keyword.value.attr
        elif keyword.arg == "fqn":
            fqn = _literal_string(keyword.value)
        elif keyword.arg == "key":
            key = _literal_string(keyword.value)
    if entity_type and fqn:
        return OMEntityInfo(entity_type=entity_type, fqn=fqn, key=key)
    return None


def _literal_string(node: ast.expr) -> str:
    if isinstance(node, ast.Constant):
        return str(node.value)
    if isinstance(node, ast.JoinedStr):
        parts: List[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(str(value.value))
            elif isinstance(value, ast.FormattedValue) and isinstance(value.value, ast.Name):
                parts.append("{" + value.value.id + "}")
            else:
                parts.append("{...}")
        return "".join(parts)
    return ""


def _parse_omentity_line(line: str) -> Optional[OMEntityInfo]:
    entity_match = re.search(r"Entity\.(\w+)", line)
    fqn_match = re.search(r'fqn="([^"]+)"', line)
    key_match = re.search(r'key="([^"]+)"', line)
    if not entity_match or not fqn_match:
        return None
    return OMEntityInfo(
        entity_type=entity_match.group(1),
        fqn=fqn_match.group(1),
        key=key_match.group(1) if key_match else None,
    )


def _key_mismatches(existing: TaskOMEntity, generated: TaskOMEntity) -> List[Tuple[str, str, Optional[str], Optional[str]]]:
    existing_keys = {
        (item.entity_type, normalize_fqn(item.fqn)): item.key
        for item in existing.inlets + existing.outlets
    }
    generated_keys = {
        (item.entity_type, normalize_fqn(item.fqn)): item.key
        for item in generated.inlets + generated.outlets
    }
    mismatches: List[Tuple[str, str, Optional[str], Optional[str]]] = []
    for identity, existing_key in existing_keys.items():
        if identity in generated_keys and existing_key != generated_keys[identity]:
            mismatches.append((identity[0], identity[1], existing_key, generated_keys[identity]))
    return mismatches


def _detect_suspicious_fqn(fqn: str) -> Optional[SuspiciousFQN]:
    parts = fqn.split(".")
    if len(parts) >= 4:
        return SuspiciousFQN(
            fqn=fqn,
            reason=f"FQN has {len(parts)} parts; expected server.schema.table",
            suggested_correction=f"{parts[0]}.{'.'.join(parts[2:])}",
        )
    return None


def _attachments_by_kind(run_dir: Path, item: Dict) -> Dict[str, Path]:
    feedback_dir = run_dir / "attachments" / f"feedback-{int(item['id']):06d}"
    by_kind: Dict[str, Path] = {}
    for attachment in item.get("attachments", []):
        kind = attachment.get("kind")
        filename = attachment.get("filename")
        if not kind or not filename:
            continue
        path = feedback_dir / Path(filename).name
        if path.exists():
            by_kind[kind] = path
    return by_kind


def _remote_archive_script(query: str) -> bytes:
    script = f"""set -euo pipefail
APP_DIR="$1"
PORT="$2"
AUTH_ENV="$APP_DIR/.runtime/auth.env"
if [ ! -f "$AUTH_ENV" ]; then
  echo "Missing auth env: $AUTH_ENV" >&2
  exit 1
fi
set -a
. "$AUTH_ENV"
set +a
curl -fsS -u "$AF_DAGS_HELPER_AUTH_USER:$AF_DAGS_HELPER_AUTH_PASSWORD" \
  "http://127.0.0.1:$PORT/api/feedback/dag-issues/archive?{query}"
"""
    return script.encode("utf-8")


def _ssh_command(ssh_command: Optional[str]) -> List[str]:
    command = ssh_command or os.environ.get("AF_DAGS_HELPER_SSH_COMMAND") or "ssh"
    return shlex.split(command)


def _create_run_dir(inbox_dir: Path) -> Path:
    inbox_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    candidate = inbox_dir / stamp
    suffix = 1
    while candidate.exists():
        suffix += 1
        candidate = inbox_dir / f"{stamp}-{suffix}"
    candidate.mkdir(parents=True)
    return candidate


def _safe_extract_archive(archive_path: Path, run_dir: Path) -> None:
    run_dir = run_dir.resolve()
    with tarfile.open(archive_path, mode="r:gz") as archive:
        for member in archive.getmembers():
            target = (run_dir / member.name).resolve()
            if not _is_within(target, run_dir):
                raise ValueError(f"Archive member escapes run directory: {member.name}")
        archive.extractall(run_dir)


def _is_within(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
    except ValueError:
        return False
    return True


def _quote_block(text: str) -> str:
    return "\n".join(f"> {line}" if line else ">" for line in text.splitlines()) or ">"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch and analyze AF DAGs Helper feedback")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser("fetch", help="Fetch DAG issue feedback archive from the deployed service")
    fetch.add_argument("--host", default=DEFAULT_HOST)
    fetch.add_argument("--app-dir", default=DEFAULT_APP_DIR)
    fetch.add_argument("--port", type=int, default=DEFAULT_PORT)
    fetch.add_argument("--mode", choices=["all", "new"], default="new")
    fetch.add_argument("--mark-exported", action="store_true")
    fetch.add_argument("--inbox-dir", type=Path, default=DEFAULT_INBOX_DIR)
    fetch.add_argument("--ssh-command", default=None)

    analyze = subparsers.add_parser("analyze", help="Analyze an extracted feedback inbox run")
    analyze.add_argument("run_dir", type=Path)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "fetch":
        run_dir = fetch_feedback_archive(
            inbox_dir=args.inbox_dir,
            host=args.host,
            app_dir=args.app_dir,
            port=args.port,
            mode=args.mode,
            mark_exported=args.mark_exported,
            ssh_command=args.ssh_command,
        )
        print(run_dir)
        return 0
    if args.command == "analyze":
        review_path = analyze_feedback_run(args.run_dir)
        print(review_path)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
