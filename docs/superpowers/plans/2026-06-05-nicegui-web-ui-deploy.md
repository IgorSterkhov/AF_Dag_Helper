# NiceGUI Web UI Deploy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a FastAPI + NiceGUI web interface for AF DAGs Helper and deploy it repeatably to `ivm-1`.

**Architecture:** Extract DAG analysis orchestration into `web/analysis_service.py`, expose it through a NiceGUI hybrid workspace in `web/app.py`, and deploy from GitHub with `scripts/deploy_ivm1.sh`. The existing CLI and Tkinter app stay functional; the first web version reads but does not write `config/server_mapping.yaml`.

**Tech Stack:** Python 3.9+, FastAPI, NiceGUI, Uvicorn, sqlglot, PyYAML, unittest, systemd.

---

## File Structure

- Create `web/__init__.py`: marks the web package.
- Create `web/analysis_service.py`: shared analysis workflow, temporary source handling, text formatting, graph-data generation.
- Create `web/server_files.py`: safe server-side DAG discovery and path resolution.
- Create `web/app.py`: FastAPI app, `/health`, NiceGUI page, CLI flags for host/port.
- Create `tests/__init__.py`: marks test package.
- Create `tests/test_web_analysis_service.py`: service-level tests using project fixtures and temporary DAG files.
- Create `tests/test_web_server_files.py`: server file discovery/path traversal tests.
- Create `tests/test_web_app.py`: health endpoint test.
- Create `scripts/deploy_ivm1.sh`: local deploy script that updates `~/dev/af_dags_helper` on `ivm-1` and restarts systemd.
- Modify `requirements.txt`: add web runtime dependencies.
- Modify `README.md`: add web UI local run and deploy commands.
- Modify `CLAUDE.md`: add web run/deploy commands and architecture note.

## Task 1: Server File Discovery

**Files:**
- Create: `web/__init__.py`
- Create: `web/server_files.py`
- Create: `tests/__init__.py`
- Create: `tests/test_web_server_files.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_web_server_files.py
import tempfile
import unittest
from pathlib import Path

from web.server_files import ServerFileBrowser


class ServerFileBrowserTest(unittest.TestCase):
    def test_lists_only_python_files_from_allowed_directories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            samples = root / "Dags samples"
            forbidden = root / "private"
            samples.mkdir()
            forbidden.mkdir()
            (samples / "a.py").write_text("print('a')", encoding="utf-8")
            (samples / "notes.txt").write_text("ignore", encoding="utf-8")
            (forbidden / "secret.py").write_text("print('secret')", encoding="utf-8")

            browser = ServerFileBrowser(root, allowed_dirs=("Dags samples",))

            self.assertEqual(browser.list_dag_files(), ["Dags samples/a.py"])

    def test_resolve_rejects_path_traversal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Dags samples").mkdir()
            browser = ServerFileBrowser(root, allowed_dirs=("Dags samples",))

            with self.assertRaises(ValueError):
                browser.resolve("Dags samples/../secret.py")

    def test_resolve_returns_allowed_python_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            samples = root / "Dags samples"
            samples.mkdir()
            dag = samples / "sample.py"
            dag.write_text("print('ok')", encoding="utf-8")
            browser = ServerFileBrowser(root, allowed_dirs=("Dags samples",))

            self.assertEqual(browser.resolve("Dags samples/sample.py"), dag.resolve())


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
"./venv/Scripts/python.exe" -m unittest tests.test_web_server_files -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'web.server_files'`.

- [ ] **Step 3: Implement server file browser**

```python
# web/__init__.py
"""Web interface package for AF DAGs Helper."""
```

```python
# tests/__init__.py
"""Test package."""
```

```python
# web/server_files.py
"""Safe server-side DAG file discovery."""

from pathlib import Path
from typing import Iterable, List, Tuple


class ServerFileBrowser:
    """Lists and resolves DAG files from a small allowlist under project root."""

    def __init__(
        self,
        project_root: Path,
        allowed_dirs: Iterable[str] = ("Dags samples", "Dags for test"),
    ):
        self.project_root = Path(project_root).resolve()
        self.allowed_dirs: Tuple[str, ...] = tuple(allowed_dirs)

    def list_dag_files(self) -> List[str]:
        files: List[str] = []
        for dirname in self.allowed_dirs:
            base = (self.project_root / dirname).resolve()
            if not base.exists() or not base.is_dir():
                continue
            for path in sorted(base.rglob("*.py")):
                resolved = path.resolve()
                if self._is_allowed(resolved):
                    files.append(resolved.relative_to(self.project_root).as_posix())
        return files

    def resolve(self, relative_path: str) -> Path:
        candidate = (self.project_root / relative_path).resolve()
        if candidate.suffix != ".py":
            raise ValueError("Only .py DAG files are allowed")
        if not candidate.exists() or not candidate.is_file():
            raise ValueError(f"DAG file does not exist: {relative_path}")
        if not self._is_allowed(candidate):
            raise ValueError(f"DAG file is outside allowed directories: {relative_path}")
        return candidate

    def _is_allowed(self, path: Path) -> bool:
        for dirname in self.allowed_dirs:
            base = (self.project_root / dirname).resolve()
            try:
                path.relative_to(base)
            except ValueError:
                continue
            return True
        return False
```

- [ ] **Step 4: Run tests to verify GREEN**

Run:

```bash
"./venv/Scripts/python.exe" -m unittest tests.test_web_server_files -v
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add web/__init__.py web/server_files.py tests/__init__.py tests/test_web_server_files.py
git commit -m "test: add safe server DAG file browser"
```

## Task 2: Shared Analysis Service

**Files:**
- Create: `web/analysis_service.py`
- Create: `tests/test_web_analysis_service.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_web_analysis_service.py
import tempfile
import unittest
from pathlib import Path

from web.analysis_service import DAGAnalysisRequest, DAGAnalysisService


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MAPPING_FILE = PROJECT_ROOT / "config" / "server_mapping.yaml"


class DAGAnalysisServiceTest(unittest.TestCase):
    def test_analyzes_sample_dag_with_graph_data(self):
        service = DAGAnalysisService(PROJECT_ROOT, MAPPING_FILE)
        request = DAGAnalysisRequest(
            dag_path=PROJECT_ROOT / "Dags samples" / "api_ch3_dict_sc_suppliers.py",
            force_all_tasks=True,
            compare_existing=True,
            initial_view="dag",
        )

        result = service.analyze(request)

        self.assertEqual(result.dag_id, "api_ch3_dict_sc_suppliers")
        self.assertIn("task_update_supplier_office_links", result.generated_text)
        self.assertIn("MATCH", result.difference_text)
        self.assertIn("dag_view", result.graph_data)
        self.assertGreater(result.output_count, 0)

    def test_writes_source_to_runtime_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime_dir = Path(tmp)
            service = DAGAnalysisService(PROJECT_ROOT, MAPPING_FILE, runtime_dir=runtime_dir)

            dag_path = service.write_source_to_runtime_file(
                "sample_runtime",
                "from airflow.models import DAG\n",
            )

            self.assertTrue(dag_path.exists())
            self.assertEqual(dag_path.suffix, ".py")
            self.assertTrue(dag_path.read_text(encoding="utf-8").startswith("from airflow"))
            self.assertTrue(dag_path.resolve().is_relative_to(runtime_dir.resolve()))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
"./venv/Scripts/python.exe" -m unittest tests.test_web_analysis_service -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'web.analysis_service'`.

- [ ] **Step 3: Implement analysis service**

```python
# web/analysis_service.py
"""Shared DAG analysis service for web UI."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from analyzer.dag_parser import DAGParser
from analyzer.models import SQLAnalysisResult
from analyzer.sql_analyzer import SQLAnalyzer
from generator.fqn_builder import FQNBuilder
from generator.omentity_generator import OMEntityGenerator
from test_against_samples import extract_existing_omentity
from visualizer.cytoscape_diagram import CytoscapeGraphBuilder
from visualizer.diagram import DataFlowDiagram


@dataclass
class DAGAnalysisRequest:
    dag_path: Path
    force_all_tasks: bool = True
    compare_existing: bool = True
    initial_view: str = "dag"


@dataclass
class DAGAnalysisResult:
    dag_id: str
    generated_text: str
    difference_text: str
    text_diagram: str
    graph_data: Dict[str, Any]
    warnings: List[str] = field(default_factory=list)
    task_count: int = 0
    output_count: int = 0


class DAGAnalysisService:
    """Runs the existing parser/analyzer/generator pipeline for UI consumers."""

    def __init__(
        self,
        project_root: Path,
        mapping_file: Path,
        runtime_dir: Optional[Path] = None,
    ):
        self.project_root = Path(project_root).resolve()
        self.mapping_file = Path(mapping_file)
        self.runtime_dir = Path(runtime_dir or self.project_root / ".runtime" / "uploads")

    def write_source_to_runtime_file(self, name: str, source: str) -> Path:
        safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name)
        if not safe_name:
            safe_name = "uploaded_dag"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        path = self.runtime_dir / f"{safe_name}.py"
        path.write_text(source, encoding="utf-8")
        return path

    def analyze(self, request: DAGAnalysisRequest) -> DAGAnalysisResult:
        dag_parser = DAGParser()
        sql_analyzer = SQLAnalyzer()
        fqn_builder = FQNBuilder(str(self.mapping_file))
        generator = OMEntityGenerator(fqn_builder)

        dag_result = dag_parser.parse_file(str(request.dag_path))
        existing_omentity = extract_existing_omentity(str(request.dag_path)) if request.compare_existing else {}

        if request.force_all_tasks:
            for task in dag_result.tasks:
                task.has_omentity = False

        sql_results = {
            sql_var: sql_analyzer.analyze(sql_code)
            for sql_var, sql_code in dag_result.sql_variables.items()
        }

        func_sql_results = {}
        for func_name, func_info in dag_result.functions.items():
            combined = SQLAnalysisResult()
            for sql_var in func_info.sql_variables:
                if sql_var in sql_results:
                    combined = combined.merge(sql_results[sql_var])
            if combined.inlets or combined.outlets or combined.dictionaries:
                func_sql_results[func_name] = combined

        outputs = generator.generate_for_dag(dag_result, func_sql_results)
        dag_id = dag_result.dag_id or "Unknown"
        text_diagram = DataFlowDiagram().render_all(outputs) if outputs else ""
        graph_data: Dict[str, Any] = {}
        if outputs:
            graph_data = CytoscapeGraphBuilder().build_full_data(outputs, dag_id)
            graph_data["initial_view"] = request.initial_view

        return DAGAnalysisResult(
            dag_id=dag_id,
            generated_text=self._format_generated_text(outputs, dag_result, request.force_all_tasks),
            difference_text=self._format_difference_text(outputs, existing_omentity),
            text_diagram=text_diagram,
            graph_data=graph_data,
            warnings=list(dag_result.warnings),
            task_count=len(dag_result.tasks),
            output_count=len(outputs),
        )

    def _format_generated_text(self, outputs, dag_result, force_all_tasks: bool) -> str:
        lines = [
            "# " + "=" * 65,
            f"# DAG: {dag_result.dag_id or 'Unknown'}",
            f"# Mode: {'force all tasks' if force_all_tasks else 'missing OMEntity only'}",
            f"# Tasks processed: {len(outputs)}",
            "# " + "=" * 65,
            "",
        ]
        if not outputs:
            lines.append("# No tasks with SQL/API lineage were found")
            return "\n".join(lines)

        for output in outputs:
            lines.append(output.generated_code)
            lines.append("")
        return "\n".join(lines)

    def _format_difference_text(self, outputs, existing_omentity) -> str:
        if not existing_omentity:
            return "(comparison disabled or no existing OMEntity found)"

        lines: List[str] = []
        for output in outputs:
            existing = existing_omentity.get(output.task_id)
            lines.append("# " + "-" * 65)
            lines.append(f"# Task: {output.task_id}")
            if not existing or (not existing.inlets and not existing.outlets):
                lines.append("# (no existing OMEntity)")
                continue

            gen_inlet_fqns = {item.fqn for item in output.inlets}
            gen_outlet_fqns = {item.fqn for item in output.outlets}
            exist_inlet_fqns = {item.fqn for item in existing.inlets}
            exist_outlet_fqns = {item.fqn for item in existing.outlets}

            extra_inlets = gen_inlet_fqns - exist_inlet_fqns
            missing_inlets = exist_inlet_fqns - gen_inlet_fqns
            extra_outlets = gen_outlet_fqns - exist_outlet_fqns
            missing_outlets = exist_outlet_fqns - gen_outlet_fqns

            if not extra_inlets and not missing_inlets and not extra_outlets and not missing_outlets:
                lines.append("# MATCH")
                continue

            lines.append("# MISMATCH")
            for label, values in (
                ("Extra inlets", extra_inlets),
                ("Missing inlets", missing_inlets),
                ("Extra outlets", extra_outlets),
                ("Missing outlets", missing_outlets),
            ):
                if values:
                    lines.append(f"# {label}:")
                    for fqn in sorted(values):
                        lines.append(f"#   - {fqn}")
        return "\n".join(lines)
```

- [ ] **Step 4: Run tests to verify GREEN**

Run:

```bash
"./venv/Scripts/python.exe" -m unittest tests.test_web_analysis_service -v
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add web/analysis_service.py tests/test_web_analysis_service.py
git commit -m "feat: add shared DAG analysis service"
```

## Task 3: FastAPI + NiceGUI App

**Files:**
- Create: `web/app.py`
- Create: `tests/test_web_app.py`
- Modify: `requirements.txt`

- [ ] **Step 1: Write failing health endpoint test**

```python
# tests/test_web_app.py
import unittest

from fastapi.testclient import TestClient

from web.app import app


class WebAppTest(unittest.TestCase):
    def test_health_endpoint(self):
        client = TestClient(app)
        response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Add dependencies before running RED**

```text
sqlglot>=23.0.0
pyyaml>=6.0
fastapi>=0.115.0
nicegui>=2.0.0
uvicorn>=0.30.0
python-multipart>=0.0.9
httpx>=0.27.0
```

- [ ] **Step 3: Install dependencies**

Run:

```bash
"./venv/Scripts/python.exe" -m pip install -r requirements.txt
```

Expected: dependencies install successfully.

- [ ] **Step 4: Run test to verify RED**

Run:

```bash
"./venv/Scripts/python.exe" -m unittest tests.test_web_app -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'web.app'`.

- [ ] **Step 5: Implement FastAPI + NiceGUI app**

```python
# web/app.py
"""FastAPI + NiceGUI web interface for AF DAGs Helper."""

import argparse
import html as html_lib
import os
import sys
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI
from nicegui import ui

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from web.analysis_service import DAGAnalysisRequest, DAGAnalysisService  # noqa: E402
from web.server_files import ServerFileBrowser  # noqa: E402
from visualizer.cytoscape_viewer import CytoscapeViewer  # noqa: E402


app = FastAPI(title="AF DAGs Helper")


@app.get("/health")
def health():
    return {"status": "ok", "service": "af-dags-helper"}


class WebState:
    def __init__(self):
        self.source_mode = "server"
        self.uploaded_source: Optional[str] = None
        self.uploaded_name = "uploaded_dag"


def create_ui():
    project_root = ROOT_DIR
    mapping_file = Path(os.environ.get("AF_DAGS_HELPER_MAPPING_FILE", project_root / "config" / "server_mapping.yaml"))
    runtime_dir = Path(os.environ.get("AF_DAGS_HELPER_RUNTIME_DIR", project_root / ".runtime" / "uploads"))
    service = DAGAnalysisService(project_root, mapping_file, runtime_dir=runtime_dir)
    browser = ServerFileBrowser(project_root)
    state = WebState()

    ui.add_head_html("<style>.nicegui-content{max-width:none}</style>")

    with ui.header().classes("items-center"):
        ui.label("AF DAGs Helper").classes("text-h6")
        ui.space()
        ui.label("FastAPI + NiceGUI").classes("text-caption")

    with ui.row().classes("w-full no-wrap items-start"):
        with ui.card().classes("w-1/3 min-w-[360px]"):
            ui.label("Source").classes("text-h6")
            source_tabs = ui.tabs().classes("w-full")
            with source_tabs:
                server_tab = ui.tab("Server file")
                upload_tab = ui.tab("Upload")
                paste_tab = ui.tab("Paste")

            server_files = browser.list_dag_files()
            selected_file = ui.select(server_files, label="DAG file", value=server_files[0] if server_files else None).classes("w-full")
            paste_area = ui.textarea(label="Paste DAG source").classes("w-full").props("rows=12")
            upload_label = ui.label("No file uploaded")

            def on_upload(event):
                content = event.content.read()
                state.uploaded_source = content.decode("utf-8")
                state.uploaded_name = Path(event.name).stem
                upload_label.set_text(f"Uploaded: {event.name}")

            with ui.tab_panels(source_tabs, value=server_tab).classes("w-full"):
                with ui.tab_panel(server_tab):
                    ui.label("Choose a DAG from allowed project folders.")
                with ui.tab_panel(upload_tab):
                    ui.upload(on_upload=on_upload, auto_upload=True).props("accept=.py").classes("w-full")
                    upload_label
                with ui.tab_panel(paste_tab):
                    paste_area

            force = ui.checkbox("Force all tasks", value=True)
            compare = ui.checkbox("Compare existing OMEntity", value=True)
            diagram_view = ui.toggle({"dag": "DAG view", "task": "Task view"}, value="dag")
            analyze_btn = ui.button("Analyze", icon="play_arrow").classes("w-full")

        with ui.column().classes("w-2/3"):
            summary = ui.markdown("Select a source and run analysis.")
            with ui.tabs().classes("w-full") as result_tabs:
                generated_tab = ui.tab("Generated OMEntity")
                diff_tab = ui.tab("Difference")
                text_diagram_tab = ui.tab("Text Diagram")
                graph_tab = ui.tab("Interactive Diagram")
                warnings_tab = ui.tab("Warnings")

            generated = ui.codemirror("", language="Python").classes("w-full")
            diff = ui.codemirror("", language="Markdown").classes("w-full")
            text_diagram = ui.codemirror("", language="Markdown").classes("w-full")
            diagram_html = ui.html("").classes("w-full")
            warnings = ui.codemirror("", language="Markdown").classes("w-full")

            with ui.tab_panels(result_tabs, value=generated_tab).classes("w-full"):
                with ui.tab_panel(generated_tab):
                    generated
                with ui.tab_panel(diff_tab):
                    diff
                with ui.tab_panel(text_diagram_tab):
                    text_diagram
                with ui.tab_panel(graph_tab):
                    diagram_html
                with ui.tab_panel(warnings_tab):
                    warnings

    def analyze():
        try:
            active_tab = source_tabs.value
            if active_tab == server_tab:
                if not selected_file.value:
                    ui.notify("No server DAG file selected", type="warning")
                    return
                dag_path = browser.resolve(selected_file.value)
            elif active_tab == upload_tab:
                if not state.uploaded_source:
                    ui.notify("Upload a .py DAG first", type="warning")
                    return
                dag_path = service.write_source_to_runtime_file(state.uploaded_name, state.uploaded_source)
            else:
                if not paste_area.value:
                    ui.notify("Paste DAG source first", type="warning")
                    return
                dag_path = service.write_source_to_runtime_file("pasted_dag", paste_area.value)

            result = service.analyze(DAGAnalysisRequest(
                dag_path=dag_path,
                force_all_tasks=force.value,
                compare_existing=compare.value,
                initial_view=diagram_view.value,
            ))
            summary.set_content(
                f"**DAG:** `{result.dag_id}`  \n"
                f"**Tasks:** {result.task_count}  \n"
                f"**Outputs:** {result.output_count}"
            )
            generated.set_value(result.generated_text)
            diff.set_value(result.difference_text)
            text_diagram.set_value(result.text_diagram)
            if result.graph_data:
                html = CytoscapeViewer()._build_html(result.graph_data, f"Lineage: {result.dag_id}")
                srcdoc = html_lib.escape(html, quote=True)
                diagram_html.set_content(
                    f'<iframe srcdoc="{srcdoc}" style="width:100%;height:70vh;border:1px solid #ddd;border-radius:6px;"></iframe>'
                )
            else:
                diagram_html.set_content("<p>No graph data available.</p>")
            warnings.set_value("\n".join(result.warnings) if result.warnings else "No warnings")
            ui.notify("Analysis complete", type="positive")
        except Exception as exc:
            ui.notify(f"Analysis failed: {exc}", type="negative", close_button=True)

    analyze_btn.on_click(analyze)


create_ui()
ui.run_with(app)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("AF_DAGS_HELPER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("AF_DAGS_HELPER_PORT", "8000")))
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ in {"__main__", "__mp_main__"}:
    main()
```

- [ ] **Step 6: Run tests to verify GREEN**

Run:

```bash
"./venv/Scripts/python.exe" -m unittest tests.test_web_app -v
```

Expected: 1 test passes.

- [ ] **Step 7: Commit**

```bash
git add requirements.txt web/app.py tests/test_web_app.py
git commit -m "feat: add FastAPI NiceGUI web app"
```

## Task 4: Deploy Script

**Files:**
- Create: `scripts/deploy_ivm1.sh`

- [ ] **Step 1: Write deploy script**

```bash
#!/usr/bin/env bash
set -euo pipefail

HOST="${AF_DAGS_HELPER_DEPLOY_HOST:-ivm-1}"
BRANCH="${AF_DAGS_HELPER_BRANCH:-master}"
REMOTE_URL="${AF_DAGS_HELPER_REMOTE_URL:-git@github.com:IgorSterkhov/AF_Dag_Helper.git}"
APP_DIR="${AF_DAGS_HELPER_APP_DIR:-/home/igor.sterhov/dev/af_dags_helper}"
PORT="${AF_DAGS_HELPER_PORT:-8000}"
SERVICE="af-dags-helper.service"

ssh "$HOST" bash -s -- "$APP_DIR" "$REMOTE_URL" "$BRANCH" "$PORT" "$SERVICE" <<'REMOTE'
set -euo pipefail

APP_DIR="$1"
REMOTE_URL="$2"
BRANCH="$3"
PORT="$4"
SERVICE="$5"

mkdir -p "$(dirname "$APP_DIR")"

if [ ! -d "$APP_DIR/.git" ]; then
  git clone "$REMOTE_URL" "$APP_DIR"
fi

cd "$APP_DIR"
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"

if ss -ltn "sport = :$PORT" | grep -q ":$PORT"; then
  if ! systemctl is-active --quiet "$SERVICE"; then
    echo "Port $PORT is already in use by another process"
    ss -ltnp "sport = :$PORT" || true
    exit 1
  fi
fi

python3 -m venv .venv
.venv/bin/python -m ensurepip --upgrade
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

sudo tee "/etc/systemd/system/$SERVICE" >/dev/null <<UNIT
[Unit]
Description=AF DAGs Helper web UI
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$APP_DIR
Environment=AF_DAGS_HELPER_HOST=0.0.0.0
Environment=AF_DAGS_HELPER_PORT=$PORT
ExecStart=$APP_DIR/.venv/bin/python -m web.app --host 0.0.0.0 --port $PORT
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE"
sudo systemctl restart "$SERVICE"
sudo systemctl --no-pager --full status "$SERVICE"

echo "AF DAGs Helper is deployed at: http://$(hostname -f):$PORT"
REMOTE
```

- [ ] **Step 2: Make script executable and lint shell syntax**

Run:

```bash
chmod +x scripts/deploy_ivm1.sh
bash -n scripts/deploy_ivm1.sh
```

Expected: no output, exit 0.

- [ ] **Step 3: Commit**

```bash
git add scripts/deploy_ivm1.sh
git commit -m "chore: add ivm-1 deploy script"
```

## Task 5: Documentation

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Document web run commands**

Add to both docs:

```markdown
### Web UI

```bash
python -m web.app --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000`.
```

- [ ] **Step 2: Document deployment**

Add to README:

```markdown
### Deploy to ivm-1

```bash
scripts/deploy_ivm1.sh
```

The script deploys `master` to `~/dev/af_dags_helper` on `ivm-1`, creates `.venv`, installs dependencies, and restarts `af-dags-helper.service` on port `8000`.
```

- [ ] **Step 3: Commit**

```bash
git add README.md CLAUDE.md
git commit -m "docs: add web UI run and deploy notes"
```

## Task 6: Full Verification And GitHub Push

**Files:**
- No direct edits unless verification finds a defect.

- [ ] **Step 1: Run focused tests**

```bash
"./venv/Scripts/python.exe" -m unittest tests.test_web_server_files tests.test_web_analysis_service tests.test_web_app -v
```

Expected: all focused tests pass.

- [ ] **Step 2: Run compile check**

```bash
"./venv/Scripts/python.exe" -m py_compile main.py gui/app.py web/app.py web/analysis_service.py web/server_files.py analyzer/sql_analyzer.py generator/fqn_builder.py generator/omentity_generator.py visualizer/cytoscape_diagram.py visualizer/cytoscape_viewer.py
```

Expected: exit 0.

- [ ] **Step 3: Run existing smoke regression**

```bash
PYTHONIOENCODING=utf-8 "./venv/Scripts/python.exe" test_against_samples.py
```

Expected: exit 0. Existing printed mismatches are acceptable.

- [ ] **Step 4: Run deploy script syntax check**

```bash
bash -n scripts/deploy_ivm1.sh
```

Expected: exit 0.

- [ ] **Step 5: Push to GitHub**

```bash
git push git@github.com:IgorSterkhov/AF_Dag_Helper.git master
git fetch git@github.com:IgorSterkhov/AF_Dag_Helper.git master:refs/remotes/origin/master
```

Expected: GitHub `master` points at the local HEAD.

## Task 7: Deploy To `ivm-1`

**Files:**
- No local edits unless deployment exposes a defect.

- [ ] **Step 1: Run deployment**

```bash
scripts/deploy_ivm1.sh
```

Expected: systemd service starts successfully on `ivm-1`.

- [ ] **Step 2: Verify remote health**

```bash
ssh ivm-1 curl -fsS http://127.0.0.1:8000/health
```

Expected:

```json
{"status":"ok","service":"af-dags-helper"}
```

- [ ] **Step 3: Verify service status**

```bash
ssh ivm-1 systemctl --no-pager --full status af-dags-helper.service
```

Expected: service is `active (running)`.
