"""FastAPI + NiceGUI web interface for AF DAGs Helper."""

import argparse
import html as html_lib
import os
import sys
from dataclasses import replace
from pathlib import Path
from typing import Dict, List, Optional, Set

import uvicorn
from fastapi import FastAPI, HTTPException, Response
from nicegui import ui

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from visualizer.cytoscape_viewer import CytoscapeViewer  # noqa: E402
from web.analysis_service import DAGAnalysisRequest, DAGAnalysisService  # noqa: E402
from web.auth import BasicAuthMiddleware  # noqa: E402
from web.clipboard_ui import bind_copy_button, install_clipboard_script, set_clipboard_text  # noqa: E402
from web.feedback_store import AnalysisFeedbackContext, FeedbackStore  # noqa: E402
from web.repository_browser import RepositoryBrowser  # noqa: E402


app = FastAPI(title="AF DAGs Helper")
app.add_middleware(BasicAuthMiddleware)


@app.get("/health")
def health():
    return {"status": "ok", "service": "af-dags-helper"}


def feedback_store_from_env() -> FeedbackStore:
    root = Path(os.environ.get("AF_DAGS_HELPER_FEEDBACK_DIR", ROOT_DIR / ".runtime" / "feedback"))
    return FeedbackStore(root)


def _feedback_records_payload(
    feedback_type: str,
    mode: str,
    mark_exported: bool,
) -> Dict[str, List[Dict]]:
    try:
        store = feedback_store_from_env()
        records = store.list_feedback(feedback_type, mode=mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    payload = {"items": records}
    if mark_exported:
        store.mark_exported(record["id"] for record in records)
    return payload


@app.get("/api/feedback/dag-issues")
def list_dag_issue_feedback(mode: str = "all", mark_exported: bool = False):
    return _feedback_records_payload("analysis_issue", mode, mark_exported)


@app.get("/api/feedback/dag-issues/archive")
def download_dag_issue_feedback_archive(mode: str = "all", mark_exported: bool = False):
    try:
        store = feedback_store_from_env()
        records = store.list_feedback("analysis_issue", mode=mode)
        archive_bytes = store.build_dag_issue_archive(records)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if mark_exported:
        store.mark_exported(record["id"] for record in records)
    return Response(
        content=archive_bytes,
        media_type="application/gzip",
        headers={"Content-Disposition": "attachment; filename=dag-issues-feedback.tar.gz"},
    )


@app.get("/api/feedback/global")
def list_global_feedback(mode: str = "all", mark_exported: bool = False):
    return _feedback_records_payload("global", mode, mark_exported)


class WebState:
    """Small in-memory UI state for one NiceGUI process."""

    def __init__(self):
        self.active_source_tab = "repo"
        self.uploaded_source: Optional[str] = None
        self.uploaded_name = "uploaded_dag"
        self.uploaded_original_filename = "uploaded_dag.py"
        self.generated_text = ""
        self.graph_data = {}
        self.dag_id = ""
        self.selected_repo: Optional[str] = None
        self.selected_dag_node: Optional[str] = None
        self.picker_dir = ""
        self.picker_nodes: List[Dict] = []
        self.picker_expanded_dirs: Set[str] = set()
        self.picker_search = ""
        self.picker_selected_node: Optional[str] = None
        self.last_analysis_context: Optional[AnalysisFeedbackContext] = None


def _source_tab_name(active_tab, repo_tab, upload_tab, paste_tab) -> str:
    for tab, name in ((repo_tab, "repo"), (upload_tab, "upload"), (paste_tab, "paste")):
        if active_tab is tab or active_tab == tab:
            return name
    if isinstance(active_tab, str) and active_tab in {"repo", "upload", "paste"}:
        return str(active_tab)
    props = getattr(active_tab, "_props", None) or getattr(active_tab, "props", None)
    if isinstance(props, dict) and props.get("name") in {"repo", "upload", "paste"}:
        return str(props["name"])
    return ""


def _resolve_current_source_path(
    *,
    active_tab: str,
    repo_name: Optional[str],
    selected_dag_node: Optional[str],
    uploaded_source: Optional[str],
    uploaded_name: str,
    pasted_source: str,
    repository_browser: RepositoryBrowser,
    service: DAGAnalysisService,
) -> Path:
    if active_tab == "repo":
        if not repo_name:
            raise ValueError("No repository selected")
        if not selected_dag_node:
            raise ValueError("No DAG selected")
        return repository_browser.resolve_dag_path(repo_name, selected_dag_node)
    if active_tab == "upload":
        if not uploaded_source:
            raise ValueError("Upload a .py DAG first")
        return service.write_source_to_runtime_file(uploaded_name, uploaded_source)
    if active_tab == "paste":
        if not pasted_source:
            raise ValueError("Paste DAG source first")
        return service.write_source_to_runtime_file("pasted_dag", pasted_source)
    raise ValueError("Select a source tab")


async def _read_upload_event_source(event) -> tuple[str, str]:
    uploaded_file = event.file
    return Path(uploaded_file.name).stem, await uploaded_file.text(encoding="utf-8")


def _safe_python_basename(name: str) -> str:
    basename = Path(name or "").name
    if basename.endswith(".py"):
        basename = basename[:-3]
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in basename)
    safe = safe.strip("_")
    return f"{safe or 'pasted_dag'}.py"


def _feedback_source_filename(
    source_type: str,
    dag_id: str,
    original_filename: Optional[str],
    requested_filename: Optional[str],
) -> str:
    if requested_filename:
        return _safe_python_basename(requested_filename)
    if source_type == "upload" and original_filename:
        return _safe_python_basename(original_filename)
    if dag_id and dag_id != "Unknown":
        return _safe_python_basename(dag_id)
    return "pasted_dag.py"


def _ancestor_dir_ids(path: str, node_type: str) -> List[str]:
    parts = [part for part in path.split("/") if part]
    directory_parts = parts[:-1] if node_type == "file" else parts[:-1]
    return [
        "dir:" + "/".join(directory_parts[:index])
        for index in range(1, len(directory_parts) + 1)
    ]


def _containing_dir_ids(path: str) -> List[str]:
    parts = [part for part in path.split("/") if part][:-1]
    return [
        "dir:" + "/".join(parts[:index])
        for index in range(1, len(parts) + 1)
    ]


def _visible_dag_picker_rows(nodes: List[Dict], expanded_dirs: Set[str], search: str, selected_node: Optional[str]) -> List[Dict]:
    query = search.strip().lower()
    matching_file_ids = set()
    visible_dir_ids = set()
    if query:
        for node in nodes:
            if node.get("type") == "file" and query in node.get("name", "").lower():
                matching_file_ids.add(node["id"])
                visible_dir_ids.update(_containing_dir_ids(node.get("path", "")))

    visible_rows = []
    for node in nodes:
        node_type = node.get("type")
        node_id = node.get("id")
        if query:
            visible = node_id in matching_file_ids if node_type == "file" else node_id in visible_dir_ids
        else:
            visible = all(ancestor in expanded_dirs for ancestor in _ancestor_dir_ids(node.get("path", ""), node_type))
        if not visible:
            continue

        row = dict(node)
        row["expanded"] = bool(query) or node_id in expanded_dirs
        row["icon"] = "folder_open" if row["type"] == "dir" and row["expanded"] else "folder" if row["type"] == "dir" else "description"
        row["is_selected"] = node_id == selected_node
        visible_rows.append(row)
    return visible_rows


def create_ui():
    project_root = ROOT_DIR
    mapping_file = Path(os.environ.get("AF_DAGS_HELPER_MAPPING_FILE", project_root / "config" / "server_mapping.yaml"))
    runtime_dir = Path(os.environ.get("AF_DAGS_HELPER_RUNTIME_DIR", project_root / ".runtime" / "uploads"))
    service = DAGAnalysisService(project_root, mapping_file, runtime_dir=runtime_dir)
    repos_root = Path(os.environ.get("AF_DAGS_HELPER_REPOS_DIR", Path.home() / "repos"))
    repository_registry = Path(os.environ.get(
        "AF_DAGS_HELPER_REPOSITORY_REGISTRY",
        runtime_dir.parent / "repositories.json",
    ))
    repository_browser = RepositoryBrowser(repos_root, repository_registry)
    state = WebState()
    generated_clipboard_key = "generated_omentity"

    ui.add_head_html(
        """
        <style>
          .nicegui-content {
            max-width: none;
            height: calc(100vh - 56px);
            overflow: hidden;
          }
          .web-main {
            height: calc(100vh - 56px);
            min-height: 0;
            overflow: hidden;
          }
          .source-drawer {
            width: 360px;
          }
          .source-drawer-toggle-btn {
            position: fixed;
            left: 360px;
            top: 50%;
            transform: translateY(-50%);
            width: 32px;
            min-width: 32px;
            height: 96px;
            padding: 0;
            border-radius: 0 8px 8px 0;
            background: var(--q-primary) !important;
            color: white !important;
            border: 1px solid rgba(255, 255, 255, 0.35);
            border-left: 0;
            box-shadow: 0 10px 22px rgba(15, 23, 42, 0.28);
            clip-path: polygon(0 0, 100% 14%, 100% 86%, 0 100%);
            cursor: pointer;
            z-index: 3001;
            transition: left 180ms ease, background-color 180ms ease, box-shadow 180ms ease;
          }
          .source-drawer-toggle-btn.drawer-closed {
            left: 0;
          }
          .source-drawer-toggle-btn:hover {
            background: var(--q-primary) !important;
            filter: brightness(0.92);
            box-shadow: 0 12px 26px rgba(15, 23, 42, 0.34);
          }
          .source-drawer-toggle-btn .q-icon {
            transition: transform 180ms ease;
          }
          .source-code-pane {
            height: 100%;
            min-height: 0;
            overflow: hidden;
          }
          .source-code-editor {
            flex: 1 1 auto;
            height: auto;
            min-height: 0;
          }
          .source-code-editor .cm-editor {
            height: 100%;
            min-height: 0;
          }
          .source-code-editor .cm-scroller {
            overflow: auto;
          }
          .result-pane {
            height: 100%;
            min-height: 0;
            overflow: hidden;
          }
          .result-panels {
            flex: 1 1 auto;
            min-height: 0;
            overflow: hidden;
          }
          .result-tab-panel {
            height: 100%;
            min-height: 0;
            overflow: hidden;
            padding: 8px 0 0;
            display: flex;
            flex-direction: column;
            gap: 8px;
          }
          .generated-actions,
          .diagram-actions {
            flex: 0 0 auto;
          }
          .result-editor {
            flex: 1 1 auto;
            height: auto;
            min-height: 0;
          }
          .result-editor .cm-editor {
            height: 100%;
            min-height: 0;
          }
          .result-editor .cm-scroller {
            overflow: auto;
          }
          .diagram-container {
            flex: 1 1 auto;
            height: auto;
            min-height: 0;
            overflow: hidden;
          }
          .diagram-container iframe {
            width: 100%;
            height: 100%;
            border: 1px solid #ddd;
            border-radius: 6px;
          }
          .dag-picker-card {
            width: min(1100px, 96vw);
            max-width: 96vw;
          }
          .dag-picker-table {
            height: 64vh;
            border: 1px solid #ddd;
            border-radius: 6px;
          }
          .dag-picker-table .q-table__middle {
            max-height: 64vh;
          }
          .dag-picker-table .q-table tbody td {
            height: 30px;
            padding: 0 8px;
            font-size: 13px;
          }
          .dag-picker-table .q-table thead th {
            height: 32px;
            padding: 0 8px;
            font-size: 12px;
          }
          .dag-picker-table tbody tr {
            cursor: pointer;
          }
          .dag-name-content {
            display: flex;
            align-items: center;
            gap: 6px;
            min-width: 0;
          }
          .dag-name-text,
          .dag-meta-text {
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
          }
          .dag-chevron-spacer {
            width: 16px;
            flex: 0 0 16px;
          }
          .dag-selected-name {
            color: #0d47a1;
            font-weight: 600;
          }
          .dag-muted-cell {
            color: #667085;
          }
        </style>
        """
    )
    install_clipboard_script()

    def registered_repo_names():
        return repository_browser.registered_repositories()

    def addable_repo_names():
        registered = set(registered_repo_names())
        return [name for name in repository_browser.discover_repositories() if name not in registered]

    def clear_analysis_context():
        state.last_analysis_context = None
        try:
            report_issue_btn.disable()
        except NameError:
            pass

    def clear_selected_dag():
        clear_analysis_context()
        state.selected_dag_node = None
        state.picker_dir = ""
        state.picker_nodes = []
        state.picker_expanded_dirs = set()
        state.picker_search = ""
        state.picker_selected_node = None
        selected_dag_label.set_text("No DAG selected")

    def refresh_selected_repo():
        state.active_source_tab = "repo"
        selected_repo = repo_select.value
        state.selected_repo = selected_repo
        clear_selected_dag()

    def refresh_repo_controls(preferred_repo: Optional[str] = None):
        registered = registered_repo_names()
        selected_repo = preferred_repo if preferred_repo in registered else repo_select.value
        if selected_repo not in registered:
            selected_repo = registered[0] if registered else None
        repo_select.set_options(registered, value=selected_repo)
        settings_registered_select.set_options(registered, value=selected_repo)
        addable = addable_repo_names()
        settings_discovered_select.set_options(addable, value=addable[0] if addable else None)
        refresh_selected_repo()

    def on_repo_change(_event=None):
        state.active_source_tab = "repo"
        refresh_selected_repo()

    def load_dag_picker_index(refresh: bool = False):
        selected_repo = repo_select.value
        if not selected_repo:
            state.picker_nodes = []
            state.picker_expanded_dirs = set()
            return
        state.picker_nodes = repository_browser.build_dag_index(selected_repo, refresh=refresh)
        state.picker_expanded_dirs = {
            node["id"] for node in state.picker_nodes
            if node["type"] == "dir"
        }
        if state.picker_selected_node:
            selected_node = next((node for node in state.picker_nodes if node["id"] == state.picker_selected_node), None)
            if selected_node:
                state.picker_expanded_dirs.update(_containing_dir_ids(selected_node["path"]))

    def render_dag_picker():
        rows = _visible_dag_picker_rows(
            state.picker_nodes,
            state.picker_expanded_dirs,
            state.picker_search,
            state.picker_selected_node,
        )
        dag_table.update_rows(rows, clear_selection=False)
        dag_table.update()
        total_dags = sum(1 for node in state.picker_nodes if node["type"] == "file")
        visible_dags = sum(1 for row in rows if row["type"] == "file")
        if state.picker_search:
            picker_count_label.set_text(f"{visible_dags} of {total_dags} DAGs")
        else:
            picker_count_label.set_text(f"{total_dags} DAGs")
        if state.picker_selected_node:
            picker_select_btn.enable()
        else:
            picker_select_btn.disable()

    def on_picker_search(event):
        state.picker_search = event.value or ""
        render_dag_picker()

    def toggle_picker_directory(node_id: str):
        if state.picker_search:
            return
        if node_id in state.picker_expanded_dirs:
            state.picker_expanded_dirs.remove(node_id)
        else:
            state.picker_expanded_dirs.add(node_id)
        render_dag_picker()

    def select_picker_row(row: Dict):
        if row.get("type") != "file":
            return
        state.picker_selected_node = row["id"]
        render_dag_picker()

    def confirm_picker_selection():
        if not state.picker_selected_node:
            ui.notify("Select a .py DAG first", type="warning")
            return
        selected = next((node for node in state.picker_nodes if node["id"] == state.picker_selected_node), None)
        if not selected or selected.get("type") != "file":
            ui.notify("Select a .py DAG first", type="warning")
            return
        clear_analysis_context()
        state.selected_repo = repo_select.value
        state.selected_dag_node = selected["node_id"]
        selected_dag_label.set_text(f"Selected DAG: {selected['path']}")
        preview_source_path(repository_browser.resolve_dag_path(repo_select.value, selected["node_id"]))
        dag_picker_dialog.close()

    def picker_event_row(event) -> Dict:
        row = event.args
        if isinstance(row, list) and row:
            row = row[0]
        return row if isinstance(row, dict) else {}

    def handle_picker_row_click(event):
        row = picker_event_row(event)
        if row.get("type") == "dir":
            toggle_picker_directory(row["id"])
            return
        select_picker_row(row)

    def handle_picker_row_double_click(event):
        row = picker_event_row(event)
        if row.get("type") == "dir":
            toggle_picker_directory(row["id"])
            return
        select_picker_row(row)
        confirm_picker_selection()

    def refresh_dag_picker():
        try:
            load_dag_picker_index(refresh=True)
            render_dag_picker()
            ui.notify("DAG list refreshed", type="positive")
        except Exception as exc:
            ui.notify(f"Failed to refresh DAG list: {exc}", type="negative", close_button=True)

    def open_dag_picker():
        state.active_source_tab = "repo"
        if not repo_select.value:
            ui.notify("Select a repository first", type="warning")
            return
        try:
            state.picker_search = ""
            picker_search_input.set_value("")
            picker_repo_label.set_text(f"Repository: {repo_select.value}")
            state.picker_selected_node = state.selected_dag_node if state.selected_repo == repo_select.value else None
            load_dag_picker_index()
            render_dag_picker()
            dag_picker_dialog.open()
        except Exception as exc:
            ui.notify(f"Failed to load DAG list: {exc}", type="negative", close_button=True)

    def add_selected_repository():
        repo_name = settings_discovered_select.value
        if not repo_name:
            ui.notify("No repository folder selected", type="warning")
            return
        try:
            repository_browser.add_repository(repo_name)
            refresh_repo_controls(preferred_repo=repo_name)
            settings_status.set_content(f"Registered `{repo_name}`.")
            ui.notify(f"Repository registered: {repo_name}", type="positive")
        except Exception as exc:
            ui.notify(f"Failed to add repository: {exc}", type="negative", close_button=True)

    def remove_selected_repository():
        repo_name = settings_registered_select.value
        if not repo_name:
            ui.notify("No registered repository selected", type="warning")
            return
        repository_browser.remove_repository(repo_name)
        refresh_repo_controls()
        settings_status.set_content(f"Removed `{repo_name}` from the web UI registry.")
        ui.notify(f"Repository removed: {repo_name}", type="positive")

    def pull_repository_from_settings():
        repo_name = settings_registered_select.value
        if not repo_name:
            ui.notify("No registered repository selected", type="warning")
            return
        pull_repository(repo_name)

    def pull_current_repository():
        repo_name = repo_select.value
        if not repo_name:
            ui.notify("No repository selected", type="warning")
            return
        pull_repository(repo_name)

    def pull_repository(repo_name: str):
        try:
            output = repository_browser.pull_repository(repo_name) or "Already up to date."
            refresh_repo_controls(preferred_repo=repo_name)
            settings_status.set_content(f"**{repo_name}:**\n\n```text\n{output}\n```")
            ui.notify(f"Git pull complete: {repo_name}", type="positive")
        except Exception as exc:
            ui.notify(f"Git pull failed: {exc}", type="negative", close_button=True)

    def pull_all_repositories():
        try:
            results = repository_browser.pull_all()
            refresh_repo_controls(preferred_repo=repo_select.value)
            if not results:
                settings_status.set_content("No registered repositories.")
                ui.notify("No registered repositories", type="warning")
                return
            lines = [f"{name}: {output or 'Already up to date.'}" for name, output in results.items()]
            settings_status.set_content("```text\n" + "\n".join(lines) + "\n```")
            ui.notify("Git pull complete for all repositories", type="positive")
        except Exception as exc:
            ui.notify(f"Git pull all failed: {exc}", type="negative", close_button=True)

    def set_source_preview(title: str, source: str):
        source_preview_label.set_text(title)
        source_preview.set_value(source)

    def preview_source_path(path: Path):
        try:
            set_source_preview(path.name, path.read_text(encoding="utf-8"))
        except Exception as exc:
            set_source_preview(path.name, f"# Failed to load source preview: {exc}")

    def preview_paste_source(_event=None):
        clear_analysis_context()
        state.active_source_tab = "paste"
        source = paste_area.value or ""
        title = "Pasted DAG source" if source else "Select a DAG to preview source"
        set_source_preview(title, source)

    def set_drawer_toggle_class(class_name: str, enabled: bool):
        if enabled and class_name not in source_drawer_toggle_btn.classes:
            source_drawer_toggle_btn.classes.append(class_name)
        if not enabled and class_name in source_drawer_toggle_btn.classes:
            source_drawer_toggle_btn.classes.remove(class_name)

    def sync_source_drawer_toggle(_event=None):
        opened = bool(source_drawer.value)
        set_drawer_toggle_class("drawer-closed", not opened)
        source_drawer_toggle_btn.props["icon"] = "chevron_left" if opened else "chevron_right"
        source_drawer_toggle_btn.update()

    def toggle_source_drawer():
        source_drawer.set_value(not bool(source_drawer.value))
        sync_source_drawer_toggle()

    def close_source_drawer():
        source_drawer.set_value(False)
        sync_source_drawer_toggle()

    with ui.dialog() as help_dialog, ui.card().classes("max-w-[720px]"):
        ui.label("Справка по AF DAGs Helper").classes("text-h6")
        ui.markdown(
            """
            **Что делает сервис:** анализирует Python DAG Airflow, находит SQL/API lineage и генерирует `OMEntity`
            для `inlets` и `outlets`.

            **Workflow:** откройте выезжающее Source drawer кнопкой меню в header или боковой кнопкой
            `source drawer toggle handle`. В drawer выберите источник: Repo, Upload или Paste. Для Repo сначала
            выберите зарегистрированный репозиторий, затем нажмите Browse DAG и выберите `.py` DAG в компактной
            таблице с раскрываемыми папками, поиском и колонками последнего коммита. После выбора исходный код
            появится в блоке DAG Source. При необходимости включите Force all tasks для пересчета всех задач и
            Compare existing OMEntity для сравнения с уже прописанными сущностями. Затем нажмите Analyze; после
            запуска Analyze drawer закрывается автоматически.

            **Результаты:** вкладка Generated OMEntity содержит готовый код и кнопки Copy/Save/Report issue.
            Copy копирует текущий сгенерированный OMEntity в буфер обмена, Save скачивает его в файл.
            Difference показывает отличие от существующего `OMEntity`. Text Diagram дает текстовую схему lineage.
            Interactive Diagram показывает граф, где DAG view группирует результат по DAG, а Task view фокусируется
            на связях задач. Warnings содержит предупреждения парсера и подсказки по неоднозначным местам.

            **Обратная связь:** кнопка с иконкой сообщения в header открывает форму общих пожеланий по сервису.
            На вкладке Generated OMEntity кнопка Report issue сохраняет замечание по текущему анализу, снимок DAG,
            сгенерированный OMEntity, Difference, предупреждения и метаданные анализа. Для DAG из
            репозитория также сохраняются имя repo, путь к файлу и commit, который был выбран при анализе.

            **Элементы интерфейса:** кнопка с тремя полосками в header и трапециевидный source drawer toggle handle
            управляют Source drawer; стрелка показывает, в какую сторону откроется или закроется меню. Левая часть
            рабочей области - DAG Source с readonly-просмотром кода выбранного DAG, правая часть - результаты анализа.
            Кнопка Settings в header открывает управление репозиториями: add/remove и git pull. Переключатель DAG view
            / Task view находится на вкладке Interactive Diagram и меняет вид текущей диаграммы без повторного запуска
            Analyze.
            """
        )
        with ui.row().classes("w-full justify-end"):
            ui.button("Закрыть", on_click=help_dialog.close)

    with ui.dialog() as settings_dialog, ui.card().classes("max-w-[760px] w-full"):
        ui.label("Repository settings").classes("text-h6")
        ui.markdown(
            f"""
            Repositories root: `{repos_root}`

            Add existing git folders from this directory to make them available in the Repo tab.
            """
        )
        settings_registered_select = ui.select([], label="Registered repository").classes("w-full")
        settings_discovered_select = ui.select([], label="Folder in repos").classes("w-full")
        with ui.row().classes("w-full"):
            ui.button("Refresh", icon="refresh", on_click=lambda: refresh_repo_controls())
            ui.button("Add", icon="add", on_click=add_selected_repository)
            ui.button("Remove", icon="delete", on_click=remove_selected_repository)
        with ui.row().classes("w-full"):
            ui.button("Git pull selected", icon="download", on_click=pull_repository_from_settings)
            ui.button("Git pull all", icon="sync", on_click=pull_all_repositories)
        settings_status = ui.markdown("").classes("w-full")
        with ui.row().classes("w-full justify-end"):
            ui.button("Close", on_click=settings_dialog.close)

    def submit_global_feedback():
        message = global_feedback_text.value or ""
        try:
            feedback_store_from_env().create_global_feedback(message)
        except ValueError as exc:
            ui.notify(str(exc), type="warning")
            return
        except Exception as exc:
            ui.notify(f"Failed to save feedback: {exc}", type="negative", close_button=True)
            return
        global_feedback_text.set_value("")
        global_feedback_dialog.close()
        ui.notify("Feedback saved", type="positive")

    def analysis_issue_needs_filename(context: Optional[AnalysisFeedbackContext]) -> bool:
        return bool(
            context
            and context.source_type == "paste"
            and not context.source_filename
            and (not context.dag_id or context.dag_id == "Unknown")
        )

    def open_analysis_issue_dialog():
        context = state.last_analysis_context
        if not context:
            ui.notify("Run Analyze before reporting an issue", type="warning")
            return
        issue_feedback_text.set_value("")
        issue_filename_input.set_value("")
        issue_filename_input.set_visibility(analysis_issue_needs_filename(context))
        analysis_issue_dialog.open()

    def submit_analysis_issue_feedback():
        context = state.last_analysis_context
        if not context:
            ui.notify("Run Analyze before reporting an issue", type="warning")
            return
        message = issue_feedback_text.value or ""
        source_filename = context.source_filename
        if analysis_issue_needs_filename(context):
            requested_filename = issue_filename_input.value or ""
            if not requested_filename.strip():
                ui.notify("DAG filename is required for pasted source without dag_id", type="warning")
                return
            source_filename = _feedback_source_filename(
                context.source_type,
                context.dag_id,
                context.original_filename,
                requested_filename,
            )
        try:
            feedback_store_from_env().create_analysis_issue_feedback(
                message,
                replace(context, source_filename=source_filename),
            )
        except ValueError as exc:
            ui.notify(str(exc), type="warning")
            return
        except Exception as exc:
            ui.notify(f"Failed to save issue feedback: {exc}", type="negative", close_button=True)
            return
        issue_feedback_text.set_value("")
        issue_filename_input.set_value("")
        analysis_issue_dialog.close()
        ui.notify("Issue feedback saved", type="positive")

    with ui.dialog() as global_feedback_dialog, ui.card().classes("max-w-[640px] w-full"):
        ui.label("Обратная связь").classes("text-h6")
        global_feedback_text = ui.textarea(
            label="Feedback",
            placeholder="Идеи, пожелания или замечания по сервису",
        ).classes("w-full").props("rows=7")
        with ui.row().classes("w-full justify-end"):
            ui.button("Send", icon="send", on_click=submit_global_feedback)
            ui.button("Cancel", on_click=global_feedback_dialog.close)

    with ui.dialog() as analysis_issue_dialog, ui.card().classes("max-w-[680px] w-full"):
        ui.label("Report analysis issue").classes("text-h6")
        ui.markdown("Опишите, что автоматический анализ распознал неверно.")
        issue_feedback_text = ui.textarea(
            label="What was recognized incorrectly?",
        ).classes("w-full").props("rows=7")
        issue_filename_input = ui.input(
            label="DAG filename",
            placeholder="daily_sales.py",
        ).classes("w-full")
        issue_filename_input.set_visibility(False)
        with ui.row().classes("w-full justify-end"):
            ui.button("Send issue", icon="send", on_click=submit_analysis_issue_feedback)
            ui.button("Cancel", on_click=analysis_issue_dialog.close)

    dag_picker_columns = [
        {"name": "name", "label": "Name", "field": "name", "align": "left", "sortable": True},
        {"name": "mtime_display", "label": "Commit date", "field": "mtime_display", "align": "left", "sortable": True},
        {"name": "git_author", "label": "Author", "field": "git_author", "align": "left", "sortable": True},
        {"name": "git_message_short", "label": "Commit", "field": "git_message_short", "align": "left", "sortable": True},
    ]
    with ui.dialog() as dag_picker_dialog, ui.card().classes("dag-picker-card"):
        with ui.row().classes("w-full items-center"):
            ui.label("Select DAG").classes("text-h6")
            picker_repo_label = ui.label("").classes("text-caption text-grey-7")
            ui.space()
            ui.button(icon="refresh", on_click=refresh_dag_picker).props("flat round dense").tooltip("Refresh DAG list")
        with ui.row().classes("w-full items-center"):
            picker_search_input = ui.input(
                placeholder="Search DAG filename...",
                on_change=on_picker_search,
            ).props("dense outlined clearable debounce=300").classes("grow")
            picker_count_label = ui.label("0 DAGs").classes("text-caption text-grey-7")
        dag_table = ui.table(
            rows=[],
            columns=dag_picker_columns,
            row_key="id",
            pagination=0,
        ).props("dense flat virtual-scroll hide-pagination").classes("dag-picker-table w-full")
        dag_table.add_slot(
            "body-cell-name",
            """
            <q-td :props="props">
              <div class="dag-name-content" :style="{ paddingLeft: `${props.row.level * 18}px` }" :title="props.row.path">
                <q-icon
                  v-if="props.row.type === 'dir'"
                  :name="props.row.expanded ? 'keyboard_arrow_down' : 'keyboard_arrow_right'"
                  size="16px"
                />
                <span v-else class="dag-chevron-spacer"></span>
                <q-icon
                  :name="props.row.icon"
                  size="16px"
                  :class="props.row.type === 'dir' ? 'text-amber-8' : 'text-blue-grey-7'"
                />
                <span class="dag-name-text" :class="props.row.is_selected ? 'dag-selected-name' : ''">
                  {{ props.row.name }}
                </span>
              </div>
            </q-td>
            """,
        )
        dag_table.add_slot(
            "body-cell-mtime_display",
            '<q-td :props="props" class="dag-muted-cell"><span class="dag-meta-text">{{ props.value }}</span></q-td>',
        )
        dag_table.add_slot(
            "body-cell-git_author",
            '<q-td :props="props" class="dag-muted-cell"><span class="dag-meta-text" :title="props.value">{{ props.value }}</span></q-td>',
        )
        dag_table.add_slot(
            "body-cell-git_message_short",
            '<q-td :props="props" class="dag-muted-cell"><span class="dag-meta-text" :title="props.row.git_message">{{ props.value }}</span></q-td>',
        )
        dag_table.on("row-click", handle_picker_row_click, js_handler="(_, row) => emit(row)")
        dag_table.on("row-dblclick", handle_picker_row_double_click, js_handler="(_, row) => emit(row)")
        with ui.row().classes("w-full justify-end"):
            picker_select_btn = ui.button("Select", icon="check", on_click=confirm_picker_selection)
            picker_select_btn.disable()
            ui.button("Cancel", on_click=dag_picker_dialog.close)

    with ui.left_drawer(value=False, elevated=True).props("overlay width=360").classes("source-drawer") as source_drawer:
        ui.label("Source").classes("text-h6")
        def on_source_tab_change(event):
            if isinstance(event.value, str) and event.value in {"repo", "upload", "paste"}:
                if state.active_source_tab != event.value:
                    clear_analysis_context()
                state.active_source_tab = event.value

        with ui.tabs(value=state.active_source_tab, on_change=on_source_tab_change).classes("w-full") as source_tabs:
            repo_tab = ui.tab("repo", label="Repo")
            upload_tab = ui.tab("upload", label="Upload")
            paste_tab = ui.tab("paste", label="Paste")

        async def on_upload(event):
            clear_analysis_context()
            state.active_source_tab = "upload"
            state.uploaded_name, state.uploaded_source = await _read_upload_event_source(event)
            state.uploaded_original_filename = event.file.name
            upload_label.set_text(f"Uploaded: {event.file.name}")
            set_source_preview(event.file.name, state.uploaded_source)

        with ui.tab_panels(source_tabs, value=state.active_source_tab).classes("w-full"):
            with ui.tab_panel("repo"):
                ui.label("Choose a DAG from registered repositories.")
                repo_select = ui.select([], label="Repository", on_change=on_repo_change).classes("w-full")
                selected_dag_label = ui.label("No DAG selected")
                with ui.row().classes("w-full"):
                    ui.button("Browse DAG...", icon="folder_open", on_click=open_dag_picker)
                    ui.button("Refresh", icon="refresh", on_click=lambda: refresh_repo_controls())
                    ui.button("Git pull", icon="download", on_click=pull_current_repository)
            with ui.tab_panel("upload"):
                ui.upload(on_upload=on_upload, auto_upload=True).props("accept=.py").classes("w-full")
                upload_label = ui.label("No file uploaded")
            with ui.tab_panel("paste"):
                paste_area = ui.textarea(
                    label="Paste DAG source",
                    on_change=preview_paste_source,
                ).classes("w-full").props("rows=12")

        force = ui.checkbox("Force all tasks", value=True)
        compare = ui.checkbox("Compare existing OMEntity", value=True)

        with ui.row().classes("w-full"):
            analyze_btn = ui.button("Analyze", icon="play_arrow").classes("grow")

    with ui.header().classes("items-center"):
        ui.button(icon="menu", on_click=toggle_source_drawer).props("flat round dense text-color=white").tooltip("Source menu")
        ui.label("AF DAGs Helper").classes("text-h6")
        ui.button(icon="help_outline", on_click=help_dialog.open).props("flat round dense text-color=white").tooltip("Справка")
        ui.button(icon="rate_review", on_click=global_feedback_dialog.open).props("flat round dense text-color=white").tooltip("Обратная связь")
        ui.button(icon="settings", on_click=settings_dialog.open).props("flat round dense text-color=white").tooltip("Settings")
        ui.space()

    source_drawer_toggle_btn = ui.button(icon="chevron_right", on_click=toggle_source_drawer).props(
        "flat dense text-color=white"
    ).classes("source-drawer-toggle-btn source-drawer-toggle-handle drawer-closed").tooltip("Source drawer toggle handle")
    source_drawer.on_value_change(sync_source_drawer_toggle)

    with ui.row().classes("web-main w-full no-wrap items-stretch q-pa-md gap-4 overflow-hidden"):
        with ui.card().classes("source-code-pane w-2/5 min-w-[420px] min-h-0"):
            ui.label("DAG Source").classes("text-h6")
            source_preview_label = ui.label("Select a DAG to preview source").classes("text-caption text-grey-7")
            source_preview = ui.codemirror("", language="Python").props("readonly").classes("w-full source-code-editor")

        with ui.column().classes("result-pane w-3/5 min-h-0 overflow-hidden"):
            summary = ui.markdown("Select a source and run analysis.")
            with ui.tabs().classes("w-full") as result_tabs:
                generated_tab = ui.tab("Generated OMEntity")
                diff_tab = ui.tab("Difference")
                text_diagram_tab = ui.tab("Text Diagram")
                graph_tab = ui.tab("Interactive Diagram")
                warnings_tab = ui.tab("Warnings")

            with ui.tab_panels(result_tabs, value=generated_tab).classes("result-panels w-full"):
                with ui.tab_panel(generated_tab).classes("result-tab-panel"):
                    with ui.row().classes("generated-actions w-full justify-end"):
                        copy_btn = ui.button("Copy", icon="content_copy")
                        download_btn = ui.button("Save", icon="download")
                        report_issue_btn = ui.button("Report issue", icon="report_problem", on_click=open_analysis_issue_dialog)
                        report_issue_btn.disable()
                    generated = ui.codemirror("", language="Python").classes("w-full result-editor")
                with ui.tab_panel(diff_tab).classes("result-tab-panel"):
                    diff = ui.codemirror("", language="Markdown").classes("w-full result-editor")
                with ui.tab_panel(text_diagram_tab).classes("result-tab-panel"):
                    text_diagram = ui.codemirror("", language="Markdown").classes("w-full result-editor")
                with ui.tab_panel(graph_tab).classes("result-tab-panel"):
                    def on_diagram_view_change(_event=None):
                        render_diagram()

                    with ui.row().classes("diagram-actions w-full justify-end"):
                        diagram_view = ui.toggle(
                            {"dag": "DAG view", "task": "Task view"},
                            value="dag",
                            on_change=on_diagram_view_change,
                        ).props("dense")
                    diagram_html = ui.html("", sanitize=False).classes("diagram-container w-full")
                with ui.tab_panel(warnings_tab).classes("result-tab-panel"):
                    warnings = ui.codemirror("", language="Markdown").classes("w-full result-editor")

    def current_source_type() -> str:
        return state.active_source_tab or _source_tab_name(source_tabs.value, repo_tab, upload_tab, paste_tab)

    def resolve_current_dag_path() -> Path:
        active_tab = current_source_type()
        return _resolve_current_source_path(
            active_tab=active_tab,
            repo_name=repo_select.value,
            selected_dag_node=state.selected_dag_node,
            uploaded_source=state.uploaded_source,
            uploaded_name=state.uploaded_name,
            pasted_source=paste_area.value or "",
            repository_browser=repository_browser,
            service=service,
        )

    def render_diagram():
        if not state.graph_data:
            diagram_html.set_content("<p>No graph data available.</p>")
            return
        graph_data = dict(state.graph_data)
        graph_data["initial_view"] = diagram_view.value
        html = CytoscapeViewer()._build_html(graph_data, f"Lineage: {state.dag_id or 'Unknown'}")
        srcdoc = html_lib.escape(html, quote=True)
        diagram_html.set_content(f'<iframe srcdoc="{srcdoc}"></iframe>')

    def analyze():
        try:
            clear_analysis_context()
            active_tab = current_source_type()
            dag_path = resolve_current_dag_path()
            source_text = dag_path.read_text(encoding="utf-8")
            repo_name = repo_select.value if active_tab == "repo" else None
            repo_path = state.selected_dag_node.removeprefix("file:") if active_tab == "repo" and state.selected_dag_node else None
            repo_commit = repository_browser.repo_head_revision(repo_name) if repo_name else None
            original_filename = state.uploaded_original_filename if active_tab == "upload" else None
            close_source_drawer()
            result = service.analyze(DAGAnalysisRequest(
                dag_path=dag_path,
                force_all_tasks=force.value,
                compare_existing=compare.value,
                initial_view=diagram_view.value,
            ))
            state.generated_text = result.generated_text
            set_clipboard_text(generated_clipboard_key, result.generated_text)
            summary.set_content(
                f"**DAG:** `{result.dag_id}`  \n"
                f"**Tasks:** {result.task_count}  \n"
                f"**Outputs:** {result.output_count}"
            )
            generated.set_value(result.generated_text)
            diff.set_value(result.difference_text)
            text_diagram.set_value(result.text_diagram)
            state.graph_data = result.graph_data
            state.dag_id = result.dag_id
            render_diagram()
            warnings_text = "\n".join(result.warnings) if result.warnings else "No warnings"
            warnings.set_value(warnings_text)
            source_filename = None
            if active_tab == "repo" and repo_path:
                source_filename = Path(repo_path).name
            elif active_tab == "upload":
                source_filename = _feedback_source_filename(active_tab, result.dag_id, original_filename, None)
            elif active_tab == "paste" and result.dag_id and result.dag_id != "Unknown":
                source_filename = _feedback_source_filename(active_tab, result.dag_id, None, None)
            state.last_analysis_context = AnalysisFeedbackContext(
                source_type=active_tab,
                dag_id=result.dag_id,
                repo_name=repo_name,
                dag_path=repo_path,
                repo_commit=repo_commit,
                original_filename=original_filename,
                source_text=source_text,
                analysis_options={
                    "force_all_tasks": bool(force.value),
                    "compare_existing": bool(compare.value),
                    "initial_view": diagram_view.value,
                },
                analysis_summary={
                    "task_count": result.task_count,
                    "output_count": result.output_count,
                    "warnings_count": len(result.warnings),
                },
                generated_text=result.generated_text,
                difference_text=result.difference_text,
                warnings_text=warnings_text,
                source_filename=source_filename,
            )
            report_issue_btn.enable()
            ui.notify("Analysis complete", type="positive")
        except Exception as exc:
            ui.notify(f"Analysis failed: {exc}", type="negative", close_button=True)

    def download_generated():
        if not state.generated_text:
            ui.notify("Nothing to save", type="warning")
            return
        ui.download(state.generated_text.encode("utf-8"), filename="omentity_output.txt", media_type="text/plain")

    analyze_btn.on_click(analyze)
    bind_copy_button(
        copy_btn,
        key=generated_clipboard_key,
        success_message="Generated OMEntity copied",
        empty_message="Nothing to copy",
        failure_message="Clipboard copy failed",
    )
    download_btn.on_click(download_generated)
    refresh_repo_controls()


@ui.page("/")
def index():
    create_ui()


ui.run_with(app, title="AF DAGs Helper", language="ru")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("AF_DAGS_HELPER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("AF_DAGS_HELPER_PORT", "8000")))
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ in {"__main__", "__mp_main__"}:
    main()
