"""FastAPI + NiceGUI web interface for AF DAGs Helper."""

import argparse
import html as html_lib
import json
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

from visualizer.cytoscape_viewer import CytoscapeViewer  # noqa: E402
from web.analysis_service import DAGAnalysisRequest, DAGAnalysisService  # noqa: E402
from web.auth import BasicAuthMiddleware  # noqa: E402
from web.repository_browser import RepositoryBrowser  # noqa: E402


app = FastAPI(title="AF DAGs Helper")
app.add_middleware(BasicAuthMiddleware)


@app.get("/health")
def health():
    return {"status": "ok", "service": "af-dags-helper"}


class WebState:
    """Small in-memory UI state for one NiceGUI process."""

    def __init__(self):
        self.uploaded_source: Optional[str] = None
        self.uploaded_name = "uploaded_dag"
        self.generated_text = ""
        self.graph_data = {}
        self.dag_id = ""
        self.selected_repo: Optional[str] = None
        self.selected_dag_node: Optional[str] = None
        self.picker_dir = ""


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
          .source-pane {
            height: 100%;
            min-height: 0;
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
          .dag-picker-list {
            max-height: 58vh;
            overflow: auto;
            border: 1px solid #ddd;
            border-radius: 6px;
            padding: 8px;
          }
          .dag-picker-entry {
            width: 100%;
            justify-content: flex-start;
            text-align: left;
          }
        </style>
        """
    )

    def registered_repo_names():
        return repository_browser.registered_repositories()

    def addable_repo_names():
        registered = set(registered_repo_names())
        return [name for name in repository_browser.discover_repositories() if name not in registered]

    def clear_selected_dag():
        state.selected_dag_node = None
        state.picker_dir = ""
        selected_dag_label.set_text("No DAG selected")

    def refresh_selected_repo():
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
        refresh_selected_repo()

    def render_dag_picker():
        picker_list.clear()
        selected_repo = repo_select.value
        with picker_list:
            if not selected_repo:
                state.picker_dir = ""
                picker_path_label.set_text("Current folder: /")
                ui.label("Select a repository first.").classes("text-grey-7")
                return
            try:
                listing = repository_browser.list_directory(selected_repo, state.picker_dir)
            except Exception as exc:
                picker_path_label.set_text("Current folder: /")
                ui.label(f"Failed to load DAG folder: {exc}").classes("text-negative")
                return

            state.picker_dir = listing["current"]
            current_label = f"/{state.picker_dir}" if state.picker_dir else "/"
            picker_path_label.set_text(f"Current folder: {current_label}")

            if not listing["directories"] and not listing["files"]:
                ui.label("No folders or .py DAG files here.").classes("text-grey-7")

            for directory in listing["directories"]:
                ui.button(
                    directory["name"],
                    icon="folder",
                    on_click=lambda path=directory["path"]: navigate_dag_picker(path),
                ).props("flat align=left").classes("dag-picker-entry")

            for dag_file in listing["files"]:
                ui.button(
                    dag_file["name"],
                    icon="description",
                    on_click=lambda file=dag_file: select_dag_from_picker(file),
                ).props("flat align=left").classes("dag-picker-entry")

    def navigate_dag_picker(path: str):
        state.picker_dir = "" if path in {"", "."} else path.strip("/")
        render_dag_picker()

    def go_picker_up():
        if not state.picker_dir:
            return
        parent = Path(state.picker_dir).parent.as_posix()
        navigate_dag_picker("" if parent == "." else parent)

    def select_dag_from_picker(dag_file):
        state.selected_repo = repo_select.value
        state.selected_dag_node = dag_file["node_id"]
        selected_dag_label.set_text(f"Selected DAG: {dag_file['path']}")
        dag_picker_dialog.close()

    def open_dag_picker():
        if not repo_select.value:
            ui.notify("Select a repository first", type="warning")
            return
        if state.selected_dag_node and state.selected_repo == repo_select.value:
            selected_path = Path(state.selected_dag_node.removeprefix("file:"))
            parent = selected_path.parent.as_posix()
            state.picker_dir = "" if parent == "." else parent
        else:
            state.picker_dir = ""
        render_dag_picker()
        dag_picker_dialog.open()

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

    with ui.dialog() as help_dialog, ui.card().classes("max-w-[720px]"):
        ui.label("Справка по AF DAGs Helper").classes("text-h6")
        ui.markdown(
            """
            **Что делает сервис:** анализирует Python DAG Airflow, находит SQL/API lineage и генерирует `OMEntity`
            для `inlets` и `outlets`.

            **Workflow:** выберите исходник во вкладках Source: Repo, Upload или Paste. Во вкладке Repo сначала
            выберите зарегистрированный репозиторий, затем нажмите Browse DAG и выберите `.py` DAG в окне навигации. При
            необходимости включите Force all tasks для пересчета всех задач и Compare existing OMEntity для
            сравнения с уже прописанными сущностями. Затем нажмите Analyze.

            **Результаты:** вкладка Generated OMEntity содержит готовый код и кнопки Copy/Save. Difference показывает
            отличие от существующего `OMEntity`. Text Diagram дает текстовую схему lineage. Interactive Diagram
            показывает граф, где DAG view группирует результат по DAG, а Task view фокусируется на связях задач.
            Warnings содержит предупреждения парсера и подсказки по неоднозначным местам.

            **Элементы интерфейса:** левая панель отвечает за выбор источника и параметры анализа. Кнопка Settings
            в header открывает управление репозиториями: add/remove и git pull. Правая панель отвечает за просмотр
            результата. Переключатель DAG view / Task view находится на вкладке Interactive Diagram и меняет вид
            текущей диаграммы без повторного запуска Analyze.
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

    with ui.dialog() as dag_picker_dialog, ui.card().classes("max-w-[820px] w-full"):
        ui.label("Select DAG").classes("text-h6")
        with ui.row().classes("w-full items-center"):
            ui.button("Up", icon="arrow_upward", on_click=go_picker_up).props("flat")
            picker_path_label = ui.label("Current folder: /").classes("text-caption text-grey-7")
        picker_list = ui.column().classes("dag-picker-list w-full")
        with ui.row().classes("w-full justify-end"):
            ui.button("Cancel", on_click=dag_picker_dialog.close)

    with ui.header().classes("items-center"):
        ui.label("AF DAGs Helper").classes("text-h6")
        ui.button(icon="help_outline", on_click=help_dialog.open).props("flat round dense text-color=white").tooltip("Справка")
        ui.button(icon="settings", on_click=settings_dialog.open).props("flat round dense text-color=white").tooltip("Settings")
        ui.space()

    with ui.row().classes("web-main w-full no-wrap items-stretch q-pa-md gap-4 overflow-hidden"):
        with ui.card().classes("source-pane w-1/3 min-w-[360px]"):
            ui.label("Source").classes("text-h6")
            with ui.tabs().classes("w-full") as source_tabs:
                repo_tab = ui.tab("repo", label="Repo")
                upload_tab = ui.tab("upload", label="Upload")
                paste_tab = ui.tab("paste", label="Paste")

            def on_upload(event):
                content = event.content.read()
                state.uploaded_source = content.decode("utf-8")
                state.uploaded_name = Path(event.name).stem
                upload_label.set_text(f"Uploaded: {event.name}")

            with ui.tab_panels(source_tabs, value=repo_tab).classes("w-full"):
                with ui.tab_panel(repo_tab):
                    ui.label("Choose a DAG from registered repositories.")
                    repo_select = ui.select([], label="Repository", on_change=on_repo_change).classes("w-full")
                    selected_dag_label = ui.label("No DAG selected")
                    with ui.row().classes("w-full"):
                        ui.button("Browse DAG...", icon="folder_open", on_click=open_dag_picker)
                        ui.button("Refresh", icon="refresh", on_click=lambda: refresh_repo_controls())
                        ui.button("Git pull", icon="download", on_click=pull_current_repository)
                with ui.tab_panel(upload_tab):
                    ui.upload(on_upload=on_upload, auto_upload=True).props("accept=.py").classes("w-full")
                    upload_label = ui.label("No file uploaded")
                with ui.tab_panel(paste_tab):
                    paste_area = ui.textarea(label="Paste DAG source").classes("w-full").props("rows=12")

            force = ui.checkbox("Force all tasks", value=True)
            compare = ui.checkbox("Compare existing OMEntity", value=True)

            with ui.row().classes("w-full"):
                analyze_btn = ui.button("Analyze", icon="play_arrow").classes("grow")

        with ui.column().classes("result-pane w-2/3 min-h-0 overflow-hidden"):
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

    def resolve_current_dag_path() -> Path:
        active_tab = _source_tab_name(source_tabs.value, repo_tab, upload_tab, paste_tab)
        if active_tab == "repo":
            if not repo_select.value:
                raise ValueError("No repository selected")
            if not state.selected_dag_node:
                raise ValueError("No DAG selected")
            return repository_browser.resolve_dag_path(repo_select.value, state.selected_dag_node)
        if active_tab == "upload":
            if not state.uploaded_source:
                raise ValueError("Upload a .py DAG first")
            return service.write_source_to_runtime_file(state.uploaded_name, state.uploaded_source)
        if active_tab == "paste" and not paste_area.value:
            raise ValueError("Paste DAG source first")
        if active_tab == "paste":
            return service.write_source_to_runtime_file("pasted_dag", paste_area.value)
        raise ValueError("Select a source tab")

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
            dag_path = resolve_current_dag_path()
            result = service.analyze(DAGAnalysisRequest(
                dag_path=dag_path,
                force_all_tasks=force.value,
                compare_existing=compare.value,
                initial_view=diagram_view.value,
            ))
            state.generated_text = result.generated_text
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
            warnings.set_value("\n".join(result.warnings) if result.warnings else "No warnings")
            ui.notify("Analysis complete", type="positive")
        except Exception as exc:
            ui.notify(f"Analysis failed: {exc}", type="negative", close_button=True)

    def copy_generated():
        if not state.generated_text:
            ui.notify("Nothing to copy", type="warning")
            return
        ui.run_javascript(f"navigator.clipboard.writeText({json.dumps(state.generated_text)})")
        ui.notify("Generated OMEntity copied", type="positive")

    def download_generated():
        if not state.generated_text:
            ui.notify("Nothing to save", type="warning")
            return
        ui.download(state.generated_text.encode("utf-8"), filename="omentity_output.txt", media_type="text/plain")

    analyze_btn.on_click(analyze)
    copy_btn.on_click(copy_generated)
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
