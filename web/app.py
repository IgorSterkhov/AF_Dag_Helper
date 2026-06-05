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
from web.server_files import ServerFileBrowser  # noqa: E402


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


def create_ui():
    project_root = ROOT_DIR
    mapping_file = Path(os.environ.get("AF_DAGS_HELPER_MAPPING_FILE", project_root / "config" / "server_mapping.yaml"))
    runtime_dir = Path(os.environ.get("AF_DAGS_HELPER_RUNTIME_DIR", project_root / ".runtime" / "uploads"))
    service = DAGAnalysisService(project_root, mapping_file, runtime_dir=runtime_dir)
    browser = ServerFileBrowser(project_root)
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
        </style>
        """
    )

    with ui.dialog() as help_dialog, ui.card().classes("max-w-[720px]"):
        ui.label("Справка по AF DAGs Helper").classes("text-h6")
        ui.markdown(
            """
            **Что делает сервис:** анализирует Python DAG Airflow, находит SQL/API lineage и генерирует `OMEntity`
            для `inlets` и `outlets`.

            **Workflow:** выберите исходник во вкладках Source: Server file, Upload или Paste. При необходимости
            включите Force all tasks для пересчета всех задач и Compare existing OMEntity для сравнения с уже
            прописанными сущностями. Затем нажмите Analyze.

            **Результаты:** вкладка Generated OMEntity содержит готовый код и кнопки Copy/Save. Difference показывает
            отличие от существующего `OMEntity`. Text Diagram дает текстовую схему lineage. Interactive Diagram
            показывает граф, где DAG view группирует результат по DAG, а Task view фокусируется на связях задач.
            Warnings содержит предупреждения парсера и подсказки по неоднозначным местам.

            **Элементы интерфейса:** левая панель отвечает за выбор источника и параметры анализа. Правая панель
            отвечает за просмотр результата. Переключатель DAG view / Task view находится на вкладке Interactive
            Diagram и меняет вид текущей диаграммы без повторного запуска Analyze.
            """
        )
        with ui.row().classes("w-full justify-end"):
            ui.button("Закрыть", on_click=help_dialog.close)

    with ui.header().classes("items-center"):
        ui.label("AF DAGs Helper").classes("text-h6")
        ui.button(icon="help_outline", on_click=help_dialog.open).props("flat round dense").tooltip("Справка")
        ui.space()

    with ui.row().classes("web-main w-full no-wrap items-stretch q-pa-md gap-4 overflow-hidden"):
        with ui.card().classes("source-pane w-1/3 min-w-[360px]"):
            ui.label("Source").classes("text-h6")
            with ui.tabs().classes("w-full") as source_tabs:
                server_tab = ui.tab("server", label="Server file")
                upload_tab = ui.tab("upload", label="Upload")
                paste_tab = ui.tab("paste", label="Paste")

            server_files = browser.list_dag_files()

            def on_upload(event):
                content = event.content.read()
                state.uploaded_source = content.decode("utf-8")
                state.uploaded_name = Path(event.name).stem
                upload_label.set_text(f"Uploaded: {event.name}")

            with ui.tab_panels(source_tabs, value=server_tab).classes("w-full"):
                with ui.tab_panel(server_tab):
                    ui.label("Choose a DAG from allowed project folders.")
                    selected_file = ui.select(
                        server_files,
                        label="DAG file",
                        value=server_files[0] if server_files else None,
                    ).classes("w-full")
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
        active_tab = source_tabs.value
        if active_tab == "server":
            if not selected_file.value:
                raise ValueError("No server DAG file selected")
            return browser.resolve(selected_file.value)
        if active_tab == "upload":
            if not state.uploaded_source:
                raise ValueError("Upload a .py DAG first")
            return service.write_source_to_runtime_file(state.uploaded_name, state.uploaded_source)
        if not paste_area.value:
            raise ValueError("Paste DAG source first")
        return service.write_source_to_runtime_file("pasted_dag", paste_area.value)

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
