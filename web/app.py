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
from web.server_files import ServerFileBrowser  # noqa: E402


app = FastAPI(title="AF DAGs Helper")


@app.get("/health")
def health():
    return {"status": "ok", "service": "af-dags-helper"}


class WebState:
    """Small in-memory UI state for one NiceGUI process."""

    def __init__(self):
        self.uploaded_source: Optional[str] = None
        self.uploaded_name = "uploaded_dag"
        self.generated_text = ""


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
          .nicegui-content { max-width: none; }
          .result-editor .cm-editor { min-height: 62vh; }
        </style>
        """
    )

    with ui.header().classes("items-center"):
        ui.label("AF DAGs Helper").classes("text-h6")
        ui.space()
        ui.label("FastAPI + NiceGUI").classes("text-caption")

    with ui.row().classes("w-full no-wrap items-start q-pa-md"):
        with ui.card().classes("w-1/3 min-w-[360px]"):
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
            diagram_view = ui.toggle({"dag": "DAG view", "task": "Task view"}, value="dag")

            with ui.row().classes("w-full"):
                analyze_btn = ui.button("Analyze", icon="play_arrow").classes("grow")
                copy_btn = ui.button("Copy", icon="content_copy")
                download_btn = ui.button("Download", icon="download")

        with ui.column().classes("w-2/3"):
            summary = ui.markdown("Select a source and run analysis.")
            with ui.tabs().classes("w-full") as result_tabs:
                generated_tab = ui.tab("Generated OMEntity")
                diff_tab = ui.tab("Difference")
                text_diagram_tab = ui.tab("Text Diagram")
                graph_tab = ui.tab("Interactive Diagram")
                warnings_tab = ui.tab("Warnings")

            generated = ui.codemirror("", language="Python").classes("w-full result-editor")
            diff = ui.codemirror("", language="Markdown").classes("w-full result-editor")
            text_diagram = ui.codemirror("", language="Markdown").classes("w-full result-editor")
            diagram_html = ui.html("", sanitize=False).classes("w-full")
            warnings = ui.codemirror("", language="Markdown").classes("w-full result-editor")

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

    def copy_generated():
        if not state.generated_text:
            ui.notify("Nothing to copy", type="warning")
            return
        ui.run_javascript(f"navigator.clipboard.writeText({json.dumps(state.generated_text)})")
        ui.notify("Generated OMEntity copied", type="positive")

    def download_generated():
        if not state.generated_text:
            ui.notify("Nothing to download", type="warning")
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
