import asyncio
import io
import json
import os
import re
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from web.app import app
from web.feedback_store import AnalysisFeedbackContext, FeedbackStore


def _nicegui_elements(html: str):
    match = re.search(r"parseElements\(String\.raw`(.+?)`\)", html, flags=re.DOTALL)
    if not match:
        raise AssertionError("NiceGUI element tree was not found in response")
    return json.loads(match.group(1))


def _descendants(elements, element):
    for child_id in element.get("children", []):
        child = elements[str(child_id)]
        yield child
        yield from _descendants(elements, child)


def _sample_feedback_context() -> AnalysisFeedbackContext:
    return AnalysisFeedbackContext(
        source_type="repo",
        dag_id="daily_sales",
        repo_name="analytics",
        dag_path="dags/daily_sales.py",
        repo_commit="abc123",
        original_filename=None,
        source_text="from airflow import DAG\n",
        analysis_options={"force_all_tasks": True, "compare_existing": True, "initial_view": "dag"},
        analysis_summary={"task_count": 2, "output_count": 1, "warnings_count": 0},
        generated_text="inlets=[]\noutlets=[]\n",
        difference_text="# MATCH\n",
    )


class WebAppTest(unittest.TestCase):
    def test_health_endpoint(self):
        client = TestClient(app)
        response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ok")

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_root_page_requires_authentication(self):
        client = TestClient(app)
        response = client.get("/")

        self.assertEqual(response.status_code, 401)
        self.assertIn("Basic", response.headers["www-authenticate"])

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_root_page_accepts_valid_credentials(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))

        self.assertEqual(response.status_code, 200)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_valid_credentials_create_session_cookie(self):
        client = TestClient(app)
        first_response = client.get("/", auth=("admin", "secret"))
        second_response = client.get("/")

        self.assertIn("af_dags_helper_auth", first_response.headers["set-cookie"])
        self.assertEqual(second_response.status_code, 200)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_feedback_api_routes_require_authentication(self):
        client = TestClient(app)

        self.assertEqual(client.get("/api/feedback/dag-issues").status_code, 401)
        self.assertEqual(client.get("/api/feedback/global").status_code, 401)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_feedback_api_routes_separate_global_and_dag_issues(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FeedbackStore(Path(tmp))
            store.create_global_feedback("General note")
            store.create_analysis_issue_feedback("Wrong DAG result", _sample_feedback_context())
            with patch.dict("os.environ", {"AF_DAGS_HELPER_FEEDBACK_DIR": tmp}):
                client = TestClient(app)
                global_response = client.get("/api/feedback/global", auth=("admin", "secret"))
                issue_response = client.get("/api/feedback/dag-issues", auth=("admin", "secret"))

        self.assertEqual(global_response.status_code, 200)
        self.assertEqual(issue_response.status_code, 200)
        self.assertEqual([item["type"] for item in global_response.json()["items"]], ["global"])
        self.assertEqual([item["type"] for item in issue_response.json()["items"]], ["analysis_issue"])

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_dag_issue_archive_route_returns_tar_gz(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FeedbackStore(Path(tmp))
            store.create_analysis_issue_feedback("Wrong DAG result", _sample_feedback_context())
            with patch.dict("os.environ", {"AF_DAGS_HELPER_FEEDBACK_DIR": tmp}):
                client = TestClient(app)
                response = client.get("/api/feedback/dag-issues/archive", auth=("admin", "secret"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "application/gzip")
        with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as archive:
            self.assertIn("feedback.json", archive.getnames())

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_root_page_contains_help_dialog_content(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))

        self.assertIn("Справка по AF DAGs Helper", response.text)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_help_dialog_describes_drawer_workflow(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))

        self.assertIn("source drawer toggle handle", response.text)
        self.assertIn("DAG Source", response.text)
        self.assertIn("Analyze drawer", response.text)
        self.assertIn("выезжающее Source drawer", response.text)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_help_dialog_describes_feedback_workflow(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))

        self.assertIn("Обратная связь", response.text)
        self.assertIn("Report issue", response.text)
        self.assertIn("снимок DAG", response.text)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_header_has_help_button_next_to_title(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        self.assertNotIn("FastAPI + NiceGUI", response.text)

        headers = [element for element in elements.values() if element["tag"] == "nicegui-header"]
        self.assertEqual(len(headers), 1)
        header_children = [elements[str(child_id)] for child_id in headers[0]["children"]]
        title_index = next(
            index for index, child in enumerate(header_children)
            if child.get("text") == "AF DAGs Helper"
        )
        help_button = header_children[title_index + 1]

        self.assertEqual(help_button["tag"], "q-btn")
        self.assertEqual(help_button.get("props", {}).get("icon"), "help_outline")
        self.assertEqual(help_button.get("props", {}).get("text-color"), "white")

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_header_has_settings_button(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        headers = [element for element in elements.values() if element["tag"] == "nicegui-header"]
        self.assertEqual(len(headers), 1)
        header_descendants = list(_descendants(elements, headers[0]))

        self.assertTrue(
            any(
                element["tag"] == "q-btn"
                and element.get("props", {}).get("icon") == "settings"
                and element.get("props", {}).get("text-color") == "white"
                for element in header_descendants
            )
        )

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_header_has_feedback_button(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        headers = [element for element in elements.values() if element["tag"] == "nicegui-header"]
        self.assertEqual(len(headers), 1)
        header_descendants = list(_descendants(elements, headers[0]))

        self.assertTrue(
            any(
                element["tag"] == "q-btn"
                and element.get("props", {}).get("icon") == "rate_review"
                and element.get("props", {}).get("text-color") == "white"
                for element in header_descendants
            )
        )

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_header_has_source_drawer_menu_button(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        headers = [element for element in elements.values() if element["tag"] == "nicegui-header"]
        self.assertEqual(len(headers), 1)
        header_descendants = list(_descendants(elements, headers[0]))

        self.assertTrue(
            any(
                element["tag"] == "q-btn"
                and element.get("props", {}).get("icon") == "menu"
                and element.get("props", {}).get("text-color") == "white"
                for element in header_descendants
            )
        )

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_source_controls_live_in_overlay_drawer(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        drawers = [
            element for element in elements.values()
            if "source-drawer" in element.get("class", [])
        ]
        self.assertEqual(len(drawers), 1)
        self.assertEqual(drawers[0].get("props", {}).get("model-value"), False)
        self.assertIn("overlay", drawers[0].get("props", {}))
        drawer_descendants = list(_descendants(elements, drawers[0]))

        self.assertTrue(
            any(
                element["tag"] == "q-btn"
                and element.get("props", {}).get("label") == "Analyze"
                for element in drawer_descendants
            )
        )
        self.assertTrue(
            any(
                element["tag"] == "q-btn"
                and element.get("props", {}).get("label") == "Browse DAG..."
                for element in drawer_descendants
            )
        )
        self.assertFalse(
            any("source-pane" in element.get("class", []) for element in elements.values())
        )

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_floating_drawer_handle_is_closed_by_default(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        handles = [
            element for element in elements.values()
            if "source-drawer-toggle-btn" in element.get("class", [])
        ]

        self.assertEqual(len(handles), 1)
        self.assertIn("source-drawer-toggle-handle", handles[0].get("class", []))
        self.assertIn("drawer-closed", handles[0].get("class", []))
        self.assertEqual(handles[0].get("props", {}).get("icon"), "chevron_right")
        self.assertIn("top: 50%", response.text)
        self.assertIn("transform: translateY(-50%)", response.text)
        self.assertIn("background: var(--q-primary)", response.text)
        self.assertIn("clip-path: polygon", response.text)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_main_layout_contains_source_code_preview(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        source_previews = [
            element for element in elements.values()
            if "source-code-pane" in element.get("class", [])
        ]
        self.assertEqual(len(source_previews), 1)
        preview_descendants = list(_descendants(elements, source_previews[0]))

        self.assertTrue(any(element.get("text") == "DAG Source" for element in preview_descendants))
        self.assertTrue(
            any(
                element["tag"] == "nicegui-codemirror"
                and "readonly" in element.get("props", {})
                for element in preview_descendants
            )
        )

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_source_tabs_use_repo_tab(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        tabs = [
            element.get("props", {}).get("label")
            for element in elements.values()
            if element["tag"] == "q-tab"
        ]

        self.assertNotIn("Server file", response.text)
        self.assertIn("Repo", tabs)
        self.assertIn("Upload", tabs)
        self.assertIn("Paste", tabs)
        self.assertNotIn("Server file", tabs)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_upload_and_paste_source_tab_panels_have_inputs(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        upload_panels = [
            element for element in elements.values()
            if element["tag"] == "q-tab-panel"
            and element.get("props", {}).get("name") == "upload"
        ]
        paste_panels = [
            element for element in elements.values()
            if element["tag"] == "q-tab-panel"
            and element.get("props", {}).get("name") == "paste"
        ]

        self.assertEqual(len(upload_panels), 1)
        self.assertEqual(len(paste_panels), 1)
        self.assertTrue(any(element["tag"] == "nicegui-upload" for element in _descendants(elements, upload_panels[0])))
        self.assertTrue(
            any(
                element["tag"] == "nicegui-input"
                and element.get("props", {}).get("label") == "Paste DAG source"
                for element in _descendants(elements, paste_panels[0])
            )
        )

    def test_source_tab_name_accepts_tab_object_and_string_values(self):
        import web.app as web_app

        resolver = getattr(web_app, "_source_tab_name", None)
        self.assertIsNotNone(resolver)

        repo_tab = object()
        upload_tab = object()
        paste_tab = object()

        self.assertEqual(resolver(repo_tab, repo_tab, upload_tab, paste_tab), "repo")
        self.assertEqual(resolver(upload_tab, repo_tab, upload_tab, paste_tab), "upload")
        self.assertEqual(resolver(paste_tab, repo_tab, upload_tab, paste_tab), "paste")
        self.assertEqual(resolver("repo", repo_tab, upload_tab, paste_tab), "repo")
        self.assertEqual(resolver("upload", repo_tab, upload_tab, paste_tab), "upload")
        self.assertEqual(resolver("paste", repo_tab, upload_tab, paste_tab), "paste")
        self.assertEqual(resolver(SimpleNamespace(props={"name": "repo"}), repo_tab, upload_tab, paste_tab), "repo")
        self.assertEqual(resolver(SimpleNamespace(_props={"name": "paste"}), repo_tab, upload_tab, paste_tab), "paste")

    def test_safe_feedback_source_filename_uses_dag_id_for_paste(self):
        import web.app as web_app

        self.assertEqual(web_app._feedback_source_filename("paste", "sales_daily", None, None), "sales_daily.py")

    def test_safe_python_basename_rejects_path_components(self):
        import web.app as web_app

        self.assertEqual(web_app._safe_python_basename("../bad/name"), "name.py")
        self.assertEqual(web_app._safe_python_basename("daily sales.py"), "daily_sales.py")

    def test_resolve_current_source_path_writes_uploaded_source(self):
        import web.app as web_app

        class FakeService:
            def __init__(self):
                self.calls = []

            def write_source_to_runtime_file(self, name, source):
                self.calls.append((name, source))
                return Path("/tmp/uploaded.py")

        service = FakeService()

        path = web_app._resolve_current_source_path(
            active_tab="upload",
            repo_name=None,
            selected_dag_node=None,
            uploaded_source="from airflow import DAG\n",
            uploaded_name="daily_dag",
            pasted_source="",
            repository_browser=None,
            service=service,
        )

        self.assertEqual(path, Path("/tmp/uploaded.py"))
        self.assertEqual(service.calls, [("daily_dag", "from airflow import DAG\n")])

    def test_resolve_current_source_path_writes_pasted_source(self):
        import web.app as web_app

        class FakeService:
            def __init__(self):
                self.calls = []

            def write_source_to_runtime_file(self, name, source):
                self.calls.append((name, source))
                return Path("/tmp/pasted.py")

        service = FakeService()

        path = web_app._resolve_current_source_path(
            active_tab="paste",
            repo_name=None,
            selected_dag_node=None,
            uploaded_source=None,
            uploaded_name="uploaded_dag",
            pasted_source="print('dag')\n",
            repository_browser=None,
            service=service,
        )

        self.assertEqual(path, Path("/tmp/pasted.py"))
        self.assertEqual(service.calls, [("pasted_dag", "print('dag')\n")])

    def test_read_upload_event_source_uses_nicegui_file_api(self):
        import web.app as web_app

        class FakeUploadFile:
            name = "daily_sales.py"

            async def text(self, encoding="utf-8"):
                self.encoding = encoding
                return "from airflow import DAG\n"

        filename, source = asyncio.run(
            web_app._read_upload_event_source(SimpleNamespace(file=FakeUploadFile()))
        )

        self.assertEqual(filename, "daily_sales")
        self.assertEqual(source, "from airflow import DAG\n")

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_repo_tab_contains_repository_select_and_browse_button(self):
        with tempfile.TemporaryDirectory() as tmp:
            repos_root = Path(tmp)
            repo = repos_root / "analytics"
            (repo / ".git").mkdir(parents=True)
            (repo / "dags" / "daily").mkdir(parents=True)
            (repo / "dags" / "daily" / "sales.py").write_text("print('sales')", encoding="utf-8")
            with patch.dict("os.environ", {"AF_DAGS_HELPER_REPOS_DIR": str(repos_root)}):
                client = TestClient(app)
                response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        repo_panels = [
            element for element in elements.values()
            if element["tag"] == "q-tab-panel"
            and element.get("props", {}).get("name") == "repo"
        ]
        self.assertEqual(len(repo_panels), 1)
        descendants = list(_descendants(elements, repo_panels[0]))

        self.assertTrue(
            any(
                element["tag"] == "nicegui-select"
                and element.get("props", {}).get("label") == "Repository"
                for element in descendants
            )
        )
        self.assertTrue(
            any(
                element["tag"] == "q-btn"
                and element.get("props", {}).get("label") == "Browse DAG..."
                and element.get("props", {}).get("icon") == "folder_open"
                for element in descendants
            )
        )
        self.assertFalse(any(element["tag"] == "q-tree" for element in descendants))

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_dag_picker_dialog_contains_compact_table_controls(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        dialogs = [element for element in elements.values() if element["tag"] == "nicegui-dialog"]
        dialog_descendants = [
            descendant
            for dialog in dialogs
            for descendant in _descendants(elements, dialog)
        ]
        labels = {
            element.get("props", {}).get("label") or element.get("text")
            for element in dialog_descendants
        }
        table_columns = [
            column
            for element in dialog_descendants
            if element["tag"] == "nicegui-table"
            for column in element.get("props", {}).get("columns", [])
        ]
        placeholders = {
            element.get("props", {}).get("placeholder")
            for element in dialog_descendants
        }
        icons = {
            element.get("props", {}).get("icon")
            for element in dialog_descendants
            if element["tag"] == "q-btn"
        }

        self.assertIn("Select DAG", labels)
        self.assertIn("Search DAG filename...", placeholders)
        self.assertIn("Select", labels)
        self.assertIn("Cancel", labels)
        self.assertIn("refresh", icons)
        self.assertTrue(any(element["tag"] == "nicegui-table" for element in dialog_descendants))
        self.assertIn("Commit date", {column.get("label") for column in table_columns})

    def test_visible_dag_picker_rows_respect_expanded_dirs_and_search(self):
        import web.app as web_app

        nodes = [
            {"id": "dir:dags", "type": "dir", "name": "dags", "path": "dags", "level": 0},
            {"id": "dir:dags/daily", "type": "dir", "name": "daily", "path": "dags/daily", "level": 1},
            {
                "id": "file:dags/daily/sales_report.py",
                "type": "file",
                "name": "sales_report.py",
                "path": "dags/daily/sales_report.py",
                "level": 2,
            },
            {
                "id": "file:dags/daily/stock_sync.py",
                "type": "file",
                "name": "stock_sync.py",
                "path": "dags/daily/stock_sync.py",
                "level": 2,
            },
            {"id": "dir:archive", "type": "dir", "name": "archive", "path": "archive", "level": 0},
            {
                "id": "file:archive/old_sales.py",
                "type": "file",
                "name": "old_sales.py",
                "path": "archive/old_sales.py",
                "level": 1,
            },
        ]

        collapsed = web_app._visible_dag_picker_rows(nodes, set(), "", None)
        expanded = web_app._visible_dag_picker_rows(nodes, {"dir:dags", "dir:dags/daily"}, "", "file:dags/daily/sales_report.py")
        searched = web_app._visible_dag_picker_rows(nodes, set(), "sales", None)

        self.assertEqual([row["id"] for row in collapsed], ["dir:dags", "dir:archive"])
        self.assertEqual([row["id"] for row in expanded], [
            "dir:dags",
            "dir:dags/daily",
            "file:dags/daily/sales_report.py",
            "file:dags/daily/stock_sync.py",
            "dir:archive",
        ])
        self.assertTrue(expanded[2]["is_selected"])
        self.assertEqual([row["id"] for row in searched], [
            "dir:dags",
            "dir:dags/daily",
            "file:dags/daily/sales_report.py",
            "dir:archive",
            "file:archive/old_sales.py",
        ])
        self.assertTrue(all(row["expanded"] for row in searched if row["type"] == "dir"))

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_settings_dialog_contains_repository_actions(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        dialogs = [element for element in elements.values() if element["tag"] == "nicegui-dialog"]
        dialog_descendants = [
            descendant
            for dialog in dialogs
            for descendant in _descendants(elements, dialog)
        ]
        labels = {
            element.get("props", {}).get("label") or element.get("text")
            for element in dialog_descendants
        }

        self.assertIn("Repository settings", labels)
        self.assertIn("Add", labels)
        self.assertIn("Remove", labels)
        self.assertIn("Git pull selected", labels)
        self.assertIn("Git pull all", labels)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_default_repositories_root_is_user_home_repos(self):
        os.environ.pop("AF_DAGS_HELPER_REPOS_DIR", None)
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)
        markdown = [
            element.get("props", {}).get("innerHTML", "")
            for element in elements.values()
            if element["tag"] == "nicegui-markdown"
        ]

        self.assertTrue(any(str(Path.home() / "repos") in content for content in markdown))

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_result_tabs_own_their_content_widgets(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        result_panels = [
            element for element in elements.values()
            if element["tag"] == "q-tab-panels"
            and element.get("props", {}).get("model-value") == "Generated OMEntity"
        ]
        self.assertEqual(len(result_panels), 1)

        panel_ids = result_panels[0]["children"]
        panels = [elements[str(panel_id)] for panel_id in panel_ids]
        self.assertTrue(all(panel.get("children") for panel in panels))

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_result_area_uses_single_scroll_layout(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        main_rows = [
            element for element in elements.values()
            if "web-main" in element.get("class", [])
        ]
        self.assertEqual(len(main_rows), 1)
        self.assertIn("overflow-hidden", main_rows[0]["class"])

        result_panes = [
            element for element in elements.values()
            if "result-pane" in element.get("class", [])
        ]
        self.assertEqual(len(result_panes), 1)
        self.assertIn("min-h-0", result_panes[0]["class"])

        result_panels = [
            element for element in elements.values()
            if "result-panels" in element.get("class", [])
        ]
        self.assertEqual(len(result_panels), 1)

        panel_ids = result_panels[0]["children"]
        panels = [elements[str(panel_id)] for panel_id in panel_ids]
        self.assertTrue(all("result-tab-panel" in panel.get("class", []) for panel in panels))

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_diagram_view_toggle_lives_in_interactive_diagram_tab(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        graph_panels = [
            element for element in elements.values()
            if element["tag"] == "q-tab-panel"
            and element.get("props", {}).get("name") == "Interactive Diagram"
        ]
        self.assertEqual(len(graph_panels), 1)
        graph_descendants = list(_descendants(elements, graph_panels[0]))

        self.assertTrue(any(element["tag"] == "q-btn-toggle" for element in graph_descendants))

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_generated_tab_owns_copy_and_save_actions(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        generated_panels = [
            element for element in elements.values()
            if element["tag"] == "q-tab-panel"
            and element.get("props", {}).get("name") == "Generated OMEntity"
        ]
        self.assertEqual(len(generated_panels), 1)
        generated_descendants = list(_descendants(elements, generated_panels[0]))
        labels = {
            element.get("props", {}).get("label")
            for element in generated_descendants
            if element["tag"] == "q-btn"
        }

        self.assertIn("Copy", labels)
        self.assertIn("Save", labels)

    @patch.dict("os.environ", {"AF_DAGS_HELPER_AUTH_USER": "admin", "AF_DAGS_HELPER_AUTH_PASSWORD": "secret"})
    def test_generated_tab_owns_report_issue_action_after_copy_and_save(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))
        elements = _nicegui_elements(response.text)

        generated_panels = [
            element for element in elements.values()
            if element["tag"] == "q-tab-panel"
            and element.get("props", {}).get("name") == "Generated OMEntity"
        ]
        self.assertEqual(len(generated_panels), 1)
        button_labels = [
            element.get("props", {}).get("label")
            for element in _descendants(elements, generated_panels[0])
            if element["tag"] == "q-btn"
        ]

        self.assertEqual(
            [label for label in button_labels if label in {"Copy", "Save", "Report issue"}],
            ["Copy", "Save", "Report issue"],
        )


if __name__ == "__main__":
    unittest.main()
