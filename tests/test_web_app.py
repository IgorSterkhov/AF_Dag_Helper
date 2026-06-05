import json
import os
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from web.app import app


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
    def test_root_page_contains_help_dialog_content(self):
        client = TestClient(app)
        response = client.get("/", auth=("admin", "secret"))

        self.assertIn("Справка по AF DAGs Helper", response.text)

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
        self.assertNotIn("Server file", tabs)

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


if __name__ == "__main__":
    unittest.main()
