import json
import re
import unittest
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
