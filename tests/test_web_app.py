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


if __name__ == "__main__":
    unittest.main()
