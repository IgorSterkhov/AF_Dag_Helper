import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from web.app import app


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


if __name__ == "__main__":
    unittest.main()
