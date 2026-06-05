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
