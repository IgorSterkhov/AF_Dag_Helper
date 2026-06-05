import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from web.repository_browser import RepositoryBrowser


class RepositoryBrowserTest(unittest.TestCase):
    def test_discovers_only_direct_git_repositories(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "repo_a" / ".git").mkdir(parents=True)
            (root / "repo_b").mkdir()
            (root / "nested" / "repo_c" / ".git").mkdir(parents=True)

            browser = RepositoryBrowser(root, root / "registry.json")

            self.assertEqual(browser.discover_repositories(), ["repo_a"])

    def test_registers_and_removes_repository_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "repo_a" / ".git").mkdir(parents=True)
            registry = root / "runtime" / "repositories.json"
            browser = RepositoryBrowser(root, registry)

            browser.add_repository("repo_a")
            browser.add_repository("repo_a")

            self.assertEqual(browser.registered_repositories(), ["repo_a"])
            self.assertEqual(json.loads(registry.read_text(encoding="utf-8")), {"repositories": ["repo_a"]})

            browser.remove_repository("repo_a")

            self.assertEqual(browser.registered_repositories(), [])

    def test_rejects_repository_path_traversal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "repo_a" / ".git").mkdir(parents=True)
            browser = RepositoryBrowser(root, root / "registry.json")

            with self.assertRaises(ValueError):
                browser.add_repository("../repo_a")

    def test_builds_nested_dag_tree_for_registered_repository(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo_a"
            (repo / ".git").mkdir(parents=True)
            (repo / "dags" / "daily").mkdir(parents=True)
            (repo / "dags" / "daily" / "sales.py").write_text("print('sales')", encoding="utf-8")
            (repo / "dags" / "daily" / "notes.txt").write_text("ignore", encoding="utf-8")
            (repo / "root_dag.py").write_text("print('root')", encoding="utf-8")
            browser = RepositoryBrowser(root, root / "registry.json")
            browser.add_repository("repo_a")

            tree = browser.build_dag_tree("repo_a")

            self.assertEqual(
                tree,
                [
                    {
                        "id": "dir:dags",
                        "label": "dags",
                        "children": [
                            {
                                "id": "dir:dags/daily",
                                "label": "daily",
                                "children": [
                                    {
                                        "id": "file:dags/daily/sales.py",
                                        "label": "sales.py",
                                        "path": "dags/daily/sales.py",
                                    }
                                ],
                            }
                        ],
                    },
                    {"id": "file:root_dag.py", "label": "root_dag.py", "path": "root_dag.py"},
                ],
            )

    def test_resolves_only_python_dags_inside_registered_repository(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo_a"
            (repo / ".git").mkdir(parents=True)
            (repo / "dags").mkdir()
            dag = repo / "dags" / "sales.py"
            dag.write_text("print('sales')", encoding="utf-8")
            (repo / "notes.txt").write_text("ignore", encoding="utf-8")
            browser = RepositoryBrowser(root, root / "registry.json")
            browser.add_repository("repo_a")

            self.assertEqual(browser.resolve_dag_path("repo_a", "file:dags/sales.py"), dag.resolve())

            with self.assertRaises(ValueError):
                browser.resolve_dag_path("repo_a", "file:notes.txt")
            with self.assertRaises(ValueError):
                browser.resolve_dag_path("repo_a", "file:../outside.py")

    def test_lists_directory_for_dag_picker_navigation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo_a"
            (repo / ".git").mkdir(parents=True)
            (repo / "dags" / "daily").mkdir(parents=True)
            (repo / "dags" / "daily" / "sales.py").write_text("print('sales')", encoding="utf-8")
            (repo / "dags" / "daily" / "notes.txt").write_text("ignore", encoding="utf-8")
            (repo / "root_dag.py").write_text("print('root')", encoding="utf-8")
            browser = RepositoryBrowser(root, root / "registry.json")
            browser.add_repository("repo_a")

            root_listing = browser.list_directory("repo_a")
            nested_listing = browser.list_directory("repo_a", "dags/daily")

            self.assertEqual(root_listing["current"], "")
            self.assertIsNone(root_listing["parent"])
            self.assertEqual(root_listing["directories"], [{"name": "dags", "path": "dags"}])
            self.assertEqual(root_listing["files"], [{"name": "root_dag.py", "node_id": "file:root_dag.py", "path": "root_dag.py"}])

            self.assertEqual(nested_listing["current"], "dags/daily")
            self.assertEqual(nested_listing["parent"], "dags")
            self.assertEqual(nested_listing["directories"], [])
            self.assertEqual(nested_listing["files"], [
                {"name": "sales.py", "node_id": "file:dags/daily/sales.py", "path": "dags/daily/sales.py"}
            ])

    def test_list_directory_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo_a"
            (repo / ".git").mkdir(parents=True)
            browser = RepositoryBrowser(root, root / "registry.json")
            browser.add_repository("repo_a")

            with self.assertRaises(ValueError):
                browser.list_directory("repo_a", "../")

    def test_list_directory_ignores_symlinked_entries_outside_repository(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo_a"
            outside = root / "outside"
            (repo / ".git").mkdir(parents=True)
            outside.mkdir()
            (outside / "leak.py").write_text("print('leak')", encoding="utf-8")
            try:
                (repo / "external").symlink_to(outside, target_is_directory=True)
            except (NotImplementedError, OSError) as exc:
                self.skipTest(f"symlinks are not available: {exc}")
            browser = RepositoryBrowser(root, root / "registry.json")
            browser.add_repository("repo_a")

            listing = browser.list_directory("repo_a")

            self.assertEqual(listing["directories"], [])
            self.assertEqual(listing["files"], [])

    def test_pull_repository_uses_git_without_shell(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            repo = root / "repo_a"
            (repo / ".git").mkdir(parents=True)
            browser = RepositoryBrowser(root, root / "registry.json")
            browser.add_repository("repo_a")

            with patch("web.repository_browser.subprocess.run") as run:
                run.return_value.stdout = "Already up to date.\n"
                run.return_value.stderr = ""

                output = browser.pull_repository("repo_a")

            run.assert_called_once_with(
                ["git", "-C", str(repo.resolve()), "pull", "--ff-only"],
                capture_output=True,
                text=True,
                check=True,
                timeout=120,
            )
            self.assertEqual(output, "Already up to date.")


if __name__ == "__main__":
    unittest.main()
