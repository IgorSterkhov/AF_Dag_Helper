from pathlib import Path
import unittest


class DeployScriptTest(unittest.TestCase):
    def test_deploy_defaults_repos_dir_to_server_home(self):
        script = Path("scripts/deploy_ivm1.sh").read_text(encoding="utf-8")

        self.assertIn('REPOS_DIR="${AF_DAGS_HELPER_REPOS_DIR:-/home/igor.sterhov/repos}"', script)
        self.assertNotIn('REPOS_DIR="$APP_DIR/repos"', script)


if __name__ == "__main__":
    unittest.main()
