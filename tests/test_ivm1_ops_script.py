import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = "scripts/ivm1_ops.sh"


class Ivm1OpsScriptTest(unittest.TestCase):
    def test_list_contains_core_actions(self):
        result = subprocess.run(
            ["bash", str(SCRIPT), "--list"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        for action in ("deploy", "status", "health", "logs", "follow", "restart", "version", "credentials", "ssh"):
            self.assertIn(action, result.stdout)

    def test_help_describes_interactive_and_direct_usage(self):
        result = subprocess.run(
            ["bash", str(SCRIPT), "--help"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )

        self.assertIn("Interactive", result.stdout)
        self.assertIn("Direct commands", result.stdout)

    def test_script_has_valid_bash_syntax(self):
        subprocess.run(["bash", "-n", str(SCRIPT)], cwd=ROOT, check=True)


if __name__ == "__main__":
    unittest.main()
