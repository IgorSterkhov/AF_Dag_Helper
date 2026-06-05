# ivm-1 Terminal Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local bash terminal menu for deploying and monitoring the AF DAGs Helper service on `ivm-1`.

**Architecture:** Add one local bash script, `scripts/ivm1_ops.sh`, which wraps existing deploy and SSH/systemd/journalctl/curl operations. It supports an interactive arrow-key menu plus direct subcommands for automation and tests.

**Tech Stack:** Bash, SSH, systemd, journalctl, curl, Python unittest for no-network script behavior.

---

## File Structure

- Create: `scripts/ivm1_ops.sh` - local terminal interface and direct subcommands.
- Create: `tests/test_ivm1_ops_script.py` - no-network tests for list/help/syntax behavior.
- Modify: `README.md` - document menu usage and useful subcommands.
- Modify: `CLAUDE.md` - add project command references.

---

### Task 1: No-Network Tests

**Files:**
- Create: `tests/test_ivm1_ops_script.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_ivm1_ops_script.py`:

```python
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ivm1_ops.sh"


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
```

- [ ] **Step 2: Verify RED**

Run:

```bash
./venv/Scripts/python.exe -m unittest tests.test_ivm1_ops_script -v
```

Expected: fail because `scripts/ivm1_ops.sh` does not exist yet.

---

### Task 2: Bash Ops Menu

**Files:**
- Create: `scripts/ivm1_ops.sh`

- [ ] **Step 1: Implement direct commands**

Create `scripts/ivm1_ops.sh` with:

- strict bash mode;
- configurable host/app dir/service/port/log lines;
- `--help`;
- `--list`;
- direct commands: `deploy`, `status`, `health`, `logs`, `follow`, `restart`, `version`, `credentials`, `ssh`.

- [ ] **Step 2: Implement interactive menu**

Add bash-only arrow navigation:

- up/down arrows change selected item;
- enter runs selected action;
- number keys run matching action;
- `q` exits.

- [ ] **Step 3: Verify GREEN**

Run:

```bash
./venv/Scripts/python.exe -m unittest tests.test_ivm1_ops_script -v
bash -n scripts/ivm1_ops.sh
```

Expected: all pass.

- [ ] **Step 4: Make script executable**

Run:

```bash
chmod +x scripts/ivm1_ops.sh
```

---

### Task 3: Docs

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update README**

Document:

```bash
scripts/ivm1_ops.sh
scripts/ivm1_ops.sh health
scripts/ivm1_ops.sh follow
scripts/ivm1_ops.sh deploy
```

- [ ] **Step 2: Update CLAUDE**

Add the same command references under project commands.

---

### Task 4: Verification and Commit

**Files:**
- All files from Tasks 1-3

- [ ] **Step 1: Run local verification**

Run:

```bash
./venv/Scripts/python.exe -m unittest tests.test_ivm1_ops_script tests.test_web_app tests.test_web_server_files tests.test_web_analysis_service -v
bash -n scripts/ivm1_ops.sh
bash -n scripts/deploy_ivm1.sh
```

Expected: all pass.

- [ ] **Step 2: Run VM smoke checks**

Run:

```bash
scripts/ivm1_ops.sh health
scripts/ivm1_ops.sh version
scripts/ivm1_ops.sh status
```

Expected:

- health reports `/health` success, unauthenticated `/` as `401`, authenticated `/` as `200`, and session-cookie `/` as `200`;
- version shows deployed SHA;
- status shows active service.

- [ ] **Step 3: Commit and push**

Run:

```bash
git add scripts/ivm1_ops.sh tests/test_ivm1_ops_script.py README.md CLAUDE.md docs/superpowers/specs/2026-06-05-ivm1-terminal-ops-design.md docs/superpowers/plans/2026-06-05-ivm1-terminal-ops.md
git commit -m "feat: add ivm-1 terminal ops menu"
git push git@github.com:IgorSterkhov/AF_Dag_Helper.git master
```
