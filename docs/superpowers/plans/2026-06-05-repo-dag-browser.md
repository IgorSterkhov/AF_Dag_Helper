# Repo DAG Browser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add safe server-side git repository registration, update, nested DAG browsing, and analysis from the web UI.

**Architecture:** Create a focused `web.repository_browser.RepositoryBrowser` for repository discovery, registry persistence, DAG tree building, path resolution, and git pulls. Keep `web.app` as the NiceGUI composition layer that binds repository actions into settings and source selection. Reuse `DAGAnalysisService` unchanged.

**Tech Stack:** Python stdlib `pathlib`, `json`, `subprocess`; FastAPI + NiceGUI; `unittest` + `fastapi.testclient`.

---

### Task 1: Repository Browser Backend

**Files:**
- Create: `web/repository_browser.py`
- Create: `tests/test_repository_browser.py`

- [ ] Write failing tests for discovering direct `.git` folders, registering/removing repos in `repositories.json`, rejecting traversal, building nested `.py` DAG tree, resolving selected DAG paths, and invoking `git pull --ff-only` without shell.
- [ ] Run `./venv/Scripts/python.exe -m unittest tests.test_repository_browser -v` and verify RED.
- [ ] Implement `RepositoryBrowser` with methods `registered_repositories`, `discover_repositories`, `add_repository`, `remove_repository`, `build_dag_tree`, `resolve_dag_path`, `pull_repository`, and `pull_all`.
- [ ] Run `./venv/Scripts/python.exe -m unittest tests.test_repository_browser -v` and verify GREEN.

### Task 2: Web UI Integration

**Files:**
- Modify: `web/app.py`
- Modify: `tests/test_web_app.py`

- [ ] Write failing tests that the source tab is labeled `Repo`, the header has a settings gear button, settings modal exposes repository actions, and the Repo panel contains repository selection plus a tree.
- [ ] Run `./venv/Scripts/python.exe -m unittest tests.test_web_app -v` and verify RED.
- [ ] Instantiate `RepositoryBrowser` with `AF_DAGS_HELPER_REPOS_DIR` defaulting to `~/repos` and registry under the existing runtime directory.
- [ ] Replace `Server file` UI with Repo selection, DAG tree, selected DAG label, refresh and git pull actions.
- [ ] Add settings modal with registered/discovered lists and add/remove/pull actions.
- [ ] Update `resolve_current_dag_path()` to resolve selected repo DAGs through `RepositoryBrowser`.
- [ ] Run `./venv/Scripts/python.exe -m unittest tests.test_web_app -v` and verify GREEN.

### Task 3: Verification, Commit, Deploy

**Files:**
- Verify all changed files.

- [ ] Run `./venv/Scripts/python.exe -m unittest tests.test_repository_browser tests.test_web_app tests.test_web_server_files tests.test_web_analysis_service -v`.
- [ ] Run `./venv/Scripts/python.exe -m py_compile web/app.py web/repository_browser.py web/auth.py web/analysis_service.py web/server_files.py`.
- [ ] Review `git diff -- web/app.py web/repository_browser.py tests/test_repository_browser.py tests/test_web_app.py docs/superpowers/specs/2026-06-05-repo-dag-browser-design.md docs/superpowers/plans/2026-06-05-repo-dag-browser.md`.
- [ ] Commit only the files for this task.
- [ ] Push to GitHub and deploy with `scripts/ivm1_ops.sh deploy`.
- [ ] Verify VM health, deployed SHA, and remote HTML contains the Repo tab, settings button, and repository controls.
