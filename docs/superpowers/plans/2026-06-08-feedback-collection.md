# Feedback Collection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add authenticated local feedback collection for general product notes and analysis-specific DAG issue reports, including reproducible analysis attachments.

**Architecture:** Keep persistence in a new `web/feedback_store.py` module and leave `web/app.py` responsible for UI composition, analysis context capture, and FastAPI route wiring. Store metadata in SQLite and store analysis snapshots as files under `.runtime/feedback/attachments`, then expose separate authenticated API routes for DAG issues and global feedback.

**Tech Stack:** Python stdlib (`sqlite3`, `tarfile`, `hashlib`, `json`, `tempfile`), FastAPI, NiceGUI, existing BasicAuth middleware, `unittest`/`pytest`.

---

## File Structure

- Create `web/feedback_store.py`
  - Owns SQLite schema creation, feedback writes, attachment writes, listing, export status updates, and `.tar.gz` archive creation.
  - Exposes dataclasses for analysis feedback context and attachment manifests.
- Create `tests/test_feedback_store.py`
  - Unit tests for storage, attachment manifest fields, filtering, export marking, and archive contents.
- Modify `web/repository_browser.py`
  - Add `repo_head_revision(name: str) -> str` so analysis feedback can store the exact repo commit used for a selected DAG.
- Modify `web/app.py`
  - Add `last_analysis_context` to `WebState`.
  - Initialize `FeedbackStore`.
  - Add FastAPI routes:
    - `GET /api/feedback/dag-issues`
    - `GET /api/feedback/dag-issues/archive`
    - `GET /api/feedback/global`
  - Add header feedback dialog and button.
  - Add `Report issue` button beside `Copy` and `Save` in `Generated OMEntity`.
  - Capture DAG source, generated output, diff, options, warnings, and repo metadata after successful analysis.
  - Update help text to describe feedback.
- Modify `tests/test_web_app.py`
  - Test new UI controls and help text.
  - Test authenticated API behavior and feedback type separation.

## Task 1: Feedback Store Datamodel And Schema

**Files:**
- Create: `web/feedback_store.py`
- Test: `tests/test_feedback_store.py`

- [ ] **Step 1: Write failing schema and global feedback tests**

```python
def test_global_feedback_saves_without_attachments(self):
    store = FeedbackStore(self.root)

    record = store.create_global_feedback("Please add CSV export")

    self.assertEqual(record["type"], "global")
    self.assertEqual(record["status"], "new")
    self.assertEqual(record["message"], "Please add CSV export")
    self.assertEqual(record["attachments"], [])
    self.assertTrue((self.root / "feedback.sqlite3").exists())
```

Run: `python -m pytest tests/test_feedback_store.py::FeedbackStoreTest::test_global_feedback_saves_without_attachments -q`

Expected: FAIL with `ImportError` or `NameError` because `FeedbackStore` does not exist.

- [ ] **Step 2: Implement schema and global feedback insert**

Add `FeedbackStore.__init__`, `_initialize_schema`, `create_global_feedback`, and `get_feedback`.

```python
class FeedbackStore:
    def __init__(self, root_dir: Path):
        self.root_dir = Path(root_dir)
        self.db_path = self.root_dir / "feedback.sqlite3"
        self.attachments_dir = self.root_dir / "attachments"
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.attachments_dir.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()
```

Use this schema exactly:

```sql
CREATE TABLE IF NOT EXISTS feedback (
    id INTEGER PRIMARY KEY,
    created_at TEXT NOT NULL,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT NOT NULL,
    source_type TEXT,
    dag_id TEXT,
    repo_name TEXT,
    dag_path TEXT,
    repo_commit TEXT,
    original_filename TEXT,
    analysis_options_json TEXT,
    analysis_summary_json TEXT,
    exported_at TEXT
);
CREATE TABLE IF NOT EXISTS feedback_attachments (
    id INTEGER PRIMARY KEY,
    feedback_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    filename TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    content_type TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(feedback_id) REFERENCES feedback(id)
);
```

- [ ] **Step 3: Run store tests**

Run: `python -m pytest tests/test_feedback_store.py::FeedbackStoreTest::test_global_feedback_saves_without_attachments -q`

Expected: PASS.

## Task 2: Analysis Issue Attachments

**Files:**
- Modify: `web/feedback_store.py`
- Modify: `tests/test_feedback_store.py`

- [ ] **Step 1: Write failing analysis feedback test**

```python
def test_analysis_issue_feedback_saves_required_attachments(self):
    store = FeedbackStore(self.root)
    context = AnalysisFeedbackContext(
        source_type="repo",
        dag_id="daily_sales",
        repo_name="analytics",
        dag_path="dags/daily_sales.py",
        repo_commit="abc123",
        original_filename=None,
        source_text="from airflow import DAG\n",
        analysis_options={"force_all_tasks": True, "compare_existing": True, "initial_view": "dag"},
        analysis_summary={"task_count": 2, "output_count": 1, "warnings_count": 0},
        generated_text="inlets=[]\noutlets=[]\n",
        difference_text="# MATCH\n",
        warnings_text="",
    )

    record = store.create_analysis_issue_feedback("Wrong outlet", context)

    kinds = {attachment["kind"] for attachment in record["attachments"]}
    self.assertEqual(kinds, {"dag_source", "generated_omentity", "difference", "metadata"})
    self.assertEqual(record["repo_name"], "analytics")
    self.assertEqual(record["repo_commit"], "abc123")
    self.assertTrue(all(attachment["sha256"] for attachment in record["attachments"]))
    self.assertTrue(all(attachment["size_bytes"] > 0 for attachment in record["attachments"]))
```

Run: `python -m pytest tests/test_feedback_store.py::FeedbackStoreTest::test_analysis_issue_feedback_saves_required_attachments -q`

Expected: FAIL because `AnalysisFeedbackContext` and `create_analysis_issue_feedback` do not exist.

- [ ] **Step 2: Implement analysis context dataclass and attachment writes**

Add:

```python
@dataclass
class AnalysisFeedbackContext:
    source_type: str
    dag_id: str
    repo_name: Optional[str]
    dag_path: Optional[str]
    repo_commit: Optional[str]
    original_filename: Optional[str]
    source_text: str
    analysis_options: Dict[str, Any]
    analysis_summary: Dict[str, Any]
    generated_text: str
    difference_text: str
    warnings_text: str = ""
    source_filename: Optional[str] = None
```

Attachment rules:

- DAG source filename:
  - `context.source_filename` if provided;
  - else `context.original_filename` if provided;
  - else `<context.dag_id>.py` if `dag_id` is present and not `Unknown`;
  - else `pasted_dag.py`.
- Always create `dag_source__<safe_basename>.py`, `generated_omentity.py`, `difference.md`, and `metadata.json`.
- Create `warnings.md` only when `warnings_text.strip()` is not empty and not equal to `No warnings`.
- Insert the feedback row and attachment rows in one transaction.
- If file writing fails, remove the partially created `feedback-000001` directory and do not leave a visible feedback row.

- [ ] **Step 3: Run analysis attachment tests**

Run: `python -m pytest tests/test_feedback_store.py -q`

Expected: PASS for global and analysis issue storage tests.

## Task 3: Filtering, Export Marking, And Archive

**Files:**
- Modify: `web/feedback_store.py`
- Modify: `tests/test_feedback_store.py`

- [ ] **Step 1: Write failing filter/export/archive tests**

```python
def test_new_mode_filters_and_mark_exported_updates_only_returned_rows(self):
    store = FeedbackStore(self.root)
    global_record = store.create_global_feedback("General")
    issue_record = store.create_analysis_issue_feedback("Wrong DAG", self.sample_context())

    new_issues = store.list_feedback("analysis_issue", mode="new")
    store.mark_exported([record["id"] for record in new_issues])

    self.assertEqual([record["id"] for record in new_issues], [issue_record["id"]])
    self.assertEqual(store.get_feedback(issue_record["id"])["status"], "exported")
    self.assertEqual(store.get_feedback(global_record["id"])["status"], "new")

def test_dag_issue_archive_contains_manifest_and_attachments(self):
    store = FeedbackStore(self.root)
    issue = store.create_analysis_issue_feedback("Wrong DAG", self.sample_context())

    archive_bytes = store.build_dag_issue_archive([issue])

    with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as archive:
        names = archive.getnames()
    self.assertIn("feedback.json", names)
    self.assertIn("attachments/feedback-000001/generated_omentity.py", names)
```

Run: `python -m pytest tests/test_feedback_store.py -q`

Expected: FAIL because list/export/archive helpers do not exist.

- [ ] **Step 2: Implement listing, export marking, and tar.gz generation**

Add methods:

```python
def list_feedback(self, feedback_type: str, mode: str = "all") -> List[Dict[str, Any]]:
    ...

def mark_exported(self, feedback_ids: Iterable[int]) -> None:
    ...

def build_dag_issue_archive(self, records: List[Dict[str, Any]]) -> bytes:
    ...
```

Validation:

- `feedback_type` must be `global` or `analysis_issue`.
- `mode` must be `all` or `new`.
- Archive contains `feedback.json` and all files listed in each record's attachment manifest.

- [ ] **Step 3: Run feedback store suite**

Run: `python -m pytest tests/test_feedback_store.py -q`

Expected: PASS.

## Task 4: Repository Commit Helper

**Files:**
- Modify: `web/repository_browser.py`
- Modify: `tests/test_repository_browser.py`

- [ ] **Step 1: Write failing repo head revision test**

```python
def test_repo_head_revision_returns_current_commit(self):
    browser = RepositoryBrowser(self.repos_root, self.registry_file)
    browser.add_repository("analytics")

    revision = browser.repo_head_revision("analytics")

    self.assertEqual(len(revision), 40)
```

Run: `python -m pytest tests/test_repository_browser.py::RepositoryBrowserTest::test_repo_head_revision_returns_current_commit -q`

Expected: FAIL because `repo_head_revision` does not exist.

- [ ] **Step 2: Implement public helper**

```python
def repo_head_revision(self, name: str) -> str:
    return self._repo_revision(self._registered_repo_path(name))
```

- [ ] **Step 3: Run repository browser tests**

Run: `python -m pytest tests/test_repository_browser.py -q`

Expected: PASS.

## Task 5: FastAPI Feedback Routes

**Files:**
- Modify: `web/app.py`
- Modify: `tests/test_web_app.py`

- [ ] **Step 1: Write failing API separation/auth tests**

```python
@patch.dict("os.environ", {
    "AF_DAGS_HELPER_AUTH_USER": "admin",
    "AF_DAGS_HELPER_AUTH_PASSWORD": "secret",
})
def test_feedback_api_routes_require_authentication(self):
    client = TestClient(app)

    self.assertEqual(client.get("/api/feedback/dag-issues").status_code, 401)
    self.assertEqual(client.get("/api/feedback/global").status_code, 401)

@patch.dict("os.environ", {
    "AF_DAGS_HELPER_AUTH_USER": "admin",
    "AF_DAGS_HELPER_AUTH_PASSWORD": "secret",
})
def test_feedback_api_routes_separate_global_and_dag_issues(self):
    with tempfile.TemporaryDirectory() as tmp:
        store = FeedbackStore(Path(tmp))
        store.create_global_feedback("General")
        store.create_analysis_issue_feedback("Wrong DAG", sample_context())
        with patch.dict("os.environ", {"AF_DAGS_HELPER_FEEDBACK_DIR": tmp}):
            client = TestClient(app)
            global_response = client.get("/api/feedback/global", auth=("admin", "secret"))
            issue_response = client.get("/api/feedback/dag-issues", auth=("admin", "secret"))

    self.assertEqual(len(global_response.json()["items"]), 1)
    self.assertEqual(global_response.json()["items"][0]["type"], "global")
    self.assertEqual(len(issue_response.json()["items"]), 1)
    self.assertEqual(issue_response.json()["items"][0]["type"], "analysis_issue")
```

Run: `python -m pytest tests/test_web_app.py -q`

Expected: FAIL because routes do not exist.

- [ ] **Step 2: Add route helpers and endpoints**

Add in `web/app.py`:

```python
def feedback_store_from_env() -> FeedbackStore:
    root = Path(os.environ.get("AF_DAGS_HELPER_FEEDBACK_DIR", ROOT_DIR / ".runtime" / "feedback"))
    return FeedbackStore(root)

def _parse_feedback_query(mode: str, mark_exported: bool) -> tuple[str, bool]:
    if mode not in {"all", "new"}:
        raise HTTPException(status_code=400, detail="mode must be 'all' or 'new'")
    return mode, mark_exported
```

Add routes:

```python
@app.get("/api/feedback/dag-issues")
def list_dag_issue_feedback(mode: str = "all", mark_exported: bool = False):
    ...

@app.get("/api/feedback/dag-issues/archive")
def download_dag_issue_feedback_archive(mode: str = "all", mark_exported: bool = False):
    ...

@app.get("/api/feedback/global")
def list_global_feedback(mode: str = "all", mark_exported: bool = False):
    ...
```

The archive route returns `Response(content=archive_bytes, media_type="application/gzip", headers={"Content-Disposition": "attachment; filename=dag-issues-feedback.tar.gz"})`.

- [ ] **Step 3: Run API tests**

Run: `python -m pytest tests/test_web_app.py -q`

Expected: PASS for auth and route separation tests.

## Task 6: Analysis Context Capture

**Files:**
- Modify: `web/app.py`
- Modify: `tests/test_web_app.py`

- [ ] **Step 1: Write focused tests for context helpers**

```python
def test_safe_feedback_source_filename_uses_dag_id_for_paste(self):
    self.assertEqual(web_app._feedback_source_filename("paste", "sales_daily", None, None), "sales_daily.py")

def test_safe_feedback_source_filename_rejects_path_components(self):
    self.assertEqual(web_app._safe_python_basename("../bad/name"), "bad_name.py")
```

Run: `python -m pytest tests/test_web_app.py::WebAppTest::test_safe_feedback_source_filename_uses_dag_id_for_paste -q`

Expected: FAIL because helpers do not exist.

- [ ] **Step 2: Add helpers and `WebState.last_analysis_context`**

Add:

```python
self.last_analysis_context: Optional[AnalysisFeedbackContext] = None
```

Add helper functions near `_read_upload_event_source`:

```python
def _safe_python_basename(name: str) -> str:
    stem = Path(name or "").name
    if stem.endswith(".py"):
        stem = stem[:-3]
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in stem).strip("_")
    return f"{safe or 'pasted_dag'}.py"

def _feedback_source_filename(source_type: str, dag_id: str, original_filename: Optional[str], requested_filename: Optional[str]) -> str:
    if requested_filename:
        return _safe_python_basename(requested_filename)
    if source_type == "upload" and original_filename:
        return _safe_python_basename(original_filename)
    if dag_id and dag_id != "Unknown":
        return _safe_python_basename(dag_id)
    return "pasted_dag.py"
```

In `analyze()`, read `source_text = dag_path.read_text(encoding="utf-8")` before calling `service.analyze(...)`. After success, create `AnalysisFeedbackContext` from captured source, result fields, options, and repo metadata.

- [ ] **Step 3: Run helper tests**

Run: `python -m pytest tests/test_web_app.py -q`

Expected: PASS for helper tests and existing UI tests.

## Task 7: NiceGUI Feedback UI

**Files:**
- Modify: `web/app.py`
- Modify: `tests/test_web_app.py`

- [ ] **Step 1: Write failing UI tests**

```python
def test_header_has_feedback_button(self):
    response = client.get("/", auth=("admin", "secret"))
    elements = _nicegui_elements(response.text)
    header_descendants = list(_descendants(elements, header))
    self.assertTrue(any(
        element["tag"] == "q-btn" and element.get("props", {}).get("icon") == "rate_review"
        for element in header_descendants
    ))

def test_generated_tab_has_report_issue_action_next_to_copy_and_save(self):
    labels = labels_for_generated_tab_buttons(response.text)
    self.assertEqual([label for label in labels if label in {"Copy", "Save", "Report issue"}], ["Copy", "Save", "Report issue"])
```

Run: `python -m pytest tests/test_web_app.py -q`

Expected: FAIL because buttons/dialogs do not exist.

- [ ] **Step 2: Add dialogs and UI callbacks**

Add global feedback dialog:

- textarea label `Feedback`
- Send button `Send`
- on submit: validate non-empty, call `feedback_store_from_env().create_global_feedback(message)`, clear textarea, close dialog, notify positive.

Add analysis issue dialog:

- textarea label `What was recognized incorrectly?`
- optional filename input label `DAG filename` shown/used only when source type is `paste` and `dag_id` is missing or `Unknown`.
- Send button `Send issue`.
- on submit: validate context and message, compute source filename, call `create_analysis_issue_feedback`, clear textarea, close dialog, notify positive.

Add `Report issue` button in generated tab action row:

```python
report_issue_btn = ui.button("Report issue", icon="report_problem", on_click=open_analysis_issue_dialog)
report_issue_btn.disable()
```

Enable it after successful analysis and disable it when source changes enough to invalidate context.

- [ ] **Step 3: Update help text**

In the help markdown, add:

```markdown
**Обратная связь:** кнопка с иконкой сообщения в header открывает форму общих пожеланий. На вкладке Generated OMEntity кнопка Report issue сохраняет замечание по текущему анализу вместе со снимком DAG, сгенерированным OMEntity, Difference и метаданными анализа.
```

- [ ] **Step 4: Run UI tests**

Run: `python -m pytest tests/test_web_app.py -q`

Expected: PASS.

## Task 8: Full Verification, Commit, Push, Deploy

**Files:**
- Verify all modified files.
- Commit only files touched by this feedback feature.

- [ ] **Step 1: Run local tests**

Run:

```bash
python -m pytest tests/test_feedback_store.py tests/test_repository_browser.py tests/test_web_app.py -q
python -m py_compile web/feedback_store.py web/repository_browser.py web/app.py
```

Expected: all tests pass and `py_compile` exits 0.

- [ ] **Step 2: Run local smoke check**

Run with auth and temporary feedback dir:

```bash
AF_DAGS_HELPER_AUTH_USER=admin AF_DAGS_HELPER_AUTH_PASSWORD=secret AF_DAGS_HELPER_FEEDBACK_DIR=/tmp/af-dags-helper-feedback-smoke python -m web.app --host 127.0.0.1 --port 8010
```

Then check:

```bash
curl -u admin:secret http://127.0.0.1:8010/api/feedback/dag-issues
curl -u admin:secret http://127.0.0.1:8010/api/feedback/global
```

Expected: both return JSON with `items`.

- [ ] **Step 3: Commit scoped changes**

Run:

```bash
git status --short
git add web/feedback_store.py web/app.py web/repository_browser.py tests/test_feedback_store.py tests/test_web_app.py tests/test_repository_browser.py docs/superpowers/plans/2026-06-08-feedback-collection.md
git commit -m "feat: collect web feedback locally"
```

Do not add existing unrelated dirty files:

- `.claude/settings.json`
- `.claude/settings.local.json`
- `Dags for test/*`
- `Dags samples/*`
- `config/server_mapping.yaml`
- `CHECKPOINT.md`
- `scripts/deploy_ivm1.md`

- [ ] **Step 4: Push and deploy after local verification**

Run:

```bash
git push origin master
scripts/deploy_ivm1.sh
```

If WSL SSH cannot reach `ivm-1`, use the known tsh override:

```bash
AF_DAGS_HELPER_DEPLOY_HOST=igor.sterhov@ivm-1.ivms.vm.dm.v2.wb-cloud.ru
export AF_DAGS_HELPER_DEPLOY_HOST
ssh() { /mnt/c/Windows/System32/tsh17.exe ssh "$@"; }
source scripts/deploy_ivm1.sh
```

Expected: service restarts on `ivm-1`, `/health` remains OK, and feedback API routes are reachable behind auth.

## Self-Review

- Spec coverage: global feedback UI, analysis issue UI, SQLite/filesystem storage, separate DAG/global API routes, archive route, repo DAG snapshots, generated output and diff snapshots, pasted DAG filename fallback, auth protection, help text update, and local-before-deploy verification all have tasks.
- Placeholder scan: no task relies on placeholder language; implementation steps include exact files, method names, commands, and expected results.
- Type consistency: `AnalysisFeedbackContext`, `FeedbackStore.create_global_feedback`, `FeedbackStore.create_analysis_issue_feedback`, `list_feedback`, `mark_exported`, and `build_dag_issue_archive` names are consistent across tests, app integration, and API plan.
