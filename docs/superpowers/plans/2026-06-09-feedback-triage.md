# Feedback Triage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local CLI workflow that fetches DAG feedback archives from `ivm-1`, stores them under ignored `.runtime/feedback_inbox/`, and generates review reports comparing existing and generated OMEntity.

**Architecture:** Add one focused Python CLI at `scripts/feedback_triage.py` with `fetch` and `analyze` subcommands. Keep the CLI standard-library only so it works from normal WSL shell sessions, and add a thin `ivm1_ops.sh` wrapper plus `CLAUDE.md` workflow instructions.

**Tech Stack:** Python stdlib (`argparse`, `ast`, `subprocess`, `tarfile`, `json`, `shlex`, `datetime`, `urllib.parse`), Bash wrapper tests with `unittest`.

---

### Task 1: Feedback Triage CLI Fetch

**Files:**
- Create: `scripts/feedback_triage.py`
- Create: `tests/test_feedback_triage.py`

- [ ] Write a failing test that builds an in-memory tar archive, mocks `subprocess.run`, calls `fetch_feedback_archive(...)`, and asserts:
  - SSH command includes host and `bash -s`;
  - extracted `feedback.json` exists;
  - saved `dag-issues-feedback.tar.gz` exists;
  - `fetch.json` records `mode`, `mark_exported`, `host`, and `item_count`.
- [ ] Implement `fetch_feedback_archive(...)`:
  - build SSH command from `AF_DAGS_HELPER_SSH_COMMAND` or `ssh`;
  - remote script sources `$APP_DIR/.runtime/auth.env`;
  - remote script curls `/api/feedback/dag-issues/archive?mode=<mode>&mark_exported=<bool>`;
  - create unique run dir in `.runtime/feedback_inbox/`;
  - save archive, safely extract, write `fetch.json`;
  - print the run dir.
- [ ] Run `venv/Scripts/python.exe -m unittest tests.test_feedback_triage`.

### Task 2: Feedback Triage Analyze

**Files:**
- Modify: `scripts/feedback_triage.py`
- Modify: `tests/test_feedback_triage.py`

- [ ] Write a failing test that creates a fake extracted feedback run with:
  - `feedback.json`;
  - `dag_source__sample.py`;
  - `generated_omentity.py`;
  - `difference.md`;
  - `metadata.json`.
- [ ] The test calls `analyze_feedback_run(...)` and asserts `review.md` contains:
  - user message;
  - existing/generated OMEntity comparison;
  - diagnosis text for server mapping mismatch.
- [ ] Implement:
  - attachment lookup by `kind`;
  - existing OMEntity extraction from DAG source;
  - generated OMEntity parsing from attachment;
  - comparison and diagnosis bullets;
  - `review.md` writer.
- [ ] Run `venv/Scripts/python.exe -m unittest tests.test_feedback_triage`.

### Task 3: Ops Wrapper And Project Instructions

**Files:**
- Modify: `scripts/ivm1_ops.sh`
- Modify: `tests/test_ivm1_ops_script.py`
- Modify: `CLAUDE.md`

- [ ] Add direct command `feedback-fetch` to `scripts/ivm1_ops.sh`, which runs `scripts/feedback_triage.py fetch --mode new`.
- [ ] Update `--help` and interactive menu labels.
- [ ] Add tests that `--list` and `--help` include `feedback-fetch`.
- [ ] Add `CLAUDE.md` instructions: when asked to analyze new feedback, fetch, analyze, read `review.md`, propose code changes, do not mark exported or modify analyzer automatically.
- [ ] Run `bash -n scripts/ivm1_ops.sh` and `venv/Scripts/python.exe -m unittest tests.test_ivm1_ops_script`.

### Task 4: Verification And Scoped Commit

**Files:**
- Verify all changed files.

- [ ] Run:
  - `venv/Scripts/python.exe -m unittest tests.test_feedback_triage tests.test_ivm1_ops_script`
  - `venv/Scripts/python.exe -m py_compile scripts/feedback_triage.py`
  - `bash -n scripts/ivm1_ops.sh`
- [ ] Run a safe local no-export smoke when SSH is available:
  - `scripts/feedback_triage.py fetch --mode new`
  - `scripts/feedback_triage.py analyze <run-dir>`
- [ ] Commit only:
  - `scripts/feedback_triage.py`
  - `tests/test_feedback_triage.py`
  - `scripts/ivm1_ops.sh`
  - `tests/test_ivm1_ops_script.py`
  - `CLAUDE.md`
  - `docs/superpowers/specs/2026-06-09-feedback-triage-design.md`
  - `docs/superpowers/plans/2026-06-09-feedback-triage.md`

## Self-Review

- Spec coverage: fetch, local ignored storage, analyze report, future Codex workflow, SSH fallback, conservative export semantics, and no automatic code changes are covered.
- Placeholder scan: no placeholders or open-ended implementation instructions remain.
- Type consistency: `fetch_feedback_archive`, `analyze_feedback_run`, `.runtime/feedback_inbox`, and `feedback-fetch` are used consistently.
