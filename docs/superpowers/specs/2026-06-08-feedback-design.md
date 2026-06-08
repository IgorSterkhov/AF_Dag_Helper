# Feedback Collection Design

## Goal

Add local feedback collection to the FastAPI + NiceGUI web UI:

- global product feedback from the header;
- contextual feedback for incorrect DAG analysis from the `Generated OMEntity` result tab;
- local persistence with enough context to reproduce the reported analysis later, even if the application code or DAG repository changes.

## Non-Goals

- No external issue tracker integration.
- No email/notification delivery.
- No admin UI for feedback browsing in the first version.
- No VM deployment before local tests and local smoke checks pass.

## UI Design

### Global Feedback

Add a header button with icon `feedback` or `rate_review`, near Help and Settings.

Click opens a modal dialog:

- textarea for product ideas, wishes, and general notes;
- `Send` button;
- positive/negative NiceGUI notification after submit.

Global feedback is not tied to a DAG and does not create attachments in the first version.

### Analysis Issue Feedback

Add a button in the `Generated OMEntity` tab action row, to the right of `Copy` and `Save`:

```text
Copy | Save | Report issue
```

Button behavior:

- disabled until a successful Analyze run creates analysis context;
- opens a modal textarea asking what was recognized incorrectly;
- saves the note plus a full analysis snapshot.

For analysis issue feedback, always save attachments:

- DAG source snapshot;
- generated OMEntity tab content;
- Difference tab content;
- metadata JSON.

Optionally save warnings as `warnings.md` when warnings are present.

## Analysis Context Snapshot

After every successful Analyze, store a `last_analysis_context` in `WebState`:

- `source_type`: `repo`, `upload`, or `paste`;
- `dag_id`;
- `repo_name`, `dag_path`, `repo_commit` for repo DAGs;
- original filename for upload;
- source text that was analyzed;
- analysis options: `force_all_tasks`, `compare_existing`, `initial_view`;
- result summary: `task_count`, `output_count`, `warnings_count`;
- generated output text;
- difference text;
- warnings text.

The feedback submit flow must use this captured context, not re-read a repo file after the fact. This guarantees the saved feedback matches the analysis result shown on screen.

## Paste Filename Rules

For pasted DAG source:

1. Prefer `dag_id` extracted by the analyzer.
2. Save the DAG snapshot as `<dag_id>.py` when `dag_id` exists.
3. If `dag_id` is missing, do not block Analyze. When the user submits analysis feedback, show an additional modal field asking for a safe filename.
4. Validate the provided filename as a basename only; strip unsafe characters; add `.py` automatically.

## Storage

Use SQLite for metadata and a local attachments directory for files:

```text
.runtime/feedback/
  feedback.sqlite3
  attachments/
    2026/
      06/
        08/
          feedback-000001/
            dag_source__my_dag.py
            generated_omentity.py
            difference.md
            metadata.json
            warnings.md
```

Default root:

```text
<project>/.runtime/feedback
```

Allow override:

```text
AF_DAGS_HELPER_FEEDBACK_DIR
```

## Database Schema

### `feedback`

```text
id INTEGER PRIMARY KEY
created_at TEXT NOT NULL
type TEXT NOT NULL              -- global | analysis_issue
status TEXT NOT NULL            -- new | exported
message TEXT NOT NULL
source_type TEXT                -- repo | upload | paste | null
dag_id TEXT
repo_name TEXT
dag_path TEXT
repo_commit TEXT
original_filename TEXT
analysis_options_json TEXT
analysis_summary_json TEXT
exported_at TEXT
```

### `feedback_attachments`

```text
id INTEGER PRIMARY KEY
feedback_id INTEGER NOT NULL
kind TEXT NOT NULL              -- dag_source | generated_omentity | difference | warnings | metadata
filename TEXT NOT NULL
relative_path TEXT NOT NULL
content_type TEXT NOT NULL
sha256 TEXT NOT NULL
size_bytes INTEGER NOT NULL
created_at TEXT NOT NULL
FOREIGN KEY(feedback_id) REFERENCES feedback(id)
```

Rationale: analysis issue feedback has multiple files. A separate attachments table avoids widening the main feedback table and keeps future additions cheap.

## API Design

All feedback API routes are protected by the same auth middleware as the web UI. `/health` remains public.

Separate global feedback from DAG analysis feedback because day-to-day triage mostly needs DAG reports.

### DAG Analysis Feedback Metadata

```text
GET /api/feedback/dag-issues?mode=all|new&mark_exported=false|true
```

Returns JSON metadata for `type=analysis_issue` only. Each row includes attachment manifests, but not file contents.

`mode=new` filters to `status=new`.

`mark_exported=true` marks returned rows as `exported` only after the response payload has been prepared successfully. Default is `false`.

### DAG Analysis Feedback Archive

```text
GET /api/feedback/dag-issues/archive?mode=all|new&mark_exported=false|true
```

Returns a `tar.gz` archive:

```text
feedback.json
attachments/
  feedback-000001/
    dag_source__my_dag.py
    generated_omentity.py
    difference.md
    metadata.json
    warnings.md
```

Use `application/gzip` or `application/x-tar` with `.tar.gz` filename.

### Global Feedback Metadata

```text
GET /api/feedback/global?mode=all|new&mark_exported=false|true
```

Returns JSON metadata for `type=global` only. It has no archive endpoint in the first version because global feedback has no attachments.

## Backend Components

Create `web/feedback_store.py`.

Responsibilities:

- initialize SQLite schema;
- create global feedback;
- create analysis issue feedback and attachment files;
- list feedback by type/status;
- build DAG issue archive;
- mark exported records.

Keep `web/app.py` as UI composition and request wiring. Avoid putting SQLite or filesystem details directly into UI callbacks.

## Error Handling

- Empty feedback message: show validation warning; do not save.
- Missing analysis context for `Report issue`: disable button; if reached anyway, show warning.
- Missing pasted DAG filename when `dag_id` is unknown: ask for filename before saving.
- Attachment write failure: do not create partial visible feedback record; clean up partial attachment directory when possible.
- Archive creation failure: do not mark rows exported.

## Testing

Unit tests:

- `FeedbackStore` creates schema.
- Global feedback saves without attachments.
- Analysis issue feedback saves metadata and all required attachments.
- Attachment SHA256 and size are recorded.
- `mode=new` filters correctly.
- `mark_exported=true` changes status only for returned rows.
- Archive contains `feedback.json` and attachment files.

Web app/TestClient tests:

- Header has global feedback button.
- `Generated OMEntity` tab has `Report issue` next to `Copy` and `Save`.
- API routes require auth.
- DAG issue metadata route returns only analysis issues.
- Global route returns only global feedback.

Local smoke before deployment:

- Start local web server with auth env.
- Submit/upload or paste a DAG, run Analyze manually or via future E2E test.
- Submit analysis issue feedback.
- Verify SQLite row and attachments exist.
- Verify DAG issue archive endpoint returns a non-empty `.tar.gz`.

## Help Text

Because this changes UI/UX, update the in-app help dialog in the same implementation:

- explain the header feedback button;
- explain `Report issue` in `Generated OMEntity`;
- mention that DAG issue feedback saves the DAG snapshot and generated analysis result locally.
