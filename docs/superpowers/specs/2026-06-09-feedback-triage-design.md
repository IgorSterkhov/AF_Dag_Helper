# Feedback Triage Design

## Goal

Add a local workflow for Codex to fetch DAG analysis feedback from the deployed `ivm-1` web service, store it under ignored local runtime state, compare generated OMEntity with existing OMEntity in the attached DAG snapshot, and prepare a review report before proposing analyzer code changes.

## Approach

Use a project-local Python CLI instead of an MCP server or a Codex skill for the first version.

Recommended command flow:

```bash
scripts/feedback_triage.py fetch --mode new
scripts/feedback_triage.py analyze .runtime/feedback_inbox/<run-dir>
```

The fetch command calls the authenticated feedback archive endpoint from inside the VM over SSH. It sources the server-side `.runtime/auth.env` on the VM and runs local-to-VM `curl` against `127.0.0.1`, so credentials are not copied into the repository and do not need to be printed in chat.

The analyze command reads the extracted archive, parses the attached DAG source for existing OMEntity, parses the generated OMEntity attachment, compares both, and writes a local `review.md`. The triage CLI uses only the Python standard library so `fetch` and `analyze` work from the normal WSL shell without requiring the project virtualenv.

## Local Storage

Fetched feedback lives under:

```text
.runtime/feedback_inbox/<YYYYmmdd-HHMMSS>/
  dag-issues-feedback.tar.gz
  feedback.json
  fetch.json
  review.md
  attachments/
    feedback-000001/
      dag_source__...
      generated_omentity.py
      difference.md
      metadata.json
      warnings.md
```

`.runtime/` is already ignored by `.gitignore`, so this data is local-only and is not deployed back to the server.

## Export Semantics

Default fetch behavior is conservative:

- `--mode new`
- `--mark-exported` is off by default

Codex should first fetch and analyze feedback without marking it exported. Use `--mark-exported` only when explicitly requested or after confirming the local archive was saved and extracted successfully.

## SSH Behavior

Defaults:

- host: `AF_DAGS_HELPER_DEPLOY_HOST` or `ivm-1`
- app dir: `AF_DAGS_HELPER_APP_DIR` or `/home/igor.sterhov/dev/af_dags_helper`
- port: `AF_DAGS_HELPER_PORT` or `8000`
- SSH command: `AF_DAGS_HELPER_SSH_COMMAND` or `ssh`

If direct `ssh ivm-1` fails in WSL/Codex, use:

```bash
AF_DAGS_HELPER_SSH_COMMAND="/mnt/c/Windows/System32/tsh17.exe ssh" \
AF_DAGS_HELPER_DEPLOY_HOST="igor.sterhov@ivm-1.ivms.vm.dm.v2.wb-cloud.ru" \
scripts/feedback_triage.py fetch --mode new
```

## Review Report

`review.md` should include, for every DAG issue:

- feedback id and timestamp;
- user message;
- source metadata: source type, repo name, DAG path, repo commit, DAG id;
- attachment paths;
- comparison of existing DAG OMEntity and generated OMEntity;
- Difference attachment excerpt/reference;
- diagnosis bullets.

Diagnosis is heuristic in the first version. It should call out common causes:

- generated and existing FQN differ only by server prefix -> likely server mapping issue;
- generated is empty for a task that has existing OMEntity -> parser did not associate SQL/API with that task;
- key values differ -> key assignment mismatch;
- suspicious existing FQN has too many path parts -> existing DAG reference may be wrong;
- parser warnings were attached -> inspect warnings before changing analyzer code.

## Future Codex Workflow

When the user says “проанализируй новые замечания”:

1. Run `scripts/feedback_triage.py fetch --mode new`.
2. If direct SSH fails, retry with `AF_DAGS_HELPER_SSH_COMMAND="/mnt/c/Windows/System32/tsh17.exe ssh"` and the full VM host.
3. Run `scripts/feedback_triage.py analyze <printed-run-dir>`.
4. Read `review.md`.
5. Summarize the likely root cause and propose service code changes in chat.
6. Do not modify analyzer code until the proposal is discussed.

## Non-Goals

- No MCP server in the first version.
- No automatic analyzer code changes.
- No automatic deletion of local feedback inbox runs.
- No automatic `mark_exported=true` by default.
