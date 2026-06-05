# NiceGUI Web UI And VM Deploy Design

## Goal

Add a server-hosted web interface for AF DAGs Helper and a repeatable deployment path to `ivm-1`. The web UI will expose the existing DAG analysis, OMEntity generation, comparison output, and interactive lineage diagram through FastAPI + NiceGUI without replacing the existing Tkinter GUI or CLI.

## Current Context

The project currently has:

- CLI entry point in `main.py`.
- Desktop Tkinter GUI in `gui/app.py`.
- Analysis pipeline in `analyzer/`, `generator/`, and `visualizer/`.
- Interactive Cytoscape HTML diagram rendering in `visualizer/templates/dag_lineage.html`.
- `config/server_mapping.yaml` as a git-tracked mapping file.

The project does not currently have a long-running web server. The current HTML diagram is opened locally through `pywebview` or a temporary browser file.

On `ivm-1`:

- SSH alias `ivm-1` works.
- Deployment root should be under `~/dev/`.
- Existing `~/dev/ch_pipeliner` exists and was not running during inspection.
- Python 3.11 and git are available.
- GitHub access from the VM works.
- Passwordless `sudo` worked during inspection.

## Chosen Approach

Use FastAPI + NiceGUI with a shared analysis service:

- `web/app.py` owns the FastAPI app, NiceGUI UI, and `/health` endpoint.
- `web/analysis_service.py` owns reusable DAG analysis logic used by the web UI and suitable for Tkinter/CLI reuse if those entry points are refactored.
- `web/server_files.py` owns safe server-side DAG file discovery.
- `scripts/deploy_ivm1.sh` deploys from GitHub to `~/dev/af_dags_helper` and manages the service on `ivm-1`.

This keeps the server interface close to the existing pipeline while giving the VM a real HTTP process on a known port.

## UI Scope

The first web UI will use a hybrid workspace layout:

- Left/source panel:
  - Upload a `.py` DAG file.
  - Paste DAG source code.
  - Select a server-side DAG file from allowed project folders.
  - Toggle `Force all tasks`.
  - Toggle `Compare existing OMEntity`.
  - Select initial diagram view: DAG view or task view.
- Right/results panel:
  - Summary and warnings.
  - `Generated OMEntity` tab.
  - `Difference` tab.
  - `Text Diagram` tab.
  - `Interactive Diagram` tab using the existing Cytoscape graph data and template.
  - Actions to copy or download generated text.

For uploaded or pasted DAG source, the app writes a temporary file under `.runtime/uploads/` and analyzes that file. `.runtime/` is runtime state and must not be committed.

## Mapping Behavior

The web UI reads `config/server_mapping.yaml`, but it does not write to it.

Mapping changes remain a developer workflow:

- The UI may show generated mapping suggestions or warnings as text.
- The UI must not dirty the git worktree by editing `config/server_mapping.yaml`.
- Any mapping update should be made locally, reviewed, tested, committed, and deployed through git.

## Server File Safety

Server-side file selection is limited to project-owned DAG folders:

- `Dags samples/`
- `Dags for test/`

The server file browser must:

- Resolve paths against the project root.
- Reject path traversal.
- Only expose `.py` files.
- Return display paths relative to the project root.

## Analysis Service

The service should provide one primary API for web use:

- Input:
  - DAG path.
  - `force_all_tasks`.
  - `compare_existing`.
  - mapping file path.
  - initial diagram view.
- Output:
  - DAG id.
  - generated text output.
  - existing OMEntity and difference text when requested.
  - text diagram.
  - structured Cytoscape graph data.
  - warnings.
  - task count and output count.

The first implementation can keep CLI/Tkinter behavior unchanged while extracting shared analysis logic to avoid copying more code into the web layer.

## Deployment

Add `scripts/deploy_ivm1.sh`.

Expected behavior:

1. Run locally from the repository root.
2. SSH to `ivm-1`.
3. Create `~/dev/af_dags_helper` if missing.
4. Clone or update from `git@github.com:IgorSterkhov/AF_Dag_Helper.git` or HTTPS if that is the available VM remote.
5. Reset the VM checkout to the requested branch, default `master`.
6. Create or update `.venv`.
7. Install `requirements.txt`.
8. Install or update a systemd service named `af-dags-helper.service`.
9. Start/restart the service.
10. Print service status and URL.

The service should run:

```bash
python -m web.app --host 0.0.0.0 --port 8000
```

The deploy script must check that the target port is not already used by an unrelated process before starting the service. It must not stop or modify `ch_pipeliner`.

## Runtime Configuration

Defaults:

- Host: `0.0.0.0`
- Port: `8000`
- Project root: repository root
- Runtime directory: `.runtime/`
- Mapping file: `config/server_mapping.yaml`

Environment overrides:

- `AF_DAGS_HELPER_HOST`
- `AF_DAGS_HELPER_PORT`
- `AF_DAGS_HELPER_RUNTIME_DIR`
- `AF_DAGS_HELPER_MAPPING_FILE`

## Dependencies

Update `requirements.txt` with:

- `fastapi`
- `nicegui`
- `uvicorn`
- `python-multipart`

Keep existing dependencies:

- `sqlglot`
- `pyyaml`

## Testing

Add focused tests around the new service layer:

- Analyze server-side fixture DAG with force mode enabled.
- Analyze pasted/uploaded source through a temporary file path.
- Verify compare mode includes existing OMEntity difference text for a sample DAG.
- Verify Cytoscape graph data is present when outputs exist.
- Verify server file discovery only returns allowed `.py` files.
- Verify path traversal is rejected.
- Verify `/health` returns success.

Run existing smoke checks:

- `python -m py_compile` for changed Python files.
- `python test_against_samples.py`.

`test_against_samples.py` is considered a smoke regression when it exits `0`; existing reported DAG mismatches are not a failure for this feature.

## Out Of Scope

The first version will not:

- Edit `config/server_mapping.yaml` from the browser.
- Provide authentication.
- Provide persistent user sessions beyond NiceGUI runtime state.
- Replace or remove the Tkinter UI.
- Deploy or manage `ch_pipeliner`.
- Add a reverse proxy or TLS.

## Open Decisions Resolved

- Workflow: Hybrid workspace.
- Mapping behavior: read-only in UI.
- Deployment target: `~/dev/af_dags_helper` on `ivm-1`.
- Default web port: `8000`.
- Existing GUI: keep Tkinter and CLI working.
