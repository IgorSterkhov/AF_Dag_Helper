# Repo DAG Browser Design

## Goal

Add server-side DAG repository support to the NiceGUI web UI so users can register existing git repositories from a server `repos` directory, update them with `git pull`, browse nested DAG files, and analyze the selected DAG with the existing analysis options.

## Repository Scope

Repositories are existing directories under `AF_DAGS_HELPER_REPOS_DIR`, defaulting to `~/repos` locally and `/home/igor.sterhov/repos` in the ivm-1 deployment. The UI never clones remote URLs and never operates outside that root. A folder is considered addable when it is a direct child of the repos root and contains `.git`.

Registered repositories are stored in `<runtime_dir>/repositories.json`. The stored value is only the repository directory name, not an arbitrary path. Removing a repository unregisters it from the web UI and does not delete files on disk.

## UI

The header gets a settings button with a gear icon. Settings opens a modal containing registered repositories, discovered addable folders, and actions: refresh, add selected folder, remove selected repository, git pull selected, and git pull all.

The source tab `Server file` is renamed to `Repo`. It contains repository selection, a DAG tree for the selected repository, a selected DAG label, and refresh/pull actions. The tree preserves folder hierarchy and only `.py` files are selectable for analysis. Upload and Paste tabs remain unchanged.

## Data Flow

`RepositoryBrowser` owns repository discovery, registration persistence, DAG tree generation, path resolution, and `git pull` execution. `web.app` uses it for UI state and passes the resolved DAG path into the existing `DAGAnalysisService`. Existing options such as Force all tasks and Compare existing OMEntity continue to apply.

## Safety

All filesystem resolution checks use `Path.resolve()` and `relative_to()` against the configured repos root and selected repository root. `git pull` uses `subprocess.run(["git", "-C", repo_path, "pull", "--ff-only"], ...)` without shell. Path traversal and non-`.py` DAG selection are rejected.
