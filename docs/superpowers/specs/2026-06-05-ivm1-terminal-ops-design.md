# ivm-1 Terminal Ops Design

## Goal

Add a local terminal interface for routine management of the AF DAGs Helper service deployed on `ivm-1`.

The interface must let the user run one local script and choose operational actions such as deploy, service status, logs, health checks, restart, deployed version, credentials, and SSH shell.

## Approach

Create one bash script: `scripts/ivm1_ops.sh`.

The script runs locally from the repository and calls existing project operations through SSH. It does not install an agent on the VM and does not replace `scripts/deploy_ivm1.sh`; deploy remains delegated to the existing script.

The script supports two modes:

- Interactive TUI: `scripts/ivm1_ops.sh`
- Direct subcommand mode: `scripts/ivm1_ops.sh health`, `scripts/ivm1_ops.sh deploy`, etc.

Interactive mode uses bash-only arrow navigation, so it does not require `dialog`, `whiptail`, or `fzf`. Direct mode makes the tool testable and usable in aliases or future automation.

## Configuration

Defaults:

- Host: `ivm-1`
- App dir: `/home/igor.sterhov/dev/af_dags_helper`
- Service: `af-dags-helper.service`
- Port: `8000`

Environment overrides:

- `AF_DAGS_HELPER_DEPLOY_HOST`
- `AF_DAGS_HELPER_APP_DIR`
- `AF_DAGS_HELPER_SERVICE`
- `AF_DAGS_HELPER_PORT`
- `AF_DAGS_HELPER_LOG_LINES`

## Menu Actions

1. Deploy changes - calls `scripts/deploy_ivm1.sh`.
2. Service status - runs `systemctl --no-pager --full status af-dags-helper.service`.
3. Health and auth check - verifies `/health`, `/` without auth (`401` expected), `/` with credentials from `.runtime/auth.env` (`200` expected), and `/` with only the issued session cookie (`200` expected).
4. Follow logs - runs `journalctl -u af-dags-helper.service -f -n 100`.
5. Recent logs - runs `journalctl -u af-dags-helper.service -n 200 --no-pager`.
6. Restart service - runs `sudo systemctl restart af-dags-helper.service`, then status.
7. Version info - shows local `HEAD`, local `origin/master`, and deployed VM `HEAD`.
8. Show credentials - prints `.runtime/auth.env` from the VM.
9. Open SSH shell - runs `ssh ivm-1`.
10. Exit.

## Error Handling

The script uses `set -euo pipefail` so failed SSH, curl, git, or deploy commands stop the current action.

Interactive mode pauses after each action and returns to the menu. Direct mode exits with the action status code.

The health/auth check is explicit about expected status codes:

- `/health` must return a successful response.
- `/` without auth must return `401`.
- `/` with credentials must return `200`.
- `/` with the session cookie issued after Basic Auth must return `200`.

If credentials are missing, the health/auth check reports it and returns non-zero.

## Testing

Add Python unittest coverage for no-network behavior:

- `scripts/ivm1_ops.sh --list` returns the expected action names.
- `scripts/ivm1_ops.sh --help` documents interactive and direct usage.
- `bash -n scripts/ivm1_ops.sh` passes.

Network/VM actions are verified manually through the direct subcommands after implementation:

- `scripts/ivm1_ops.sh health`
- `scripts/ivm1_ops.sh version`
- `scripts/ivm1_ops.sh status`

## Documentation

Update `README.md` and `CLAUDE.md` with the new ops menu entrypoint and common direct commands.
