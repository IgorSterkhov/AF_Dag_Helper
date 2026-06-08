#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

HOST="${AF_DAGS_HELPER_DEPLOY_HOST:-ivm-1}"
APP_DIR="${AF_DAGS_HELPER_APP_DIR:-/home/igor.sterhov/dev/af_dags_helper}"
SERVICE="${AF_DAGS_HELPER_SERVICE:-af-dags-helper.service}"
PORT="${AF_DAGS_HELPER_PORT:-8000}"
LOG_LINES="${AF_DAGS_HELPER_LOG_LINES:-200}"
FOLLOW_LINES="${AF_DAGS_HELPER_FOLLOW_LINES:-100}"
DEPLOY_SCRIPT="${AF_DAGS_HELPER_DEPLOY_SCRIPT:-$SCRIPT_DIR/deploy_ivm1.sh}"

DIRECT_ACTIONS=(deploy status health logs follow restart version credentials ssh feedback-fetch)
MENU_ACTIONS=(deploy status health follow logs restart version credentials ssh feedback-fetch exit)
MENU_LABELS=(
  "Deploy changes"
  "Service status"
  "Health and auth check"
  "Follow logs"
  "Recent logs"
  "Restart service"
  "Version info"
  "Show web credentials"
  "Open SSH shell"
  "Fetch new DAG issue feedback"
  "Exit"
)

print_help() {
  cat <<HELP
AF DAGs Helper ivm-1 ops menu

Interactive:
  scripts/ivm1_ops.sh

Direct commands:
  scripts/ivm1_ops.sh deploy       Run scripts/deploy_ivm1.sh
  scripts/ivm1_ops.sh status       Show systemd service status
  scripts/ivm1_ops.sh health       Check /health, unauthenticated /, authenticated /
  scripts/ivm1_ops.sh logs         Show recent service logs
  scripts/ivm1_ops.sh follow       Follow service logs
  scripts/ivm1_ops.sh restart      Restart systemd service
  scripts/ivm1_ops.sh version      Show local and deployed git revisions
  scripts/ivm1_ops.sh credentials  Show deployed web UI credentials
  scripts/ivm1_ops.sh ssh          Open SSH shell on the VM
  scripts/ivm1_ops.sh feedback-fetch
                                   Fetch new DAG issue feedback into .runtime/feedback_inbox

Options:
  --list                           Print direct command names
  --help                           Show this help

Environment:
  AF_DAGS_HELPER_DEPLOY_HOST       Default: ivm-1
  AF_DAGS_HELPER_APP_DIR           Default: /home/igor.sterhov/dev/af_dags_helper
  AF_DAGS_HELPER_SERVICE           Default: af-dags-helper.service
  AF_DAGS_HELPER_PORT              Default: 8000
  AF_DAGS_HELPER_LOG_LINES         Default: 200
HELP
}

list_actions() {
  printf '%s\n' "${DIRECT_ACTIONS[@]}"
}

run_deploy() {
  "$DEPLOY_SCRIPT"
}

service_status() {
  ssh "$HOST" "systemctl --no-pager --full status '$SERVICE'"
}

recent_logs() {
  ssh "$HOST" "journalctl -u '$SERVICE' -n '$LOG_LINES' --no-pager"
}

follow_logs() {
  ssh -t "$HOST" "journalctl -u '$SERVICE' -f -n '$FOLLOW_LINES'"
}

restart_service() {
  ssh "$HOST" "sudo systemctl restart '$SERVICE' && systemctl --no-pager --full status '$SERVICE'"
}

health_check() {
  ssh "$HOST" bash -s -- "$APP_DIR" "$PORT" <<'REMOTE'
set -euo pipefail

APP_DIR="$1"
PORT="$2"
AUTH_ENV="$APP_DIR/.runtime/auth.env"

echo "GET /health"
curl -fsS "http://127.0.0.1:$PORT/health"
echo

no_auth_code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/")"
echo "GET / without auth: $no_auth_code (expected 401)"
if [ "$no_auth_code" != "401" ]; then
  exit 1
fi

if [ ! -f "$AUTH_ENV" ]; then
  echo "Missing auth env: $AUTH_ENV" >&2
  exit 1
fi

set -a
. "$AUTH_ENV"
set +a

cookie_jar="$(mktemp)"
trap 'rm -f "$cookie_jar"' EXIT

auth_code="$(
  curl -s -c "$cookie_jar" -o /dev/null -w '%{http_code}' \
    -u "$AF_DAGS_HELPER_AUTH_USER:$AF_DAGS_HELPER_AUTH_PASSWORD" \
    "http://127.0.0.1:$PORT/"
)"
echo "GET / with auth: $auth_code (expected 200)"
if [ "$auth_code" != "200" ]; then
  exit 1
fi

if ! grep -q 'af_dags_helper_auth' "$cookie_jar"; then
  echo "Missing af_dags_helper_auth session cookie" >&2
  exit 1
fi

cookie_code="$(curl -s -b "$cookie_jar" -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/")"
echo "GET / with session cookie: $cookie_code (expected 200)"
if [ "$cookie_code" != "200" ]; then
  exit 1
fi
REMOTE
}

version_info() {
  echo "Local HEAD:       $(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "Local origin:     $(git -C "$REPO_ROOT" rev-parse --short origin/master 2>/dev/null || echo unknown)"
  echo "Deployed HEAD:    $(ssh "$HOST" "git -C '$APP_DIR' rev-parse --short HEAD")"
  echo
  ssh "$HOST" "git -C '$APP_DIR' status --short --branch"
}

show_credentials() {
  echo "Credentials file: $APP_DIR/.runtime/auth.env"
  ssh "$HOST" "cat '$APP_DIR/.runtime/auth.env'"
}

open_shell() {
  ssh "$HOST"
}

feedback_fetch() {
  "$REPO_ROOT/scripts/feedback_triage.py" fetch \
    --host "$HOST" \
    --app-dir "$APP_DIR" \
    --port "$PORT" \
    --mode new
}

run_action() {
  case "$1" in
    deploy) run_deploy ;;
    status) service_status ;;
    health) health_check ;;
    logs) recent_logs ;;
    follow) follow_logs ;;
    restart) restart_service ;;
    version) version_info ;;
    credentials) show_credentials ;;
    ssh) open_shell ;;
    feedback-fetch) feedback_fetch ;;
    *)
      echo "Unknown action: $1" >&2
      echo "Run scripts/ivm1_ops.sh --help" >&2
      return 2
      ;;
  esac
}

pause_for_menu() {
  printf '\nPress Enter to return to menu...'
  IFS= read -r _
}

render_menu() {
  local selected="$1"
  clear
  echo "AF DAGs Helper ivm-1 ops"
  echo "Host: $HOST"
  echo "Service: $SERVICE"
  echo
  echo "Use Up/Down arrows, number keys, Enter, or q."
  echo

  local i
  for i in "${!MENU_LABELS[@]}"; do
    if [ "$i" -eq "$selected" ]; then
      printf ' > %2d. %s\n' "$((i + 1))" "${MENU_LABELS[$i]}"
    else
      printf '   %2d. %s\n' "$((i + 1))" "${MENU_LABELS[$i]}"
    fi
  done
}

interactive_menu() {
  if [ ! -t 0 ]; then
    echo "Interactive mode requires a TTY. Use --help for direct commands." >&2
    return 1
  fi

  local selected=0
  local key
  local action
  local index

  while true; do
    render_menu "$selected"
    IFS= read -rsn1 key || true

    case "$key" in
      $'\x1b')
        IFS= read -rsn2 -t 0.1 key || true
        case "$key" in
          "[A")
            selected=$((selected - 1))
            if [ "$selected" -lt 0 ]; then
              selected=$((${#MENU_LABELS[@]} - 1))
            fi
            ;;
          "[B")
            selected=$((selected + 1))
            if [ "$selected" -ge "${#MENU_LABELS[@]}" ]; then
              selected=0
            fi
            ;;
        esac
        ;;
      "")
        action="${MENU_ACTIONS[$selected]}"
        if [ "$action" = "exit" ]; then
          return 0
        fi
        clear
        run_action "$action"
        pause_for_menu
        ;;
      q|Q)
        return 0
        ;;
      [1-9])
        index=$((key - 1))
        if [ "$index" -ge 0 ] && [ "$index" -lt "${#MENU_ACTIONS[@]}" ]; then
          action="${MENU_ACTIONS[$index]}"
          if [ "$action" = "exit" ]; then
            return 0
          fi
          clear
          run_action "$action"
          pause_for_menu
        fi
        ;;
    esac
  done
}

main() {
  case "${1:-}" in
    "")
      interactive_menu
      ;;
    --help|-h)
      print_help
      ;;
    --list)
      list_actions
      ;;
    deploy|status|health|logs|follow|restart|version|credentials|ssh|feedback-fetch)
      run_action "$1"
      ;;
    *)
      echo "Unknown command: $1" >&2
      echo "Run scripts/ivm1_ops.sh --help" >&2
      return 2
      ;;
  esac
}

main "$@"
