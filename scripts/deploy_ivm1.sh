#!/usr/bin/env bash
set -euo pipefail

HOST="${AF_DAGS_HELPER_DEPLOY_HOST:-ivm-1}"
BRANCH="${AF_DAGS_HELPER_BRANCH:-master}"
REMOTE_URL="${AF_DAGS_HELPER_REMOTE_URL:-https://github.com/IgorSterkhov/AF_Dag_Helper.git}"
APP_DIR="${AF_DAGS_HELPER_APP_DIR:-/home/igor.sterhov/dev/af_dags_helper}"
PORT="${AF_DAGS_HELPER_PORT:-8000}"
SERVICE="af-dags-helper.service"

ssh "$HOST" bash -s -- "$APP_DIR" "$REMOTE_URL" "$BRANCH" "$PORT" "$SERVICE" <<'REMOTE'
set -euo pipefail

APP_DIR="$1"
REMOTE_URL="$2"
BRANCH="$3"
PORT="$4"
SERVICE="$5"

mkdir -p "$(dirname "$APP_DIR")"

if [ ! -d "$APP_DIR/.git" ]; then
  git clone "$REMOTE_URL" "$APP_DIR"
fi

cd "$APP_DIR"
git remote set-url origin "$REMOTE_URL"
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"

if ss -ltn "sport = :$PORT" | grep -q ":$PORT"; then
  if ! systemctl is-active --quiet "$SERVICE"; then
    echo "Port $PORT is already in use by another process"
    ss -ltnp "sport = :$PORT" || true
    exit 1
  fi
fi

python3 -m venv .venv
.venv/bin/python -m ensurepip --upgrade
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt

AUTH_ENV="$APP_DIR/.runtime/auth.env"
REPOS_DIR="$APP_DIR/repos"
mkdir -p "$(dirname "$AUTH_ENV")"
mkdir -p "$REPOS_DIR"
if [ ! -f "$AUTH_ENV" ]; then
  AUTH_PASSWORD="$("$APP_DIR/.venv/bin/python" -c 'import secrets; print(secrets.token_urlsafe(24))')"
  umask 077
  {
    printf 'AF_DAGS_HELPER_AUTH_USER=admin\n'
    printf 'AF_DAGS_HELPER_AUTH_PASSWORD=%s\n' "$AUTH_PASSWORD"
  } >"$AUTH_ENV"
  echo "Created auth credentials in $AUTH_ENV"
else
  echo "Using existing auth credentials in $AUTH_ENV"
fi
chmod 600 "$AUTH_ENV"

sudo tee "/etc/systemd/system/$SERVICE" >/dev/null <<UNIT
[Unit]
Description=AF DAGs Helper web UI
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$APP_DIR
Environment=AF_DAGS_HELPER_HOST=0.0.0.0
Environment=AF_DAGS_HELPER_PORT=$PORT
Environment=AF_DAGS_HELPER_REPOS_DIR=$REPOS_DIR
EnvironmentFile=$AUTH_ENV
ExecStart=$APP_DIR/.venv/bin/python -m web.app --host 0.0.0.0 --port $PORT
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE"
sudo systemctl restart "$SERVICE"
sudo systemctl --no-pager --full status "$SERVICE"

echo "AF DAGs Helper is deployed at: http://$(hostname -f):$PORT"
REMOTE
