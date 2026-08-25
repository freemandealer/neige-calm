#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "$HOME/.cargo/env" ]]; then
    source "$HOME/.cargo/env"
fi

cd "$PROJECT_DIR"

CODEX_BIN="$(command -v codex-w || true)"
if [[ -z "$CODEX_BIN" ]]; then
    echo "start.sh: codex-w was not found in PATH" >&2
    exit 1
fi
export CALM_CODEX_BIN="$CODEX_BIN"
exec make prod PROD_LISTEN="${PROD_LISTEN:-0.0.0.0:4040}"
