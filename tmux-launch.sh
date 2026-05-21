#!/usr/bin/env bash

set -euo pipefail

SESSION_NAME="${1:-gossip-lab}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux is not installed or not on PATH."
    exit 1
fi

if tmux has-session -t "${SESSION_NAME}" 2>/dev/null; then
    tmux kill-session -t "${SESSION_NAME}"
fi

tmux new-session -d -s "${SESSION_NAME}" -n lab \
    "cd '${SCRIPT_DIR}' && go run server.go; exec bash"

sleep 1

for client_id in $(seq 1 8); do
    tmux split-window -t "${SESSION_NAME}:0" -d \
        "cd '${SCRIPT_DIR}' && go run client.go ${client_id}; exec bash"
    tmux select-layout -t "${SESSION_NAME}:0" tiled
done

tmux select-pane -t "${SESSION_NAME}:0.0"
tmux attach-session -t "${SESSION_NAME}"
