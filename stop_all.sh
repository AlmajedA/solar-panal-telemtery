#!/usr/bin/env bash

SESSION_NAME="solar_system"

tmux kill-session -t $SESSION_NAME 2>/dev/null && \
echo "Stopped $SESSION_NAME" || \
echo "Session not running"