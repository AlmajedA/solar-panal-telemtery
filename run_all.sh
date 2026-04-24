#!/usr/bin/env bash

SESSION_NAME="solar_system"

# Start a new detached tmux session
tmux new-session -d -s $SESSION_NAME -n collector

# Window 1 — Central Collector
tmux send-keys -t $SESSION_NAME:collector "python central_collector/central_collector.py" C-m

# Window 2 — Alert Service
tmux new-window -t $SESSION_NAME -n alert
tmux send-keys -t $SESSION_NAME:alert "python alert_service/alert_service.py" C-m

# Window 3 — Edge Collector
tmux new-window -t $SESSION_NAME -n edge
tmux send-keys -t $SESSION_NAME:edge "python edge_collector/edge_collector.py" C-m

# Window 4 — Panel Adapter
tmux new-window -t $SESSION_NAME -n panel
tmux send-keys -t $SESSION_NAME:panel "python panel_adapter/panel_adapter.py --site Site-A --panels 10 --step 5 --minutes 10" C-m

# Window 5 — Dashboard Backend
tmux new-window -t $SESSION_NAME -n dashboard
tmux send-keys -t $SESSION_NAME:dashboard "uvicorn dashboard.backend:app --host 0.0.0.0 --port 8000 --reload" C-m

# Attach to session
tmux attach-session -t $SESSION_NAME