#!/bin/bash
# Detached pipeline runner — called by cron agent, runs in background, exits immediately.
# The pipeline writes its own log and state; agent checks results afterward.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/data/pipeline_run.log"
PID_FILE="$SCRIPT_DIR/data/pipeline.pid"
LOCK_FILE="$SCRIPT_DIR/data/pipeline.lock"

# Load .env file if present
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/.env"
    set +a
fi

# Prevent double-run
if [ -f "$LOCK_FILE" ]; then
    EXISTING_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [ -n "$EXISTING_PID" ] && kill -0 "$EXISTING_PID" 2>/dev/null; then
        echo "Pipeline already running (pid $EXISTING_PID). Exiting."
        exit 0
    fi
    rm -f "$LOCK_FILE"
fi

# Write lock file
echo $$ > "$LOCK_FILE"

# Rotate log
[ -f "$LOG_FILE" ] && mv "$LOG_FILE" "${LOG_FILE}.prev" 2>/dev/null || true

# Run in background, fully detached from parent session
nohup python3 "$SCRIPT_DIR/scraper/daily_update.py" \
    > "$LOG_FILE" 2>&1 &

PIPELINE_PID=$!
echo $PIPELINE_PID > "$PID_FILE"
echo $PIPELINE_PID > "$LOCK_FILE"

echo "Pipeline launched: pid=$PIPELINE_PID log=$LOG_FILE"
echo "Check progress: bash $SCRIPT_DIR/check_pipeline.sh"
