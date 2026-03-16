#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/data/pipeline.pid"
LOG_FILE="$SCRIPT_DIR/data/pipeline_run.log"
LOCK_FILE="$SCRIPT_DIR/data/pipeline.lock"

if [ ! -f "$PID_FILE" ]; then
    echo "NOT_STARTED — no pid file"
    exit 1
fi

PID=$(cat "$PID_FILE")

# Check if python3 daily_update.py is still running (not just a zombie shell)
if ps -p "$PID" -o stat= 2>/dev/null | grep -qv Z; then
    LINES=$(wc -l < "$LOG_FILE" 2>/dev/null || echo 0)
    echo "RUNNING (pid=$PID, log=${LINES} lines)"
    tail -5 "$LOG_FILE" 2>/dev/null
    exit 2   # still running
fi

# Process done — clean up and show result
rm -f "$LOCK_FILE" "$PID_FILE"
echo "FINISHED"
echo ""
# Show summary from pipeline state
python3 -c "
import json
try:
    with open('$SCRIPT_DIR/data/pipeline_state.json') as f: state = json.load(f)
    runs = [r for r in state['runs'] if r.get('finished_at')]
    if not runs: print('No finished runs found'); exit(1)
    last = max(runs, key=lambda r: r['finished_at'])
    steps = last.get('steps', [])
    new = next((s.get('new',0) for s in steps if s['step']=='scrape'), 0)
    total = next((s.get('total',0) for s in steps if s['step']=='scrape'), 0)
    failed = [s['step'] for s in steps if s['status']=='fail']
    print(f'Run: {last[\"run_id\"]}')
    print(f'OK: {last[\"ok\"]}  New: {new}  Total: {total}')
    if failed: print(f'Failed: {failed}')
    else: print('All steps passed ✅')
except Exception as e:
    print(f'Error reading state: {e}')
" 2>&1
exit 0
