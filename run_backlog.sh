#!/bin/bash
# Backlog processor — runs independently of the daily pipeline.
# Handles the slow catch-up work that would timeout the daily run:
#   1. fetch_zenrows --backlog  : retry all 150+ previously-blocked detail pages (~30min)
#   2. enrich_location --backlog: Overpass enrichment for 105 missing listings (~20min)
#   3. enrich_commerce --backlog: commerce data for 23 missing listings (~10min)
#   4. enrich_listings          : re-score everything with fresh data
#
# Run manually or schedule via cron at a quiet time (e.g. 02:00 UTC).
# Does NOT interfere with the daily 08:00 pipeline.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/data/backlog_run.log"
LOCK_FILE="$SCRIPT_DIR/data/backlog.lock"

if [ -f "$LOCK_FILE" ]; then
    EXISTING_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [ -n "$EXISTING_PID" ] && kill -0 "$EXISTING_PID" 2>/dev/null; then
        echo "Backlog already running (pid $EXISTING_PID). Exiting."
        exit 0
    fi
    rm -f "$LOCK_FILE"
fi

# Load .env
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

echo $$ > "$LOCK_FILE"

cleanup() { rm -f "$LOCK_FILE"; }
trap cleanup EXIT

echo "🔄 Backlog processor started $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG_FILE"
echo "=================================================" | tee -a "$LOG_FILE"

run_step() {
    local name="$1"; shift
    echo "" | tee -a "$LOG_FILE"
    echo "▶ $name $*" | tee -a "$LOG_FILE"
    local t0=$(date +%s)
    if python3 "$SCRIPT_DIR/scraper/$name" "$@" >> "$LOG_FILE" 2>&1; then
        local elapsed=$(( $(date +%s) - t0 ))
        echo "  ✅ $name done in ${elapsed}s" | tee -a "$LOG_FILE"
    else
        local elapsed=$(( $(date +%s) - t0 ))
        echo "  ⚠️  $name exited non-zero after ${elapsed}s (continuing)" | tee -a "$LOG_FILE"
    fi
}

# Step 1: ZenRows detail pages for all blocked listings
# antibot=True, 3 workers, ~10-30s each → expect 30-60 min for 150 listings
echo "📋 Step 1/4: ZenRows detail fetch (backlog)..." | tee -a "$LOG_FILE"
run_step fetch_zenrows.py --backlog

# Step 2: Location enrichment for all missing listings
# 4 workers, Overpass queries → expect 20-40 min for 105 listings
echo "📍 Step 2/4: Location enrichment (backlog)..." | tee -a "$LOG_FILE"
run_step enrich_location.py --backlog

# Step 3: Commerce enrichment for all missing listings
echo "🛒 Step 3/4: Commerce enrichment (backlog)..." | tee -a "$LOG_FILE"
run_step enrich_commerce.py --backlog

# Step 4: Re-score and push to Sheets with all new data
echo "🧠 Step 4/4: Re-enrich + score + Sheets..." | tee -a "$LOG_FILE"
run_step enrich_listings.py

echo "" | tee -a "$LOG_FILE"
echo "✅ Backlog complete $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG_FILE"
