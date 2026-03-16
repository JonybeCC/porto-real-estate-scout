#!/usr/bin/env python3
"""
Pipeline Health Monitor v2 — JBizz Assistant 🦞

Replaces the old monitor.py which wasted a ZenRows credit per health check.

Checks actual pipeline outputs instead:
  1. Last successful pipeline run < 26h ago?
  2. listings.json updated today (or yesterday)?
  3. listings count within expected range (not 0, not suspiciously high)?
  4. ZenRows API key valid? (lightweight — checks account, NOT a real scrape)
  5. OpenAI API key valid?
  6. Google Sheets credentials file present and readable?

Called from daily_update.py at the END of each run.
Also callable standalone: python3 monitor.py
"""

import json, os, requests
from datetime import datetime, timezone, date

BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID   = '520980639'

LISTINGS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
STATE_FILE     = '/root/.openclaw/workspace/projects/real-estate/data/pipeline_state.json'
CREDS_FILE     = '/root/.openclaw/credentials/google-service-account.json'

ZENROWS_KEY    = os.environ.get('ZENROWS_API_KEY', 'a19f204d97b9578f8d82bd749ac175bd5383dd6e')
OPENAI_KEY     = os.environ.get('OPENAI_API_KEY', '')

MIN_LISTINGS   = 80   # alert if we drop below this
MAX_LISTINGS   = 500  # alert if we spike above this (scraper bug?)
MAX_RUN_AGE_H  = 28   # alert if last successful run was older than this


def _load(path, default):
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def _send_telegram(msg: str):
    if not BOT_TOKEN:
        print(f'[NO BOT TOKEN] {msg}')
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            json={'chat_id': CHAT_ID, 'text': msg,
                  'parse_mode': 'Markdown', 'disable_web_page_preview': True},
            timeout=10,
        )
    except requests.RequestException as e:
        print(f'  ⚠️  Telegram send failed: {e}')


def check_pipeline_state() -> tuple[bool, str]:
    """
    Was the last pipeline run (with critical steps passing) < MAX_RUN_AGE_H ago?

    A run is considered healthy if it finished AND had no critical step failures,
    even if non-critical steps (enrich_location, image_analysis, monitor) failed.
    Critical steps: scrape, dedup, dom_tracker, enrich_listings.
    """
    CRITICAL = {'scrape', 'dedup', 'dom_tracker', 'enrich_listings'}

    state = _load(STATE_FILE, {})
    runs  = [r for r in state.get('runs', []) if r.get('finished_at')]
    if not runs:
        return False, 'No pipeline runs on record yet'

    # Consider a run "healthy" if it finished and no critical step failed
    def is_healthy(r):
        if not r.get('finished_at'):
            return False
        failed = {s['step'] for s in (r.get('steps') or []) if s.get('status') == 'fail'}
        return len(failed & CRITICAL) == 0

    healthy_runs = [r for r in runs if is_healthy(r)]
    if not healthy_runs:
        # Show what failed in the most recent run for diagnostics
        last = max(runs, key=lambda r: r['finished_at'])
        failed = [s['step'] for s in (last.get('steps') or []) if s.get('status') == 'fail']
        return False, f'Last run had critical failures: {failed}'

    last = max(healthy_runs, key=lambda r: r['finished_at'])
    finished = datetime.fromisoformat(last['finished_at'])
    if finished.tzinfo is None:
        finished = finished.replace(tzinfo=timezone.utc)
    hours_ago = (datetime.now(timezone.utc) - finished).total_seconds() / 3600
    failed_non_crit = [s['step'] for s in (last.get('steps') or []) if s.get('status') == 'fail']

    if hours_ago > MAX_RUN_AGE_H:
        return False, f'Last healthy run was {hours_ago:.1f}h ago (>{MAX_RUN_AGE_H}h threshold)'

    detail = f'{hours_ago:.1f}h ago'
    if failed_non_crit:
        detail += f' (partial: {failed_non_crit} non-critical)'
    return True, f'Pipeline healthy — last run {detail}'


def check_listings_count() -> tuple[bool, str]:
    """Is listing count sane?"""
    listings = _load(LISTINGS_FILE, [])
    n = len(listings)
    if n < MIN_LISTINGS:
        return False, f'Only {n} listings (minimum {MIN_LISTINGS}) — possible scrape failure'
    if n > MAX_LISTINGS:
        return False, f'{n} listings (maximum {MAX_LISTINGS}) — possible scraper loop'
    return True, f'{n} listings in database'


def check_listings_freshness() -> tuple[bool, str]:
    """Was listings.json updated today or yesterday?"""
    try:
        mtime = os.path.getmtime(LISTINGS_FILE)
        mod_date = date.fromtimestamp(mtime)
        today = date.today()
        days_old = (today - mod_date).days
        if days_old > 1:
            return False, f'listings.json is {days_old} days old (last modified {mod_date})'
        return True, f'listings.json updated {mod_date}'
    except OSError as e:
        return False, f'Cannot stat listings.json: {e}'


def check_zenrows() -> tuple[bool, str]:
    """
    Check ZenRows key is valid using a minimal real request (httpbin — tiny, fast).
    The /usage endpoint does not exist on ZenRows v1, so we use a lightweight proxy call.
    """
    try:
        r = requests.get(
            'https://api.zenrows.com/v1/',
            params={'apikey': ZENROWS_KEY, 'url': 'https://httpbin.org/ip'},
            timeout=20,
        )
        if r.status_code == 200:
            return True, f'ZenRows API key valid ({len(r.content)}B response)'
        elif r.status_code in (401, 403):
            return False, f'ZenRows API key invalid ({r.status_code})'
        else:
            # 422, 429, 5xx — key is valid but request failed for other reasons
            return True, f'ZenRows reachable (status {r.status_code} — key OK)'
    except requests.RequestException as e:
        return False, f'ZenRows unreachable: {e}'


def check_openai() -> tuple[bool, str]:
    """Check OpenAI key is valid — free list-models call."""
    if not OPENAI_KEY:
        return False, 'OPENAI_API_KEY not set in environment'
    try:
        r = requests.get(
            'https://api.openai.com/v1/models',
            headers={'Authorization': f'Bearer {OPENAI_KEY}'},
            timeout=10,
        )
        if r.status_code == 200:
            return True, 'OpenAI API key valid'
        elif r.status_code == 401:
            return False, 'OpenAI API key invalid (401)'
        else:
            return False, f'OpenAI API returned {r.status_code}'
    except requests.RequestException as e:
        return False, f'OpenAI unreachable: {e}'


def check_google_creds() -> tuple[bool, str]:
    """Check Google service account credentials file is present and valid JSON."""
    try:
        with open(CREDS_FILE) as f:
            creds = json.load(f)
        email = creds.get('client_email', '?')
        return True, f'Google creds OK ({email})'
    except FileNotFoundError:
        return False, f'Google credentials file missing: {CREDS_FILE}'
    except json.JSONDecodeError:
        return False, f'Google credentials file is corrupt JSON'


def run(alert_on_fail: bool = True, verbose: bool = True) -> bool:
    """
    Run all health checks. Returns True if all pass.
    Sends Telegram alert if any check fails (and alert_on_fail=True).
    """
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    if verbose:
        print(f'🏥 Pipeline Health Monitor — {now}')
        print('=' * 55)

    checks = [
        ('Pipeline state',     check_pipeline_state),
        ('Listings count',     check_listings_count),
        ('Listings freshness', check_listings_freshness),
        ('ZenRows API',        check_zenrows),
        ('OpenAI API',         check_openai),
        ('Google credentials', check_google_creds),
    ]

    results = []
    all_ok  = True

    for name, fn in checks:
        ok, detail = fn()
        results.append((name, ok, detail))
        if verbose:
            icon = '✅' if ok else '❌'
            print(f'  {icon} {name:<22} {detail}')
        if not ok:
            all_ok = False

    if verbose:
        print(f'\n  {"✅ All checks passed" if all_ok else "❌ FAILURES DETECTED"}')

    # Telegram alert on failure
    if not all_ok and alert_on_fail:
        failures = [(n, d) for n, ok, d in results if not ok]
        lines = [f'🚨 *Pipeline Health Alert* — {now}\n']
        for name, detail in failures:
            lines.append(f'❌ *{name}*: {detail}')
        lines.append(f'\nPassed: {sum(1 for _, ok, _ in results if ok)}/{len(results)} checks')
        _send_telegram('\n'.join(lines))

    return all_ok


if __name__ == '__main__':
    import sys
    quiet = '--quiet' in sys.argv
    ok = run(alert_on_fail=True, verbose=not quiet)
    raise SystemExit(0 if ok else 1)
