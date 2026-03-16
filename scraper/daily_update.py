#!/usr/bin/env python3
"""
Daily Listings Updater v3 — JBizz Assistant 🦞

Pipeline order (each step skipped safely if it fails):
  1. scrape       — ZenRows → listings.json
  2. dedup        — Remove true duplicates (pHash + vision + text signals)
  3. geocode      — New listings only → geocoded.json (lat/lng/distances)
  4. zenrows_detail— New listings only → listing_details_zenrows.json
  5. location     — New listings only → geocoded.json (Overpass + schools + signals)
  6. commerce     — New listings only → commerce.json (supermarket, pharmacy, metro)
  7. image_batch  — New listings only → image_analysis.json (GPT-4o vision, batched)
  8. dom_tracker  — All listings      → dom_tracker.json (days on market, price Δ)
  9. enricher     — All listings      → enriched_listings.json + Google Sheets
 10. price_alerts — All listings      → Telegram alerts on price drops / new high scores
 11. cleanup      — All listings      → Remove confirmed-deleted listings
 12. monitor      — Health check      → Telegram alert if anything looks wrong

State is tracked in data/pipeline_state.json.
Run python3 pipeline_state.py --history for a summary.
"""

import json, csv, os, re, subprocess, sys, time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from pipeline_state import PipelineRun
from paths import PATHS

# ── Load environment variables (.env file takes priority, then openclaw.json) ─
def _load_env():
    """
    Load API keys from:
      1. .env file in the project root (most reliable — survives gateway restarts)
      2. openclaw.json env section (fallback — gets wiped by gateway updates)
    Keys already set in the OS environment are never overridden.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # 1. .env file (preferred — persists across gateway updates)
    dot_env = os.path.join(project_root, '.env')
    if os.path.exists(dot_env):
        try:
            with open(dot_env) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    key, _, val = line.partition('=')
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key and val and key not in os.environ:
                        os.environ[key] = val
        except Exception as e:
            print(f'⚠️  Could not load .env: {e}')

    # 2. openclaw.json env section (fallback)
    openclaw_cfg = os.path.normpath(os.path.join(project_root, '..', '..', 'openclaw.json'))
    if os.path.exists(openclaw_cfg):
        try:
            with open(openclaw_cfg) as f:
                cfg = json.load(f)
            for key, value in cfg.get('env', {}).items():
                if key not in os.environ:
                    os.environ[key] = value
        except Exception:
            pass

_load_env()

API_KEY    = os.environ.get('ZENROWS_API_KEY', 'a19f204d97b9578f8d82bd749ac175bd5383dd6e')
OPENAI_KEY = os.environ.get('OPENAI_API_KEY')
CREDS_FILE = PATHS.google_service_account
JSON_FILE  = PATHS.listings
CSV_FILE   = os.path.join(PATHS.data, 'listings.csv')
SHEET_URL  = PATHS.sheet_url
SHEET_NAME = PATHS.sheet_name
SCRIPT_DIR = PATHS.scraper

SHAPE    = '%28%28ovgzFrr%60t%40n%60%40omEsWm%5BzGe%7C%40vc%40oUjiA%60%40fHdfFciCjuA%29%29'
BASE_URL = f'https://www.idealista.pt/areas/arrendar-casas/com-preco-max_3100,preco-min_1650,t2,t3/?shape={SHAPE}'
MAX_PAGES = int(os.environ.get('PIPELINE_MAX_PAGES', '10'))  # override with PIPELINE_MAX_PAGES=1 for test runs

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


# ── ZenRows scraping ──────────────────────────────────────────────────────────

def fetch_page(url: str, retry: int = 2) -> str | None:
    params = {
        'apikey': API_KEY, 'url': url,
        'js_render': 'true', 'premium_proxy': 'true', 'wait': '2000',
    }
    for attempt in range(1, retry + 1):
        try:
            r = requests.get('https://api.zenrows.com/v1/', params=params, timeout=180)
            if r.status_code == 200 and len(r.text) > 10000:
                return r.text
            if r.status_code == 422:
                print(f'  ⚠️  ZenRows 422 (attempt {attempt}/{retry}) — retrying with longer wait')
                params['wait'] = '4000'
                time.sleep(3)
                continue
            print(f'  ⚠️  Page {r.status_code} / {len(r.text)}B (attempt {attempt}/{retry})')
            if attempt < retry:
                time.sleep(5)
        except requests.Timeout:
            print(f'  ⚠️  Timeout attempt {attempt}/{retry}')
            if attempt < retry:
                time.sleep(5)
        except requests.RequestException as e:
            print(f'  ⚠️  Fetch error: {e}')
            if attempt < retry:
                time.sleep(5)
    return None


def parse_price(text: str) -> int | None:
    if not text:
        return None
    nums = re.sub(r'[^\d]', '', text.split('€')[0])
    return int(nums) if nums else None


def parse_size(text: str) -> int | None:
    if not text:
        return None
    m = re.search(r'(\d+[\.,]?\d*)\s*m²', text)
    return int(float(m.group(1).replace(',', '.'))) if m else None


PORTO_ZONES = [
    'pinhais da foz', 'nevogilde', 'foz do douro', 'foz velha', 'foz', 'gondarém',
    'serralves', 'pinheiro manso', 'massarelos', 'boavista', 'aldoar',
    'lordelo do ouro', 'bessa leite', 'bessa', 'aviz', 'cristo rei', 'cedofeita',
    'bonfim', 'matosinhos', 'paranhos', 'ramalde', 'campanhã', 'baixa', 'porto',
]


def extract_street(title: str) -> str:
    m = re.search(r'(?:na|no|em)\s+([^,]+)', title)
    return m.group(1).strip() if m else ''


def extract_neighbourhood(title: str) -> str:
    """
    Extract neighbourhood from Idealista title, skipping house numbers and s/n.

    Titles follow the pattern:
      'Apartamento T2 na Rua X, [number_or_s/n], [neighbourhood], [full zone]'
      'Apartamento T3 na Rua X, [neighbourhood], [full zone]'

    The old code always took parts[1] which grabbed house numbers (13, 350, s/n)
    instead of the actual neighbourhood for ~73/157 listings.
    """
    parts = [p.strip() for p in title.split(',')]
    for p in parts[1:]:            # skip first part (street name with Rua/Av/...)
        if not p:
            continue
        if re.match(r'^\d+$|^s/n$', p, re.IGNORECASE):
            continue               # skip bare house numbers and s/n
        if len(p) < 3:
            continue
        pl = p.lower()
        # Prefer known Porto zones for reliable matching
        for z in PORTO_ZONES:
            if z in pl:
                return p
        # Otherwise return first non-numeric, non-short token
        return p
    return parts[-1].strip() if parts else ''


def parse_listing(article) -> dict:
    listing = {}
    listing['id']   = article.get('data-element-id', '')
    listing['city'] = 'Porto - Foz Zone'

    title_el = article.select_one('a.item-link')
    if title_el:
        listing['title'] = title_el.get('title', title_el.get_text(strip=True))
        href = title_el.get('href', '')
        listing['url'] = f'https://www.idealista.pt{href}' if href.startswith('/') else href
    else:
        listing['title'] = ''
        listing['url']   = f'https://www.idealista.pt/imovel/{listing["id"]}/'

    listing['street']       = extract_street(listing['title'])
    listing['neighborhood'] = extract_neighbourhood(listing['title'])

    price_el = article.select_one('.item-price')
    listing['price_raw'] = price_el.get_text(strip=True) if price_el else ''
    listing['price_eur'] = parse_price(listing['price_raw'])

    details = [d.get_text(strip=True) for d in article.select('span.item-detail')]
    listing['rooms']   = ''
    listing['size_m2'] = None
    listing['floor']   = ''
    for d in details:
        if re.match(r'^T\d$|^estúdio$', d, re.IGNORECASE):
            listing['rooms'] = d
        elif 'm²' in d:
            listing['size_m2'] = parse_size(d)
        elif re.search(r'andar|r/c|cave|piso', d, re.IGNORECASE):
            listing['floor'] = d

    tags, seen = [], set()
    for sel in ['span.listing-tags','span.item-parking','span.tag',
                'div.item-tags span','li.item-tag','span[class*="tag"]']:
        for t in article.select(sel):
            txt = t.get_text(strip=True)
            if txt and txt not in seen:
                seen.add(txt); tags.append(txt)
    raw = str(article).lower()
    if not any('garag' in t.lower() for t in tags) and ('garagem' in raw or 'lugar de garagem' in raw):
        tags.append('Garagem incluída')
    if not any('vista' in t.lower() for t in tags) and ('vista mar' in raw or 'sea view' in raw):
        tags.append('Vista mar')
    listing['tags'] = '|'.join(tags)

    desc_el = article.select_one('p.ellipsis')
    listing['description'] = desc_el.get_text(strip=True) if desc_el else ''

    photos = []
    for img in article.select('img[src], img[data-src]')[:4]:
        src = img.get('src') or img.get('data-src', '')
        if src and 'idealista' in src and src not in photos:
            photos.append(src)
    listing['photos']       = '|'.join(photos[:3])
    listing['date_scraped'] = datetime.now().strftime('%Y-%m-%d')
    return listing


def scrape_all() -> dict:
    all_listings = {}
    consecutive_empty = 0
    for page in range(1, MAX_PAGES + 1):
        url = BASE_URL if page == 1 else BASE_URL.replace('?', f'pagina-{page}?')
        print(f'  📄 Page {page}...', end=' ', flush=True)
        html = fetch_page(url)
        if not html:
            consecutive_empty += 1
            print(f'failed ({consecutive_empty} consecutive)')
            if consecutive_empty >= 2:
                print('  ⛔ 2 consecutive failures — stopping')
                break
            continue
        consecutive_empty = 0
        soup     = BeautifulSoup(html, 'lxml')
        articles = soup.select('article.item') or soup.select('article[data-element-id]')
        print(f'{len(articles)} listings', end='')
        if not articles:
            print(' — done.')
            break
        new = 0
        for article in articles:
            l = parse_listing(article)
            if l['id'] and l['id'] not in all_listings:
                all_listings[l['id']] = l; new += 1
        print(f' ({new} new)')
        if new == 0 and page > 1:
            print('  ℹ️  No new IDs — last page reached')
            break
        if page < MAX_PAGES:
            time.sleep(2)
    return all_listings


# ── Data I/O ─────────────────────────────────────────────────────────────────

def load_existing() -> dict:
    try:
        with open(JSON_FILE, encoding='utf-8') as f:
            return {l['id']: l for l in json.load(f) if l.get('id')}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_all(listings_dict: dict):
    listings = list(listings_dict.values())
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)
    fields = ['id','city','title','street','neighborhood','price_raw','price_eur',
              'rooms','size_m2','floor','tags','description','photos','date_scraped','url']
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(listings)


def append_to_sheet(new_listings: list):
    if not new_listings:
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
        gc    = gspread.authorize(creds)
        sh    = gc.open_by_url(SHEET_URL)
        try:
            ws = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)
        rows = [[
            l.get('id',''), l.get('city',''), l.get('rooms',''), l.get('price_eur',''),
            l.get('size_m2',''), l.get('floor',''), l.get('neighborhood',''), l.get('street',''),
            l.get('tags',''), l.get('description','')[:200], l.get('url',''),
            (l.get('photos','').split('|')[0] if l.get('photos') else ''),
            l.get('date_scraped',''),
        ] for l in new_listings]
        ws.append_rows(rows, value_input_option='RAW')
        print(f'  ✅ Appended {len(rows)} rows to Listings sheet')
    except Exception as e:
        print(f'  ⚠️  Sheet append failed: {e}')


# ── Pipeline step runner ──────────────────────────────────────────────────────

def run_script(name: str, timeout: int = 300, args: list | None = None) -> tuple[bool, str]:
    """
    Run a helper script with timeout.
    Returns (success: bool, error_msg: str).
    exit 2 = partial success (run_image_batch batching), treated as OK.
    """
    cmd = ['python3', os.path.join(SCRIPT_DIR, name)] + (args or [])
    print(f'  ▶ {name}' + (f' {" ".join(args)}' if args else ''), flush=True)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        for line in r.stdout.splitlines():
            if line.strip():
                print(f'    {line.strip()}', flush=True)
        if r.returncode not in (0, 2):
            err_detail = (r.stderr or r.stdout or '').strip()[-300:]
            print(f'  ⚠️  exited {r.returncode}', flush=True)
            if err_detail:
                print(f'  ⚠️  stderr: {err_detail[:300]}', flush=True)
            return False, err_detail or f'exit {r.returncode}'
        return True, ''
    except subprocess.TimeoutExpired:
        msg = f'timed out after {timeout}s'
        print(f'  ⚠️  {name} {msg}', flush=True)
        return False, msg
    except OSError as e:
        msg = f'could not start: {e}'
        print(f'  ⚠️  {name} {msg}', flush=True)
        return False, msg


# ── Main pipeline ─────────────────────────────────────────────────────────────

def main():
    run = PipelineRun.start('daily_update')
    new_listings: list = []
    merged: dict = {}

    # Trap SIGTERM (e.g. from outer `timeout` command or systemd) so finish()
    # is always called and pipeline_state.json never gets finished_at=None.
    import signal as _signal
    def _sigterm_handler(signum, frame):
        print('\n⚠️  SIGTERM received — writing partial finish to state', flush=True)
        run.finish(ok=False, error='killed by SIGTERM (outer timeout or systemd)')
        raise SystemExit(1)
    _signal.signal(_signal.SIGTERM, _sigterm_handler)

    try:
        return _run_pipeline(run, new_listings, merged)
    except SystemExit:
        raise  # propagate the clean exit from SIGTERM handler
    except Exception as exc:
        # Ensure finish() is always called even on unhandled exceptions
        run.step_fail('unhandled_exception', error=str(exc)[:200])
        run.finish(ok=False, error=str(exc)[:200])
        print(f'\n❌ Unhandled exception: {exc}')
        import traceback; traceback.print_exc()
        return 0


def _run_pipeline(run: 'PipelineRun', new_listings: list, merged: dict) -> int:
    print('🦞 JBizz Assistant — Daily Listings Update v3')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}')
    print('=' * 55)

    # ── Step 1: Scrape ────────────────────────────────────────────────────────
    print('\n🔍 [1/12] Scraping idealista.pt...')
    existing = load_existing()
    print(f'   Existing: {len(existing)} listings')
    scraped = scrape_all()
    print(f'   Scraped:  {len(scraped)} listings')

    if not scraped:
        run.step_fail('scrape', error='ZenRows returned 0 listings')
        run.finish(ok=False, total=len(existing))
        print('\n❌ SCRAPE FAILED — no listings returned')
        return 0

    new_ids      = set(scraped.keys()) - set(existing.keys())
    gone_ids     = set(existing.keys()) - set(scraped.keys())
    new_listings = [scraped[i] for i in new_ids]

    print(f'   🆕 New: {len(new_listings)} | Gone: {len(gone_ids)}')
    for l in new_listings[:5]:
        print(f'      • {l["rooms"]} €{l["price_eur"]}/mo {l["size_m2"]}m² {l["neighborhood"]}')

    merged = {**existing, **scraped}
    save_all(merged)
    run.step_ok('scrape', total=len(merged), new=len(new_listings), gone=len(gone_ids))

    if new_listings:
        append_to_sheet(new_listings)

    # ── Step 2: Dedup ─────────────────────────────────────────────────────────
    print('\n🧹 [2/12] Dedup...')
    ok, err = run_script('dedup.py', timeout=600)
    if ok:
        try:
            with open(JSON_FILE) as f:
                merged = {l['id']: l for l in json.load(f) if l.get('id')}
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        run.step_ok('dedup', listings_after=len(merged))
    else:
        run.step_fail('dedup', error=err or 'non-zero exit')

    # ── Step 3: Geocode new listings ──────────────────────────────────────────
    if new_listings:
        print('\n🗺️  [3/12] Geocode (new listings)...')
        ok, err = run_script('geocode.py', timeout=300)
        run.step_ok('geocode') if ok else run.step_fail('geocode', error=err)
    else:
        run.step_skip('geocode', reason='no new listings')

    # ── Step 4: ZenRows detail fetch (new listings) ───────────────────────────
    if new_listings:
        print('\n📋 [4/12] ZenRows detail fetch (new listings)...')
        ok, err = run_script('fetch_zenrows.py', timeout=1800)
        run.step_ok('fetch_zenrows') if ok else run.step_fail('fetch_zenrows', error=err)
    else:
        run.step_skip('fetch_zenrows', reason='no new listings')

    # ── Step 5: Location enrichment (new listings) ────────────────────────────
    if new_listings:
        print('\n📍 [5/12] Location enrichment (new listings)...')
        ok, err = run_script('enrich_location.py', timeout=1200)
        run.step_ok('enrich_location') if ok else run.step_fail('enrich_location', error=err)
    else:
        run.step_skip('enrich_location', reason='no new listings')

    # ── Step 6: Commerce enrichment (new listings) ────────────────────────────
    if new_listings:
        print('\n🛒 [6/12] Commerce enrichment (new listings)...')
        ok, err = run_script('enrich_commerce.py', timeout=600)
        run.step_ok('enrich_commerce') if ok else run.step_fail('enrich_commerce', error=err)
    else:
        run.step_skip('enrich_commerce', reason='no new listings')

    # ── Step 7: Image analysis (new listings — batched, SIGTERM-safe) ─────────
    if new_listings:
        print('\n🖼️  [7/12] Image analysis (new listings)...')
        ok, err = run_script('run_image_batch.py', timeout=600)
        run.step_ok('image_analysis') if ok else run.step_fail('image_analysis', error=err)
    else:
        run.step_skip('image_analysis', reason='no new listings')

    # ── Step 8: DOM tracker (all listings) ────────────────────────────────────
    print('\n📅 [8/12] DOM tracker...')
    ok, err = run_script('dom_tracker.py', timeout=60)
    run.step_ok('dom_tracker') if ok else run.step_fail('dom_tracker', error=err)

    # ── Step 9: Enrich + score + push to Sheets (all listings) ───────────────
    print('\n🧠 [9/12] Enrich + score + Sheets...')
    ok, err = run_script('enrich_listings.py', timeout=900)
    run.step_ok('enrich_listings') if ok else run.step_fail('enrich_listings', error=err)

    # ── Step 10: Price alerts ─────────────────────────────────────────────────
    print('\n💰 [10/12] Price alerts...')
    ok, err = run_script('price_tracker.py', timeout=60)
    run.step_ok('price_tracker') if ok else run.step_fail('price_tracker', error=err)

    # ── Step 11: Cleanup (remove confirmed-deleted) ───────────────────────────
    print('\n🗑️  [11/12] Cleanup...')
    ok, err = run_script('cleanup.py', args=['--apply'], timeout=60)
    run.step_ok('cleanup') if ok else run.step_fail('cleanup', error=err)

    # ── Step 12: Health monitor ───────────────────────────────────────────────
    print('\n🏥 [12/12] Health check...')
    ok, err = run_script('monitor.py', timeout=30)
    run.step_ok('monitor') if ok else run.step_fail('monitor', error=err or 'health check failed')

    # ── Finish ────────────────────────────────────────────────────────────────
    # Critical steps: if any of these fail, the pipeline is truly broken.
    # Non-critical steps (enrich_location, image_analysis, monitor) failing
    # means partial data but the core output (sheet, scoring) is still valid.
    CRITICAL_STEPS = {'scrape', 'dedup', 'dom_tracker', 'enrich_listings'}

    failed_steps     = [s for s in run.steps if s['status'] == 'fail']
    critical_failed  = [s for s in failed_steps if s['step'] in CRITICAL_STEPS]
    all_ok           = len(critical_failed) == 0  # OK if no critical failures

    run.finish(
        ok=all_ok,
        new_listings=len(new_listings),
        total_listings=len(merged),
        failed_steps=[s['step'] for s in failed_steps],
        critical_failures=[s['step'] for s in critical_failed],
    )

    status_icon = '✅' if all_ok else '⚠️ '
    print(f'\n{status_icon} Done — {len(new_listings)} new | {len(merged)} total')
    if failed_steps:
        print(f'   Non-critical failures (data may be partial): {[s["step"] for s in failed_steps]}')
    if critical_failed:
        print(f'   ❌ CRITICAL failures: {[s["step"] for s in critical_failed]}')

    return len(new_listings)


if __name__ == '__main__':
    main()
