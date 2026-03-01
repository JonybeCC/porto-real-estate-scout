#!/usr/bin/env python3
"""
Daily Listings Updater v2 — JBizz Assistant 🦞
Fix: removed json_response=true (was returning 14KB JSON instead of full HTML)
     added wait=2000 for JS rendering
     improved article selector fallbacks
     better new vs existing dedup logic
"""

import json, csv, time, re, subprocess
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

API_KEY    = 'a19f204d97b9578f8d82bd749ac175bd5383dd6e'
CREDS_FILE = '/root/.openclaw/credentials/google-service-account.json'
JSON_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
CSV_FILE   = '/root/.openclaw/workspace/projects/real-estate/data/listings.csv'
LOG_FILE   = '/root/.openclaw/workspace/projects/real-estate/data/update_log.json'
SHEET_URL  = 'https://docs.google.com/spreadsheets/d/1Ljk9s45BovbRfk1QIUGgGxbjnpIgs2F52rJjDNGQOQI/edit'
SHEET_NAME = 'Listings'
SCRIPT_DIR = '/root/.openclaw/workspace/projects/real-estate/scraper'

SHAPE    = '%28%28ovgzFrr%60t%40n%60%40omEsWm%5BzGe%7C%40vc%40oUjiA%60%40fHdfFciCjuA%29%29'
BASE_URL = f'https://www.idealista.pt/areas/arrendar-casas/com-preco-max_3100,preco-min_1650,t2,t3/?shape={SHAPE}'
MAX_PAGES = 10

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']


def fetch_page(url: str) -> str | None:
    """Fetch a listing page via ZenRows. Returns raw HTML string."""
    params = {
        'apikey': API_KEY,
        'url': url,
        'js_render': 'true',
        'premium_proxy': 'true',
        'wait': '2000',          # ← wait 2s for JS to render listings
        # NOTE: do NOT set json_response=true — it returns truncated JSON, not full HTML
    }
    try:
        r = requests.get('https://api.zenrows.com/v1/', params=params, timeout=180)
        if r.status_code == 200 and len(r.text) > 10000:
            return r.text
        print(f'  ⚠️  Page returned {r.status_code} / {len(r.text)}B — skipping')
        return None
    except Exception as e:
        print(f'  ⚠️  Fetch error: {e}')
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


def extract_neighborhood(title: str) -> str:
    parts = title.split(',')
    return parts[1].strip() if len(parts) > 1 else ''


def extract_street(title: str) -> str:
    m = re.search(r'(?:na|no|em)\s+([^,]+)', title)
    return m.group(1).strip() if m else ''


def parse_listing(article) -> dict:
    listing = {}
    listing['id'] = article.get('data-element-id', '')
    listing['city'] = 'Porto - Foz Zone'

    title_el = article.select_one('a.item-link')
    if title_el:
        listing['title'] = title_el.get('title', title_el.get_text(strip=True))
        href = title_el.get('href', '')
        listing['url'] = f'https://www.idealista.pt{href}' if href.startswith('/') else href
    else:
        listing['title'] = ''
        listing['url'] = f'https://www.idealista.pt/imovel/{listing["id"]}/'

    listing['street']       = extract_street(listing['title'])
    listing['neighborhood'] = extract_neighborhood(listing['title'])

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

    # Tags: garage, sea view, elevator etc.
    tags, seen_tags = [], set()
    for sel in ['span.listing-tags', 'span.item-parking', 'span.tag',
                'div.item-tags span', 'li.item-tag', 'span[class*="tag"]']:
        for t in article.select(sel):
            txt = t.get_text(strip=True)
            if txt and txt not in seen_tags:
                seen_tags.add(txt)
                tags.append(txt)
    raw_html = str(article).lower()
    if not any('garag' in t.lower() for t in tags) and ('garagem' in raw_html or 'lugar de garagem' in raw_html):
        tags.append('Garagem incluída')
    if not any('vista' in t.lower() for t in tags) and ('vista mar' in raw_html or 'sea view' in raw_html):
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
    """Scrape all pages, return listings dict keyed by ID."""
    all_listings = {}
    consecutive_empty = 0

    for page in range(1, MAX_PAGES + 1):
        url = BASE_URL if page == 1 else BASE_URL.replace('?', f'pagina-{page}?')
        print(f'  📄 Page {page}...', end=' ', flush=True)

        html = fetch_page(url)
        if not html:
            consecutive_empty += 1
            print(f'fetch failed ({consecutive_empty} in a row)')
            if consecutive_empty >= 2:
                print('  ⛔ 2 consecutive failures — stopping pagination')
                break
            continue

        consecutive_empty = 0
        soup = BeautifulSoup(html, 'lxml')

        # Try multiple selectors (Idealista occasionally changes structure)
        articles = soup.select('article.item') or soup.select('article[data-element-id]')
        print(f'{len(articles)} listings', end='')

        if not articles:
            # Last page reached
            print(' — no more listings, done.')
            break

        new = 0
        for article in articles:
            l = parse_listing(article)
            if l['id'] and l['id'] not in all_listings:
                all_listings[l['id']] = l
                new += 1

        print(f' ({new} new this page)')

        if new == 0 and page > 1:
            # All listings on this page already seen — likely last page
            print('  ℹ️  No new IDs on this page — stopping pagination')
            break

        if page < MAX_PAGES:
            time.sleep(2)

    return all_listings


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

    fields = ['id', 'city', 'title', 'street', 'neighborhood', 'price_raw', 'price_eur',
              'rooms', 'size_m2', 'floor', 'tags', 'description', 'photos', 'date_scraped', 'url']
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(listings)


def append_to_sheet(new_listings: list):
    if not new_listings:
        return
    print('  🔑 Connecting to Google Sheets...')
    try:
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


def run_script(name: str, timeout: int = 300):
    """Run a helper script and print key output lines."""
    try:
        r = subprocess.run(
            ['python3', f'{SCRIPT_DIR}/{name}'],
            capture_output=True, text=True, timeout=timeout
        )
        for line in r.stdout.splitlines():
            if line.strip():
                print(f'    {line.strip()}')
        if r.returncode != 0 and r.stderr:
            print(f'  ⚠️  {name} stderr: {r.stderr[:300]}')
        return r.returncode == 0
    except subprocess.TimeoutExpired:
        print(f'  ⚠️  {name} timed out after {timeout}s')
        return False
    except Exception as e:
        print(f'  ⚠️  {name} failed: {e}')
        return False


def log_run(new_count: int, total_count: int):
    try:
        with open(LOG_FILE) as f:
            log = json.load(f)
    except:
        log = []
    log.append({'timestamp': datetime.now().isoformat(),
                'new_listings': new_count, 'total_listings': total_count})
    with open(LOG_FILE, 'w') as f:
        json.dump(log[-30:], f, indent=2)


def main():
    print('🦞 JBizz Assistant — Daily Listings Update')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}')
    print('=' * 50)

    existing = load_existing()
    print(f'📦 Existing listings: {len(existing)}')

    print('\n🔍 Scraping idealista.pt...')
    scraped = scrape_all()
    print(f'   Total scraped: {len(scraped)}')

    if not scraped:
        print('\n❌ SCRAPE FAILED — no listings returned. Check ZenRows credits or URL.')
        return 0

    new_ids      = set(scraped.keys()) - set(existing.keys())
    new_listings = [scraped[i] for i in new_ids]
    print(f'\n🆕 New listings found: {len(new_listings)}')
    for l in new_listings[:5]:
        print(f"   • {l['rooms']} | €{l['price_eur']}/mo | {l['size_m2']}m² | {l['neighborhood']}")

    # Merge: new scraped data can update prices on existing listings
    merged = {**existing, **scraped}
    save_all(merged)
    print(f'💾 Saved: {len(merged)} listings total')

    if new_listings:
        append_to_sheet(new_listings)

    print('\n🧹 Dedup...')
    run_script('dedup.py')

    # Reload after dedup
    try:
        with open(JSON_FILE) as f:
            merged = {l['id']: l for l in json.load(f) if l.get('id')}
    except:
        pass

    print('\n📅 DOM tracker...')
    run_script('dom_tracker.py', timeout=60)

    print('\n🧠 Enricher + Sheets...')
    run_script('enrich_listings.py', timeout=600)

    print('\n💰 Price tracker...')
    run_script('price_tracker.py', timeout=60)

    log_run(len(new_listings), len(merged))
    print(f'\n✅ Done — {len(new_listings)} new listings | {len(merged)} total')
    return len(new_listings)


if __name__ == '__main__':
    main()
