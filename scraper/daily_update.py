#!/usr/bin/env python3
"""
Daily Listings Updater — Created by JBizz Assistant 🦞
Scrapes new listings, deduplicates against existing data, appends to CSV + Google Sheet
"""

import json
import csv
import time
import re
import urllib.parse
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# Config
API_KEY = 'a19f204d97b9578f8d82bd749ac175bd5383dd6e'
CREDS_FILE = '/root/.openclaw/credentials/google-service-account.json'
JSON_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
CSV_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.csv'
LOG_FILE = '/root/.openclaw/workspace/projects/real-estate/data/update_log.json'
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1Ljk9s45BovbRfk1QIUGgGxbjnpIgs2F52rJjDNGQOQI/edit'
SHEET_NAME = 'Listings'

SHAPE = '%28%28ovgzFrr%60t%40n%60%40omEsWm%5BzGe%7C%40vc%40oUjiA%60%40fHdfFciCjuA%29%29'
BASE_URL = f'https://www.idealista.pt/areas/arrendar-casas/com-preco-max_3100,preco-min_1650,t2,t3/?shape={SHAPE}'
MAX_PAGES = 10

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def fetch_page(url):
    params = {
        'apikey': API_KEY,
        'url': url,
        'js_render': 'true',
        'premium_proxy': 'true',
        'json_response': 'true',
    }
    try:
        r = requests.get('https://api.zenrows.com/v1/', params=params, timeout=180)
        r.raise_for_status()
        data = r.json()
        return data.get('html', '')
    except Exception as e:
        print(f'  ⚠️  Fetch error: {e}')
        return None

def parse_price(text):
    if not text:
        return None
    nums = re.sub(r'[^\d]', '', text.split('€')[0])
    return int(nums) if nums else None

def parse_size(text):
    if not text:
        return None
    m = re.search(r'(\d+[\.,]?\d*)\s*m²', text)
    return int(float(m.group(1).replace(',', '.'))) if m else None

def extract_neighborhood(title):
    parts = title.split(',')
    return parts[1].strip() if len(parts) > 1 else ''

def extract_street(title):
    m = re.search(r'(?:na|no|em)\s+([^,]+)', title)
    return m.group(1).strip() if m else ''

def parse_listing(article):
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
        listing['url'] = ''

    listing['street'] = extract_street(listing['title'])
    listing['neighborhood'] = extract_neighborhood(listing['title'])

    price_el = article.select_one('.item-price')
    listing['price_raw'] = price_el.get_text(strip=True) if price_el else ''
    listing['price_eur'] = parse_price(listing['price_raw'])

    details = [d.get_text(strip=True) for d in article.select('span.item-detail')]
    listing['rooms'] = ''
    listing['size_m2'] = None
    listing['floor'] = ''
    for d in details:
        if re.match(r'^T\d$|^estúdio$', d, re.IGNORECASE):
            listing['rooms'] = d
        elif 'm²' in d:
            listing['size_m2'] = parse_size(d)
        elif re.search(r'andar|r/c|cave|piso', d, re.IGNORECASE):
            listing['floor'] = d

    # Wide tag extraction + fallback HTML scan for garage/sea view
    tags = []
    seen_tags = set()
    for sel in ['span.listing-tags', 'span.item-parking', 'span.tag',
                'div.item-tags span', 'li.item-tag', 'span[class*="tag"]']:
        for t in article.select(sel):
            txt = t.get_text(strip=True)
            if txt and txt not in seen_tags:
                seen_tags.add(txt); tags.append(txt)
    raw_html = str(article).lower()
    if not any('garag' in t.lower() for t in tags):
        if 'garagem' in raw_html or 'lugar de garagem' in raw_html:
            tags.append('Garagem incluída')
    if not any('vista' in t.lower() for t in tags):
        if 'vista mar' in raw_html or 'sea view' in raw_html:
            tags.append('Vista mar')
    listing['tags'] = '|'.join(tags)

    desc_el = article.select_one('p.ellipsis')
    listing['description'] = desc_el.get_text(strip=True) if desc_el else ''  # No truncation

    photos = []
    for img in article.select('img[src], img[data-src]')[:4]:
        src = img.get('src') or img.get('data-src', '')
        if src and ('idealista' in src or 'st3.' in src) and src not in photos:
            photos.append(src)
    listing['photos'] = '|'.join(photos[:3])
    listing['date_scraped'] = datetime.now().strftime('%Y-%m-%d')

    return listing

def scrape_all():
    """Scrape all pages and return listings dict keyed by ID"""
    all_listings = {}
    for page in range(1, MAX_PAGES + 1):
        if page == 1:
            url = BASE_URL
        else:
            url = BASE_URL.replace('?', f'pagina-{page}?')

        print(f'  📄 Page {page}...', end=' ', flush=True)
        html = fetch_page(url)
        if not html:
            break

        soup = BeautifulSoup(html, 'lxml')
        articles = soup.select('article.item')
        print(f'{len(articles)} listings', end='')

        if not articles:
            print(' — done.')
            break

        new = 0
        for article in articles:
            l = parse_listing(article)
            if l['id'] and l['id'] not in all_listings:
                all_listings[l['id']] = l
                new += 1

        print(f' ({new} unique)')
        if page < MAX_PAGES:
            time.sleep(2)

    return all_listings

def load_existing():
    """Load existing listings from JSON, return dict keyed by ID"""
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {l['id']: l for l in data if l.get('id')}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_all(listings_dict):
    """Save full dataset to JSON and CSV"""
    listings = list(listings_dict.values())
    with open(JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)

    fields = ['id', 'city', 'title', 'street', 'neighborhood', 'price_raw', 'price_eur',
              'rooms', 'size_m2', 'floor', 'tags', 'description', 'photos', 'date_scraped', 'url']
    with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(listings)

def append_to_sheet(new_listings):
    """Append only new listings to Google Sheet"""
    print('  🔑 Connecting to Google Sheets...')
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=1000, cols=20)

    rows = []
    for l in new_listings:
        rows.append([
            l.get('id', ''),
            l.get('city', ''),
            l.get('rooms', ''),
            l.get('price_eur', ''),
            l.get('size_m2', ''),
            l.get('floor', ''),
            l.get('neighborhood', ''),
            l.get('street', ''),
            l.get('tags', ''),
            l.get('description', '')[:200],
            l.get('url', ''),
            l.get('photos', '').split('|')[0] if l.get('photos') else '',
            l.get('date_scraped', ''),
        ])

    if rows:
        ws.append_rows(rows, value_input_option='RAW')
        print(f'  ✅ Appended {len(rows)} new rows to sheet')
    else:
        print('  ℹ️  No new rows to append')

def log_run(new_count, total_count):
    """Append to update log"""
    try:
        with open(LOG_FILE, 'r') as f:
            log = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        log = []

    log.append({
        'timestamp': datetime.now().isoformat(),
        'new_listings': new_count,
        'total_listings': total_count,
    })
    with open(LOG_FILE, 'w') as f:
        json.dump(log[-30:], f, indent=2)  # Keep last 30 runs

def main():
    import subprocess

    SCRIPT_DIR = '/root/.openclaw/workspace/projects/real-estate/scraper'

    print(f'🦞 JBizz Assistant — Daily Listings Update')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}')
    print('=' * 50)

    # Load what we already have
    existing = load_existing()
    print(f'📦 Existing listings: {len(existing)}')

    # Scrape fresh data
    print('\n🔍 Scraping idealista.pt...')
    scraped = scrape_all()
    print(f'   Total scraped: {len(scraped)}')

    # Find genuinely new listings
    new_ids = set(scraped.keys()) - set(existing.keys())
    new_listings = [scraped[id_] for id_ in new_ids]
    print(f'\n🆕 New listings found: {len(new_listings)}')

    if new_listings:
        for l in new_listings[:5]:
            print(f"   • {l['rooms']} | €{l['price_eur']}/mo | {l['size_m2']}m² | {l['neighborhood']}")

    # Merge and save
    merged = {**existing, **scraped}  # scraped overwrites (updates prices etc.)
    save_all(merged)
    print(f'\n💾 Total saved: {len(merged)} listings')

    # Append new ones to sheet
    if new_listings:
        append_to_sheet(new_listings)

    # Run dedup on the full merged dataset
    print('\n🧹 Running duplicate detection...')
    try:
        result = subprocess.run(
            ['python3', f'{SCRIPT_DIR}/dedup.py'],
            capture_output=True, text=True, timeout=300
        )
        for line in result.stdout.splitlines():
            if any(k in line for k in ['Results:', 'Clean dataset', 'No duplicates', 'updated']):
                print(f'  {line.strip()}')
    except Exception as e:
        print(f'  ⚠️  Dedup error: {e}')

    # Reload merged count after dedup (may have changed)
    try:
        with open(JSON_FILE, 'r') as f:
            merged = {l['id']: l for l in json.load(f) if l.get('id')}
    except Exception:
        pass

    # Log the run
    log_run(len(new_listings), len(merged))

    # ── Post-scrape pipeline ──────────────────────────────────────────────────
    print('\n📊 Running DOM tracker...')
    try:
        result = subprocess.run(
            ['python3', f'{SCRIPT_DIR}/dom_tracker.py'],
            capture_output=True, text=True, timeout=60
        )
        for line in result.stdout.splitlines():
            if any(k in line for k in ['✅', '💸', '⏰', 'DOM', 'new listing', 'gone', 'price change']):
                print(f'  {line.strip()}')
        if result.returncode != 0:
            print(f'  ⚠️  DOM tracker error: {result.stderr[:200]}')
    except Exception as e:
        print(f'  ⚠️  DOM tracker failed: {e}')

    print('\n🧠 Running enricher (this may take a few minutes)...')
    try:
        result = subprocess.run(
            ['python3', f'{SCRIPT_DIR}/enrich_listings.py'],
            capture_output=True, text=True, timeout=600
        )
        for line in result.stdout.splitlines():
            if any(k in line for k in ['✅', '🏆', '📊', '🎯', 'pushed', 'Score', 'Avg']):
                print(f'  {line.strip()}')
        if result.returncode != 0:
            print(f'  ⚠️  Enricher error: {result.stderr[:200]}')
    except Exception as e:
        print(f'  ⚠️  Enricher failed: {e}')

    # Run monitoring check
    print('\n📊 Running monitoring check...')
    try:
        result = subprocess.run(
            ['python3', f'{SCRIPT_DIR}/monitor.py'],
            capture_output=True, text=True, timeout=30
        )
        for line in result.stdout.splitlines():
            if line.strip():
                print(f'  {line.strip()}')
        if result.returncode != 0:
            print(f'  ⚠️  Monitoring error: {result.stderr[:200]}')
    except Exception as e:
        print(f'  ⚠️  Monitoring failed: {e}')

    print(f'\n✅ Update complete — {len(new_listings)} new listings added')
    return len(new_listings)

if __name__ == '__main__':
    main()
