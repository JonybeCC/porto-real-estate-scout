#!/usr/bin/env python3
"""
Idealista.pt Rental Scraper — Created by JBizz Assistant 🦞
Scrapes rental listings from idealista.pt via ScraperAPI
"""

import requests
from bs4 import BeautifulSoup
import csv
import json
import time
import re
from datetime import datetime
import urllib.parse

API_KEY = '3ffdb5a92e83a68d35200f2df255b83a'
OUTPUT_CSV = '/root/.openclaw/workspace/projects/real-estate/data/listings.csv'
OUTPUT_JSON = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'

# Target areas: Porto - Foz zone (Foz do Douro, Nevogilde, Lordelo do Ouro, Pinhais da Foz)
# Idealista uses freguesia slugs — searching Porto broadly and filtering by neighborhood keywords
SHAPE = '%28%28ovgzFrr%60t%40n%60%40omEsWm%5BzGe%7C%40vc%40oUjiA%60%40fHdfFciCjuA%29%29'
BASE_SEARCH = f'https://www.idealista.pt/areas/arrendar-casas/com-preco-max_3100,preco-min_1650,t2,t3/?shape={SHAPE}'

SEARCH_CONFIGS = [
    {
        'city': 'Porto - Foz Zone',
        'zones': [],  # No keyword filter — URL already scoped to polygon + T2/T3 + price
        'base_url': BASE_SEARCH,
        'max_pages': 10,
    },
]

FOZ_ZONES = []  # No additional filtering needed

def fetch_page(url):
    encoded = urllib.parse.quote(url, safe='')
    proxy_url = f'http://api.scraperapi.com?api_key={API_KEY}&render=true&url={encoded}'
    try:
        r = requests.get(proxy_url, timeout=90)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f'  Error fetching {url}: {e}')
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
    if m:
        return int(float(m.group(1).replace(',', '.')))
    return None

def extract_neighborhood(title):
    """Extract neighborhood from title like 'Apartamento T2 na Rua X, Bairro, Freguesia'"""
    # Pattern: "em/na/no Rua/Avenida X, NEIGHBORHOOD"
    m = re.search(r',\s*([^,]+),\s*[A-Z][^,]{5,}$', title)
    if m:
        return m.group(1).strip()
    # Try simpler: take the part after the street address
    parts = title.split(',')
    if len(parts) >= 2:
        # Usually: "Apartamento T2 na Rua X, Neighborhood, Freguesia..."
        return parts[1].strip() if len(parts) > 1 else ''
    return ''

def extract_street(title):
    """Extract street address from title"""
    m = re.search(r'(?:na|no|em)\s+([^,]+)', title)
    return m.group(1).strip() if m else ''

def is_in_foz_zone(title):
    """Check if listing is in target Foz area"""
    title_lower = title.lower()
    return any(zone in title_lower for zone in FOZ_ZONES)

def parse_listing(article, city):
    listing = {}

    # ID
    listing['id'] = article.get('data-element-id', '')
    listing['city'] = city

    # Title & URL
    title_el = article.select_one('a.item-link')
    if title_el:
        listing['title'] = title_el.get('title', title_el.get_text(strip=True))
        href = title_el.get('href', '')
        listing['url'] = f'https://www.idealista.pt{href}' if href.startswith('/') else href
    else:
        listing['title'] = ''
        listing['url'] = ''

    # Extract street and neighborhood from title
    listing['street'] = extract_street(listing['title'])
    listing['neighborhood'] = extract_neighborhood(listing['title'])

    # Price
    price_el = article.select_one('.item-price')
    listing['price_raw'] = price_el.get_text(strip=True) if price_el else ''
    listing['price_eur'] = parse_price(listing['price_raw'])

    # Details
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

    # Tags (De luxo, Garagem, Vista mar, etc.) — cast a wide net across common selectors
    tags = []
    tag_selectors = [
        'span.listing-tags', 'span.item-parking', 'span.tag', 'span.item-tag',
        'div.item-tags span', 'li.item-tag', 'span.tag-container',
        'span[class*="tag"]', 'span[class*="parking"]', 'span[class*="garage"]',
    ]
    seen_tags = set()
    for sel in tag_selectors:
        for t in article.select(sel):
            txt = t.get_text(strip=True)
            if txt and txt not in seen_tags:
                seen_tags.add(txt)
                tags.append(txt)
    # Also scan raw HTML for garage/sea view keywords as fallback
    raw_html = str(article).lower()
    if not any('garag' in t.lower() for t in tags):
        if 'garagem' in raw_html or 'garage' in raw_html or 'lugar de garagem' in raw_html:
            tags.append('Garagem incluída')
    if not any('vista' in t.lower() for t in tags):
        if 'vista mar' in raw_html or 'sea view' in raw_html:
            tags.append('Vista mar')
    listing['tags'] = '|'.join(tags)

    # Description — capture full text (no truncation; enricher will use it all)
    desc_el = article.select_one('p.ellipsis')
    listing['description'] = desc_el.get_text(strip=True) if desc_el else ''

    # Photos
    photos = []
    for img in article.select('img[src], img[data-src]')[:4]:
        src = img.get('src') or img.get('data-src', '')
        if src and ('idealista' in src or 'st3.' in src) and src not in photos:
            photos.append(src)
    listing['photos'] = '|'.join(photos[:3])

    listing['date_scraped'] = datetime.now().strftime('%Y-%m-%d')

    return listing

def scrape_config(config):
    listings = []
    seen_ids = set()
    base_url = config['base_url']
    max_pages = config['max_pages']
    city = config['city']
    filter_zones = config.get('zones')

    print(f"\n🏙️  Scraping {city}")
    print(f"   Target zones: {', '.join(filter_zones) if filter_zones else 'all'}")

    for page in range(1, max_pages + 1):
        if page == 1:
            url = base_url
        elif '?' in base_url:
            url = base_url.replace('?', f'pagina-{page}?')
        else:
            url = f"{base_url}pagina-{page}"
        print(f"\n  📄 Page {page}: {url}")

        html = fetch_page(url)
        if not html:
            break

        soup = BeautifulSoup(html, 'lxml')
        articles = soup.select('article.item')
        print(f"  Found {len(articles)} listings", end='')

        if not articles:
            print(" — stopping.")
            break

        page_listings = []
        filtered_out = 0

        for article in articles:
            listing = parse_listing(article, city)
            if not listing['id'] or listing['id'] in seen_ids:
                continue
            seen_ids.add(listing['id'])

            # Filter by zone if specified
            if filter_zones and FOZ_ZONES and not is_in_foz_zone(listing['title']):
                filtered_out += 1
                continue

            page_listings.append(listing)

        listings.extend(page_listings)
        print(f" → kept {len(page_listings)} in target zone ({filtered_out} filtered out)")

        if page < max_pages:
            time.sleep(2)

    return listings

def main():
    all_listings = []

    print('🦞 JBizz Assistant — Idealista.pt Rental Scraper')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 60)

    for config in SEARCH_CONFIGS:
        listings = scrape_config(config)
        all_listings.extend(listings)

    print(f'\n✅ Total listings scraped: {len(all_listings)}')

    if all_listings:
        # Save CSV
        fields = ['id', 'city', 'title', 'street', 'neighborhood', 'price_raw', 'price_eur',
                  'rooms', 'size_m2', 'floor', 'tags', 'description', 'photos', 'date_scraped', 'url']
        with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_listings)
        print(f'💾 CSV: {OUTPUT_CSV}')

        # Save JSON
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(all_listings, f, ensure_ascii=False, indent=2)
        print(f'💾 JSON: {OUTPUT_JSON}')

        # Summary stats
        prices = [l['price_eur'] for l in all_listings if l['price_eur']]
        sizes = [l['size_m2'] for l in all_listings if l['size_m2']]
        if prices:
            print(f'\n📊 Price range: €{min(prices):,} – €{max(prices):,}/mo | Avg: €{sum(prices)//len(prices):,}')
        if sizes:
            print(f'📐 Size range: {min(sizes)}m² – {max(sizes)}m² | Avg: {sum(sizes)//len(sizes)}m²')

        print('\n🏠 Sample listings:')
        print('-' * 60)
        for l in all_listings[:5]:
            print(f"  {l['rooms']} | €{l['price_eur']:,}/mo | {l['size_m2']}m²")
            print(f"  📍 {l['neighborhood']}")
            print(f"  🔗 {l['url']}")
            print()

    return all_listings

if __name__ == '__main__':
    main()
