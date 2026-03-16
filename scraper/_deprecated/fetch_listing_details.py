#!/usr/bin/env python3
"""
Listing Detail Fetcher — JBizz Assistant 🦞
Fetches individual idealista.pt listing pages via ScraperAPI to extract:
  - Full description (untruncated)
  - Area bruta & área útil
  - Energy certificate
  - WCs / bathrooms
  - Date listed / last updated
  - Parking count
  - Condominium fee
  - Reliable floor number

Saves to: data/listing_details.json (resumable — skips already-fetched)
"""

import json
import re
import os
import time
import urllib.parse
import requests
from bs4 import BeautifulSoup
from datetime import datetime

LISTINGS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listings_deduped.json'
DETAILS_FILE   = '/root/.openclaw/workspace/projects/real-estate/data/listing_details.json'

API_KEY = os.environ.get('SCRAPERAPI_KEY', '3ffdb5a92e83a68d35200f2df255b83a')


def fetch_page(url: str) -> str | None:
    encoded = urllib.parse.quote(url, safe='')
    proxy = f'http://api.scraperapi.com?api_key={API_KEY}&render=true&url={encoded}&country_code=pt'
    try:
        r = requests.get(proxy, timeout=60)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f'    ⚠️  Fetch error: {e}')
        return None


def parse_int(text: str) -> int | None:
    if not text:
        return None
    nums = re.sub(r'[^\d]', '', str(text))
    return int(nums) if nums else None


def parse_detail_page(html: str, listing_id: str) -> dict:
    soup = BeautifulSoup(html, 'lxml')
    result = {'id': listing_id, 'fetched_at': datetime.now().strftime('%Y-%m-%d')}

    # ── Full description ──────────────────────────────────────────────────────
    desc_el = soup.select_one('div.comment, div#descriptionContainer, div.adDetailDescription')
    if desc_el:
        result['full_description'] = desc_el.get_text(separator='\n', strip=True)

    # ── Specs table (area bruta, útil, WCs, etc.) ─────────────────────────────
    # Idealista detail pages have a characteristics list
    for li in soup.select('div.details-property_features li, ul.details-property li, li.details-property-feature-one'):
        text = li.get_text(strip=True).lower()

        if 'área bruta' in text or 'area bruta' in text:
            m = re.search(r'(\d+)', text)
            if m: result['area_bruta'] = int(m.group(1))

        elif 'área útil' in text or 'area util' in text or 'área habitável' in text:
            m = re.search(r'(\d+)', text)
            if m: result['area_util'] = int(m.group(1))

        elif 'casa' in text and ('banho' in text or 'wc' in text):
            m = re.search(r'(\d+)', text)
            if m: result['wcs'] = int(m.group(1))

        elif 'garagem' in text or 'lugar' in text and 'estaciona' in text:
            m = re.search(r'(\d+)', text)
            result['parking_spaces'] = int(m.group(1)) if m else 1

        elif 'condomínio' in text or 'condominio' in text:
            m = re.search(r'(\d+)', text)
            if m: result['condo_fee'] = int(m.group(1))

        elif 'certificad' in text and ('energético' in text or 'energetico' in text or any(g in text for g in ['classe a','classe b','classe c','classe d','classe e','classe f','classe g'])):
            for grade in ['A+', 'A', 'B', 'C', 'D', 'E', 'F', 'G']:
                if grade.lower() in text:
                    result['energy_cert'] = grade
                    break

        elif 'andar' in text or 'piso' in text:
            m = re.search(r'(\d+)', text)
            if m: result['floor_detail'] = int(m.group(1))

    # ── Energy cert alternative locations ────────────────────────────────────
    if 'energy_cert' not in result:
        cert_el = soup.select_one('span.icon-energy, div.energy-certificate, span[class*="energy"]')
        if cert_el:
            cert_text = cert_el.get_text(strip=True)
            for grade in ['A+', 'A', 'B', 'C', 'D', 'E', 'F', 'G']:
                if grade in cert_text:
                    result['energy_cert'] = grade
                    break

    # ── Date listed / last updated ────────────────────────────────────────────
    # Look for "Publicado" or "Atualizado" dates
    for el in soup.select('p.stats-text, div.stats, span.stats'):
        text = el.get_text(strip=True)
        if 'publicad' in text.lower() or 'anunciad' in text.lower():
            m = re.search(r'(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})', text)
            if m:
                d, mo, y = m.groups()
                y = f'20{y}' if len(y) == 2 else y
                result['date_listed'] = f'{y}-{mo.zfill(2)}-{d.zfill(2)}'

        if 'atualiza' in text.lower() or 'updated' in text.lower():
            m = re.search(r'(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})', text)
            if m:
                d, mo, y = m.groups()
                y = f'20{y}' if len(y) == 2 else y
                result['date_updated'] = f'{y}-{mo.zfill(2)}-{d.zfill(2)}'

    # Also check meta tags for dates
    for meta in soup.select('meta[property="article:published_time"], meta[name="date"]'):
        content = meta.get('content', '')
        if content and 'date_listed' not in result:
            result['date_listed'] = content[:10]

    # ── Tags / features (more complete from detail page) ──────────────────────
    feature_tags = []
    for el in soup.select('div.details-property_features span, ul.details-property-feature span'):
        txt = el.get_text(strip=True)
        if txt:
            feature_tags.append(txt)
    if feature_tags:
        result['detail_tags'] = ', '.join(feature_tags[:20])

    return result


def main(limit: int | None = None, ids_only: list | None = None):
    print('🦞 Listing Detail Fetcher')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 55)

    with open(LISTINGS_FILE, encoding='utf-8') as f:
        listings = json.load(f)

    # Load existing details
    existing = {}
    try:
        with open(DETAILS_FILE, encoding='utf-8') as f:
            existing = {d['id']: d for d in json.load(f)}
        print(f'   {len(existing)} already fetched')
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    if ids_only:
        to_fetch = [l for l in listings if l['id'] in ids_only and l['id'] not in existing]
    else:
        to_fetch = [l for l in listings if l['id'] not in existing]

    if limit:
        to_fetch = to_fetch[:limit]

    print(f'📦 {len(to_fetch)} listings to fetch details for\n')

    details = list(existing.values())

    for i, listing in enumerate(to_fetch):
        lid = listing['id']
        url = listing.get('url', f'https://www.idealista.pt/imovel/{lid}/')
        print(f'  [{i+1:3d}/{len(to_fetch)}] {listing.get("rooms","?")} {listing.get("size_m2","?")}m² — {url}')

        html = fetch_page(url)
        if not html:
            print(f'    ⚠️  Skipped (fetch failed)')
            time.sleep(2)
            continue

        detail = parse_detail_page(html, lid)

        # Report what was extracted
        found = [k for k in ['full_description', 'area_bruta', 'area_util', 'wcs', 'energy_cert', 'date_listed', 'parking_spaces'] if k in detail]
        print(f'    ✓ Extracted: {", ".join(found) if found else "minimal data"}')
        if 'area_bruta' in detail:
            print(f'    📐 Bruta: {detail.get("area_bruta")}m² | Útil: {detail.get("area_util","?")}m²')

        details.append(detail)
        existing[lid] = detail

        if (i + 1) % 5 == 0:
            with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
                json.dump(details, f, ensure_ascii=False, indent=2)
            print(f'  💾 Saved ({i+1} done)')

        time.sleep(2)

    with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
        json.dump(details, f, ensure_ascii=False, indent=2)
    print(f'\n✅ {len(details)} total details saved → {DETAILS_FILE}')


if __name__ == '__main__':
    import sys
    # Quick test mode: python3 fetch_listing_details.py --test
    if '--test' in sys.argv:
        main(limit=3)
    elif '--ids' in sys.argv:
        idx = sys.argv.index('--ids')
        ids = sys.argv[idx+1].split(',')
        main(ids_only=ids)
    else:
        main()
