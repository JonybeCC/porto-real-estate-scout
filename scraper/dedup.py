#!/usr/bin/env python3
"""
Smart Duplicate Detector v2 — Created by JBizz Assistant 🦞
Strategy:
  1. Structural exact match: same street + size + price → DEFINITE dupe
  2. Structural near match: same street + size within 5m² + price within 5% → CANDIDATE
  3. For candidates: use GPT-4o vision to compare photos across listings
  4. Remove confirmed dupes, keep one per property
"""

import json
import re
import requests
import base64
import os
import shutil
from io import BytesIO
from PIL import Image
from datetime import datetime
from openai import OpenAI

JSON_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
DUPES_FILE = '/root/.openclaw/workspace/projects/real-estate/data/duplicates.json'
CLEAN_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings_deduped.json'

OPENAI_KEY = os.environ.get('OPENAI_API_KEY')
client = OpenAI(api_key=OPENAI_KEY)


def load_listings():
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_street_normalized(title):
    """Normalize street name for comparison"""
    m = re.search(r'(?:na|no|em)\s+([^,]+)', title)
    if not m:
        return ''
    street = m.group(1).strip().lower()
    # Remove house numbers
    street = re.sub(r'\s*,?\s*\d+.*$', '', street)
    street = re.sub(r'\s+', ' ', street)
    return street.strip()


def price_similar(p1, p2, pct=5):
    if not p1 or not p2:
        return False
    return abs(p1 - p2) / max(p1, p2) * 100 <= pct


def fetch_image_b64(url, max_size=(800, 600)):
    """Fetch image and return as base64 for GPT-4o"""
    try:
        r = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.idealista.pt/'
        })
        img = Image.open(BytesIO(r.content)).convert('RGB')
        img.thumbnail(max_size, Image.LANCZOS)
        buf = BytesIO()
        img.save(buf, format='JPEG', quality=85)
        return base64.b64encode(buf.getvalue()).decode('utf-8')
    except Exception as e:
        return None


def vision_compare(listing_a, listing_b):
    """Use GPT-4o vision to compare photos from two listings"""
    photos_a = [p for p in listing_a.get('photos', '').split('|') if p and 'idealista.pt/blur' in p]
    photos_b = [p for p in listing_b.get('photos', '').split('|') if p and 'idealista.pt/blur' in p]

    if not photos_a or not photos_b:
        return None, 'no_photos'

    # Fetch up to 2 photos from each listing
    images_content = []
    for url in (photos_a[:2] + photos_b[:2]):
        b64 = fetch_image_b64(url)
        if b64:
            images_content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "low"}
            })

    if len(images_content) < 2:
        return None, 'fetch_failed'

    prompt = f"""You are helping identify duplicate real estate listings.

Listing A: {listing_a.get('rooms')} | {listing_a.get('size_m2')}m² | €{listing_a.get('price_eur')}/mo | {listing_a.get('neighborhood')}
Listing B: {listing_b.get('rooms')} | {listing_b.get('size_m2')}m² | €{listing_b.get('price_eur')}/mo | {listing_b.get('neighborhood')}

The first {min(2, len(photos_a))} image(s) are from Listing A, the next {min(2, len(photos_b))} from Listing B.

Are these photos showing the same physical apartment? Look for: matching architecture, balcony/windows, furniture layout, floor type, building exterior visible from windows, distinctive features.

Reply with ONLY:
SAME - [one sentence reason]
DIFFERENT - [one sentence reason]
UNCERTAIN - [one sentence reason]"""

    try:
        response = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=[{
                "role": "user",
                "content": [{"type": "text", "text": prompt}] + images_content
            }],
            max_tokens=100
        )
        answer = response.choices[0].message.content.strip()
        is_same = answer.upper().startswith('SAME')
        return is_same, answer
    except Exception as e:
        return None, f'error: {e}'


def find_candidates(listings):
    """Find structurally similar pairs"""
    definite = []
    candidates = []

    for i in range(len(listings)):
        for j in range(i + 1, len(listings)):
            a, b = listings[i], listings[j]

            street_a = extract_street_normalized(a.get('title', ''))
            street_b = extract_street_normalized(b.get('title', ''))

            # Must have a street to compare
            if not street_a or not street_b:
                continue

            # Same street (or very similar — remove common abbreviations)
            if street_a != street_b:
                continue

            size_a = a.get('size_m2')
            size_b = b.get('size_m2')
            price_a = a.get('price_eur')
            price_b = b.get('price_eur')

            # Exact match on street + size + price → definite dupe, no vision needed
            if size_a == size_b and price_a == price_b:
                definite.append((i, j, 'exact_structural_match'))
                continue

            # Near match → needs vision confirmation
            if size_a and size_b and abs(size_a - size_b) <= 5 and price_similar(price_a, price_b):
                candidates.append((i, j))

    return definite, candidates


def main():
    print('🦞 JBizz Assistant — Smart Duplicate Detector v2')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 55)

    listings = load_listings()
    print(f'📦 Loaded {len(listings)} listings')

    # Find candidates
    print('\n🔍 Finding structural candidates...')
    definite_dupes, vision_candidates = find_candidates(listings)
    print(f'   Definite dupes (exact match): {len(definite_dupes)}')
    print(f'   Candidates for vision check: {len(vision_candidates)}')

    confirmed_dupes = []

    # Process definite dupes
    for i, j, reason in definite_dupes:
        a, b = listings[i], listings[j]
        print(f'\n  🔴 DEFINITE DUPE: [{a["id"]}] vs [{b["id"]}]')
        print(f'     {a["rooms"]} {a["size_m2"]}m² €{a["price_eur"]}/mo — {a["neighborhood"]}')
        print(f'     Reason: {reason}')
        confirmed_dupes.append({
            'keep': a['id'],
            'remove': b['id'],
            'method': reason,
            'listing_a': {'id': a['id'], 'price': a.get('price_eur'), 'size': a.get('size_m2'), 'url': a.get('url')},
            'listing_b': {'id': b['id'], 'price': b.get('price_eur'), 'size': b.get('size_m2'), 'url': b.get('url')},
        })

    # Vision check for near-matches
    if vision_candidates:
        print(f'\n🔬 Running vision comparison on {len(vision_candidates)} candidate pairs...')
        for i, j in vision_candidates:
            a, b = listings[i], listings[j]
            print(f'\n  Checking [{a["id"]}] vs [{b["id"]}]...')
            print(f'    {a["rooms"]} {a["size_m2"]}m² €{a["price_eur"]}/mo | {b["rooms"]} {b["size_m2"]}m² €{b["price_eur"]}/mo')

            is_same, verdict = vision_compare(a, b)
            print(f'    Vision: {verdict}')

            if is_same:
                print(f'    → 🔴 DUPLICATE')
                confirmed_dupes.append({
                    'keep': a['id'],
                    'remove': b['id'],
                    'method': 'vision_comparison',
                    'vision_verdict': verdict,
                    'listing_a': {'id': a['id'], 'price': a.get('price_eur'), 'size': a.get('size_m2'), 'url': a.get('url')},
                    'listing_b': {'id': b['id'], 'price': b.get('price_eur'), 'size': b.get('size_m2'), 'url': b.get('url')},
                })
            elif is_same is None:
                print(f'    → ⚠️  UNCERTAIN (keeping both)')
            else:
                print(f'    → 🟢 DIFFERENT')

    print(f'\n\n📊 Results: {len(confirmed_dupes)} duplicates confirmed')

    if confirmed_dupes:
        # Save dupe report
        with open(DUPES_FILE, 'w', encoding='utf-8') as f:
            json.dump(confirmed_dupes, f, ensure_ascii=False, indent=2)
        print(f'💾 Duplicate report: {DUPES_FILE}')

        # Build clean list — use set to handle transitive dupes
        ids_to_remove = set()
        for d in confirmed_dupes:
            # Always remove the one with the higher listing ID (newer = duplicate)
            keep_id = d['keep']
            remove_id = d['remove']
            # If remove_id is already being kept by another rule, swap
            if remove_id not in ids_to_remove:
                ids_to_remove.add(remove_id)

        clean = [l for l in listings if l['id'] not in ids_to_remove]

        with open(CLEAN_FILE, 'w', encoding='utf-8') as f:
            json.dump(clean, f, ensure_ascii=False, indent=2)

        # Backup and overwrite
        shutil.copy(JSON_FILE, JSON_FILE + '.bak')
        shutil.copy(CLEAN_FILE, JSON_FILE)

        print(f'✅ Clean dataset: {len(clean)} listings (removed {len(ids_to_remove)})')
        print(f'💾 listings.json updated (backup: .bak)')
    else:
        print('✅ No duplicates found — dataset is clean!')

    return confirmed_dupes


if __name__ == '__main__':
    main()
