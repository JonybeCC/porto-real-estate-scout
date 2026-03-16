#!/usr/bin/env python3
"""
Commerce & Amenity Enricher — JBizz Assistant 🦞
Uses Nominatim + Overpass API to find nearby:
  - Supermarkets (Continente, Pingo Doce, Lidl, Aldi, Mercadona, Auchan, Intermarché)
  - Pharmacies
  - Restaurants / cafes
  - Metro stations
  - Schools

Adds to each listing:
  nearest_supermarket, nearest_supermarket_km
  nearest_pharmacy, nearest_pharmacy_km
  nearest_restaurant_area (description of nearby dining)
  metro_distance_km
  commerce_notes (human-readable summary)
"""

import json
import time
import math
import signal
import requests
from datetime import datetime

GEO_FILE      = '/root/.openclaw/workspace/projects/real-estate/data/geocoded.json'
ENRICHED_FILE = '/root/.openclaw/workspace/projects/real-estate/data/enriched_listings.json'
COMMERCE_FILE = '/root/.openclaw/workspace/projects/real-estate/data/commerce.json'

OVERPASS_MIRRORS = [
    'https://overpass.private.coffee/api/interpreter',   # most reliable
    'https://overpass.kumi.systems/api/interpreter',
    'https://overpass-api.de/api/interpreter',           # main (often overloaded)
]
OVERPASS_URL = OVERPASS_MIRRORS[0]  # default; overpass_query() tries all mirrors
HEADERS = {'User-Agent': 'JBizzRealEstateTracker/1.0 (jmcscavalheiro@gmail.com)'}

SUPERMARKET_BRANDS = ['Continente', 'Pingo Doce', 'Lidl', 'Aldi', 'Mercadona',
                       'Auchan', 'Intermarché', 'El Corte Inglés', 'Mini Preço',
                       'Minipreço', 'Spar', 'Jumbo']

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return round(R * 2 * math.asin(math.sqrt(a)), 2)

def overpass_query(lat, lng, radius_m, amenity_filters):
    """Query Overpass for nearby POIs, trying multiple mirrors on failure."""
    filters_str = '\n'.join([f'  node[{f}](around:{radius_m},{lat},{lng});' for f in amenity_filters])
    query = f"""[out:json][timeout:20];
(
{filters_str}
);
out body;
"""
    for mirror in OVERPASS_MIRRORS:
        for attempt in range(2):
            try:
                r = requests.post(mirror, data={'data': query}, headers=HEADERS, timeout=25)
                if r.status_code == 200:
                    return r.json().get('elements', [])
                time.sleep(1)
            except requests.Timeout:
                time.sleep(2)
            except Exception:
                time.sleep(1)
    return []

def find_nearest(lat, lng, elements):
    """Find nearest element and return (name, distance_km)"""
    best = None
    best_dist = float('inf')
    for el in elements:
        elat = el.get('lat')
        elon = el.get('lon')
        if elat and elon:
            d = haversine(lat, lng, elat, elon)
            if d < best_dist:
                best_dist = d
                name = el.get('tags', {}).get('name', 'Unknown')
                best = (name, round(best_dist, 2))
    return best

def enrich_listing(lid, lat, lng):
    result = {'id': lid, 'lat': lat, 'lng': lng}

    # Supermarkets (500m radius first, expand if none found)
    for radius in [500, 1000, 2000]:
        supers = overpass_query(lat, lng, radius, [
            '"shop"="supermarket"',
            '"shop"="grocery"',
        ])
        # Prefer big brands
        big = [e for e in supers if any(b.lower() in e.get('tags',{}).get('name','').lower() for b in SUPERMARKET_BRANDS)]
        target = big if big else supers
        nearest = find_nearest(lat, lng, target)
        if nearest:
            result['nearest_supermarket'] = nearest[0]
            result['nearest_supermarket_km'] = nearest[1]
            break
        time.sleep(0.3)

    # Pharmacies
    for radius in [300, 700, 1500]:
        pharmas = overpass_query(lat, lng, radius, ['"amenity"="pharmacy"'])
        nearest = find_nearest(lat, lng, pharmas)
        if nearest:
            result['nearest_pharmacy'] = nearest[0]
            result['nearest_pharmacy_km'] = nearest[1]
            break
        time.sleep(0.3)

    # Restaurants/cafes (get count + nearest within 300m)
    restaurants = overpass_query(lat, lng, 300, [
        '"amenity"="restaurant"',
        '"amenity"="cafe"',
        '"amenity"="bar"',
    ])
    result['restaurants_300m'] = len(restaurants)
    nearest_resto = find_nearest(lat, lng, restaurants)
    if nearest_resto:
        result['nearest_restaurant'] = nearest_resto[0]
        result['nearest_restaurant_km'] = nearest_resto[1]

    # Metro
    metro = overpass_query(lat, lng, 2000, [
        '"station"="subway"',
        '"railway"="subway_entrance"',
        '"railway"="station"',
    ])
    nearest_metro = find_nearest(lat, lng, metro)
    if nearest_metro:
        result['nearest_metro'] = nearest_metro[0]
        result['nearest_metro_km'] = nearest_metro[1]

    # Schools / international
    schools = overpass_query(lat, lng, 1000, ['"amenity"="school"', '"amenity"="college"'])
    result['schools_1km'] = len(schools)

    # Beach
    beach = overpass_query(lat, lng, 3000, ['"natural"="beach"', '"leisure"="beach_resort"'])
    nearest_beach = find_nearest(lat, lng, beach)
    if nearest_beach:
        result['nearest_beach'] = nearest_beach[0]
        result['nearest_beach_km'] = nearest_beach[1]

    # Build human-readable summary
    notes = []
    if result.get('nearest_supermarket_km'):
        s = result['nearest_supermarket']
        d = result['nearest_supermarket_km']
        walk = 'walking' if d < 0.4 else f'{d}km'
        notes.append(f"{s} ({walk})")
    if result.get('nearest_pharmacy_km'):
        d = result['nearest_pharmacy_km']
        walk = 'walking' if d < 0.3 else f'{d}km'
        notes.append(f"Pharmacy {walk}")
    if result.get('restaurants_300m', 0) > 0:
        notes.append(f"{result['restaurants_300m']} restaurants/cafes within 300m")
    if result.get('nearest_metro_km'):
        notes.append(f"Metro {result['nearest_metro_km']}km ({result.get('nearest_metro','')})")
    if result.get('nearest_beach_km'):
        notes.append(f"Beach {result['nearest_beach_km']}km")

    result['commerce_notes'] = ' · '.join(notes)
    return result

def main():
    print('🦞 Commerce Enricher')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 55)

    with open(GEO_FILE) as f:
        geo_list = json.load(f)

    # Load existing
    try:
        with open(COMMERCE_FILE) as f:
            existing = {c['id']: c for c in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {}

    to_process = [g for g in geo_list if g.get('lat') and g['id'] not in existing]
    print(f'📦 {len(to_process)} listings to process ({len(existing)} already done)\n')

    results = list(existing.values())

    # SIGTERM handler — save progress before dying
    def _on_sigterm(signum, frame):
        with open(COMMERCE_FILE, 'w') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f'\n💾 SIGTERM — saved {len(results)} entries to {COMMERCE_FILE}', flush=True)
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _on_sigterm)

    for i, g in enumerate(to_process):
        lid = g['id']
        lat, lng = g['lat'], g['lng']
        print(f'  [{i+1:3d}/{len(to_process)}] {lid} ({lat:.4f},{lng:.4f})', end=' ')

        res = enrich_listing(lid, lat, lng)
        results.append(res)
        existing[lid] = res

        notes_short = res.get('commerce_notes', '')[:70]
        print(f'→ {notes_short}')

        if (i + 1) % 5 == 0:  # checkpoint every 5 (was 10)
            with open(COMMERCE_FILE, 'w') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f'  💾 Saved ({i+1} done)')

        time.sleep(1.2)  # Overpass rate limit

    with open(COMMERCE_FILE, 'w') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f'\n✅ {len(results)} listings enriched → {COMMERCE_FILE}')

    # Merge into enriched_listings.json
    print('\n🔗 Merging into enriched_listings.json...')
    with open(ENRICHED_FILE) as f:
        enriched = json.load(f)

    commerce_map = {c['id']: c for c in results}
    updated = 0
    for e in enriched:
        c = commerce_map.get(e['id'])
        if c:
            e['nearest_supermarket'] = c.get('nearest_supermarket', '')
            e['nearest_supermarket_km'] = c.get('nearest_supermarket_km', '')
            e['nearest_pharmacy'] = c.get('nearest_pharmacy', '')
            e['nearest_pharmacy_km'] = c.get('nearest_pharmacy_km', '')
            e['restaurants_300m'] = c.get('restaurants_300m', 0)
            e['nearest_metro'] = c.get('nearest_metro', '')
            e['nearest_metro_km'] = c.get('nearest_metro_km', '')
            e['nearest_beach'] = c.get('nearest_beach', '')
            e['nearest_beach_km'] = c.get('nearest_beach_km', '')
            e['schools_1km'] = c.get('schools_1km', 0)
            e['commerce_notes'] = c.get('commerce_notes', '')
            updated += 1

    with open(ENRICHED_FILE, 'w') as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)
    print(f'  ✅ {updated}/{len(enriched)} updated')
    print(f'\nCopy to UI: cp {ENRICHED_FILE} /var/www/html/realestate/')

if __name__ == '__main__':
    main()
