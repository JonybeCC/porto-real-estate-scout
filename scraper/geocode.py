#!/usr/bin/env python3
"""
Geocoder — JBizz Assistant 🦞
Uses Nominatim (free, no key) to get lat/lng for each listing.
Calculates distance from Foz beach reference point.
Adds: lat, lng, distance_to_foz_km, distance_to_sea_km
"""

import json
import time
import math
import requests
from datetime import datetime

LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings_deduped.json'
ENRICHED_FILE = '/root/.openclaw/workspace/projects/real-estate/data/enriched_listings.json'
GEO_FILE      = '/root/.openclaw/workspace/projects/real-estate/data/geocoded.json'

# Foz do Douro reference points
FOZ_CENTRE = (41.1579, -8.6773)   # Praça Gonçalves Zarco, Foz
FOZ_BEACH  = (41.1545, -8.6810)   # Praia de Matosinhos / Foz beach
PORTO_CENTRE = (41.1579, -8.6291) # Aliados

NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search'
HEADERS = {'User-Agent': 'JBizzRealEstateTracker/1.0 (jmcscavalheiro@gmail.com)'}

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return round(R * 2 * math.asin(math.sqrt(a)), 2)

def geocode(street, neighborhood, city='Porto'):
    """Try progressively broader queries until we get a result."""
    queries = []

    # Build clean street name
    street_clean = street.strip().rstrip(',').strip() if street else ''

    if street_clean and len(street_clean) > 5:
        queries.append(f'{street_clean}, Porto, Portugal')
    if neighborhood and len(neighborhood) > 3 and not neighborhood.isdigit():
        queries.append(f'{neighborhood}, Porto, Portugal')
    queries.append('Foz do Douro, Porto, Portugal')  # fallback

    for q in queries:
        try:
            r = requests.get(NOMINATIM_URL, params={
                'q': q, 'format': 'json', 'limit': 1,
                'countrycodes': 'pt', 'addressdetails': 1
            }, headers=HEADERS, timeout=10)
            results = r.json()
            if results:
                res = results[0]
                lat, lon = float(res['lat']), float(res['lon'])
                return {
                    'lat': lat, 'lng': lon,
                    'geocode_query': q,
                    'geocode_display': res.get('display_name', '')[:100],
                    'geocode_confidence': 'street' if street_clean in q else 'neighborhood',
                }
            time.sleep(1.1)  # Nominatim rate limit: 1 req/sec
        except Exception as e:
            time.sleep(1)
            continue

    return None

def main():
    print('🦞 Geocoder — Nominatim')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 50)

    with open(LISTINGS_FILE, encoding='utf-8') as f:
        listings = json.load(f)

    # Load existing
    try:
        with open(GEO_FILE, encoding='utf-8') as f:
            existing = {g['id']: g for g in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {}

    to_geo = [l for l in listings if l['id'] not in existing]
    print(f'📦 {len(listings)} listings | {len(existing)} already geocoded | {len(to_geo)} to process\n')

    results = list(existing.values())

    for i, listing in enumerate(to_geo):
        lid   = listing['id']
        street = listing.get('street', '')
        hood   = listing.get('neighborhood', '')
        title  = listing.get('title', '')

        # Extract street from title if field is empty/vague
        if not street or len(street) < 5 or street.isdigit():
            import re
            m = re.search(r'(?:na|no|em)\s+(Rua|Avenida|Av\.|Travessa|Largo|Praça|Alameda)\s+[^,]+', title, re.IGNORECASE)
            if m:
                street = m.group(0).replace('na ', '').replace('no ', '').replace('em ', '').strip()

        print(f'  [{i+1:3d}/{len(to_geo)}] {lid} | {listing.get("rooms")} {listing.get("size_m2")}m² | {street[:30]} | {hood[:25]}', end=' ')

        geo = geocode(street, hood)

        if geo:
            geo['id'] = lid
            geo['dist_to_foz_km']  = haversine(geo['lat'], geo['lng'], *FOZ_CENTRE)
            geo['dist_to_sea_km']  = haversine(geo['lat'], geo['lng'], *FOZ_BEACH)
            geo['dist_to_centre_km'] = haversine(geo['lat'], geo['lng'], *PORTO_CENTRE)
            results.append(geo)
            existing[lid] = geo
            print(f'→ {geo["lat"]:.4f},{geo["lng"]:.4f} | {geo["dist_to_sea_km"]}km from sea [{geo["geocode_confidence"]}]')
        else:
            print('→ ❌ failed')

        if (i + 1) % 20 == 0:
            with open(GEO_FILE, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f'  💾 Saved ({i+1} done)')

        time.sleep(1.2)

    with open(GEO_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f'\n✅ {len(results)} listings geocoded → {GEO_FILE}')

    # Stats
    dists = [g['dist_to_sea_km'] for g in results if g.get('dist_to_sea_km') is not None]
    if dists:
        print(f'📊 Distance to sea: {min(dists)} – {max(dists)} km | Avg: {sum(dists)/len(dists):.2f}km')

    # Merge into enriched
    print('\n🔗 Merging geo data into enriched_listings.json...')
    try:
        with open(ENRICHED_FILE, encoding='utf-8') as f:
            enriched = json.load(f)
        geo_map = {g['id']: g for g in results}
        updated = 0
        for e in enriched:
            g = geo_map.get(e['id'])
            if g:
                e['lat']               = g.get('lat')
                e['lng']               = g.get('lng')
                e['dist_to_sea_km']    = g.get('dist_to_sea_km')
                e['dist_to_foz_km']    = g.get('dist_to_foz_km')
                e['dist_to_centre_km'] = g.get('dist_to_centre_km')
                e['geocode_confidence'] = g.get('geocode_confidence')
                updated += 1
        with open(ENRICHED_FILE, 'w', encoding='utf-8') as f:
            json.dump(enriched, f, ensure_ascii=False, indent=2)
        print(f'  ✅ {updated}/{len(enriched)} enriched listings updated with geo data')
    except Exception as ex:
        print(f'  ⚠️  Could not merge: {ex}')

if __name__ == '__main__':
    main()
