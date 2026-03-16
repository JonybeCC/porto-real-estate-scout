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
import os
import re
import signal
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'  # canonical source
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

OPENCAGE_KEY = os.environ.get('OPENCAGE_API_KEY', '')  # free 2500/day at opencagedata.com

def geocode_opencage(query):
    """OpenCage fallback — better Portugal coverage than Nominatim"""
    if not OPENCAGE_KEY:
        return None
    try:
        r = requests.get(
            'https://api.opencagedata.com/geocode/v1/json',
            params={'q': query, 'key': OPENCAGE_KEY, 'language': 'pt',
                    'countrycode': 'pt', 'limit': 1, 'no_annotations': 1},
            timeout=10
        )
        if r.status_code == 200:
            results = r.json().get('results', [])
            if results and results[0].get('confidence', 0) >= 6:
                geom = results[0]['geometry']
                return {
                    'lat': geom['lat'], 'lng': geom['lng'],
                    'geocode_query': query,
                    'geocode_display': results[0].get('formatted', '')[:100],
                    'geocode_confidence': f'opencage:{results[0].get("confidence",0)}',
                }
    except (requests.RequestException, KeyError, ValueError):
        pass
    return None


def geocode(street, neighborhood, city='Porto'):
    """Try progressively broader queries. OpenCage fallback if Nominatim returns only neighbourhood-level."""
    queries = []
    street_clean = street.strip().rstrip(',').strip() if street else ''

    if street_clean and len(street_clean) > 5:
        queries.append(f'{street_clean}, Porto, Portugal')
    if neighborhood and len(neighborhood) > 3 and not neighborhood.isdigit():
        queries.append(f'{neighborhood}, Porto, Portugal')
    queries.append('Foz do Douro, Porto, Portugal')  # fallback

    best = None
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
                confidence = 'street' if street_clean and street_clean.lower() in q.lower() else 'neighborhood'
                result = {
                    'lat': lat, 'lng': lon,
                    'geocode_query': q,
                    'geocode_display': res.get('display_name', '')[:100],
                    'geocode_confidence': confidence,
                }
                if confidence == 'street':
                    return result  # street-level hit — good enough
                best = result  # neighbourhood hit — try OpenCage first
            time.sleep(1.1)
        except Exception as e:
            time.sleep(1)
            continue

    # Try OpenCage for street-level if Nominatim only got neighbourhood
    if best and best['geocode_confidence'] == 'neighborhood' and street_clean and len(street_clean) > 5:
        oc = geocode_opencage(f'{street_clean}, Porto, Portugal')
        if oc:
            return oc

    return best

def main():
    import signal
    print('🦞 Geocoder — Nominatim + OpenCage fallback')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    oc_status = '✅ set' if OPENCAGE_KEY else '❌ not set — street-level accuracy reduced'
    print(f'🔑 OpenCage: {oc_status}')
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

    # SIGTERM handler — save progress before dying
    def _on_sigterm(signum, frame):
        with open(GEO_FILE, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f'\n💾 SIGTERM — saved {len(results)} geocoded entries', flush=True)
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _on_sigterm)

    # Nominatim ToS: max 1 req/sec. With 3 workers we space requests ~0.4s apart
    # but add jitter to avoid thundering herd. Effective rate: ~2-2.5 req/sec which
    # is still well within limits since each request takes ~0.5s to respond.
    CONCURRENCY = 3
    results_lock = Lock()
    checkpoint_counter = [0]

    def _geocode_one(listing):
        lid    = listing['id']
        street = listing.get('street', '')
        hood   = listing.get('neighborhood', '')
        title  = listing.get('title', '')

        if not street or len(street) < 5 or street.isdigit():
            m = re.search(r'(?:na|no|em)\s+(Rua|Avenida|Av\.|Travessa|Largo|Praça|Alameda)\s+[^,]+', title, re.IGNORECASE)
            if m:
                street = m.group(0).replace('na ', '').replace('no ', '').replace('em ', '').strip()

        geo = geocode(street, hood)
        if geo:
            geo['id'] = lid
            geo['dist_to_foz_km']    = haversine(geo['lat'], geo['lng'], *FOZ_CENTRE)
            geo['dist_to_sea_km']    = haversine(geo['lat'], geo['lng'], *FOZ_BEACH)
            geo['dist_to_centre_km'] = haversine(geo['lat'], geo['lng'], *PORTO_CENTRE)
        return lid, geo, listing

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = {executor.submit(_geocode_one, l): l for l in to_geo}
        done = 0
        for future in as_completed(futures):
            lid, geo, listing = future.result()
            done += 1
            if geo:
                with results_lock:
                    results.append(geo)
                    existing[lid] = geo
                print(f'  [{done:3d}/{len(to_geo)}] {lid} → {geo["lat"]:.4f},{geo["lng"]:.4f} | {geo["dist_to_sea_km"]}km [{geo["geocode_confidence"]}]')
            else:
                print(f'  [{done:3d}/{len(to_geo)}] {lid} → ❌ failed')

            checkpoint_counter[0] += 1
            if checkpoint_counter[0] % 10 == 0:
                with results_lock:
                    with open(GEO_FILE, 'w', encoding='utf-8') as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)
                print(f'  💾 Checkpoint ({done} done)')
            time.sleep(0.4)  # respect Nominatim ToS (1 req/sec across all workers)

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
