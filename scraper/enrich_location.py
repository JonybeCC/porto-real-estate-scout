#!/usr/bin/env python3
"""
Location Enricher v3 — JBizz Assistant 🦞  ★ CANONICAL AUTHORITY for geocoded.json ★

This is the SINGLE SOURCE OF TRUTH for all location-derived fields.
It is the ONLY script that writes enrichment data to geocoded.json.
Do NOT add location enrichment to other scripts — add it here.

What it enriches (per listing, stored in geocoded.json):
  Geocoding:     lat, lng, dist_to_sea/foz/centre (written by geocode.py, READ-ONLY here)
  Elevation:     elevation_m, walk_time_sea_min     (from enrich_geo.py on first run)
  Overpass/OSM:  parks_800m, nearest_park
                 hospitals_3km, nearest_hospital, nearest_hospital_km
                 bus_stops_400m
                 noise_penalty, noise_sources
  Schools:       school_score, nearest_good_school, nearest_school_km
  Commerce tier: supermarket_tier                   (from commerce.json nearest_supermarket)
  Desc signals:  is_furnished, kitchen_equipped, has_suite, has_fireplace, has_ac,
                 has_pool, has_concierge, light_mentioned, double_glazing,
                 renovation_year, description_bonus_pts

Superseded (archived to scraper/_deprecated/):
  - enrich_advanced.py     (subset of the above)
  - enrich_noise_schools.py (subset of the above)
"""

import json, os, time, math, re, requests
from datetime import datetime

GEO_FILE      = '/root/.openclaw/workspace/projects/real-estate/data/geocoded.json'
DETAILS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
COMMERCE_FILE = '/root/.openclaw/workspace/projects/real-estate/data/commerce.json'
LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
OVERPASS_URL  = 'https://overpass-api.de/api/interpreter'

# Porto top schools (name, lat, lng, rating)
PORTO_SCHOOLS = [
    ('Colégio Luso-Francês',           41.1612, -8.6290, 9.5),
    ('Colégio de Nossa Srª do Rosário', 41.1580, -8.6350, 9.0),
    ('Colégio Alemão do Porto',         41.1571, -8.6401, 9.0),
    ('Oporto British School',           41.1750, -8.6260, 8.5),
    ('CLIP - Porto Int. School',        41.1654, -8.6889, 8.5),
    ('Esc. Sec. Rodrigues de Freitas',  41.1501, -8.6093, 8.0),
    ('Esc. Sec. Carolina Michaëlis',    41.1618, -8.6250, 7.5),
    ('Esc. Sec. Filipa de Vilhena',     41.1582, -8.5999, 7.5),
    ('Esc. Artística Soares dos Reis',  41.1472, -8.6100, 7.5),
    ('Esc. Sec. Garcia de Orta',        41.1732, -8.5881, 7.0),
    ('Esc. Sec. Fontes Pereira de Melo',41.1558, -8.6163, 7.0),
    ('Esc. Básica do Amial',            41.1750, -8.5940, 6.5),
    ('Esc. Básica de Aldoar',           41.1712, -8.6720, 6.5),
    ('Esc. Básica Pinheiro Manso',      41.1644, -8.6668, 6.5),
]

SUPERMARKET_TIERS = {
    3: ['el corte inglés', 'corte inglês', 'waitrose'],
    2: ['continente', 'pingo doce', 'minipreço', 'mercadona', 'auchan'],
    1: ['lidl', 'aldi', 'intermarché', 'dia'],
}


def haversine(lat1, lng1, lat2, lng2) -> float:
    R = 6371
    dlat, dlng = math.radians(lat2-lat1), math.radians(lng2-lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlng/2)**2
    return round(R * 2 * math.asin(math.sqrt(a)), 3)


OVERPASS_MIRRORS = [
    'https://maps.mail.ru/osm/tools/overpass/api/interpreter',  # fast + reliable
    'https://overpass-api.de/api/interpreter',                   # main (occasionally 504)
    'https://overpass.private.coffee/api/interpreter',           # sometimes down
    'https://overpass.kumi.systems/api/interpreter',             # sometimes down
]

def overpass_request(query: str) -> list:
    """Try multiple Overpass mirrors with retry."""
    for mirror in OVERPASS_MIRRORS:
        for attempt in range(2):
            try:
                r = requests.post(mirror, data={'data': query}, timeout=25)
                if r.status_code == 200:
                    return r.json().get('elements', [])
            except (requests.RequestException, ValueError, KeyError):
                pass
            time.sleep(1)
    return []


def overpass_all(lat: float, lng: float) -> dict:
    """
    Two-pass Overpass query (amenities + roads) with mirror fallback.
    Returns parsed dict with parks, hospitals, buses, noise sources.
    """
    # Pass 1: Amenities (parks, hospitals, buses)
    q1 = f"""[out:json][timeout:20];
(
  way["leisure"="park"]["name"](around:800,{lat},{lng});
  node["amenity"="hospital"]["name"](around:3000,{lat},{lng});
  node["amenity"="clinic"]["name"](around:2000,{lat},{lng});
  way["amenity"="hospital"]["name"](around:3000,{lat},{lng});
  node["highway"="bus_stop"](around:400,{lat},{lng});
);
out center tags;"""
    elements_1 = overpass_request(q1)
    time.sleep(1)

    # Pass 2: Noise sources (roads/rail)
    q2 = f"""[out:json][timeout:15];
(
  way["highway"~"motorway|trunk"](around:250,{lat},{lng});
  way["highway"="primary"](around:150,{lat},{lng});
  way["railway"~"rail|tram"](around:300,{lat},{lng});
);
out center tags;"""
    elements_2 = overpass_request(q2)

    elements = elements_1 + elements_2
    if not elements:
        return {}

    def tag(e, k): return e.get('tags', {}).get(k, '')
    def center_lat(e): return e.get('center', e).get('lat', lat)
    def center_lng(e): return e.get('center', e).get('lon', lng)

    parks     = [e for e in elements if tag(e,'leisure') == 'park' and tag(e,'name')]
    hospitals = [e for e in elements if tag(e,'amenity') in ('hospital','clinic') and tag(e,'name')]
    buses     = [e for e in elements if tag(e,'highway') == 'bus_stop']
    motorways = [e for e in elements if tag(e,'highway') in ('motorway','trunk')]
    primary   = [e for e in elements if tag(e,'highway') == 'primary']
    rail      = [e for e in elements if tag(e,'railway') == 'rail']
    tram      = [e for e in elements if tag(e,'railway') == 'tram']

    hosp_sorted = sorted(hospitals, key=lambda e: haversine(lat, lng, center_lat(e), center_lng(e)))
    nearest_hosp = hosp_sorted[0] if hosp_sorted else None
    hosp_km = haversine(lat, lng, center_lat(nearest_hosp), center_lng(nearest_hosp)) if nearest_hosp else None

    noise_penalty = 0
    noise_sources = []
    if motorways:
        noise_penalty -= 3; noise_sources.append(f'Motorway/trunk <250m ({len(motorways)} segments)')
    elif primary:
        noise_penalty -= 2; noise_sources.append(f'Primary road <150m')
    if rail:
        noise_penalty -= 2; noise_sources.append('Railway <300m')
    elif tram:
        noise_penalty -= 1; noise_sources.append('Tram <200m')

    park_names = [tag(e,'name') for e in parks]

    return {
        'parks_800m':          len(parks),
        'nearest_park':        park_names[0] if park_names else '',
        'hospitals_3km':       len(hospitals),
        'nearest_hospital':    f'{tag(nearest_hosp,"name")} ({hosp_km:.1f}km)' if nearest_hosp else '',
        'nearest_hospital_km': hosp_km,
        'bus_stops_400m':      len(buses),
        'noise_penalty':       max(noise_penalty, -5),
        'noise_sources':       ', '.join(noise_sources) if noise_sources else 'None detected',
    }


def _legacy_overpass_all(lat: float, lng: float) -> dict:
    """Legacy single-query version kept for reference."""
    query = f"""[out:json][timeout:25];
(
  way["leisure"="park"]["name"](around:800,{lat},{lng});
  relation["leisure"="park"]["name"](around:800,{lat},{lng});
  node["amenity"="hospital"]["name"](around:3000,{lat},{lng});
  node["amenity"="clinic"]["name"](around:2000,{lat},{lng});
  way["amenity"="hospital"]["name"](around:3000,{lat},{lng});
  node["highway"="bus_stop"](around:400,{lat},{lng});
  way["highway"="motorway"](around:250,{lat},{lng});
  way["highway"="trunk"](around:200,{lat},{lng});
  way["highway"="primary"](around:150,{lat},{lng});
  way["railway"="rail"](around:300,{lat},{lng});
  way["railway"="tram"](around:200,{lat},{lng});
);
out center tags;"""
    try:
        r = requests.post(OVERPASS_URL, data={'data': query}, timeout=30)
        if r.status_code != 200:
            return {}
        elements = r.json().get('elements', [])

        def tag(e, k): return e.get('tags', {}).get(k, '')
        def center_lat(e): return e.get('center', e).get('lat', lat)
        def center_lng(e): return e.get('center', e).get('lon', lng)

        parks     = [e for e in elements if tag(e,'leisure') in ('park','garden') and tag(e,'name')]
        hospitals = [e for e in elements if tag(e,'amenity') in ('hospital','clinic') and tag(e,'name')]
        buses     = [e for e in elements if tag(e,'highway') == 'bus_stop']
        motorways = [e for e in elements if tag(e,'highway') in ('motorway','trunk')]
        primary   = [e for e in elements if tag(e,'highway') == 'primary']
        rail      = [e for e in elements if tag(e,'railway') == 'rail']
        tram      = [e for e in elements if tag(e,'railway') == 'tram']

        # Nearest hospital
        hosp_sorted = sorted(hospitals, key=lambda e: haversine(lat, lng, center_lat(e), center_lng(e)))
        nearest_hosp = hosp_sorted[0] if hosp_sorted else None
        hosp_km = haversine(lat, lng, center_lat(nearest_hosp), center_lng(nearest_hosp)) if nearest_hosp else None

        # Noise
        noise_penalty = 0
        noise_sources = []
        if motorways:
            noise_penalty -= 3; noise_sources.append(f'Motorway/trunk road <250m ({len(motorways)} segments)')
        elif primary:
            noise_penalty -= 2; noise_sources.append(f'Primary road <150m ({len(primary)} segments)')
        if rail:
            noise_penalty -= 2; noise_sources.append(f'Railway <300m')
        elif tram:
            noise_penalty -= 1; noise_sources.append(f'Tram line <200m')

        park_names = [tag(e,'name') for e in parks]

        return {
            'parks_800m':         len(parks),
            'nearest_park':       park_names[0] if park_names else '',
            'hospitals_3km':      len(hospitals),
            'nearest_hospital':   f'{tag(nearest_hosp,"name")} ({hosp_km:.1f}km)' if nearest_hosp else '',
            'nearest_hospital_km': hosp_km,
            'bus_stops_400m':     len(buses),
            'noise_penalty':      max(noise_penalty, -5),
            'noise_sources':      ', '.join(noise_sources) if noise_sources else 'None detected',
        }
    except Exception as e:
        print(f'    ⚠️  Overpass error: {str(e)[:60]}')
        return {}


def school_score(lat: float, lng: float) -> tuple:
    best_score, best_name, best_dist = 0.0, '', 99.0
    for name, slat, slng, rating in PORTO_SCHOOLS:
        dist = haversine(lat, lng, slat, slng)
        if dist <= 1.5:
            decay = max(0, 1 - dist / 1.5)
            weighted = rating * decay
            if weighted > best_score:
                best_score, best_name, best_dist = weighted, f'{name} ({dist:.1f}km)', dist
    return round(best_score, 1), best_name, round(best_dist, 2)


def extract_signals(desc: str, tags: str = '') -> dict:
    c = (desc + ' ' + tags).lower()
    signals = {
        'is_furnished':     any(x in c for x in ['mobilado','mobilada','furnished','com móveis','com mobília']),
        'kitchen_equipped': any(x in c for x in ['cozinha equipada','cozinha completa','electrodomésticos','eletrodomésticos']),
        'has_suite':        any(x in c for x in ['suite','suíte','master bedroom']),
        'has_fireplace':    any(x in c for x in ['lareira','fireplace','salamandra','recuperador']),
        'has_ac':           any(x in c for x in ['ar condicionado','ar-condicionado','a/c','climatização']),
        'has_pool':         any(x in c for x in ['piscina','swimming pool']),
        'has_concierge':    any(x in c for x in ['porteiro','concierge','segurança 24','vigilância 24']),
        'light_mentioned':  any(x in c for x in ['muito luminoso','bastante luminoso','luz natural abundante']),
        'double_glazing':   any(x in c for x in ['vidros duplos','caixilharia pvc','isolamento acústico']),
        'renovation_year':  None,
    }
    for pat in [r'remodelado em (\d{4})', r'renovado em (\d{4})', r'obras em (\d{4})', r'construção (\d{4})']:
        m = re.search(pat, c)
        if m:
            yr = int(m.group(1))
            if 1990 <= yr <= 2026:
                signals['renovation_year'] = yr
                break

    pts = 0
    if signals['is_furnished']:     pts += 4
    if signals['kitchen_equipped']: pts += 2
    if signals['has_suite']:        pts += 2
    if signals['has_fireplace']:    pts += 1
    if signals['has_ac']:           pts += 2
    if signals['has_pool']:         pts += 3
    if signals['has_concierge']:    pts += 2
    if signals['light_mentioned']:  pts += 1
    if signals['double_glazing']:   pts += 1
    yr = signals['renovation_year']
    if yr:
        age = 2026 - yr
        pts += 4 if age <= 2 else (3 if age <= 5 else (2 if age <= 10 else 1))
    signals['description_bonus_pts'] = pts
    return signals


def supermarket_tier(name: str) -> int:
    n = name.lower()
    for tier, brands in SUPERMARKET_TIERS.items():
        if any(b in n for b in brands): return tier
    return 2



def enrich_one(g: dict, listings: dict, details: dict, commerce: dict) -> dict:
    """Enrich a single listing — safe to run in thread pool."""
    import copy
    g = copy.deepcopy(g)
    lid = g['id']
    lat, lng = g['lat'], g['lng']
    l   = listings.get(lid, {})
    det = details.get(lid, {})
    com = commerce.get(lid, {})

    ov = overpass_all(lat, lng)
    g.update(ov)

    s_score, s_name, s_dist = school_score(lat, lng)
    g['school_score']        = s_score
    g['nearest_good_school'] = s_name
    g['nearest_school_km']   = s_dist

    desc = (det.get('full_description', '') or '') + ' ' + (l.get('description', '') or '')
    tags = l.get('tags', '') or ''
    sig  = extract_signals(desc, tags)
    g.update(sig)
    g['supermarket_tier'] = supermarket_tier(com.get('nearest_supermarket', ''))

    return g


def save_checkpoint(geo_map, commerce, done, total, force=False):
    """Save geo + commerce. Called every 10 listings and on SIGTERM."""
    with open(GEO_FILE, 'w') as f:
        json.dump(list(geo_map.values()), f, ensure_ascii=False, indent=2)
    with open(COMMERCE_FILE, 'w') as f:
        json.dump(list(commerce.values()), f, ensure_ascii=False, indent=2)
    if force:
        print(f'  💾 SIGTERM checkpoint saved ({done}/{total})', flush=True)
    else:
        print(f'  💾 Checkpoint ({done}/{total})', flush=True)


def main():
    import signal
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    CONCURRENCY = 4  # 4 parallel — safe for Overpass public mirrors

    print('📍 Location Enricher v3 (concurrent, 4 parallel)')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 55)

    with open(GEO_FILE) as f:      geo_list = json.load(f)
    with open(DETAILS_FILE) as f:  details  = {d['id']: d for d in json.load(f)}
    with open(LISTINGS_FILE) as f: listings = {l['id']: l for l in json.load(f)}
    try:
        with open(COMMERCE_FILE) as f: commerce = {c['id']: c for c in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        commerce = {}

    geo_map = {g['id']: g for g in geo_list}

    to_enrich = [g for g in geo_list if g.get('lat') and
                 (g.get('noise_penalty') is None or g.get('parks_800m') is None or
                  g.get('school_score') is None or g.get('is_furnished') is None)]

    total = len(to_enrich)
    est   = max(1, total // CONCURRENCY) * 2  # rough minutes
    print(f'📦 {total} listings need enrichment (~{est} min at {CONCURRENCY}x parallel)\n')

    lock = threading.Lock()
    done_ref = [0]  # mutable for signal handler closure

    # SIGTERM handler: save progress before dying
    def _on_sigterm(signum, frame):
        with lock:
            save_checkpoint(geo_map, commerce, done_ref[0], total, force=True)
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _on_sigterm)

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {ex.submit(enrich_one, g, listings, details, commerce): g['id'] for g in to_enrich}

        for future in as_completed(futures):
            lid = futures[future]
            try:
                result = future.result()
                with lock:
                    geo_map[lid] = result
                    if lid in commerce:
                        for field in ['parks_800m','nearest_park','hospitals_3km','nearest_hospital',
                                      'nearest_hospital_km','bus_stops_400m','noise_penalty','noise_sources',
                                      'school_score','nearest_good_school','supermarket_tier',
                                      'is_furnished','kitchen_equipped','has_suite','has_fireplace',
                                      'has_ac','has_pool','has_concierge','double_glazing',
                                      'renovation_year','description_bonus_pts']:
                            commerce[lid][field] = result.get(field)
                    done_ref[0] += 1
                    done = done_ref[0]

                    flags = []
                    if result.get('parks_800m', 0) > 0:            flags.append(f'🌳{result["parks_800m"]}')
                    if result.get('bus_stops_400m', 0) > 0:         flags.append(f'🚌{result["bus_stops_400m"]}')
                    if (result.get('noise_penalty') or 0) < 0:      flags.append(f'🔊{result["noise_penalty"]}')
                    if result.get('school_score', 0) > 0:            flags.append(f'🏫{result["school_score"]}')
                    if result.get('is_furnished'):                   flags.append('🛋️')
                    if result.get('has_suite'):                      flags.append('🛁')
                    if result.get('has_ac'):                         flags.append('❄️')
                    if result.get('renovation_year'):                flags.append(f'🔨{result["renovation_year"]}')
                    bonus = result.get('description_bonus_pts', 0)
                    if bonus > 0: flags.append(f'+{bonus}pts')
                    print(f'  [{done:3d}/{total}] {lid} {" ".join(flags) or "basic"}', flush=True)

                    if done % 10 == 0:  # checkpoint every 10 (was 20) for better SIGTERM resilience
                        save_checkpoint(geo_map, commerce, done, total)

            except Exception as e:
                with lock:
                    done_ref[0] += 1
                print(f'  ⚠️  [{done_ref[0]}/{total}] {lid}: {str(e)[:60]}', flush=True)

    # Final save
    save_checkpoint(geo_map, commerce, done_ref[0], total)

    gv = list(geo_map.values())
    print(f'\n✅ Done — {done}/{total} enriched')
    print(f'   🏫 School:    {sum(1 for g in gv if g.get("school_score",0)>0)}')
    print(f'   🔊 Noisy:     {sum(1 for g in gv if (g.get("noise_penalty") or 0)<0)}')
    print(f'   🌳 Parks:     {sum(1 for g in gv if g.get("parks_800m",0)>0)}')
    print(f'   🚌 Buses:     {sum(1 for g in gv if g.get("bus_stops_400m",0)>=4)}')
    print(f'   🛋️  Furnished: {sum(1 for g in gv if g.get("is_furnished"))}')
    print(f'   🛁 Suite:     {sum(1 for g in gv if g.get("has_suite"))}')
    print(f'   ❄️  AC:        {sum(1 for g in gv if g.get("has_ac"))}')
    print(f'   🔨 Reno:      {sum(1 for g in gv if g.get("renovation_year"))}')


if __name__ == '__main__':
    main()
