#!/usr/bin/env python3
"""
Noise & School Enricher — JBizz Assistant 🦞
Feature 9: School quality score via proximity to known Porto schools
Feature 10: Noise penalty via Overpass road/rail proximity

APIs: Overpass (free), no key needed.
Porto top schools: manually curated coordinates (INE data not API-accessible).
"""

import json, time, math, requests
from datetime import datetime

GEO_FILE      = '/root/.openclaw/workspace/projects/real-estate/data/geocoded.json'
COMMERCE_FILE = '/root/.openclaw/workspace/projects/real-estate/data/commerce.json'
OVERPASS_URL  = 'https://overpass-api.de/api/interpreter'

# ── Porto school database ──────────────────────────────────────────────────
# Curated from DGEEC rankings + local knowledge. Rating 1-10.
PORTO_SCHOOLS = [
    # Name, lat, lng, rating, type (public/private), level (secondary/primary)
    ('Colégio Luso-Francês',          41.1612, -8.6290, 9.5, 'private',  'secondary'),
    ('Colégio de Nossa Srª do Rosário',41.1580, -8.6350, 9.0, 'private',  'secondary'),
    ('Colégio Alemão do Porto',        41.1571, -8.6401, 9.0, 'private',  'international'),
    ('Oporto British School',          41.1750, -8.6260, 8.5, 'private',  'international'),
    ('CLIP - Porto Int. School',       41.1654, -8.6889, 8.5, 'private',  'international'),
    ('Esc. Sec. Rodrigues de Freitas', 41.1501, -8.6093, 8.0, 'public',   'secondary'),
    ('Esc. Sec. Carolina Michaëlis',   41.1618, -8.6250, 7.5, 'public',   'secondary'),
    ('Esc. Sec. Filipa de Vilhena',    41.1582, -8.5999, 7.5, 'public',   'secondary'),
    ('Esc. Artística Soares dos Reis', 41.1472, -8.6100, 7.5, 'public',   'secondary'),
    ('Esc. Sec. Garcia de Orta',       41.1732, -8.5881, 7.0, 'public',   'secondary'),
    ('Esc. Sec. Fontes Pereira de Melo',41.1558,-8.6163, 7.0, 'public',   'secondary'),
    ('Esc. Básica do Amial',           41.1750, -8.5940, 6.5, 'public',   'primary'),
    ('Esc. Básica de Aldoar',          41.1712, -8.6720, 6.5, 'public',   'primary'),
    ('Esc. Básica Pinheiro Manso',     41.1644, -8.6668, 6.5, 'public',   'primary'),
]


def haversine(lat1, lng1, lat2, lng2) -> float:
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    return round(R * 2 * math.asin(math.sqrt(a)), 3)


def school_score(lat: float, lng: float) -> tuple[float, str, float]:
    """
    Returns (score 0-10, nearest_school_name, distance_km).
    Score based on quality of best school within 1.5km.
    """
    best_score = 0.0
    best_name  = ''
    best_dist  = 99.0

    for name, slat, slng, rating, stype, level in PORTO_SCHOOLS:
        dist = haversine(lat, lng, slat, slng)
        if dist <= 1.5:
            # Weighted score: rating × distance decay
            decay = max(0, 1 - dist / 1.5)
            weighted = rating * decay
            if weighted > best_score:
                best_score = weighted
                best_name  = f'{name} ({dist:.1f}km)'
                best_dist  = dist

    return round(best_score, 1), best_name, round(best_dist, 2)


def overpass_query(lat: float, lng: float, radius_m: int, filters: list) -> list:
    """Query Overpass API for features near a point."""
    filter_str = '\n'.join([
        f'  way[{f}](around:{radius_m},{lat},{lng});'
        for f in filters
    ])
    query = f'[out:json][timeout:10];\n(\n{filter_str}\n);\nout center;'
    try:
        r = requests.post(OVERPASS_URL, data={'data': query}, timeout=15)
        if r.status_code == 200:
            return r.json().get('elements', [])
    except:
        pass
    return []


def noise_assessment(lat: float, lng: float) -> tuple[int, list]:
    """
    Returns (penalty 0 to -5, list of noise sources).
    Queries Overpass for major roads and railway within 500m.
    """
    penalty = 0
    sources = []

    # Major roads within 200m
    motorways = overpass_query(lat, lng, 200, ['"highway"="motorway"', '"highway"="trunk"'])
    if motorways:
        penalty -= 3
        sources.append(f'motorway/trunk road <200m ({len(motorways)} segments)')

    # Busy roads within 200m
    if not motorways:
        primary = overpass_query(lat, lng, 150, ['"highway"="primary"'])
        if primary:
            penalty -= 2
            sources.append(f'primary road <150m ({len(primary)} segments)')

    # Railway within 300m
    railway = overpass_query(lat, lng, 300, ['"railway"="rail"', '"railway"="tram"'])
    if railway:
        rail_types = set(e.get('tags', {}).get('railway', '') for e in railway)
        if 'rail' in rail_types:
            penalty -= 2
            sources.append(f'railway <300m')
        elif 'tram' in rail_types:
            penalty -= 1
            sources.append(f'tram line <300m')

    return max(penalty, -5), sources


def main():
    print('🏫🔊 Noise & School Enricher')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 55)

    with open(GEO_FILE) as f:
        geo_list = json.load(f)
    geo_map = {g['id']: g for g in geo_list}

    try:
        with open(COMMERCE_FILE) as f:
            commerce_list = json.load(f)
        commerce_map = {c['id']: c for c in commerce_list}
    except:
        commerce_map = {}

    # Find listings needing enrichment
    needs_work = [g for g in geo_list
                  if g.get('lat') and
                  (g.get('school_score') is None or g.get('noise_penalty') is None)]

    print(f'📦 {len(needs_work)} listings to enrich\n')

    done = 0
    for g in needs_work:
        lid  = g['id']
        lat, lng = g['lat'], g['lng']

        # School score
        s_score, s_name, s_dist = school_score(lat, lng)
        g['school_score']        = s_score
        g['nearest_good_school'] = s_name
        g['nearest_school_km']   = s_dist

        # Noise penalty (Overpass query — rate limited)
        penalty, sources = noise_assessment(lat, lng)
        g['noise_penalty']  = penalty
        g['noise_sources']  = ', '.join(sources) if sources else 'None detected'

        # Also update commerce_map
        if lid in commerce_map:
            commerce_map[lid]['school_score']        = s_score
            commerce_map[lid]['nearest_good_school'] = s_name
            commerce_map[lid]['noise_penalty']       = penalty
            commerce_map[lid]['noise_sources']       = g['noise_sources']

        done += 1
        noise_str  = f'noise={penalty}' if penalty else 'quiet'
        school_str = f'school={s_score}' if s_score else 'no school nearby'
        print(f'  [{done:3d}/{len(needs_work)}] {lid} | {noise_str} | {school_str}')

        if sources:
            print(f'    🔊 {", ".join(sources[:2])}')
        if s_name:
            print(f'    🏫 {s_name}')

        # Checkpoint every 15
        if done % 15 == 0:
            with open(GEO_FILE, 'w') as f:
                json.dump(list(geo_map.values()), f, ensure_ascii=False, indent=2)
            if commerce_map:
                with open(COMMERCE_FILE, 'w') as f:
                    json.dump(list(commerce_map.values()), f, ensure_ascii=False, indent=2)
            print(f'  💾 Checkpoint saved')

        time.sleep(1.2)  # Overpass rate limit

    # Final save
    with open(GEO_FILE, 'w') as f:
        json.dump(list(geo_map.values()), f, ensure_ascii=False, indent=2)
    if commerce_map:
        with open(COMMERCE_FILE, 'w') as f:
            json.dump(list(commerce_map.values()), f, ensure_ascii=False, indent=2)

    with_schools = sum(1 for g in geo_map.values() if g.get('school_score', 0) > 0)
    with_noise   = sum(1 for g in geo_map.values() if g.get('noise_penalty') is not None)
    noisy        = sum(1 for g in geo_map.values() if (g.get('noise_penalty') or 0) < 0)

    print(f'\n✅ Done')
    print(f'   🏫 With school score:  {with_schools}/{len(geo_map)}')
    print(f'   🔊 Noise assessed:     {with_noise}/{len(geo_map)}')
    print(f'   ⚠️  Noisy listings:    {noisy}/{len(geo_map)}')


if __name__ == '__main__':
    main()
