#!/usr/bin/env python3
"""
Geo Enricher — adds elevation + walk time to sea + OpenCage fallback geocoding
APIs used (all free, no key required):
  - Open Topo Data (elevation)
  - openrouteservice (walk time isochrone)
  - OpenCage (geocoding fallback, 2500/day free with key)
"""

import json, requests, time, os
from datetime import datetime

GEO_FILE      = '/root/.openclaw/workspace/projects/real-estate/data/geocoded.json'
LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'

# Porto sea reference points (Foz beach area)
SEA_LAT, SEA_LNG = 41.1490, -8.6780

ORS_BASE = 'https://api.openrouteservice.org/v2'
TOPO_BASE = 'https://api.opentopodata.org/v1/srtm30m'
ORS_KEY = os.environ.get('ORS_API_KEY', '')  # free at openrouteservice.org — 2000 req/day


def get_elevation(lat, lng):
    """Open Topo Data — free, no key"""
    try:
        r = requests.get(f'{TOPO_BASE}?locations={lat},{lng}', timeout=10)
        if r.status_code == 200:
            results = r.json().get('results', [])
            if results:
                return round(results[0].get('elevation', 0), 1)
    except (requests.RequestException, ValueError, KeyError):
        pass
    return None


def get_walk_time(lat, lng, dest_lat=SEA_LAT, dest_lng=SEA_LNG):
    """
    openrouteservice walking directions — free tier 2000 req/day.
    Returns walk time in minutes to nearest sea point.
    Falls back to straight-line estimate if no key or fails.
    """
    if ORS_KEY:
        try:
            r = requests.post(
                f'{ORS_BASE}/directions/foot-walking/json',
                headers={'Authorization': ORS_KEY, 'Content-Type': 'application/json'},
                json={'coordinates': [[lng, lat], [dest_lng, dest_lat]]},
                timeout=15
            )
            if r.status_code == 200:
                routes = r.json().get('routes', [])
                if routes:
                    secs = routes[0]['summary']['duration']
                    return round(secs / 60, 1)
        except (requests.RequestException, ValueError, KeyError):
            pass

    # Fallback: straight-line distance with Porto terrain factor
    # Porto is hilly — multiply flat distance by 1.4 avg terrain factor
    # Then assume 5 km/h walking pace
    import math
    dlat = abs(lat - dest_lat)
    dlng = abs(lng - dest_lng)
    dist_km = math.sqrt(dlat**2 + dlng**2) * 111  # rough km
    terrain_factor = 1.4  # Porto hills adjustment
    walk_min = (dist_km * terrain_factor / 5.0) * 60
    return round(walk_min, 1)


def opencage_geocode(address, api_key):
    """OpenCage fallback — better Portugal coverage than Nominatim"""
    try:
        r = requests.get(
            'https://api.opencagedata.com/geocode/v1/json',
            params={'q': address, 'key': api_key, 'language': 'pt', 'countrycode': 'pt', 'limit': 1},
            timeout=10
        )
        if r.status_code == 200:
            results = r.json().get('results', [])
            if results:
                geom = results[0]['geometry']
                return geom['lat'], geom['lng'], 'opencage'
    except (requests.RequestException, ValueError, KeyError):
        pass
    return None, None, None


def main():
    print('🌍 Geo Enricher — elevation + walk times')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(f'🔑 ORS key: {"✅ set" if ORS_KEY else "❌ not set — using straight-line fallback"}')
    print('=' * 55)

    with open(GEO_FILE) as f:
        geo_list = json.load(f)

    geo_map = {g['id']: g for g in geo_list}

    to_enrich = [g for g in geo_list if g.get('lat') and not g.get('elevation_m') and not g.get('walk_time_sea_min')]
    print(f'📦 {len(to_enrich)} listings to enrich with elevation + walk time\n')

    done = 0
    for g in to_enrich:
        lid = g['id']
        lat, lng = g['lat'], g['lng']

        # Elevation
        elev = get_elevation(lat, lng)
        g['elevation_m'] = elev
        time.sleep(0.3)  # Open Topo Data rate limit

        # Walk time to sea
        walk_min = get_walk_time(lat, lng)
        g['walk_time_sea_min'] = walk_min

        done += 1
        elev_str = f'{elev}m' if elev is not None else '?m'
        print(f'  [{done:3d}/{len(to_enrich)}] {lid} → elev={elev_str} walk={walk_min}min')

        # Checkpoint every 20
        if done % 20 == 0:
            with open(GEO_FILE, 'w') as f:
                json.dump(list(geo_map.values()), f, ensure_ascii=False, indent=2)
            print(f'  💾 Checkpoint saved')

        time.sleep(0.5)

    with open(GEO_FILE, 'w') as f:
        json.dump(list(geo_map.values()), f, ensure_ascii=False, indent=2)

    has_elev = sum(1 for g in geo_map.values() if g.get('elevation_m') is not None)
    has_walk = sum(1 for g in geo_map.values() if g.get('walk_time_sea_min'))
    print(f'\n✅ Done')
    print(f'   Elevation data: {has_elev}/{len(geo_map)}')
    print(f'   Walk times:     {has_walk}/{len(geo_map)}')


if __name__ == '__main__':
    main()
