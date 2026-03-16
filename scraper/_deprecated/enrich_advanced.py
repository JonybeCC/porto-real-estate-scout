#!/usr/bin/env python3
"""
Advanced Enrichment — JBizz Assistant 🦞
Adds to geocoded.json + commerce.json:
  - Parks within 800m (Overpass)
  - Hospital/clinic within 3km (Overpass)
  - Bus stops within 400m (Overpass)
  - Supermarket brand tier (Continente/El Corte > Pingo Doce > Lidl/Aldi)
  - Street view building facade score via Mapillary + GPT-5.1
    (requires MAPILLARY_ACCESS_TOKEN env var)

Description signal extraction (updates enriched directly):
  - Furnished status
  - Equipped kitchen
  - Master suite
  - Renovation year
  - Fireplace, AC, pool, concierge
  - Natural light mentions
"""

import json, os, time, math, re, requests, base64
from datetime import datetime
from openai import OpenAI

GEO_FILE      = '/root/.openclaw/workspace/projects/real-estate/data/geocoded.json'
DETAILS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
COMMERCE_FILE = '/root/.openclaw/workspace/projects/real-estate/data/commerce.json'
LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
OVERPASS_URL  = 'https://overpass-api.de/api/interpreter'

MAPILLARY_TOKEN = os.environ.get('MAPILLARY_ACCESS_TOKEN', '')
OPENAI_CLIENT   = OpenAI(api_key=os.environ.get('OPENAI_API_KEY', ''))

# Supermarket brand tiers (1=budget, 2=standard, 3=premium)
SUPERMARKET_TIERS = {
    3: ['el corte inglés', 'corte inglés', 'waitrose', 'marks & spencer'],
    2: ['continente', 'pingo doce', 'minipreço', 'mercadona', 'auchan'],
    1: ['lidl', 'aldi', 'intermarché', 'dia', 'deshop'],
}


def haversine(lat1, lng1, lat2, lng2) -> float:
    R = 6371
    dlat, dlng = math.radians(lat2-lat1), math.radians(lng2-lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlng/2)**2
    return round(R * 2 * math.asin(math.sqrt(a)), 3)


def overpass_query(lat, lng, radius, filters) -> list:
    """Each filter is a full Overpass statement like 'way["leisure"="park"]["name"]'."""
    filter_str = '\n'.join([f'  {f}(around:{radius},{lat},{lng});' for f in filters])
    query = f'[out:json][timeout:15];\n(\n{filter_str}\n);\nout center tags;'
    try:
        r = requests.post(OVERPASS_URL, data={'data': query}, timeout=20)
        if r.status_code == 200:
            return r.json().get('elements', [])
    except (requests.RequestException, ValueError, KeyError, OSError):
        pass
    return []


def supermarket_tier(name: str) -> int:
    n = name.lower()
    for tier, brands in SUPERMARKET_TIERS.items():
        if any(b in n for b in brands):
            return tier
    return 2  # default: standard


def get_mapillary_image(lat: float, lng: float) -> str | None:
    """Get closest street-level image URL from Mapillary (free 10k/mo)."""
    if not MAPILLARY_TOKEN:
        return None
    try:
        r = requests.get('https://graph.mapillary.com/images', params={
            'access_token': MAPILLARY_TOKEN,
            'fields': 'id,thumb_1024_url,computed_geometry',
            'bbox': f'{lng-0.001},{lat-0.001},{lng+0.001},{lat+0.001}',
            'limit': 10,
        }, timeout=15)
        if r.status_code == 200:
            imgs = r.json().get('data', [])
            if imgs:
                # Pick closest image to the listing coordinates
                def dist(img):
                    coords = img.get('computed_geometry', {}).get('coordinates', [0, 0])
                    return haversine(lat, lng, coords[1], coords[0])
                closest = min(imgs, key=dist)
                return closest.get('thumb_1024_url')
    except (requests.RequestException, ValueError, KeyError, OSError):
        pass
    return None


def score_building_facade(img_url: str, address: str) -> dict:
    """
    Use GPT-5.1 vision to assess building facade quality from street view image.
    Returns: {facade_score: 1-10, facade_notes: str, building_condition: str}
    """
    try:
        r = requests.get(img_url, timeout=20,
                         headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code != 200 or len(r.content) < 5000:
            return {}
        b64 = base64.b64encode(r.content).decode()

        resp = OPENAI_CLIENT.chat.completions.create(
            model='gpt-5.1',
            messages=[{'role': 'user', 'content': [
                {'type': 'text', 'text': f'''You are assessing the exterior of a building at: {address}
This is a street-level photo. Score the building facade and street quality for a renter considering this property.

Respond EXACTLY in this format:
FACADE_SCORE: [1-10, where 1=derelict 5=average Porto residential 8=well-maintained modern 10=premium]
BUILDING_CONDITION: [Excellent/Good/Fair/Poor]
STREET_QUALITY: [Quiet residential / Busy road / Commercial street / Mixed]
FACADE_NOTES: [2 sentences describing what you see — building age, maintenance, street character]'''},
                {'type': 'image_url', 'image_url': {'url': f'data:image/jpeg;base64,{b64}', 'detail': 'low'}},
            ]}],
            max_completion_tokens=200, temperature=0
        )
        text = resp.choices[0].message.content

        def g(k):
            m = re.search(rf'{k}:\s*(.+)', text, re.IGNORECASE)
            return m.group(1).strip() if m else ''

        score_raw = g('FACADE_SCORE')
        try:
            score = int(re.search(r'\d+', score_raw).group())
        except (AttributeError, ValueError, TypeError):
            score = None

        return {
            'facade_score': score,
            'building_condition': g('BUILDING_CONDITION'),
            'street_quality': g('STREET_QUALITY'),
            'facade_notes': g('FACADE_NOTES'),
        }
    except Exception as e:
        print(f'    ⚠️  Facade scoring error: {str(e)[:60]}')
        return {}


def extract_description_signals(desc: str, tags: str = '') -> dict:
    """Extract lifestyle/quality signals from listing description text."""
    combined = (desc + ' ' + tags).lower()
    signals  = {}

    # Furnished
    signals['is_furnished'] = any(x in combined for x in [
        'mobilado', 'mobilada', 'mobiliado', 'furnished', 'equipado', 'equipada',
        'com móveis', 'com mobília',
    ])
    # Equipped kitchen
    signals['kitchen_equipped'] = any(x in combined for x in [
        'cozinha equipada', 'cozinha completa', 'electrodomésticos', 'eletrodomésticos',
        'frigorifico', 'lava-louça', 'máquina lavar louça',
    ])
    # Master suite
    signals['has_suite'] = any(x in combined for x in [
        'suite', 'suíte', 'master bedroom', 'quarto com casa de banho',
    ])
    # Fireplace
    signals['has_fireplace'] = any(x in combined for x in [
        'lareira', 'fireplace', 'salamandra', 'recuperador',
    ])
    # Air conditioning
    signals['has_ac'] = any(x in combined for x in [
        'ar condicionado', 'ar-condicionado', 'ac ', 'a/c', 'climatização',
        'split system', 'daikin', 'mitsubishi',
    ])
    # Pool
    signals['has_pool'] = any(x in combined for x in [
        'piscina', 'swimming pool', 'pool',
    ])
    # Concierge / security
    signals['has_concierge'] = any(x in combined for x in [
        'porteiro', 'concierge', 'segurança 24', 'vigilância 24',
    ])
    # Natural light explicitly mentioned
    signals['light_mentioned'] = any(x in combined for x in [
        'muito luminoso', 'bastante luminoso', 'excelente luminosidade',
        'muita luz natural', 'muito luz', 'luz natural abundante',
    ])
    # Double glazing / sound insulation
    signals['double_glazing'] = any(x in combined for x in [
        'vidros duplos', 'caixilharia pvc', 'vidros temperados', 'isolamento acústico',
        'insonorização', 'janelas antirruído',
    ])

    # Renovation year extraction
    signals['renovation_year'] = None
    for pattern in [
        r'remodelado em (\d{4})', r'renovado em (\d{4})', r'renovação (\d{4})',
        r'obras em (\d{4})', r'requalificado em (\d{4})',
        r'construído em (\d{4})', r'construcao (\d{4})',
    ]:
        m = re.search(pattern, combined)
        if m:
            year = int(m.group(1))
            if 1990 <= year <= 2026:
                signals['renovation_year'] = year
                break

    return signals


def description_score(signals: dict) -> int:
    """Convert description signals to bonus points."""
    pts = 0
    if signals.get('is_furnished'):      pts += 4
    if signals.get('kitchen_equipped'):  pts += 2
    if signals.get('has_suite'):         pts += 2
    if signals.get('has_fireplace'):     pts += 1
    if signals.get('has_ac'):            pts += 2
    if signals.get('has_pool'):          pts += 3
    if signals.get('has_concierge'):     pts += 2
    if signals.get('light_mentioned'):   pts += 1
    if signals.get('double_glazing'):    pts += 1

    # Renovation recency bonus
    yr = signals.get('renovation_year')
    if yr:
        age = 2026 - yr
        if age <= 2:    pts += 4  # renovated in last 2 years
        elif age <= 5:  pts += 3
        elif age <= 10: pts += 2
        else:           pts += 1

    return pts


def main():
    print('🔬 Advanced Enrichment')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(f'🗺️  Mapillary: {"✅ configured" if MAPILLARY_TOKEN else "❌ not set — skipping street view"}')
    print('=' * 55)

    with open(GEO_FILE) as f:        geo_list = json.load(f)
    with open(DETAILS_FILE) as f:    details  = {d['id']: d for d in json.load(f)}
    with open(LISTINGS_FILE) as f:   listings = {l['id']: l for l in json.load(f)}
    try:
        with open(COMMERCE_FILE) as f: commerce = {c['id']: c for c in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        commerce = {}

    geo_map = {g['id']: g for g in geo_list}

    to_enrich = [g for g in geo_list if g.get('lat') and g.get('parks_800m') is None]
    print(f'📦 {len(to_enrich)} listings to enrich\n')

    done = 0
    for g in to_enrich:
        lid = g['id']
        lat, lng = g['lat'], g['lng']
        l   = listings.get(lid, {})
        det = details.get(lid, {})
        com = commerce.get(lid, {})

        print(f'  [{done+1:3d}/{len(to_enrich)}] {lid}', end=' ')

        # ── 1. Parks — named public parks only (excludes private gardens) ────
        parks = overpass_query(lat, lng, 800, [
            'way["leisure"="park"]["name"]',
            'relation["leisure"="park"]["name"]',
        ])
        park_names = [e.get('tags', {}).get('name', '') for e in parks if e.get('tags', {}).get('name')]
        g['parks_800m']   = len(parks)
        g['nearest_park'] = park_names[0] if park_names else ''
        time.sleep(0.5)

        # ── 2. Hospitals/Clinics (named only) ────────────────────────────────
        hospitals = overpass_query(lat, lng, 3000, [
            'node["amenity"="hospital"]["name"]',
            'node["amenity"="clinic"]["name"]',
            'way["amenity"="hospital"]["name"]',
        ])
        hosp_names = sorted(
            [(e.get('tags', {}).get('name', ''), haversine(lat, lng,
                e.get('center', e).get('lat', lat), e.get('center', e).get('lon', lng)))
             for e in hospitals if e.get('tags', {}).get('name')],
            key=lambda x: x[1]
        )
        g['hospitals_3km']    = len(hospitals)
        g['nearest_hospital'] = f'{hosp_names[0][0]} ({hosp_names[0][1]:.1f}km)' if hosp_names else ''
        g['nearest_hospital_km'] = hosp_names[0][1] if hosp_names else None
        time.sleep(0.5)

        # ── 3. Bus stops ──────────────────────────────────────────────────────
        buses = overpass_query(lat, lng, 400, [
            'node["highway"="bus_stop"]',
            'node["public_transport"="stop_position"]',
        ])
        g['bus_stops_400m'] = len(buses)
        time.sleep(0.5)

        # ── 4. Supermarket tier ───────────────────────────────────────────────
        super_name = com.get('nearest_supermarket', '')
        g['supermarket_tier'] = supermarket_tier(super_name)

        # ── 5. Description signals ────────────────────────────────────────────
        desc = (det.get('full_description', '') or '') + ' ' + (l.get('description', '') or '')
        tags = l.get('tags', '') or ''
        signals = extract_description_signals(desc, tags)
        g.update(signals)
        g['description_bonus_pts'] = description_score(signals)

        # ── 6. Street view facade score (Mapillary + GPT-5.1) ─────────────────
        if MAPILLARY_TOKEN:
            addr = det.get('full_address', '') or l.get('title', '')
            img_url = get_mapillary_image(lat, lng)
            if img_url:
                facade = score_building_facade(img_url, addr)
                g.update(facade)
                print(f'📸 facade={facade.get("facade_score","?")}', end=' ')
            time.sleep(1)

        # Update commerce map too
        if lid in commerce:
            for field in ['parks_800m', 'nearest_park', 'hospitals_3km', 'nearest_hospital',
                          'nearest_hospital_km', 'bus_stops_400m', 'supermarket_tier',
                          'is_furnished', 'kitchen_equipped', 'has_suite', 'has_fireplace',
                          'has_ac', 'has_pool', 'has_concierge', 'double_glazing',
                          'renovation_year', 'description_bonus_pts']:
                commerce[lid][field] = g.get(field)

        done += 1
        flags = []
        if g.get('parks_800m'):        flags.append(f'🌳{g["parks_800m"]}parks')
        if g.get('hospitals_3km'):     flags.append(f'🏥{g["hospitals_3km"]}hosp')
        if g.get('bus_stops_400m'):    flags.append(f'🚌{g["bus_stops_400m"]}buses')
        if g.get('is_furnished'):      flags.append('🛋️furnished')
        if g.get('has_suite'):         flags.append('🛁suite')
        if g.get('has_ac'):            flags.append('❄️ac')
        if g.get('renovation_year'):   flags.append(f'🔨{g["renovation_year"]}')
        if g.get('description_bonus_pts', 0) > 0: flags.append(f'+{g["description_bonus_pts"]}pts')
        print(' '.join(flags) or '(basic)')

        # Checkpoint every 10
        if done % 10 == 0:
            with open(GEO_FILE, 'w') as f:
                json.dump(list(geo_map.values()), f, ensure_ascii=False, indent=2)
            with open(COMMERCE_FILE, 'w') as f:
                json.dump(list(commerce.values()), f, ensure_ascii=False, indent=2)
            print(f'  💾 Checkpoint ({done} done)')

        time.sleep(1.2)

    # Final save
    with open(GEO_FILE, 'w') as f:
        json.dump(list(geo_map.values()), f, ensure_ascii=False, indent=2)
    with open(COMMERCE_FILE, 'w') as f:
        json.dump(list(commerce.values()), f, ensure_ascii=False, indent=2)

    furnished_count = sum(1 for g in geo_map.values() if g.get('is_furnished'))
    parks_count     = sum(1 for g in geo_map.values() if g.get('parks_800m', 0) > 0)
    suite_count     = sum(1 for g in geo_map.values() if g.get('has_suite'))
    reno_count      = sum(1 for g in geo_map.values() if g.get('renovation_year'))
    ac_count        = sum(1 for g in geo_map.values() if g.get('has_ac'))
    bus_avg         = sum(g.get('bus_stops_400m', 0) for g in geo_map.values()) / max(len(geo_map), 1)

    print(f'\n✅ Done — {done} listings advanced-enriched')
    print(f'   🛋️  Furnished:       {furnished_count}')
    print(f'   🛁  With suite:      {suite_count}')
    print(f'   ❄️   With AC:         {ac_count}')
    print(f'   🔨  Renovation year: {reno_count}')
    print(f'   🌳  Near park:       {parks_count}')
    print(f'   🚌  Avg bus stops:   {bus_avg:.1f}')


if __name__ == '__main__':
    main()
