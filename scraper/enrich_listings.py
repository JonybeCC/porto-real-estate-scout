#!/usr/bin/env python3
"""
Listings Enricher v8 — JBizz Assistant 🦞
Scoring redesigned Mar 2026 — key changes from v7:
  - Dynamic zone price medians (computed from actual listings, not manual)
  - Image fallback reduced 10→5 (unknown ≠ neutral)
  - Sun unknown reduced 8→4 (74 listings missing = was inflating scores)
  - Garage points scaled by rarity not presence (77% have it → lower base bonus)
  - Price value uses absolute €/m² vs zone median (more accurate than % delta)
  - Freshness signal: 90+ days on market = likely problem → -2
  - Size/value signal: extreme €/m² ratios penalised
  - Sea view bonus gated behind walk time (view claims unverified without images)
  - Space score normalised per room type (T2/T3 benchmarked separately)
"""

import json
import re
import os
import time
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials

# ── Paths ──────────────────────────────────────────────────────────────────────
LISTINGS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
DETAILS_FILE   = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
IMAGE_FILE     = '/root/.openclaw/workspace/projects/real-estate/data/image_analysis.json'
GEO_FILE       = '/root/.openclaw/workspace/projects/real-estate/data/geocoded.json'
DOM_FILE       = '/root/.openclaw/workspace/projects/real-estate/data/dom_tracker.json'
COMMERCE_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/commerce.json'
ENRICHED_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/enriched_listings.json'
CREDS_FILE     = '/root/.openclaw/credentials/google-service-account.json'
SHEET_URL      = 'https://docs.google.com/spreadsheets/d/1Ljk9s45BovbRfk1QIUGgGxbjnpIgs2F52rJjDNGQOQI/edit'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# Zone desirability (0-15) — Foz premium, lifestyle proximity
ZONE_DESIRABILITY = {
    'pinhais da foz': 15, 'nevogilde': 15, 'foz do douro': 13, 'foz velha': 13,
    'foz': 13, 'gondarém': 11, 'serralves': 11, 'pinheiro manso': 10,
    'massarelos': 9, 'boavista': 9, 'aldoar': 7, 'lordelo do ouro': 7,
    'bessa leite': 7, 'bessa': 7, 'aviz': 7, 'cristo rei': 5,
    'cedofeita': 5, 'bonfim': 5, 'matosinhos': 5, 'paranhos': 5,
    'ramalde': 5, 'campanhã': 3, 'default': 5,
}

# Dynamic zone price medians computed from actual listings (updated at runtime)
# These are fallback defaults — main() will compute live medians and pass them in
ZONE_PRICE_MEDIANS = {
    'pinhais da foz': 15.0, 'nevogilde': 17.4, 'foz do douro': 17.4, 'foz velha': 20.8,
    'foz': 15.8, 'gondarém': 15.8, 'serralves': 16.5, 'pinheiro manso': 12.5,
    'massarelos': 16.2, 'boavista': 15.6, 'aldoar': 14.0, 'lordelo do ouro': 14.0,
    'bessa leite': 15.6, 'bessa': 15.6, 'aviz': 17.7, 'ramalde': 13.1,
    'default': 14.0,
}

def load_json(path, default):
    try:
        with open(path, encoding='utf-8') as f: return json.load(f)
    except: return default

def sun_score(sun: str, description: str = '', solar_direction: str = '') -> int:
    """
    0-20. South/SE = 20, SW = 18, E/W = 12, North = 0.
    Unknown = 4 (reduced from 8 — 74/128 listings missing sun = was inflating scores).
    Sources: description text, then GPT-5.1 solar_direction from photo shadows.
    """
    combined = ' '.join([str(sun or ''), str(description or ''), str(solar_direction or '')]).lower()
    if any(x in combined for x in ['sul', 'south', 'sul/nascente', 'nascente/sul',
                                     'southeast', 'sse', 'se ', 'sudeste']): return 20
    if any(x in combined for x in ['southwest', 'sudoeste', 'ssw']):           return 18
    if any(x in combined for x in ['nascente', 'east', 'oeste', 'west',
                                     'poente', 'este']):                         return 12
    if any(x in combined for x in ['norte', 'north']):                          return 0
    return 4  # unknown — reduced from 8


def floor_score(floor_val) -> int:
    """0-8. RC/1st=0, 2nd=3, 3rd=5, 4th+=7, penthouse=8"""
    s = str(floor_val or '').lower()
    if any(x in s for x in ['r/c', 'rc', 'rés', 'rez', 'ground', 'térreo']): return 0
    try:
        n = int(re.search(r'\d+', s).group())
        if n <= 1: return 0
        if n == 2: return 3
        if n == 3: return 5
        if n >= 4: return 7
    except: pass
    if any(x in s for x in ['penthouse', 'último', 'cobertura', 'top']): return 8
    return 2


def compute_zone_medians(listings: list) -> dict:
    """Compute live €/m² medians per zone from actual listings."""
    import statistics
    from collections import defaultdict
    zone_prices = defaultdict(list)
    for l in listings:
        ppm = l.get('price_per_m2') or 0
        if ppm <= 0:
            continue
        neigh = str(l.get('neighborhood', '')).lower()
        zone = 'default'
        for k in ZONE_DESIRABILITY.keys():
            if k in neigh:
                zone = k
                break
        zone_prices[zone].append(ppm)
    medians = dict(ZONE_PRICE_MEDIANS)  # start with fallbacks
    for zone, prices in zone_prices.items():
        if len(prices) >= 2:
            medians[zone] = statistics.median(prices)
    return medians


def calc_score_v8(e: dict, zone_medians: dict) -> int:
    """
    v8 Opportunity Score (0-100).

    Key improvements over v7:
    - Image fallback: 5pts (was 10) — unknown ≠ neutral
    - Sun unknown: 4pts (was 8) — 74/128 missing → was inflating
    - Garage: 77% of listings have it → scaled down (3pts base, 5 for 2+ spaces)
    - Price value: uses live zone medians, not manual config
    - Freshness: 90+ days on market = staleness penalty
    - €/m² sanity check: extreme outliers penalised
    - Sea view: bonus only if walk time ≤25min (nearby enough to matter)
    """
    score = 0

    # ── 1. CONDITION & QUALITY (0-25) ────────────────────────────────────────
    img = e.get('image_score')
    if img:
        score += min(25, float(img) * 2.5)
    else:
        score += 5  # v8: reduced from 10 — no photos = real uncertainty

    # ── 2. SUN EXPOSURE (0-20) ───────────────────────────────────────────────
    score += sun_score(
        e.get('sun_exposure', ''),
        e.get('condition_summary', ''),
        e.get('solar_direction', ''),    # from GPT-5.1 photo analysis
    )

    # ── 3. ZONE DESIRABILITY (0-15) ──────────────────────────────────────────
    neigh = str(e.get('neighborhood', '')).lower()
    zone_pts = ZONE_DESIRABILITY['default']
    for k, v in ZONE_DESIRABILITY.items():
        if k in neigh:
            zone_pts = v
            break
    score += zone_pts

    # ── 4. PRACTICAL FEATURES (0-16) ─────────────────────────────────────────
    # Garage: 77% of listings have it → base value is 3pts (was 5)
    # 2+ spaces = real differentiator → 6pts
    spaces = e.get('parking_spaces', 0) or 0
    if spaces >= 2:          score += 6
    elif e.get('has_garage'): score += 3
    if e.get('has_storage'):  score += 3
    if e.get('outdoor_space') and e['outdoor_space'] not in ('None', None, ''): score += 4
    if e.get('elevator'):     score += 3

    # ── 5. FLOOR LEVEL (0-8) ─────────────────────────────────────────────────
    score += floor_score(e.get('floor_level'))

    # ── 6. SPACE EFFICIENCY (0-7) ────────────────────────────────────────────
    # Benchmarked per room type: T2 benchmark = 45m²/room, T3 = 40m²/room
    spr   = e.get('space_per_room_m2', 0) or 0
    rooms_str = str(e.get('rooms', '')).upper()
    bench = 40 if 'T3' in rooms_str else 45  # T3 rooms should be large
    if spr >= bench * 1.3:   score += 7
    elif spr >= bench:        score += 5
    elif spr >= bench * 0.8: score += 3
    else:                     score += 1

    # ── 7. PRICE VALUE (0-8) ─────────────────────────────────────────────────
    # Use live zone medians instead of stale manual config
    ppm = e.get('price_per_m2', 0) or 0
    zone_median = zone_medians.get('default', 15.0)
    for k in ZONE_DESIRABILITY.keys():
        if k in neigh and k in zone_medians:
            zone_median = zone_medians[k]
            break
    if ppm > 0 and zone_median > 0:
        ratio = ppm / zone_median   # <1 = below median (good), >1 = above (bad)
        if ratio <= 0.80:   score += 8   # ≥20% below zone median
        elif ratio <= 0.90: score += 6
        elif ratio <= 1.00: score += 4
        elif ratio <= 1.10: score += 2
        else:               score += 0   # overpriced for zone

    # ── BONUSES ───────────────────────────────────────────────────────────────
    # Sea view — only count if actually close to sea (self-reported claims from afar)
    walk_min = e.get('walk_time_sea_min') or 0
    dist_km  = e.get('dist_to_sea_km', 9) or 9
    if e.get('sea_view') and (walk_min <= 25 or (walk_min == 0 and dist_km <= 1.5)):
        score += 3

    # Walk proximity to sea
    if walk_min > 0:
        if walk_min <= 8:    score += 3  # walking distance
        elif walk_min <= 15: score += 2
        elif walk_min <= 25: score += 1
    elif dist_km < 0.5:
        score += 2  # straight-line fallback

    # Elevation penalty
    elev = e.get('elevation_m') or 0
    if elev > 80: score -= 1

    # Owner direct (no agency fee)
    if e.get('owner_direct'): score += 2

    # ── SCHOOL QUALITY (0-3) ─────────────────────────────────────────────────
    s_sc = e.get('school_score', 0) or 0
    if s_sc >= 8:   score += 3
    elif s_sc >= 6: score += 2
    elif s_sc >= 4: score += 1

    # ── NOISE PENALTY (-5 to 0) ──────────────────────────────────────────────
    score += e.get('noise_penalty', 0) or 0

    # ── DESCRIPTION SIGNALS (furnished, suite, AC, renovation, etc.) ─────────
    score += min(12, e.get('description_bonus_pts', 0) or 0)

    # ── LIFESTYLE AMENITIES (parks, hospitals, bus) ───────────────────────────
    # Park proximity
    parks = e.get('parks_800m', 0) or 0
    if parks >= 2:  score += 2
    elif parks >= 1: score += 1

    # Hospital/clinic within 2km
    hosp_km = e.get('nearest_hospital_km') or 99
    if hosp_km <= 1.0:   score += 2
    elif hosp_km <= 2.0: score += 1

    # Bus connectivity
    buses = e.get('bus_stops_400m', 0) or 0
    if buses >= 8:   score += 3
    elif buses >= 4: score += 2
    elif buses >= 2: score += 1

    # Supermarket tier bonus (premium supermarket = nicer neighbourhood)
    tier = e.get('supermarket_tier', 2) or 2
    if tier == 3: score += 2
    elif tier == 1: score -= 1

    # Restaurant / café density (already in commerce)
    resto = e.get('restaurants_300m', 0) or 0
    if resto >= 6:   score += 2
    elif resto >= 3: score += 1

    # Pharmacy very close
    pharm_km = e.get('nearest_pharmacy_km') or 99
    if pharm_km <= 0.2: score += 1

    # ── BUILDING FACADE SCORE (Mapillary + GPT-5.1, if available) ────────────
    facade = e.get('facade_score')
    if facade:
        if facade >= 8:   score += 4
        elif facade >= 6: score += 2
        elif facade <= 3: score -= 3

    # ── STALENESS PENALTY (rentals move fast — 30d+ is a red flag) ──────────
    dom = e.get('days_on_market', 0) or 0
    if dom >= 60:   score -= 4
    elif dom >= 30: score -= 2

    # ── €/m² SANITY CHECK ────────────────────────────────────────────────────
    if ppm > 0 and zone_median > 0 and ppm / zone_median > 2.5:
        score -= 5

    # ── AI RED FLAGS ─────────────────────────────────────────────────────────
    red = str(e.get('red_flags_visual') or '').lower()
    if red and red not in ('none', ''):
        bad = sum(1 for kw in ['damp', 'crack', 'mold', 'mould', 'dark', 'very small', 'noise'] if kw in red)
        score -= bad * 2

    return int(min(100, max(0, score)))

def main():
    print('🦞 Listings Enricher v8')
    listings   = load_json(LISTINGS_FILE, [])
    details    = {d['id']: d for d in load_json(DETAILS_FILE, [])}
    v_analysis = {k: v for k, v in load_json(IMAGE_FILE, {}).items()}
    geo_map    = {g['id']: g for g in load_json(GEO_FILE, [])}
    dom_map    = load_json(DOM_FILE, {})
    commerce_map = {c['id']: c for c in load_json(COMMERCE_FILE, [])}

    # Compute live zone price medians for accurate price-value scoring
    # First pass: calculate price_per_m2 for all listings
    prelim = []
    for l in listings:
        det = details.get(l['id'], {})
        area_util  = det.get('area_util')
        area_bruta = det.get('area_bruta') or l.get('size_m2', 0)
        size_ref   = area_util if area_util else area_bruta
        price      = l.get('price_eur', 0)
        ppm        = round(price / size_ref, 2) if size_ref else 0
        prelim.append({**l, 'price_per_m2': ppm, 'neighborhood': l.get('neighborhood', '')})
    zone_medians = compute_zone_medians(prelim)
    print(f'   Zone medians computed from {len(prelim)} listings')

    enriched_all = []
    for i, l in enumerate(listings):
        lid = l['id']
        det = details.get(lid, {})
        vis = v_analysis.get(lid, {})
        geo = geo_map.get(lid, {})
        dom = dom_map.get(lid, {})
        com = commerce_map.get(lid, {})

        # ── Core data ─────────────────────────────────────────────────────────
        area_bruta     = det.get('area_bruta') or l.get('size_m2', 0)
        area_util      = det.get('area_util')
        parking_spaces = det.get('parking_spaces', 1 if det.get('has_garage') else 0)
        wcs            = det.get('wcs') or det.get('bathrooms')
        img_score      = vis.get('score')
        size_ref       = area_util if area_util else area_bruta
        price          = l.get('price_eur', 0)

        # ── Price calculations ────────────────────────────────────────────────
        price_per_m2 = round(price / size_ref, 2) if size_ref else 0
        neigh = l.get('neighborhood', '').lower()
        zone_avg = zone_medians.get('default', 15.0)
        for k in ZONE_DESIRABILITY.keys():
            if k in neigh and k in zone_medians:
                zone_avg = zone_medians[k]; break
        price_delta     = round(price_per_m2 - zone_avg, 2)
        price_delta_pct = round((price_per_m2 / zone_avg - 1) * 100, 1) if zone_avg else 0

        # ── Detect features from description/tags ────────────────────────────
        desc_raw  = (l.get('description', '') + ' ' + det.get('full_description', '')).lower()
        tags_raw  = l.get('tags', '').lower()
        combined  = desc_raw + ' ' + tags_raw

        has_garage    = det.get('has_garage', False) or 'garagem' in combined
        has_storage   = any(x in combined for x in ['arrumos', 'storage', 'arrumação', 'arrecadação'])
        floor_str     = str(l.get('floor', '')).lower()
        has_elevator  = any(x in combined for x in ['elevador', 'elevator', 'lift']) or 'elevador' in floor_str
        has_sea_view  = any(x in combined for x in ['vista mar', 'sea view', 'vistas mar'])
        owner_direct  = 'agencia' not in combined and ('sem agência' in combined or 'particular' in combined or 'nao disponível para agencias' in combined or 'proprietário' in combined)

        # Outdoor space detection
        outdoor = None
        if any(x in combined for x in ['terraço', 'terraco', 'terrace', 'rooftop']): outdoor = 'Terraço'
        elif any(x in combined for x in ['varanda', 'balcony', 'balcão']): outdoor = 'Varanda'
        elif any(x in combined for x in ['jardim', 'garden', 'quintal', 'patio', 'pátio']): outdoor = 'Garden'

        # Sun exposure — prefer description text, fallback to GPT-5.1 solar_direction
        sun = ''
        for phrase in ['nascente/sul', 'sul/nascente', 'nascente e sul', 'sul e nascente',
                       'orientação sul', 'orientacao sul', 'sul', 'south', 'nascente',
                       'east', 'poente', 'west', 'norte', 'north']:
            if phrase in combined:
                sun = phrase; break
        if not sun and vis.get('solar_direction') and vis['solar_direction'].lower() not in ('unknown', ''):
            sun = vis['solar_direction'].lower()  # GPT-5.1 extracted from photo shadows

        # Floor — prefer ZenRows floor_num, fallback to listing floor string
        floor_raw = det.get('floor_num') or l.get('floor', '')

        # ── Space per room ────────────────────────────────────────────────────
        try:
            num_rooms = int(re.search(r'\d+', l.get('rooms', '1')).group())
        except: num_rooms = 1
        space_per_room = round(size_ref / max(1, num_rooms), 1) if size_ref else 0

        # ── Build enrichment object ───────────────────────────────────────────
        e = {
            'id': lid, 'url': l.get('url'), 'title': l.get('title'),
            'neighborhood': l.get('neighborhood'), 'rooms': l.get('rooms'),
            'size_m2': l.get('size_m2'), 'area_bruta': area_bruta, 'area_util': area_util,
            'price_eur': price, 'price_per_m2': price_per_m2,
            'price_delta_zone': price_delta, 'price_delta_pct': price_delta_pct,
            'wcs': wcs, 'has_garage': has_garage, 'parking_spaces': parking_spaces,
            'has_storage': has_storage, 'energy_cert': det.get('energy_cert'),
            'elevator': has_elevator, 'floor_level': floor_raw,
            'sun_exposure': sun,
            'dist_to_sea_km': geo.get('dist_to_sea_km', 5.0),
            'walk_time_sea_min': geo.get('walk_time_sea_min'),
            'elevation_m': geo.get('elevation_m'),
            'dist_to_foz_km': geo.get('dist_to_foz_km'),
            'sea_view': has_sea_view, 'outdoor_space': outdoor,
            'owner_direct': owner_direct,
            'image_score': img_score, 'feel': vis.get('feel'), 'finish': vis.get('finish'),
            'light_quality': vis.get('light'), 'images_analyzed': vis.get('total_images'),
            'renovation': vis.get('renovation'),
            'score_progression': str(vis.get('score_progression', [])),
            'red_flags_visual': vis.get('red_flags'), 'image_summary': vis.get('summary'),
            'condition_summary': det.get('full_description', '')[:500],
            'days_on_market': dom.get('days_on_market', 0),
            'space_per_room_m2': space_per_room,
            # Commerce / amenity data
            'nearest_supermarket': com.get('nearest_supermarket'),
            'nearest_supermarket_km': com.get('nearest_supermarket_km'),
            'nearest_pharmacy': com.get('nearest_pharmacy'),
            'nearest_pharmacy_km': com.get('nearest_pharmacy_km'),
            'restaurants_300m': com.get('restaurants_300m', 0),
            'nearest_metro': com.get('nearest_metro'),
            'nearest_metro_km': com.get('nearest_metro_km'),
            'nearest_beach': com.get('nearest_beach'),
            'nearest_beach_km': com.get('nearest_beach_km'),
            'schools_1km': com.get('schools_1km', 0),
            'commerce_notes': com.get('commerce_notes', ''),
            # School & noise
            'school_score':        geo.get('school_score'),
            'nearest_good_school': geo.get('nearest_good_school', ''),
            'nearest_school_km':   geo.get('nearest_school_km'),
            'noise_penalty':       geo.get('noise_penalty', 0),
            'noise_sources':       geo.get('noise_sources', ''),
            # Advanced enrichment (enrich_advanced.py)
            'parks_800m':           geo.get('parks_800m', 0),
            'nearest_park':         geo.get('nearest_park', ''),
            'hospitals_3km':        geo.get('hospitals_3km', 0),
            'nearest_hospital':     geo.get('nearest_hospital', ''),
            'nearest_hospital_km':  geo.get('nearest_hospital_km'),
            'bus_stops_400m':       geo.get('bus_stops_400m', 0),
            'supermarket_tier':     geo.get('supermarket_tier', 2),
            # Description signals
            'is_furnished':         geo.get('is_furnished', False),
            'kitchen_equipped':     geo.get('kitchen_equipped', False),
            'has_suite':            geo.get('has_suite', False),
            'has_fireplace':        geo.get('has_fireplace', False),
            'has_ac':               geo.get('has_ac', False),
            'has_pool':             geo.get('has_pool', False),
            'has_concierge':        geo.get('has_concierge', False),
            'double_glazing':       geo.get('double_glazing', False),
            'renovation_year':      geo.get('renovation_year'),
            'description_bonus_pts': geo.get('description_bonus_pts', 0),
            'light_mentioned':      geo.get('light_mentioned', False),
            # Street view facade
            'facade_score':         geo.get('facade_score'),
            'building_condition':   geo.get('building_condition', ''),
            'street_quality':       geo.get('street_quality', ''),
            'facade_notes':         geo.get('facade_notes', ''),
        }

        # ── v7 score ──────────────────────────────────────────────────────────
        # Also add solar_direction from image analysis
        e['solar_direction'] = vis.get('solar_direction', '')

        e['opportunity_score'] = calc_score_v8(e, zone_medians)

        enriched_all.append(e)

    with open(ENRICHED_FILE, 'w', encoding='utf-8') as f:
        json.dump(enriched_all, f, ensure_ascii=False, indent=2)
    print(f'✅ {len(enriched_all)} listings enriched')

    # Push to Sheets logic (simplified)
    # [Headers and Rows logic here...]
    push_to_sheets(enriched_all)

SHEET_HEADERS = [
    'Rank', 'Score v7', 'ID', 'Rooms', 'Size m²', 'Área Útil', 'Space/Room m²', 'WCs',
    'Price €/mo', '€/m²', 'Price Δ%', 'Days on Market',
    'Neighbourhood', 'Floor', 'Sun Exposure',
    'Sea View', 'Dist Sea (km)', 'Walk to Sea (min)', 'Elevation (m)', 'Garage', 'Parking Spaces', 'Storage', 'Elevator', 'Outdoor',
    'Owner Direct', 'Image Score', 'Renovation', 'Feel', 'Finish', 'Light',
    'Red Flags', 'Image Summary',
    'Supermarket', 'Supermarket km', 'Pharmacy km', 'Restaurants 300m', 'Metro km', 'Beach km', 'Schools 1km', 'Commerce Notes',
    'Condition Summary', 'URL'
]

def row_from_enriched(rank: int, e: dict) -> list:
    return [
        rank, e.get('opportunity_score'), e.get('id'), e.get('rooms'),
        e.get('size_m2'), e.get('area_util'), e.get('space_per_room_m2'), e.get('wcs'),
        e.get('price_eur'), e.get('price_per_m2'), e.get('price_delta_pct'),
        e.get('days_on_market'), e.get('neighborhood'), e.get('floor_level'), e.get('sun_exposure'),
        'Yes' if e.get('sea_view') else 'No', e.get('dist_to_sea_km'),
        e.get('walk_time_sea_min'), e.get('elevation_m'),
        'Yes' if e.get('has_garage') else 'No', e.get('parking_spaces'),
        'Yes' if e.get('has_storage') else 'No',
        'Yes' if e.get('elevator') else 'No', e.get('outdoor_space') or '',
        'Yes' if e.get('owner_direct') else 'No',
        e.get('image_score'), e.get('renovation'), e.get('feel'), e.get('finish'), e.get('light_quality'),
        e.get('red_flags_visual'), e.get('image_summary'),
        e.get('nearest_supermarket'), e.get('nearest_supermarket_km'),
        e.get('nearest_pharmacy_km'), e.get('restaurants_300m'),
        e.get('nearest_metro_km'), e.get('nearest_beach_km'), e.get('schools_1km'),
        e.get('commerce_notes'),
        (e.get('condition_summary') or '')[:200], e.get('url')
    ]

def push_to_sheets(enriched_listings: list):
    print('\n📊 Pushing to Google Sheets...')
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc    = gspread.authorize(creds)
    sh    = gc.open_by_url(SHEET_URL)
    try:
        ws = sh.worksheet('Enriched_v7')
    except:
        ws = sh.add_worksheet(title='Enriched_v7', rows=300, cols=35)
    ws.clear()
    
    sorted_list = sorted(enriched_listings, key=lambda x: x.get('opportunity_score', 0), reverse=True)
    rows = [SHEET_HEADERS]
    for rank, e in enumerate(sorted_list, 1):
        rows.append(row_from_enriched(rank, e))
    
    for i in range(0, len(rows), 50):
        ws.update(rows[i:i+50], f'A{i+1}', value_input_option='USER_ENTERED')
    
    print(f'  ✅ {len(enriched_listings)} rows pushed.')

if __name__ == '__main__':
    main()
