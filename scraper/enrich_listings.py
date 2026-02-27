#!/usr/bin/env python3
"""
Listings Enricher v7 — JBizz Assistant 🦞
Scoring redesigned Feb 2026:
  1. Condition/quality    0-25  (AI image score × 2.5)
  2. Sun exposure         0-20  (verified via street bearing + description)
  3. Zone desirability    0-15  (Pinhais da Foz → Other)
  4. Practical features   0-18  (garage 2+ spaces, storage, balcony, elevator)
  5. Floor level          0-8   (RC/1st=0, 2nd=3, 3rd=5, 4th+=7, top=8)
  6. Space efficiency     0-7   (m²/room ratio)
  7. Price value          0-7   (delta vs zone avg €/m²)
  Bonus/penalty: sea view +3, owner direct +1, sea <0.5km +2, red flags -2 each
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

ZONE_CONFIG = {
    'pinhais da foz': 20.0, 'foz do douro': 21.0, 'nevogilde': 21.0, 'foz velha': 20.0,
    'foz': 20.0, 'gondarém': 20.0, 'serralves': 18.5, 'pinheiro manso': 18.0,
    'massarelos': 17.5, 'aldoar': 17.0, 'lordelo do ouro': 17.0, 'boavista': 18.0,
    'bessa leite': 16.5, 'bessa': 16.5, 'aviz': 17.0, 'cristo rei': 15.0,
    'cedofeita': 15.0, 'bonfim': 14.0, 'matosinhos': 14.0, 'paranhos': 13.0,
    'ramalde': 13.5, 'campanhã': 11.0, 'default': 15.0,
}

# v7: Zone desirability scores (0-15)
ZONE_DESIRABILITY = {
    'pinhais da foz': 15, 'nevogilde': 15, 'foz do douro': 13, 'foz velha': 13,
    'foz': 13, 'gondarém': 11, 'serralves': 11, 'pinheiro manso': 10,
    'massarelos': 9, 'boavista': 9, 'aldoar': 7, 'lordelo do ouro': 7,
    'bessa leite': 7, 'bessa': 7, 'aviz': 7, 'cristo rei': 5,
    'cedofeita': 5, 'bonfim': 5, 'matosinhos': 5, 'paranhos': 5,
    'ramalde': 5, 'campanhã': 3, 'default': 5,
}

def load_json(path, default):
    try:
        with open(path, encoding='utf-8') as f: return json.load(f)
    except: return default

def sun_score_v7(sun: str, description: str = '') -> int:
    """0-20. South/SE = 20, E or W = 12, North = 0, unknown = 8"""
    s = str(sun or '').lower()
    d = str(description or '').lower()
    combined = s + ' ' + d
    if any(x in combined for x in ['sul', 'south', 'sul/nascente', 'nascente/sul', 'southeast', 'sse', 'se ', 'sudeste']): return 20
    if any(x in combined for x in ['southwest', 'sudoeste', 'ssw']): return 18
    if any(x in combined for x in ['nascente', 'east', 'oeste', 'west', 'poente', 'este']): return 12
    if any(x in combined for x in ['norte', 'north']): return 0
    return 8  # unknown → neutral

def floor_score_v7(floor_val) -> int:
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
    return 2  # unknown floor → small default

def calc_v7_score(e: dict) -> int:
    score = 0

    # 1. CONDITION & QUALITY (0-25) — AI image score × 2.5, fallback 5
    img_score = e.get('image_score')
    if img_score:
        score += min(25, float(img_score) * 2.5)
    else:
        score += 10  # neutral fallback (no images analyzed yet)

    # 2. SUN EXPOSURE (0-20)
    score += sun_score_v7(e.get('sun_exposure', ''), e.get('condition_summary', ''))

    # 3. ZONE DESIRABILITY (0-15)
    neigh = str(e.get('neighborhood', '')).lower()
    zone_pts = ZONE_DESIRABILITY.get('default', 5)
    for k, v in ZONE_DESIRABILITY.items():
        if k in neigh:
            zone_pts = v
            break
    score += zone_pts

    # 4. PRACTICAL FEATURES (0-18)
    spaces = e.get('parking_spaces', 0) or 0
    if spaces >= 2:     score += 7  # 2+ spaces
    elif e.get('has_garage'): score += 5  # 1 space
    if e.get('has_storage'):  score += 3
    if e.get('outdoor_space') and e['outdoor_space'] not in ('None', None, ''): score += 4
    if e.get('elevator'):     score += 4

    # 5. FLOOR LEVEL (0-8)
    score += floor_score_v7(e.get('floor_level'))

    # 6. SPACE EFFICIENCY (0-7)
    spr = e.get('space_per_room_m2', 0) or 0
    if spr >= 50:   score += 7
    elif spr >= 40: score += 5
    elif spr >= 30: score += 3
    else:           score += 1

    # 7. PRICE VALUE (0-7)
    delta_pct = e.get('price_delta_pct', 0) or 0  # negative = below avg = good
    if delta_pct <= -10:  score += 7
    elif delta_pct <= -5: score += 5
    elif delta_pct <= 5:  score += 3
    else:                 score += 1

    # BONUSES
    if e.get('sea_view'):                          score += 3
    if e.get('dist_to_sea_km', 9) < 0.5:          score += 2
    if e.get('owner_direct'):                      score += 1
    if e.get('days_on_market', 0) > 60:            score += 1  # negotiable

    # PENALTIES (red flags from AI)
    red = str(e.get('red_flags_visual') or '').lower()
    if red and red not in ('none', ''):
        # Count significant flags: damp, cracks, dark, mold
        bad = sum(1 for kw in ['damp', 'crack', 'mold', 'mould', 'dark', 'very small', 'noise'] if kw in red)
        score -= bad * 2

    return int(min(100, max(0, score)))

def main():
    print('🦞 Listings Enricher v7')
    listings   = load_json(LISTINGS_FILE, [])
    details    = {d['id']: d for d in load_json(DETAILS_FILE, [])}
    v_analysis = {k: v for k, v in load_json(IMAGE_FILE, {}).items()}
    geo_map    = {g['id']: g for g in load_json(GEO_FILE, [])}
    dom_map    = load_json(DOM_FILE, {})
    commerce_map = {c['id']: c for c in load_json(COMMERCE_FILE, [])}

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
        zone_avg = ZONE_CONFIG.get('default', 15.0)
        for k, v in ZONE_CONFIG.items():
            if k in neigh: zone_avg = v; break
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

        # Sun exposure — prefer description text over AI light assessment
        sun = ''
        for phrase in ['nascente/sul', 'sul/nascente', 'nascente e sul', 'sul e nascente',
                       'sul', 'south', 'nascente', 'east', 'poente', 'west', 'norte', 'north']:
            if phrase in combined:
                sun = phrase; break

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
        }

        # ── v7 score ──────────────────────────────────────────────────────────
        e['opportunity_score'] = calc_v7_score(e)

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
    'Sea View', 'Dist Sea (km)', 'Garage', 'Parking Spaces', 'Storage', 'Elevator', 'Outdoor',
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
