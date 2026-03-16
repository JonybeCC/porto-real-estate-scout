#!/usr/bin/env python3
"""
Listings Enricher v9 — JBizz Assistant 🦞

Responsibilities (narrowed from v8 monolith):
  - Load and join data from all source files
  - Extract features from listings, details, geo, images, commerce
  - Delegate scoring to scoring.py (calc_score)
  - Delegate Sheets push to sheets.py (push_to_sheets)

v9 changes vs v8:
  - Scoring extracted to scoring.py (single source of truth for weights)
  - Sheets push extracted to sheets.py
  - raw_score stored alongside opportunity_score (normalised 0-100)
  - score_breakdown stored per listing for Sheet columns
  - area_util now sourced from ZenRows detail (was always None before antibot fix)
  - full_description from ZenRows detail used for sun/feature parsing
  - photo_urls from ZenRows detail used for image analysis (full-res 1500px)
"""

import json, re, os, sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from paths import PATHS
from scoring import calc_score, compute_zone_medians


def load_json(path, default):
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def extract_features(l: dict, det: dict, vis: dict, geo: dict,
                     dom: dict, com: dict, zone_medians: dict) -> dict:
    """
    Join all data sources into a flat enriched dict for one listing.
    Returns the dict ready for scoring and Sheet push.
    """
    lid = l['id']

    # ── Core size data ─────────────────────────────────────────────────────────
    # area_util now populated from ZenRows detail (v4 with antibot bypass)
    area_bruta  = det.get('area_bruta') or l.get('size_m2', 0)
    area_util   = det.get('area_util')  # None if ZenRows hasn't fetched yet
    size_ref    = area_util if area_util else area_bruta
    price       = l.get('price_eur', 0)

    # ── Price calculations ─────────────────────────────────────────────────────
    price_per_m2 = round(price / size_ref, 2) if size_ref else 0
    neigh        = l.get('neighborhood', '').lower()
    zone_median  = zone_medians.get('default', 15.0)
    for k in zone_medians:
        if k != 'default' and k in neigh:
            zone_median = zone_medians[k]; break
    price_delta     = round(price_per_m2 - zone_median, 2)
    price_delta_pct = round((price_per_m2 / zone_median - 1) * 100, 1) if zone_median else 0

    # ── Combined description text (listing + ZenRows detail) ─────────────────
    # ZenRows full_description is the authoritative text — much richer than snippet
    detail_desc = det.get('full_description', '')
    listing_desc = l.get('description', '')
    tags_raw    = l.get('tags', '').lower()
    combined    = (detail_desc + ' ' + listing_desc + ' ' + tags_raw).lower()

    # ── Feature detection ──────────────────────────────────────────────────────
    # has_garage: prefer ZenRows detail (parsed from full page), fallback to text
    has_garage    = det.get('has_garage') or 'garagem' in combined or 'garagem incluída' in combined
    has_storage   = det.get('has_storage') or any(x in combined for x in ['arrumos', 'arrecadação', 'arrumação'])
    parking_spaces= det.get('parking_spaces', 1 if has_garage else 0)
    wcs           = det.get('wcs') or det.get('bathrooms')
    elevator      = det.get('elevator') or any(x in combined for x in ['elevador', 'elevator', 'lift'])
    floor_raw     = det.get('floor_num') if det.get('floor_num') is not None else l.get('floor', '')

    has_sea_view  = any(x in combined for x in ['vista mar', 'sea view', 'vistas mar'])
    owner_direct  = (any(x in combined for x in ['sem agência', 'proprietário', 'particular',
                                                   'nao disponível para agencias'])
                     and 'agencia' not in combined)

    # Outdoor space
    outdoor = None
    if any(x in combined for x in ['terraço', 'terraco', 'terrace', 'rooftop']):  outdoor = 'Terraço'
    elif any(x in combined for x in ['varanda', 'balcony']):                       outdoor = 'Varanda'
    elif any(x in combined for x in ['jardim', 'garden', 'quintal', 'patio', 'pátio']): outdoor = 'Garden'

    # Sun exposure: prefer ZenRows detail (has orientation text), then listing, then vision
    sun = det.get('sun_exposure', '')
    if not sun:
        for phrase in ['nascente/sul', 'sul/nascente', 'nascente e sul', 'sul e nascente',
                       'orientação sul', 'exposição sul', 'sul', 'south', 'nascente',
                       'east', 'poente', 'oeste', 'north', 'norte']:
            if phrase in combined:
                sun = phrase; break
    if not sun and vis.get('solar_direction') and vis['solar_direction'].lower() not in ('unknown', ''):
        sun = vis['solar_direction'].lower()

    # Space per room
    try:
        num_rooms = int(re.search(r'\d+', l.get('rooms', '1')).group())
    except (AttributeError, ValueError):
        num_rooms = 1
    space_per_room = round(size_ref / max(1, num_rooms), 1) if size_ref else 0

    # ── Build enriched dict ───────────────────────────────────────────────────
    e = {
        # Identity
        'id': lid, 'url': l.get('url'), 'title': l.get('title'),
        'neighborhood': l.get('neighborhood'), 'rooms': l.get('rooms'),
        # Size
        'size_m2': l.get('size_m2'), 'area_bruta': area_bruta, 'area_util': area_util,
        'space_per_room_m2': space_per_room, 'wcs': wcs,
        # Price
        'price_eur': price, 'price_per_m2': price_per_m2,
        'price_delta_zone': price_delta, 'price_delta_pct': price_delta_pct,
        # Features
        'has_garage': has_garage, 'parking_spaces': parking_spaces,
        'has_storage': has_storage, 'elevator': elevator, 'floor_level': floor_raw,
        'energy_cert': det.get('energy_cert'), 'year_built': det.get('year_built'),
        'outdoor_space': outdoor, 'sea_view': has_sea_view,
        'sun_exposure': sun, 'owner_direct': owner_direct,
        # Location
        'dist_to_sea_km':    geo.get('dist_to_sea_km', 5.0),
        'walk_time_sea_min': geo.get('walk_time_sea_min'),
        'elevation_m':       geo.get('elevation_m'),
        'dist_to_foz_km':    geo.get('dist_to_foz_km'),
        # Image analysis (v5)
        'image_score':        vis.get('score'),
        'area_quality_score': vis.get('area_quality_score'),
        'feel':               vis.get('feel'),
        'finish':             vis.get('finish'),
        'light_quality':      vis.get('light'),
        'area_impression':    vis.get('area_impression'),
        'wide_angle_flag':    vis.get('wide_angle_flag', False),
        'renovation':         vis.get('renovation'),
        'renovation_year':    vis.get('renovation_year'),
        'images_analyzed':    vis.get('total_images'),
        'red_flags_visual':   vis.get('red_flags'),
        'image_summary':      vis.get('summary'),
        'solar_direction':    vis.get('solar_direction', ''),
        # Description (full text from ZenRows or snippet from scraper)
        'full_description':   detail_desc[:500] or listing_desc[:300],
        # DOM
        'days_on_market': dom.get('days_on_market', 0),
        # Commerce
        'nearest_supermarket':    com.get('nearest_supermarket'),
        'nearest_supermarket_km': com.get('nearest_supermarket_km'),
        'nearest_pharmacy':       com.get('nearest_pharmacy'),
        'nearest_pharmacy_km':    com.get('nearest_pharmacy_km'),
        'restaurants_300m':       com.get('restaurants_300m', 0),
        'nearest_metro':          com.get('nearest_metro'),
        'nearest_metro_km':       com.get('nearest_metro_km'),
        'nearest_beach':          com.get('nearest_beach'),
        'nearest_beach_km':       com.get('nearest_beach_km'),
        'schools_1km':            com.get('schools_1km', 0),
        # Location enrichment (from geocoded.json via enrich_location.py)
        'school_score':           geo.get('school_score'),
        'nearest_good_school':    geo.get('nearest_good_school', ''),
        'nearest_school_km':      geo.get('nearest_school_km'),
        'noise_penalty':          geo.get('noise_penalty', 0),
        'noise_sources':          geo.get('noise_sources', ''),
        'parks_800m':             geo.get('parks_800m', 0),
        'nearest_park':           geo.get('nearest_park', ''),
        'hospitals_3km':          geo.get('hospitals_3km', 0),
        'nearest_hospital':       geo.get('nearest_hospital', ''),
        'nearest_hospital_km':    geo.get('nearest_hospital_km'),
        'bus_stops_400m':         geo.get('bus_stops_400m', 0),
        'supermarket_tier':       geo.get('supermarket_tier', 2),
        # Description signals
        'is_furnished':           geo.get('is_furnished', False),
        'kitchen_equipped':       geo.get('kitchen_equipped', False),
        'has_suite':              geo.get('has_suite', False),
        'has_fireplace':          geo.get('has_fireplace', False),
        'has_ac':                 geo.get('has_ac', False),
        'has_pool':               geo.get('has_pool', False),
        'has_concierge':          geo.get('has_concierge', False),
        'double_glazing':         geo.get('double_glazing', False),
        'description_bonus_pts':  geo.get('description_bonus_pts', 0),
        'light_mentioned':        geo.get('light_mentioned', False),
        # Facade
        'facade_score':           geo.get('facade_score'),
        'building_condition':     geo.get('building_condition', ''),
        'street_quality':         geo.get('street_quality', ''),
        # Photos (from ZenRows detail page — full-res 1500px despite /blur/ URL prefix)
        'photo_urls':             det.get('photo_urls', []),
        'photo_count':            det.get('photo_count', 0),
    }

    # ── Score ─────────────────────────────────────────────────────────────────
    score_result = calc_score(e, zone_medians)
    e['raw_score']         = score_result['raw_score']
    e['opportunity_score'] = score_result['opportunity_score']   # normalised 0-100
    e['score_breakdown']   = score_result['score_breakdown']     # component dict

    return e


def main():
    print('🦞 Listings Enricher v9')
    listings     = load_json(PATHS.listings, [])
    details      = {d['id']: d for d in load_json(PATHS.details_zenrows, [])}
    v_analysis   = load_json(PATHS.image_analysis, {})
    geo_map      = {g['id']: g for g in load_json(PATHS.geocoded, [])}
    dom_map      = load_json(PATHS.dom_tracker, {})
    commerce_map = {c['id']: c for c in load_json(PATHS.commerce, [])}

    # Compute live zone medians (first pass — just need price_per_m2)
    prelim = []
    for l in listings:
        det       = details.get(l['id'], {})
        area_util = det.get('area_util')
        size_ref  = area_util or det.get('area_bruta') or l.get('size_m2', 0)
        price     = l.get('price_eur', 0)
        ppm       = round(price / size_ref, 2) if size_ref else 0
        prelim.append({**l, 'price_per_m2': ppm})
    zone_medians = compute_zone_medians(prelim)
    print(f'   Zone medians computed from {len(prelim)} listings')

    enriched_all = []
    for l in listings:
        lid = l['id']
        e = extract_features(
            l, details.get(lid, {}), v_analysis.get(lid, {}),
            geo_map.get(lid, {}), dom_map.get(lid, {}), commerce_map.get(lid, {}),
            zone_medians,
        )
        enriched_all.append(e)

    with open(PATHS.enriched, 'w', encoding='utf-8') as f:
        json.dump(enriched_all, f, ensure_ascii=False, indent=2)
    print(f'✅ {len(enriched_all)} listings enriched')

    # Stats
    has_util  = sum(1 for e in enriched_all if e.get('area_util'))
    has_img   = sum(1 for e in enriched_all if e.get('image_score') is not None)
    has_sun   = sum(1 for e in enriched_all if e.get('sun_exposure'))
    avg_score = sum(e['opportunity_score'] for e in enriched_all) / len(enriched_all)
    max_raw   = max(e['raw_score'] for e in enriched_all)
    print(f'   area_util: {has_util}/{len(enriched_all)} | image_score: {has_img}/{len(enriched_all)} | sun: {has_sun}/{len(enriched_all)}')
    print(f'   Avg score: {avg_score:.1f} | Max raw: {max_raw}')

    # Push to Sheets
    from sheets import push_to_sheets
    push_to_sheets(enriched_all)


if __name__ == '__main__':
    main()
