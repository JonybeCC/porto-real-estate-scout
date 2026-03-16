"""
scoring.py — Opportunity Score v8 engine.

Single source of truth for all scoring weights and calc_score_v8().
Imported by enrich_listings.py and analyze_images.py.

Score is NOT capped at 100. The theoretical max is ~145.
enrich_listings.py stores raw score + normalised 0-100 score separately.
This lets you sort by raw score (exact ordering) and display the 0-100 for UX.
"""

import re

# ── Zone desirability weights (0-15) ─────────────────────────────────────────
ZONE_DESIRABILITY: dict[str, int] = {
    'pinhais da foz': 15, 'nevogilde': 15, 'foz do douro': 13, 'foz velha': 13,
    'foz': 13, 'gondarém': 11, 'serralves': 11, 'pinheiro manso': 10,
    'massarelos': 9, 'boavista': 9, 'aldoar': 7, 'lordelo do ouro': 7,
    'bessa leite': 7, 'bessa': 7, 'aviz': 7, 'cristo rei': 5,
    'cedofeita': 5, 'bonfim': 5, 'matosinhos': 5, 'paranhos': 5,
    'ramalde': 5, 'campanhã': 3, 'default': 5,
}

# ── Fallback zone price medians (€/m²) — updated live from dataset ───────────
ZONE_PRICE_MEDIANS_FALLBACK: dict[str, float] = {
    'pinhais da foz': 15.0, 'nevogilde': 17.4, 'foz do douro': 17.4, 'foz velha': 20.8,
    'foz': 15.8, 'gondarém': 15.8, 'serralves': 16.5, 'pinheiro manso': 12.5,
    'massarelos': 16.2, 'boavista': 15.6, 'aldoar': 14.0, 'lordelo do ouro': 14.0,
    'bessa leite': 15.6, 'bessa': 15.6, 'aviz': 17.7, 'ramalde': 13.1, 'default': 14.0,
}

# ── Image scoring weights (mirrored in analyze_images.py SCORE_CONFIG) ───────
IMG_FINISH_BONUS:  dict[str, int] = {'Luxury': 6, 'Premium': 3, 'Standard': 0, 'Basic': -2}
IMG_LIGHT_BONUS:   dict[str, int] = {'Excellent': 4, 'Good': 2, 'Average': 0, 'Poor': -2}
IMG_AREA_BONUS:    dict[str, int] = {'Spacious': 3, 'Adequate': 0, 'Cramped': -4}
IMG_RENOV_BONUS:   dict[str, int] = {
    'New Build': 8, 'Fully Renovated': 5, 'Partially Renovated': 2, 'Original': -2, 'Unknown': 0,
}

# ── Theoretical max score (for normalisation) ─────────────────────────────────
# image: 25+6+4+3+8=46, sun:20, zone:15, features:6+3+4+3=16, floor:8,
# space:7, price:8, bonuses:3+3, school:3, desc:12, parks:2, hospital:2,
# bus:3, super:2, resto:2, pharm:1, facade:4 = ~163 theoretical
# In practice top score is ~90-95 with real data, uncapped.
THEORETICAL_MAX = 145  # conservative practical max for normalisation display


def zone_key(neighbourhood: str) -> str:
    """Return the zone key matching this neighbourhood string."""
    n = (neighbourhood or '').lower()
    for k in ZONE_DESIRABILITY:
        if k != 'default' and k in n:
            return k
    return 'default'


def sun_score(sun: str, description: str = '', solar_direction: str = '') -> int:
    """
    0-20. South/SE=20, SW=18, E/W=12, North=0, unknown=4.
    Checks sun field, then description text, then GPT solar_direction.
    """
    combined = ' '.join([str(sun or ''), str(description or ''), str(solar_direction or '')]).lower()
    if any(x in combined for x in ['sul, este', 'sul e nascente', 'nascente e sul',
                                     'sul/nascente', 'nascente/sul', 'orientação sul',
                                     'exposição sul', 'sul', 'south', 'sse', 'se ', 'sudeste']):
        return 20
    if any(x in combined for x in ['southwest', 'sudoeste', 'ssw']):    return 18
    if any(x in combined for x in ['nascente', 'east', 'oeste', 'west', 'poente', 'este']): return 12
    if any(x in combined for x in ['norte', 'north']):                  return 0
    return 4  # unknown


def floor_score(floor_val) -> int:
    """0-8. RC/1=0, 2=3, 3=5, 4+=7, penthouse/cobertura=8."""
    s = str(floor_val or '').lower()
    if any(x in s for x in ['r/c', 'rc', 'rés', 'rez', 'ground', 'térreo']): return 0
    try:
        n = int(re.search(r'\d+', s).group())
        if n <= 1: return 0
        if n == 2: return 3
        if n == 3: return 5
        return 7
    except (AttributeError, ValueError):
        pass
    if any(x in s for x in ['penthouse', 'último', 'cobertura', 'top']): return 8
    return 2


def calc_score(e: dict, zone_medians: dict) -> dict:
    """
    Compute opportunity score with full component breakdown.

    Returns dict with:
      'raw_score'        — uncapped integer (can exceed 100)
      'opportunity_score'— normalised 0-100 (for display)
      'score_breakdown'  — dict of component → points (for Sheet columns)
    """
    breakdown: dict[str, int] = {}
    neigh = str(e.get('neighborhood', '')).lower()
    zk    = zone_key(neigh)

    # ── 1. CONDITION & IMAGE QUALITY (0-47) ───────────────────────────────────
    img = e.get('image_score')
    if img is not None:
        cond_pts = min(25, int(float(img) * 2.5))
        fin_pts  = IMG_FINISH_BONUS.get(e.get('finish', 'Standard'), 0)
        lgt_pts  = IMG_LIGHT_BONUS.get(e.get('light_quality', 'Average'), 0)

        aq = e.get('area_quality_score')
        if isinstance(aq, (int, float)):
            if aq >= 9:   area_pts = 4
            elif aq >= 7: area_pts = 2
            elif aq <= 3: area_pts = -2
            else:         area_pts = 0
        else:
            area_pts = IMG_AREA_BONUS.get(e.get('area_impression', 'Adequate'), 0)

        if e.get('wide_angle_flag') and area_pts > 0:
            area_pts -= 1  # modest uncertainty discount, not a zero-out

        ren_pts = IMG_RENOV_BONUS.get(e.get('renovation', 'Unknown'), 0)

        breakdown['image_condition'] = cond_pts
        breakdown['image_finish']    = fin_pts
        breakdown['image_light']     = lgt_pts
        breakdown['image_area']      = area_pts
        breakdown['image_renovation']= ren_pts
    else:
        cond_pts = fin_pts = lgt_pts = area_pts = ren_pts = 0
        breakdown['image_condition'] = 5  # no photos ≠ neutral — small base
        breakdown['image_finish'] = breakdown['image_light'] = 0
        breakdown['image_area'] = breakdown['image_renovation'] = 0

    # ── 2. SUN EXPOSURE (0-20) ────────────────────────────────────────────────
    sun_pts = sun_score(e.get('sun_exposure', ''), e.get('full_description', ''), e.get('solar_direction', ''))
    breakdown['sun'] = sun_pts

    # ── 3. ZONE DESIRABILITY (0-15) ───────────────────────────────────────────
    zone_pts = ZONE_DESIRABILITY.get(zk, ZONE_DESIRABILITY['default'])
    breakdown['zone'] = zone_pts

    # ── 4. PRACTICAL FEATURES (0-16) ─────────────────────────────────────────
    spaces = e.get('parking_spaces', 0) or 0
    garage_pts = 6 if spaces >= 2 else (3 if e.get('has_garage') else 0)
    storage_pts = 3 if e.get('has_storage') else 0
    outdoor_pts = 4 if (e.get('outdoor_space') and e['outdoor_space'] not in ('None', None, '')) else 0
    elevator_pts = 3 if e.get('elevator') else 0
    breakdown['garage']   = garage_pts
    breakdown['storage']  = storage_pts
    breakdown['outdoor']  = outdoor_pts
    breakdown['elevator'] = elevator_pts

    # ── 5. FLOOR LEVEL (0-8) ─────────────────────────────────────────────────
    floor_pts = floor_score(e.get('floor_level'))
    breakdown['floor'] = floor_pts

    # ── 6. SPACE EFFICIENCY (0-7) ─────────────────────────────────────────────
    spr   = e.get('space_per_room_m2', 0) or 0
    rooms_str = str(e.get('rooms', '')).upper()
    bench = 40 if 'T3' in rooms_str else 45
    if spr >= bench * 1.3:   space_pts = 7
    elif spr >= bench:        space_pts = 5
    elif spr >= bench * 0.8: space_pts = 3
    else:                     space_pts = 1
    breakdown['space_efficiency'] = space_pts

    # ── 7. PRICE VALUE vs ZONE MEDIAN (0-8) ───────────────────────────────────
    ppm         = e.get('price_per_m2', 0) or 0
    zone_median = zone_medians.get(zk, zone_medians.get('default', 15.0))
    if ppm > 0 and zone_median > 0:
        ratio = ppm / zone_median
        if ratio <= 0.80:   price_pts = 8
        elif ratio <= 0.90: price_pts = 6
        elif ratio <= 1.00: price_pts = 4
        elif ratio <= 1.10: price_pts = 2
        else:               price_pts = 0
    else:
        price_pts = 0
    breakdown['price_value'] = price_pts

    # ── 8. SEA PROXIMITY ──────────────────────────────────────────────────────
    walk_min = e.get('walk_time_sea_min') or 0
    dist_km  = e.get('dist_to_sea_km', 9) or 9
    sea_view_pts = 0
    if e.get('sea_view') and (walk_min <= 25 or (walk_min == 0 and dist_km <= 1.5)):
        sea_view_pts = 3
    if walk_min > 0:
        walk_pts = 3 if walk_min <= 8 else (2 if walk_min <= 15 else (1 if walk_min <= 25 else 0))
    elif dist_km < 0.5:
        walk_pts = 2
    else:
        walk_pts = 0
    breakdown['sea_view']    = sea_view_pts
    breakdown['sea_walk']    = walk_pts

    # ── 9. BONUSES & SIGNALS ──────────────────────────────────────────────────
    elev_penalty = -1 if (e.get('elevation_m') or 0) > 80 else 0
    owner_pts    = 2 if e.get('owner_direct') else 0
    school_pts   = (3 if (e.get('school_score') or 0) >= 8 else
                    2 if (e.get('school_score') or 0) >= 6 else
                    1 if (e.get('school_score') or 0) >= 4 else 0)
    noise_pts    = e.get('noise_penalty', 0) or 0  # typically -5 to 0
    desc_pts     = min(12, e.get('description_bonus_pts', 0) or 0)
    parks_pts    = 2 if (e.get('parks_800m') or 0) >= 2 else (1 if (e.get('parks_800m') or 0) >= 1 else 0)
    hosp_km      = e.get('nearest_hospital_km') or 99
    hosp_pts     = 2 if hosp_km <= 1.0 else (1 if hosp_km <= 2.0 else 0)
    buses        = e.get('bus_stops_400m', 0) or 0
    bus_pts      = 3 if buses >= 8 else (2 if buses >= 4 else (1 if buses >= 2 else 0))
    tier         = e.get('supermarket_tier', 2) or 2
    super_pts    = 2 if tier == 3 else (-1 if tier == 1 else 0)
    resto        = e.get('restaurants_300m', 0) or 0
    resto_pts    = 2 if resto >= 6 else (1 if resto >= 3 else 0)
    pharm_pts    = 1 if (e.get('nearest_pharmacy_km') or 99) <= 0.2 else 0
    facade       = e.get('facade_score')
    facade_pts   = (4 if facade and facade >= 8 else
                    2 if facade and facade >= 6 else
                    -3 if facade and facade <= 3 else 0)
    dom          = e.get('days_on_market', 0) or 0
    stale_pts    = -4 if dom >= 60 else (-2 if dom >= 30 else 0)
    sanity_pts   = -5 if (ppm > 0 and zone_median > 0 and ppm / zone_median > 2.5) else 0
    red          = str(e.get('red_flags_visual') or '').lower()
    redflag_pts  = -sum(2 for kw in ['damp','crack','mold','mould','dark','very small','noise'] if kw in red) if red and red not in ('none','') else 0

    breakdown['elevation']    = elev_penalty
    breakdown['owner_direct'] = owner_pts
    breakdown['school']       = school_pts
    breakdown['noise']        = noise_pts
    breakdown['description']  = desc_pts
    breakdown['parks']        = parks_pts
    breakdown['hospital']     = hosp_pts
    breakdown['bus']          = bus_pts
    breakdown['supermarket']  = super_pts
    breakdown['restaurants']  = resto_pts
    breakdown['pharmacy']     = pharm_pts
    breakdown['facade']       = facade_pts
    breakdown['staleness']    = stale_pts
    breakdown['price_sanity'] = sanity_pts
    breakdown['red_flags']    = redflag_pts

    # ── Total ─────────────────────────────────────────────────────────────────
    img_total = (breakdown['image_condition'] + breakdown['image_finish'] +
                 breakdown['image_light'] + breakdown['image_area'] +
                 breakdown['image_renovation'])
    raw_score = (img_total + sun_pts + zone_pts + garage_pts + storage_pts +
                 outdoor_pts + elevator_pts + floor_pts + space_pts + price_pts +
                 sea_view_pts + walk_pts + elev_penalty + owner_pts + school_pts +
                 noise_pts + desc_pts + parks_pts + hosp_pts + bus_pts + super_pts +
                 resto_pts + pharm_pts + facade_pts + stale_pts + sanity_pts + redflag_pts)

    # Normalised 0-100 for display (doesn't clip the raw value)
    normalised = int(round(max(0, min(100, raw_score / THEORETICAL_MAX * 100))))

    return {
        'raw_score':         raw_score,
        'opportunity_score': normalised,
        'score_breakdown':   breakdown,
    }


def compute_zone_medians(listings: list) -> dict:
    """Compute live €/m² medians per zone from current dataset."""
    import statistics
    from collections import defaultdict
    zone_prices: dict[str, list] = defaultdict(list)
    for l in listings:
        ppm = l.get('price_per_m2') or 0
        if ppm > 0:
            zone_prices[zone_key(l.get('neighborhood', ''))].append(ppm)
    medians = dict(ZONE_PRICE_MEDIANS_FALLBACK)
    for zone, prices in zone_prices.items():
        if len(prices) >= 2:
            medians[zone] = statistics.median(prices)
    return medians
