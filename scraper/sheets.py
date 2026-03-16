"""
sheets.py — Google Sheets push for enriched listings.

Extracted from enrich_listings.py v8 monolith.
Handles: auth, column layout, sorting, batch write, retry logic.

Score breakdown columns added (v9):
  Each scoring component is its own column so you can see exactly why
  a listing scored high or low without reverse-engineering the numbers.
"""

import json, time, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from paths import PATHS

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]

# ── Column definitions ────────────────────────────────────────────────────────
# Main sheet columns
MAIN_HEADERS = [
    # Identity & ranking
    'Rank', 'Score (0-100)', 'Raw Score', 'ID', 'Rooms', 'Neighbourhood', 'URL',
    # Size
    'Size m²', 'Área Útil', 'Space/Room m²', 'WCs',
    # Price
    'Price €/mo', '€/m²', 'Price Δ%', 'Days on Market',
    # Location
    'Floor', 'Elevator', 'Sun Exposure', 'Sea View',
    'Dist Sea km', 'Walk Sea min', 'Elevation m',
    # Features
    'Garage', 'Parking Spaces', 'Storage', 'Outdoor', 'Owner Direct',
    # Building
    'Year Built', 'Energy Cert',
    # Image
    'Image Score', 'Renovation', 'Feel', 'Finish', 'Light', 'Red Flags', 'Image Summary',
    # Commerce
    'Supermarket', 'Super km', 'Pharmacy km', 'Restaurants 300m',
    'Metro km', 'Beach km', 'Schools 1km',
    # Description
    'Full Description',
]

# Score breakdown columns (separate tab)
BREAKDOWN_HEADERS = [
    'Rank', 'ID', 'Score', 'Raw',
    # Image components
    'img_condition', 'img_finish', 'img_light', 'img_area', 'img_renovation',
    # Location & zone
    'sun', 'zone', 'floor',
    # Features
    'garage', 'storage', 'outdoor', 'elevator',
    # Space & price
    'space_efficiency', 'price_value',
    # Sea
    'sea_view', 'sea_walk',
    # Lifestyle
    'school', 'noise', 'parks', 'bus', 'hospital',
    'supermarket', 'restaurants', 'pharmacy', 'facade',
    # Bonuses/penalties
    'description', 'owner_direct', 'elevation',
    'staleness', 'price_sanity', 'red_flags',
]


def _row_main(rank: int, e: dict) -> list:
    return [
        rank,
        e.get('opportunity_score'),
        e.get('raw_score'),
        e.get('id'),
        e.get('rooms'),
        e.get('neighborhood'),
        e.get('url'),
        e.get('size_m2'),
        e.get('area_util'),
        e.get('space_per_room_m2'),
        e.get('wcs'),
        e.get('price_eur'),
        e.get('price_per_m2'),
        e.get('price_delta_pct'),
        e.get('days_on_market'),
        e.get('floor_level'),
        'Yes' if e.get('elevator') else 'No',
        e.get('sun_exposure'),
        'Yes' if e.get('sea_view') else 'No',
        e.get('dist_to_sea_km'),
        e.get('walk_time_sea_min'),
        e.get('elevation_m'),
        'Yes' if e.get('has_garage') else 'No',
        e.get('parking_spaces'),
        'Yes' if e.get('has_storage') else 'No',
        e.get('outdoor_space') or '',
        'Yes' if e.get('owner_direct') else 'No',
        e.get('year_built'),
        e.get('energy_cert'),
        e.get('image_score'),
        e.get('renovation'),
        e.get('feel'),
        e.get('finish'),
        e.get('light_quality'),
        e.get('red_flags_visual'),
        (e.get('image_summary') or '')[:150],
        e.get('nearest_supermarket'),
        e.get('nearest_supermarket_km'),
        e.get('nearest_pharmacy_km'),
        e.get('restaurants_300m'),
        e.get('nearest_metro_km'),
        e.get('nearest_beach_km'),
        e.get('schools_1km'),
        (e.get('full_description') or '')[:300],
    ]


def _row_breakdown(rank: int, e: dict) -> list:
    bd = e.get('score_breakdown', {})
    return [
        rank, e.get('id'), e.get('opportunity_score'), e.get('raw_score'),
        bd.get('image_condition', ''), bd.get('image_finish', ''),
        bd.get('image_light', ''), bd.get('image_area', ''), bd.get('image_renovation', ''),
        bd.get('sun', ''), bd.get('zone', ''), bd.get('floor', ''),
        bd.get('garage', ''), bd.get('storage', ''), bd.get('outdoor', ''), bd.get('elevator', ''),
        bd.get('space_efficiency', ''), bd.get('price_value', ''),
        bd.get('sea_view', ''), bd.get('sea_walk', ''),
        bd.get('school', ''), bd.get('noise', ''), bd.get('parks', ''), bd.get('bus', ''),
        bd.get('hospital', ''), bd.get('supermarket', ''), bd.get('restaurants', ''),
        bd.get('pharmacy', ''), bd.get('facade', ''),
        bd.get('description', ''), bd.get('owner_direct', ''), bd.get('elevation', ''),
        bd.get('staleness', ''), bd.get('price_sanity', ''), bd.get('red_flags', ''),
    ]


def _write_sheet(ws, rows: list[list], chunk_size: int = 50):
    """Clear sheet and write rows in chunks to stay under Sheets API quota."""
    ws.clear()
    time.sleep(1)
    for i in range(0, len(rows), chunk_size):
        ws.update(rows[i:i + chunk_size], f'A{i + 1}', value_input_option='USER_ENTERED')
        time.sleep(0.6)


def push_to_sheets(enriched_listings: list, retries: int = 3):
    """Push sorted enriched listings to Google Sheets (two tabs: main + breakdown)."""
    print('\n📊 Pushing to Google Sheets...')

    sorted_list = sorted(enriched_listings, key=lambda x: x.get('raw_score', 0), reverse=True)

    main_rows      = [MAIN_HEADERS]
    breakdown_rows = [BREAKDOWN_HEADERS]
    for rank, e in enumerate(sorted_list, 1):
        main_rows.append(_row_main(rank, e))
        breakdown_rows.append(_row_breakdown(rank, e))

    for attempt in range(1, retries + 1):
        try:
            creds = Credentials.from_service_account_file(PATHS.google_service_account, scopes=SCOPES)
            gc    = gspread.authorize(creds)
            sh    = gc.open_by_url(PATHS.sheet_url)

            # Main tab
            try:
                ws_main = sh.worksheet('Enriched_v9')
            except Exception:
                ws_main = sh.add_worksheet(title='Enriched_v9', rows=400, cols=len(MAIN_HEADERS) + 2)
            _write_sheet(ws_main, main_rows)

            # Breakdown tab
            try:
                ws_bd = sh.worksheet('Score_Breakdown')
            except Exception:
                ws_bd = sh.add_worksheet(title='Score_Breakdown', rows=400, cols=len(BREAKDOWN_HEADERS) + 2)
            _write_sheet(ws_bd, breakdown_rows)

            print(f'  ✅ {len(enriched_listings)} rows pushed (attempt {attempt}).')
            print(f'  📋 Main tab: Enriched_v9 | Breakdown tab: Score_Breakdown')
            return

        except Exception as e:
            print(f'  ⚠️  Sheets push failed (attempt {attempt}/{retries}): {e}')
            if attempt < retries:
                time.sleep(10 * attempt)

    print('  ❌ Sheets push gave up after all retries — local JSON is saved.')
