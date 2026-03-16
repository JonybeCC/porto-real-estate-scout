"""
paths.py — Single source of truth for all project paths.

Import this in every script instead of hardcoding absolute paths.
This makes the project portable and eliminates 42 hardcoded path occurrences.

Usage:
    from paths import PATHS
    listings = load_json(PATHS.listings)
"""

import os
from dataclasses import dataclass

# Resolve project root relative to this file (scraper/paths.py → project root)
_SCRAPER_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRAPER_DIR)
_DATA_DIR     = os.path.join(_PROJECT_ROOT, 'data')
_CREDS_DIR    = os.path.normpath(os.path.join(_PROJECT_ROOT, '..', '..', '..', 'credentials'))


@dataclass(frozen=True)
class _Paths:
    # Project roots
    project:  str
    scraper:  str
    data:     str
    creds:    str

    # Data files
    listings:              str
    listings_deduped:      str
    geocoded:              str
    enriched:              str
    details_zenrows:       str
    image_analysis:        str
    dom_tracker:           str
    commerce:              str
    pipeline_state:        str
    pipeline_log:          str
    pipeline_pid:          str
    pipeline_lock:         str
    duplicates:            str
    price_history:         str
    relistings:            str

    # Credentials
    google_service_account: str

    # Sheets
    sheet_url:  str
    sheet_name: str


def _p(filename: str) -> str:
    return os.path.join(_DATA_DIR, filename)


PATHS = _Paths(
    project  = _PROJECT_ROOT,
    scraper  = _SCRAPER_DIR,
    data     = _DATA_DIR,
    creds    = _CREDS_DIR,

    listings              = _p('listings.json'),
    listings_deduped      = _p('listings_deduped.json'),
    geocoded              = _p('geocoded.json'),
    enriched              = _p('enriched_listings.json'),
    details_zenrows       = _p('listing_details_zenrows.json'),
    image_analysis        = _p('image_analysis.json'),
    dom_tracker           = _p('dom_tracker.json'),
    commerce              = _p('commerce.json'),
    pipeline_state        = _p('pipeline_state.json'),
    pipeline_log          = _p('pipeline_run.log'),
    pipeline_pid          = _p('pipeline.pid'),
    pipeline_lock         = _p('pipeline.lock'),
    duplicates            = _p('duplicates.json'),
    price_history         = _p('price_history.json'),
    relistings            = _p('relistings.json'),

    google_service_account = os.path.join(_CREDS_DIR, 'google-service-account.json'),

    sheet_url  = 'https://docs.google.com/spreadsheets/d/1Ljk9s45BovbRfk1QIUGgGxbjnpIgs2F52rJjDNGQOQI/edit',
    sheet_name = 'Listings',
)


if __name__ == '__main__':
    print('=== Project Paths ===')
    for field, value in PATHS.__dataclass_fields__.items():
        val = getattr(PATHS, field)
        exists = '✅' if os.path.exists(val) else '❌' if field not in ('sheet_url', 'sheet_name') else '🌐'
        print(f'  {exists} {field:<30} {val}')
