# Data Directory

All runtime data files. **Not committed to git** (see `.gitignore`).

## Active Files

| File | Written by | Read by | Description |
|------|-----------|---------|-------------|
| `listings.json` | `daily_update.py` (scrape), `dedup.py` | Everyone | Canonical listing source. 157 listings. |
| `geocoded.json` | `geocode.py`, `enrich_location.py` | `enrich_listings.py` | lat/lng + distances + location signals |
| `enriched_listings.json` | `enrich_listings.py` | `push_to_sheets`, `monitor.py` | Final scored output → Google Sheets |
| `listing_details_zenrows.json` | `fetch_zenrows.py` | `dedup.py`, `enrich_listings.py` | ZenRows detail page data (area_util, photos, garage). Currently 16/157 — rest blocked by Cloudflare. |
| `image_analysis.json` | `analyze_images.py` | `enrich_listings.py` | GPT-4o vision scores (97/157 listings) |
| `dom_tracker.json` | `dom_tracker.py` | `enrich_listings.py` | Days on market + price history per listing |
| `commerce.json` | `enrich_commerce.py` | `enrich_listings.py` | Supermarket, pharmacy, metro, beach, schools proximity |
| `pipeline_state.json` | `pipeline_state.py` | `monitor.py`, `check_pipeline.sh` | Last 30 run records with step results |
| `duplicates.json` | `dedup.py` | `cleanup.py` | Confirmed duplicate pairs |
| `price_history.json` | `price_tracker.py` | `price_tracker.py` | Price change history per listing |
| `relistings.json` | `dom_tracker.py` | — | Listings that reappeared after going inactive |

## Transient Files

| File | Purpose |
|------|---------|
| `pipeline_run.log` | stdout/stderr of current run. Rotated to `.prev` on next run. |
| `pipeline_run.log.prev` | Previous run log |
| `pipeline.pid` | PID of running pipeline process |
| `pipeline.lock` | Lock file preventing double-starts |
| `listings.json.bak` | Backup before dedup modifies listings |

## Removed (dead files)

- `listing_details.json` — Old fetch_listing_details.py output (now _deprecated)
- `_deprecated_condition_scores.json` — Old scoring system leftover
- `monitor_data.json` — Redundant with pipeline_state.json
- `update_log.json` — Superseded by pipeline_state.json
- `listings_deduped.json` — Only written when dupes found, not useful separately

## Known Data Quality Issues

| Field | Coverage | Root Cause | Fix |
|-------|----------|-----------|-----|
| `area_util` | 0/157 | ZenRows detail pages Cloudflare-blocked | Residential proxy |
| `image_score` | 97/157 | Same — no photos for 60 listings | Same |
| `sun_exposure` | 42/157 | Rarely stated in descriptions | GPT vision covers 97 |
| `school_score` | 125/157 | 32 listings outside Overpass school radius | Extend radius |
