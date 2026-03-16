# 🦞 Porto Real Estate Scout

Automated pipeline that scrapes, enriches, scores and ranks rental listings from [Idealista.pt](https://www.idealista.pt) for a defined geographic zone of Porto. Results are pushed to Google Sheets with a ranked opportunity score and a live web dashboard.

---

## Architecture Overview

```
Idealista.pt  ←  ZenRows (JS render + residential proxy)
      │
      ▼
daily_update.py          ← 12-step pipeline orchestrator (entry point)
      │
      ├─ 1.  scrape         → listings.json              (ZenRows HTML → parsed listings)
      ├─ 2.  dedup          → listings.json              (5-signal duplicate removal)
      ├─ 3.  geocode        → geocoded.json              (Nominatim + OpenCage fallback)
      ├─ 4.  fetch_zenrows  → listing_details_zenrows.json (full description, photos, garage, WCs)
      ├─ 5.  enrich_location→ geocoded.json  ★ SSOT ★   (OSM/Overpass: parks, hospitals, bus,
      │                                                   noise, schools, description signals)
      ├─ 6.  enrich_commerce→ commerce.json              (supermarket, pharmacy, metro, beach)
      ├─ 7.  run_image_batch→ image_analysis.json        (GPT-4o: condition, area, renovation,
      │                                                   finish, light, wide-angle, solar direction)
      ├─ 8.  dom_tracker    → dom_tracker.json           (days on market, price change history)
      ├─ 9.  enrich_listings→ enriched_listings.json     (merge all → opportunity score v8)
      │                    → Google Sheets               (Enriched_vX tab, 51 columns)
      ├─ 10. price_tracker  → Telegram alerts            (price drops ≥5%, new high-score listings)
      ├─ 11. cleanup        → listings.json              (remove confirmed-deleted listings)
      └─ 12. monitor        → Telegram alert if unhealthy (staleness, API validity, count sanity)
```

Steps 3–7 run **only when new listings are found**, keeping daily runs fast when nothing changed.
All state is tracked in `data/pipeline_state.json` (last 30 runs, per-step status).

---

## Active Scripts (`scraper/`)

| Script | Role |
|--------|------|
| `daily_update.py` | 12-step pipeline orchestrator — the only entry point |
| `pipeline_state.py` | Run state tracker: step-level success/fail/skip, 30-run history |
| `geocode.py` | Address → lat/lng via Nominatim (OSM), OpenCage fallback for street-level accuracy |
| `fetch_zenrows.py` | ZenRows scraper for individual listing pages (desc, photos, garage, WCs, area_util) |
| `enrich_location.py` | **★ CANONICAL authority for `geocoded.json` ★** — Overpass/OSM: parks, hospitals, bus stops, noise penalty, school quality score, description signals (furnished, AC, suite, etc.) |
| `enrich_commerce.py` | Amenity distances: nearest supermarket/pharmacy/metro/beach, restaurant count |
| `enrich_geo.py` | Elevation (Open Topo Data) + walk time to sea (ORS or terrain-adjusted estimate) |
| `analyze_images.py` | GPT-4o iterative vision scoring: condition (1-10), area quality (1-10), wide-angle detection, renovation, feel, finish, light, solar direction |
| `run_image_batch.py` | SIGTERM-safe wrapper: calls `analyze_images.py --batch 10`, designed for cron |
| `enrich_listings.py` | Merges all data sources, computes opportunity score v8, pushes to Google Sheets |
| `dedup.py` | 5-signal duplicate detector: reference codes → description similarity → structural key → photo pHash → GPT vision |
| `dom_tracker.py` | Tracks days-on-market and price changes per listing across daily runs |
| `price_tracker.py` | Sends Telegram alerts on price drops ≥5% or newly high-scoring listings |
| `cleanup.py` | Removes confirmed-deleted listings from `listings.json` |
| `monitor.py` | Health checks pipeline state age, listing count sanity, ZenRows/OpenAI/Google creds |

**Archived (`scraper/_deprecated/`):** `enrich_advanced.py`, `enrich_noise_schools.py`, `assess_condition.py`, `fetch_listing_details.py`, `push_to_sheets.py`, `relisting_detector.py`, `scraper.py` — all superseded by the scripts above.

---

## Data Files (`data/`)

| File | Description | Written by |
|------|-------------|------------|
| `listings.json` | **Source of truth** — all scraped listings (id, price, rooms, size, neighbourhood, URL) | `daily_update.py` (scrape step) |
| `listing_details_zenrows.json` | Full ZenRows detail per listing (description, photos, garage, WCs, area_util, energy cert) | `fetch_zenrows.py` |
| `geocoded.json` | **CANONICAL location data** — lat/lng, distances, Overpass signals, school score, noise penalty, description signals | `geocode.py` (base) + `enrich_location.py` (enrichment) |
| `commerce.json` | Amenity distances per listing | `enrich_commerce.py` |
| `image_analysis.json` | GPT-4o vision results per listing | `analyze_images.py` |
| `dom_tracker.json` | Days-on-market and price history per listing | `dom_tracker.py` |
| `price_history.json` | Longitudinal price + score per listing | `price_tracker.py` |
| `enriched_listings.json` | **Final output** — 82 fields per listing, scored and merged | `enrich_listings.py` |
| `pipeline_state.json` | Pipeline run history (last 30 runs, per-step status) | `pipeline_state.py` |
| `duplicates.json` | Confirmed duplicate pairs from last dedup run | `dedup.py` |
| `relistings.json` | Same-property relisting candidates (price comparison) | `dedup.py` |
| `update_log.json` | Lightweight daily run log (new/total counts) | `daily_update.py` |

> **Key invariant:** `enrich_location.py` is the **only** script that writes enrichment fields to `geocoded.json`. All other scripts are read-only on that file. This prevents conflicting writes and stale data from multiple enrichers overwriting each other.

---

## Opportunity Score v8 (0–100)

Scoring is **modular** — each dimension is a separate, tuneable component. Weights live in `_IMG_*_BONUS` dicts at the top of `enrich_listings.py` and mirror `SCORE_CONFIG` in `analyze_images.py`.

| Dimension | Max pts | Source |
|-----------|---------|--------|
| **Condition** (image AI score × 2.5) | 25 | GPT-4o vision |
| **Finish quality** (Premium +3, Luxury +6, Basic −2) | ±6 | GPT-4o vision |
| **Natural light** (Excellent +4, Poor −2) | ±4 | GPT-4o vision |
| **Area quality** (1-10 score, wide-angle discounted) | ±4 | GPT-4o vision |
| **Renovation** (New Build +8, Original −2) | ±8 | GPT-4o vision |
| **Sun exposure** (South/SE=20, North=0, photo shadows as fallback) | 20 | Description + vision |
| **Zone desirability** (Nevogilde/Pinhais da Foz=15, Campanhã=3) | 15 | Neighbourhood |
| **Garage** (2+ spaces=6, 1=3), **storage** (+3), **outdoor** (+4), **elevator** (+3) | 16 | ZenRows |
| **Floor level** (RC=0, 4th+=7, penthouse=8) | 8 | ZenRows |
| **Space efficiency** (m²/room vs T2/T3 benchmarks) | 7 | Calculated |
| **Price vs zone median** (live-computed, ≥20% below=8, overpriced=0) | 8 | Calculated |
| **Bonuses:** sea view (gated by walk time), walk to sea, owner direct | +8 | Various |
| **Penalties:** staleness 30d=−2 / 60d=−4, noise (motorway/rail), AI red flags | −15 | Various |

---

## Duplicate Detection (`dedup.py`)

Signals are evaluated in order. Earlier signals are faster and more precise; later signals use API calls.

1. **Reference code match** — Internal agency codes (e.g. `apor_250801`) extracted from descriptions → `DEFINITE`
2. **Description similarity ≥80%** — Boilerplate-filtered (vacation rental templates skipped), neighbourhood sanity check → `HIGH`
3. **Structural composite key** — Exact `(rooms, area_util, wcs, floor)` match → `HIGH`
4. **Photo pHash** — Perceptual hash comparison within plausible groups (same rooms + size ±10m² + neighbourhood) → `HIGH`
5. **GPT-4o-mini vision** — For remaining ambiguous pairs: "different angles of same room = SAME unit" → `HIGH`

The group-first approach limits photo comparison to ~41 pairs (vs 8,256 naive cross-product).

---

## Zone / Search Area

The Idealista search area is a polygon encoded using **Google Polyline format**, double-wrapped in parentheses:

```
shape=((encoded_polyline))
```

**Current zone:** Porto Foz / Aldoar / Nevogilde / Boavista — 4.3km × 2.5km, 7 vertices.

To draw a new zone, use the interactive map tool:

```bash
python3 zone_selector.py --map            # opens interactive Leaflet map in browser
python3 zone_selector.py --current        # show current zone details
python3 zone_selector.py --decode "((...))"  # decode any existing shape
python3 zone_selector.py --encode zone.json  # encode new coordinates
```

See [`ZONE_SELECTOR.md`](ZONE_SELECTOR.md) for full documentation.

---

## APIs & Dependencies

| API | Purpose | Auth | Cost |
|-----|---------|------|------|
| [ZenRows](https://www.zenrows.com) | JS-render Idealista (Cloudflare bypass) | `ZENROWS_API_KEY` | Paid |
| [OpenAI GPT-4o](https://platform.openai.com) | Vision scoring, duplicate detection | `OPENAI_API_KEY` | Per token |
| [Nominatim (OSM)](https://nominatim.org) | Primary geocoding | None | Free |
| [OpenCage](https://opencagedata.com) | Street-level geocoding fallback | `OPENCAGE_API_KEY` | Free 2,500/day |
| [Overpass API (OSM)](https://overpass-api.de) | Amenity/POI queries (parks, hospitals, buses) | None | Free |
| [Open Topo Data](https://www.opentopodata.org) | Elevation per listing | None | Free |
| [OpenRouteService](https://openrouteservice.org) | Walking directions to sea (optional) | `ORS_API_KEY` | Free 2,000/day |
| [Google Sheets API](https://developers.google.com/sheets) | Output spreadsheet | Service account JSON | Free |
| [Telegram Bot API](https://core.telegram.org/bots) | Price drop / high-score alerts | `TELEGRAM_BOT_TOKEN` | Free |

```bash
pip install requests beautifulsoup4 gspread google-auth openai pillow lxml
```

---

## Setup

### Environment variables

```bash
export ZENROWS_API_KEY="your_key"
export OPENAI_API_KEY="sk-..."
export OPENCAGE_API_KEY="your_key"
export TELEGRAM_BOT_TOKEN="bot_token:..."
export ORS_API_KEY="your_key"       # optional — better walk times
```

OpenClaw users: add to `openclaw.json` under `env`.

### Google Sheets service account

Place your service account JSON at:
```
~/.openclaw/credentials/google-service-account.json
```

Enable the Google Sheets API and share the spreadsheet with the service account email.

---

## Running

```bash
# Full daily pipeline (normally run by cron at 08:00 UTC)
python3 scraper/daily_update.py

# Image analysis only (batched, SIGTERM-safe)
python3 scraper/run_image_batch.py            # process next 10 unscored listings
python3 scraper/run_image_batch.py --status   # check how many pending

# Scoring only (re-score all with latest data, push to Sheets)
python3 scraper/enrich_listings.py

# Pipeline history
python3 scraper/pipeline_state.py --history

# Health check
python3 scraper/monitor.py

# Zone selector
python3 zone_selector.py --map
```

---

## Architecture Principles

1. **Single source of truth**: `enrich_location.py` exclusively owns `geocoded.json` enrichment fields. Other scripts read, never write enrichment data to it.

2. **Bounded execution**: `analyze_images.py` is called via `run_image_batch.py` which processes exactly 10 listings per invocation. No long-running processes that can be killed by system events.

3. **SIGTERM resilience**: All long-running scripts (`enrich_location.py`, `analyze_images.py`, `fetch_zenrows.py`, `enrich_commerce.py`, `geocode.py`) have signal handlers that checkpoint progress before exiting. Checkpoints every 3–10 records.

4. **State tracking**: `pipeline_state.json` is the authoritative record of what completed successfully. The daily pipeline uses this to skip already-done steps on partial failures.

5. **No hardcoded secrets**: All API keys and tokens are read from environment variables. Defaults (where present) are for development convenience only.

6. **Fail gracefully**: Every pipeline step is independent. A failure in step 5 does not prevent steps 6–12 from running. Failed steps are logged and reported in the daily Telegram summary.

---

## Output

Results are written to:

1. **`data/enriched_listings.json`** — full dataset, 82 fields per listing
2. **Google Sheets** — `Enriched_YYYY-MM-DD` tab (timestamped) + live `Enriched_v7` tab:

| Rank | Score | ID | Rooms | Size | Área Útil | €/mo | €/m² | Sea View | Image Score | Area Quality | Renovation | Feel | Red Flags | … | URL |
|------|-------|----|-------|------|-----------|------|------|----------|-------------|--------------|------------|------|-----------|---|-----|

3. **Web dashboard** — `ui/index.html` (auto-deployed to CDN)
4. **Zone selector** — `ui/zone_selector.html` (Leaflet.js interactive map)
