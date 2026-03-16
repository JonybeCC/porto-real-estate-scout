# Changelog — Porto Real Estate Scout 🦞

All notable changes to this project are documented here in reverse-chronological order.
Format: `[version/tag] YYYY-MM-DD — Summary`

---

## [v10] 2026-03-16 — Detached Execution & Architecture Hardening

**The big fix:** Pipeline was being killed by SIGTERM every morning because the cron
agent's `exec` tool timed out after ~10 minutes, killing the long-running subprocess.

### Added
- `run_pipeline.sh` — Launches pipeline with `nohup`, exits in <1 second. Lock file
  prevents double-runs. Sources `.env` before launching. Rotates previous log.
- `check_pipeline.sh` — Polls PID status; exit 0=done / 1=not started / 2=still running.
  Prints `pipeline_state.json` summary when finished.
- `.env` file loading in `daily_update.py` — API keys persisted independently of gateway
  config (which gets wiped on gateway updates). Load order: `.env` → `openclaw.json` → env.
- `PIPELINE_MAX_PAGES` env override — `PIPELINE_MAX_PAGES=1` for fast test runs.
- SIGTERM handler in `daily_update.py` — `finish()` always writes state even on kill.

### Fixed
- `run_script()` now returns `(bool, error_str)` — all step failures capture actual stderr.
  Previously every `error` field in pipeline state was empty string.
- `dedup.py` — `OpenAI(api_key=None)` crashed at import. Now `client=None` gracefully
  and `vision_compare()` returns early with `skipped_no_openai_key`.
- `dedup.py` — `fetch_photo_hashes()` was fetching 5 photos × 12s timeout per listing,
  sequentially, for all group listings. Pipeline could hang for hours. Fixed: cap at
  30 listings, 3 photos, 4s timeout. Worst case ~6 minutes.
- `enrich_location.py` — Overpass mirrors reordered. `maps.mail.ru` (0.4s) is now first;
  `overpass.private.coffee` and `kumi.systems` (both down) moved to end.
- `monitor.py` — Non-critical step failures (`enrich_location`, `image_analysis`) no longer
  mark the run unhealthy. Critical steps: `scrape`, `dedup`, `dom_tracker`, `enrich_listings`.

### Architecture sanity check: 17/17 checks pass

---

## [v9] 2026-03-13 — Dedup Hang Fix + Error Visibility

### Fixed
- `dedup.py` `fetch_photo_hashes()` photo fetch now bounded (30 listings × 3 photos × 4s).
- `daily_update.py` `run_script()` captures stderr and passes to `step_fail()`.
- `dedup.py` graceful client initialisation when `OPENAI_API_KEY` is missing.

### Added
- `OPENAI_API_KEY` added to `openclaw.json` env section (later superseded by `.env` file).

---

## [v8.5] 2026-03-03 — Location Enricher Timeout Increase

### Fixed
- `enrich_location.py` timeout increased from 600s to 1200s (20 min). With 130 listings
  and 4 concurrent Overpass workers, 600s was too tight.

---

## [v8.4] 2026-03-02 — Zone Selector + README Overhaul

### Added
- `zone_selector.py` — Fully decoded Idealista shape parameter (Google Polyline format,
  double-wrapped `(( ))`). CLI: `--decode`, `--decode-url`, `--encode`, `--map`,
  `--show`, `--geojson`, `--current`.
- `ui/zone_selector.html` — Interactive Leaflet.js map: click to draw polygon, drag
  vertices, live Idealista URL, export JSON/GeoJSON. Pre-loaded with current Porto zone.
- `ZONE_SELECTOR.md` — Encoding algorithm, CLI reference, 3 methods for changing zone.
- `README.md` — Rewritten: architecture diagram, scoring table (v8), dedup signal pipeline,
  API reference, architecture principles.
- Encoding verified: round-trip encode→decode→encode produces identical output.

---

## [v8.3] 2026-03-02 — Full Pipeline Overhaul (10 Improvements)

### Changed
- `daily_update.py` — Rebuilt as 12-step orchestrator with `PipelineRun` state tracking.
  Steps: scrape → dedup → geocode → fetch_zenrows → enrich_location → enrich_commerce →
  image_analysis → dom_tracker → enrich_listings → price_tracker → cleanup → monitor.
- Critical vs non-critical step classification. Non-critical failures don't block pipeline.
- Step results (new, total, errors) persisted to `pipeline_state.json` with 30-run history.
- `try/finally` wrapper ensures `finish()` always called.

### Added
- `pipeline_state.py` — `PipelineRun` class: `start()`, `step_ok()`, `step_fail()`,
  `step_skip()`, `finish()`. Stores last 30 runs. `--history` CLI flag.
- `monitor.py` — Health checks: pipeline freshness, listing count sanity, API key validity
  (ZenRows functional test, OpenAI, Google), data file freshness.

---

## [v8.2] 2026-03-02 — Code Hardening Audit

### Fixed
- Eliminated all bare `except:` clauses → specific exception types throughout.
- Removed all hardcoded API keys → environment variable reads only.
- Added SIGTERM handlers to long-running enrichment scripts with checkpoint saves.
- Timeout hardening: all `requests.get/post` calls have explicit timeouts.
- `geocode.py` — Added OpenCage fallback for street-level accuracy when Nominatim returns
  only neighbourhood-level results.

---

## [v8.1] 2026-03-02 — SIGTERM-Safe Image Analysis

### Added
- `run_image_batch.py` — SIGTERM-safe wrapper: runs `analyze_images.py --batch 10`.
  Exit 0 = all done, Exit 2 = more batches remaining. Safe for cron.
- `analyze_images.py --batch N` mode — processes N listings per invocation, checkpoints
  after each image group.

### Fixed
- `analyze_images.py --full` skips listings already at v5 schema; `--force` to override.

---

## [v8.0] 2026-03-02 — Dedup v3 + Image Analyzer v5

### Added
- `dedup.py` v3 — 5-signal detection pipeline:
  1. Reference code extraction (boilerplate-safe regex)
  2. Description similarity >80% (TF-IDF cosine, boilerplate-filtered)
  3. Composite structural key (rooms + area_util + wcs + floor)
  4. Photo pHash within plausible groups (same rooms, size ±10m², neighbourhood)
  5. GPT-4o-mini vision for ambiguous pairs
  Group-first approach: compares 57 pairs vs 8,256 naive cross-product.

- `analyze_images.py` v5 — Modular scoring:
  - Condition (1-10), Finish (Basic/Standard/Premium/Luxury)
  - Natural light (Poor/Good/Excellent), Area quality (1-10)
  - Renovation status (Original/Renovated/New Build)
  - Wide-angle flag (discounts area_quality if detected)
  - Solar direction from shadows/window orientation
  - Red flags: mould, damage, temporary fixtures

### Fixed
- Wide-angle detection now applies a modest discount (not disqualification).

---

## [v7.x] 2026-03-01 — Overpass v3 + Image Analyzer v4 (GitHub baseline)
_Commits: `dd58fde`, `288e556`_

### Changed
- `enrich_location.py` v3 — 4x concurrent Overpass workers (ThreadPoolExecutor).
  Multiple working mirrors with fallback. SIGTERM handler with checkpoint save.
- `analyze_images.py` v4 — `--full` mode scans all listings; `--id` for single listing.
  Improved prompt for wide-angle and solar detection.

---

## [v6.x] 2026-03-01 — v8.1 Scoring + Advanced Signals (GitHub)
_Commits: `7ef2dd8`, `8d78456`_

### Added
- `enrich_listings.py` v8.1 — Dynamic zone medians (live from dataset, not hardcoded).
  Advanced signals: amenity proximity, description parsing for furnished/AC/suite,
  street view facade scoring, supermarket tier (Lidl/Pingo Doce/El Corte Inglés).
- Staleness penalty: −2pts at 30 days, −4pts at 60 days.
- Re-listing detector v2: identifies same property returning at different price.
- Image scoring fallback: uses description signals when no photos available.
- Sun exposure: 'unknown' no longer scores 0 — uses description-based fallback.

---

## [v5.x] 2026-03-01 — UI + Sample Data (GitHub)
_Commit: `51a8905`_

### Added
- `ui/index.html` — Web dashboard: sortable table, map view, score histogram.
- Sample data for demo purposes.

---

## [v4.x] 2026-03-01 — Noise/Schools + UI Overhaul (GitHub)
_Commit: `9ba1d89`_

### Added
- `enrich_noise_schools.py` — Overpass queries for motorway/rail noise sources,
  school quality scoring (private/public, rating).
- `price_tracker.py` — Sends Telegram alerts on price drops ≥5%.
- `cleanup.py` — Removes confirmed-deleted listings.
- Relisting detector v1.
- UI overhaul: filter panel, price/score sorting.

---

## [v3.x] 2026-02-28 — Photo Recovery + Price Tracker (GitHub)
_Commit: `79267db`_

### Added
- Gallery-click photo recovery for Idealista's lazy-loaded images.
- Fail classification: distinguish Cloudflare block vs network timeout vs parse error.
- `unblurred_photos` fallback for image AI (uses full-res when available).

---

## [v2.x] 2026-02-27 — Elevation + Walk Times (GitHub)
_Commit: `b6c87ff`_

### Added
- `enrich_geo.py` — Elevation via Open Topo Data, walk time to sea via
  OpenRouteService (falls back to terrain-adjusted estimate).
- `geocode.py` — OpenCage API fallback for street-level accuracy.
- Walk time scoring integrated into opportunity score.

---

## [v1.0] 2026-02-27 — Initial Commit (GitHub)
_Commit: `deb68e9`_

### Added
- `scraper.py` — ZenRows-based Idealista.pt scraper, 10 pages, JSON output.
- `geocode.py` — Nominatim geocoding, sea distance calculation.
- `enrich_listings.py` — Opportunity score v7: zone, price, condition, image, amenities.
- `fetch_listing_details.py` — ZenRows detail page fetcher.
- `analyze_images.py` — GPT-4o vision scoring (condition, area quality).
- `push_to_sheets.py` — Google Sheets integration.
- `daily_update.py` — Initial orchestrator (sequential, no state tracking).
- `README.md` — Initial documentation.

---

## Architecture Evolution Summary

| Version | Key Milestone |
|---------|--------------|
| v1.0 | Working scraper + basic scoring to Google Sheets |
| v2.x | Elevation, walk times, street-level geocoding |
| v3.x | Photo recovery, price tracker |
| v4.x | Noise/school enrichment, relisting detection |
| v5.x | Web dashboard |
| v6.x | Dynamic scoring, advanced signals, staleness penalties |
| v7.x | Concurrent Overpass, image analysis v4 |
| v8.0 | Dedup v3 (5 signals), image v5 (modular), code audit |
| v8.3 | 12-step pipeline orchestrator, state tracking, health monitor |
| v8.4 | Zone selector tool, interactive map, README overhaul |
| v8.5 | Enricher timeout fix |
| v9 | Dedup hang fix, error visibility |
| v10 | **Detached execution** — pipeline no longer killed by cron agent timeout |
