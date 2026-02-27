# 🏠 Porto Real Estate Scout

Automated pipeline that scrapes, enriches, scores and ranks rental listings from [Idealista.pt](https://www.idealista.pt) for a defined geographic area of Porto. Results are pushed to Google Sheets with a ranked opportunity score.

---

## How It Works

Six scripts run in sequence. Each one enriches the data further before the final scorer combines everything into a single opportunity score per listing.

```
Idealista.pt
    │  (ZenRows — JS rendering + Cloudflare bypass)
    ▼
1. daily_update.py         → data/listings.json
   Scrapes all listing pages for the search URL/polygon.
   Extracts: price, size, rooms, floor, neighborhood, URL.

    │
    ▼
2. fetch_zenrows.py        → data/listing_details_zenrows.json
   Fetches each individual listing page.
   Extracts: full description, garage count, área útil,
             WCs, floor number, energy cert, photo URLs.

    │
    ▼
3. geocode.py              → data/geocoded.json
   Geocodes each address via Nominatim (OSM) — free, no key.
   Computes: lat/lng, distance to sea, distance to Foz center.

    │
    ▼
4. enrich_commerce.py      → data/commerce.json
   Queries Overpass API (OSM) for nearby amenities.
   Finds: nearest supermarket, pharmacy, beach, metro stop,
          restaurants within 300m, schools within 1km.

    │
    ▼
5. analyze_images.py       → data/image_analysis.json
   Sends listing photos to GPT-5.1 vision in batches of 3.
   Iterates until confidence is high or score stabilises (min 5 imgs).
   Returns: score/10, feel, finish, light quality, red flags,
            area impression, renovation status, summary.

    │
    ▼
6. enrich_listings.py      → data/enriched_listings.json
                           → Google Sheets (Enriched_v7 tab)
   Merges all data sources. Calculates opportunity score v7.
   Pushes ranked results to Google Sheets.
```

---

## Opportunity Score v7 (0–100)

Each listing is scored across 7 dimensions plus bonuses/penalties:

| Dimension | Max | Source |
|---|---|---|
| Condition & quality (AI image score × 2.5) | 25 | GPT-5.1 |
| Sun exposure (south/SE = 20, north = 0) | 20 | Description text |
| Zone desirability | 15 | Neighbourhood name |
| Practical features (garage, storage, elevator, terrace) | 18 | ZenRows |
| Floor level (RC = 0, 4th+ = 7, penthouse = 8) | 8 | ZenRows |
| Space efficiency (m² per room) | 7 | Calculated |
| Price vs zone average | 7 | Calculated |

**Bonuses:** sea view (+3), <500m to sea (+2), owner direct (+1), 60+ days on market (+1)

**Penalties:** AI-detected red flags (damp, dark, cheap fittings) = −2 each

### Zone Desirability Reference

```
Nevogilde / Pinhais da Foz   15  (top tier, walking distance to sea)
Foz do Douro / Foz Velha     13
Gondarém / Serralves         11
Pinheiro Manso               10
Massarelos / Boavista         9
Aldoar / Lordelo / Bessa      7
Aviz / Cedofeita / Bonfim     5
Ramalde / Campanhã            3–5
```

---

## APIs Used

| API | Purpose | Key Required | Cost |
|---|---|---|---|
| [ZenRows](https://www.zenrows.com) | Bypass Cloudflare / JS render Idealista | Yes | Paid |
| [OpenAI GPT-5.1](https://platform.openai.com) | Vision model — image condition scoring | Yes | Per token |
| [Nominatim (OSM)](https://nominatim.org) | Address → lat/lng geocoding | No | Free |
| [Overpass API (OSM)](https://overpass-api.de) | Nearby amenities query | No | Free |
| [Google Sheets API](https://developers.google.com/sheets) | Push results to spreadsheet | Service account | Free |

### Potential Upgrades (from [public-apis](https://github.com/public-apis/public-apis))

Free APIs that could improve accuracy with zero cost:

- **[OpenCage](https://opencagedata.com)** — better geocoding for Portuguese addresses, 2,500 free/day. Drop-in replacement for Nominatim in `geocode.py`.
- **[Geokeo](https://geokeo.com)** — free geocoding, 2,500/day, no key needed. Alternative Nominatim fallback.
- **[Transport for Lisbon](https://emel.city-platform.com/opendata/)** — real bus route/stop data. Add to `enrich_commerce.py` to count bus stops within 500m.
- **[Teleport API](https://developers.teleport.org/)** — free quality of life scores by city/urban area. Could add Porto-level scores (safety, cost of living, commute) as a scoring dimension.
- **[openrouteservice.org](https://openrouteservice.org/)** — free isochrone API (walk/bike/transit reach in X minutes). Could replace straight-line distance-to-sea with actual walk time.
- **[Open Topo Data](https://www.opentopodata.org)** — free elevation API. Could add floor/hill context — useful for Porto's hilly terrain.

---

## Setup

### Requirements

```bash
pip install requests beautifulsoup4 gspread google-auth openai
```

### Environment Variables

```bash
export OPENAI_API_KEY="sk-proj-..."
```

### Credentials

Place your Google service account JSON at:
```
credentials/google-service-account.json
```

Enable the Google Sheets API and share the target spreadsheet with the service account email.

---

## Running the Pipeline

### Full run (from scratch)

```bash
python3 scraper/daily_update.py       # ~2-5 min depending on pages
python3 scraper/fetch_zenrows.py      # ~15-20 min for 100+ listings
python3 scraper/geocode.py            # ~5 min
python3 scraper/enrich_commerce.py    # ~10 min (Overpass queries)
python3 scraper/analyze_images.py     # ~30-40 min (GPT-5.1 vision)
python3 scraper/enrich_listings.py    # ~1 min + Sheets push
```

### Incremental update (daily)

Each script resumes from where it left off — already-processed listings are skipped automatically.

```bash
python3 scraper/daily_update.py       # picks up new listings
python3 scraper/fetch_zenrows.py      # fetches only new ones
python3 scraper/analyze_images.py     # scores only unscored ones
python3 scraper/enrich_listings.py    # always re-scores all with latest data
```

---

## Search Area

The scraper targets a geographic polygon on Idealista. Update `SHAPE` and `BASE_URL` in `daily_update.py` to change the area.

Current search:
- **Types:** T2, T3 apartments
- **Price:** €1,650 – €3,100/month
- **Zone:** Foz do Douro, Aldoar, Nevogilde, Boavista, Pinheiro Manso, Lordelo do Ouro

```
https://www.idealista.pt/areas/arrendar-casas/
com-preco-max_3100,preco-min_1650,t2,t3/
?shape=((ovgzFrr`t@n`@omEsWm[zGe|@vc@oUjiA`@fHdfFciCjuA))
```

---

## Output

Results land in two places:

1. **`data/enriched_listings.json`** — full dataset with all fields
2. **Google Sheets** (`Enriched_v7` tab) — ranked table with columns:

```
Rank | Score v7 | ID | Rooms | Size m² | Área Útil | Space/Room | WCs |
Price/mo | €/m² | Price Δ% | Days on Market | Neighbourhood | Floor |
Sun Exposure | Sea View | Dist Sea | Garage | Parking | Storage |
Elevator | Outdoor | Owner Direct | Image Score | Renovation | Feel |
Finish | Light | Red Flags | Image Summary | Supermarket | Supermarket km |
Pharmacy km | Restaurants 300m | Metro km | Beach km | Schools 1km |
Commerce Notes | Condition Summary | URL
```

---

## File Reference

| File | Role |
|---|---|
| `scraper/daily_update.py` | Step 1 — scrape listing index pages |
| `scraper/fetch_zenrows.py` | Step 2 — fetch individual listing detail pages |
| `scraper/geocode.py` | Step 3 — geocode addresses |
| `scraper/enrich_commerce.py` | Step 4 — fetch nearby amenities via Overpass |
| `scraper/analyze_images.py` | Step 5 — GPT-5.1 iterative image analysis |
| `scraper/enrich_listings.py` | Step 6 — merge all data + score + push to Sheets |
| `scraper/dom_tracker.py` | Track days-on-market per listing across runs |
| `scraper/dedup.py` | Deduplication utility |
| `scraper/monitor.py` | Price change monitoring |
| `data/listings.json` | Raw scraped listings (source of truth) |
| `data/listing_details_zenrows.json` | ZenRows detail data per listing |
| `data/geocoded.json` | Geocoded coordinates |
| `data/commerce.json` | Nearby amenity data |
| `data/image_analysis.json` | GPT-5.1 scores |
| `data/enriched_listings.json` | Final merged + scored output |
| `data/dom_tracker.json` | Days-on-market history |
