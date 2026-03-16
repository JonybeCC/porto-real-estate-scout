# Zone Selector — Drawing Custom Search Areas for Idealista.pt

## How the Shape Parameter Works

Idealista.pt encodes the search polygon as a **Google Polyline string**, double-wrapped in parentheses and URL-encoded:

```
https://www.idealista.pt/areas/arrendar-casas/
  com-preco-max_3100,preco-min_1650,t2,t3/
  ?shape=%28%28ovgzFrr%60t%40n%60%40omEsWm%5BzGe%7C%40vc%40oUjiA%60%40fHdfFciCjuA%29%29
```

After URL-decoding:
```
shape=((ovgzFrr`t@n`@omEsWm[zGe|@vc@oUjiA`@fHdfFciCjuA))
```

The inner string `ovgzFrr\`t@n\`@omEsWm[zGe|@vc@oUjiA\`@fHdfFciCjuA` is a standard **Google Polyline** encoding of the polygon vertices.

---

## Google Polyline Encoding

The encoding algorithm:
1. Multiply lat/lng by `1e5` and round to integer
2. Compute delta from previous point (the encoding is delta-based, not absolute)
3. Left-shift by 1; if negative, invert bits (zig-zag encoding)
4. Split into 5-bit chunks from LSB; add `0x20` (continuation bit) to all but last
5. Add 63 to each chunk → ASCII character

The **closing point** (repeat of first coordinate) must be included in the encoding.

### Example: Current Porto Foz Zone

```
Encoded: ovgzFrr`t@n`@omEsWm[zGe|@vc@oUjiA`@fHdfFciCjuA

Decodes to 8 points (7 unique + closing):
  [1] 41.168560, -8.691780   ← NW corner (coast)
  [2] 41.163200, -8.658740   ← N mid (Nevogilde)
  [3] 41.167140, -8.654190   ← NE (Boavista corridor)
  [4] 41.165720, -8.644400   ← E (Bessa/Pinheiro Manso)
  [5] 41.159840, -8.640800   ← SE border
  [6] 41.147940, -8.640970   ← S mid
  [7] 41.146460, -8.677960   ← SW (Aldoar south)
  [8] 41.168560, -8.691780   ← closing point (= point 1)

Zone size: ~4.3km wide × 2.5km tall
```

---

## Interactive Map Tool

The easiest way to create a new zone:

```bash
python3 zone_selector.py --map
```

This opens a **Leaflet.js map** in your browser where you can:
- Click to place polygon vertices
- Drag markers to adjust positions
- See the live-generated Idealista URL as you draw
- Set price/type filters
- Copy the URL or export coordinates as JSON/GeoJSON

The current zone is pre-loaded so you can start from it and adjust.

---

## CLI Reference

```bash
# Show current zone (coordinates + URL)
python3 zone_selector.py --current

# Decode any shape parameter to coordinates
python3 zone_selector.py --decode "((ovgzFrr\`t@n\`@omEsWm[zGe|@vc@oUjiA\`@fHdfFciCjuA))"

# Decode from a full Idealista URL
python3 zone_selector.py --decode-url "https://www.idealista.pt/...?shape=..."

# Encode from a JSON coordinates file → print URL
python3 zone_selector.py --encode my_zone.json --price-min 1650 --price-max 3100 --types t2,t3

# Export current zone as GeoJSON
python3 zone_selector.py --geojson

# Open interactive map (pre-populated with current zone)
python3 zone_selector.py --map

# Open current zone on map (view-only)
python3 zone_selector.py --show
```

---

## Changing the Search Zone

### Method 1: Interactive Map (recommended)

1. Run `python3 zone_selector.py --map`
2. Clear the existing polygon (🗑 Clear button)
3. Click on the map to draw your new zone (at least 3 points)
4. Adjust filters (price range, apartment types)
5. Copy the generated URL or use "💾 Export JSON" to save coordinates

### Method 2: Manual Coordinates

1. Find coordinates for your desired zone (e.g. from Google Maps)
2. Save them as a JSON file: `[[lat1, lng1], [lat2, lng2], ...]`
3. Run: `python3 zone_selector.py --encode my_zone.json`
4. The script will print the new `SHAPE` and `BASE_URL` values for `daily_update.py`

### Method 3: GeoJSON Import

If you have a GeoJSON polygon (e.g. from QGIS, geojson.io):
1. Extract the coordinates array from the `geometry.coordinates[0]` field
2. Convert from GeoJSON `[lng, lat]` to Python `[lat, lng]` order
3. Save as JSON and use Method 2

---

## Applying a New Zone to the Pipeline

After generating your new shape, update `scraper/daily_update.py`:

```python
# Replace these two constants near the top of daily_update.py:
SHAPE    = 'your_new_url_encoded_shape_here'
BASE_URL = f'https://www.idealista.pt/areas/arrendar-casas/com-preco-max_3100,preco-min_1650,t2,t3/?shape={SHAPE}'
```

Also update `CURRENT_ZONE` and `CURRENT_ENCODED` in `zone_selector.py` to keep things in sync.

---

## GeoJSON Integration

The tool can export and import GeoJSON, making it compatible with:

- **Google Maps** — import GeoJSON layer via "My Maps"
- **QGIS** — open as vector layer for visual editing
- **geojson.io** — paste and view/edit online
- **OpenLayers / Mapbox** — use as a filter polygon in web maps

```bash
python3 zone_selector.py --geojson
# Writes: data/current_zone.geojson
```

---

## Multiple Zones / City Support

`zone_selector.py` supports any Portuguese city via the `--types` and city path parameter. To search in Lisbon:

```bash
python3 zone_selector.py --encode lisbon_zone.json \
  --price-min 1800 --price-max 4000 --types t2,t3
```

The script will output a URL with `arrendar-casas` which works across all Idealista.pt cities.

To run the full pipeline on a different zone, create a copy of `daily_update.py` with updated `SHAPE`/`BASE_URL` constants, or pass the URL as a CLI argument (future enhancement).
