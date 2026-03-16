#!/usr/bin/env python3
"""
Zone Selector for Idealista.pt search URLs — JBizz Assistant 🦞

The Idealista search area is defined by a custom zone polygon encoded using
Google Polyline format, double-wrapped in parentheses:

  shape=((encoded_polyline))

where `encoded_polyline` is standard Google Polyline encoding:
  - Coordinates are integers (lat/lng × 1e5, rounded)
  - Delta-encoded (each point is the diff from the previous)
  - Each delta is zig-zag encoded, then split into 5-bit chunks
  - Each chunk is ASCII-offset by 63 and OR'd with 0x20 for continuation

The closing coordinate must repeat the first point to close the polygon.

Usage:
  # Decode an existing shape URL to coordinates:
  python3 zone_selector.py --decode "((ovgzFrr\`t@n\`@omEsWm[zGe|@vc@oUjiA\`@fHdfFciCjuA))"

  # Decode from a full URL:
  python3 zone_selector.py --decode-url "https://www.idealista.pt/areas/arrendar-casas/...?shape=..."

  # Encode coordinates from a JSON file and generate the search URL:
  python3 zone_selector.py --encode my_zone.json --price-min 1650 --price-max 3100 --types t2,t3

  # Open an interactive map to draw a new zone:
  python3 zone_selector.py --map

  # Show the current zone on a static map:
  python3 zone_selector.py --show

  # Export current zone to GeoJSON:
  python3 zone_selector.py --geojson

JSON format for --encode:
  [[lat1, lng1], [lat2, lng2], ...]   (do NOT repeat the closing point — added automatically)

Example:
  [
    [41.168560, -8.691780],
    [41.163200, -8.658740],
    [41.167140, -8.654190]
  ]
"""

import json
import math
import os
import sys
import urllib.parse
import webbrowser
from datetime import datetime

# ── Current zone definition ───────────────────────────────────────────────────
# Porto Foz/Aldoar/Nevogilde zone — the search area used by the daily pipeline.
# Edit this list to change the zone, then run --encode to regenerate BASE_URL
# in daily_update.py. The closing point is auto-added during encoding.
CURRENT_ZONE = [
    (41.168560, -8.691780),  # NW corner — coast near Matosinhos border
    (41.163200, -8.658740),  # N mid — Nevogilde north
    (41.167140, -8.654190),  # NE — Boavista corridor
    (41.165720, -8.644400),  # E mid — Boavista / Pinheiro Manso
    (41.159840, -8.640800),  # SE — Bessa / Massarelos border
    (41.147940, -8.640970),  # S mid — Ramalde south
    (41.146460, -8.677960),  # SW — Aldoar south / coast approach
]

CURRENT_ENCODED = 'ovgzFrr`t@n`@omEsWm[zGe|@vc@oUjiA`@fHdfFciCjuA'


# ── Polyline encoding / decoding ──────────────────────────────────────────────

def _encode_value(value: float, precision: int = 5) -> str:
    """Encode a single lat or lng delta as a Google Polyline string."""
    v = int(round(value * (10 ** precision)))
    v = v << 1
    if v < 0:
        v = ~v
    chunks = []
    while v >= 0x20:
        chunks.append(chr((0x20 | (v & 0x1f)) + 63))
        v >>= 5
    chunks.append(chr(v + 63))
    return ''.join(chunks)


def encode_polyline(coords: list[tuple[float, float]]) -> str:
    """
    Encode a list of (lat, lng) tuples to Google Polyline format.
    The closing point (repeat of first) is appended automatically.

    Args:
        coords: List of (lat, lng) tuples. Do NOT include closing point.

    Returns:
        Encoded polyline string (without outer parentheses).
    """
    if not coords:
        raise ValueError('coords must not be empty')

    # Auto-close polygon
    closed = list(coords)
    if closed[-1] != closed[0]:
        closed.append(closed[0])

    result = []
    prev_lat, prev_lng = 0, 0
    for lat, lng in closed:
        result.append(_encode_value(lat - prev_lat))
        result.append(_encode_value(lng - prev_lng))
        prev_lat, prev_lng = lat, lng
    return ''.join(result)


def decode_polyline(polyline_str: str) -> list[tuple[float, float]]:
    """
    Decode a Google Polyline string to a list of (lat, lng) tuples.
    The closing duplicate coordinate is retained.

    Args:
        polyline_str: Encoded polyline string (without outer parentheses).

    Returns:
        List of (lat, lng) tuples.
    """
    index, lat, lng = 0, 0, 0
    coordinates = []
    while index < len(polyline_str):
        # Decode latitude delta
        result, shift = 0, 0
        while True:
            b = ord(polyline_str[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        lat += (~(result >> 1) if result & 1 else result >> 1)

        # Decode longitude delta
        result, shift = 0, 0
        while True:
            b = ord(polyline_str[index]) - 63
            index += 1
            result |= (b & 0x1f) << shift
            shift += 5
            if b < 0x20:
                break
        lng += (~(result >> 1) if result & 1 else result >> 1)

        coordinates.append((lat / 1e5, lng / 1e5))
    return coordinates


def shape_param_to_coords(shape: str) -> list[tuple[float, float]]:
    """
    Parse the Idealista shape parameter value (raw or URL-encoded) to coordinates.

    Args:
        shape: The shape parameter value, e.g. '((ovgzFrr...))' or URL-encoded.

    Returns:
        List of (lat, lng) tuples (including closing point).
    """
    # URL-decode if needed
    decoded = urllib.parse.unquote(shape)
    # Strip outer double parentheses: ((...)) → ...
    inner = decoded.strip().lstrip('(').rstrip(')')
    return decode_polyline(inner)


def coords_to_shape_param(coords: list[tuple[float, float]]) -> str:
    """
    Encode coordinates to the Idealista shape parameter value (URL-encoded).

    Args:
        coords: List of (lat, lng) tuples (closing point added automatically).

    Returns:
        URL-encoded shape parameter, e.g. '%28%28ovgzFrr...%29%29'
    """
    encoded = encode_polyline(coords)
    raw     = f'(({encoded}))'
    return urllib.parse.quote(raw, safe='')


def build_url(
    coords: list[tuple[float, float]],
    price_min: int = 1650,
    price_max: int = 3100,
    types: str = 't2,t3',
    city: str = 'arrendar-casas',
) -> str:
    """
    Build a full Idealista search URL from zone coordinates and filters.

    Args:
        coords:    List of (lat, lng) polygon vertices.
        price_min: Minimum monthly rent in EUR.
        price_max: Maximum monthly rent in EUR.
        types:     Comma-separated apartment types, e.g. 't2,t3'.
        city:      Idealista city path segment.

    Returns:
        Full Idealista search URL.
    """
    price_part = f'com-preco-max_{price_max},preco-min_{price_min}'
    type_part  = ','.join(types.split(','))
    filters    = f'{price_part},{type_part}'
    shape      = coords_to_shape_param(coords)
    return f'https://www.idealista.pt/areas/{city}/{filters}/?shape={shape}'


# ── Utility: GeoJSON export ───────────────────────────────────────────────────

def to_geojson(coords: list[tuple[float, float]], name: str = 'Zone') -> dict:
    """
    Convert coordinates to a GeoJSON FeatureCollection.
    Coordinates are [lng, lat] as per GeoJSON spec.

    Args:
        coords: List of (lat, lng) tuples.
        name:   Feature name property.

    Returns:
        GeoJSON FeatureCollection dict.
    """
    # GeoJSON uses [lng, lat] order and requires closed ring
    ring = [[lng, lat] for lat, lng in coords]
    if ring[-1] != ring[0]:
        ring.append(ring[0])
    return {
        'type': 'FeatureCollection',
        'features': [{
            'type': 'Feature',
            'properties': {'name': name, 'encoded': encode_polyline(coords)},
            'geometry': {'type': 'Polygon', 'coordinates': [ring]},
        }]
    }


def zone_stats(coords: list[tuple[float, float]]) -> dict:
    """Return basic stats about the zone polygon."""
    lats = [c[0] for c in coords]
    lngs = [c[1] for c in coords]
    center_lat = (max(lats) + min(lats)) / 2
    center_lng = (max(lngs) + min(lngs)) / 2
    width_km   = (max(lngs) - min(lngs)) * 111 * math.cos(math.radians(center_lat))
    height_km  = (max(lats) - min(lats)) * 111
    return {
        'center':    (round(center_lat, 5), round(center_lng, 5)),
        'bounds':    {'min_lat': min(lats), 'max_lat': max(lats),
                      'min_lng': min(lngs), 'max_lng': max(lngs)},
        'size_km':   (round(width_km, 1), round(height_km, 1)),
        'n_vertices': len(coords),
    }


# ── Interactive HTML map ──────────────────────────────────────────────────────

def generate_map_html(existing_coords: list[tuple[float, float]] | None = None) -> str:
    """
    Generate an interactive Leaflet.js map for drawing or viewing a zone polygon.

    Features:
    - Draw a polygon on the map by clicking to place vertices
    - Edit existing polygon by dragging points
    - Live preview of the Idealista URL and encoded shape
    - Export coordinates as JSON
    - Undo last point
    - Copy URL to clipboard

    Args:
        existing_coords: Optional existing polygon to pre-populate.

    Returns:
        HTML string ready to write to a file.
    """
    # Pre-populate JSON if existing coords given
    existing_json = 'null'
    if existing_coords:
        # Exclude closing point for the editor
        pts = existing_coords[:-1] if existing_coords[-1] == existing_coords[0] else existing_coords
        existing_json = json.dumps([[lat, lng] for lat, lng in pts])

    center_lat = 41.158 if not existing_coords else sum(c[0] for c in existing_coords) / len(existing_coords)
    center_lng = -8.665 if not existing_coords else sum(c[1] for c in existing_coords) / len(existing_coords)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Idealista Zone Selector — JBizz 🦞</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
  <style>
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0f0f0f; color: #e0e0e0; display: flex; height: 100vh; }}
    #map {{ flex: 1; }}
    #panel {{ width: 380px; background: #1a1a1a; padding: 20px; overflow-y: auto; display: flex; flex-direction: column; gap: 16px; border-left: 1px solid #333; }}
    h1 {{ font-size: 18px; color: #ff6b35; }}
    h2 {{ font-size: 13px; color: #888; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }}
    .card {{ background: #222; border-radius: 8px; padding: 14px; }}
    .instructions {{ font-size: 13px; line-height: 1.6; color: #aaa; }}
    .instructions li {{ margin-left: 16px; }}
    #coords-list {{ font-size: 12px; font-family: monospace; color: #7ec8e3; max-height: 160px; overflow-y: auto; }}
    .coord-item {{ padding: 2px 0; border-bottom: 1px solid #2a2a2a; display: flex; justify-content: space-between; align-items: center; }}
    .coord-item button {{ background: #333; border: none; color: #ff6b35; cursor: pointer; font-size: 10px; padding: 2px 6px; border-radius: 3px; }}
    #url-output {{ font-size: 11px; font-family: monospace; color: #7ec8e3; word-break: break-all; background: #111; padding: 10px; border-radius: 6px; min-height: 60px; }}
    #encoded-output {{ font-size: 11px; font-family: monospace; color: #a8d8a8; background: #111; padding: 8px; border-radius: 6px; word-break: break-all; }}
    .btn {{ padding: 9px 14px; border: none; border-radius: 6px; cursor: pointer; font-size: 13px; font-weight: 500; width: 100%; }}
    .btn-primary {{ background: #ff6b35; color: white; }}
    .btn-primary:hover {{ background: #ff8855; }}
    .btn-secondary {{ background: #333; color: #e0e0e0; }}
    .btn-secondary:hover {{ background: #444; }}
    .btn-danger {{ background: #8b0000; color: white; }}
    .btn-danger:hover {{ background: #aa0000; }}
    .btn-group {{ display: flex; gap: 8px; }}
    .btn-group .btn {{ flex: 1; }}
    #stats {{ font-size: 12px; color: #aaa; line-height: 1.7; }}
    .stat-row {{ display: flex; justify-content: space-between; }}
    .stat-val {{ color: #e0e0e0; font-family: monospace; }}
    #status {{ font-size: 12px; padding: 8px 12px; border-radius: 6px; background: #1e3a2a; color: #7ec8e3; text-align: center; }}
    select, input[type=number] {{ background: #2a2a2a; border: 1px solid #444; color: #e0e0e0; padding: 6px 10px; border-radius: 5px; font-size: 13px; width: 100%; }}
    .filter-row {{ display: flex; gap: 8px; align-items: center; }}
    .filter-row label {{ font-size: 12px; color: #888; white-space: nowrap; }}
    textarea {{ background: #111; border: 1px solid #333; color: #e0e0e0; padding: 8px; border-radius: 6px; font-family: monospace; font-size: 11px; width: 100%; resize: vertical; }}
  </style>
</head>
<body>
<div id="map"></div>
<div id="panel">
  <div>
    <h1>🦞 Zone Selector</h1>
    <p style="font-size:12px;color:#666;margin-top:4px">Idealista.pt · Porto Real Estate Scout</p>
  </div>

  <div class="card">
    <h2>How to use</h2>
    <ul class="instructions">
      <li>Click on the map to add polygon vertices</li>
      <li>Drag markers to adjust</li>
      <li>Need ≥3 points for a valid zone</li>
      <li>Polygon closes automatically</li>
    </ul>
  </div>

  <div class="card">
    <h2>Search filters</h2>
    <div class="filter-row" style="margin-bottom:8px">
      <label>Min €</label>
      <input type="number" id="price-min" value="1650" min="0" max="10000" step="50">
      <label>Max €</label>
      <input type="number" id="price-max" value="3100" min="0" max="10000" step="50">
    </div>
    <div class="filter-row">
      <label>Types</label>
      <input type="text" id="apt-types" value="t2,t3" style="background:#2a2a2a;border:1px solid #444;color:#e0e0e0;padding:6px 10px;border-radius:5px;font-size:13px;flex:1">
    </div>
  </div>

  <div class="card">
    <h2>Vertices (<span id="coord-count">0</span>)</h2>
    <div id="coords-list"></div>
  </div>

  <div class="card">
    <h2>Zone stats</h2>
    <div id="stats">—</div>
  </div>

  <div class="card">
    <h2>Encoded shape</h2>
    <div id="encoded-output">—</div>
  </div>

  <div class="card">
    <h2>Search URL</h2>
    <div id="url-output">Add at least 3 points to generate URL</div>
  </div>

  <div class="btn-group">
    <button class="btn btn-primary" onclick="copyUrl()">📋 Copy URL</button>
    <button class="btn btn-primary" onclick="openUrl()">🔗 Open</button>
  </div>

  <div class="btn-group">
    <button class="btn btn-secondary" onclick="exportJson()">💾 Export JSON</button>
    <button class="btn btn-secondary" onclick="exportGeojson()">📦 GeoJSON</button>
  </div>

  <div>
    <h2 style="margin-bottom:8px">Import coordinates (JSON)</h2>
    <textarea id="import-text" rows="4" placeholder='[[41.168, -8.691], [41.163, -8.658], ...]'></textarea>
    <button class="btn btn-secondary" onclick="importCoords()" style="margin-top:6px">Import</button>
  </div>

  <div class="btn-group">
    <button class="btn btn-secondary" onclick="undoLast()">↩ Undo</button>
    <button class="btn btn-danger" onclick="clearAll()">🗑 Clear</button>
  </div>

  <div id="status">Click on the map to start drawing</div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
// ── State ────────────────────────────────────────────────────────────────────
const state = {{ points: [], markers: [], polygon: null }};
const EXISTING = {existing_json};
const CENTER   = [{center_lat}, {center_lng}];

// ── Map init ─────────────────────────────────────────────────────────────────
const map = L.map('map').setView(CENTER, 13);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
    attribution: '© OpenStreetMap contributors'
}}).addTo(map);
L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{}});

const markerIcon = L.divIcon({{
    className: '',
    html: '<div style="width:12px;height:12px;background:#ff6b35;border:2px solid white;border-radius:50%;cursor:move"></div>',
    iconSize: [12, 12], iconAnchor: [6, 6]
}});

// ── Polyline encode (JS mirror of Python) ────────────────────────────────────
function encodeValue(v) {{
    v = Math.round(v * 1e5);
    v = v << 1;
    if (v < 0) v = ~v;
    let result = '';
    while (v >= 0x20) {{
        result += String.fromCharCode((0x20 | (v & 0x1f)) + 63);
        v >>= 5;
    }}
    return result + String.fromCharCode(v + 63);
}}

function encodePolyline(pts) {{
    let result = '', prevLat = 0, prevLng = 0;
    const closed = [...pts];
    if (closed.length > 0 && (closed[0][0] !== closed[closed.length-1][0] || closed[0][1] !== closed[closed.length-1][1])) {{
        closed.push(closed[0]);
    }}
    for (const [lat, lng] of closed) {{
        result += encodeValue(lat - prevLat) + encodeValue(lng - prevLng);
        prevLat = lat; prevLng = lng;
    }}
    return result;
}}

// ── UI update ─────────────────────────────────────────────────────────────────
function update() {{
    const pts = state.points;
    document.getElementById('coord-count').textContent = pts.length;

    // Coords list
    const list = document.getElementById('coords-list');
    list.innerHTML = pts.map((p, i) =>
        `<div class="coord-item">
            <span>[${{i+1}}] ${{p[0].toFixed(5)}}, ${{p[1].toFixed(5)}}</span>
            <button onclick="removePoint(${{i}})">✕</button>
         </div>`
    ).join('');

    // Remove old polygon
    if (state.polygon) {{ map.removeLayer(state.polygon); state.polygon = null; }}

    if (pts.length < 2) {{
        document.getElementById('encoded-output').textContent = '—';
        document.getElementById('url-output').textContent = 'Add at least 3 points to generate URL';
        document.getElementById('stats').textContent = '—';
        return;
    }}

    // Draw polygon
    const latlngs = pts.map(p => [p[0], p[1]]);
    state.polygon = L.polygon(latlngs, {{ color: '#ff6b35', fillOpacity: 0.15, weight: 2 }}).addTo(map);

    if (pts.length < 3) return;

    // Encode
    const encoded = encodePolyline(pts);
    document.getElementById('encoded-output').textContent = encoded;

    // URL
    const priceMin = document.getElementById('price-min').value;
    const priceMax = document.getElementById('price-max').value;
    const types = document.getElementById('apt-types').value;
    const shape = '((' + encoded + '))';
    const shapeEnc = encodeURIComponent(shape);
    const url = `https://www.idealista.pt/areas/arrendar-casas/com-preco-max_${{priceMax}},preco-min_${{priceMin}},${{types}}/?shape=${{shapeEnc}}`;
    document.getElementById('url-output').textContent = url;

    // Stats
    const lats = pts.map(p => p[0]), lngs = pts.map(p => p[1]);
    const cLat = (Math.max(...lats) + Math.min(...lats)) / 2;
    const cLng = (Math.max(...lngs) + Math.min(...lngs)) / 2;
    const wKm  = (Math.max(...lngs) - Math.min(...lngs)) * 111 * Math.cos(cLat * Math.PI/180);
    const hKm  = (Math.max(...lats) - Math.min(...lats)) * 111;
    document.getElementById('stats').innerHTML = `
        <div class="stat-row"><span>Center</span><span class="stat-val">${{cLat.toFixed(4)}}, ${{cLng.toFixed(4)}}</span></div>
        <div class="stat-row"><span>Size</span><span class="stat-val">${{wKm.toFixed(1)}}km × ${{hKm.toFixed(1)}}km</span></div>
        <div class="stat-row"><span>Vertices</span><span class="stat-val">${{pts.length}}</span></div>
    `;
}}

// ── Map click handler ─────────────────────────────────────────────────────────
map.on('click', e => {{
    const pt = [e.latlng.lat, e.latlng.lng];
    state.points.push(pt);
    const marker = L.marker(pt, {{ icon: markerIcon, draggable: true }})
        .addTo(map)
        .on('drag', ev => {{
            const idx = state.markers.indexOf(marker);
            if (idx !== -1) state.points[idx] = [ev.latlng.lat, ev.latlng.lng];
            update();
        }});
    state.markers.push(marker);
    update();
    document.getElementById('status').textContent = `${{state.points.length}} point(s) placed`;
}});

// ── Actions ──────────────────────────────────────────────────────────────────
function removePoint(i) {{
    state.points.splice(i, 1);
    map.removeLayer(state.markers[i]);
    state.markers.splice(i, 1);
    update();
}}

function undoLast() {{
    if (!state.points.length) return;
    const i = state.points.length - 1;
    state.points.pop();
    map.removeLayer(state.markers[i]);
    state.markers.pop();
    update();
}}

function clearAll() {{
    state.points = [];
    state.markers.forEach(m => map.removeLayer(m));
    state.markers = [];
    if (state.polygon) {{ map.removeLayer(state.polygon); state.polygon = null; }}
    update();
    document.getElementById('status').textContent = 'Cleared — click to start';
}}

function copyUrl() {{
    const url = document.getElementById('url-output').textContent;
    if (url.startsWith('Add')) return;
    navigator.clipboard.writeText(url).then(() => {{
        document.getElementById('status').textContent = '✅ URL copied to clipboard';
    }});
}}

function openUrl() {{
    const url = document.getElementById('url-output').textContent;
    if (!url.startsWith('Add')) window.open(url, '_blank');
}}

function exportJson() {{
    const data = JSON.stringify(state.points, null, 2);
    download('zone_coords.json', data);
}}

function exportGeojson() {{
    if (state.points.length < 3) return;
    const ring = [...state.points, state.points[0]].map(p => [p[1], p[0]]);
    const geojson = {{
        type: 'FeatureCollection',
        features: [{{
            type: 'Feature',
            properties: {{ name: 'Custom Zone', encoded: encodePolyline(state.points) }},
            geometry: {{ type: 'Polygon', coordinates: [ring] }}
        }}]
    }};
    download('zone.geojson', JSON.stringify(geojson, null, 2));
}}

function importCoords() {{
    try {{
        const pts = JSON.parse(document.getElementById('import-text').value);
        clearAll();
        for (const p of pts) {{
            const pt = [p[0], p[1]];
            state.points.push(pt);
            const marker = L.marker(pt, {{ icon: markerIcon, draggable: true }})
                .addTo(map)
                .on('drag', ev => {{
                    const idx = state.markers.indexOf(marker);
                    if (idx !== -1) state.points[idx] = [ev.latlng.lat, ev.latlng.lng];
                    update();
                }});
            state.markers.push(marker);
        }}
        if (pts.length) map.fitBounds(state.markers.map(m => m.getLatLng()));
        update();
        document.getElementById('status').textContent = `Imported ${{pts.length}} points`;
    }} catch(e) {{
        document.getElementById('status').textContent = '❌ Invalid JSON: ' + e.message;
    }}
}}

function download(filename, content) {{
    const a = document.createElement('a');
    a.href = 'data:text/plain;charset=utf-8,' + encodeURIComponent(content);
    a.download = filename;
    a.click();
}}

// ── Pre-populate with existing zone if provided ───────────────────────────────
if (EXISTING) {{
    document.getElementById('import-text').value = JSON.stringify(EXISTING, null, 2);
    importCoords();
}}

// Update URL when filters change
document.getElementById('price-min').addEventListener('input', update);
document.getElementById('price-max').addEventListener('input', update);
document.getElementById('apt-types').addEventListener('input', update);
</script>
</body>
</html>'''


# ── CLI ───────────────────────────────────────────────────────────────────────

def cmd_decode(shape: str):
    """Decode a shape parameter and print coordinates."""
    coords = shape_param_to_coords(shape)
    stats  = zone_stats(coords)
    print(f'\n📍 Decoded {len(coords)} coordinates (including closing point):')
    for i, (lat, lng) in enumerate(coords):
        label = ' ← closing' if i == len(coords) - 1 and coords[0] == coords[-1] else ''
        print(f'  [{i+1:2d}] lat={lat:.6f}, lng={lng:.6f}{label}')
    print(f'\n📐 Zone stats:')
    print(f'  Center:    {stats["center"][0]:.5f}, {stats["center"][1]:.5f}')
    print(f'  Size:      {stats["size_km"][0]}km × {stats["size_km"][1]}km')
    print(f'  Vertices:  {stats["n_vertices"]} (including closing point)')
    # GeoJSON
    geo = to_geojson(coords)
    print(f'\n📦 GeoJSON:\n{json.dumps(geo, indent=2)}')


def cmd_decode_url(url: str):
    """Extract and decode the shape parameter from a full Idealista URL."""
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)
    shape  = params.get('shape', [''])[0]
    if not shape:
        print('❌ No shape parameter found in URL')
        return
    cmd_decode(shape)


def cmd_encode(coords_file: str, price_min: int, price_max: int, types: str):
    """Encode coordinates from a JSON file to a search URL."""
    with open(coords_file) as f:
        raw = json.load(f)
    coords = [(p[0], p[1]) for p in raw]
    url = build_url(coords, price_min, price_max, types)
    encoded = encode_polyline(coords)
    print(f'\n✅ Encoded {len(coords)} coordinates')
    print(f'   Shape: (({encoded}))')
    print(f'\n🔗 Search URL:\n{url}')
    print(f'\n📋 For daily_update.py BASE_URL:')
    shape_encoded = urllib.parse.quote(f'(({encoded}))', safe='')
    price_part = f'com-preco-max_{price_max},preco-min_{price_min}'
    print(f'SHAPE    = \'{shape_encoded}\'')
    print(f'BASE_URL = f\'https://www.idealista.pt/areas/arrendar-casas/{price_part},{types}/?shape={{SHAPE}}\'')


def cmd_show():
    """Show the current zone on a static map (open HTML in browser)."""
    html = generate_map_html(CURRENT_ZONE)
    path = '/tmp/zone_current.html'
    with open(path, 'w') as f:
        f.write(html)
    print(f'✅ Opening current zone in browser: {path}')
    webbrowser.open(f'file://{path}')


def cmd_map():
    """Open an interactive map for drawing a new zone."""
    html = generate_map_html(CURRENT_ZONE)
    path = '/tmp/zone_selector.html'
    with open(path, 'w') as f:
        f.write(html)
    print(f'✅ Zone selector opened: {path}')
    print('   Draw a polygon, then copy the generated URL or export coordinates.')
    webbrowser.open(f'file://{path}')


def cmd_geojson():
    """Export the current zone as GeoJSON."""
    geo  = to_geojson(CURRENT_ZONE + [CURRENT_ZONE[0]], name='Porto Foz Zone')
    path = os.path.join(os.path.dirname(__file__), 'data', 'current_zone.geojson')
    with open(path, 'w') as f:
        json.dump(geo, f, indent=2)
    print(f'✅ GeoJSON saved to {path}')
    print(json.dumps(geo, indent=2))


def cmd_current():
    """Show the current zone used by the daily pipeline."""
    print(f'\n📍 Current pipeline zone ({len(CURRENT_ZONE)} vertices):')
    for i, (lat, lng) in enumerate(CURRENT_ZONE):
        print(f'  [{i+1}] {lat:.6f}, {lng:.6f}')
    url = build_url(CURRENT_ZONE)
    stats = zone_stats(CURRENT_ZONE)
    print(f'\n📐 Size: {stats["size_km"][0]}km × {stats["size_km"][1]}km')
    print(f'🔗 URL:\n{url}')


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]

    if not args or '--help' in args or '-h' in args:
        print(__doc__)
        return

    if '--decode' in args:
        i = args.index('--decode')
        cmd_decode(args[i + 1])

    elif '--decode-url' in args:
        i = args.index('--decode-url')
        cmd_decode_url(args[i + 1])

    elif '--encode' in args:
        i          = args.index('--encode')
        coords_file = args[i + 1]
        price_min  = int(args[args.index('--price-min') + 1]) if '--price-min' in args else 1650
        price_max  = int(args[args.index('--price-max') + 1]) if '--price-max' in args else 3100
        types      = args[args.index('--types') + 1] if '--types' in args else 't2,t3'
        cmd_encode(coords_file, price_min, price_max, types)

    elif '--map' in args:
        cmd_map()

    elif '--show' in args:
        cmd_show()

    elif '--geojson' in args:
        cmd_geojson()

    elif '--current' in args:
        cmd_current()

    else:
        print(f'Unknown arguments: {args}')
        print(__doc__)


if __name__ == '__main__':
    main()
