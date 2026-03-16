#!/usr/bin/env python3
"""
Smart Duplicate Detector v3 — JBizz Assistant 🦞

Core idea: different agencies photograph the same unit from different angles.
Photos won't share URLs — each agency uploads independently. So we need visual
similarity within plausible groups (same rooms + size ±10m² + neighbourhood).

Detection pipeline:
  Pass 1 — Exact text signals (fast, no API calls):
    1a. Shared internal reference code in description text    → DEFINITE
    1b. Description similarity >80% (boilerplate-filtered)   → HIGH
    1c. Composite key (rooms + area_util + wcs + floor)      → HIGH

  Pass 2 — Visual comparison within plausible groups:
    Group by (rooms, size_bucket ±10m², neighbourhood)
    For each group with >1 listing:
      2a. Perceptual hash (pHash) of first 5 photos each     → HIGH if match
      2b. GPT-4o-mini vision for pairs that passed pHash ambiguity
          — compare up to 3 photos from each side
          — tell it explicitly: "different angles of same room = SAME unit"

  Rationale for group-first approach:
    - We only compare T2 ~110m² Foz vs T2 ~110m² Foz — not all 129 × 129
    - 24 pairs in 12 groups vs 8256 naive comparisons
    - pHash will catch pixel-similar shots fast; vision handles the rest
"""

import json, re, os, signal, time, shutil, base64, math
from io import BytesIO
from datetime import datetime
from collections import defaultdict
from difflib import SequenceMatcher
import requests
from openai import OpenAI, RateLimitError, APITimeoutError

try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

JSON_FILE   = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
DETAILS_FILE= '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
DUPES_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/duplicates.json'
CLEAN_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listings_deduped.json'

# Initialise client lazily — crashes at startup if key is missing.
# We allow it to be None so that passes 1 (text dedup) still work without OpenAI.
_openai_key = os.environ.get('OPENAI_API_KEY')
try:
    client = OpenAI(api_key=_openai_key) if _openai_key else None
except Exception:
    client = None
if client is None:
    print('⚠️  OPENAI_API_KEY not set — vision dedup (pass 2b) will be skipped. Text/pHash dedup still active.')


# ── Agency boilerplate patterns — skip fingerprinting if description starts with these ──
BOILERPLATE_PATTERNS = [
    r'^reservas e pedidos de informa',     # vacation rental portal template
    r'^para obter informa',
    r'^contacte-nos para',
    r'^ligue para',
    r'^visita (?:virtual|online)',
    r'^exclusivo\s+[\w\s]+agência',
]


def is_boilerplate(desc: str) -> bool:
    """True if description is a generic agency template, not property-specific."""
    if not desc:
        return True
    head = desc.lower().strip()[:120]
    return any(re.match(p, head) for p in BOILERPLATE_PATTERNS)


def extract_street(title: str, description: str) -> str:
    """
    Extract normalized street name. Prefers description (more specific) over title.
    Returns '' if only neighbourhood-level info found.
    """
    combined = (description or '') + ' ' + (title or '')
    # Look for explicit street mentions
    m = re.search(
        r'(Rua|Avenida|Av\.|Travessa|Largo|Praça|Alameda|Rua d[aoe]|Beco)\s+[A-ZÀ-Üa-zà-ü][^\.,\n\(\)]{3,40}',
        combined, re.IGNORECASE
    )
    if not m:
        return ''
    street = m.group(0).strip().lower()
    street = re.sub(r'\s*,?\s*n[oº]?\s*\d+.*$', '', street)  # remove house numbers
    street = re.sub(r'\s+', ' ', street).strip()
    # Must be longer than a bare neighbourhood name
    return street if len(street) > 8 else ''


def extract_ref_codes(desc: str) -> list[str]:
    """Extract internal reference codes from description text."""
    if not desc:
        return []
    # Common patterns: Ref: ABC123, REF-XYZ, Referência: 12345, apor_250801
    patterns = [
        r'ref(?:erência|erencia|\.?)[:\s#\-]+([A-Z0-9_\-]{4,20})',
        r'\bapor_([A-Z0-9_\-]{4,20})',
        r'\bIMO[-\s]?([A-Z0-9]{4,15})',
        r'processo\s+n[oº]?[:\s]+([A-Z0-9\-/]{4,20})',
    ]
    codes = []
    for p in patterns:
        for m in re.finditer(p, desc, re.IGNORECASE):
            code = m.group(1).upper().strip()
            if code not in ('INTERNA', 'INTERNE', 'INTERNE', 'DISPONIVEL'):  # skip generic words
                codes.append(code)
    return list(set(codes))


def desc_similarity(a: str, b: str) -> float:
    """Character-level similarity ratio on first 600 chars."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a[:600].lower(), b[:600].lower()).ratio()


def phash(img_bytes: bytes, hash_size: int = 8) -> int | None:
    """
    Perceptual hash (difference hash) of an image.
    Returns None if PIL unavailable or image can't be decoded.
    """
    if not HAS_PIL:
        return None
    try:
        img = Image.open(BytesIO(img_bytes)).convert('L')
        img = img.resize((hash_size + 1, hash_size), Image.LANCZOS)
        pixels = list(img.getdata())
        bits = 0
        for row in range(hash_size):
            for col in range(hash_size):
                if pixels[row * (hash_size + 1) + col] > pixels[row * (hash_size + 1) + col + 1]:
                    bits |= 1 << (row * hash_size + col)
        return bits
    except Exception:
        return None


def hamming(a: int, b: int) -> int:
    return bin(a ^ b).count('1')


def fetch_photo_hashes(listing_ids: list, details_map: dict, max_per_listing: int = 5) -> dict:
    """
    Fetch first N photos for each listing and compute pHashes.
    Returns {lid: [hash, ...]}.
    Uses a short per-photo timeout (4s) and skips listings with no photo URLs in details.
    Sequential with a cap of 30 listings to avoid hanging the pipeline.
    """
    result = {}
    # Cap at 30 listings to keep runtime bounded (~30 × 5 photos × 4s max = 10min worst case)
    for lid in listing_ids[:30]:
        d = details_map.get(lid, {})
        photos = (d.get('photo_urls') or d.get('unblurred_photos') or [])[:max_per_listing]
        if not photos:
            result[lid] = []
            continue
        hashes = []
        for url in photos[:3]:  # max 3 photos per listing for speed
            if not url.startswith('http'):
                continue
            try:
                r = requests.get(url, timeout=4, headers={  # 4s not 12s
                    'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.idealista.pt/'
                })
                if r.status_code == 200 and len(r.content) > 3000:
                    h = phash(r.content)
                    if h is not None:
                        hashes.append(h)
            except (requests.RequestException, OSError):
                pass
        result[lid] = hashes
    return result


def photos_are_same(hashes_a: list, hashes_b: list, threshold: int = 8) -> bool:
    """Returns True if any photo pair has Hamming distance ≤ threshold (very similar)."""
    if not hashes_a or not hashes_b:
        return False
    for ha in hashes_a:
        for hb in hashes_b:
            if hamming(ha, hb) <= threshold:
                return True
    return False


def fetch_image_b64(url: str) -> str | None:
    """Fetch image as base64 for GPT vision. Returns None on failure."""
    try:
        r = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.idealista.pt/'
        })
        if r.status_code == 200 and len(r.content) > 5000:
            if HAS_PIL:
                img = Image.open(BytesIO(r.content)).convert('RGB')
                img.thumbnail((800, 600), Image.LANCZOS)
                buf = BytesIO()
                img.save(buf, format='JPEG', quality=82)
                return base64.b64encode(buf.getvalue()).decode()
            return base64.b64encode(r.content).decode()
    except (requests.RequestException, OSError):
        pass
    return None


def vision_compare(listing_a: dict, listing_b: dict, details_a: dict, details_b: dict) -> tuple[bool | None, str]:
    """GPT-4o-mini vision comparison. Returns (is_same, verdict_text). Skipped if no client."""
    if client is None:
        return None, 'skipped_no_openai_key'
    photos_a = (details_a.get('photo_urls') or details_a.get('unblurred_photos') or [])[:3]
    photos_b = (details_b.get('photo_urls') or details_b.get('unblurred_photos') or [])[:3]

    if not photos_a or not photos_b:
        return None, 'no_photos'

    images_content = []
    for url in photos_a[:2] + photos_b[:2]:
        b64 = fetch_image_b64(url)
        if b64:
            images_content.append({
                'type': 'image_url',
                'image_url': {'url': f'data:image/jpeg;base64,{b64}', 'detail': 'low'}
            })

    if len(images_content) < 2:
        return None, 'fetch_failed'

    prompt = f"""Are these photos showing the SAME physical apartment listed by different agencies?

Listing A: {listing_a.get('rooms')} {listing_a.get('size_m2')}m² €{listing_a.get('price_eur')}/mo {listing_a.get('neighborhood','')}
Listing B: {listing_b.get('rooms')} {listing_b.get('size_m2')}m² €{listing_b.get('price_eur')}/mo {listing_b.get('neighborhood','')}

First {min(2, len(photos_a))} image(s) = Listing A. Next {min(2, len(photos_b))} = Listing B.

Match indicators: same floor plan, identical window view, matching distinctive features (tiles, mouldings, fixtures), same room proportions.
Different angles of same room still count as SAME property.

Reply ONLY:
SAME - [reason in ≤10 words]
DIFFERENT - [reason in ≤10 words]
UNCERTAIN - [reason in ≤10 words]"""

    for attempt in range(1, 4):
        try:
            resp = client.chat.completions.create(
                model='gpt-4o-mini',
                messages=[{'role': 'user', 'content': [{'type': 'text', 'text': prompt}] + images_content}],
                max_tokens=80,
                temperature=0,
            )
            answer = resp.choices[0].message.content.strip()
            return answer.upper().startswith('SAME'), answer
        except RateLimitError:
            time.sleep(10 * attempt)
        except APITimeoutError:
            time.sleep(5 * attempt)
        except Exception as e:
            return None, f'error: {e}'
    return None, 'max_retries_exceeded'


def build_plausible_groups(listings: list, details_map: dict) -> dict:
    """
    Group listings that could plausibly be the same unit:
    same rooms + size within ±10m² + same neighbourhood.
    Returns {group_key: [{'id', 'photos', 'listing'}, ...]}
    """
    groups = defaultdict(list)
    for l in listings:
        lid   = l['id']
        d     = details_map.get(lid, {})
        rooms = l.get('rooms', '')
        size  = l.get('size_m2') or 0
        neigh = (l.get('neighborhood') or '').lower()[:25]
        photos = d.get('photo_urls') or d.get('unblurred_photos') or []
        if isinstance(photos, str):
            photos = [p for p in photos.split('|') if p.startswith('http')]
        if rooms and size and neigh:
            size_bucket = round(size / 10) * 10  # ±10m² bucket
            key = (rooms, size_bucket, neigh)
            groups[key].append({'id': lid, 'photos': photos, 'listing': l})
    return {k: v for k, v in groups.items() if len(v) > 1}


def main():
    print('🦞 Smart Duplicate Detector v3')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(f'   PIL available (pHash): {"✅" if HAS_PIL else "❌ — install Pillow for photo hashing"}')
    print('=' * 55)

    try:
        with open(JSON_FILE, encoding='utf-8') as f:
            listings = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f'❌ Cannot load listings: {e}')
        return []

    try:
        with open(DETAILS_FILE, encoding='utf-8') as f:
            details_map = {d['id']: d for d in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        details_map = {}

    print(f'📦 {len(listings)} listings | {len(details_map)} with ZenRows details')

    # ── Pass 1: Fast text signals (no API calls) ──────────────────────────────
    ref_map     = defaultdict(list)
    struct_map  = defaultdict(list)
    desc_corpus = {}

    for l in listings:
        lid   = l['id']
        d     = details_map.get(lid, {})
        desc  = d.get('full_description', '') or ''
        title = l.get('title', '')

        for code in extract_ref_codes(desc):
            ref_map[code].append(lid)

        area_util = d.get('area_util')
        wcs       = d.get('wcs')
        floor_num = d.get('floor_num')
        rooms     = l.get('rooms', '')
        if area_util and wcs and rooms and floor_num is not None:
            struct_map[(rooms, float(area_util), int(wcs), str(floor_num))].append(lid)

        if not is_boilerplate(desc) and len(desc) > 80:
            desc_corpus[lid] = desc

    definite: list[dict] = []
    confirmed_by_vision: list[dict] = []
    seen_pairs: set = set()

    def add_definite(a, b, method, confidence='HIGH'):
        pair = tuple(sorted([a, b]))
        if pair in seen_pairs:
            return
        seen_pairs.add(pair)
        la = next((l for l in listings if l['id'] == a), {})
        lb = next((l for l in listings if l['id'] == b), {})
        definite.append({
            'id_a': a, 'id_b': b, 'method': method, 'confidence': confidence,
            'listing_a': {'price': la.get('price_eur'), 'size': la.get('size_m2'), 'url': la.get('url')},
            'listing_b': {'price': lb.get('price_eur'), 'size': lb.get('size_m2'), 'url': lb.get('url')},
        })

    print('\n🔍 Pass 1a: Reference codes...')
    for code, ids in ref_map.items():
        if len(ids) > 1:
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    add_definite(ids[i], ids[j], f'Shared ref code: {code}', 'DEFINITE')
    print(f'   → {len(definite)} definite')

    print('🔍 Pass 1b: Description similarity (boilerplate-filtered)...')
    desc_ids = list(desc_corpus.keys())
    desc_count = 0
    for i in range(len(desc_ids)):
        for j in range(i + 1, len(desc_ids)):
            a, b = desc_ids[i], desc_ids[j]
            if tuple(sorted([a, b])) in seen_pairs:
                continue
            ratio = desc_similarity(desc_corpus[a], desc_corpus[b])
            if ratio >= 0.80:
                la = next((l for l in listings if l['id'] == a), {})
                lb = next((l for l in listings if l['id'] == b), {})
                na = (la.get('neighborhood') or '').lower()[:20]
                nb = (lb.get('neighborhood') or '').lower()[:20]
                if na == nb or SequenceMatcher(None, na, nb).ratio() > 0.7:
                    add_definite(a, b, f'Description similarity {ratio:.0%}', 'HIGH')
                    desc_count += 1
    print(f'   → {desc_count} HIGH')

    print('🔍 Pass 1c: Composite structural key (rooms+area_util+wcs+floor)...')
    struct_count = 0
    for key, ids in struct_map.items():
        if len(ids) > 1:
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    add_definite(ids[i], ids[j], f'Exact structural key: {key}', 'HIGH')
                    struct_count += 1
    print(f'   → {struct_count} HIGH')

    # ── Pass 2: Visual comparison within plausible groups ────────────────────
    groups = build_plausible_groups(listings, details_map)
    total_pairs = sum(len(v) * (len(v) - 1) // 2 for v in groups.values())
    print(f'\n🖼️  Pass 2: Visual comparison')
    print(f'   {len(groups)} plausible groups | {total_pairs} pairs to check')

    # Fetch pHashes for all listings in groups
    group_ids = list(set(m['id'] for members in groups.values() for m in members))
    if HAS_PIL:
        print(f'   Fetching photos & computing pHashes for {len(group_ids)} listings...')
        photo_hashes = fetch_photo_hashes(group_ids, details_map, max_per_listing=6)
    else:
        photo_hashes = {}

    phash_count = 0
    vision_count = 0

    for group_key, members in groups.items():
        rooms, size_b, neigh = group_key
        print(f'\n   Group: {rooms} ~{size_b}m² {neigh[:20]} ({len(members)} listings)')

        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                ma, mb = members[i], members[j]
                a, b = ma['id'], mb['id']
                pair = tuple(sorted([a, b]))
                if pair in seen_pairs:
                    continue

                la, lb = ma['listing'], mb['listing']
                size_a = la.get('size_m2') or 0
                size_b_val = lb.get('size_m2') or 0

                print(f'     {a} ({size_a}m² €{la.get("price_eur")}) vs '
                      f'{b} ({size_b_val}m² €{lb.get("price_eur")})', end=' ')

                # Step 2a: pHash comparison (fast, no API)
                if HAS_PIL:
                    hashes_a = photo_hashes.get(a, [])
                    hashes_b = photo_hashes.get(b, [])
                    if hashes_a and hashes_b:
                        if photos_are_same(hashes_a, hashes_b, threshold=8):
                            print('→ 📷 pHash match — SAME unit')
                            add_definite(a, b, 'Photo pHash match (same unit, different angles/agency)', 'HIGH')
                            phash_count += 1
                            continue
                        else:
                            print('→ pHash different', end=' ')

                # Step 2b: GPT vision (only if pHash inconclusive or PIL unavailable)
                is_same, verdict = vision_compare(la, lb, details_map.get(a, {}), details_map.get(b, {}))
                print(f'→ 👁️  {verdict[:50]}')
                vision_count += 1
                if is_same is True:
                    seen_pairs.add(pair)
                    confirmed_by_vision.append({
                        'id_a': a, 'id_b': b, 'method': 'vision+group', 'confidence': 'HIGH',
                        'vision_verdict': verdict,
                        'listing_a': {'price': la.get('price_eur'), 'size': size_a, 'url': la.get('url')},
                        'listing_b': {'price': lb.get('price_eur'), 'size': size_b_val, 'url': lb.get('url')},
                    })
                time.sleep(0.5)  # rate limit

    all_dupes = definite + confirmed_by_vision
    print(f'\n\n📊 Results: {len(all_dupes)} duplicates confirmed')
    print(f'   Text signals (ref/desc/struct): {len(definite)}')
    print(f'   Photo pHash matches:            {phash_count}')
    print(f'   Vision confirmed:               {len(confirmed_by_vision)}')
    print(f'   Vision API calls made:          {vision_count}')

    with open(DUPES_FILE, 'w', encoding='utf-8') as f:
        json.dump(all_dupes, f, ensure_ascii=False, indent=2)
    print(f'💾 {DUPES_FILE}')

    if all_dupes:
        ids_to_remove = set()
        for d in all_dupes:
            # Keep older ID (first scraped); remove the newer re-listing
            keep   = min(d['id_a'], d['id_b'])
            remove = max(d['id_a'], d['id_b'])
            ids_to_remove.add(remove)

        clean = [l for l in listings if l['id'] not in ids_to_remove]
        with open(CLEAN_FILE, 'w', encoding='utf-8') as f:
            json.dump(clean, f, ensure_ascii=False, indent=2)
        shutil.copy(JSON_FILE, JSON_FILE + '.bak')
        shutil.copy(CLEAN_FILE, JSON_FILE)
        print(f'✅ Clean dataset: {len(clean)} listings (removed {len(ids_to_remove)})')
    else:
        # Still write the deduped file as a clean copy
        with open(CLEAN_FILE, 'w', encoding='utf-8') as f:
            json.dump(listings, f, ensure_ascii=False, indent=2)
        print('✅ No duplicates found — dataset is clean')

    return all_dupes


if __name__ == '__main__':
    main()
