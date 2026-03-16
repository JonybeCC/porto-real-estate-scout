#!/usr/bin/env python3
"""
ZenRows Detail Fetcher v3 — JBizz Assistant 🦞
Changes from v2:
  - Parses XHR responses for photo URLs (Idealista lazy-loads photos via AJAX)
  - Fail classification: blocked vs deleted vs timeout vs empty
  - Marks listings as deleted/inactive if confirmed gone
  - Retries blocked listings once with longer timeout
  - Re-fetches no-photo entries to try XHR extraction
"""

import json, re, os, time, signal, threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
DETAILS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
API_KEY       = os.environ.get('ZENROWS_API_KEY', 'a19f204d97b9578f8d82bd749ac175bd5383dd6e')
CONCURRENCY   = 3


# ── Fetch ────────────────────────────────────────────────────────────────────

def fetch_raw_click_gallery(lid: str, url: str) -> tuple[str, dict]:
    """
    Special fetch for listings where photos are behind a JS click event.
    Uses ZenRows js_instructions to simulate clicking the gallery open.
    """
    import json as _json
    instructions = _json.dumps([
        {"wait": 2000},
        {"click": ".main-multimedia-block, .gallery-trigger, [class*='gallery'], [class*='photo'], [data-testid='gallery']"},
        {"wait": 2000},
    ])
    params = {
        'url': url, 'apikey': API_KEY,
        'js_render': 'true', 'json_response': 'true', 'premium_proxy': 'true',
        'js_instructions': instructions,
    }
    try:
        r = requests.get('https://api.zenrows.com/v1/', params=params, timeout=150)
        data = r.json() if 'application/json' in r.headers.get('content-type', '') else {}
        html = data.get('html', '')
        xhr  = data.get('xhr', [])
        if r.status_code == 200 and len(html) > 5000:
            return lid, {'html': html, 'xhr': xhr, 'status': 'ok', 'status_code': 200, 'reason': ''}
        return lid, {'html': html, 'xhr': [], 'status': 'blocked',
                     'status_code': r.status_code, 'reason': f'HTTP {r.status_code}'}
    except requests.exceptions.Timeout:
        return lid, {'html': '', 'xhr': [], 'status': 'timeout', 'status_code': 0, 'reason': 'timeout'}
    except Exception as e:
        return lid, {'html': '', 'xhr': [], 'status': 'error', 'status_code': 0, 'reason': str(e)[:80]}


def fetch_raw(lid: str, url: str, timeout: int = 120) -> tuple[str, dict]:
    """
    Returns (lid, result_dict) where result_dict has:
      - html: str
      - xhr: list
      - status: 'ok' | 'blocked' | 'deleted' | 'timeout' | 'error'
      - status_code: int
      - reason: str
    """
    params = {
        'url': url, 'apikey': API_KEY,
        'js_render': 'true', 'json_response': 'true',
        'premium_proxy': 'true',
    }
    try:
        r = requests.get('https://api.zenrows.com/v1/', params=params, timeout=timeout)
        data = r.json() if r.headers.get('content-type', '').startswith('application/json') else {}
        html = data.get('html', '')
        xhr  = data.get('xhr', [])

        if r.status_code == 200 and len(html) > 5000:
            return lid, {'html': html, 'xhr': xhr, 'status': 'ok',
                         'status_code': 200, 'reason': ''}

        # Classify failure
        txt = html.lower()
        if any(x in txt for x in ['anúncio desactivado', 'anuncio desactivado',
                                    'no longer available', 'este anúncio já não',
                                    'imovel removido', 'página não encontrada']):
            return lid, {'html': html, 'xhr': [], 'status': 'deleted',
                         'status_code': r.status_code, 'reason': 'listing removed/deactivated'}

        if r.status_code in (403, 429) or 'captcha' in txt or 'cf-challenge' in txt or len(html) < 2000:
            return lid, {'html': html, 'xhr': [], 'status': 'blocked',
                         'status_code': r.status_code, 'reason': f'Cloudflare/rate-limit (HTTP {r.status_code})'}

        if r.status_code == 404:
            return lid, {'html': html, 'xhr': [], 'status': 'deleted',
                         'status_code': 404, 'reason': '404 not found — listing gone'}

        return lid, {'html': html, 'xhr': [], 'status': 'error',
                     'status_code': r.status_code,
                     'reason': f'Unexpected response: HTTP {r.status_code}, html={len(html)}B'}

    except requests.exceptions.Timeout:
        return lid, {'html': '', 'xhr': [], 'status': 'timeout',
                     'status_code': 0, 'reason': f'Timed out after {timeout}s'}
    except Exception as e:
        return lid, {'html': '', 'xhr': [], 'status': 'error',
                     'status_code': 0, 'reason': str(e)[:100]}


# ── Photo extraction ─────────────────────────────────────────────────────────

def extract_photos_from_html(html: str) -> list:
    """Extract photo URLs from HTML — works when photos are server-rendered."""
    photos = re.findall(
        r'https://img\d+\.idealista\.pt/blur/WEB_DETAIL[^"\'\s<>]+\.jpg',
        html, re.IGNORECASE
    )
    if not photos:
        photos = re.findall(
            r'https://img\d+\.idealista\.pt/[^"\'\s<>]+/id\.pro\.pt\.image\.master/[^"\'\s<>]+\.jpg',
            html, re.IGNORECASE
        )
    seen = set()
    return [p for p in photos if not (p in seen or seen.add(p))][:15]


def extract_photos_from_xhr(xhr: list) -> list:
    """
    Extract photo URLs from ZenRows XHR responses.
    Idealista loads photos via AJAX — URLs appear in XHR response bodies.
    """
    photos = []
    seen = set()

    for req in xhr:
        # XHR entry can be dict with 'response' key or just a string
        body = ''
        if isinstance(req, dict):
            body = req.get('response', '') or req.get('body', '') or req.get('responseText', '')
            # Also check the URL itself — sometimes the XHR URL is the image endpoint
            req_url = req.get('url', '')
            if 'idealista' in req_url and '.jpg' in req_url:
                if req_url not in seen:
                    seen.add(req_url)
                    photos.append(req_url)
        elif isinstance(req, str):
            body = req

        if not body:
            continue

        # Search in XHR response body
        found = re.findall(
            r'https://img\d+\.idealista\.pt/blur/WEB_DETAIL[^"\'\s\\<>]+\.jpg',
            body, re.IGNORECASE
        )
        if not found:
            found = re.findall(
                r'https://img\d+\.idealista\.pt/[^"\'\s\\<>]+/id\.pro\.pt\.image\.master/[^"\'\s\\<>]+\.jpg',
                body, re.IGNORECASE
            )
        # Also check for JSON-encoded paths (backslash-escaped)
        if not found:
            found_raw = re.findall(r'id\\.pro\\.pt\\.image\\.master\\/([a-f0-9\\/]+\\.jpg)', body)
            found = [f'https://img4.idealista.pt/blur/WEB_DETAIL/0/id.pro.pt.image.master/{p.replace(chr(92),"/")}' for p in found_raw]

        for p in found:
            if p not in seen:
                seen.add(p)
                photos.append(p)

    return photos[:15]


def extract_photos_from_json_blobs(html: str) -> list:
    """
    Last resort: find JSON blobs in HTML that contain image paths.
    Idealista embeds __NEXT_DATA__ or similar with image arrays.
    """
    photos = []
    seen = set()

    # Look for JSON embedded in script tags
    json_blobs = re.findall(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', html, re.DOTALL)
    json_blobs += re.findall(r'window\.__(?:INITIAL|NEXT)_(?:DATA|STATE)__\s*=\s*(\{.*?\});', html, re.DOTALL)

    for blob in json_blobs:
        found = re.findall(r'id\.pro\.pt\.image\.master/([a-f0-9/]+\.jpg)', blob)
        for p in found:
            url = f'https://img4.idealista.pt/blur/WEB_DETAIL/0/id.pro.pt.image.master/{p}'
            if url not in seen:
                seen.add(url)
                photos.append(url)

    return photos[:15]


# ── Parse ────────────────────────────────────────────────────────────────────

def parse(html: str, xhr: list, lid: str) -> dict:
    d = {
        'id': lid,
        'fetched_at': datetime.now().strftime('%Y-%m-%d'),
        'html_size': len(html),
        'active': True,
    }

    soup = BeautifulSoup(html, 'lxml')
    txt  = html.lower()

    # ── Active check ──────────────────────────────────────────────────────────
    if any(x in txt for x in ['anúncio desactivado', 'anuncio desactivado',
                               'no longer available', 'este anúncio já não']):
        d['active'] = False

    # ── Full description ──────────────────────────────────────────────────────
    for sel in ['div.comment', 'div#description', 'div.adDetailDescription',
                'section.detail-info', 'div[class*="description"]']:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 50:
            d['full_description'] = el.get_text(separator='\n', strip=True)[:3000]
            break

    # ── Area bruta / útil ─────────────────────────────────────────────────────
    for label, key in [('área bruta', 'area_bruta'), ('área útil', 'area_util'),
                        ('area bruta', 'area_bruta'), ('area util', 'area_util'),
                        ('área habitável', 'area_util')]:
        idx = txt.find(label)
        if idx > 0:
            context = html[max(0, idx-200):idx+50]
            m = re.search(r'<span[^>]*>(\d+[\.,]?\d*)</span>\s*m²', context, re.IGNORECASE)
            if not m:
                m = re.search(r'(\d+[\.,]?\d*)\s*m²', context)
            if m and key not in d:
                try:
                    d[key] = float(m.group(1).replace(',', '.'))
                except (ValueError, AttributeError):
                    pass

    # ── WCs ───────────────────────────────────────────────────────────────────
    m = re.search(r'(\d+)\s+cas(?:a|as)\s+de\s+banho', txt)
    if m:
        d['wcs'] = int(m.group(1))
    else:
        m = re.search(r'(\d+)\s*wc', txt)
        if m:
            d['wcs'] = int(m.group(1))

    # ── Garage ────────────────────────────────────────────────────────────────
    if 'garagem' in txt or 'lugar de garagem' in txt or 'estacionamento privat' in txt:
        d['has_garage'] = True
        m = re.search(r'(\d+)\s*lugar(?:es)?\s+(?:de\s+)?garagem', txt)
        if m:
            d['parking_spaces'] = int(m.group(1))
        else:
            m = re.search(r'garagem\s+com\s+(\d+)', txt)
            d['parking_spaces'] = int(m.group(1)) if m else 1

    # ── Energy cert ───────────────────────────────────────────────────────────
    idx = txt.find('certificado energ')
    if idx > 0:
        context = html[idx:idx+500]
        for grade in ['A+', 'A', 'B+', 'B', 'B-', 'C', 'D', 'E', 'F', 'G']:
            if re.search(rf'(?<![a-z]){re.escape(grade.lower())}(?![a-z])', context.lower()):
                d['energy_cert'] = grade
                break

    # ── Floor ─────────────────────────────────────────────────────────────────
    m = re.search(r'(\d+)[oºª]\s*andar', txt)
    if m:
        d['floor_num'] = int(m.group(1))

    # ── Full address ──────────────────────────────────────────────────────────
    addr_el = soup.select_one('span.main-info__title-minor, h2.detail-info-title')
    if addr_el:
        d['full_address'] = addr_el.get_text(strip=True)[:200]

    # ── Photos — 3-tier extraction ────────────────────────────────────────────
    # Tier 1: HTML (server-rendered listings)
    photos = extract_photos_from_html(html)

    # Tier 2: XHR responses (AJAX lazy-loaded listings — the most common case)
    if not photos and xhr:
        photos = extract_photos_from_xhr(xhr)
        if photos:
            d['photo_source'] = 'xhr'

    # Tier 3: JSON blobs in script tags (__NEXT_DATA__ etc)
    if not photos:
        photos = extract_photos_from_json_blobs(html)
        if photos:
            d['photo_source'] = 'json_blob'

    if photos:
        d['photo_urls'] = photos
        if 'photo_source' not in d:
            d['photo_source'] = 'html'

    # Also store unblurred master paths for reference
    master_paths = re.findall(r'/id\.pro\.pt\.image\.master/([a-f0-9/]+\.jpg)', html)
    if master_paths:
        seen_m = set()
        d['unblurred_photos'] = [
            f'https://img4.idealista.pt/id.pro.pt.image.master/{p}'
            for p in master_paths if not (p in seen_m or seen_m.add(p))
        ][:15]

    return d


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print('🦞 ZenRows Detail Fetcher v3')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(f'🔀 Concurrency: {CONCURRENCY}')
    print('=' * 55)

    with open(LISTINGS_FILE, encoding='utf-8') as f:
        listings = json.load(f)

    # Load existing
    existing = {}
    try:
        with open(DETAILS_FILE, encoding='utf-8') as f:
            existing = {d['id']: d for d in json.load(f)}
        print(f'   {len(existing)} already fetched')
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    # Re-fetch: new listings + existing ones without photos (to try XHR)
    no_photos_ids = {d['id'] for d in existing.values()
                     if not d.get('photo_urls') and d.get('active', True)
                     and d.get('fetch_status') not in ('deleted', 'blocked')}
    to_fetch = [l for l in listings
                if l['id'] not in existing or l['id'] in no_photos_ids]

    print(f'📦 {len(to_fetch)} to fetch (new + {len(no_photos_ids)} missing photos)\n')

    # Track failures for summary
    blocked, deleted, timeouts, errors = [], [], [], []
    done = 0
    lock = threading.Lock()

    # SIGTERM handler — flush progress before dying
    def _on_sigterm(signum, frame):
        details_list = list(existing.values())
        with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
            json.dump(details_list, f, ensure_ascii=False, indent=2)
        print(f'\n💾 SIGTERM — saved {len(details_list)} entries to {DETAILS_FILE}', flush=True)
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _on_sigterm)

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {
            ex.submit(fetch_raw, l['id'],
                      l.get('url', f'https://www.idealista.pt/imovel/{l["id"]}/')): l
            for l in to_fetch
        }

        for future in as_completed(futures):
            listing = futures[future]
            lid = listing['id']
            done += 1

            try:
                _, result = future.result()
                status = result['status']

                if status == 'ok':
                    detail = parse(result['html'], result['xhr'], lid)
                    detail['fetch_status'] = 'ok'

                    # Remove stale entry and add fresh one
                    existing[lid] = detail

                    flags = []
                    if detail.get('has_garage'):      flags.append(f'🚗×{detail.get("parking_spaces",1)}')
                    if detail.get('area_util'):        flags.append(f'Útil:{detail["area_util"]}m²')
                    if detail.get('area_bruta'):       flags.append(f'Bruta:{detail["area_bruta"]}m²')
                    if detail.get('wcs'):              flags.append(f'WC:{detail["wcs"]}')
                    if detail.get('energy_cert'):      flags.append(f'E:{detail["energy_cert"]}')
                    if detail.get('photo_urls'):
                        src = detail.get('photo_source', 'html')
                        flags.append(f'📸{len(detail["photo_urls"])}[{src}]')
                    if detail.get('full_description'): flags.append('📝')
                    if not detail.get('active'):       flags.append('⚠️INACTIVE')

                    print(f'  ✅ [{done:3d}/{len(to_fetch)}] {listing.get("rooms")} {listing.get("size_m2")}m² '
                          f'{lid} {" ".join(flags) or "(basic)"}')

                elif status == 'deleted':
                    deleted.append(lid)
                    # Mark as deleted but keep existing data if we had it
                    if lid in existing:
                        existing[lid]['active'] = False
                        existing[lid]['fetch_status'] = 'deleted'
                    else:
                        existing[lid] = {'id': lid, 'active': False, 'fetch_status': 'deleted',
                                         'fetch_reason': result['reason'],
                                         'fetched_at': datetime.now().strftime('%Y-%m-%d')}
                    print(f'  🗑️  [{done:3d}/{len(to_fetch)}] {lid} — DELETED: {result["reason"]}')

                elif status == 'blocked':
                    blocked.append(lid)
                    # IMPORTANT: Do NOT overwrite existing good data — just note the block
                    # The listing may have been fetched successfully before
                    if lid in existing:
                        existing[lid]['last_block_at'] = datetime.now().strftime('%Y-%m-%d %H:%M')
                        # Only set fetch_status to blocked if we have no useful data
                        if not existing[lid].get('full_description') and not existing[lid].get('photo_urls'):
                            existing[lid]['fetch_status'] = 'blocked'
                    else:
                        existing[lid] = {'id': lid, 'fetch_status': 'blocked',
                                         'fetch_reason': result['reason'],
                                         'html_size': 0,
                                         'fetched_at': datetime.now().strftime('%Y-%m-%d')}
                    print(f'  🚫 [{done:3d}/{len(to_fetch)}] {lid} — BLOCKED: {result["reason"]} (existing data preserved)')

                elif status == 'timeout':
                    timeouts.append(lid)
                    print(f'  ⏱️  [{done:3d}/{len(to_fetch)}] {lid} — TIMEOUT after 120s')

                else:
                    errors.append(lid)
                    print(f'  ❌ [{done:3d}/{len(to_fetch)}] {lid} — ERROR: {result["reason"]}')

            except Exception as e:
                errors.append(lid)
                print(f'  ❌ [{done:3d}/{len(to_fetch)}] {lid} — EXCEPTION: {str(e)[:60]}')

            # Checkpoint every 10
            if done % 10 == 0:
                details_list = list(existing.values())
                with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(details_list, f, ensure_ascii=False, indent=2)
                print(f'  💾 Checkpoint ({done} done)')

    # Retry no-photo listings with gallery-click strategy
    no_photo_ok = [lid for lid, d in existing.items()
                   if d.get('fetch_status') == 'ok' and not d.get('photo_urls') and d.get('active', True)]
    if no_photo_ok:
        print(f'\n🖼️  Retrying {len(no_photo_ok)} no-photo listings with gallery-click...')
        for lid in no_photo_ok[:10]:  # max 10 at once (expensive)
            listing = next((l for l in listings if l['id'] == lid), None)
            if not listing:
                continue
            url_l = listing.get('url', f'https://www.idealista.pt/imovel/{lid}/')
            _, result = fetch_raw_click_gallery(lid, url_l)
            if result['status'] == 'ok':
                photos = extract_photos_from_html(result['html'])
                if not photos:
                    photos = extract_photos_from_xhr(result['xhr'])
                if photos:
                    existing[lid]['photo_urls'] = photos
                    existing[lid]['photo_source'] = 'gallery_click'
                    print(f'  ✅ Gallery click: {lid} → {len(photos)} photos')
                else:
                    print(f'  ⚠️  Gallery click: {lid} → no photos in HTML/XHR')
            else:
                print(f'  ❌ Gallery click failed: {lid} — {result["reason"]}')
            time.sleep(2)

    # Retry blocked listings once with longer timeout
    if blocked:
        print(f'\n🔄 Retrying {len(blocked)} blocked listings (longer timeout)...')
        time.sleep(5)
        for lid in blocked:
            listing = next((l for l in listings if l['id'] == lid), None)
            if not listing:
                continue
            url = listing.get('url', f'https://www.idealista.pt/imovel/{lid}/')
            _, result = fetch_raw(lid, url, timeout=180)
            if result['status'] == 'ok':
                detail = parse(result['html'], result['xhr'], lid)
                detail['fetch_status'] = 'ok'
                existing[lid] = detail
                photos_n = len(detail.get('photo_urls', []))
                print(f'  ✅ Retry OK: {lid} | 📸{photos_n}')
            else:
                print(f'  ❌ Retry failed: {lid} — {result["reason"]}')
            time.sleep(3)

    # Final save
    details_list = list(existing.values())
    with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
        json.dump(details_list, f, ensure_ascii=False, indent=2)

    # Stats
    active    = [d for d in details_list if d.get('active', True)]
    has_photos = sum(1 for d in active if d.get('photo_urls'))
    photo_sources = {}
    for d in active:
        src = d.get('photo_source', 'none' if not d.get('photo_urls') else 'html')
        photo_sources[src] = photo_sources.get(src, 0) + 1

    print(f'\n✅ Done: {len(details_list)} total | {len(active)} active | {len(deleted)} deleted')
    print(f'   📸 With photos: {has_photos}/{len(active)}')
    print(f'   📸 Photo sources: {photo_sources}')
    print(f'   🚗 Garages:   {sum(1 for d in active if d.get("has_garage"))}')
    print(f'   📐 Área útil: {sum(1 for d in active if d.get("area_util"))}')
    print(f'   🚿 WCs:       {sum(1 for d in active if d.get("wcs"))}')
    if blocked:
        print(f'\n⚠️  Still blocked ({len(blocked)}): {blocked}')
        print(f'   → These may need manual retry in 10-15 min (rate limited)')
    if deleted:
        print(f'\n🗑️  Confirmed deleted ({len(deleted)}): {deleted}')
        print(f'   → These can be removed from listings.json')
    if timeouts:
        print(f'\n⏱️  Timed out ({len(timeouts)}): {timeouts}')


if __name__ == '__main__':
    main()
