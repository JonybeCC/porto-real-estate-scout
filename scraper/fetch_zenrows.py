#!/usr/bin/env python3
"""
ZenRows Detail Fetcher v4 — JBizz Assistant 🦞

v4 changes vs v3:
  - antibot=True param: bypasses Cloudflare on detail pages (was blocked 100% of time)
  - Parses area_util, area_bruta, floor_num, wcs, garage, elevator directly from HTML
  - Full description extracted from the detail page
  - Photos: blur/WEB_DETAIL URLs are actually full-res 1500px JPEGs (confirmed 188KB, 2940 variance)
  - Concurrency: 3 workers (was already concurrent, maintained)
  - Retries blocked listings with antibot=True before marking as failed

Critical finding: Idealista's blur/ CDN path is NOT blurred — images are 1500×1126px,
variance=2940 (sharp). Use these URLs directly for GPT vision analysis.
"""

import json, re, os, time, signal
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

import sys
sys.path.insert(0, os.path.dirname(__file__))
from paths import PATHS

LISTINGS_FILE = PATHS.listings
DETAILS_FILE  = PATHS.details_zenrows
API_KEY       = os.environ.get('ZENROWS_API_KEY', 'a19f204d97b9578f8d82bd749ac175bd5383dd6e')
CONCURRENCY   = 3


def fetch_raw(lid: str, url: str, timeout: int = 120, antibot: bool = True) -> tuple[str, dict]:
    """
    Fetch listing detail page via ZenRows with antibot bypass.
    Returns (lid, result_dict).
    antibot=True is the key fix — bypasses Cloudflare that blocked 100% of detail pages.

    Note: ZenRows with antibot sometimes returns content-type=text/plain even when
    the body is JSON. We try json.loads() regardless of content-type.
    timeout=120 (was 90) — antibot requests take 6-30s, need headroom.
    """
    import json as _json
    params = {
        'url': url,
        'apikey': API_KEY,
        'js_render': 'true',
        'antibot': 'true' if antibot else 'false',
        'premium_proxy': 'true',
        'wait': '3000',
        'json_response': 'true',
    }
    try:
        r = requests.get('https://api.zenrows.com/v1/', params=params, timeout=timeout)
        # ZenRows sometimes returns content-type=text/plain even for JSON responses.
        # Always try JSON parse first, fall back to raw text.
        data = {}
        try:
            data = r.json()
        except Exception:
            try:
                data = _json.loads(r.text)
            except Exception:
                pass
        html = data.get('html', r.text if not data else '')

        if r.status_code == 200 and len(html) > 10000:
            return lid, {'html': html, 'status': 'ok', 'status_code': 200, 'reason': ''}

        txt = html.lower()
        if any(x in txt for x in ['anúncio desactivado', 'este anúncio já não', 'imovel removido', 'página não encontrada']):
            return lid, {'html': '', 'status': 'deleted', 'status_code': r.status_code, 'reason': 'listing removed'}
        if r.status_code == 404:
            return lid, {'html': '', 'status': 'deleted', 'status_code': 404, 'reason': '404 not found'}
        if r.status_code in (403, 429) or 'captcha' in txt or len(html) < 5000:
            return lid, {'html': html, 'status': 'blocked', 'status_code': r.status_code,
                         'reason': f'CF/rate-limit ({r.status_code})'}

        return lid, {'html': html, 'status': 'error', 'status_code': r.status_code,
                     'reason': f'Unexpected {r.status_code}, html={len(html)}B'}

    except requests.Timeout:
        return lid, {'html': '', 'status': 'timeout', 'status_code': 0, 'reason': f'timeout after {timeout}s'}
    except Exception as e:
        return lid, {'html': '', 'status': 'error', 'status_code': 0, 'reason': str(e)[:100]}


def parse_detail(lid: str, html: str, listing: dict) -> dict:
    """
    Parse all available fields from listing detail page HTML.
    Returns enriched detail dict.
    """
    soup = BeautifulSoup(html, 'lxml')
    result = {
        'id': lid,
        'fetch_status': 'ok',
        'fetch_reason': '',
        'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M'),
    }

    # ── Core metrics from info-features block ─────────────────────────────────
    # Pattern: <span><span>111</span>m² área bruta</span>
    m = re.search(r'<span>(\d+)</span>\s*m² área bruta', html)
    if m:
        result['area_bruta'] = int(m.group(1))

    m = re.search(r'<span>(\d+)</span>\s*m[²2] área útil', html)
    if m:
        result['area_util'] = int(m.group(1))

    m = re.search(r'<span>(T\d+|[Ee]stúdio)</span>', html)
    if m:
        result['rooms'] = m.group(1)

    m = re.search(r'<span>(\d+)º andar</span>', html)
    if m:
        result['floor_num'] = int(m.group(1))
    elif re.search(r'[Rr]/[Cc]|[Rr]és.do.[Cc]hão|[Tt]érreo', html):
        result['floor_num'] = 0

    m = re.search(r'<span>(\d+)</span>\s*(?:casa|casas) de banho', html)
    if m:
        result['wcs'] = int(m.group(1))

    m = re.search(r'<span>(\d+)</span>\s*quarto', html, re.IGNORECASE)
    if m:
        result['bedrooms'] = int(m.group(1))

    result['elevator']   = bool(re.search(r'com elevador|com ascensor', html, re.IGNORECASE))
    result['has_garage'] = bool(re.search(r'[Gg]aragem incluída|[Gg]aragem|lugar de garagem', html))
    result['has_storage'] = bool(re.search(r'arrecadação|arrumos|arrumação', html, re.IGNORECASE))

    # Parking spaces
    m = re.search(r'(\d+)\s*lugar(?:es)? de garagem', html, re.IGNORECASE)
    if m:
        result['parking_spaces'] = int(m.group(1))
    elif result.get('has_garage'):
        result['parking_spaces'] = 1

    # Energy certificate
    m = re.search(r'[Cc]lasse\s+([A-G][+]?)', html)
    if m:
        result['energy_cert'] = m.group(1)

    # Year built
    m = re.search(r'construído em (\d{4})', html, re.IGNORECASE)
    if m:
        result['year_built'] = int(m.group(1))

    # ── Full description ──────────────────────────────────────────────────────
    # Find the largest text block that looks like a property description
    # Boilerplate patterns to reject — ZenRows sometimes grabs page chrome
    # instead of the actual listing description
    BOILERPLATE_STARTS = (
        'comentário do anunciante',
        'disponível em: português',
        'adicionar a tua nota',
        'guardar favorito',
    )

    def is_real_description(txt: str) -> bool:
        """True if text looks like an actual property description, not page chrome."""
        tl = txt.lower().strip()
        if len(txt) < 80:
            return False
        if any(tl.startswith(bp) for bp in BOILERPLATE_STARTS):
            return False
        # Must contain at least one property keyword
        keywords = ['apartamento', 'moradia', 'quarto', 'cozinha', 'sala', 'wc',
                    'arrendamento', 'garagem', 'varanda', 'andar', 'área', 'rua',
                    'm²', 'euro', '€', 'divisão']
        return any(kw in tl for kw in keywords)

    desc_candidates = []
    for el in soup.select('div[class*="comment"], div[class*="description"], .commentsContainer, [class*="adDescription"]'):
        txt = el.get_text(' ', strip=True)
        if is_real_description(txt):
            desc_candidates.append(txt)

    # Fallback: regex for property-keyword-rich text block
    if not desc_candidates:
        m = re.search(
            r'((?:apartamento|moradia|estúdio|imóvel|arrendamento)[^<]{200,3000})',
            html, re.IGNORECASE | re.DOTALL
        )
        if m:
            raw = BeautifulSoup(m.group(1), 'lxml').get_text(' ', strip=True)
            if is_real_description(raw):
                desc_candidates.append(raw)

    # Last resort: find largest text block near property keywords
    if not desc_candidates:
        for kw in ['quarto', 'cozinha', 'varanda', 'elevador']:
            idx = html.lower().find(kw)
            if idx > 200:
                chunk = html[max(0, idx - 500):idx + 2000]
                txt = BeautifulSoup(chunk, 'lxml').get_text(' ', strip=True)
                if is_real_description(txt):
                    desc_candidates.append(txt)
                    break

    if desc_candidates:
        # Pick longest real description (not boilerplate)
        result['full_description'] = max(desc_candidates, key=len)[:1500]
    else:
        # Try the HTML region around the first property keyword we find
        idx = html.lower().find('quarto')
        if idx > 100:
            chunk = html[max(0, idx-300):idx+2000]
            result['full_description'] = BeautifulSoup(chunk, 'lxml').get_text(' ', strip=True)[:800]

    # ── Sun/orientation from description ─────────────────────────────────────
    desc_lower = result.get('full_description', '').lower()
    for orient in ['sul, este e oeste', 'sul e nascente', 'nascente e sul', 'sul/nascente',
                   'nascente/sul', 'orientação sul', 'exposição sul', 'sul',
                   'nascente', 'poente', 'norte']:
        if orient in desc_lower:
            result['sun_exposure'] = orient
            break

    # ── Photos: blur/WEB_DETAIL URLs are full-res 1500px (confirmed) ─────────
    # The /blur/ prefix is just Idealista's CDN naming — images are sharp JPEGs
    photo_urls = list(dict.fromkeys(  # preserve order, deduplicate
        re.findall(
            r'https://img\d+\.idealista\.pt/blur/WEB_DETAIL/0/[^\"\s<>]+\.jpg',
            html
        )
    ))
    # Filter to unique image IDs (remove WEB_DETAIL_TOP-L-L duplicates)
    seen_ids, unique_photos = set(), []
    for p in photo_urls:
        m = re.search(r'/enh/(\d+)\.jpg$', p)
        img_id = m.group(1) if m else p
        if img_id not in seen_ids:
            seen_ids.add(img_id)
            unique_photos.append(p)

    result['photo_urls']   = unique_photos[:20]
    result['photo_count']  = len(unique_photos)

    return result


def main():
    print('🦞 ZenRows Detail Fetcher v4 (antibot bypass)')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}')
    print('=' * 55)

    try:
        with open(LISTINGS_FILE, encoding='utf-8') as f:
            listings = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f'❌ Cannot load listings: {e}'); return

    # Load existing details
    try:
        with open(DETAILS_FILE, encoding='utf-8') as f:
            existing = {d['id']: d for d in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {}

    # Mode selection:
    #   --backlog : process ALL previously-blocked/failed listings (slow, run separately)
    #   default   : only process NEW listings (never-seen-before) — fast, for daily pipeline
    #
    # This split is critical: daily run has ~1-5 new listings (seconds),
    # backlog has ~150 listings (minutes). Mixing them caused 1800s timeout failures.
    backlog_mode = '--backlog' in sys.argv

    to_fetch = []
    for l in listings:
        lid = l['id']
        ex  = existing.get(lid, {})
        status = ex.get('fetch_status', 'new')
        if backlog_mode:
            # Backlog: retry everything except confirmed ok/deleted
            if status not in ('ok', 'deleted'):
                to_fetch.append(l)
        else:
            # Daily mode: ONLY new listings (never attempted before)
            if status == 'new' or lid not in existing:
                to_fetch.append(l)

    mode_label = 'BACKLOG (all blocked/failed)' if backlog_mode else 'DAILY (new listings only)'
    print(f'📦 {len(listings)} listings | {len(existing)} in cache')
    print(f'🔄 {len(to_fetch)} to fetch [{mode_label}]')
    if not backlog_mode:
        backlog_count = sum(1 for l in listings
                           if existing.get(l['id'], {}).get('fetch_status') not in ('ok', 'deleted', 'new')
                           and l['id'] in existing)
        if backlog_count:
            print(f'   ℹ️  {backlog_count} previously-blocked listings in backlog → run with --backlog to process')
    print()

    if not to_fetch:
        print('✅ All listings already have detail data — nothing to fetch')
        return

    results = dict(existing)  # start with cached data

    # SIGTERM handler — save progress
    def _on_sigterm(signum, frame):
        with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
            json.dump(list(results.values()), f, ensure_ascii=False, indent=2)
        print(f'\n💾 SIGTERM — saved {len(results)} detail records', flush=True)
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _on_sigterm)

    ok_count = blocked_count = error_count = deleted_count = 0
    listings_map = {l['id']: l for l in listings}

    def _fetch_one(listing):
        lid = listing['id']
        url = listing.get('url', f'https://www.idealista.pt/imovel/{lid}/')
        _, raw = fetch_raw(lid, url, timeout=90, antibot=True)
        if raw['status'] == 'ok':
            detail = parse_detail(lid, raw['html'], listing)
            return lid, detail, 'ok'
        else:
            return lid, {
                'id': lid, 'fetch_status': raw['status'],
                'fetch_reason': raw['reason'], 'html_size': len(raw.get('html', '')),
                'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M'),
            }, raw['status']

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = {executor.submit(_fetch_one, l): l for l in to_fetch}
        done = 0
        for future in as_completed(futures):
            lid, detail, status = future.result()
            done += 1
            results[lid] = detail

            if status == 'ok':
                ok_count += 1
                area_util   = detail.get('area_util', '?')
                area_bruta  = detail.get('area_bruta', '?')
                photos      = detail.get('photo_count', 0)
                has_desc    = bool(detail.get('full_description'))
                print(f'  [{done:3d}/{len(to_fetch)}] ✅ {lid} | '
                      f'util={area_util}m² bruta={area_bruta}m² '
                      f'garage={detail.get("has_garage")} '
                      f'photos={photos} desc={has_desc}')
            elif status == 'deleted':
                deleted_count += 1
                print(f'  [{done:3d}/{len(to_fetch)}] 🗑️  {lid} — deleted/inactive')
            elif status == 'blocked':
                blocked_count += 1
                print(f'  [{done:3d}/{len(to_fetch)}] ❌ {lid} — still blocked: {detail.get("fetch_reason","")}')
            else:
                error_count += 1
                print(f'  [{done:3d}/{len(to_fetch)}] ⚠️  {lid} — {status}: {detail.get("fetch_reason","")}')

            # Checkpoint every 10
            if done % 10 == 0:
                with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(list(results.values()), f, ensure_ascii=False, indent=2)
                print(f'  💾 Checkpoint ({done}/{len(to_fetch)})')

            time.sleep(1.0)  # rate limiting

    # Final save
    with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
        json.dump(list(results.values()), f, ensure_ascii=False, indent=2)

    total_ok = sum(1 for d in results.values() if d.get('fetch_status') == 'ok')
    print(f'\n📊 Results: ok={ok_count} blocked={blocked_count} error={error_count} deleted={deleted_count}')
    print(f'✅ {total_ok}/{len(listings)} listings now have full detail data → {DETAILS_FILE}')


if __name__ == '__main__':
    main()
