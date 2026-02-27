#!/usr/bin/env python3
"""
ZenRows Detail Fetcher v2 — JBizz Assistant 🦞
Fixes:
  - Area bruta/util regex (HTML structure: <span>142</span> m² área bruta)
  - Photo extraction: /blur/WEB_DETAIL/ URLs (173KB full detail images)
  - WC extraction from full description
  - Energy cert extraction
  - Concurrent fetching (3 at a time)

Saves to: data/listing_details_zenrows.json (resumable)
"""

import json, re, os, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
DETAILS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
API_KEY       = os.environ.get('ZENROWS_API_KEY', 'a19f204d97b9578f8d82bd749ac175bd5383dd6e')
CONCURRENCY   = 3   # safe for developer plan


def fetch_raw(lid: str, url: str) -> tuple[str, dict | None]:
    params = {
        'url': url, 'apikey': API_KEY,
        'js_render': 'true', 'json_response': 'true', 'premium_proxy': 'true', 'autoparse': 'true',
    }
    try:
        r = requests.get('https://api.zenrows.com/v1/', params=params, timeout=120)
        if r.status_code == 200:
            data = r.json()
            html = data.get('html', '')
            if len(html) > 5000:
                return lid, html
        return lid, None
    except Exception as e:
        return lid, None


def parse(html: str, lid: str) -> dict:
    d = {'id': lid, 'fetched_at': datetime.now().strftime('%Y-%m-%d'), 'html_size_kb': len(html) // 1000}

    # ── Full description ──────────────────────────────────────────────────────
    # Idealista puts the description in a div with class "comment"
    soup = BeautifulSoup(html, 'lxml')
    for sel in ['div.comment', 'div#description', 'div.adDetailDescription']:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 50:
            d['full_description'] = el.get_text(separator='\n', strip=True)[:3000]
            break

    txt = html.lower()

    # ── Area bruta / útil ─────────────────────────────────────────────────────
    # HTML pattern: <span>142</span> m² área bruta
    # or: <span>142</span> m² área útil
    for label, key in [('área bruta', 'area_bruta'), ('área útil', 'area_util'),
                        ('area bruta', 'area_bruta'), ('area util', 'area_util'),
                        ('área habitável', 'area_util')]:
        # Find label position and look back for the number
        idx = txt.find(label)
        if idx > 0:
            # Grab the surrounding 200 chars before the label
            context = html[max(0, idx-200):idx+50]
            m = re.search(r'<span[^>]*>(\d+[\.,]?\d*)</span>\s*m²', context, re.IGNORECASE)
            if not m:
                m = re.search(r'(\d+[\.,]?\d*)\s*m²', context)
            if m and key not in d:
                try:
                    d[key] = float(m.group(1).replace(',', '.'))
                except:
                    pass

    # ── WCs ───────────────────────────────────────────────────────────────────
    m = re.search(r'(\d+)\s+cas(?:a|as)\s+de\s+banho', txt)
    if m:
        d['wcs'] = int(m.group(1))
    else:
        # From description: "2 casas de banho" or "2 wc"
        m = re.search(r'(\d+)\s*wc', txt)
        if m:
            d['wcs'] = int(m.group(1))

    # ── Garage ────────────────────────────────────────────────────────────────
    if 'garagem' in txt or 'lugar de garagem' in txt or 'estacionamento privat' in txt:
        d['has_garage'] = True
        # Count spaces
        m = re.search(r'(\d+)\s*lugar(?:es)?\s+(?:de\s+)?garagem', txt)
        if m:
            d['parking_spaces'] = int(m.group(1))
        else:
            # "garagem com 2 lugares"
            m = re.search(r'garagem\s+com\s+(\d+)', txt)
            d['parking_spaces'] = int(m.group(1)) if m else 1

    # ── Energy cert ───────────────────────────────────────────────────────────
    # Look for the section near "certificado energético"
    idx = txt.find('certificado energ')
    if idx > 0:
        context = html[idx:idx+500]
        for grade in ['A+', 'A', 'B+', 'B', 'B-', 'C', 'D', 'E', 'F', 'G']:
            if re.search(rf'(?<![a-z]){re.escape(grade.lower())}(?![a-z])', context.lower()):
                d['energy_cert'] = grade
                break

    # ── Photos ────────────────────────────────────────────────────────────────
    # Primary: /blur/WEB_DETAIL/ URLs — full-resolution detail images (173KB each)
    photos = re.findall(
        r'https://img\d+\.idealista\.pt/blur/WEB_DETAIL(?:[^"\']*)?/[^"\']+\.jpg',
        html, re.IGNORECASE
    )
    # Fallback: any idealista CDN image URL with id.pro.pt.image.master
    if not photos:
        photos = re.findall(
            r'https://img\d+\.idealista\.pt/[^"\']+/id\.pro\.pt\.image\.master/[^"\']+\.jpg',
            html, re.IGNORECASE
        )
    # Deduplicate while preserving order, prefer WEB_DETAIL over thumbnails
    seen = set()
    unique_photos = []
    for p in photos:
        # Skip tiny thumbnails (480_360 etc in URL path for listing pages)
        if p not in seen:
            seen.add(p)
            unique_photos.append(p)
    if unique_photos:
        d['photo_urls'] = unique_photos[:15]  # keep up to 15

    # Also extract the image master paths for direct (unblurred) access attempt
    master_paths = re.findall(r'/id\.pro\.pt\.image\.master/([a-f0-9/]+\.jpg)', html)
    seen_m = set()
    unique_masters = []
    for p in master_paths:
        if p not in seen_m:
            seen_m.add(p)
            unique_masters.append(f'https://img4.idealista.pt/id.pro.pt.image.master/{p}')
    if unique_masters:
        d['unblurred_photos'] = unique_masters[:15]

    # ── Floor ─────────────────────────────────────────────────────────────────
    m = re.search(r'(\d+)[oºª]\s*andar', txt)
    if m:
        d['floor_num'] = int(m.group(1))

    # ── Active / gone ─────────────────────────────────────────────────────────
    if any(x in txt for x in ['anúncio desactivado', 'anuncio desactivado', 'no longer available']):
        d['active'] = False
    else:
        d['active'] = True

    return d


def main():
    print('🦞 ZenRows Detail Fetcher v2')
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
        print(f'   {len(existing)} already fetched — resuming')
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    # Also re-fetch entries that have html but no photos (regex may have missed them)
    no_photos_ids = {d['id'] for d in existing.values() if not d.get('photo_urls')}
    to_fetch = [l for l in listings if l['id'] not in existing or l['id'] in no_photos_ids]
    print(f'📦 {len(to_fetch)} to fetch ({len(listings)} total)\n')

    details = list(existing.values())
    done = 0

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as ex:
        futures = {
            ex.submit(fetch_raw, l['id'], l.get('url', f'https://www.idealista.pt/imovel/{l["id"]}/')): l
            for l in to_fetch
        }

        for future in as_completed(futures):
            listing = futures[future]
            lid = listing['id']
            try:
                _, html = future.result()
                if html:
                    detail = parse(html, lid)
                    details.append(detail)
                    existing[lid] = detail

                    flags = []
                    if detail.get('has_garage'):   flags.append(f'🚗×{detail.get("parking_spaces",1)}')
                    if detail.get('area_util'):     flags.append(f'Útil:{detail["area_util"]}m²')
                    if detail.get('area_bruta'):    flags.append(f'Bruta:{detail["area_bruta"]}m²')
                    if detail.get('wcs'):           flags.append(f'WC:{detail["wcs"]}')
                    if detail.get('energy_cert'):   flags.append(f'E:{detail["energy_cert"]}')
                    if detail.get('photo_urls'):    flags.append(f'📸{len(detail["photo_urls"])}')
                    if detail.get('full_description'): flags.append('📝')

                    done += 1
                    print(f'  ✅ [{done:3d}/{len(to_fetch)}] {listing.get("rooms")} {listing.get("size_m2")}m² '
                          f'{lid} {" ".join(flags) or "(basic)"}')
                else:
                    done += 1
                    print(f'  ⚠️  [{done:3d}/{len(to_fetch)}] {lid} — failed/empty')
            except Exception as e:
                done += 1
                print(f'  ❌ [{done:3d}/{len(to_fetch)}] {lid} — {str(e)[:40]}')

            # Save every 10
            if done % 10 == 0:
                with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(details, f, ensure_ascii=False, indent=2)
                print(f'  💾 Checkpoint saved ({done} done)')

    # Final save
    with open(DETAILS_FILE, 'w', encoding='utf-8') as f:
        json.dump(details, f, ensure_ascii=False, indent=2)

    # Stats
    garages   = sum(1 for d in details if d.get('has_garage'))
    has_util  = sum(1 for d in details if d.get('area_util'))
    has_wcs   = sum(1 for d in details if d.get('wcs'))
    has_photos = sum(1 for d in details if d.get('photo_urls'))
    has_energy = sum(1 for d in details if d.get('energy_cert'))
    has_desc  = sum(1 for d in details if d.get('full_description'))

    print(f'\n✅ Done: {len(details)} total')
    print(f'   🚗 Garages:   {garages}')
    print(f'   📐 Área útil: {has_util}')
    print(f'   🚿 WCs:       {has_wcs}')
    print(f'   📸 Photos:    {has_photos}')
    print(f'   ⚡ Energy:    {has_energy}')
    print(f'   📝 Full desc: {has_desc}')

    # Quick check on 34809959
    fav = existing.get('34809959', {})
    if fav:
        print(f'\n🎯 34809959: garage={fav.get("has_garage")} ×{fav.get("parking_spaces","?")} | '
              f'util={fav.get("area_util","?")} | wcs={fav.get("wcs","?")} | '
              f'photos={len(fav.get("photo_urls",[]))} | desc={len(fav.get("full_description",""))}chars')


if __name__ == '__main__':
    main()
