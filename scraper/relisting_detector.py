#!/usr/bin/env python3
"""
Re-listing Detector — JBizz Assistant 🦞
Detects when the same property is re-listed under a new ID.
Checks: same neighbourhood + rooms + size (±10%) + price (±15%) but different ID.
Also cross-references address strings when available.
"""

import json, re, os
from datetime import datetime, date
from itertools import combinations

LISTINGS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
DETAILS_FILE   = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
ENRICHED_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/enriched_listings.json'
RELIST_FILE    = '/root/.openclaw/workspace/projects/real-estate/data/relistings.json'
BOT_TOKEN      = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID        = '520980639'


def load_json(path, default):
    try:
        with open(path) as f: return json.load(f)
    except: return default


def normalise_address(s: str) -> str:
    """Strip numbers, lowercase, remove common filler words for fuzzy matching."""
    s = s.lower()
    for w in ['apartamento', 'arrendamento', 't2', 't3', 'na', 'no', 'em', 'rua', 'avenida', 'av.', 'de', 'da', 'do']:
        s = re.sub(r'\b' + w + r'\b', '', s)
    return re.sub(r'\s+', ' ', s).strip()


def similarity_score(a: dict, b: dict, details: dict) -> tuple[float, list]:
    """Return (0-1 score, reasons list)."""
    reasons = []
    score = 0.0

    # Same rooms
    if a.get('rooms') and a['rooms'] == b.get('rooms'):
        score += 0.25
        reasons.append(f'same rooms ({a["rooms"]})')

    # Same neighbourhood
    neigh_a = (a.get('neighborhood') or '').lower()
    neigh_b = (b.get('neighborhood') or '').lower()
    if neigh_a and neigh_b and (neigh_a in neigh_b or neigh_b in neigh_a or neigh_a == neigh_b):
        score += 0.2
        reasons.append(f'same neighbourhood')

    # Similar size (±10%)
    sz_a, sz_b = a.get('size_m2') or 0, b.get('size_m2') or 0
    if sz_a and sz_b and abs(sz_a - sz_b) / max(sz_a, sz_b) <= 0.10:
        score += 0.25
        reasons.append(f'similar size ({sz_a}m² vs {sz_b}m²)')

    # Similar price (±15%)
    pr_a, pr_b = a.get('price_eur') or 0, b.get('price_eur') or 0
    if pr_a and pr_b:
        diff_pct = abs(pr_a - pr_b) / max(pr_a, pr_b)
        if diff_pct <= 0.15:
            score += 0.15
            reasons.append(f'similar price (€{pr_a} vs €{pr_b}, {diff_pct*100:.0f}% diff)')

    # Address overlap from details
    det_a = details.get(a['id'], {})
    det_b = details.get(b['id'], {})
    addr_a = normalise_address(det_a.get('full_address', '') or a.get('title', ''))
    addr_b = normalise_address(det_b.get('full_address', '') or b.get('title', ''))
    if addr_a and addr_b and len(addr_a) > 8 and len(addr_b) > 8:
        # Word overlap
        words_a = set(addr_a.split())
        words_b = set(addr_b.split())
        common = words_a & words_b - {'porto', 'foz', ''}
        if len(common) >= 2:
            score += 0.15
            reasons.append(f'address overlap: {common}')

    return score, reasons


def run():
    print(f'🔍 Re-listing Detector — {date.today()}')
    listings = load_json(LISTINGS_FILE, [])
    details  = {d['id']: d for d in load_json(DETAILS_FILE, [])}
    existing = load_json(RELIST_FILE, [])
    known_pairs = {(r['id_a'], r['id_b']) for r in existing}

    candidates = []
    comparisons = 0

    # Only compare active listings with both rooms and size known
    valid = [l for l in listings if l.get('rooms') and l.get('size_m2') and l.get('price_eur')]
    print(f'📦 Comparing {len(valid)} valid listings ({len(valid)*(len(valid)-1)//2} pairs)...')

    for a, b in combinations(valid, 2):
        if a['id'] == b['id']:
            continue
        comparisons += 1
        score, reasons = similarity_score(a, b, details)
        if score >= 0.65:  # threshold
            pair = tuple(sorted([a['id'], b['id']]))
            if pair not in known_pairs:
                pr_a, pr_b = a.get('price_eur', 0), b.get('price_eur', 0)
                price_diff_pct = round((pr_b - pr_a) / pr_a * 100, 1) if pr_a else 0
                # Determine which is newer (higher ID = more recent listing)
                newer = b if b['id'] > a['id'] else a
                older = a if b['id'] > a['id'] else b
                candidates.append({
                    'id_a': older['id'], 'id_b': newer['id'],
                    'similarity': round(score, 2),
                    'price_a': older.get('price_eur'), 'price_b': newer.get('price_eur'),
                    'price_diff_pct': round((newer.get('price_eur',0) - older.get('price_eur',0)) / max(older.get('price_eur',1), 1) * 100, 1),
                    'rooms': a.get('rooms'), 'size_m2': a.get('size_m2'),
                    'neighborhood': a.get('neighborhood'),
                    'reasons': reasons,
                    'url_a': older.get('url'), 'url_b': newer.get('url'),
                    'detected_at': str(date.today()),
                })
                known_pairs.add(pair)

    all_relistings = existing + candidates
    with open(RELIST_FILE, 'w') as f:
        json.dump(all_relistings, f, ensure_ascii=False, indent=2)

    print(f'✅ {comparisons} pairs checked | {len(candidates)} new potential re-listings found')
    for c in candidates[:5]:
        sign = '+' if c['price_diff_pct'] > 0 else ''
        print(f"   {c['id_a']} → {c['id_b']} | sim={c['similarity']} | "
              f"{sign}{c['price_diff_pct']}% price | {', '.join(c['reasons'][:2])}")

    # Telegram alert
    if candidates and BOT_TOKEN:
        import requests as req
        lines = [f'🔁 *{len(candidates)} potential re-listings detected!*\n']
        for c in candidates[:5]:
            sign = '+' if c['price_diff_pct'] > 0 else ''
            lines.append(f'• {c.get("rooms")} {c.get("size_m2")}m² {c.get("neighborhood","")[:20]}')
            lines.append(f'  Old: €{c["price_a"]} {c["url_a"]}')
            lines.append(f'  New: €{c["price_b"]} ({sign}{c["price_diff_pct"]}%) {c["url_b"]}')
        req.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            json={'chat_id': CHAT_ID, 'text': '\n'.join(lines),
                  'parse_mode': 'Markdown', 'disable_web_page_preview': True}, timeout=10)

    return len(candidates)


if __name__ == '__main__':
    run()
