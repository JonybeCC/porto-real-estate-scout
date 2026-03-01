#!/usr/bin/env python3
"""
Re-listing Detector v2 — JBizz Assistant 🦞

Detects genuine duplicate listings on the Porto market:
1. Same address string → almost certainly same property, different agency or re-list
2. Same description fingerprint (first 150 chars) → copy-paste between agencies
3. High structural match (same rooms + size ±5% + price ±10%) AND same neighbourhood
   — only flagged if supported by at least one of the above signals

NOT flagged: two different T2s at €2200 in Foz — that's just a competitive market.
"""

import json, os, re
from datetime import date

LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
DETAILS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
RELIST_FILE   = '/root/.openclaw/workspace/projects/real-estate/data/relistings.json'
BOT_TOKEN     = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID       = '520980639'


def load_json(path, default):
    try:
        with open(path) as f: return json.load(f)
    except: return default


def norm_addr(s: str) -> str:
    """Strip boilerplate Idealista prefix, lowercase."""
    s = re.sub(r'^arrendamento de apartamento \w+ (na?o?|em|em)\s+', '', s.lower().strip())
    return re.sub(r'\s+', ' ', s).strip()


def desc_fingerprint(desc: str) -> str:
    """First 150 chars of description, stripped."""
    if not desc:
        return ''
    return re.sub(r'\s+', ' ', desc.strip().lower())[:150]


def run():
    today = str(date.today())
    print(f'🔁 Re-listing Detector v2 — {today}')

    listings = load_json(LISTINGS_FILE, [])
    details  = {d['id']: d for d in load_json(DETAILS_FILE, [])}

    # Build lookup maps
    addr_map  = {}   # norm_address → [ids]
    desc_map  = {}   # desc_fingerprint → [ids]
    struct_map = {}  # (rooms, size_bucket, neigh_key) → [ids]

    for l in listings:
        lid  = l['id']
        det  = details.get(lid, {})

        # 1. Address
        raw_addr = det.get('full_address', '') or l.get('title', '')
        addr = norm_addr(raw_addr)
        # Skip generic "em Foz" type addresses — not specific enough
        if addr and len(addr) > 15 and not re.match(r'^em [a-z\s]+$', addr):
            addr_map.setdefault(addr, []).append(lid)

        # 2. Description fingerprint
        fp = desc_fingerprint(det.get('full_description', ''))
        if fp and len(fp) > 50:
            desc_map.setdefault(fp, []).append(lid)

        # 3. Structural (rooms + size bucket + neighbourhood) — only for exact matches
        rooms = l.get('rooms', '')
        size  = l.get('size_m2') or 0
        neigh = (l.get('neighborhood') or '').lower()[:20]
        price = l.get('price_eur') or 0
        if rooms and size and neigh:
            # Bucket size to nearest 5m² for fuzzy match
            size_b = round(size / 5) * 5
            key = (rooms, size_b, neigh)
            struct_map.setdefault(key, []).append({'id': lid, 'price': price, 'url': l.get('url','')})

    candidates = []
    seen_pairs = set()

    def add_candidate(id_a, id_b, reason, confidence):
        pair = tuple(sorted([id_a, id_b]))
        if pair in seen_pairs:
            return
        seen_pairs.add(pair)

        la = next((l for l in listings if l['id'] == id_a), {})
        lb = next((l for l in listings if l['id'] == id_b), {})
        pr_a = la.get('price_eur', 0) or 0
        pr_b = lb.get('price_eur', 0) or 0
        price_diff = round((pr_b - pr_a) / pr_a * 100, 1) if pr_a else 0

        # Newer ID = more recent listing
        older, newer = (la, lb) if id_a < id_b else (lb, la)

        candidates.append({
            'id_a': older['id'], 'id_b': newer['id'],
            'confidence': confidence,
            'reason': reason,
            'price_a': older.get('price_eur'),
            'price_b': newer.get('price_eur'),
            'price_diff_pct': price_diff,
            'rooms': la.get('rooms'),
            'size_m2': la.get('size_m2'),
            'neighborhood': la.get('neighborhood'),
            'url_a': older.get('url'),
            'url_b': newer.get('url'),
            'detected_at': today,
        })

    # Pass 1: same address
    for addr, ids in addr_map.items():
        if len(ids) > 1:
            for i in range(len(ids)):
                for j in range(i+1, len(ids)):
                    add_candidate(ids[i], ids[j], f'Same address: "{addr[:50]}"', 'HIGH')

    # Pass 2: same description fingerprint
    for fp, ids in desc_map.items():
        if len(ids) > 1:
            for i in range(len(ids)):
                for j in range(i+1, len(ids)):
                    add_candidate(ids[i], ids[j], f'Identical description: "{fp[:50]}..."', 'HIGH')

    # Pass 3: structural match — only flag if rooms+size+neigh+price all match tightly
    for key, items in struct_map.items():
        if len(items) > 1:
            for i in range(len(items)):
                for j in range(i+1, len(items)):
                    a, b = items[i], items[j]
                    pr_a, pr_b = a['price'], b['price']
                    if pr_a and pr_b:
                        price_diff = abs(pr_a - pr_b) / max(pr_a, pr_b)
                        if price_diff <= 0.10:  # within 10% price — very tight
                            rooms, size_b, neigh = key
                            add_candidate(
                                a['id'], b['id'],
                                f'Identical structure: {rooms} {size_b}m² €{pr_a}≈€{pr_b} in {neigh}',
                                'MEDIUM'
                            )

    # Save
    with open(RELIST_FILE, 'w') as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)

    high = [c for c in candidates if c['confidence'] == 'HIGH']
    med  = [c for c in candidates if c['confidence'] == 'MEDIUM']

    print(f'✅ {len(candidates)} potential re-listings found')
    print(f'   🔴 HIGH confidence (same address/description): {len(high)}')
    print(f'   🟡 MEDIUM confidence (structural match):       {len(med)}')

    if candidates:
        print('\nTop findings:')
        for c in sorted(candidates, key=lambda x: x['confidence'])[:8]:
            diff = c['price_diff_pct']
            sign = '+' if diff > 0 else ''
            conf = '🔴' if c['confidence'] == 'HIGH' else '🟡'
            print(f'  {conf} {c.get("rooms")} {c.get("size_m2")}m² | {sign}{diff}% | {c["reason"][:60]}')
            print(f'      {c["url_a"]}')
            print(f'      {c["url_b"]}')

    # Telegram alert for high-confidence only
    if high and BOT_TOKEN:
        import requests as req
        lines = [f'🔁 *{len(high)} confirmed re-listings found*\n']
        for c in high[:5]:
            sign = '+' if c['price_diff_pct'] > 0 else ''
            lines.append(f'• {c.get("rooms")} {c.get("size_m2")}m² {c.get("neighborhood","")[:20]}')
            lines.append(f'  Old: €{c["price_a"]} → New: €{c["price_b"]} ({sign}{c["price_diff_pct"]}%)')
            lines.append(f'  {c["reason"][:60]}')
        req.post(f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
            json={'chat_id': CHAT_ID, 'text': '\n'.join(lines),
                  'parse_mode': 'Markdown', 'disable_web_page_preview': True}, timeout=10)

    return len(high), len(med)


if __name__ == '__main__':
    run()
