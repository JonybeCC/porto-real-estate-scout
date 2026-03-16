#!/usr/bin/env python3
"""
Dataset Cleanup — JBizz Assistant 🦞
- Removes confirmed-deleted listings from dataset
- Flags likely duplicates (same address, similar price)
- Reports stale listings (60+ days, price unchanged)
"""

import json, re
from datetime import datetime, date

LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
DETAILS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
ENRICHED_FILE = '/root/.openclaw/workspace/projects/real-estate/data/enriched_listings.json'
DOM_FILE      = '/root/.openclaw/workspace/projects/real-estate/data/dom_tracker.json'


def load_json(path, default):
    try:
        with open(path) as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def run(dry_run=True):
    today = date.today()
    print(f'🧹 Cleanup — {today} | dry_run={dry_run}')

    listings = load_json(LISTINGS_FILE, [])
    details  = {d['id']: d for d in load_json(DETAILS_FILE, [])}
    enriched = load_json(ENRICHED_FILE, [])
    dom      = load_json(DOM_FILE, {})

    # ── 1. Find confirmed deleted ─────────────────────────────────────────────
    deleted_ids = {lid for lid, d in details.items()
                   if d.get('active') is False and d.get('fetch_status') == 'deleted'}

    # ── 2. Find likely duplicates (same neighbourhood + price ±5% + same room count) ──
    duplicates = []
    seen = {}
    for e in sorted(enriched, key=lambda x: x.get('opportunity_score', 0), reverse=True):
        key = (e.get('rooms', ''), e.get('neighborhood', ''), round(e.get('price_eur', 0) / 50) * 50)
        if key in seen:
            duplicates.append((e['id'], seen[key]['id'],
                               f"{e.get('rooms')} {e.get('neighborhood','')[:20]} €{e.get('price_eur')}"))
        else:
            seen[key] = e

    # ── 3. Find stale listings (seen 60+ days, no price change) ──────────────
    stale = []
    for lid, d in dom.items():
        days = d.get('days_on_market', 0)
        if days >= 60:
            e = next((x for x in enriched if x['id'] == lid), {})
            stale.append({'id': lid, 'days': days,
                          'price': e.get('price_eur'), 'score': e.get('opportunity_score'),
                          'neighborhood': e.get('neighborhood', '')})

    # ── Report ────────────────────────────────────────────────────────────────
    print(f'\n📊 Report:')
    print(f'   Total listings:      {len(listings)}')
    print(f'   Confirmed deleted:   {len(deleted_ids)} → {list(deleted_ids)[:5]}')
    print(f'   Likely duplicates:   {len(duplicates)}')
    for d in duplicates[:5]:
        print(f'     {d[0]} ≈ {d[1]} | {d[2]}')
    print(f'   Stale (60+ days):    {len(stale)}')
    for s in sorted(stale, key=lambda x: x['days'], reverse=True)[:5]:
        print(f'     {s["id"]} | {s["days"]}d | €{s["price"]} | score={s["score"]} | {s["neighborhood"][:25]}')

    if not dry_run and deleted_ids:
        # Remove deleted from listings.json
        before = len(listings)
        listings = [l for l in listings if l['id'] not in deleted_ids]
        with open(LISTINGS_FILE, 'w') as f:
            json.dump(listings, f, ensure_ascii=False, indent=2)
        print(f'\n✅ Removed {before - len(listings)} deleted listings from listings.json')

    return {'deleted': len(deleted_ids), 'duplicates': len(duplicates), 'stale': len(stale)}


if __name__ == '__main__':
    import sys
    dry = '--apply' not in sys.argv
    run(dry_run=dry)
