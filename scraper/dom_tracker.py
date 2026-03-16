#!/usr/bin/env python3
"""
Days on Market Tracker — JBizz Assistant 🦞
Tracks how long each listing has been visible and detects price changes.

Run daily after scraping. Updates dom_tracker.json.
"""

import json
import os
from datetime import datetime, date

LISTINGS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
DOM_FILE       = '/root/.openclaw/workspace/projects/real-estate/data/dom_tracker.json'
TODAY          = date.today().isoformat()


def load_json(path: str, default):
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def days_between(d1: str, d2: str) -> int:
    try:
        return (date.fromisoformat(d2) - date.fromisoformat(d1)).days
    except (ValueError, TypeError):
        return 0


def main():
    print('🦞 Days on Market Tracker')
    print(f'📅 {TODAY}')
    print('=' * 50)

    listings = load_json(LISTINGS_FILE, [])
    tracker  = load_json(DOM_FILE, {})

    print(f'📦 {len(listings)} active listings | {len(tracker)} in tracker')

    active_ids = {l['id'] for l in listings}
    new_count = 0
    price_changes = []
    gone_now = []

    # ── Update / add active listings ──────────────────────────────────────────
    for listing in listings:
        lid    = listing['id']
        price  = listing.get('price_eur') or 0
        scraped = listing.get('date_scraped', TODAY)

        if lid not in tracker:
            # New listing
            tracker[lid] = {
                'first_seen':    scraped,
                'last_seen':     TODAY,
                'days_on_market': days_between(scraped, TODAY),
                'price_history': [{'date': scraped, 'price': price}] if price else [],
                'status':        'active',
            }
            new_count += 1
        else:
            entry = tracker[lid]
            entry['last_seen'] = TODAY
            entry['status']    = 'active'
            entry['days_on_market'] = days_between(entry['first_seen'], TODAY)

            # Price change detection
            history = entry.get('price_history', [])
            if price and (not history or history[-1]['price'] != price):
                old_price = history[-1]['price'] if history else None
                history.append({'date': TODAY, 'price': price})
                entry['price_history'] = history
                if old_price and old_price != price:
                    change = price - old_price
                    price_changes.append({
                        'id': lid,
                        'old': old_price,
                        'new': price,
                        'change': change,
                        'pct': round(change / old_price * 100, 1),
                    })

    # ── Mark missing listings as "gone" ───────────────────────────────────────
    for lid, entry in tracker.items():
        if lid not in active_ids and entry.get('status') == 'active':
            entry['status'] = 'gone'
            entry['gone_since'] = TODAY
            gone_now.append(lid)

    # ── Remove listings gone > 7 days ─────────────────────────────────────────
    to_remove = []
    for lid, entry in tracker.items():
        if entry.get('status') == 'gone' and entry.get('gone_since'):
            gone_days = days_between(entry['gone_since'], TODAY)
            if gone_days > 7:
                to_remove.append(lid)
    for lid in to_remove:
        del tracker[lid]

    # ── Save ──────────────────────────────────────────────────────────────────
    with open(DOM_FILE, 'w', encoding='utf-8') as f:
        json.dump(tracker, f, ensure_ascii=False, indent=2)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f'\n✅ Tracker updated:')
    print(f'   {new_count} new listings added')
    print(f'   {len(gone_now)} listings marked gone: {gone_now}')
    print(f'   {len(to_remove)} old "gone" listings removed')

    if price_changes:
        print(f'\n💸 Price changes detected ({len(price_changes)}):')
        for pc in price_changes:
            arrow = '📉' if pc['change'] < 0 else '📈'
            print(f'   {arrow} {pc["id"]}: €{pc["old"]} → €{pc["new"]} ({pc["pct"]:+.1f}%)')
    else:
        print(f'\n   No price changes detected today.')

    # ── Stats ─────────────────────────────────────────────────────────────────
    active = [e for e in tracker.values() if e.get('status') == 'active']
    doms = [e['days_on_market'] for e in active if e.get('days_on_market') is not None]
    if doms:
        print(f'\n📊 DOM stats (active {len(active)} listings):')
        print(f'   Avg: {sum(doms)/len(doms):.1f} days | Max: {max(doms)} | Min: {min(doms)}')
        stale = [lid for lid, e in tracker.items() if e.get('status') == 'active' and e.get('days_on_market', 0) > 45]
        if stale:
            print(f'   ⏰ Stale (>45 days): {len(stale)} listings — potential negotiation leverage')

    return tracker


if __name__ == '__main__':
    main()
