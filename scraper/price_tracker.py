#!/usr/bin/env python3
"""
Price History Tracker — JBizz Assistant 🦞
Tracks price changes per listing across runs.
Sends Telegram alerts for:
  - Price drops ≥5%
  - New high-score listings (score ≥75)
  - Listings back on market after being gone
"""

import json, os
from datetime import datetime, date

ENRICHED_FILE   = '/root/.openclaw/workspace/projects/real-estate/data/enriched_listings.json'
PRICE_HIST_FILE = '/root/.openclaw/workspace/projects/real-estate/data/price_history.json'
DOM_FILE        = '/root/.openclaw/workspace/projects/real-estate/data/dom_tracker.json'

BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT_ID   = '520980639'
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1Ljk9s45BovbRfk1QIUGgGxbjnpIgs2F52rJjDNGQOQI/edit'

ALERT_SCORE_THRESHOLD = 75   # alert when new listing scores ≥ this
ALERT_DROP_PCT        = 5.0  # alert when price drops ≥ this %


def send_telegram(msg: str):
    if not BOT_TOKEN:
        print(f"[TELEGRAM] {msg[:100]}")
        return
    import requests
    requests.post(
        f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage',
        json={'chat_id': CHAT_ID, 'text': msg, 'parse_mode': 'Markdown',
              'disable_web_page_preview': True},
        timeout=10
    )


def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return default


def run():
    today = str(date.today())
    print(f'💰 Price Tracker — {today}')

    enriched = load_json(ENRICHED_FILE, [])
    history  = load_json(PRICE_HIST_FILE, {})  # {id: [{date, price, score}]}

    alerts = []
    new_high_score = []
    price_drops    = []
    back_on_market = []

    for listing in enriched:
        lid   = listing['id']
        price = listing.get('price_eur', 0)
        score = listing.get('opportunity_score', 0)

        if lid not in history:
            history[lid] = []

        prev_entries = history[lid]
        today_entry  = {'date': today, 'price': price, 'score': score}

        # Skip if already logged today
        if prev_entries and prev_entries[-1]['date'] == today:
            continue

        # New listing (never seen before)
        if not prev_entries:
            history[lid] = [today_entry]
            if score >= ALERT_SCORE_THRESHOLD:
                new_high_score.append(listing)
            continue

        prev = prev_entries[-1]

        # Price drop check
        if prev['price'] and price < prev['price']:
            drop_pct = (prev['price'] - price) / prev['price'] * 100
            if drop_pct >= ALERT_DROP_PCT:
                price_drops.append({**listing, 'drop_pct': round(drop_pct, 1),
                                    'old_price': prev['price']})

        # Score improved significantly (re-analysis updated it)
        if score >= ALERT_SCORE_THRESHOLD and prev['score'] < ALERT_SCORE_THRESHOLD:
            new_high_score.append(listing)

        history[lid].append(today_entry)

    # Save updated history
    with open(PRICE_HIST_FILE, 'w') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    # Print summary
    print(f'   Tracked: {len(history)} listings')
    print(f'   Price drops: {len(price_drops)}')
    print(f'   New high-score: {len(new_high_score)}')

    # Send alerts
    if price_drops:
        lines = ['💸 *Price Drops Detected!*\n']
        for l in price_drops[:5]:
            lines.append(f'↘️ *{l["drop_pct"]}% drop* | {l.get("rooms")} {l.get("size_m2")}m² '
                         f'€{l["old_price"]} → €{l["price_eur"]}/mo [score: {l["opportunity_score"]}]')
            lines.append(f'   _{l.get("neighborhood","")[:30]}_ — {l.get("url","")}')
        send_telegram('\n'.join(lines))
        print(f'   📨 Price drop alert sent')

    if new_high_score:
        lines = ['🌟 *New High-Score Listings!*\n']
        for l in new_high_score[:5]:
            sv = '🌊' if l.get('sea_view') else ''
            g  = f'🚗×{l.get("parking_spaces",0)}' if l.get('has_garage') else ''
            lines.append(f'*[{l["opportunity_score"]}]* {l.get("rooms")} {l.get("size_m2")}m² '
                         f'€{l.get("price_eur")}/mo {sv}{g}')
            lines.append(f'   _{l.get("neighborhood","")[:30]}_ — {l.get("url","")}')
        send_telegram('\n'.join(lines))
        print(f'   📨 High-score alert sent')

    if not price_drops and not new_high_score:
        print('   No alerts to send — no significant changes')

    return len(price_drops), len(new_high_score)


if __name__ == '__main__':
    run()
