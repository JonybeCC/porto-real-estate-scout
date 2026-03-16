#!/usr/bin/env python3
"""
Apartment Condition Assessor — Created by JBizz Assistant 🦞
Uses GPT-4o vision + description to assess each listing's condition,
then writes results to a new 'Condition' tab in Google Sheets.
"""

import json
import re
import requests
import base64
import os
import time
from io import BytesIO
from PIL import Image
from datetime import datetime
from openai import OpenAI
import gspread
from google.oauth2.service_account import Credentials

JSON_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
CREDS_FILE = '/root/.openclaw/credentials/google-service-account.json'
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1Ljk9s45BovbRfk1QIUGgGxbjnpIgs2F52rJjDNGQOQI/edit'
CONDITION_FILE = '/root/.openclaw/workspace/projects/real-estate/data/condition_scores.json'

OPENAI_KEY = os.environ.get('OPENAI_API_KEY')
client = OpenAI(api_key=OPENAI_KEY)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


def fetch_image_b64(url):
    try:
        r = requests.get(url, timeout=15, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.idealista.pt/'
        })
        img = Image.open(BytesIO(r.content)).convert('RGB')
        img.thumbnail((640, 480), Image.LANCZOS)
        buf = BytesIO()
        img.save(buf, format='JPEG', quality=80)
        return base64.b64encode(buf.getvalue()).decode('utf-8')
    except Exception:
        return None


def assess_listing(listing):
    """Use GPT-4o to assess condition from photos + description"""
    photos = [p for p in listing.get('photos', '').split('|') if p and 'idealista.pt/blur' in p]
    description = listing.get('description', '')

    # Build image content
    image_content = []
    for url in photos[:3]:
        b64 = fetch_image_b64(url)
        if b64:
            image_content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{b64}", "detail": "low"}
            })

    prompt = f"""You are a real estate expert assessing a rental apartment in Porto, Portugal.

Listing details:
- Type: {listing.get('rooms')} | Size: {listing.get('size_m2')}m² | Price: €{listing.get('price_eur')}/mo
- Floor: {listing.get('floor')} | Neighborhood: {listing.get('neighborhood')}
- Tags: {listing.get('tags', 'none')}
- Description: {description[:500]}

{"Photos are attached." if image_content else "No photos available."}

Assess this apartment and respond in this EXACT format:
CONDITION_SCORE: [1-10, where 1=poor/rundown, 10=brand new/luxury]
RENOVATION_STATUS: [Original/Partially Renovated/Fully Renovated/New Build]
NATURAL_LIGHT: [Poor/Average/Good/Excellent]
FURNISHING: [Unfurnished/Partially Furnished/Fully Furnished]
KEY_FEATURES: [comma-separated list of standout features, max 5]
RED_FLAGS: [comma-separated issues or concerns, or "None"]
VALUE_RATING: [Poor/Fair/Good/Excellent value for price]
SUMMARY: [2-3 sentence honest assessment]"""

    messages = [{
        "role": "user",
        "content": ([{"type": "text", "text": prompt}] + image_content) if image_content else prompt
    }]

    try:
        response = client.chat.completions.create(
            model='gpt-4o-mini',
            messages=messages,
            max_tokens=400
        )
        return parse_assessment(response.choices[0].message.content.strip())
    except Exception as e:
        return {'error': str(e)}


def parse_assessment(text):
    result = {}
    patterns = {
        'condition_score': r'CONDITION_SCORE:\s*(\d+)',
        'renovation_status': r'RENOVATION_STATUS:\s*(.+)',
        'natural_light': r'NATURAL_LIGHT:\s*(.+)',
        'furnishing': r'FURNISHING:\s*(.+)',
        'key_features': r'KEY_FEATURES:\s*(.+)',
        'red_flags': r'RED_FLAGS:\s*(.+)',
        'value_rating': r'VALUE_RATING:\s*(.+)',
        'summary': r'SUMMARY:\s*(.+?)(?=\n[A-Z_]+:|$)',
    }
    for key, pattern in patterns.items():
        m = re.search(pattern, text, re.DOTALL)
        if m:
            val = m.group(1).strip()
            if key == 'condition_score':
                try:
                    val = int(val)
                except (ValueError, TypeError):
                    val = 0
            result[key] = val
    result['raw'] = text
    return result


def score_emoji(score):
    if score >= 9: return '⭐⭐⭐⭐⭐'
    if score >= 7: return '⭐⭐⭐⭐'
    if score >= 5: return '⭐⭐⭐'
    if score >= 3: return '⭐⭐'
    return '⭐'


def main():
    print('🦞 JBizz Assistant — Apartment Condition Assessor')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 55)

    # Load listings
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        listings = json.load(f)
    print(f'📦 {len(listings)} listings to assess')

    # Load existing assessments if any (to avoid re-running)
    existing = {}
    try:
        with open(CONDITION_FILE, 'r') as f:
            existing = {a['id']: a for a in json.load(f)}
        print(f'   ({len(existing)} already assessed)')
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    assessments = list(existing.values())
    assessed_ids = set(existing.keys())

    to_assess = [l for l in listings if l['id'] not in assessed_ids]
    print(f'   {len(to_assess)} new assessments needed')
    print()

    for i, listing in enumerate(to_assess):
        lid = listing['id']
        print(f'  [{i+1}/{len(to_assess)}] {listing.get("rooms")} {listing.get("size_m2")}m² €{listing.get("price_eur")}/mo — {listing.get("neighborhood")}')

        assessment = assess_listing(listing)
        assessment['id'] = lid
        assessment['url'] = listing.get('url', '')
        assessment['assessed_at'] = datetime.now().strftime('%Y-%m-%d')

        score = assessment.get('condition_score', 0)
        print(f'       {score_emoji(score)} Score: {score}/10 | {assessment.get("renovation_status","")} | {assessment.get("value_rating","")}')

        assessments.append(assessment)
        assessed_ids.add(lid)

        # Save progress every 10
        if (i + 1) % 10 == 0:
            with open(CONDITION_FILE, 'w') as f:
                json.dump(assessments, f, ensure_ascii=False, indent=2)
            print(f'  💾 Progress saved ({i+1} done)')

        time.sleep(0.5)  # rate limit

    # Final save
    with open(CONDITION_FILE, 'w') as f:
        json.dump(assessments, f, ensure_ascii=False, indent=2)
    print(f'\n✅ All {len(assessments)} assessments complete')

    # Push to Google Sheets — new tab
    print('\n📊 Pushing to Google Sheets...')
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SHEET_URL)

    # Create/clear Condition tab
    try:
        ws = sh.worksheet('Condition')
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title='Condition', rows=500, cols=15)

    headers = [
        'ID', 'Score', '⭐', 'Renovation', 'Natural Light', 'Furnishing',
        'Key Features', 'Red Flags', 'Value Rating', 'Summary', 'Assessed', 'URL'
    ]

    rows = [headers]
    # Sort by score descending
    sorted_assessments = sorted(assessments, key=lambda x: x.get('condition_score', 0) or 0, reverse=True)

    for a in sorted_assessments:
        score = a.get('condition_score', '')
        rows.append([
            a.get('id', ''),
            score,
            score_emoji(score) if isinstance(score, int) else '',
            a.get('renovation_status', ''),
            a.get('natural_light', ''),
            a.get('furnishing', ''),
            a.get('key_features', ''),
            a.get('red_flags', ''),
            a.get('value_rating', ''),
            a.get('summary', '')[:300],
            a.get('assessed_at', ''),
            a.get('url', ''),
        ])

    ws.update(rows, value_input_option='RAW')

    # Format header
    ws.format('A1:L1', {
        'backgroundColor': {'red': 0.1, 'green': 0.4, 'blue': 0.1},
        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
    })
    sh.batch_update({'requests': [{
        'updateSheetProperties': {
            'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1}},
            'fields': 'gridProperties.frozenRowCount'
        }
    }]})

    print(f'✅ Condition tab updated with {len(assessments)} assessments')
    print(f'🔗 {SHEET_URL}')

    # Stats
    scores = [a.get('condition_score') for a in assessments if isinstance(a.get('condition_score'), int)]
    if scores:
        print(f'\n📊 Score distribution:')
        for threshold, label in [(9, '⭐⭐⭐⭐⭐ (9-10)'), (7, '⭐⭐⭐⭐ (7-8)'), (5, '⭐⭐⭐ (5-6)'), (3, '⭐⭐ (3-4)'), (0, '⭐ (1-2)')]:
            count = sum(1 for s in scores if s >= threshold and s < (threshold + 2 if threshold < 9 else 11))
            if count:
                print(f'  {label}: {count} listings')


if __name__ == '__main__':
    main()
