#!/usr/bin/env python3
"""
Push listings to Google Sheets — Created by JBizz Assistant 🦞
"""
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

CREDS_FILE = '/root/.openclaw/credentials/google-service-account.json'
JSON_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
SHEET_URL = 'https://docs.google.com/spreadsheets/d/1Ljk9s45BovbRfk1QIUGgGxbjnpIgs2F52rJjDNGQOQI/edit'
SHEET_NAME = 'Listings'

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def main():
    # Auth
    print('🔑 Authenticating...')
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)

    # Open sheet
    print('📊 Opening spreadsheet...')
    sh = gc.open_by_url(SHEET_URL)

    # Get or create worksheet
    try:
        ws = sh.worksheet(SHEET_NAME)
        ws.clear()
        print(f'  Cleared existing sheet: {SHEET_NAME}')
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=500, cols=20)
        print(f'  Created new sheet: {SHEET_NAME}')

    # Load data
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        listings = json.load(f)
    print(f'📦 Loaded {len(listings)} listings')

    # Headers
    headers = [
        'ID', 'City', 'Rooms', 'Price (€/mo)', 'Size (m²)', 'Floor',
        'Neighborhood', 'Street', 'Tags', 'Description', 'URL',
        'Photos', 'Date Scraped'
    ]

    # Build rows
    rows = [headers]
    for l in listings:
        rows.append([
            l.get('id', ''),
            l.get('city', ''),
            l.get('rooms', ''),
            l.get('price_eur', ''),
            l.get('size_m2', ''),
            l.get('floor', ''),
            l.get('neighborhood', ''),
            l.get('street', ''),
            l.get('tags', ''),
            l.get('description', '')[:200],
            l.get('url', ''),
            l.get('photos', '').split('|')[0] if l.get('photos') else '',  # First photo only
            l.get('date_scraped', ''),
        ])

    # Push all at once
    print(f'⬆️  Pushing {len(rows)-1} rows...')
    ws.update(rows, value_input_option='RAW')

    # Format header row
    ws.format('A1:M1', {
        'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
        'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
    })

    # Freeze header
    sh.batch_update({'requests': [{
        'updateSheetProperties': {
            'properties': {'sheetId': ws.id, 'gridProperties': {'frozenRowCount': 1}},
            'fields': 'gridProperties.frozenRowCount'
        }
    }]})

    # Auto-resize columns
    sh.batch_update({'requests': [{
        'autoResizeDimensions': {
            'dimensions': {'sheetId': ws.id, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 13}
        }
    }]})

    print(f'\n✅ Done! {len(listings)} listings pushed to Google Sheets')
    print(f'🔗 {SHEET_URL}')

    # Stats
    prices = [l['price_eur'] for l in listings if l.get('price_eur')]
    sizes = [l['size_m2'] for l in listings if l.get('size_m2')]
    print(f'\n📊 Summary:')
    print(f'  Listings: {len(listings)}')
    print(f'  Price: €{min(prices):,} – €{max(prices):,} | Avg €{sum(prices)//len(prices):,}')
    print(f'  Size: {min(sizes)}m² – {max(sizes)}m² | Avg {sum(sizes)//len(sizes)}m²')
    print(f'  Scraped: {datetime.now().strftime("%Y-%m-%d %H:%M")}')

if __name__ == '__main__':
    main()
