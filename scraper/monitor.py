#!/usr/bin/env python3
"""
Monitoring system for the real estate scraper
Checks if the scraper is working and sends alerts if it fails
"""

import json
import requests
import smtplib
import ssl
from datetime import datetime
import os

# Configuration
MONITORING_CONFIG = {
    'CHECK_INTERVAL_MINUTES': 30,
    'MAX_FAILURES_BEFORE_ALERT': 3,
    'ALERT_RECIPIENTS': [
        'user@example.com',  # Replace with actual email
    ],
    
    # ZenRows API Key (same as in fetch_zenrows.py)
    'ZENROWS_API_KEY': os.environ.get('ZENROWS_API_KEY', 'a19f204d97b9578f8d82bd749ac175bd5383dd6e'),
    
    # Monitoring data file
    'MONITOR_DATA_FILE': '/root/.openclaw/workspace/projects/real-estate/data/monitor_data.json',
    
    # Idealista test URL
    'TEST_URL': 'https://www.idealista.pt/areas/arrendar-casas/com-preco-max_3100,preco-min_1650,t2,t3/',
}

# Telegram alert configuration
TELEGRAM_API_TOKEN = os.environ.get('TELEGRAM_API_TOKEN', '8520877947:AAFYkG_J6HmBc5n3HOHw4xjHItlBLS6F5UM')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '520980639')

def send_telegram_alert(message):
    """Send alert via Telegram"""
    if TELEGRAM_API_TOKEN == 'YOUR_TELEGRAM_BOT_TOKEN':
        print(f'⚠️  Telegram not configured: {message}')
        return
    
    try:
        url = f'https://api.telegram.org/bot{TELEGRAM_API_TOKEN}/sendMessage'
        params = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'Markdown'
        }
        response = requests.post(url, params=params, timeout=10)
        if response.status_code == 200:
            print(f'✅ Telegram alert sent')
        else:
            print(f'❌ Telegram API error: {response.status_code}')
    except Exception as e:
        print(f'❌ Telegram alert failed: {e}')


def send_alert(subject, message):
    """Send alert via multiple channels"""
    print(f'🚨 {subject}')
    print(f'{message}')
    
    # Send Telegram alert
    send_telegram_alert(f'*{subject}*\n\n{message}')
    
    # Email would be configured here for production
    # send_email(subject, message)

def check_scraper_health():
    """Check if the scraper can access Idealista"""
    print(f'🔍 Checking scraper health at {datetime.now()}')
    
    # Test ZenRows API
    params = {
        'url': MONITORING_CONFIG['TEST_URL'],
        'apikey': MONITORING_CONFIG['ZENROWS_API_KEY'],
        'js_render': 'true',
        'json_response': 'true',
        'premium_proxy': 'true',
    }
    
    try:
        response = requests.get('https://api.zenrows.com/v1/', params=params, timeout=60)
        
        if response.status_code == 200:
            print('✅ ZenRows connection successful')
            return True
        else:
            print(f'❌ ZenRows returned status {response.status_code}')
            return False
    except Exception as e:
        print(f'❌ ZenRows connection failed: {e}')
        return False

def load_monitor_data():
    """Load monitoring data from file"""
    try:
        with open(MONITORING_CONFIG['MONITOR_DATA_FILE'], 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            'failures': 0,
            'last_check': None,
            'last_success': None,
            'alert_sent': False,
        }

def save_monitor_data(data):
    """Save monitoring data to file"""
    try:
        with open(MONITORING_CONFIG['MONITOR_DATA_FILE'], 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f'❌ Failed to save monitor data: {e}')

def main():
    """Main monitoring function"""
    print('🔍 Real Estate Scraper Monitoring System')
    
    # Load existing data
    data = load_monitor_data()
    
    # Check scraper health
    is_healthy = check_scraper_health()
    
    # Update monitoring data
    current_time = datetime.now().isoformat()
    
    if is_healthy:
        data['failures'] = 0
        data['last_success'] = current_time
        data['alert_sent'] = False
        print('✅ Scraper is healthy')
    else:
        data['failures'] += 1
        data['last_check'] = current_time
        print(f'⚠️  Scraper failure #{data["failures"]}')
        
        # Check if we should send an alert
        if data['failures'] >= MONITORING_CONFIG['MAX_FAILURES_BEFORE_ALERT'] and not data['alert_sent']:
            subject = 'Real Estate Scraper Failure Alert'
            message = (f'The real estate scraper has failed {data["failures"]} consecutive times.\n\n'
                     f'Last successful check: {data.get("last_success", "Never")}\n'
                     f'Last check: {data["last_check"]}\n\n'
                     f'Please check the scraper configuration and Idealista access.\n\n'
                     f'Monitor data file: {MONITORING_CONFIG["MONITOR_DATA_FILE"]}')
            send_alert(subject, message)
            data['alert_sent'] = True
    
    # Save updated data
    save_monitor_data(data)
    
    print('📊 Monitoring check complete')

if __name__ == '__main__':
    main()