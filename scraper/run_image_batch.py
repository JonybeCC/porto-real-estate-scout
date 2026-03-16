#!/usr/bin/env python3
"""
Image Analysis Batch Runner — JBizz Assistant 🦞

Designed to be called from a cron job. Processes BATCH_SIZE listings per run,
saves progress, and exits. The cron job re-fires on a schedule until all done.

This way: no single long-running process that can be SIGTERM'd by gateway restarts.
Each batch takes ~3-5 minutes max. State is always on disk between runs.

Usage:
  python3 run_image_batch.py              # process next BATCH_SIZE unscored listings
  python3 run_image_batch.py --status     # show pending count without processing

Exit codes:
  0 = all done (nothing remaining)
  2 = batch processed, more remain
  1 = error
"""

import subprocess, sys, json, os
from datetime import datetime

BATCH_SIZE   = 10
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
DETAILS_FILE = os.path.join(SCRIPT_DIR, '../data/listing_details_zenrows.json')
IMAGE_FILE   = os.path.join(SCRIPT_DIR, '../data/image_analysis.json')


def count_pending() -> int:
    try:
        with open(DETAILS_FILE) as f:
            details = json.load(f)
        try:
            with open(IMAGE_FILE) as f:
                existing = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            existing = {}

        pending = 0
        for d in details:
            lid    = d['id']
            photos = d.get('photo_urls') or d.get('unblurred_photos') or []
            if not isinstance(photos, list) or len(photos) < 3:
                continue
            entry = existing.get(lid, {})
            # Needs work if: no score at all, or has v4 score (missing area_quality_score)
            if not entry.get('score') or 'area_quality_score' not in entry:
                pending += 1
        return pending
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f'❌ Error reading data files: {e}')
        return -1


def main():
    if '--status' in sys.argv:
        n = count_pending()
        print(f'📊 Pending image analysis: {n} listings')
        return 0

    print(f'🦞 Image Batch Runner — {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}')

    pending = count_pending()
    if pending <= 0:
        print('✅ All listings already scored — nothing to do')
        return 0

    print(f'📦 {pending} listings pending | processing next {min(pending, BATCH_SIZE)}')

    # Each batch of 10 listings takes ~3-5 min. Hard cap at 8 min to stay safe.
    BATCH_TIMEOUT = 480  # 8 minutes per batch
    try:
        result = subprocess.run(
            [sys.executable, os.path.join(SCRIPT_DIR, 'analyze_images.py'),
             '--full', '--batch', str(BATCH_SIZE)],
            cwd=os.path.join(SCRIPT_DIR, '..'),
            timeout=BATCH_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        print(f'⚠️  Batch timed out after {BATCH_TIMEOUT}s — progress was checkpointed, re-run to continue')
        return 2  # treat as "more remain"
    except OSError as e:
        print(f'❌ Could not start analyze_images.py: {e}')
        return 1

    remaining = count_pending()
    print(f'\n📊 After batch: {remaining} listings still pending')

    if remaining > 0:
        print(f'⏭️  More work remains — re-run to continue (or wait for next cron fire)')
        return 2  # signal to caller: more work remains
    else:
        print('🎉 All listings scored!')
        return 0


if __name__ == '__main__':
    sys.exit(main())
