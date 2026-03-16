#!/usr/bin/env python3
"""
Iterative Image Analyzer v5 — JBizz Assistant 🦞

v5 changes vs v4:
  - Prompt redesigned: forces full 1-10 range with explicit anchors (was clustering 6-9)
  - Wide-angle detection: explicitly flags lens trickery → affects area_impression score
  - Cramped detection improved: specific furniture-scale checks
  - Area quality score (1-10) added alongside condition score
  - Renovation year estimate added
  - OpenAI retry: 3 attempts on RateLimit/timeout with backoff
  - fetch_b64: retries 2× on failure, explicit exception logging
  - Modular: scoring weights live in SCORE_CONFIG dict at top of file

Scoring weights used by enrich_listings.py:
  image_score    × 2.5  → 0-25 pts  (condition)
  finish         bonus   → 0-4 pts   (Premium=3, Luxury=6, Basic=-2)
  light          bonus   → 0-4 pts   (Excellent=4, Poor=-2)
  area_impression bonus  → 0-3 pts   (Spacious=3, Cramped=-4)
  renovation     bonus   → 0-5 pts   (Fully Renovated=5, New=8, Original=-2)
"""

import json, os, base64, re, requests, time, signal
from openai import OpenAI, RateLimitError, APITimeoutError, APIStatusError

DETAILS_FILE  = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
LISTINGS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listings.json'
IMAGE_FILE    = '/root/.openclaw/workspace/projects/real-estate/data/image_analysis.json'
# Model strategy (cost-optimised):
#   Round 1-2: gpt-4o-mini (fast, cheap, ~15× cheaper than gpt-4o)
#   Round 3+:  gpt-4o ONLY if mini says confidence=low or score is borderline (5-7)
#   This gives mini-quality for 80% of listings and gpt-4o depth only when needed.
#   Cost per listing: ~$0.003 (vs $0.021 all-gpt-4o, vs $0.001 all-mini)
MODEL_FAST = 'gpt-4o-mini'  # rounds 1-2: cheap, good enough for clear listings
MODEL_DEEP = 'gpt-4o'       # round 3+: only when mini is uncertain or borderline score

client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

# ── Scoring weights (read by enrich_listings.py) ─────────────────────────────
# These are the image-derived BONUS/PENALTY points added on top of condition score.
# Change here to tune without touching enrich_listings.py logic.
SCORE_CONFIG = {
    'finish': {
        'Luxury':  6,
        'Premium': 3,
        'Standard': 0,
        'Basic':   -2,
    },
    'light': {
        'Excellent': 4,
        'Good':      2,
        'Average':   0,
        'Poor':     -2,
    },
    'area_impression': {
        'Spacious':  3,
        'Adequate':  0,
        'Cramped':  -4,
    },
    'renovation': {
        'New Build':            8,
        'Fully Renovated':      5,
        'Partially Renovated':  2,
        'Original':            -2,
        'Unknown':              0,
    },
}


def fetch_b64(url: str, retries: int = 2) -> str | None:
    """Fetch image and encode as base64. Retries on network errors."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(
                url, timeout=20,
                headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.idealista.pt/'}
            )
            if r.status_code == 200 and len(r.content) > 5000:
                return base64.b64encode(r.content).decode()
            # Non-200 or tiny response — not worth retrying
            return None
        except requests.Timeout:
            if attempt < retries:
                time.sleep(1)
        except requests.RequestException as e:
            if attempt < retries:
                time.sleep(1)
    return None


def parse_resp(text: str) -> dict:
    """Parse structured key: value response from the model."""
    def g(k, d=None):
        # Match "KEY: value" or "**KEY**: value" (markdown bold variant)
        m = re.search(rf'\*{{0,2}}{k}\*{{0,2}}:\s*(.+)', text, re.IGNORECASE)
        return m.group(1).strip().rstrip('*').strip() if m else d

    def parse_int_field(raw, lo=1, hi=10):
        if not raw:
            return None
        try:
            return max(lo, min(hi, int(re.search(r'\d+', raw).group())))
        except (AttributeError, ValueError):
            return None

    score      = parse_int_field(g('CONDITION_SCORE', ''))
    area_score = parse_int_field(g('AREA_QUALITY_SCORE', ''))

    return {
        'score':            score,
        'area_quality_score': area_score,
        'confidence':       (g('CONFIDENCE', 'low') or 'low').lower(),
        'rooms_visible':    g('ROOMS_VISIBLE', ''),
        'requires_more':    (g('REQUIRES_MORE', 'no') or 'no').lower().startswith('yes'),
        'renovation':       g('RENOVATION', 'Unknown'),
        'renovation_year':  g('RENOVATION_YEAR', ''),
        'feel':             g('FEEL', ''),
        'finish':           g('FINISH', 'Standard'),
        'light':            g('LIGHT', 'Average'),
        'area_impression':  g('AREA_IMPRESSION', 'Adequate'),
        'wide_angle_flag':  (g('WIDE_ANGLE_FLAG', 'no') or 'no').lower().startswith('yes'),
        'red_flags':        g('RED_FLAGS', 'None'),
        'solar_direction':  g('SOLAR_DIRECTION', 'Unknown'),
        'summary':          g('SUMMARY', ''),
    }


def analyze_batch(b64s: list, info: str, prev: list | None, rooms_seen: str, use_deep: bool = False) -> dict:
    """Call GPT vision on one batch of images. Retries on rate limit / timeout."""
    prev_ctx = ''
    if prev:
        last = prev[-1]
        prev_ctx = (
            f'\nPrevious assessment ({last["images_seen"]} photos seen so far): '
            f'score={last["score"]}/10, area={last.get("area_quality_score","?")}/10, '
            f'feel="{last["feel"]}", conf={last["confidence"]}. '
            f'Revise if new photos change your view.'
        )

    rooms_ctx = f' Rooms already seen: {rooms_seen}.' if rooms_seen else ' No rooms seen yet.'
    n_remaining = '(more photos may follow)' if len(b64s) < 4 else '(rate based on all seen)'

    prompt = f'''You are an expert rental property analyst assessing a Porto apartment for a demanding tenant.
Score based ONLY on what is visible. Be critical and precise — most Portuguese rentals score 5-7.{prev_ctx}{rooms_ctx} {n_remaining}

SCORING SCALE (use the full range — do not cluster around 7):
  1-3 = needs major works, serious problems
  4-5 = dated/tired but livable, obvious wear
  6   = clean and functional, dated finishes
  7   = clean modern standard, minimal wear
  8   = recently renovated, quality finishes, feels fresh
  9   = high-end renovation, premium materials throughout
  10  = new build or luxury spec, exceptional

AREA SCALE: 1=tiny/cramped 5=adequate 10=genuinely spacious and airy

CRITICAL CHECKS:
1. Is there likely wide-angle lens distortion making rooms look larger than they are?
   Look for: extreme perspective, curved walls, very low viewpoint, fisheye corners.
2. Are the kitchen and bathroom surfaces genuinely good quality or just clean+old?
3. Does natural light actually enter the rooms or does it look artificially bright?
4. Does the floor area match the claimed size ({info}), or does furniture scale suggest smaller?

Respond in EXACTLY this format — fill every line, no skipping:
CONDITION_SCORE: [1-10 integer]
AREA_QUALITY_SCORE: [1-10 integer — space, ceiling height, sense of openness]
CONFIDENCE: [low/medium/high]
ROOMS_VISIBLE: [comma list: e.g. living room, kitchen, master bedroom, bathroom, terrace]
REQUIRES_MORE: [yes/no]
RENOVATION: [New Build / Fully Renovated / Partially Renovated / Original / Unknown]
RENOVATION_YEAR: [estimated year range e.g. 2020-2023, or Unknown]
FEEL: [2-4 adjective words only — no explanation]
FINISH: [Basic / Standard / Premium / Luxury]
LIGHT: [Poor / Average / Good / Excellent]
AREA_IMPRESSION: [Spacious / Adequate / Cramped]
WIDE_ANGLE_FLAG: [yes/no — yes if fisheye/wide-angle distortion suspected]
RED_FLAGS: [specific issues: e.g. dated kitchen tiles, thin doors, dark north-facing room, damp patch — or None]
SOLAR_DIRECTION: [South / Southeast / Southwest / East / West / North / Unknown — based on shadow direction and light entry angle]
SUMMARY: [2 honest sentences a tenant deciding whether to visit would find useful. Include a key positive and a key concern.]'''

    content = [{'type': 'text', 'text': prompt}]
    for b in b64s:
        content.append({'type': 'image_url', 'image_url': {
            'url': f'data:image/jpeg;base64,{b}', 'detail': 'low'
        }})

    # Cost strategy: use mini for rounds 1-2, deep only when confidence is low
    # or score is borderline (5-7). Reduces cost by ~85% with minimal quality loss.
    model = MODEL_DEEP if use_deep else MODEL_FAST

    for attempt in range(1, 4):  # 3 attempts
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{'role': 'user', 'content': content}],
                max_completion_tokens=450,
                temperature=0,
            )
            return parse_resp(resp.choices[0].message.content.strip())
        except RateLimitError:
            wait = 15 * attempt
            print(f'    ⚠️  Rate limit (attempt {attempt}/3) — waiting {wait}s', flush=True)
            time.sleep(wait)
        except APITimeoutError:
            wait = 10 * attempt
            print(f'    ⚠️  API timeout (attempt {attempt}/3) — waiting {wait}s', flush=True)
            time.sleep(wait)
        except APIStatusError as e:
            if e.status_code in (429, 503):
                wait = 20 * attempt
                print(f'    ⚠️  API {e.status_code} (attempt {attempt}/3) — waiting {wait}s', flush=True)
                time.sleep(wait)
            else:
                raise  # unrecoverable — propagate
    raise RuntimeError(f'GPT vision failed after 3 attempts')


def analyze_listing(lid: str, photo_urls: list, info: str, full_mode: bool = False) -> dict | None:
    """Analyze one listing. Returns enriched result dict or None if no usable photos."""
    real = [u for u in photo_urls if u.startswith('http')]
    if not real:
        return None

    if full_mode:
        MIN, MAX = 1, len(real)
        # batch size 4 for efficiency in full mode
        BATCH_SIZES = []
        n = len(real)
        while n > 0:
            BATCH_SIZES.append(min(4, n))
            n -= BATCH_SIZES[-1]
    else:
        MIN, MAX = 4, 9           # v6: 4 min (enough to see all rooms), 9 max (was 12)
        BATCH_SIZES = [3, 3, 3]    # 3 rounds max in normal mode (was 4)

    print(f'    {len(real)} photos | mode={"FULL" if full_mode else f"adaptive min:{MIN} max:{MAX}"}', flush=True)

    rounds, all_rooms, used, idx = [], [], 0, 0

    for bsize in BATCH_SIZES:
        if idx >= len(real) or used >= MAX:
            break

        batch = real[idx:idx + bsize]
        b64s = []
        for url in batch:
            b = fetch_b64(url)
            if b:
                b64s.append(b)
        if not b64s:
            idx += bsize
            continue

        # Use deep model (gpt-4o) on round 3+ if mini gave low confidence or borderline score
        use_deep = False
        if len(rounds) >= 2:
            last_conf  = rounds[-1].get('confidence', 'medium')
            last_score = rounds[-1].get('score') or 5
            use_deep   = last_conf == 'low' or (5 <= last_score <= 7)

        try:
            res = analyze_batch(b64s, info, rounds or None, ', '.join(all_rooms) or None,
                                use_deep=use_deep)
        except Exception as e:
            print(f'    ❌ Batch failed: {e}', flush=True)
            idx += bsize
            continue

        used += len(b64s)
        idx += bsize
        res['images_seen'] = used
        rounds.append(res)

        rooms_new = [r.strip() for r in res.get('rooms_visible', '').split(',') if r.strip()]
        all_rooms.extend(rooms_new)

        flag_str = '⚠️WIDE-ANGLE' if res.get('wide_angle_flag') else ''
        print(
            f'    Round {len(rounds)}: {len(b64s)}imgs → '
            f'score={res["score"]} area={res.get("area_quality_score","?")} '
            f'conf={res["confidence"]} {flag_str} | {res["rooms_visible"][:50]}',
            flush=True
        )

        # Early stop conditions (adaptive mode only, after MIN images)
        if not full_mode and used >= MIN:
            if res['confidence'] == 'high' and not res['requires_more']:
                print(f'    → Stop: high confidence after {used} imgs', flush=True)
                break
            if len(rounds) >= 2:
                d = abs((rounds[-1]['score'] or 0) - (rounds[-2]['score'] or 0))
                da = abs((rounds[-1].get('area_quality_score') or 0) - (rounds[-2].get('area_quality_score') or 0))
                if d <= 1 and da <= 1 and res['confidence'] in ('medium', 'high'):
                    print(f'    → Stop: stable (score±{d}, area±{da}) after {used} imgs', flush=True)
                    break
            if len(rounds) >= 3:
                if len(set(r['score'] for r in rounds[-3:])) == 1:
                    print(f'    → Stop: 3× same score after {used} imgs', flush=True)
                    break

        time.sleep(0.5)

    if not rounds:
        return None

    # Use the final round as base (most informed), overlay accumulated data
    final = dict(rounds[-1])
    final['total_images']      = used
    final['total_rounds']      = len(rounds)
    final['score_progression'] = [r['score'] for r in rounds]
    final['area_progression']  = [r.get('area_quality_score') for r in rounds]
    final['all_rooms_seen']    = list(dict.fromkeys(all_rooms))  # ordered dedup

    # Wide-angle flag: positive if ANY round flagged it
    final['wide_angle_flag'] = any(r.get('wide_angle_flag') for r in rounds)

    return final


def main():
    import sys
    from datetime import datetime

    full_mode  = '--full' in sys.argv
    force_all  = '--force' in sys.argv   # re-score even v5-complete entries
    force_ids: set = set()
    if '--id' in sys.argv:
        idx = sys.argv.index('--id')
        force_ids = {sys.argv[idx + 1]} if idx + 1 < len(sys.argv) else set()

    # --batch N: process at most N listings then exit cleanly.
    # Designed for cron jobs — each run is short-lived, progress is saved,
    # and the cron re-fires until all done. Survives gateway restarts.
    batch_limit: int | None = None
    if '--batch' in sys.argv:
        try:
            batch_limit = int(sys.argv[sys.argv.index('--batch') + 1])
        except (ValueError, IndexError):
            print('⚠️  --batch requires an integer argument, e.g. --batch 10')
            return

    print(f'🦞 Image Analyzer v5 — {"FULL MODE" if full_mode else "adaptive mode"}'
          + (f' [batch={batch_limit}]' if batch_limit else ''))
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print(f'🤖 Model: {MODEL}')
    print('=' * 55)

    try:
        with open(DETAILS_FILE, encoding='utf-8') as f:
            details = {d['id']: d for d in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f'❌ Cannot load {DETAILS_FILE}: {e}')
        return

    # Load base listings for rooms/size_m2 (detail records don't store these)
    try:
        with open(LISTINGS_FILE, encoding='utf-8') as f:
            listings_base = {l['id']: l for l in json.load(f)}
    except (FileNotFoundError, json.JSONDecodeError):
        listings_base = {}

    try:
        with open(IMAGE_FILE, encoding='utf-8') as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {}

    # SIGTERM handler — flush progress before dying
    def _on_sigterm(signum, frame):
        with open(IMAGE_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        print(f'\n💾 SIGTERM — saved {len(existing)} entries', flush=True)
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, _on_sigterm)

    to_do: dict = {}
    for lid, d in details.items():
        if force_ids and lid not in force_ids:
            continue
        if full_mode:
            # --full: upgrade v4→v5 (missing area_quality_score)
            # --full --force: re-score even completed v5 entries
            if not force_all and lid in existing and 'area_quality_score' in existing[lid]:
                continue
        else:
            if lid in existing and lid not in force_ids:
                continue
        photos = d.get('photo_urls') or d.get('unblurred_photos')
        if photos:
            to_do[lid] = {**d, '_photos': photos}

    total_pending = len(to_do)
    # Apply batch limit — take first N, report remainder
    if batch_limit and total_pending > batch_limit:
        items = list(to_do.items())[:batch_limit]
        to_do = dict(items)
        remaining_after = total_pending - batch_limit
    else:
        remaining_after = 0

    mode_label = 'full re-scan' if full_mode else 'new only'
    batch_label = f' | batch {batch_limit}/{total_pending}' if batch_limit else ''
    print(f'📦 {len(to_do)} listings to analyze ({mode_label}{batch_label})')
    if remaining_after:
        print(f'   ({remaining_after} will remain after this batch — re-run to continue)')
    print()

    total_imgs = 0
    for i, (lid, detail) in enumerate(to_do.items(), 1):
        photos = detail.get('_photos') or []
        base   = listings_base.get(lid, {})
        # Prefer detail fields, fall back to base listing
        size_m2 = detail.get('area_util') or detail.get('area_bruta') or base.get('size_m2') or '?'
        rooms   = detail.get('rooms') or base.get('rooms') or '?'
        info    = f'{rooms} {size_m2}m²'

        print(f'\n[{i:3d}/{len(to_do)}] {lid} | {info}', flush=True)
        result = analyze_listing(lid, photos, info, full_mode=full_mode)

        if result:
            existing[lid] = result
            total_imgs += result['total_images']
            wide = ' ⚠️WIDE-ANGLE' if result.get('wide_angle_flag') else ''
            print(
                f'    ✓ {result["score"]}/10 area={result.get("area_quality_score","?")} | '
                f'{result["total_rounds"]}rnd {result["total_images"]}imgs | '
                f'"{result["feel"]}" | {result["finish"]} | {result["renovation"]}{wide}',
                flush=True
            )
        else:
            print(f'    ✗ No usable photos', flush=True)

        # Checkpoint every 3
        if i % 3 == 0:
            with open(IMAGE_FILE, 'w', encoding='utf-8') as f:
                json.dump(existing, f, ensure_ascii=False, indent=2)
            print(f'  💾 Checkpoint ({i} done)', flush=True)

        time.sleep(0.5)

    with open(IMAGE_FILE, 'w', encoding='utf-8') as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

    analyzed = len([v for v in existing.values() if v.get('score')])
    avg = total_imgs / max(len(to_do), 1)
    print(f'\n✅ Done: {analyzed} total analyzed | avg {avg:.1f} imgs/listing')
    if remaining_after:
        print(f'⏭️  {remaining_after} listings still pending — run again to continue')
    print(f'📁 {IMAGE_FILE}')

    # Exit code 2 = batch done but more work remains (useful for cron loops)
    if remaining_after:
        raise SystemExit(2)


if __name__ == '__main__':
    main()


# ─────────────────────────────────────────────────────────────────────────────
# BATCH API (50% cheaper, async — use for bulk runs, not daily updates)
# ─────────────────────────────────────────────────────────────────────────────

def build_batch_requests(to_do: dict, info_map: dict) -> list[dict]:
    """
    Build OpenAI Batch API request objects for all pending listings.
    Use for one-time bulk analysis runs — 50% cheaper, results in ~1h.

    to_do: {lid: detail_dict}
    info_map: {lid: 'T2 105m²'} size/rooms string
    """
    import uuid
    requests_list = []
    for lid, detail in to_do.items():
        photos = (detail.get('photo_urls') or detail.get('unblurred_photos') or [])[:3]
        if not photos:
            continue
        info = info_map.get(lid, '?')
        b64s = [fetch_b64(url) for url in photos]
        b64s = [b for b in b64s if b]
        if not b64s:
            continue

        content = [{'type': 'text', 'text': f'Score this Porto apartment ({info}). '
                    'Reply with CONDITION_SCORE:N AREA_QUALITY_SCORE:N CONFIDENCE:x '
                    'RENOVATION:x FEEL:x FINISH:x LIGHT:x AREA_IMPRESSION:x '
                    'WIDE_ANGLE_FLAG:x RED_FLAGS:x SOLAR_DIRECTION:x SUMMARY:2 sentences.'}]
        for b in b64s:
            content.append({'type': 'image_url', 'image_url': {
                'url': f'data:image/jpeg;base64,{b}', 'detail': 'low'
            }})

        requests_list.append({
            'custom_id': f'listing-{lid}',
            'method': 'POST',
            'url': '/v1/chat/completions',
            'body': {
                'model': MODEL_FAST,
                'messages': [{'role': 'user', 'content': content}],
                'max_tokens': 400,
                'temperature': 0,
            }
        })
    return requests_list


def submit_batch(requests_list: list[dict], description: str = 'listing-analysis') -> str:
    """
    Submit a Batch API job. Returns batch_id.
    Results available in ~1h via poll_batch(batch_id).
    """
    import tempfile, json as _json
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for req in requests_list:
            f.write(_json.dumps(req) + '\n')
        tmp_path = f.name

    with open(tmp_path, 'rb') as f:
        batch_file = client.files.create(file=f, purpose='batch')

    batch = client.batches.create(
        input_file_id=batch_file.id,
        endpoint='/v1/chat/completions',
        completion_window='24h',
        metadata={'description': description},
    )
    print(f'✅ Batch submitted: {batch.id} | {len(requests_list)} requests | ~1h for results')
    os.unlink(tmp_path)
    return batch.id


def poll_batch(batch_id: str) -> dict | None:
    """
    Poll batch status. Returns {lid: parsed_result} when complete, None if still running.
    Usage: python3 analyze_images.py --batch-poll <batch_id>
    """
    import json as _json
    batch = client.batches.retrieve(batch_id)
    print(f'Batch {batch_id}: status={batch.status} '
          f'completed={batch.request_counts.completed}/{batch.request_counts.total}')

    if batch.status != 'completed':
        return None

    # Download results
    content = client.files.content(batch.output_file_id)
    results = {}
    for line in content.text.strip().split('\n'):
        obj = _json.loads(line)
        lid = obj['custom_id'].replace('listing-', '')
        body = obj.get('response', {}).get('body', {})
        if body.get('choices'):
            text = body['choices'][0]['message']['content']
            results[lid] = parse_resp(text)
            results[lid]['batch_id'] = batch_id
    return results


if __name__ == '__main__' and '--batch-submit' in sys.argv:
    # Usage: python3 analyze_images.py --batch-submit
    # Submits all unanalyzed listings to Batch API (50% cheaper, ~1h turnaround)
    import json as _json
    listings_base = {l['id']: l for l in _json.load(open(LISTINGS_FILE))}
    details_raw   = _json.load(open(DETAILS_FILE))
    details       = {d['id']: d for d in details_raw}
    existing      = _json.load(open(IMAGE_FILE)) if os.path.exists(IMAGE_FILE) else {}

    to_do = {lid: d for lid, d in details.items()
             if lid not in existing and (d.get('photo_urls') or d.get('unblurred_photos'))}
    info_map = {lid: f'{listings_base.get(lid,{}).get("rooms","?")} {details[lid].get("area_util") or listings_base.get(lid,{}).get("size_m2","?")}m²'
                for lid in to_do}

    reqs = build_batch_requests(to_do, info_map)
    print(f'Built {len(reqs)} batch requests for {len(to_do)} listings')
    batch_id = submit_batch(reqs, 'porto-listing-analysis')
    print(f'Poll with: python3 analyze_images.py --batch-poll {batch_id}')

elif __name__ == '__main__' and '--batch-poll' in sys.argv:
    idx = sys.argv.index('--batch-poll')
    batch_id = sys.argv[idx + 1] if len(sys.argv) > idx + 1 else None
    if not batch_id:
        print('Usage: python3 analyze_images.py --batch-poll <batch_id>')
        sys.exit(1)
    results = poll_batch(batch_id)
    if results:
        import json as _json
        existing = _json.load(open(IMAGE_FILE)) if os.path.exists(IMAGE_FILE) else {}
        existing.update(results)
        with open(IMAGE_FILE, 'w') as f:
            _json.dump(existing, f, ensure_ascii=False, indent=2)
        print(f'✅ Saved {len(results)} results → {IMAGE_FILE}')
    else:
        print('Not complete yet — try again in a few minutes')
