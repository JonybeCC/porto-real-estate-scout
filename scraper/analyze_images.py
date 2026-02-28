#!/usr/bin/env python3
"""
Iterative Image Analyzer v3 — JBizz Assistant 🦞
Analyzes listing photos with GPT-5.1 vision (upgraded from GPT-4o).
- Deeper analysis: gauges actual room sizes vs claimed m², detects faked angles
- Min 5 images before any stop condition
- Stops when score stabilizes ±1 at medium/high confidence, or 3 identical rounds
- Max 10 images (hard cap)
- Batches: 3, 3, 2, 2
"""

import json, os, base64, re, requests, time
from openai import OpenAI

DETAILS_FILE = '/root/.openclaw/workspace/projects/real-estate/data/listing_details_zenrows.json'
IMAGE_FILE   = '/root/.openclaw/workspace/projects/real-estate/data/image_analysis.json'
MODEL        = 'gpt-5.1'   # upgraded from gpt-4o

client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))


def fetch_b64(url: str) -> str | None:
    try:
        r = requests.get(url, timeout=20,
                         headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.idealista.pt/'})
        if r.status_code == 200 and len(r.content) > 5000:
            return base64.b64encode(r.content).decode()
    except:
        pass
    return None


def parse_resp(text: str) -> dict:
    def g(k, d=None):
        m = re.search(rf'{k}:\s*(.+)', text, re.IGNORECASE)
        return m.group(1).strip() if m else d
    raw = g('CONDITION_SCORE', '')
    score = None
    try:
        score = int(re.search(r'\d+', raw).group())
    except:
        pass
    return {
        'score': score,
        'confidence': (g('CONFIDENCE', 'low') or 'low').lower(),
        'rooms_visible': g('ROOMS_VISIBLE', ''),
        'requires_more': (g('REQUIRES_MORE', 'no') or 'no').lower().startswith('yes'),
        'renovation': g('RENOVATION', 'Unknown'),
        'feel': g('FEEL', ''),
        'finish': g('FINISH', 'Standard'),
        'light': g('LIGHT', 'Average'),
        'red_flags': g('RED_FLAGS', 'None'),
        'area_impression': g('AREA_IMPRESSION', 'Adequate'),
        'summary': g('SUMMARY', ''),
    }


def analyze_batch(b64s: list, info: str, prev: list, rooms_seen: str) -> dict:
    prev_ctx = ''
    if prev:
        last = prev[-1]
        prev_ctx = (f'\nPrev ({last["images_seen"]} photos): score={last["score"]}/10, '
                    f'feel="{last["feel"]}", conf={last["confidence"]}. Update if new photos change view.')

    rooms_ctx = f' Previously seen: {rooms_seen}.' if rooms_seen else ''
    seen_count = len([r for r in (rooms_seen or '').split(',') if r.strip()])
    more_hint = 'We may show more photos after this.' if seen_count < 4 else 'Rate based on all photos seen.'

    content = [{'type': 'text', 'text': f'''You are an expert real estate analyst assessing a Porto rental apartment.
Score based ONLY on what is visible in these photos. Be precise and critical.{prev_ctx}{rooms_ctx}
{more_hint}

CRITICAL TASKS:
1. Score condition honestly — most Portuguese rentals are 5-7. Reserve 8+ for genuinely renovated/premium.
2. Gauge actual room size from furniture scale. Do rooms feel large or cramped vs the claimed m²? Note if "wide-angle trickery" is likely.
3. Check kitchen and bathroom finish quality specifically — they most affect liveability.
4. Note natural light quality — crucial in Porto winters.

Respond in EXACTLY this format (fill every field, no refusals):
CONDITION_SCORE: [integer 1-10, where 1=ruin 5=dated-livable 7=clean-standard 8=recently renovated 10=luxury-new]
CONFIDENCE: [low/medium/high]
ROOMS_VISIBLE: [comma list e.g. living room, kitchen, bedroom, bathroom]
REQUIRES_MORE: [yes/no]
RENOVATION: [New Build/Fully Renovated/Partially Renovated/Original/Unknown]
FEEL: [2-4 words e.g. bright airy modern / warm classic dated / cramped dark basic]
FINISH: [Basic/Standard/Premium/Luxury]
LIGHT: [Poor/Average/Good/Excellent]
AREA_IMPRESSION: [Spacious/Adequate/Cramped - does it match claimed size or seem smaller?]
RED_FLAGS: [specific issues: dated kitchen, small bathrooms, dark hallway, cheap fittings, etc — or None]
SUMMARY: [2 honest sentences a renter deciding whether to visit would find useful]'''}]

    for b in b64s:
        content.append({'type': 'image_url', 'image_url': {'url': f'data:image/jpeg;base64,{b}', 'detail': 'low'}})

    resp = client.chat.completions.create(
        model=MODEL,
        messages=[{'role': 'user', 'content': content}],
        max_completion_tokens=400, temperature=0
    )
    return parse_resp(resp.choices[0].message.content.strip())


def analyze_listing(lid: str, photo_urls: list, info: str) -> dict | None:
    # Use blur/WEB_DETAIL photos — they're proper full-resolution detail images
    real = [u for u in photo_urls if u.startswith('http')]
    if not real:
        return None

    MIN, MAX = 5, 10
    BATCH_SIZES = [3, 3, 2, 2]

    print(f'    {len(real)} photos available (min:{MIN} max:{MAX})')

    rounds, all_rooms, used, idx = [], [], 0, 0

    for bsize in BATCH_SIZES:
        if idx >= len(real) or used >= MAX:
            break

        batch = real[idx:idx + bsize]
        b64s = [b for b in [fetch_b64(u) for u in batch] if b]
        if not b64s:
            idx += bsize
            continue

        res = analyze_batch(b64s, info, rounds or None, ', '.join(all_rooms) or None)
        used += len(b64s)
        idx += bsize
        res['images_seen'] = used
        rounds.append(res)

        new_rooms = [r.strip() for r in res.get('rooms_visible', '').split(',') if r.strip()]
        all_rooms.extend(new_rooms)

        print(f'    Round {len(rounds)}: {len(b64s)}imgs → score={res["score"]} '
              f'conf={res["confidence"]} | {res["rooms_visible"][:60]}')

        # Stop conditions — only after MIN images
        if used >= MIN:
            if res['confidence'] == 'high' and not res['requires_more']:
                print(f'    → Stop: high confidence ({used} imgs)')
                break
            if len(rounds) >= 2:
                delta = abs((rounds[-1]['score'] or 0) - (rounds[-2]['score'] or 0))
                if delta <= 1 and res['confidence'] in ('medium', 'high'):
                    print(f'    → Stop: stable ±{delta} ({used} imgs)')
                    break
            if len(rounds) >= 3:
                last3 = [r['score'] for r in rounds[-3:]]
                if len(set(last3)) == 1:
                    print(f'    → Stop: 3× same score ({used} imgs)')
                    break

        time.sleep(0.5)

    if not rounds:
        return None

    final = dict(rounds[-1])
    final['total_images'] = used
    final['total_rounds'] = len(rounds)
    final['score_progression'] = [r['score'] for r in rounds]
    final['all_rooms_seen'] = list(set(all_rooms))
    return final


def main():
    from datetime import datetime
    print('🦞 Image Analyzer v2')
    print(f'📅 {datetime.now().strftime("%Y-%m-%d %H:%M")}')
    print('=' * 55)

    with open(DETAILS_FILE) as f:
        details = {d['id']: d for d in json.load(f)}

    try:
        with open(IMAGE_FILE) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {}

    # Analyze listings that have photos — try photo_urls first, fallback to unblurred_photos
    to_do = {}
    for lid, d in details.items():
        if lid in existing:
            continue
        photos = d.get('photo_urls') or d.get('unblurred_photos')
        if photos:
            to_do[lid] = {**d, '_photos_to_use': photos}
    print(f'📦 {len(to_do)} listings with photos to analyze\n')

    total_imgs = 0
    for i, (lid, detail) in enumerate(to_do.items(), 1):
        photos = detail.get('_photos_to_use') or detail.get('photo_urls') or detail.get('unblurred_photos', [])
        # Build info string
        info = f'ID:{lid}'

        print(f'\n[{i:3d}/{len(to_do)}] {lid}')
        result = analyze_listing(lid, photos, info)

        if result:
            existing[lid] = result
            total_imgs += result['total_images']
            print(f'    ✓ Final: {result["score"]}/10 | {result["total_rounds"]} rounds, '
                  f'{result["total_images"]} imgs | "{result["feel"]}" | {result["finish"]}')
        else:
            print(f'    ✗ No usable photos')

        # Checkpoint every 5
        if i % 5 == 0:
            with open(IMAGE_FILE, 'w') as f:
                json.dump(existing, f, ensure_ascii=False, indent=2)
            print(f'  💾 Saved ({i} done)')

        time.sleep(0.5)

    with open(IMAGE_FILE, 'w') as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)

    analyzed = len([v for v in existing.values() if v.get('score')])
    avg = total_imgs / max(len(to_do), 1)
    print(f'\n✅ Done: {analyzed} listings analyzed | avg {avg:.1f} imgs/listing')
    print(f'📁 Saved: {IMAGE_FILE}')


if __name__ == '__main__':
    main()
