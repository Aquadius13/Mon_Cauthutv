#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler hoadaotv.info → IPTV JSON  v2                     ║
║                                                              ║
║   Parse: flat token list + linear state machine             ║
║   - Section HOT: text "Các Trận Hot"                        ║
║   - BLV img anchor → league → corner → home → VS → away    ║
║   - Merge trận cùng cặp đội (khác BLV) → 1 card            ║
║   - Stream: ?mode=sd|hd|fullhd|flv|flv2                     ║
║   - Logo home từ detail page (lazy-load ở trang chủ)        ║
╚══════════════════════════════════════════════════════════════╝
pip install cloudscraper beautifulsoup4 lxml requests pillow
python3 crawler_hoadaotv.py [--no-stream] [--debug] [--limit N]
"""

import argparse, base64, hashlib, io, json, re, sys, time
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin

try:
    import cloudscraper
    from bs4 import BeautifulSoup, NavigableString
    import requests
except ImportError:
    print("pip install cloudscraper beautifulsoup4 lxml requests pillow")
    sys.exit(1)

try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL = True
except ImportError:
    _PIL = False

# ── Config ────────────────────────────────────────────────────
BASE_URL    = "https://hoadaotv.info"
OUTPUT_FILE = "hoadaotv_iptv.json"
DEBUG_HTML  = "debug_hoadaotv.html"
CHROME_UA   = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/124.0.0.0 Safari/537.36")
VN_TZ       = timezone(timedelta(hours=7))
SITE_ICON   = f"{BASE_URL}/assets/image/hoadaotvlogo.png"

STREAM_MODES = [
    ("sd",     "⚪ SD"),
    ("hd",     "🔵 HD"),
    ("fullhd", "📺 Quốc Tế / Full HD"),
    ("flv",    "⚡ SD Nhanh"),
    ("flv2",   "⚡ HD Nhanh"),
]

PLACEHOLDER = {
    "padding": 0, "background_color": "#0d1829", "display": "cover",
    "url": SITE_ICON, "width": 800, "height": 440,
}

def log(*a, **kw): print(*a, **kw, flush=True)

# ── HTTP ─────────────────────────────────────────────────────
def make_scraper():
    sc = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    sc.headers.update({
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
        "Referer": BASE_URL + "/",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    })
    return sc

def fetch(url, scraper, retries=3):
    for i in range(retries):
        try:
            r = scraper.get(url, timeout=25, allow_redirects=True,
                            headers={"User-Agent": CHROME_UA,
                                     "Referer": BASE_URL + "/"})
            r.raise_for_status()
            log(f"    ✓ [{r.status_code}] {url[:80]}")
            return r.text
        except Exception as e:
            log(f"    ⚠ {i+1}/{retries}: {e}")
            if i < retries - 1: time.sleep(2 ** i)
    return None

# ── Token extraction ─────────────────────────────────────────
def get_flat_tokens(parent):
    """
    Flatten DOM thành danh sách token tuyến tính.
    Text nodes được split theo \\n để tách từng dòng riêng.
    """
    tokens = []
    for child in parent.children:
        if isinstance(child, NavigableString):
            for line in str(child).split('\n'):
                t = line.strip()
                if t:
                    tokens.append(('text', t))
        elif child.name == 'img':
            tokens.append(('img', {
                'alt': child.get('alt', '').strip(),
                'src': child.get('src', '').strip(),
            }))
        elif child.name == 'a':
            tokens.append(('a', {
                'href': child.get('href', '').strip(),
                'text': child.get_text(strip=True),
            }))
        elif child.name and child.name not in ('script', 'style', 'noscript'):
            tokens.extend(get_flat_tokens(child))
    return tokens

def is_sport_icon(v):
    src = v.get('src', '')
    return ('icon-sports' in src or 'icon_sport' in src or v.get('alt', '') == 'corner')

# ── Parse "Các Trận Hot" section ─────────────────────────────
def find_hot_container(bs):
    """
    Tìm container DOM chứa section "Các Trận Hot".
    Trả về element hoặc bs nếu không tìm thấy.
    """
    # Cách 1: tìm theo text "Các Trận Hot"
    for node in bs.find_all(string=lambda t: t and "Các Trận Hot" in t):
        parent = node.parent
        for _ in range(6):
            if parent is None: break
            # Container hợp lệ: chứa ít nhất 3 BLV img
            blv_imgs = parent.find_all('img', alt=re.compile(r'^BLV\s', re.I))
            if len(blv_imgs) >= 3:
                log(f"  → HOT container: <{parent.name} class='{' '.join(parent.get('class',[]))[:40]}'> "
                    f"({len(blv_imgs)} BLV imgs)")
                return parent
            parent = parent.parent

    # Cách 2: tìm div chứa nhiều BLV img nhất
    best, best_count = None, 0
    for div in bs.find_all(['div', 'section', 'ul']):
        count = len(div.find_all('img', alt=re.compile(r'^BLV\s', re.I), recursive=False))
        if count > best_count:
            best, best_count = div, count

    if best and best_count >= 2:
        log(f"  → HOT container (fallback): {best_count} BLV imgs")
        return best

    log("  ⚠ Không tìm được container HOT → parse toàn trang")
    return bs

def parse_card_from_segment(seg, blv_name, blv_avatar):
    """
    Parse 1 segment (tokens giữa 2 BLV img) thành dict match.
    State machine tuyến tính:
    BLV_NAME → LEAGUE → AWAIT_CORNER → HOME_IMG → HOME_TEXT 
    → AWAIT_VS → AWAY_IMG → AWAY_TEXT → TIME → LINKS
    """
    league = home_team = away_team = ''
    home_logo = away_logo = detail_url = ''
    t_str = d_str = ''
    status = 'upcoming'
    state  = 'BLV_NAME'

    for tk_t, tk_v in seg:
        # ── BLV_NAME: skip BLV name repeat text ──────────────
        if state == 'BLV_NAME':
            if tk_t == 'text':
                if tk_v == blv_name or tk_v.startswith('BLV '):
                    state = 'LEAGUE'
                else:
                    # Không có repeat → đây là league
                    league = tk_v
                    state = 'AWAIT_CORNER'
            continue

        # ── LEAGUE ────────────────────────────────────────────
        if state == 'LEAGUE':
            if tk_t == 'text':
                league = tk_v
                state = 'AWAIT_CORNER'
            elif tk_t == 'img' and is_sport_icon(tk_v):
                state = 'HOME_IMG'
            continue

        # ── AWAIT_CORNER ──────────────────────────────────────
        if state == 'AWAIT_CORNER':
            if tk_t == 'img' and is_sport_icon(tk_v):
                state = 'HOME_IMG'
            continue

        # ── HOME_IMG ──────────────────────────────────────────
        if state == 'HOME_IMG':
            if tk_t == 'img':
                if is_sport_icon(tk_v): continue
                home_team = tk_v['alt']
                home_logo = tk_v['src'] if tk_v['src'].startswith('http') else ''
                state = 'HOME_TEXT'
            continue

        # ── HOME_TEXT ─────────────────────────────────────────
        if state == 'HOME_TEXT':
            if tk_t == 'text':
                if tk_v == 'VS':
                    state = 'AWAY_IMG'
                elif re.search(r'\bLIVE\b', tk_v, re.I):
                    status = 'live'
                elif re.search(r'Kết\s*Thúc|Finished', tk_v, re.I):
                    status = 'finished'
                elif not home_team:
                    home_team = tk_v
            continue

        # ── AWAY_IMG ──────────────────────────────────────────
        if state == 'AWAY_IMG':
            if tk_t == 'img':
                if is_sport_icon(tk_v): continue
                away_logo = tk_v['src'] if tk_v['src'].startswith('http') else ''
                away_team = tk_v['alt']  # thường rỗng
                state = 'AWAY_TEXT'
            elif tk_t == 'text' and tk_v == 'VS':
                continue
            continue

        # ── AWAY_TEXT ─────────────────────────────────────────
        if state == 'AWAY_TEXT':
            if tk_t == 'text':
                if re.match(r'\d{1,2}:\d{2}', tk_v):
                    tm = re.search(r'(\d{1,2}):(\d{2})', tk_v)
                    dm = re.search(r'(\d{1,2})/(\d{2})', tk_v)
                    if tm: t_str = f"{int(tm.group(1)):02d}:{tm.group(2)}"
                    if dm: d_str = f"{int(dm.group(1)):02d}/{dm.group(2)}"
                    state = 'LINKS'
                elif not away_team:
                    away_team = tk_v
                else:
                    state = 'TIME'
            elif tk_t == 'a':
                href = tk_v['href']
                if 'hoadaotv' in href and tk_v['text'] in ('Xem','Xem Ngay','XEM NGAY'):
                    detail_url = href
            continue

        # ── TIME ──────────────────────────────────────────────
        if state == 'TIME':
            if tk_t == 'text':
                tm = re.search(r'(\d{1,2}):(\d{2})', tk_v)
                dm = re.search(r'(\d{1,2})/(\d{2})', tk_v)
                if tm: t_str = f"{int(tm.group(1)):02d}:{tm.group(2)}"
                if dm: d_str = f"{int(dm.group(1)):02d}/{dm.group(2)}"
                state = 'LINKS'
            continue

        # ── LINKS ─────────────────────────────────────────────
        if state == 'LINKS':
            if tk_t == 'a':
                href = tk_v['href']
                if 'hoadaotv' in href and tk_v['text'] in ('Xem','Xem Ngay','XEM NGAY'):
                    detail_url = href
            continue

    if not detail_url:
        return None
    if not home_team and not away_team:
        return None

    return {
        'base_title':  f"{home_team} vs {away_team}",
        'home_team':   home_team,
        'away_team':   away_team,
        'home_logo':   home_logo,
        'away_logo':   away_logo,
        'league':      league,
        'time_str':    t_str,
        'date_str':    d_str,
        'sort_key':    f"{d_str} {t_str}",
        'status':      status,
        'blv':         blv_name,
        'blv_avatar':  blv_avatar,
        'detail_url':  detail_url,
    }

def parse_hot_matches(html, bs, debug=False):
    """Parse toàn bộ section HOT → list raw match dicts."""
    container = find_hot_container(bs)
    tokens    = get_flat_tokens(container)

    if debug:
        log(f"\n  DEBUG tokens ({len(tokens)}):")
        for i, (t, v) in enumerate(tokens[:60]):
            log(f"    [{i:3d}] {t}: {str(v)[:80]}")

    # Tìm vị trí các BLV img
    blv_pos = [i for i, (t, v) in enumerate(tokens)
               if t == 'img' and isinstance(v, dict) and v['alt'].startswith('BLV ')]
    log(f"  → {len(blv_pos)} BLV anchors")

    cards = []
    blv_pos.append(len(tokens))  # sentinel

    for pi, start in enumerate(blv_pos[:-1]):
        end       = blv_pos[pi + 1]
        blv_img   = tokens[start][1]
        blv_name  = blv_img['alt'][4:].strip()
        blv_av    = blv_img['src'] if blv_img['src'].startswith('http') else ''
        seg       = tokens[start + 1 : end]

        m = parse_card_from_segment(seg, blv_name, blv_av)
        if m:
            cards.append(m)
        elif debug:
            log(f"    ⚠ Segment BLV {blv_name} → None")

    log(f"  → {len(cards)}/{len(blv_pos)-1} cards parsed")
    return cards

# ── Merge trận cùng cặp đội ──────────────────────────────────
def _nkey(s):
    return re.sub(r'[^a-z0-9]', '', s.lower())

def merge_matches(raw):
    groups = {}
    for m in raw:
        h = _nkey(m.get('home_team', ''))
        a = _nkey(m.get('away_team', ''))
        key = '__'.join(sorted([h, a])) if h and a else m['detail_url']

        if key not in groups:
            groups[key] = {**m, 'blv_sources': []}

        g = groups[key]
        seen_urls = {s['detail_url'] for s in g['blv_sources']}
        if m['detail_url'] not in seen_urls:
            g['blv_sources'].append({
                'blv':        m['blv'],
                'blv_avatar': m['blv_avatar'],
                'detail_url': m['detail_url'],
            })

        # Ưu tiên logo HTTP đầy đủ
        for field in ('home_logo', 'away_logo', 'league'):
            if not g.get(field) and m.get(field):
                g[field] = m[field]

    result = list(groups.values())
    pri = {'live': 0, 'upcoming': 1, 'finished': 2}
    result.sort(key=lambda x: (pri.get(x.get('status', 'upcoming'), 9),
                                x.get('sort_key', '')))
    return result

# ── Crawl trang detail ────────────────────────────────────────
def crawl_detail(detail_url, blv_name, scraper):
    """Lấy logo chính xác + stream info từ trang detail."""
    html = fetch(detail_url, scraper, retries=2)
    if not html: return {}
    bs = BeautifulSoup(html, 'lxml')

    info = {}

    # Logo 2 đội từ trang detail (rõ hơn trang chủ)
    # Tìm img có alt = tên đội (không phải BLV, không phải icon)
    team_imgs = []
    for img in bs.find_all('img'):
        src = img.get('src', '')
        alt = img.get('alt', '')
        if not src.startswith('http'): continue
        skip = ('icon-sports', 'icon_sport', 'hoadaotvlogo', '/uploads/', 'ads/')
        if any(s in src for s in skip): continue
        if alt.startswith('BLV'): continue
        if re.search(r'logo|icon|banner|background', src, re.I): continue
        team_imgs.append({'src': src, 'alt': alt})

    if len(team_imgs) >= 1:
        info['home_logo'] = team_imgs[0]['src']
        info['home_team'] = team_imgs[0]['alt']
    if len(team_imgs) >= 2:
        info['away_logo'] = team_imgs[1]['src']
        info['away_team'] = team_imgs[1]['alt']

    # R2 CDN thumbnail
    r2 = re.findall(
        r'https://pub-[a-f0-9]+\.r2\.dev/[^\s\'"<>]+\.(?:webp|jpg|png)[^\s\'"<>]*',
        html, re.I
    )
    if r2: info['thumb_url'] = r2[0]

    # og:image
    if not info.get('thumb_url'):
        og = bs.find('meta', attrs={'property': 'og:image'})
        if og:
            u = og.get('content', '').strip()
            skip = ('favicon', 'logo', 'icon', 'hoadaotvlogo', 'hoadaotv')
            if u.startswith('http') and not any(s in u for s in skip):
                info['thumb_url'] = u

    return info

# ── Thumbnail ────────────────────────────────────────────────
def _font(size, bold=True):
    if not _PIL: return None
    for p in [
        f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf",
        f"/usr/share/fonts/truetype/liberation/LiberationSans-{'Bold' if bold else 'Regular'}.ttf",
    ]:
        try: return ImageFont.truetype(p, size)
        except: pass
    return ImageFont.load_default()

def fetch_logo(url, max_px=400):
    if not url or not _PIL: return None
    try:
        r = requests.get(url.strip(), timeout=8,
                         headers={"User-Agent": "Mozilla/5.0"}, stream=True)
        r.raise_for_status()
        if "html" in r.headers.get("content-type", ""): return None
        data = b""
        for chunk in r.iter_content(65536):
            data += chunk
            if len(data) > 3_000_000: return None
        img = Image.open(io.BytesIO(data)).convert("RGBA")
        img.thumbnail((max_px, max_px), Image.LANCZOS)
        return img
    except: return None

def make_thumbnail(home_team, away_team, home_logo_url, away_logo_url,
                   time_str="", date_str="", status="upcoming", league=""):
    if not _PIL: return ""
    W, H = 800, 440
    canvas = Image.new("RGB", (W, H))
    draw   = ImageDraw.Draw(canvas)

    for y in range(H):
        t = y / H
        draw.line([(0, y), (W, y)],
                  fill=(int(8+8*t), int(12+14*t), int(32+22*t)))

    draw.rectangle([(0, 0), (W, 5)], fill=(255, 140, 0))
    BAR_H = 50
    draw.rectangle([(0, 5), (W, 5+BAR_H)], fill=(2, 5, 14))
    if league:
        draw.text((W//2, 5+BAR_H//2), league[:48],
                  fill=(255, 200, 40), font=_font(23), anchor="mm")
    draw.line([(0, 5+BAR_H), (W, 5+BAR_H)], fill=(255, 140, 0), width=1)

    CTOP = 5 + BAR_H + 10
    CBOT = H - 58
    AREA = CBOT - CTOP
    LMAX = min(AREA - 38, 160)
    CX = W // 2; LX = 130; RX = W - 130
    LY = CTOP + (AREA - 36) // 2
    NY = CBOT - 8

    def draw_logo(cx, cy, url, name):
        logo = fetch_logo(url, LMAX*4) if url else None
        if logo:
            if logo.mode not in ("RGBA","LA"):
                logo = logo.convert("RGBA")
            lw, lh = logo.size
            scale = min(LMAX/lw, LMAX/lh)
            nw, nh = max(1,int(lw*scale)), max(1,int(lh*scale))
            logo = logo.resize((nw,nh), Image.LANCZOS)
            ox, oy = cx-nw//2, cy-nh//2
            if logo.mode == "RGBA":
                canvas.paste(logo.convert("RGB"), (ox,oy), logo.split()[3])
            else:
                canvas.paste(logo.convert("RGB"), (ox,oy))
        else:
            half = LMAX // 2
            try:
                draw.rounded_rectangle(
                    [(cx-half,cy-half),(cx+half,cy+half)],
                    radius=12, fill=(14,26,56), outline=(65,105,185), width=2)
            except Exception:
                draw.rectangle(
                    [(cx-half,cy-half),(cx+half,cy+half)],
                    fill=(14,26,56), outline=(65,105,185), width=2)
            init = "".join(w[0].upper() for w in (name or "?").split()[:2]) or "?"
            draw.text((cx,cy), init, fill=(130,180,255), font=_font(52), anchor="mm")

        short = (name or "?")
        if len(short) > 16: short = short[:15] + "…"
        draw.text((cx+1,NY+1), short, fill=(0,0,0),      font=_font(22), anchor="mm")
        draw.text((cx,NY),     short, fill=(245,245,245), font=_font(22), anchor="mm")

    draw_logo(LX, LY, home_logo_url, home_team)
    draw_logo(RX, LY, away_logo_url, away_team)

    if status == "live":
        l1, c1, l2, c2, f1 = "● LIVE", (255,60,60), "", (255,255,255), 40
    else:
        l1, c1 = time_str or "VS", (255,255,255)
        l2, c2 = date_str or "",   (140,150,175)
        f1 = 48

    draw.text((CX+1,LY+1), l1, fill=(0,0,0), font=_font(f1), anchor="mm")
    draw.text((CX,LY),     l1, fill=c1,       font=_font(f1), anchor="mm")
    if l2:
        draw.text((CX,LY+44), l2, fill=c2, font=_font(17,False), anchor="mm")

    draw.rectangle([(0,H-52),(W,H)], fill=(2,4,12))
    draw.line([(0,H-52),(W,H-52)], fill=(255,140,0), width=1)

    buf = io.BytesIO()
    canvas.save(buf, format="JPEG", quality=90, optimize=True)
    return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()

# ── Build channel ─────────────────────────────────────────────
def make_id(*parts):
    return hashlib.md5("-".join(str(p) for p in parts).encode()).hexdigest()[:16]

def build_name(m):
    home, away = m.get('home_team',''), m.get('away_team','')
    base = f"{home} vs {away}" if home and away else m.get('base_title','')
    t, d, st = m.get('time_str',''), m.get('date_str',''), m.get('status','upcoming')
    if st == "live":     return f"{base}  🔴 LIVE"
    if st == "finished": return f"{base}  ✅"
    if t and d: return f"{base}  🕐 {t} | {d}"
    if t:       return f"{base}  🕐 {t}"
    return base

def build_channel(m, index):
    ch_id  = make_id("hdt", index,
                     re.sub(r"[^a-z0-9]","-",m.get('base_title','').lower())[:24])
    name   = build_name(m)
    league = m.get('league','')
    status = m.get('status','upcoming')
    srcs   = m.get('blv_sources',[])

    # Labels
    sc_map = {
        "live":     {"text":"● Live",          "color":"#E73131","text_color":"#fff"},
        "upcoming": {"text":"🕐 Sắp diễn ra", "color":"#d54f1a","text_color":"#fff"},
        "finished": {"text":"✅ Kết thúc",     "color":"#444444","text_color":"#fff"},
    }
    labels = [{**sc_map.get(status, sc_map["upcoming"]), "position":"top-left"}]

    n_blv = len(srcs)
    if n_blv > 1:
        labels.append({"text":f"🎙 {n_blv} BLV","position":"top-right",
                       "color":"#00601f","text_color":"#fff"})
    elif srcs and srcs[0].get('blv'):
        labels.append({"text":f"🎙 {srcs[0]['blv']}","position":"top-right",
                       "color":"#00601f","text_color":"#fff"})

    # Streams: mỗi BLV = 1 stream group, mỗi group có 5 mode links
    stream_objs = []
    for src in srcs:
        blv_name = src.get('blv','')
        blv_url  = src['detail_url']
        s_label  = f"🎙 {blv_name}" if blv_name else "Trực tiếp"
        s_id     = make_id(ch_id, blv_name)
        base     = blv_url.split("?")[0]

        links = []
        for li, (mode_key, mode_label) in enumerate(STREAM_MODES):
            links.append({
                "id":      make_id(s_id, f"l{li}"),
                "name":    mode_label,
                "type":    "iframe",
                "default": li == 0,
                "url":     f"{base}?mode={mode_key}",
                "request_headers": [
                    {"key":"Referer",    "value": blv_url},
                    {"key":"User-Agent", "value": CHROME_UA},
                ],
            })

        stream_objs.append({
            "id":           s_id,
            "name":         s_label,
            "stream_links": links,
        })

    if not stream_objs:
        stream_objs.append({
            "id": "fb", "name": "Trực tiếp",
            "stream_links": [{"id":"l0","name":"SD","type":"iframe",
                              "default":True,"url":BASE_URL+"/"}]
        })

    # Thumbnail
    la, lb     = m.get('home_logo',''), m.get('away_logo','')
    thumb_url  = m.get('thumb_url','')

    if thumb_url:
        img_obj = {"padding":0,"background_color":"#0d1829","display":"cover",
                   "url":thumb_url,"width":800,"height":440}
    elif _PIL:
        uri = make_thumbnail(
            m.get('home_team',''), m.get('away_team',''),
            la, lb, m.get('time_str',''), m.get('date_str',''),
            status, league,
        )
        img_obj = ({"padding":0,"background_color":"#0d1829","display":"cover",
                    "url":uri,"width":800,"height":440} if uri else PLACEHOLDER)
    else:
        img_obj = PLACEHOLDER

    content_name = name
    if league: content_name += f" · {league[:48]}"

    has_multi = len(stream_objs) > 1
    return {
        "id":            ch_id,
        "name":          name,
        "type":          "multi" if has_multi else "single",
        "display":       "thumbnail-only",
        "enable_detail": True,
        "image":         img_obj,
        "labels":        labels,
        "sources": [{
            "id":   make_id(ch_id,"src"),
            "name": "HoaDao TV",
            "contents": [{
                "id":      make_id(ch_id,"ct"),
                "name":    content_name,
                "streams": stream_objs,
            }],
        }],
    }

def build_json(channels):
    return {
        "id":          "hoadaotv-live",
        "name":        "Hoa Đào TV – Xem bóng đá trực tiếp",
        "url":         BASE_URL + "/",
        "description": "Nền tảng xem thể thao trực tuyến hàng đầu Việt Nam. "
                       "Trực tiếp bóng đá, bóng chuyền, esports với bình luận tiếng Việt chất lượng cao.",
        "disable_ads": True,
        "color":       "#e84040",
        "grid_number": 3,
        "image":       {"type":"cover","url":SITE_ICON},
        "groups": [{
            "id":       "tran-hot",
            "name":     "🔥 Các Trận Hot",
            "image":    None,
            "channels": channels,
        }],
    }

# ── Main ──────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-stream", action="store_true")
    ap.add_argument("--debug",     action="store_true")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    ap.add_argument("--limit",     type=int, default=0)
    args = ap.parse_args()

    log("\n" + "═"*64)
    log("  🌸  CRAWLER hoadaotv.info  v2  — Token State Machine")
    log("═"*64 + "\n")

    now_vn  = datetime.now(VN_TZ)
    scraper = make_scraper()

    log(f"📥 Tải {BASE_URL} ...")
    html = fetch(BASE_URL, scraper)
    if not html:
        log("❌ Không tải được!"); sys.exit(1)
    if "Just a moment" in html:
        log("⚠ Cloudflare block"); sys.exit(1)

    if args.debug:
        with open(DEBUG_HTML,"w",encoding="utf-8") as f: f.write(html)
        log(f"  💾 {DEBUG_HTML}")

    log("\n🔍 Parse 'Các Trận Hot'...")
    bs = BeautifulSoup(html, "lxml")
    raw = parse_hot_matches(html, bs, debug=args.debug)

    if not raw:
        log("❌ Không tìm thấy trận nào!")
        log("  Hãy chạy lại với --debug để xem token list.")
        sys.exit(1)

    log(f"\n🔀 Merge {len(raw)} cards...")
    matches = merge_matches(raw)
    log(f"  → {len(matches)} trận sau merge")

    if args.limit > 0:
        matches = matches[:args.limit]

    log("\n📋 Danh sách trận:")
    for i, m in enumerate(matches, 1):
        srcs  = m.get('blv_sources',[])
        blvs  = " | ".join(s['blv'] for s in srcs)
        log(f"  {i:02d}. [{m.get('status','?'):8s}] "
            f"{m.get('home_team','?')[:20]:20s} vs {m.get('away_team','?')[:20]:20s} | "
            f"⏰{m.get('time_str','?')} {m.get('date_str','?')} | "
            f"🎙{len(srcs)}BLV: {blvs[:40]}")

    log("\n📡 Crawl detail pages (lấy logo chính xác)...")
    if not args.no_stream:
        for m in matches:
            for src in m.get('blv_sources',[]):
                info = crawl_detail(src['detail_url'], src.get('blv',''), scraper)
                if info.get('thumb_url') and not m.get('thumb_url'):
                    m['thumb_url'] = info['thumb_url']
                if info.get('home_logo') and not m.get('home_logo'):
                    m['home_logo'] = info['home_logo']
                    if info.get('home_team'): m['home_team'] = info['home_team']
                if info.get('away_logo') and not m.get('away_logo'):
                    m['away_logo'] = info['away_logo']
                    if info.get('away_team'): m['away_team'] = info['away_team']
                time.sleep(0.3)
    else:
        log("  ⚠ --no-stream: bỏ qua")

    log("\n🏗  Build channels...")
    channels = [build_channel(m, i) for i, m in enumerate(matches, 1)]

    result = build_json(channels)
    with open(args.output,"w",encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    now_str = now_vn.strftime("%d/%m/%Y %H:%M ICT")
    log(f"\n{'═'*64}")
    log(f"  ✅ {args.output}")
    log(f"  📊 {len(channels)} trận | "
        f"{sum(len(m.get('blv_sources',[])) for m in matches)} nguồn BLV")
    log(f"  🕐 {now_str}")
    log("═"*64+"\n")

if __name__ == "__main__":
    main()
