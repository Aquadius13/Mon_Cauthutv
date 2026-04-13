#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler hoadaotv.info → IPTV JSON                          ║
║                                                              ║
║   Cấu trúc trang (đã phân tích):                            ║
║   - Section HOT: text "Các Trận Hot"                        ║
║   - Card: BLV name, league, home/away team, logo, time       ║
║   - Detail: ?mode=sd|hd|fullhd|flv|flv2                     ║
║   - Nhiều BLV cùng trận → gộp 1 card, enable_detail=true    ║
╚══════════════════════════════════════════════════════════════╝
Cài: pip install cloudscraper beautifulsoup4 lxml requests pillow
Chạy: python3 crawler_hoadaotv.py
Debug: python3 crawler_hoadaotv.py --debug --no-stream
"""

import argparse, base64, hashlib, io, json, re, sys, time
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlencode, urlparse, parse_qs

try:
    import cloudscraper
    from bs4 import BeautifulSoup
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

# Stream modes của trang
STREAM_MODES = [
    ("sd",     "⚪ SD"),
    ("hd",     "🔵 HD"),
    ("fullhd", "📺 Quốc Tế / Full HD"),
    ("flv",    "⚡ SD Nhanh"),
    ("flv2",   "⚡ HD Nhanh"),
]

PLACEHOLDER = {
    "padding":0, "background_color":"#0d1829", "display":"cover",
    "url": SITE_ICON, "width":800, "height":440,
}

def log(*a, **kw): print(*a, **kw, flush=True)

# ── HTTP ─────────────────────────────────────────────────────
def make_scraper():
    sc = cloudscraper.create_scraper(
        browser={"browser":"chrome","platform":"windows","mobile":False}
    )
    sc.headers.update({
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
        "Referer":  BASE_URL + "/",
        "Accept":   "text/html,application/xhtml+xml,*/*;q=0.8",
    })
    return sc

def fetch(url, scraper, retries=3, delay=0.4):
    for i in range(retries):
        try:
            r = scraper.get(url, timeout=25, allow_redirects=True,
                            headers={"User-Agent": CHROME_UA,
                                     "Referer": BASE_URL + "/"})
            r.raise_for_status()
            return r.text
        except Exception as e:
            wait = 2**i
            log(f"    ⚠ {i+1}/{retries}: {e} → {wait}s")
            if i < retries-1: time.sleep(wait)
    return None

# ── Parse datetime ────────────────────────────────────────────
def parse_time_date(raw):
    """'23:45 | 13/04' → ('23:45', '13/04', sort_key)"""
    if not raw: return "", "", ""
    m = re.search(r'(\d{1,2}):(\d{2})', raw)
    if not m: return "", "", ""
    hh, mm = m.group(1).zfill(2), m.group(2)
    if not (int(hh)<=23 and int(mm)<=59): return "", "", ""
    dm = re.search(r'(\d{1,2})/(\d{2})', raw)
    if dm:
        day, mon = dm.group(1).zfill(2), dm.group(2).zfill(2)
        return f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}"
    today = datetime.now(VN_TZ)
    return f"{hh}:{mm}", today.strftime("%d/%m"), f"{today.strftime('%m-%d')} {hh}:{mm}"

# ── Parse trang chủ ───────────────────────────────────────────
def parse_hot_section(bs):
    """
    Tìm section "Các Trận Hot" và parse tất cả card bên trong.
    Mỗi card HTML chứa:
      - BLV name: img alt="BLV Xxx" → "Xxx"
      - BLV avatar: img src="hoadaotv.info/uploads/..."
      - League: text sau icon sport
      - Home team: tên + logo
      - Away team: tên + logo
      - Time/date: "23:45 | 13/04"
      - Status: "LIVE", "Chưa Bắt Đầu", "Kết Thúc"
      - Link: href="/slug"
    """
    # Tìm container section HOT
    hot_section = None

    # Cách 1: tìm theo text "Các Trận Hot"
    for node in bs.find_all(string=lambda t: t and "Các Trận Hot" in t):
        p = node.parent
        for _ in range(5):
            if p is None: break
            # Tìm các card con
            cards = p.find_all("div", recursive=False)
            if len(cards) >= 2:
                hot_section = p
                break
            p = p.parent
        if hot_section: break

    # Cách 2: tìm thẻ bao quanh nhóm card BLV
    if not hot_section:
        # Dựa vào cấu trúc: div chứa nhiều card có img alt="BLV..."
        for div in bs.find_all("div"):
            blv_imgs = div.find_all("img", alt=re.compile(r'^BLV\s', re.I), recursive=False)
            if len(blv_imgs) >= 3:
                hot_section = div
                break

    if not hot_section:
        log("  ⚠ Không tìm thấy section HOT, thử parse toàn trang...")
        hot_section = bs

    # Parse từng card trong section
    matches = []
    seen_urls = set()

    # Tìm các card — mỗi card thường là div chứa img[alt^="BLV"] + link "Xem"
    # Pattern: div > img[BLV] + tên giải + logo đội + tên + giờ + link
    card_containers = _find_cards(hot_section)
    log(f"  → Tìm thấy {len(card_containers)} card")

    for card in card_containers:
        m = _parse_card(card)
        if m and m["detail_url"] not in seen_urls:
            seen_urls.add(m["detail_url"])
            matches.append(m)

    return matches

def _find_cards(container):
    """Tìm tất cả card trận đấu (có BLV img và link Xem)."""
    cards = []
    # Duyệt đệ quy tìm element chứa img alt BLV và link Xem
    for el in container.find_all(["div","article","li"]):
        blv_img = el.find("img", alt=re.compile(r'^BLV\s', re.I))
        xem_link = el.find("a", string=re.compile(r'^Xem', re.I))
        if blv_img and xem_link:
            # Đảm bảo không phải container lớn bao nhiều card
            inner_blvs = el.find_all("img", alt=re.compile(r'^BLV\s', re.I))
            if len(inner_blvs) == 1:
                cards.append(el)
    return cards

def _parse_card(card):
    """Parse 1 card → dict match."""
    # BLV name từ img alt
    blv_img = card.find("img", alt=re.compile(r'^BLV\s', re.I))
    if not blv_img: return None
    blv_name = blv_img.get("alt","").replace("BLV","").strip()
    blv_avatar = blv_img.get("src","")
    if blv_avatar and not blv_avatar.startswith("http"):
        blv_avatar = urljoin(BASE_URL, blv_avatar)

    # Link detail
    xem_link = card.find("a", string=re.compile(r'^Xem', re.I))
    if not xem_link: return None
    href = xem_link.get("href","")
    detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)

    # Lấy toàn bộ text
    raw = card.get_text(" ", strip=True)

    # League: tìm text ngắn trước các tên đội
    # Cấu trúc: BLV Xxx | League | HomeTeam | VS/LIVE | AwayTeam | time
    league = ""
    all_imgs = card.find_all("img")
    for img in all_imgs:
        alt = img.get("alt","")
        if alt and "BLV" not in alt and "corner" not in alt.lower():
            src = img.get("src","")
            if "icon-sports" in src or "icon_sport" in src:
                # Text ngay sau icon sport là tên giải
                next_sib = img.find_next_sibling(string=True)
                if next_sib and next_sib.strip():
                    league = next_sib.strip()
                break

    # League từ cấu trúc khác: tìm text giữa blv và team name
    if not league:
        spans = card.find_all(["span","p","div"])
        for sp in spans:
            t = sp.get_text(strip=True)
            if (t and 3 < len(t) < 80
                    and not re.search(r'BLV|VS|Xem|Đặt|FB88|DEBET|Bắt|LIVE|Kết', t, re.I)
                    and not re.match(r'\d', t)):
                # Ưu tiên text có chứa từ giải đấu
                if re.search(r'League|Cup|FC|Serie|Liga|Premier|Champions|Giải|Vòng|Bowl|Open', t, re.I):
                    league = t
                    break

    # Home/Away team: các img logo đội
    team_imgs = [img for img in all_imgs
                 if img.get("src","").startswith("http")
                 and not any(s in img.get("src","") for s in
                             ["/uploads/", "/icon-sports/", "/icon_sport_", "hoadaotvlogo"])]

    home_logo = team_imgs[0].get("src","") if len(team_imgs) >= 1 else ""
    away_logo = team_imgs[1].get("src","") if len(team_imgs) >= 2 else ""

    # Tên đội: tìm alt của img logo hoặc text trong card
    home_team = team_imgs[0].get("alt","") if len(team_imgs) >= 1 else ""
    away_team = team_imgs[1].get("alt","") if len(team_imgs) >= 2 else ""

    # Nếu không có alt, tìm tên từ text gần img
    if not home_team or not away_team:
        # Fallback: tìm tên đội bằng pattern "X VS Y"
        vs_m = re.search(
            r'([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .\'()-]{1,40}?)'
            r'\s+(?:VS|vs)\s+'
            r'([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .\'()-]{1,40})',
            raw, re.UNICODE)
        if vs_m:
            if not home_team: home_team = vs_m.group(1).strip()
            if not away_team: away_team = vs_m.group(2).strip()

    if not home_team and not away_team:
        return None

    # Giờ + ngày: "23:45 | 13/04" hoặc "23:45- 13/04"
    time_m = re.search(r'(\d{1,2}:\d{2})\s*[-|]\s*(\d{1,2}/\d{2})', raw)
    time_raw = f"{time_m.group(1)} | {time_m.group(2)}" if time_m else ""
    t_str, d_str, sort_k = parse_time_date(time_raw)

    # Status
    if re.search(r'\bLIVE\b|\bĐang\s+(Live|Phát)\b|\bHiệp\s+\d', raw, re.I):
        status = "live"
    elif re.search(r'Kết\s+Thúc|Finished|\bFT\b', raw, re.I):
        status = "finished"
    else:
        status = "upcoming"

    base_title = f"{home_team} vs {away_team}" if home_team and away_team else ""

    return {
        "base_title":  base_title,
        "home_team":   home_team,
        "away_team":   away_team,
        "home_logo":   home_logo,
        "away_logo":   away_logo,
        "league":      league,
        "time_str":    t_str,
        "date_str":    d_str,
        "sort_key":    sort_k,
        "status":      status,
        "blv":         blv_name,
        "blv_avatar":  blv_avatar,
        "detail_url":  detail_url,
    }

# ── Merge trận cùng cặp đội ──────────────────────────────────
def _norm(s):
    return re.sub(r'[^a-z0-9]', '', s.lower().strip())

def merge_matches(raw_list):
    """
    Gộp các trận cùng cặp đội (khác BLV) thành 1 card.
    Trả về list match với blv_sources = [{"blv":..., "blv_avatar":..., "detail_url":...}]
    """
    groups = {}  # key: norm(home)_norm(away) → match dict
    for m in raw_list:
        # Key gộp: cặp đội (không phân biệt BLV hay góc bình luận)
        h = _norm(m.get("home_team",""))
        a = _norm(m.get("away_team",""))
        # Chuẩn hóa: luôn sort để MU vs Leeds = Leeds vs MU
        key = "_".join(sorted([h, a])) if h and a else m["detail_url"]

        if key not in groups:
            groups[key] = {
                **m,
                "blv_sources": [],
            }

        # Thêm nguồn BLV
        existing_urls = {s["detail_url"] for s in groups[key]["blv_sources"]}
        if m["detail_url"] not in existing_urls:
            groups[key]["blv_sources"].append({
                "blv":        m["blv"],
                "blv_avatar": m["blv_avatar"],
                "detail_url": m["detail_url"],
            })

        # Ưu tiên logo HTTP đầy đủ
        if not groups[key]["home_logo"] and m["home_logo"]:
            groups[key]["home_logo"] = m["home_logo"]
        if not groups[key]["away_logo"] and m["away_logo"]:
            groups[key]["away_logo"] = m["away_logo"]
        # Ưu tiên league không rỗng
        if not groups[key]["league"] and m["league"]:
            groups[key]["league"] = m["league"]

    result = list(groups.values())

    # Sort: live → upcoming → finished, rồi theo giờ
    pri = {"live":0, "upcoming":1, "finished":2}
    result.sort(key=lambda x: (pri.get(x.get("status","upcoming"),9),
                                x.get("sort_key","")))
    return result

# ── Crawl stream từ trang detail ─────────────────────────────
def crawl_streams(detail_url, blv, scraper):
    """
    Crawl trang detail → extract stream URLs theo mode.
    Trả về list stream dicts.
    """
    html = fetch(detail_url, scraper, retries=2)
    if not html: return [], {}
    bs = BeautifulSoup(html, "lxml")

    streams  = []
    info     = {}
    seen     = set()

    def add(name, url, kind="iframe"):
        url = url.strip()
        if url and url not in seen and len(url) > 10:
            seen.add(url)
            streams.append({
                "name":    name,
                "url":     url,
                "type":    kind,
                "referer": detail_url,
                "blv":     blv,
            })

    # ── 1. Logo đội từ trang detail (chính xác hơn trang chủ) ──
    logo_imgs = [
        img for img in bs.find_all("img")
        if img.get("src","").startswith("http")
        and "rapid-api.icu" in img.get("src","")
        and "/image/small" in img.get("src","")
    ]
    # Loại bỏ ảnh BLV avatar
    logo_imgs = [img for img in logo_imgs
                 if not any(s in img.get("src","")
                            for s in ["/uploads/", "hoadaotv"])]
    if len(logo_imgs) >= 1:
        info["home_logo"] = logo_imgs[0]["src"]
        info["home_team"] = logo_imgs[0].get("alt","")
    if len(logo_imgs) >= 2:
        info["away_logo"] = logo_imgs[1]["src"]
        info["away_team"] = logo_imgs[1].get("alt","")

    # ── 2. R2 CDN thumbnail ──────────────────────────────────
    r2 = re.findall(
        r'https://pub-[a-f0-9]+\.r2\.dev/[^\s\'"<>]+\.(?:webp|jpg|png)[^\s\'"<>]*',
        html, re.I)
    if r2: info["thumb_url"] = r2[0]

    # ── 3. og:image ──────────────────────────────────────────
    if not info.get("thumb_url"):
        og = bs.find("meta", attrs={"property":"og:image"})
        if og:
            u = og.get("content","").strip()
            _skip = ("favicon","logo","icon","hoadaotvlogo")
            if u.startswith("http") and not any(s in u for s in _skip):
                info["thumb_url"] = u

    # ── 4. iframe player ─────────────────────────────────────
    for fr in bs.find_all("iframe", src=True):
        src = fr["src"]
        if re.search(r"live|stream|embed|player|sport|watch", src, re.I):
            add("📺 Trực tiếp", src, "iframe")

    # ── 5. m3u8 / mpd trong script ──────────────────────────
    for m in re.finditer(r'(https?://[^\s\'"<>\\]+\.m3u8[^\s\'"<>\\]*)', html):
        add("🔵 HLS", m.group(1), "hls")
    for m in re.finditer(r'(https?://[^\s\'"<>\\]+\.mpd[^\s\'"<>\\]*)', html):
        add("📺 DASH", m.group(1), "dash")

    for sc in bs.find_all("script"):
        c = sc.string or ""
        for m in re.finditer(
                r'"(?:file|src|source|url|hls|stream)"\s*:\s*"(https?://[^"]+)"', c):
            u = m.group(1)
            if re.search(r"m3u8|live|stream|cdn|video", u, re.I):
                add("🔵 Stream", u, "hls")

    # ── 6. Nếu không có stream thực → tạo mode links ─────────
    if not streams:
        base = detail_url.split("?")[0]
        for mode_key, mode_label in STREAM_MODES:
            add(mode_label, f"{base}?mode={mode_key}", "iframe")

    return streams, info

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
        ct = r.headers.get("content-type","")
        if "html" in ct or "json" in ct: return None
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

    # Gradient nền navy đậm
    for y in range(H):
        t = y / H
        draw.line([(0,y),(W,y)],
                  fill=(int(8+8*t), int(12+14*t), int(32+22*t)))

    # Accent top
    draw.rectangle([(0,0),(W,5)], fill=(255,140,0))

    # Bar giải đấu
    BAR_H = 50
    draw.rectangle([(0,5),(W,5+BAR_H)], fill=(2,5,14))
    if league:
        draw.text((W//2, 5+BAR_H//2), league[:48],
                  fill=(255,200,40), font=_font(23), anchor="mm")
    draw.line([(0,5+BAR_H),(W,5+BAR_H)], fill=(255,140,0), width=1)

    CTOP  = 5 + BAR_H + 10
    CBOT  = H - 58
    AREA  = CBOT - CTOP
    LMAX  = min(AREA - 38, 160)  # logo 160px
    CX    = W // 2
    LX    = 130
    RX    = W - 130
    LY    = CTOP + (AREA - 36) // 2
    NY    = CBOT - 8

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
                draw.rounded_rectangle([(cx-half,cy-half),(cx+half,cy+half)],
                                       radius=12, fill=(14,26,56),
                                       outline=(65,105,185), width=2)
            except Exception:
                draw.rectangle([(cx-half,cy-half),(cx+half,cy+half)],
                               fill=(14,26,56), outline=(65,105,185), width=2)
            init = "".join(w[0].upper() for w in (name or "?").split()[:2]) or "?"
            draw.text((cx,cy), init, fill=(130,180,255), font=_font(52), anchor="mm")

        short = (name or "?")
        if len(short) > 16: short = short[:15]+"…"
        draw.text((cx+1,NY+1), short, fill=(0,0,0),     font=_font(22), anchor="mm")
        draw.text((cx,NY),     short, fill=(245,245,245),font=_font(22), anchor="mm")

    draw_logo(LX, LY, home_logo_url, home_team)
    draw_logo(RX, LY, away_logo_url, away_team)

    if status == "live":
        l1, c1, l2, c2, f1 = "● LIVE", (255,60,60), "",      (255,255,255), 40
    else:
        l1, c1 = time_str or "VS", (255,255,255)
        l2, c2 = date_str or "",   (140,150,175)
        f1 = 48

    draw.text((CX+1,LY+1), l1, fill=(0,0,0),  font=_font(f1), anchor="mm")
    draw.text((CX,LY),     l1, fill=c1,        font=_font(f1), anchor="mm")
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
    home, away = m.get("home_team",""), m.get("away_team","")
    base = f"{home} vs {away}" if home and away else m.get("base_title","")
    t, d, st = m.get("time_str",""), m.get("date_str",""), m.get("status","upcoming")
    if st == "live":     return f"{base}  🔴 LIVE"
    if st == "finished": return f"{base}  ✅"
    if t and d: return f"{base}  🕐 {t} | {d}"
    if t:       return f"{base}  🕐 {t}"
    return base

def build_channel(m, all_streams_by_blv, index):
    """
    m: match dict (đã merge)
    all_streams_by_blv: {blv_name: [stream, ...]}
    """
    ch_id  = make_id("hdt", index, re.sub(r"[^a-z0-9]","-",
                                          m.get("base_title","").lower())[:24])
    name   = build_name(m)
    league = m.get("league","")
    status = m.get("status","upcoming")
    srcs   = m.get("blv_sources",[])

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
    elif srcs:
        blv_n = srcs[0].get("blv","")
        if blv_n:
            labels.append({"text":f"🎙 {blv_n}","position":"top-right",
                           "color":"#00601f","text_color":"#fff"})

    # Build stream objects — mỗi BLV là 1 "stream" group
    stream_objs = []
    for src in srcs:
        blv_name = src.get("blv","")
        blv_url  = src["detail_url"]
        s_label  = f"🎙 {blv_name}" if blv_name else "Trực tiếp"
        s_id     = make_id(ch_id, blv_name)

        # Lấy streams của BLV này
        blv_streams = all_streams_by_blv.get(blv_url, [])

        if blv_streams:
            # Có stream thực → dùng
            links = []
            for li, s in enumerate(blv_streams):
                links.append({
                    "id":      make_id(s_id, f"l{li}"),
                    "name":    s.get("name","Link"),
                    "type":    s["type"],
                    "default": li == 0,
                    "url":     s["url"],
                    "request_headers": [
                        {"key":"Referer",    "value": s.get("referer", blv_url)},
                        {"key":"User-Agent", "value": CHROME_UA},
                    ],
                })
        else:
            # Fallback: tạo mode links từ URL detail
            base = blv_url.split("?")[0]
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
        # Fallback tuyệt đối
        fb = srcs[0]["detail_url"] if srcs else BASE_URL+"/"
        stream_objs.append({
            "id": "fb", "name": "Trực tiếp",
            "stream_links": [{"id":"l0","name":"SD","type":"iframe",
                              "default":True,"url":fb}]
        })

    # Thumbnail
    la, lb = m.get("home_logo",""), m.get("away_logo","")
    thumb_url = m.get("thumb_url","")

    if thumb_url:
        img_obj = {"padding":0,"background_color":"#0d1829","display":"cover",
                   "url":thumb_url,"width":800,"height":440}
    elif _PIL:
        uri = make_thumbnail(
            m.get("home_team",""), m.get("away_team",""),
            la, lb, m.get("time_str",""), m.get("date_str",""),
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
        "enable_detail": True,      # luôn bật để chọn BLV / chất lượng
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

def build_json(channels, now_str):
    return {
        "id":          "hoadaotv-live",
        "name":        "Hoa Đào TV – Xem bóng đá trực tiếp",
        "url":         BASE_URL + "/",
        "description": "Nền tảng xem thể thao trực tuyến hàng đầu Việt Nam. Trực tiếp bóng đá, bóng chuyền, esports với bình luận tiếng Việt chất lượng cao.",
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
    ap = argparse.ArgumentParser(description="Crawler hoadaotv.info → IPTV JSON")
    ap.add_argument("--no-stream", action="store_true", help="Bỏ qua crawl stream")
    ap.add_argument("--debug",     action="store_true", help="Lưu HTML debug")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    ap.add_argument("--limit",     type=int, default=0, help="Giới hạn số trận (0=tất cả)")
    args = ap.parse_args()

    log("\n" + "═"*64)
    log("  🌸  CRAWLER hoadaotv.info  v1")
    log("  📌  'Các Trận Hot' → merge BLV → 5 stream modes")
    log("═"*64 + "\n")

    now_vn  = datetime.now(VN_TZ)
    now_str = now_vn.strftime("%d/%m/%Y %H:%M") + " ICT (UTC+7)"
    scraper = make_scraper()

    # ── Bước 1: Tải trang chủ ───────────────────────────────
    log(f"📥 Tải {BASE_URL} ...")
    html = fetch(BASE_URL, scraper)
    if not html:
        log("❌ Không tải được trang chủ!"); sys.exit(1)
    if "Just a moment" in html or "cf-browser-verification" in html:
        log("⚠ Cloudflare challenge — thử lại sau."); sys.exit(1)

    if args.debug:
        with open(DEBUG_HTML,"w",encoding="utf-8") as f: f.write(html)
        log(f"  💾 {DEBUG_HTML}")

    # ── Bước 2: Parse trang chủ ─────────────────────────────
    log("\n🔍 Parse 'Các Trận Hot'...")
    bs = BeautifulSoup(html, "lxml")
    raw_matches = parse_hot_section(bs)
    log(f"  → {len(raw_matches)} card thô")

    if not raw_matches:
        log("❌ Không tìm thấy trận nào!"); sys.exit(1)

    # ── Bước 3: Merge trận cùng cặp đội ─────────────────────
    log("\n🔀 Merge trận cùng cặp đội...")
    matches = merge_matches(raw_matches)
    log(f"  → {len(raw_matches)} card → {len(matches)} trận sau merge\n")

    if args.limit > 0:
        matches = matches[:args.limit]
        log(f"  ⚠ Giới hạn {args.limit} trận\n")

    # In thông tin
    for i, m in enumerate(matches, 1):
        n_blv = len(m.get("blv_sources",[]))
        blvs  = ", ".join(s["blv"] for s in m.get("blv_sources",[]))
        logo_ok = "✓" if m.get("home_logo") and m.get("away_logo") else "✗"
        log(f"  {i:02d}. [{m.get('status','?'):8s}] "
            f"{m.get('home_team','?')[:20]:20s} vs {m.get('away_team','?')[:20]:20s} | "
            f"⏰{m.get('time_str','?')} {m.get('date_str','?')} | "
            f"🏆{m.get('league','?')[:25]:25s} | "
            f"🎙{n_blv}BLV({blvs[:30]}) logo={logo_ok}")

    # ── Bước 4: Crawl stream ─────────────────────────────────
    log("\n📡 Crawl stream + thumbnail...")
    all_streams_by_blv = {}  # {detail_url: [stream, ...]}

    if not args.no_stream:
        total_srcs = sum(len(m.get("blv_sources",[])) for m in matches)
        done = 0
        for m in matches:
            for src in m.get("blv_sources",[]):
                done += 1
                blv_url = src["detail_url"]
                log(f"  [{done:03d}/{total_srcs}] 🎙 {src.get('blv','?'):15s} → {blv_url[-50:]}")
                streams, info = crawl_streams(blv_url, src.get("blv",""), scraper)
                all_streams_by_blv[blv_url] = streams

                # Cập nhật logo/thumb từ detail nếu tốt hơn
                if info.get("thumb_url") and not m.get("thumb_url"):
                    m["thumb_url"] = info["thumb_url"]
                if info.get("home_logo") and not m.get("home_logo"):
                    m["home_logo"] = info["home_logo"]
                    m["home_team"] = info.get("home_team", m.get("home_team",""))
                if info.get("away_logo") and not m.get("away_logo"):
                    m["away_logo"] = info["away_logo"]
                    m["away_team"] = info.get("away_team", m.get("away_team",""))

                log(f"         streams={len(streams)} "
                    f"thumb={'✓' if info.get('thumb_url') else '✗'} "
                    f"logo={'✓' if info.get('home_logo') else '✗'}{'✓' if info.get('away_logo') else '✗'}")
                time.sleep(0.35)
    else:
        log("  ⚠ Bỏ qua crawl stream (--no-stream)")

    # ── Bước 5: Build channels ──────────────────────────────
    log("\n🏗  Build channels...")
    channels = []
    for i, m in enumerate(matches, 1):
        ch = build_channel(m, all_streams_by_blv, i)
        channels.append(ch)

    # ── Bước 6: Lưu JSON ────────────────────────────────────
    result = build_json(channels, now_str)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log(f"\n{'═'*64}")
    log(f"  ✅ {args.output}")
    log(f"  📊 {len(channels)} trận | "
        f"{sum(len(m.get('blv_sources',[])) for m in matches)} nguồn BLV")
    log(f"  🕐 {now_str}")
    log("═"*64+"\n")

if __name__ == "__main__":
    main()
