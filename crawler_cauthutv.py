#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler cauthutv.shop  v7  — PRODUCTION                   ║
║   Cấu trúc thực tế (từ HTML debug):                         ║
║     Section HOT: <div id="live-score-game-hot">             ║
║     Card:        <div class="card-single">                   ║
║     Link:        <a aria-label="TeamA vs TeamB" href="/slug">║
║     Logo:        <img class="img-lazy" data-src="...">       ║
║     League:      <span class="...tracking-wider...">         ║
║     Time:        <span class="...tracking-widest...">        ║
║     BLV:         text "BLV Tên" trong card                   ║
║   v7 changes:                                                ║
║     - Thumbnail xuất WebP (CDN webp), logo ưu tiên .webp    ║
║     - Tên đội sát logo hơn, logo + chữ to hơn               ║
║     - Gộp trận giống nhau (merge blv_sources)               ║
║     - enable_detail luôn bật để chọn BLV                    ║
╚══════════════════════════════════════════════════════════════╝
pip install cloudscraper beautifulsoup4 lxml requests pillow
"""

import argparse, base64, hashlib, io, json, re, sys, time
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin

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

# ─────────────────────────────────────────────
BASE_URL    = "https://cauthutv.shop"
OUTPUT_FILE = "cauthutv_iptv.json"
DEBUG_HTML  = "debug_cauthutv.html"
CHROME_UA   = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/124.0.0.0 Safari/537.36")
VN_TZ       = timezone(timedelta(hours=7))
SITE_ICON   = f"{BASE_URL}/assets/image/favicon64.png"
PLACEHOLDER  = {"padding":0,"background_color":"#0f3460","display":"cover",
                "url":SITE_ICON,"width":512,"height":512}

def log(*a, **kw): print(*a, **kw, flush=True)

# ═══════════════════════════════════════════════════════
#  HTTP
# ═══════════════════════════════════════════════════════

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

def fetch_html(url, scraper, retries=3):
    for i in range(retries):
        try:
            r = scraper.get(url, timeout=30, allow_redirects=True)
            r.raise_for_status()
            log(f"  ✓ [{r.status_code}] {url[:80]}")
            return r.text
        except Exception as e:
            wait = 2**i
            log(f"  ⚠ {i+1}/{retries}: {e} → {wait}s")
            if i < retries-1: time.sleep(wait)
    return None

# ═══════════════════════════════════════════════════════
#  CDN WebP URL helper
# ═══════════════════════════════════════════════════════

def to_webp_url(url: str) -> str:
    """
    Cố gắng chuyển URL ảnh sang phiên bản WebP của CDN.
    Hỗ trợ các pattern phổ biến:
      • pub-xxx.r2.dev  — đã là webp, giữ nguyên
      • img-cdn/...jpg?  → thêm &format=webp
      • sofascore / rapid-api / cdn thông thường → thêm ?format=webp
      • URL không có query → đổi extension .png/.jpg → .webp
    """
    if not url:
        return url
    # Đã là webp
    if re.search(r'\.webp(\?|$)', url, re.I):
        return url
    # R2 cloudflare — giữ nguyên
    if re.search(r'pub-[a-f0-9]+\.r2\.dev', url):
        return url
    # Thêm format=webp vào query string
    sep = "&" if "?" in url else "?"
    return url + sep + "format=webp"


# ═══════════════════════════════════════════════════════
#  Parse datetime
# ═══════════════════════════════════════════════════════

def parse_datetime(time_str, date_str):
    """Ghép time + date → (time_str, date_str, sort_key)."""
    if not time_str: return "", "", ""
    tm = re.match(r'(\d{1,2}):(\d{2})', time_str.strip())
    if not tm: return "", "", ""
    hh, mm = tm.group(1).zfill(2), tm.group(2)
    if not (int(hh) <= 23 and int(mm) <= 59): return "", "", ""

    dm = re.match(r'(\d{1,2})/(\d{2})', (date_str or "").strip())
    if dm:
        day, mon = dm.group(1).zfill(2), dm.group(2).zfill(2)
        return f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}"
    else:
        today = datetime.now(VN_TZ)
        return f"{hh}:{mm}", today.strftime("%d/%m"), f"{today.strftime('%m-%d')} {hh}:{mm}"

# ═══════════════════════════════════════════════════════
#  Parse card — dựa trên cấu trúc HTML thực tế
# ═══════════════════════════════════════════════════════

def parse_card(card_div):
    """
    Parse <div class="card-single"> → dict trận đấu.
    """
    a = card_div.find("a", href=True)
    if not a: return None

    href = a.get("href","")
    if not href: return None
    detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)

    aria = a.get("aria-label","").strip()
    home = away = ""
    if " vs " in aria:
        parts = aria.split(" vs ", 1)
        home = parts[0].strip()
        away = parts[1].strip()

    if not home or not away:
        return None

    league_el = card_div.find("span", class_=lambda c: c and "tracking-wider" in c)
    league = league_el.get_text(strip=True) if league_el else ""

    time_el = card_div.find("span", class_=lambda c: c and "tracking-widest" in c)
    time_raw = time_el.get_text(strip=True) if time_el else ""

    date_raw = ""
    for span in card_div.find_all("span", class_=lambda c: c and "text-gray-400" in c):
        t = span.get_text(strip=True)
        if re.match(r'\d{1,2}/\d{2}', t):
            date_raw = t
            break

    t_str, d_str, sort_k = parse_datetime(time_raw, date_raw)

    raw_text = card_div.get_text(" ", strip=True)
    if re.search(r'\bLIVE\b|\bĐang Live\b|\bHiệp\s+\d|\bPT\s+\d', raw_text, re.I):
        status = "live"
    elif re.search(r'Kết thúc|Finished|\bFT\b', raw_text, re.I):
        status = "finished"
    else:
        status = "upcoming"

    # ── Logo — chuyển sang WebP CDN URL ──────────────
    _SKIP_LOGO = ("/bgs/", "/bg_", "/bg-", "bg-soccer", "bg-volleyball",
                  "/icon-sports/", "icon_sport_", "/background", "opacity-20")
    logos = []
    for img in card_div.find_all("img", class_=lambda c: c and "img-lazy" in c):
        src = img.get("data-src") or img.get("src","")
        if not src: continue
        if not src.startswith("http"): continue
        if any(s in src for s in _SKIP_LOGO): continue
        if src.endswith("/image/s"):
            src = src + "mall"
        logos.append(to_webp_url(src))   # ← chuyển sang webp CDN

    home_logo = logos[0] if len(logos) >= 1 else ""
    away_logo = logos[1] if len(logos) >= 2 else ""

    # ── BLV ──────────────────────────────────────────
    blv = ""
    blv_container = card_div.find(
        "div", class_=lambda c: c and "flex" in c and "items-center" in c and "gap-2" in c
    )
    if blv_container:
        txt = blv_container.get_text(" ", strip=True)
        m = re.match(r'BLV\s+(.+)', txt)
        if m:
            blv = m.group(1).strip()
    if not blv:
        m2 = re.search(r'BLV\s+([\w\s\(\)\.]+?)(?:\s*FB88|\s*DEBET|\s*XEM)', raw_text)
        if m2:
            blv = m2.group(1).strip()

    sport = card_div.get("data-type","")

    return {
        "base_title":  f"{home} vs {away}",
        "home_team":   home,
        "away_team":   away,
        "status":      status,
        "league":      league,
        "sport":       sport,
        "time_str":    t_str,
        "date_str":    d_str,
        "sort_key":    sort_k,
        "detail_url":  detail_url,
        "home_logo":   home_logo,
        "away_logo":   away_logo,
        "blv":         blv,
        "blv_sources": [{"blv":blv, "detail_url":detail_url}],
    }

# ═══════════════════════════════════════════════════════
#  Gộp trận giống nhau (merge duplicate matches)
# ═══════════════════════════════════════════════════════

def merge_duplicate_matches(matches):
    """
    Gộp các trận có cùng base_title (home vs away) thành một entry,
    hợp nhất blv_sources và lấy thông tin tốt nhất (logo, league, v.v.).
    """
    seen = {}   # base_title → index trong result
    result = []
    for m in matches:
        key = m["base_title"].strip().lower()
        if key in seen:
            # Trận đã có — merge blv_sources
            existing = result[seen[key]]
            # Thêm blv_source nếu chưa có (tránh trùng detail_url)
            existing_urls = {s["detail_url"] for s in existing["blv_sources"]}
            for src in m["blv_sources"]:
                if src["detail_url"] not in existing_urls:
                    existing["blv_sources"].append(src)
                    existing_urls.add(src["detail_url"])
            # Cập nhật logo nếu bản trước chưa có
            if not existing.get("home_logo") and m.get("home_logo"):
                existing["home_logo"] = m["home_logo"]
            if not existing.get("away_logo") and m.get("away_logo"):
                existing["away_logo"] = m["away_logo"]
            # Ưu tiên trạng thái live
            if m.get("status") == "live":
                existing["status"] = "live"
        else:
            seen[key] = len(result)
            result.append(m)

    merged = len(matches) - len(result)
    if merged:
        log(f"  🔀 Gộp {merged} trận trùng → còn {len(result)} trận")
    return result

# ═══════════════════════════════════════════════════════
#  Tìm và parse section HOT
# ═══════════════════════════════════════════════════════

def extract_hot_matches(html, bs, debug=False):
    """
    Tìm <div id="live-score-game-hot"> và parse tất cả card-single bên trong.
    """
    hot_section = bs.find(id="live-score-game-hot")

    if not hot_section:
        log("  ⚠ Không tìm thấy #live-score-game-hot, tìm theo text...")
        for node in bs.find_all(string=lambda t: t and "Các Trận Hot" in t):
            parent = node.parent
            for _ in range(6):
                if parent is None: break
                cards = parent.find_all("div", class_="card-single")
                if cards:
                    hot_section = parent
                    log(f"  → Tìm thấy qua text: {len(cards)} cards")
                    break
                parent = parent.parent
            if hot_section: break

    if not hot_section:
        if debug:
            log("  ❌ Không tìm thấy section HOT!")
            log("  Tất cả id trong trang:")
            for tag in bs.find_all(id=True):
                log(f"    #{tag['id']} <{tag.name}>")
        return []

    cards = hot_section.find_all("div", class_="card-single")
    log(f"  ✅ Tìm thấy {len(cards)} card-single trong HOT section")

    if debug:
        log(f"  Section: <{hot_section.name} id='{hot_section.get('id','')}' "
            f"class='{' '.join(hot_section.get('class',[]))[:60]}'>")

    matches = []
    for i, card in enumerate(cards):
        m = parse_card(card)
        if m:
            matches.append(m)
        elif debug:
            a = card.find("a", href=True)
            log(f"  ⚠ Card {i+1} bỏ qua: aria='{a.get('aria-label','?') if a else '?'}'")

    log(f"  → Parse được {len(matches)}/{len(cards)} trận hợp lệ")

    # ── Gộp trận giống nhau ──
    matches = merge_duplicate_matches(matches)
    return matches

# ═══════════════════════════════════════════════════════
#  Crawl detail page → stream + logo
# ═══════════════════════════════════════════════════════

def crawl_detail(detail_url, blv, scraper):
    html = fetch_html(detail_url, scraper, retries=2)
    if not html: return [], {}
    bs   = BeautifulSoup(html, "lxml")

    info = {}

    # ── R2 thumbnail (pub-xxx.r2.dev) ──
    r2_urls = re.findall(
        r'https://pub-[a-f0-9]+\.r2\.dev/[^\s\'"<>]+\.(?:webp|jpg|jpeg|png)[^\s\'"<>]*',
        html, re.I
    )
    if r2_urls:
        info["thumb_url"] = r2_urls[0]

    # ── Logo 2 đội — chuyển sang WebP CDN URL ──
    logos = []
    for img in bs.find_all("img", class_=lambda c: c and "img-lazy" in c):
        src = img.get("data-src") or img.get("src","")
        if src and src.startswith("http"):
            _skip = ("/bgs/", "/icon-sports/", "icon_sport_", "/bg_", "/background")
            if not any(s in src for s in _skip):
                logos.append(to_webp_url(src))   # ← webp CDN
    if len(logos) >= 1: info["home_logo"] = logos[0]
    if len(logos) >= 2: info["away_logo"] = logos[1]

    seen, streams = set(), []
    def add(name, url, kind):
        url = url.strip()
        if url and url not in seen and len(url) > 12:
            seen.add(url)
            streams.append({"name":name,"url":url,"type":kind,
                            "referer":detail_url,"blv":blv})

    # ── 1. iframe embed ──
    for fr in bs.find_all("iframe", src=True):
        src = fr["src"]
        if re.search(r"live|stream|embed|player|sport|watch|truc.?tiep", src, re.I):
            add("📺 Xem trực tiếp", src, "iframe")

    # ── 2. m3u8 HLS ──
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("HLS", m.group(1), "hls")

    # ── 3. DASH ──
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.mpd(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("DASH", m.group(1), "dash")

    # ── 4. JSON config trong script ──
    for sc in bs.find_all("script"):
        c = sc.string or ""

        for m in re.finditer(
                r'"(?:file|src|source|url|hls|playlist|videoUrl|streamUrl|hlsUrl|hlssrc)"\s*:\s*"(https?://[^"]+)"', c):
            u = m.group(1)
            if re.search(r"m3u8|stream|live|video|play|hls", u, re.I):
                add("HLS stream", u, "hls")

        for m in re.finditer(r'(?:playerConfig|player_config|PLAYER_CONFIG)\s*=\s*(\{[^;]+\})', c, re.S):
            try:
                import json as _json
                cfg = _json.loads(m.group(1))
                srcs = cfg.get("sources", cfg.get("source", []))
                if isinstance(srcs, str): srcs = [{"src": srcs}]
                for s in (srcs if isinstance(srcs, list) else []):
                    u = s.get("src","") or s.get("file","") or s.get("url","")
                    lbl = s.get("label","") or s.get("quality","") or "Auto"
                    if u and u.startswith("http"):
                        add(f"🎬 {lbl}", u, "hls")
            except Exception:
                pass

        for m in re.finditer(r'sources\s*:\s*\[([^\]]+)\]', c, re.S):
            inner = m.group(1)
            for sm in re.finditer(r'src\s*:\s*["\']([^"\']+)["\'].*?label\s*:\s*["\']([^"\']+)["\']', inner, re.S):
                add(f"🎬 {sm.group(2)}", sm.group(1), "hls")
            for sm in re.finditer(r'label\s*:\s*["\']([^"\']+)["\'].*?src\s*:\s*["\']([^"\']+)["\']', inner, re.S):
                add(f"🎬 {sm.group(1)}", sm.group(2), "hls")

        for m in re.finditer(r'(?:streamUrl|videoUrl|hlsUrl|playerUrl|src)\s*[=:]\s*["\']([^"\']+)["\']', c):
            u = m.group(1)
            if u.startswith("http") and re.search(r"m3u8|stream|live|cdn", u, re.I):
                add("Live stream", u, "hls")

    # ── 5. data attributes ──
    for tag in bs.find_all(attrs={"data-src": True}):
        u = tag.get("data-src","")
        if u and u.startswith("http") and re.search(r"m3u8|stream|live", u, re.I):
            add("data-src stream", u, "hls")
    for tag in bs.find_all(attrs={"data-stream": True}):
        add("data-stream", tag["data-stream"], "hls")
    for tag in bs.find_all(attrs={"data-url": True}):
        u = tag.get("data-url","")
        if u and re.search(r"m3u8|stream", u, re.I):
            add("data-url", u, "hls")

    # ── 6. Gán nhãn chất lượng ──
    _QUAL_MAP = [
        (re.compile(r'1080|fhd|fullhd|full.hd', re.I), "📺 Full HD 1080p"),
        (re.compile(r'720|hd(?!c)',              re.I), "🔵 HD 720p"),
        (re.compile(r'480|sd',                   re.I), "⚪ SD 480p"),
        (re.compile(r'360',                      re.I), "⚪ SD 360p"),
    ]
    for s in streams:
        if s["name"] in ("HLS", "HLS stream", "data-src stream", "Live stream"):
            url_l = s["url"].lower()
            for pat, label in _QUAL_MAP:
                if pat.search(url_l):
                    s["name"] = label
                    break
            else:
                if s["name"] == "HLS":
                    s["name"] = "🔵 HD Nhanh"

    if not streams:
        streams.append({"name":"📺 Xem trực tiếp","url":detail_url,"type":"iframe",
                        "referer":detail_url,"blv":blv})
    return streams, info

# ═══════════════════════════════════════════════════════
#  Thumbnail — WebP, logo to hơn, tên đội sát logo
# ═══════════════════════════════════════════════════════

def _font(size, bold=True):
    if not _PIL: return None
    for p in [
        f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf",
        f"/usr/share/fonts/truetype/liberation/LiberationSans-{'Bold' if bold else 'Regular'}.ttf",
    ]:
        try: return ImageFont.truetype(p, size)
        except: pass
    return ImageFont.load_default()

def fetch_logo(url, max_px=300):
    """
    Tải logo từ URL — thử phiên bản webp trước, fallback về URL gốc.
    """
    if not url or not _PIL: return None
    candidates = [url]
    # Thêm webp fallback nếu URL gốc chưa phải webp
    webp_url = to_webp_url(url)
    if webp_url != url:
        candidates = [webp_url, url]   # ưu tiên webp

    for try_url in candidates:
        try:
            r = requests.get(try_url.strip(), timeout=8,
                            headers={"User-Agent": "Mozilla/5.0",
                                     "Accept": "image/webp,image/*,*/*"}, stream=True)
            r.raise_for_status()
            if "html" in r.headers.get("content-type",""): continue
            data = b""
            for chunk in r.iter_content(65536):
                data += chunk
                if len(data) > 3_000_000: break
            img = Image.open(io.BytesIO(data)).convert("RGBA")
            img.thumbnail((max_px, max_px), Image.LANCZOS)
            return img
        except:
            continue
    return None

def make_thumbnail(home_team, away_team, home_logo_url, away_logo_url,
                   time_str="", date_str="", status="upcoming", league=""):
    """
    Tạo thumbnail WebP base64.
    - Logo to hơn (LMAX 155px)
    - Tên đội sát logo (NY = tâm logo + bán kính + gap nhỏ)
    - Font tên đội 23px, VS 50px, LIVE 42px
    - Output: data:image/webp;base64,...
    """
    if not _PIL: return ""

    W, H = 700, 394
    canvas = Image.new("RGB", (W, H))
    draw   = ImageDraw.Draw(canvas)

    # ── Nền gradient navy ──
    for y in range(H):
        t = y / H
        r_ = int(10 + 5*t)
        g_ = int(14 + 10*t)
        b_ = int(26 + 15*t)
        draw.line([(0,y),(W,y)], fill=(r_, g_, b_))

    # ── Viền accent ──
    draw.rectangle([(0,0),(3,H)],   fill=(255,140,0))
    draw.rectangle([(0,0),(W,3)],   fill=(255,140,0))

    # ── Bar giải đấu ──
    BAR_H = 48
    for y in range(BAR_H):
        alpha_t = 1.0 - y/BAR_H * 0.3
        draw.line([(0,3+y),(W,3+y)],
                  fill=(int(5*alpha_t), int(8*alpha_t), int(18*alpha_t)))

    if league:
        draw.text((W//2, 3+BAR_H//2+1), league[:42],
                  fill=(255, 195, 40), font=_font(24), anchor="mm")
    draw.line([(0,3+BAR_H),(W,3+BAR_H)], fill=(255,140,0), width=1)

    # ── Layout chính ──
    CTOP  = 3 + BAR_H + 10
    CBOT  = H - 48
    AREA_H = CBOT - CTOP

    # ── Logo: lớn hơn (tối đa 155px) ──
    LMAX = min(AREA_H - 20, 155)       # tăng từ 130 → 155
    CX   = W // 2
    LX   = 130
    RX   = W - 130
    # Tâm Y logo: căn giữa vùng, dịch lên một chút để nhường chỗ tên đội
    LY   = CTOP + (AREA_H - LMAX//2 - 18) // 2

    # Tên đội: sát ngay dưới logo (LY + bán kính logo + gap nhỏ)
    NY   = LY + LMAX // 2 + 18        # sát logo, không kéo xuống footer

    def draw_logo(cx, cy, url, name):
        logo = fetch_logo(url, LMAX * 3) if url else None
        if logo:
            if logo.mode != "RGBA": logo = logo.convert("RGBA")
            lw, lh = logo.size
            scale  = min((LMAX-4)/lw, (LMAX-4)/lh, 1.0)
            nw     = max(1, int(lw * scale))
            nh     = max(1, int(lh * scale))
            logo   = logo.resize((nw, nh), Image.LANCZOS)
            ox, oy = cx - nw//2, cy - nh//2
            canvas.paste(logo.convert("RGB"), (ox, oy), logo.split()[3])
        else:
            # Fallback: khung chữ tắt
            sz  = LMAX * 3 // 4
            x0, y0 = cx - sz//2, cy - sz//2
            x1, y1 = cx + sz//2, cy + sz//2
            draw.rectangle([(x0,y0),(x1,y1)],
                           fill=(15, 28, 58), outline=(70, 110, 190), width=2)
            init = "".join(w[0].upper() for w in (name or "?").split()[:2]) or "?"
            draw.text((cx, cy), init,
                      fill=(140, 185, 255), font=_font(44), anchor="mm")

        # ── Tên đội — font 23, sát logo ──
        short = (name or "?")
        if len(short) > 18: short = short[:17] + "…"
        # Shadow
        draw.text((cx+1, NY+1), short, fill=(0,0,0),      font=_font(23), anchor="mm")
        draw.text((cx,   NY),   short, fill=(240,240,240), font=_font(23), anchor="mm")

    draw_logo(LX, LY, home_logo_url, home_team)
    draw_logo(RX, LY, away_logo_url, away_team)

    # ── Vùng giữa: VS / Giờ / LIVE ──
    if status == "live":
        l1, c1 = "● LIVE", (255, 65, 65)
        l2, c2 = "",       (255, 255, 255)
        f1 = 42                         # tăng từ 36 → 42
    else:
        l1, c1 = time_str or "VS", (255, 255, 255)
        l2, c2 = date_str or "",   (145, 155, 175)
        f1 = 50                         # tăng từ 44 → 50

    draw.text((CX+1, LY+1), l1, fill=(0,0,0,140), font=_font(f1), anchor="mm")
    draw.text((CX,   LY),   l1, fill=c1,           font=_font(f1), anchor="mm")
    if l2:
        draw.text((CX, LY+40), l2, fill=c2, font=_font(16, False), anchor="mm")

    # ── Footer ──
    draw.rectangle([(0, H-44),(W, H)], fill=(5, 8, 16))
    draw.line([(0, H-44),(W, H-44)], fill=(255,140,0,90), width=1)

    # ── Lưu WebP (thay JPEG) ──
    buf = io.BytesIO()
    canvas.save(buf, format="WEBP", quality=88, method=4)
    return "data:image/webp;base64," + base64.b64encode(buf.getvalue()).decode()

# ═══════════════════════════════════════════════════════
#  Build channel + JSON
# ═══════════════════════════════════════════════════════

def make_id(*parts):
    return hashlib.md5("-".join(str(p) for p in parts).encode()).hexdigest()[:16]

def build_name(m):
    home, away = m.get("home_team",""), m.get("away_team","")
    base = f"{home} vs {away}" if home and away else m.get("base_title","")
    t, d = m.get("time_str",""), m.get("date_str","")
    st   = m.get("status","upcoming")
    if st == "live":     return f"{base}  🔴 LIVE"
    if st == "finished": return f"{base}  ✅"
    if t and d: return f"{base}  🕐 {t} | {d}"
    if t:       return f"{base}  🕐 {t}"
    return base

def build_channel(m, all_streams, index):
    ch_id  = make_id("ctt", index, re.sub(r"[^a-z0-9]","-",m.get("base_title","").lower())[:24])
    name   = build_name(m)
    league = m.get("league","")
    status = m.get("status","upcoming")

    sc_map = {
        "live":     {"text":"● Live",          "color":"#E73131","text_color":"#fff"},
        "upcoming": {"text":"🕐 Sắp diễn ra", "color":"#d54f1a","text_color":"#fff"},
        "finished": {"text":"✅ Kết thúc",     "color":"#444444","text_color":"#fff"},
    }
    labels = [{**sc_map.get(status, sc_map["live"]), "position":"top-left"}]

    blv_names = [s["blv"] for s in m.get("blv_sources",[]) if s.get("blv")]
    if len(blv_names) > 1:
        labels.append({"text":f"🎙 {len(blv_names)} BLV","position":"top-right",
                       "color":"#00601f","text_color":"#fff"})
    elif blv_names:
        labels.append({"text":f"🎙 {blv_names[0]}","position":"top-right",
                       "color":"#00601f","text_color":"#fff"})

    # ── Stream theo BLV ──────────────────────────────
    blv_groups = {}
    for s in all_streams:
        blv_groups.setdefault(s.get("blv") or "__", []).append(s)

    stream_objs = []
    for idx,(bkey,raw_s) in enumerate(blv_groups.items()):
        if not raw_s: continue
        slabel = f"🎙 {bkey}" if bkey != "__" else f"Nguồn {idx+1}"
        slinks = []
        for li,s in enumerate(raw_s):
            ref = s.get("referer", BASE_URL+"/")
            slinks.append({
                "id": make_id(ch_id,f"b{idx}",f"l{li}"),
                "name": s.get("name","Auto"),
                "type": s["type"], "default": li==0, "url": s["url"],
                "request_headers":[
                    {"key":"Referer","value":ref},
                    {"key":"User-Agent","value":CHROME_UA},
                ],
            })
        stream_objs.append({"id":make_id(ch_id,f"st{idx}"),
                             "name":slabel,"stream_links":slinks})

    if not stream_objs:
        fb = m.get("blv_sources",[{}])[0].get("detail_url",BASE_URL+"/") if m.get("blv_sources") else BASE_URL+"/"
        stream_objs.append({"id":"fb","name":"Trực tiếp","stream_links":[{
            "id":"lnk0","name":"Link 1","type":"iframe","default":True,"url":fb,
            "request_headers":[{"key":"Referer","value":fb},
                               {"key":"User-Agent","value":CHROME_UA}],
        }]})

    la = m.get("home_logo",""); lb = m.get("away_logo","")
    thumb_url = m.get("thumb_url","")

    if thumb_url:
        img_obj = {"padding":0,"background_color":"#0a0e1a","display":"cover",
                   "url":thumb_url,"width":700,"height":394}
    elif _PIL:
        uri = make_thumbnail(
            m.get("home_team",""), m.get("away_team",""),
            la, lb, m.get("time_str",""), m.get("date_str",""),
            status, league,
        )
        img_obj = ({"padding":0,"background_color":"#0a0e1a","display":"cover",
                    "url":uri,"width":700,"height":394} if uri else PLACEHOLDER)
    else:
        img_obj = PLACEHOLDER

    content_name = name
    if league and len(league) < 50: content_name += f" · {league.strip()}"

    # ── 1 BLV → phát thẳng; ≥2 BLV → vào trang thông tin chọn BLV ──
    has_multi = len(stream_objs) > 1
    return {
        "id":            ch_id,
        "name":          name,
        "type":          "multi" if has_multi else "single",
        "display":       "thumbnail-only",
        "enable_detail": has_multi,     # True chỉ khi ≥2 BLV
        "image":         img_obj,
        "labels":        labels,
        "sources": [{
            "id":   make_id(ch_id,"src"),
            "name": "CauThuTV Live",
            "contents": [{
                "id":      make_id(ch_id,"ct"),
                "name":    content_name,
                "streams": stream_objs,
            }],
        }],
    }

def build_json(channels, now_str):
    return {
        "id":          "cauthutv-live",
        "name":        "CauThu TV - Trực tiếp thể thao",
        "url":         BASE_URL + "/",
        "description": "Nền tảng xem thể thao trực tuyến hàng đầu Việt Nam. Trực tiếp bóng đá, bóng rổ, tennis, esports với bình luận tiếng Việt chất lượng cao.",
        "disable_ads": True,
        "color":       "#0f3460",
        "grid_number": 3,
        "image":       {"type":"cover","url":SITE_ICON},
        "groups": [{
            "id":       "tran-hot",
            "name":     "🔥 Các Trận Hot",
            "image":    None,
            "channels": channels,
        }],
    }

# ═══════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-stream", action="store_true", help="Bỏ qua crawl stream")
    ap.add_argument("--debug",     action="store_true", help="Lưu HTML debug")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    args = ap.parse_args()

    log("\n" + "═"*62)
    log("  🔥  CRAWLER cauthutv.shop  v7  — PRODUCTION")
    log("  📌  id='live-score-game-hot' → card-single → aria-label")
    log("  🖼   Thumbnail: WebP CDN | Logo to | Tên sát logo")
    log("  🔀  Merge trận trùng | enable_detail luôn bật")
    log("═"*62 + "\n")

    now_vn  = datetime.now(VN_TZ)
    now_str = now_vn.strftime("%d/%m/%Y %H:%M") + " ICT (UTC+7)"
    scraper = make_scraper()

    log(f"📥 Tải trang chủ {BASE_URL}...")
    html = fetch_html(BASE_URL, scraper)
    if not html:
        log("❌ Không tải được trang chủ."); sys.exit(1)
    if "Just a moment" in html or "cf-browser-verification" in html:
        log("⚠ Cloudflare challenge — thử lại sau."); sys.exit(1)

    if args.debug:
        with open(DEBUG_HTML,"w",encoding="utf-8") as f: f.write(html)
        log(f"  💾 {DEBUG_HTML}")

    bs = BeautifulSoup(html, "lxml")

    log("\n🔍 Tìm section 'Các Trận Hot' (#live-score-game-hot)...")
    matches = extract_hot_matches(html, bs, debug=args.debug)

    if not matches:
        log("❌ Không tìm thấy trận nào!")
        log("  💡 Chạy --debug để lưu HTML và kiểm tra cấu trúc.")
        sys.exit(1)

    # Sort: live → upcoming → finished, rồi theo giờ
    pri = {"live":0,"upcoming":1,"finished":2}
    matches.sort(key=lambda x: (pri.get(x.get("status","upcoming"),9), x.get("sort_key","")))
    log(f"\n  ✅ {len(matches)} trận HOT (sau merge)\n")

    # Crawl detail
    log("🖼  Crawl detail + tạo thumbnail WebP...")
    channels = []
    for i, m in enumerate(matches, 1):
        all_streams = []

        if not args.no_stream:
            for src in m.get("blv_sources",[]):
                streams, info = crawl_detail(src["detail_url"], src.get("blv",""), scraper)
                if info.get("thumb_url") and not m.get("thumb_url"):
                    m["thumb_url"] = info["thumb_url"]
                if info.get("home_logo") and not m.get("home_logo"):
                    m["home_logo"] = info["home_logo"]
                if info.get("away_logo") and not m.get("away_logo"):
                    m["away_logo"] = info["away_logo"]
                seen_u = {s["url"] for s in all_streams}
                all_streams.extend(s for s in streams if s["url"] not in seen_u)
            time.sleep(0.3)

        has_thumb = bool(m.get("thumb_url"))
        blv_count = len(m.get("blv_sources",[]))
        log(f"  [{i:03d}] {m.get('base_title','?')[:40]:40s} | "
            f"{'🔴' if m.get('status')=='live' else '🕐'} | "
            f"thumb={'R2' if has_thumb else ('WebP' if (m.get('home_logo') or m.get('away_logo')) else '✗')} | "
            f"BLV={blv_count} | streams={len(all_streams)}")

        channels.append(build_channel(m, all_streams, i))

    result = build_json(channels, now_str)
    with open(args.output,"w",encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log(f"\n{'═'*62}")
    log(f"  ✅ {args.output}  —  {len(channels)} trận HOT")
    log(f"  🕐 {now_str}")
    log("═"*62+"\n")

if __name__ == "__main__":
    main()
