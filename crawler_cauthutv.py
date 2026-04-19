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

import argparse, base64, hashlib, io, json, os, re, sys, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
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
THUMB_DIR   = "thumbnails"          # thư mục lưu file .webp (commit vào repo)
CHROME_UA   = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/124.0.0.0 Safari/537.36")
VN_TZ       = timezone(timedelta(hours=7))
SITE_ICON   = f"{BASE_URL}/assets/image/favicon64.png"
PLACEHOLDER  = {"padding":0,"background_color":"#0f3460","display":"cover",
                "url":SITE_ICON,"width":512,"height":512}

def _cdn_base() -> str:
    """
    Trả về base URL CDN cho thư mục thumbnails.
    Ưu tiên: biến môi trường THUMB_CDN_BASE (override thủ công).
    Fallback: tự động tính từ GITHUB_REPOSITORY + GITHUB_REF_NAME.
    Local: trả về chuỗi rỗng → dùng base64 fallback.
    """
    override = os.environ.get("THUMB_CDN_BASE","").rstrip("/")
    if override:
        return override
    repo   = os.environ.get("GITHUB_REPOSITORY","")   # e.g. "user/repo"
    branch = os.environ.get("GITHUB_REF_NAME","main")
    if repo:
        return f"https://raw.githubusercontent.com/{repo}/{branch}/{THUMB_DIR}"
    return ""

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

# ── Bảng mode cauthutv.shop ──────────────────────────────────────
# Danh sách mode crawl — chỉ dùng để fetch HTML, KHÔNG dùng làm nhãn stream
STREAM_MODES = [
    ("sd",   "sd"),
    ("flv",  "flv"),
    ("ndsd", "ndsd"),
    ("mp",   "mp"),
]

def _label_m3u8(url: str) -> str:
    """
    Gán nhãn stream dựa trên URL m3u8:
    - chứa 'index.m3u8' → 📺 HD
    - còn lại           → 📡 Nhà Đài
    """
    if re.search(r'index\.m3u8', url, re.I):
        return "📺 HD"
    return "📡 Nhà Đài"


def _extract_m3u8_from_page(html: str, bs, page_url: str, blv: str, seen: set) -> list:
    """
    Tách CHỈ các stream m3u8 HLS từ 1 trang.
    Bỏ hoàn toàn: iframe, DASH, URL không phải .m3u8.
    Nhãn tự động: index.m3u8 → 📺 HD | khác → 📡 Nhà Đài.
    """
    added = []

    def _add(url):
        url = url.strip()
        if not url or url in seen: return
        if ".m3u8" not in url.lower(): return   # chỉ nhận m3u8
        # Bỏ URL nhà đài chứa "livehd" — không dùng được
        label = _label_m3u8(url)
        if label == "📡 Nhà Đài" and re.search(r'livehd', url, re.I):
            return
        seen.add(url)
        added.append({"name": label, "url": url, "type": "hls",
                      "referer": page_url, "blv": blv})

    # 1. m3u8 trực tiếp trong HTML raw
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)', html):
        _add(m.group(1))

    # 2. JSON trong script tags
    for sc in bs.find_all("script"):
        c = sc.string or ""

        # key-value JSON: "file":"...", "src":"..."
        for m in re.finditer(
                r'"(?:file|src|source|url|hls|playlist|videoUrl|streamUrl|hlsUrl|hlssrc)"\s*:\s*"(https?://[^"]*\.m3u8[^"]*)"', c):
            _add(m.group(1))

        # playerConfig = {...}
        for m in re.finditer(r'(?:playerConfig|player_config|PLAYER_CONFIG)\s*=\s*(\{[^;]+\})', c, re.S):
            try:
                cfg = json.loads(m.group(1))
                srcs = cfg.get("sources", cfg.get("source", []))
                if isinstance(srcs, str): srcs = [{"src": srcs}]
                for s in (srcs if isinstance(srcs, list) else []):
                    u = s.get("src","") or s.get("file","") or s.get("url","")
                    if u and ".m3u8" in u.lower(): _add(u)
            except Exception:
                pass

        # sources:[...] mảng
        for m in re.finditer(r'sources\s*:\s*\[([^\]]+)\]', c, re.S):
            for sm in re.finditer(r"(https?://[^\s'\"]+\.m3u8[^\s'\"]*)", m.group(1)):
                _add(sm.group(1))

        # window.streamUrl = "..." style
        for m in re.finditer(r'(?:streamUrl|videoUrl|hlsUrl|hlssrc|playerUrl)\s*[=:]\s*["\']([^"\']+\.m3u8[^"\']*)["\']', c):
            _add(m.group(1))

    # 3. data-* attributes
    for attr in ("data-src", "data-stream", "data-url", "data-hls"):
        for tag in bs.find_all(attrs={attr: True}):
            u = tag.get(attr, "")
            if u and ".m3u8" in u.lower(): _add(u)

    return added


def crawl_detail(detail_url, blv, scraper):
    """
    Crawl trang trận đấu qua 4 mode (sd/flv/ndsd/mp).
    Chỉ thu thập stream m3u8 HLS:
      - URL có index.m3u8 → nhãn 📺 HD
      - URL m3u8 khác     → nhãn 📡 Nhà Đài
    Không lấy iframe / DASH / URL không phải m3u8.
    """
    base_url = re.sub(r'\?.*$', '', detail_url.strip())
    info = {}
    seen = set()
    streams = []

    # ── Lấy logo + thumbnail từ trang gốc ──
    html_base = fetch_html(base_url, scraper, retries=2)
    if html_base:
        bs_base = BeautifulSoup(html_base, "lxml")
        r2_urls = re.findall(
            r'https://pub-[a-f0-9]+\.r2\.dev/[^\s\'"<>]+\.(?:webp|jpg|jpeg|png)[^\s\'"<>]*',
            html_base, re.I
        )
        if r2_urls:
            info["thumb_url"] = r2_urls[0]

        logos = []
        for img in bs_base.find_all("img", class_=lambda c: c and "img-lazy" in c):
            src = img.get("data-src") or img.get("src","")
            if src and src.startswith("http"):
                _skip = ("/bgs/", "/icon-sports/", "icon_sport_", "/bg_", "/background")
                if not any(s in src for s in _skip):
                    logos.append(to_webp_url(src))
        if len(logos) >= 1: info["home_logo"] = logos[0]
        if len(logos) >= 2: info["away_logo"] = logos[1]

        # Thử tìm m3u8 ngay ở trang gốc
        streams.extend(_extract_m3u8_from_page(html_base, bs_base, base_url, blv, seen))

    # ── Crawl từng mode để lấy thêm m3u8 ──
    for mode_key, _ in STREAM_MODES:
        mode_url = f"{base_url}?mode={mode_key}"
        html_m = fetch_html(mode_url, scraper, retries=2)
        if not html_m:
            continue
        bs_m = BeautifulSoup(html_m, "lxml")
        new = _extract_m3u8_from_page(html_m, bs_m, mode_url, blv, seen)
        if new:
            streams.extend(new)
            log(f"    ✓ mode={mode_key}: {len(new)} m3u8 ({[s['name'] for s in new]})")
        time.sleep(0.2)

    # ── Sắp xếp: HD trước, Nhà Đài sau ──
    streams.sort(key=lambda s: (0 if s["name"] == "📺 HD" else 1, s["url"]))

    if not streams:
        log(f"    ⚠ Không tìm thấy m3u8 nào — bỏ qua stream")

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

# ═══════════════════════════════════════════════════════
#  Sport theme definitions
# ═══════════════════════════════════════════════════════

def _sport_key(sport: str, league: str) -> str:
    """Chuẩn hóa tên môn → key theme."""
    raw = (sport + " " + league).lower()
    if re.search(r'soccer|football|bóng.?đá|futsal|v\.?league|laliga|premier|bundesliga|serie|ligue|champion|world.?cup|euro|afc|asean|cup', raw):
        return "soccer"
    if re.search(r'basketball|bóng.?rổ|nba|euroleague', raw):
        return "basketball"
    if re.search(r'tennis|atp|wta|grand.?slam|wimbledon|roland', raw):
        return "tennis"
    if re.search(r'volleyball|bóng.?chuyền|vnl', raw):
        return "volleyball"
    if re.search(r'esport|e.sport|lol|dota|csgo|valorant|mobile.?legend|pubg|gaming', raw):
        return "esports"
    if re.search(r'boxing|box|mma|ufc|muay|kickbox|wrestling', raw):
        return "boxing"
    if re.search(r'baseball|softball', raw):
        return "baseball"
    if re.search(r'badminton|cầu.?lông', raw):
        return "badminton"
    if re.search(r'golf|pga|masters', raw):
        return "golf"
    if re.search(r'formula|f1|motogp|nascar|racing', raw):
        return "racing"
    return "default"

# Theme: (bg_top, bg_bot, bar_color, bar_text, accent, name_color, name_shadow, vs_color)
SPORT_THEMES = {
    #           bg_top           bg_bot          bar          bar_txt       accent         name_fg        name_sh       vs_fg
    "soccer":   ((232,245,232),  (210,235,210),  (27,122,27), (255,255,255),(27,122,27),  (20, 60, 20),  (200,230,200),(27,122,27)),
    "basketball":((255,244,224), (255,228,188),  (200,85,0),  (255,255,255),(200,85,0),   (100,40, 0),   (255,220,160),(200,85,0)),
    "tennis":   ((245,240,210),  (228,220,175),  (60,130,60), (255,255,255),(60,130,60),  (50, 80, 20),  (210,200,140),(60,130,60)),
    "volleyball":((224,238,255), (195,220,255),  (20,90,180), (255,255,255),(20,90,180),  (10, 40,120),  (180,210,255),(20,90,180)),
    "esports":  ((18, 18, 32),   (10, 10, 22),   (90,20,160), (255,255,255),(130,60,220), (200,160,255), (30, 10, 60), (130,60,220)),
    "boxing":   ((255,235,235),  (245,210,210),  (180,20,20), (255,255,255),(180,20,20),  (100, 10, 10), (240,180,180),(180,20,20)),
    "baseball": ((240,248,240),  (220,238,220),  (30,80,170), (255,255,255),(30,80,170),  (20, 50,120),  (180,210,200),(30,80,170)),
    "badminton":((240,250,255),  (215,238,252),  (0,140,180), (255,255,255),(0,140,180),  (0,  70,110),  (170,220,245),(0,140,180)),
    "golf":     ((240,248,224),  (218,238,195),  (30,100,30), (255,255,255),(30,100,30),  (20, 60, 10),  (190,225,160),(30,100,30)),
    "racing":   ((240,240,240),  (220,220,225),  (180,0,0),   (255,255,255),(180,0,0),    (60, 10, 10),  (210,200,200),(180,0,0)),
    "default":  ((240,244,252),  (218,228,248),  (30,70,160), (255,255,255),(30,70,160),  (15, 35,100),  (190,205,240),(30,70,160)),
}

def _draw_sport_pattern(draw, canvas, key, W, H, CTOP, CBOT):
    """Vẽ họa tiết nền nhẹ đặc trưng của từng môn."""
    c = {
        "soccer":    (100,180,100, 28),
        "basketball":(210,120, 30, 30),
        "tennis":    (140,160, 60, 28),
        "volleyball":(60, 120,210, 28),
        "esports":   (100, 40,200, 40),
        "boxing":    (200, 60, 60, 28),
        "baseball":  (60, 100,200, 28),
        "badminton": (30, 150,200, 28),
        "golf":      (80, 160, 50, 28),
        "racing":    (160, 20, 20, 28),
        "default":   (60, 100,200, 22),
    }.get(key, (60,100,200,22))

    rgba = c[:3] + (c[3],)
    overlay = Image.new("RGBA", (W, H), (0,0,0,0))
    od = ImageDraw.Draw(overlay)

    MID_Y = (CTOP + CBOT) // 2

    if key == "soccer":
        # ── Sân bóng đá đầy đủ (nhìn từ trên) ──
        # Toàn bộ sân nằm trong vùng CTOP→CBOT, padding 18px hai bên
        PAD  = 18
        SL   = PAD        # left
        SR   = W - PAD    # right
        ST   = CTOP + 6   # top
        SB   = CBOT - 6   # bottom
        CX_s = W // 2
        CY_s = MID_Y

        # Viền sân
        od.rectangle([(SL,ST),(SR,SB)], outline=rgba, width=2)

        # Đường giữa sân (ngang)
        od.line([(SL, CY_s),(SR, CY_s)], fill=rgba, width=2)

        # Vòng tròn giữa + điểm giữa
        RC = int((SB-ST) * 0.28)
        od.ellipse([(CX_s-RC, CY_s-RC),(CX_s+RC, CY_s+RC)], outline=rgba, width=2)
        od.ellipse([(CX_s-4, CY_s-4),(CX_s+4, CY_s+4)], fill=rgba)

        # Khu vực phạt đền trái (penalty area)
        PW = int((SR-SL) * 0.14)   # chiều rộng
        PH = int((SB-ST) * 0.55)   # chiều cao
        od.rectangle([(SL, CY_s-PH//2),(SL+PW, CY_s+PH//2)], outline=rgba, width=2)
        # Khu vực cấm địa trái (goal area)
        GW = int(PW * 0.45)
        GH = int(PH * 0.4)
        od.rectangle([(SL, CY_s-GH//2),(SL+GW, CY_s+GH//2)], outline=rgba, width=2)
        # Điểm penalty trái
        PPX = SL + int(PW * 0.75)
        od.ellipse([(PPX-3, CY_s-3),(PPX+3, CY_s+3)], fill=rgba)
        # Arc penalty trái
        od.arc([(PPX-RC//2, CY_s-RC//2),(PPX+RC//2, CY_s+RC//2)],
               300, 60, fill=rgba, width=2)

        # Khu vực phạt đền phải
        od.rectangle([(SR-PW, CY_s-PH//2),(SR, CY_s+PH//2)], outline=rgba, width=2)
        od.rectangle([(SR-GW, CY_s-GH//2),(SR, CY_s+GH//2)], outline=rgba, width=2)
        PPX2 = SR - int(PW * 0.75)
        od.ellipse([(PPX2-3, CY_s-3),(PPX2+3, CY_s+3)], fill=rgba)
        od.arc([(PPX2-RC//2, CY_s-RC//2),(PPX2+RC//2, CY_s+RC//2)],
               120, 240, fill=rgba, width=2)

        # Góc sân (corner arc nhỏ)
        CR = 10
        od.arc([(SL, ST),(SL+CR*2, ST+CR*2)],       0, 90,  fill=rgba, width=2)
        od.arc([(SR-CR*2, ST),(SR, ST+CR*2)],        90, 180, fill=rgba, width=2)
        od.arc([(SL, SB-CR*2),(SL+CR*2, SB)],       270, 360, fill=rgba, width=2)
        od.arc([(SR-CR*2, SB-CR*2),(SR, SB)],        180, 270, fill=rgba, width=2)

    elif key == "basketball":
        # ── Sân bóng rổ đầy đủ (nhìn từ trên) ──
        PAD  = 18
        SL   = PAD
        SR   = W - PAD
        ST   = CTOP + 6
        SB   = CBOT - 6
        CX_b = W // 2
        CY_b = MID_Y
        SW   = SR - SL
        SH   = SB - ST

        # Viền sân
        od.rectangle([(SL,ST),(SR,SB)], outline=rgba, width=2)

        # Đường giữa sân
        od.line([(CX_b, ST),(CX_b, SB)], fill=rgba, width=2)

        # Vòng tròn giữa sân
        RC = int(SH * 0.20)
        od.ellipse([(CX_b-RC, CY_b-RC),(CX_b+RC, CY_b+RC)], outline=rgba, width=2)

        # ── Key (vùng sơn) trái ──
        KW = int(SW * 0.19)   # chiều rộng key
        KH = int(SH * 0.50)   # chiều cao key
        od.rectangle([(SL, CY_b-KH//2),(SL+KW, CY_b+KH//2)], outline=rgba, width=2)
        # Free-throw circle trái
        FTR = int(KH * 0.38)
        FTX = SL + KW
        od.arc([(FTX-FTR, CY_b-FTR),(FTX+FTR, CY_b+FTR)], 270, 90, fill=rgba, width=2)  # phần trong sân
        od.arc([(FTX-FTR, CY_b-FTR),(FTX+FTR, CY_b+FTR)], 90, 270, fill=rgba, width=2)  # phần ngoài (đứt)

        # Rổ + backboard trái
        RIM_R = 10
        RIM_X = SL + int(KW * 0.22)
        od.ellipse([(RIM_X-RIM_R, CY_b-RIM_R),(RIM_X+RIM_R, CY_b+RIM_R)],
                   outline=rgba, width=2)
        od.line([(SL, CY_b-RIM_R*2),(SL, CY_b+RIM_R*2)], fill=rgba, width=4)

        # 3-point arc trái
        ARC_R = int(SH * 0.44)
        ARC_X = SL + int(KW * 0.22)
        od.arc([(ARC_X-ARC_R, CY_b-ARC_R),(ARC_X+ARC_R, CY_b+ARC_R)],
               300, 60, fill=rgba, width=2)
        # Đường thẳng 3-point (corner 3)
        C3_Y = int(SH * 0.14)
        od.line([(SL, CY_b-C3_Y),(SL+int(KW*0.6), CY_b-C3_Y)], fill=rgba, width=2)
        od.line([(SL, CY_b+C3_Y),(SL+int(KW*0.6), CY_b+C3_Y)], fill=rgba, width=2)

        # ── Key phải (đối xứng) ──
        od.rectangle([(SR-KW, CY_b-KH//2),(SR, CY_b+KH//2)], outline=rgba, width=2)
        FTX2 = SR - KW
        od.arc([(FTX2-FTR, CY_b-FTR),(FTX2+FTR, CY_b+FTR)], 90, 270, fill=rgba, width=2)
        od.arc([(FTX2-FTR, CY_b-FTR),(FTX2+FTR, CY_b+FTR)], 270, 90, fill=rgba, width=2)

        RIM_X2 = SR - int(KW * 0.22)
        od.ellipse([(RIM_X2-RIM_R, CY_b-RIM_R),(RIM_X2+RIM_R, CY_b+RIM_R)],
                   outline=rgba, width=2)
        od.line([(SR, CY_b-RIM_R*2),(SR, CY_b+RIM_R*2)], fill=rgba, width=4)

        ARC_X2 = SR - int(KW * 0.22)
        od.arc([(ARC_X2-ARC_R, CY_b-ARC_R),(ARC_X2+ARC_R, CY_b+ARC_R)],
               120, 240, fill=rgba, width=2)
        od.line([(SR, CY_b-C3_Y),(SR-int(KW*0.6), CY_b-C3_Y)], fill=rgba, width=2)
        od.line([(SR, CY_b+C3_Y),(SR-int(KW*0.6), CY_b+C3_Y)], fill=rgba, width=2)

    elif key == "tennis":
        # Đường kẻ ngang sân tennis
        for y in [CTOP+20, MID_Y, CBOT-20]:
            od.line([(30, y),(W-30, y)], fill=rgba, width=2)
        # Đường dọc giữa
        od.line([(W//2, CTOP+20),(W//2, CBOT-20)], fill=rgba, width=2)
        # Đường biên
        od.rectangle([(30, CTOP+20),(W-30, CBOT-20)], outline=rgba, width=2)

    elif key == "volleyball":
        # Lưới giữa sân
        NET_Y1, NET_Y2 = CTOP+10, CBOT-10
        od.line([(W//2, NET_Y1),(W//2, NET_Y2)], fill=rgba, width=3)
        for y in range(NET_Y1, NET_Y2, 12):
            od.line([(W//2-4, y),(W//2+4, y)], fill=rgba, width=1)
        # Đường biên sân
        od.rectangle([(20, CTOP+10),(W-20, CBOT-10)], outline=rgba, width=1)
        od.line([(0, MID_Y),(W, MID_Y)], fill=rgba, width=1)

    elif key == "esports":
        # Họa tiết circuit board
        import random as _rnd; _rnd.seed(42)
        for _ in range(18):
            x1 = _rnd.randint(10, W-10)
            y1 = _rnd.randint(CTOP, CBOT)
            length = _rnd.choice([30,50,70,90])
            hor = _rnd.choice([True,False])
            x2, y2 = (x1+length, y1) if hor else (x1, y1+length)
            od.line([(x1,y1),(x2,y2)], fill=rgba, width=1)
            # Dot cuối
            od.ellipse([(x2-3,y2-3),(x2+3,y2+3)], fill=rgba)
        # Hex grid nhẹ
        for gx in range(0, W, 90):
            for gy in range(CTOP, CBOT, 52):
                r2 = 22
                pts = [(gx+r2*0.5, gy),(gx+r2,gy+r2*0.87),
                       (gx+r2*0.5,gy+r2*1.73),(gx-r2*0.5,gy+r2*1.73),
                       (gx-r2,gy+r2*0.87),(gx-r2*0.5,gy)]
                od.polygon(pts, outline=rgba)

    elif key == "boxing":
        # Góc ring
        PAD = 40
        od.rectangle([(PAD,CTOP+10),(W-PAD,CBOT-10)], outline=rgba, width=2)
        # Dây ring (3 đường ngang)
        for i in range(1,4):
            y = CTOP+10 + (CBOT-CTOP-20)*i//4
            od.line([(PAD, y),(W-PAD, y)], fill=rgba, width=1)
        # Điểm góc ring
        for px,py in [(PAD,CTOP+10),(W-PAD,CTOP+10),(PAD,CBOT-10),(W-PAD,CBOT-10)]:
            od.ellipse([(px-6,py-6),(px+6,py+6)], fill=rgba)

    elif key == "baseball":
        # Kim cương diamond
        cx, cy = W//2, MID_Y + 20
        sz = 70
        pts = [(cx, cy-sz),(cx+sz, cy),(cx, cy+sz),(cx-sz, cy)]
        od.polygon(pts, outline=rgba, width=2)
        # Mound
        od.ellipse([(cx-8,cy-8),(cx+8,cy+8)], fill=rgba)
        # Foul lines
        od.line([(0, CBOT),(cx-sz, cy)], fill=rgba, width=1)
        od.line([(W, CBOT),(cx+sz, cy)], fill=rgba, width=1)

    elif key == "badminton":
        # Đường kẻ sân cầu lông
        od.rectangle([(30, CTOP+15),(W-30, CBOT-15)], outline=rgba, width=2)
        od.line([(W//2, CTOP+15),(W//2, CBOT-15)], fill=rgba, width=2)
        od.line([(30, MID_Y),(W-30, MID_Y)], fill=rgba, width=1)
        # Service box
        od.line([(30, CTOP+50),(W-30, CTOP+50)], fill=rgba, width=1)
        od.line([(30, CBOT-50),(W-30, CBOT-50)], fill=rgba, width=1)

    elif key == "golf":
        # Đường cong fairway
        for i, offset in enumerate([-60, 0, 60]):
            pts = [(0+offset, CTOP), (W//4, MID_Y+offset//2), (W, CBOT+offset//3)]
            od.line(pts, fill=rgba, width=1)
        # Hole + flag
        od.ellipse([(W//2-5, MID_Y+30),(W//2+5, MID_Y+40)], fill=rgba)
        od.line([(W//2, MID_Y-20),(W//2, MID_Y+35)], fill=rgba, width=2)
        od.polygon([(W//2,MID_Y-20),(W//2+20,MID_Y-8),(W//2,MID_Y+3)], fill=rgba)

    elif key == "racing":
        # Đường đua checkerboard nhẹ
        SZ = 18
        for gx in range(0, W, SZ*2):
            for gy in range(CTOP, CBOT, SZ*2):
                od.rectangle([(gx,gy),(gx+SZ,gy+SZ)], fill=rgba)
        # Đường racing line cong
        pts = [(0, CBOT-30),(W//4, CTOP+30),(W//2, MID_Y),(3*W//4, CTOP+60),(W, CBOT-20)]
        od.line(pts, fill=(c[0],c[1],c[2],50), width=3)

    else:  # default — không có họa tiết
        pass

    canvas.paste(overlay, mask=overlay.split()[3])


def _draw_sport_icon(canvas, key, accent, CX, MID_Y, alpha=38):
    """
    Vẽ icon tượng trưng lớn, mờ, căn giữa card — làm watermark nền.
    Dùng màu accent với độ trong suốt thấp (alpha ~38).
    """
    ic = Image.new("RGBA", canvas.size, (0,0,0,0))
    d  = ImageDraw.Draw(ic)
    ac = (accent[0], accent[1], accent[2], alpha)
    ac2 = (accent[0], accent[1], accent[2], alpha+15)
    R  = 72   # bán kính icon chính
    cx, cy = CX, MID_Y

    if key == "soccer":
        # Quả bóng tròn + ngũ giác giữa
        d.ellipse([(cx-R, cy-R),(cx+R, cy+R)], outline=ac, width=4)
        # 5 cạnh của mảnh ngũ giác giữa
        import math as _m
        for i in range(5):
            a1 = _m.radians(i*72 - 90)
            a2 = _m.radians((i+1)*72 - 90)
            r2 = R * 0.42
            x1,y1 = cx + r2*_m.cos(a1), cy + r2*_m.sin(a1)
            x2,y2 = cx + r2*_m.cos(a2), cy + r2*_m.sin(a2)
            d.line([(x1,y1),(x2,y2)], fill=ac2, width=3)
            # Đường từ ngũ giác ra viền
            x3,y3 = cx + R*_m.cos((a1+a2)/2), cy + R*_m.sin((a1+a2)/2)
            d.line([(x1,y1),(x3,y3)], fill=ac, width=2)

    elif key == "basketball":
        # Quả bóng rổ: vòng tròn + 3 đường cong dọc + 1 ngang
        import math as _m
        d.ellipse([(cx-R, cy-R),(cx+R, cy+R)], outline=ac, width=4)
        # Đường ngang
        d.line([(cx-R, cy),(cx+R, cy)], fill=ac, width=3)
        # 2 đường cong dọc (arc)
        for rx in [-R//2, R//2]:
            d.arc([(cx+rx-R//2, cy-R),(cx+rx+R//2, cy+R)], 270, 90, fill=ac, width=3)

    elif key == "tennis":
        # Vợt tennis: oval + tay cầm + lưới
        import math as _m
        RW, RH = R, int(R*0.72)
        d.ellipse([(cx-RW, cy-RH),(cx+RW, cy+RH)], outline=ac, width=4)
        # Lưới ngang
        for dy in range(-RH+14, RH, 18):
            angle = _m.acos(max(-1,min(1,dy/RH))) if RH else 0
            half_w = int(RW * _m.sin(angle))
            d.line([(cx-half_w, cy+dy),(cx+half_w, cy+dy)], fill=ac, width=1)
        # Lưới dọc
        for dx in range(-RW+16, RW, 20):
            angle = _m.acos(max(-1,min(1,dx/RW))) if RW else 0
            half_h = int(RH * _m.sin(angle))
            d.line([(cx+dx, cy-half_h),(cx+dx, cy+half_h)], fill=ac, width=1)
        # Tay cầm
        d.line([(cx, cy+RH),(cx, cy+RH+30)], fill=ac2, width=6)

    elif key == "volleyball":
        # Bóng chuyền: vòng tròn + 3 đường cong bên trong
        import math as _m
        d.ellipse([(cx-R, cy-R),(cx+R, cy+R)], outline=ac, width=4)
        for angle_deg in [30, 150, 270]:
            a = _m.radians(angle_deg)
            x1 = cx + R*0.15*_m.cos(a+_m.pi)
            y1 = cy + R*0.15*_m.sin(a+_m.pi)
            x2 = cx + R*_m.cos(a)
            y2 = cy + R*_m.sin(a)
            d.arc([(min(x1,x2)-8,min(y1,y2)-8),(max(x1,x2)+8,max(y1,y2)+8)],
                  int(_m.degrees(a))+60, int(_m.degrees(a))+240, fill=ac, width=3)

    elif key == "esports":
        # Tay cầm game controller
        BW, BH = int(R*1.5), int(R*1.0)
        bx0,by0 = cx-BW, cy-BH//2
        bx1,by1 = cx+BW, cy+BH//2
        # Body
        d.rounded_rectangle([(bx0,by0),(bx1,by1)], radius=BH//3, outline=ac, width=4)
        # D-pad trái
        px, py = cx - BW//2, cy
        ps = 12
        d.line([(px-ps,py),(px+ps,py)], fill=ac2, width=4)
        d.line([(px,py-ps),(px,py+ps)], fill=ac2, width=4)
        # Nút phải (4 vòng)
        for nx,ny in [(cx+BW//2-10,cy-8),(cx+BW//2+6,cy),(cx+BW//2-10,cy+8),(cx+BW//2-24,cy)]:
            d.ellipse([(nx-5,ny-5),(nx+5,ny+5)], outline=ac2, width=2)

    elif key == "boxing":
        # Găng tay boxing đơn giản
        GW, GH = int(R*0.85), int(R*1.2)
        for side in [-1, 1]:
            ox = cx + side * int(R*0.55)
            oy = cy
            # Thân găng
            d.rounded_rectangle([(ox-GW//2, oy-GH//2),(ox+GW//2, oy+GH//2)],
                                  radius=GW//3, outline=ac, width=4)
            # Khớp ngón
            d.line([(ox-GW//2+4, oy-GH//2+GH//4),(ox+GW//2-4, oy-GH//2+GH//4)],
                   fill=ac, width=2)

    elif key == "baseball":
        # Quả bóng baseball: vòng tròn + đường may
        d.ellipse([(cx-R, cy-R),(cx+R, cy+R)], outline=ac, width=4)
        # Đường may chữ C
        import math as _m
        d.arc([(cx-R//2, cy-R+10),(cx+R//2, cy+R-10)], 300, 60, fill=ac2, width=3)
        d.arc([(cx-R//2, cy-R+10),(cx+R//2, cy+R-10)], 120, 240, fill=ac2, width=3)

    elif key == "badminton":
        # Cầu lông: phần lông (nón) + thân
        import math as _m
        TIP = (cx, cy - R)       # đỉnh cầu
        BASE_Y = cy + int(R*0.3) # đáy nón
        BASE_R = int(R*0.55)
        # Các lông tỏa ra từ đỉnh
        for i in range(8):
            a = _m.radians(i*45)
            ex = cx + BASE_R*_m.sin(a)
            ey = BASE_Y + int(BASE_R*0.3*_m.cos(a))
            d.line([(cx, TIP[1]),(int(ex),int(ey))], fill=ac, width=2)
        # Vành đáy nón
        d.ellipse([(cx-BASE_R, BASE_Y-int(BASE_R*0.3)),
                   (cx+BASE_R, BASE_Y+int(BASE_R*0.3))], outline=ac2, width=3)
        # Thân cầu
        d.line([(cx, BASE_Y),(cx, cy+R)], fill=ac2, width=5)
        # Đầu cầu
        d.ellipse([(cx-9, cy+R-9),(cx+9, cy+R+9)], fill=ac2)

    elif key == "golf":
        # Gậy golf + lỗ hố + bóng
        FLAG_X, FLAG_Y = cx + 20, cy - R + 10
        # Cán gậy
        d.line([(cx - R//2 + 10, cy + R - 10),(cx+10, cy - R//2)],
               fill=ac, width=5)
        # Đầu gậy
        d.ellipse([(cx-R//2, cy+R-20),(cx-R//2+22, cy+R)], fill=ac2)
        # Cột cờ
        d.line([(FLAG_X, FLAG_Y),(FLAG_X, cy+20)], fill=ac, width=3)
        # Cờ
        d.polygon([(FLAG_X,FLAG_Y),(FLAG_X+22,FLAG_Y+8),(FLAG_X,FLAG_Y+18)], fill=ac2)
        # Lỗ hố
        d.ellipse([(cx+R//2-12, cy+R-10),(cx+R//2+12, cy+R+6)], fill=ac)
        # Bóng
        d.ellipse([(cx-8, cy+R//2-8),(cx+8, cy+R//2+8)], fill=ac2)

    elif key == "racing":
        # Vô lăng (steering wheel)
        d.ellipse([(cx-R, cy-R),(cx+R, cy+R)], outline=ac, width=5)
        d.ellipse([(cx-R//3, cy-R//3),(cx+R//3, cy+R//3)], outline=ac, width=3)
        # 3 căm
        import math as _m
        for deg in [90, 210, 330]:
            a = _m.radians(deg)
            x1,y1 = cx+int(R//3*_m.cos(a)), cy+int(R//3*_m.sin(a))
            x2,y2 = cx+int(R*_m.cos(a)),    cy+int(R*_m.sin(a))
            d.line([(x1,y1),(x2,y2)], fill=ac, width=5)

    else:  # default — ngôi sao 5 cánh
        import math as _m
        pts = []
        for i in range(10):
            r2 = R if i%2==0 else R//2
            a = _m.radians(i*36 - 90)
            pts.append((cx + r2*_m.cos(a), cy + r2*_m.sin(a)))
        d.polygon(pts, outline=ac, width=3)

    canvas.paste(ic, mask=ic.split()[3])


def _team_palette(home: str, away: str, base_top, base_bot, accent):
    """
    Sinh bộ màu nền độc nhất cho từng cặp đội dựa trên hash tên đội.
    Giữ nguyên sắc tổng thể của môn (base_top/bot), chỉ shift hue ±15
    và thay đổi độ sáng / saturation nhẹ để mỗi trận có màu riêng.
    """
    seed = int(hashlib.md5((home + "|" + away).lower().encode()).hexdigest(), 16)

    # Shift riêng biệt 3 kênh RGB, mỗi kênh ±20
    def shift(val, delta): return max(0, min(255, val + delta))

    dr = ((seed >> 0)  & 0xFF) % 41 - 20   # -20 … +20
    dg = ((seed >> 8)  & 0xFF) % 41 - 20
    db = ((seed >> 16) & 0xFF) % 41 - 20

    top = (shift(base_top[0], dr), shift(base_top[1], dg), shift(base_top[2], db))
    bot = (shift(base_bot[0], dr), shift(base_bot[1], dg), shift(base_bot[2], db))

    # Accent: shift nhẹ hơn (±10) để vẫn nhận ra môn thể thao
    da = ((seed >> 24) & 0xFF) % 21 - 10
    acc = (shift(accent[0], da), shift(accent[1], da//2), shift(accent[2], -da//2))

    return top, bot, acc


def make_thumbnail(home_team, away_team, home_logo_url, away_logo_url,
                   time_str="", date_str="", status="upcoming", league="", sport="",
                   blv_text=""):
    """
    Tạo thumbnail WebP kiểu card trắng giống hình mẫu:
    - Nền trắng sạch, họa tiết sân mờ ở giữa
    - Top-left: badge trạng thái (LIVE đỏ / Sắp diễn ra cam)
    - Top-right: tên giải đấu pill nhỏ
    - Giữa: logo trái + hộp giờ/tỷ số + logo phải
    - Tên đội dưới logo
    - Bottom: dải màu accent + BLV pill trái
    """
    if not _PIL: return b""

    W, H = 820, 540
    # Nền trắng hoàn toàn
    canvas = Image.new("RGB", (W, H), (255, 255, 255))
    draw   = ImageDraw.Draw(canvas)

    # ── Theme theo môn (chỉ lấy màu accent + bar_col) ──
    key = _sport_key(sport, league)
    (bg_top, bg_bot, bar_col, bar_txt,
     accent, name_fg, name_sh, vs_col) = SPORT_THEMES.get(key, SPORT_THEMES["default"])

    # Màu accent độc nhất mỗi trận
    _, _, accent = _team_palette(home_team, away_team, bg_top, bg_bot, accent)
    A = accent   # shorthand

    # ─────────────────────────────────────────────
    # VÙNG NỀN TRẮNG: vẽ pattern sân mờ nhạt vào giữa
    # ─────────────────────────────────────────────
    # Layout chiều dọc:
    #   0..5          → viền accent trên
    #   5..49         → hàng badge (44px)
    #   49..97        → thanh giải đấu nền đen (48px)
    #   97..BODY_BOT  → body (logo + VS box)
    #   BODY_BOT..H   → footer BLV (54px)
    BADGE_ROW_H = 49    # badge status
    LEAGUE_BAR_H = 48   # thanh giải đấu đen
    HEADER_H = BADGE_ROW_H + LEAGUE_BAR_H   # = 97
    FOOTER_H = 54
    BODY_TOP = HEADER_H
    BODY_BOT = H - FOOTER_H
    BODY_H   = BODY_BOT - BODY_TOP
    CX = W // 2
    MID_Y = BODY_TOP + BODY_H // 2

    # Vẽ họa tiết sân mờ
    pat_ov = Image.new("RGBA", (W, H), (0,0,0,0))
    pat_d  = ImageDraw.Draw(pat_ov)
    canvas_rgba = canvas.convert("RGBA")
    _draw_sport_pattern(pat_d, pat_ov, key, W, H, BODY_TOP, BODY_BOT)
    canvas_rgba = Image.alpha_composite(canvas_rgba, pat_ov)
    _draw_sport_icon(canvas_rgba, key, A, CX, MID_Y, alpha=18)
    canvas = canvas_rgba.convert("RGB")
    draw   = ImageDraw.Draw(canvas)

    # ─────────────────────────────────────────────
    # HEADER ROW 1: viền trên + badge trạng thái
    # ─────────────────────────────────────────────
    draw.rectangle([(0,0),(W,5)], fill=A)

    if status == "live":
        badge_col = (220, 30, 30)
        badge_txt = "● Live"
    elif status == "finished":
        badge_col = (80, 80, 80)
        badge_txt = "✅ Kết thúc"
    else:
        badge_col = (210, 75, 10)
        badge_txt = "🕐 Sắp diễn ra"

    BPX, BPY = 12, 10
    bth = 30
    btw = len(badge_txt) * 10 + 18
    draw.rounded_rectangle([(BPX, BPY + 5),(BPX + btw, BPY + 5 + bth)],
                            radius=bth//2, fill=badge_col)
    draw.text((BPX + btw//2, BPY + 5 + bth//2), badge_txt,
              fill=(255,255,255), font=_font(18), anchor="mm")

    # ─────────────────────────────────────────────
    # HEADER ROW 2: thanh giải đấu nền đen + viền màu môn thể thao
    # ─────────────────────────────────────────────
    LB_TOP = BADGE_ROW_H
    LB_BOT = BADGE_ROW_H + LEAGUE_BAR_H
    draw.rectangle([(0, LB_TOP),(W, LB_BOT)], fill=(18, 18, 18))
    # Viền accent màu theo môn thể thao (trên + dưới + trái + phải 3px)
    draw.rectangle([(0, LB_TOP),(W, LB_TOP+3)], fill=A)
    draw.rectangle([(0, LB_BOT-3),(W, LB_BOT)], fill=A)
    draw.rectangle([(0, LB_TOP),(3, LB_BOT)], fill=A)
    draw.rectangle([(W-3, LB_TOP),(W, LB_BOT)], fill=A)

    if league:
        # Font 31px, hạ thêm 15px so với tâm bar
        draw.text((CX, LB_TOP + LEAGUE_BAR_H//2 + 15), league[:32],
                  fill=(255, 255, 255), font=_font(31), anchor="mm")

    # ─────────────────────────────────────────────
    # BODY: logo lớn + ô tên đội bên dưới + hộp VS/LIVE giữa
    # ─────────────────────────────────────────────
    # Chia body thành 2 phần dọc: logo zone + name zone
    NAME_BOX_H = 44          # chiều cao ô tên đội
    NAME_BOX_GAP = 10        # khoảng cách logo → ô tên
    LOGO_ZONE_H = BODY_H - NAME_BOX_H - NAME_BOX_GAP - 8   # phần dành cho logo

    # Logo tối đa gần bằng logo zone, để lại 8px padding mỗi bên
    HALF_W = W // 2 - 20     # nửa chiều rộng dành cho 1 logo (~390px)
    LMAX = min(LOGO_ZONE_H - 8, HALF_W - 10) - 10   # giảm 10px

    LX = W // 4              # tâm logo trái
    RX = 3 * W // 4          # tâm logo phải
    LY = BODY_TOP + 4 + LMAX // 2    # tâm logo = BODY_TOP + padding + bán kính

    # Y ô tên đội: ngay dưới logo
    NY_BOX_TOP = LY + LMAX // 2 + NAME_BOX_GAP
    NY_BOX_BOT = NY_BOX_TOP + NAME_BOX_H

    def draw_logo(cx, cy, url, name):
        logo = fetch_logo(url, LMAX * 3) if url else None
        if logo:
            if logo.mode != "RGBA": logo = logo.convert("RGBA")
            lw, lh = logo.size
            scale = min((LMAX - 4)/lw, (LMAX - 4)/lh, 1.0)
            nw = max(1, int(lw*scale)); nh = max(1, int(lh*scale))
            logo = logo.resize((nw, nh), Image.LANCZOS)
            ox, oy = cx - nw//2, cy - nh//2
            # Drop shadow
            sh = Image.new("RGBA", canvas.size, (0,0,0,0))
            ImageDraw.Draw(sh).ellipse(
                [(ox+6, oy+nh),(ox+nw+6, oy+nh+16)], fill=(0,0,0,28))
            canvas.paste(sh.convert("RGB"), mask=sh.split()[3])
            canvas.paste(logo.convert("RGB"), (ox, oy), logo.split()[3])
        else:
            R2 = LMAX // 2
            draw.ellipse([(cx-R2, cy-R2),(cx+R2, cy+R2)],
                         fill=(240,240,245), outline=A, width=3)
            init = "".join(w[0].upper() for w in (name or "?").split()[:2]) or "?"
            draw.text((cx, cy), init, fill=A, font=_font(52), anchor="mm")

        # ── Tên đội — không khung viền ──
        BOX_PAD = 12
        short = (name or "?")
        if len(short) > 16: short = short[:15] + "…"
        draw.text((cx, (NY_BOX_TOP + NY_BOX_BOT)//2), short,
                  fill=(20, 20, 20), font=_font(20), anchor="mm")

    draw_logo(LX, LY, home_logo_url, home_team)
    draw_logo(RX, LY, away_logo_url, away_team)

    # ─────────────────────────────────────────────
    # HỘP TRUNG TÂM: giờ/tỷ số LIVE (căn giữa theo LY)
    # ─────────────────────────────────────────────
    if status == "live":
        box_bg  = (34, 160, 60)
        box_fg  = (255, 255, 255)
        line1   = "LIVE"
        line1_f = 22
        line2   = ""
        line2_f = 18
    else:
        box_bg  = (255, 255, 255)
        box_fg  = A
        line1   = time_str if time_str else "VS"
        line1_f = 26
        line2   = date_str if date_str else ""
        line2_f = 20

    BOX_W = 148
    BOX_H = 68 if line2 else 50
    bx0 = CX - BOX_W//2; bx1 = CX + BOX_W//2
    by0 = LY - BOX_H//2; by1 = LY + BOX_H//2

    draw.rounded_rectangle([(bx0,by0),(bx1,by1)], radius=12,
                             fill=box_bg, outline=A, width=3)

    if status == "live":
        draw.ellipse([(bx0+14, LY-6),(bx0+26, LY+6)], fill=(255,60,60))
        draw.text((CX+8, LY), "LIVE", fill=(255,255,255), font=_font(24), anchor="mm")
    else:
        if line2:
            draw.text((CX, by0 + BOX_H//2 - line1_f//2 - 2), line1,
                      fill=box_fg, font=_font(line1_f), anchor="mm")
            draw.text((CX, by0 + BOX_H//2 + line2_f//2 + 2), line2,
                      fill=(110,110,110), font=_font(line2_f, bold=False), anchor="mm")
        else:
            draw.text((CX, LY), line1, fill=box_fg, font=_font(line1_f), anchor="mm")

    # ─────────────────────────────────────────────
    # FOOTER: BLV pill
    # ─────────────────────────────────────────────
    draw.line([(0, BODY_BOT),(W, BODY_BOT)], fill=(215,215,215), width=1)

    for y in range(BODY_BOT, H):
        t = (y - BODY_BOT) / FOOTER_H
        r_ = int(246 + (A[0]-246)*t*0.35)
        g_ = int(246 + (A[1]-246)*t*0.35)
        b_ = int(246 + (A[2]-246)*t*0.35)
        draw.line([(0,y),(W,y)], fill=(r_,g_,b_))

    if blv_text:
        FY = BODY_BOT + FOOTER_H // 2
        bpw = len(blv_text) * 10 + 32
        bph = 32
        draw.rounded_rectangle([(16, FY-bph//2),(16+bpw, FY+bph//2)],
                                radius=bph//2, fill=(34,160,60))
        draw.text((16 + bpw//2, FY), f"🎙 {blv_text}",
                  fill=(255,255,255), font=_font(17, bold=False), anchor="mm")

    # Viền dưới cùng
    draw.rectangle([(0,H-4),(W,H)], fill=A)

    buf = io.BytesIO()
    canvas.save(buf, format="WEBP", quality=88, method=4)
    return buf.getvalue()


def save_thumbnail(raw_bytes: bytes, ch_id: str) -> str:
    """
    Lưu thumbnail WebP vào thumbnails/{ch_id}.webp.
    Trả về URL CDN nếu đang chạy trên GitHub Actions,
    ngược lại trả về data URI base64 (local dev).
    """
    if not raw_bytes:
        return ""

    cdn = _cdn_base()

    if cdn:
        # ── GitHub Actions: lưu file, dùng raw.githubusercontent.com ──
        thumb_dir = Path(THUMB_DIR)
        thumb_dir.mkdir(exist_ok=True)
        fpath = thumb_dir / f"{ch_id}.webp"
        fpath.write_bytes(raw_bytes)
        return f"{cdn}/{ch_id}.webp"
    else:
        # ── Local dev: trả về data URI để xem ngay không cần server ──
        return "data:image/webp;base64," + base64.b64encode(raw_bytes).decode()

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
    # Label BLV → bottom-left
    if len(blv_names) > 1:
        labels.append({"text":f"🎙 {len(blv_names)} BLV","position":"bottom-left",
                       "color":"#1a8a2e","text_color":"#fff"})
    elif blv_names:
        labels.append({"text":f"🎙 {blv_names[0]}","position":"bottom-left",
                       "color":"#1a8a2e","text_color":"#fff"})

    # ── Stream theo BLV — dedup URL chỉ trong cùng group ──
    blv_groups = {}
    for s in all_streams:
        bkey = s.get("blv") or "__"
        grp  = blv_groups.setdefault(bkey, [])
        # Bỏ trùng URL trong cùng BLV group; BLV khác được giữ nguyên dù URL giống
        if s["url"] not in {x["url"] for x in grp}:
            grp.append(s)

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

    # blv_text hiển thị trong thumbnail footer
    blv_text = blv_names[0] if len(blv_names) == 1 else (f"{len(blv_names)} BLV" if blv_names else "")

    la = m.get("home_logo",""); lb = m.get("away_logo","")
    thumb_url = m.get("thumb_url","")

    if thumb_url:
        img_obj = {"padding":0,"background_color":"#ffffff","display":"cover",
                   "url":thumb_url,"width":820,"height":540}
    elif _PIL:
        raw = make_thumbnail(
            m.get("home_team",""), m.get("away_team",""),
            la, lb, m.get("time_str",""), m.get("date_str",""),
            status, league, m.get("sport",""), blv_text,
        )
        cdn_url = save_thumbnail(raw, ch_id)
        img_obj = ({"padding":0,"background_color":"#ffffff","display":"cover",
                    "url":cdn_url,"width":820,"height":540} if cdn_url else PLACEHOLDER)
    else:
        img_obj = PLACEHOLDER

    content_name = name
    if league and len(league) < 50: content_name += f" · {league.strip()}"

    # ≥2 BLV → enable_detail True; 1 BLV → False (phát thẳng)
    has_multi = len(stream_objs) > 1
    return {
        "id":            ch_id,
        "name":          name,
        "type":          "single",
        "display":       "thumbnail-only",
        "enable_detail": has_multi,
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
        "grid_number": 2,
        "image":       {"type":"cover","url":SITE_ICON},
        "groups": [{
            "id":            "tran-hot",
            "name":          "🔥 Các Trận Hot",
            "display":       "vertical",
            "grid_number":   2,
            "enable_detail": False,
            "image":         None,
            "channels":      channels,
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
    log("  🔥  CRAWLER cauthutv.shop  v8  — PRODUCTION")
    log("  📌  id='live-score-game-hot' → card-single → aria-label")
    log("  🖼   Thumbnail: CDN WebP (raw.githubusercontent.com)")
    log("  🔀  Merge trận trùng | stream dedup per-BLV")
    cdn = _cdn_base()
    if cdn:
        log(f"  📡  CDN base: {cdn}")
    else:
        log("  💻  Local mode: thumbnail → base64 data URI")
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
                # Dedup CHỈ trong cùng BLV — KHÔNG loại stream của BLV khác dù trùng URL
                blv_key = src.get("blv") or "__"
                seen_per_blv = {s["url"] for s in all_streams if (s.get("blv") or "__") == blv_key}
                all_streams.extend(s for s in streams if s["url"] not in seen_per_blv)
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

    # ── Dọn thumbnail cũ không còn dùng ──
    if _cdn_base():
        thumb_dir = Path(THUMB_DIR)
        if thumb_dir.exists():
            active_ids = {ch["id"] for ch in channels}
            removed = 0
            for f_old in thumb_dir.glob("*.webp"):
                if f_old.stem not in active_ids:
                    f_old.unlink()
                    removed += 1
            if removed:
                log(f"  🗑  Xóa {removed} thumbnail cũ")
            log(f"  🖼  {len(active_ids)} thumbnail CDN WebP trong {THUMB_DIR}/")

    log(f"\n{'═'*62}")
    log(f"  ✅ {args.output}  —  {len(channels)} trận HOT")
    log(f"  🕐 {now_str}")
    log("═"*62+"\n")

if __name__ == "__main__":
    main()
