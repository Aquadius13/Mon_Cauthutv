#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler cauthutv.shop  v5                                  ║
║   Chiến lược MỚI: Gọi API JSON nội bộ của trang             ║
║   1. Thử các API endpoint trực tiếp (/api/matches, ...)      ║
║   2. Parse __NEXT_DATA__ từ HTML (Next.js SSR)               ║
║   3. Đọc inline JSON trong <script> tags                     ║
║   4. Fallback: cloudscraper + BeautifulSoup                  ║
║   Lọc: chỉ lấy trận trong mục "Các Trận Hot"                ║
║   Thumbnail: 2 logo không vòng tròn, nền đẹp                ║
╚══════════════════════════════════════════════════════════════╝
pip install cloudscraper beautifulsoup4 lxml requests pillow
"""

import argparse, base64, hashlib, io, json, re, sys, time
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse

try:
    import cloudscraper
    from bs4 import BeautifulSoup
    import requests
except ImportError:
    print("pip install cloudscraper beautifulsoup4 lxml requests pillow")
    sys.exit(1)

try:
    from PIL import Image, ImageDraw, ImageFont, ImageChops
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

PLACEHOLDER_IMG = {
    "padding": 0, "background_color": "#0f3460",
    "display": "cover", "url": SITE_ICON,
    "width": 512, "height": 512,
}

def log(*a, **kw): print(*a, **kw, flush=True)

# ═══════════════════════════════════════════════════════
#  HTTP helpers
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

def fetch(url, scraper, retries=3, as_json=False):
    headers = {
        "User-Agent":  CHROME_UA,
        "Referer":     BASE_URL + "/",
        "Accept":      "application/json, */*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9",
    }
    for i in range(retries):
        try:
            r = scraper.get(url, timeout=30, allow_redirects=True, headers=headers)
            r.raise_for_status()
            log(f"  ✓ [{r.status_code}] {url[:80]}")
            if as_json:
                return r.json()
            return r.text
        except Exception as e:
            wait = 2**i
            log(f"  ⚠ {i+1}/{retries}: {e} → {wait}s")
            if i < retries-1: time.sleep(wait)
    return None

# ═══════════════════════════════════════════════════════
#  API endpoint discovery — cách tiếp cận chính
# ═══════════════════════════════════════════════════════

# Các API endpoint phổ biến của trang bóng đá Next.js Việt Nam
API_CANDIDATES = [
    "/api/matches/hot",
    "/api/hot-matches",
    "/api/matches?type=hot",
    "/api/matches?category=hot",
    "/api/home",
    "/api/tran-hot",
    "/api/matches",
    "/api/live",
    "/api/football/matches",
    "/api/schedule",
]

def try_api_endpoints(scraper):
    """Thử gọi các API endpoint JSON để lấy danh sách trận HOT."""
    for path in API_CANDIDATES:
        url = BASE_URL + path
        try:
            r = scraper.get(url, timeout=10, headers={
                "User-Agent": CHROME_UA,
                "Accept": "application/json",
                "Referer": BASE_URL + "/",
            })
            if r.status_code == 200:
                ct = r.headers.get("content-type","")
                if "json" in ct or r.text.strip().startswith(("{","[")):
                    try:
                        data = r.json()
                        log(f"  ✅ API found: {url}")
                        return data, url
                    except Exception:
                        pass
        except Exception:
            pass
    return None, None

# ═══════════════════════════════════════════════════════
#  __NEXT_DATA__ và inline JSON — cách thứ 2
# ═══════════════════════════════════════════════════════

def extract_nextdata(html):
    """Lấy __NEXT_DATA__ từ HTML Next.js."""
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html, re.S
    )
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    return {}

def find_hot_matches_in_nextdata(nd):
    """
    Tìm danh sách trận HOT trong __NEXT_DATA__.
    Tìm key chứa 'hot', 'featured', 'highlight' hoặc array lớn nhất.
    """
    props = nd.get("props",{}).get("pageProps",{})
    if not props:
        return []

    # Ưu tiên key rõ ràng
    HOT_KEYS = ["hotMatches","hot_matches","featuredMatches","featured_matches",
                "highlightMatches","topMatches","matches_hot","matchesHot",
                "tranHot","tran_hot","hotGames","hot"]

    for k in HOT_KEYS:
        if k in props and isinstance(props[k], list) and props[k]:
            log(f"  → __NEXT_DATA__['{k}']: {len(props[k])} trận")
            return props[k]

    # Tìm key nào chứa 'hot' trong tên
    for k, v in props.items():
        if "hot" in k.lower() and isinstance(v, list) and v:
            log(f"  → __NEXT_DATA__ key '{k}': {len(v)} trận")
            return v

    # Tìm array có element trông như match (có home/away)
    best_key, best_list = "", []
    for k, v in props.items():
        if isinstance(v, list) and len(v) > len(best_list):
            # Kiểm tra item đầu có vẻ là match
            if v and isinstance(v[0], dict):
                keys_lower = {kk.lower() for kk in v[0].keys()}
                if any(x in keys_lower for x in
                       ("home","home_team","hometeam","team_a","slug","id")):
                    best_key, best_list = k, v

    if best_list:
        log(f"  → __NEXT_DATA__ best array key='{best_key}': {len(best_list)} items")
        return best_list

    return []

def extract_inline_json_matches(html):
    """
    Tìm JSON được nhúng trong <script> tags dạng:
    window.__DATA__ = {...}
    window.matchData = [...]
    var hotMatches = [...]
    """
    matches = []
    # Pattern tìm array JSON trong script
    patterns = [
        r'(?:hotMatches|hot_matches|tranHot|featuredMatches)\s*[=:]\s*(\[.*?\])\s*[;,]',
        r'window\.__(?:MATCHES|DATA|STATE)__\s*=\s*(\{.*?\})\s*;',
        r'"(?:hotMatches|hot_matches|tranHot)"\s*:\s*(\[.*?\])',
    ]
    for pat in patterns:
        for m in re.finditer(pat, html, re.S):
            try:
                data = json.loads(m.group(1))
                if isinstance(data, list) and data:
                    log(f"  → inline JSON: {len(data)} items")
                    return data
                if isinstance(data, dict):
                    for k,v in data.items():
                        if isinstance(v, list) and v:
                            matches.extend(v)
            except Exception:
                pass
    return matches

# ═══════════════════════════════════════════════════════
#  Parse match object từ JSON API
# ═══════════════════════════════════════════════════════

def _s(obj, *keys):
    """Tìm value theo nhiều key alias (case-insensitive)."""
    if not isinstance(obj, dict): return ""
    for k in keys:
        for dk in obj:
            if dk.lower() == k.lower():
                v = obj[dk]
                if v is not None and str(v).strip(): return str(v).strip()
    return ""

def _find_deep(obj, *keys, depth=0):
    """Tìm value trong dict lồng nhau."""
    if depth > 8: return ""
    if isinstance(obj, dict):
        v = _s(obj, *keys)
        if v: return v
        for val in obj.values():
            r = _find_deep(val, *keys, depth=depth+1)
            if r: return r
    elif isinstance(obj, list):
        for item in obj[:20]:
            r = _find_deep(item, *keys, depth=depth+1)
            if r: return r
    return ""

def _find_img_urls(obj, depth=0):
    """Tìm tất cả URL ảnh trong object."""
    urls = []
    if depth > 8: return urls
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and v.startswith("http"):
                if (re.search(r'\.(png|jpg|jpeg|svg|webp)(\?|$)', v, re.I)
                        or "image" in v.lower() or "logo" in v.lower()
                        or "badge" in v.lower() or "rapid-api" in v.lower()):
                    urls.append((k, v))
            else:
                urls.extend(_find_img_urls(v, depth+1))
    elif isinstance(obj, list):
        for item in obj[:10]:
            urls.extend(_find_img_urls(item, depth+1))
    return urls

def parse_match_from_json(obj):
    """Parse 1 match object từ JSON API → dict chuẩn."""
    if not isinstance(obj, dict): return None

    # Slug/URL
    slug = _s(obj, "slug","url","link","path","href","match_url","detail_url")
    if slug and not slug.startswith("http"):
        slug = urljoin(BASE_URL, "/" + slug.lstrip("/"))
    if not slug:
        # Tạo từ tên đội
        h = _s(obj,"home_team","home","team_a","hometeam","teamHome","home_name")
        a = _s(obj,"away_team","away","team_b","awayteam","teamAway","away_name")
        if h and a:
            slug = f"{BASE_URL}/{re.sub(r'[^a-z0-9]','-',h.lower())}-vs-{re.sub(r'[^a-z0-9]','-',a.lower())}"

    # Tên đội
    home = _s(obj,"home_team","home","team_a","hometeam","teamHome",
              "home_name","local","club_home","home_club")
    away = _s(obj,"away_team","away","team_b","awayteam","teamAway",
              "away_name","visitor","club_away","away_club")

    if not home or not away: return None
    if not slug: return None

    # Logo
    home_logo = away_logo = ""
    img_urls = _find_img_urls(obj)
    HOME_LOGO_KEYS = {"home_logo","home_image","logo_a","logoa","home_badge",
                      "home_team_logo","hometeam_logo","home_flag"}
    AWAY_LOGO_KEYS = {"away_logo","away_image","logo_b","logob","away_badge",
                      "away_team_logo","awayteam_logo","away_flag"}
    for k, v in img_urls:
        kl = k.lower()
        if kl in HOME_LOGO_KEYS or any(x in kl for x in ("home","team_a","logo_a")):
            if not home_logo: home_logo = v
        elif kl in AWAY_LOGO_KEYS or any(x in kl for x in ("away","team_b","logo_b")):
            if not away_logo: away_logo = v
    if not home_logo and len(img_urls) >= 1: home_logo = img_urls[0][1]
    if not away_logo and len(img_urls) >= 2: away_logo = img_urls[1][1]

    # Thời gian
    time_raw = _s(obj,"match_time","start_time","time","kickoff","date",
                  "matchTime","startTime","match_date","datetime")
    t_str, d_str, sort_k = parse_datetime(time_raw)

    # Status
    status_raw = _s(obj,"status","state","match_status","matchStatus").lower()
    if any(x in status_raw for x in ("live","playing","inprogress","1h","2h","ht")):
        status = "live"
    elif any(x in status_raw for x in ("ft","finish","ended","finished","full")):
        status = "finished"
    else:
        status = "upcoming"

    # League
    league = (_s(obj,"league","competition","tournament","league_name",
                 "competition_name","leagueName","league_title")
              or _find_deep(obj,"league_name","competition_name","leagueName"))

    # BLV
    blv = _s(obj,"blv","commentator","reporter","broadcaster")

    base_title = f"{home} vs {away}"
    return {
        "base_title":  base_title,
        "home_team":   home,
        "away_team":   away,
        "status":      status,
        "league":      league,
        "time_str":    t_str,
        "date_str":    d_str,
        "sort_key":    sort_k,
        "detail_url":  slug,
        "home_logo":   home_logo,
        "away_logo":   away_logo,
        "blv":         blv,
        "blv_sources": [{"blv": blv, "detail_url": slug}],
    }

# ═══════════════════════════════════════════════════════
#  Fallback: HTML scraping (BeautifulSoup)
# ═══════════════════════════════════════════════════════

_CARD_RE = re.compile(r"\bvs\b|\blive\b|:\d{2}|trực tiếp", re.I)

def scrape_html_fallback(html, bs, debug=False):
    """
    Fallback cuối cùng: parse HTML thông thường.
    Tìm tất cả <a> có href slug trận đấu và chứa text VS/LIVE.
    """
    log("  ⚡ Dùng HTML scraping fallback...")
    matches = []
    seen = set()

    # Lấy tất cả link có dạng slug trận đấu
    for a in bs.find_all("a", href=True):
        href = a.get("href","")
        if not href: continue
        url = href if href.startswith("http") else urljoin(BASE_URL, href)
        path = urlparse(url).path.rstrip("/")

        # Chỉ lấy URL có dạng slug trận (có -vs- hoặc đủ dài)
        if len(path) < 10: continue
        if url in seen: continue

        _skip = ("/api/","/tin-tuc","/news","/login","/tag/","/category/",
                 "/page/","javascript:","#",".jpg",".png",".css",".js")
        if any(s in url.lower() for s in _skip): continue

        raw = a.get_text(" ", strip=True)
        if not _CARD_RE.search(raw): continue

        # Parse card
        m = parse_html_card(a, url)
        if m:
            seen.add(url)
            matches.append(m)

    log(f"  → HTML fallback: {len(matches)} trận")
    return matches

def parse_html_card(a, detail_url):
    raw = a.get_text(" ", strip=True)
    if len(raw) < 4: return None

    home = away = ""
    # Class hints
    for tag in ["div","span","p"]:
        for cls in ["team-name","team_name","team","club","name","flex-1"]:
            cands = a.find_all(tag, class_=re.compile(cls, re.I))
            texts = [c.get_text(" ",strip=True) for c in cands
                     if c.get_text(strip=True) and len(c.get_text(strip=True)) >= 2
                     and not re.fullmatch(r"[\d\s:|\-./]+", c.get_text(strip=True))]
            if len(texts) >= 2:
                home, away = texts[0], texts[1]; break
        if home: break

    # VS regex fallback
    if not home:
        vm = re.search(
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34}?)"
            r"\s+(?:VS|vs)\s+"
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34})",
            raw, re.UNICODE)
        if vm: home, away = vm.group(1).strip(), vm.group(2).strip()

    if not home or not away: return None

    # Logos từ img tags
    imgs = []
    for img in a.find_all("img"):
        src = (img.get("src") or img.get("data-src") or "").strip()
        if not src: continue
        if not src.startswith("http"): src = urljoin(BASE_URL, src)
        _bad = ("banner","background","opengraph","favicon")
        if any(b in src.lower() for b in _bad): continue
        imgs.append(src)

    # Giờ thi đấu
    mt = ""
    m1 = re.search(r'(\d{1,2}:\d{2})\s*[|]?\s*(\d{1,2})[./](\d{1,2})', raw)
    if m1: mt = m1.group(0)
    else:
        m2 = re.search(r'(\d{1,2}):(\d{2})', raw)
        if m2 and int(m2.group(1)) <= 23 and int(m2.group(2)) <= 59:
            mt = m2.group(0)
    t_str, d_str, sort_k = parse_datetime(mt)

    status = ("live" if re.search(r"\blive\b|trực tiếp|đang phát", raw, re.I)
              else "finished" if re.search(r"kết thúc|finished|\bft\b", raw, re.I)
              else "upcoming")

    league = ""
    for d in a.find_all(["div","span"],
                         class_=re.compile(r"league|tournament|giải", re.I)):
        t = d.get_text(strip=True)
        if t and 3 < len(t) < 60: league = t; break

    return {
        "base_title":  f"{home} vs {away}",
        "home_team":   home,
        "away_team":   away,
        "status":      status,
        "league":      league,
        "time_str":    t_str,
        "date_str":    d_str,
        "sort_key":    sort_k,
        "detail_url":  detail_url,
        "home_logo":   imgs[0] if len(imgs) >= 1 else "",
        "away_logo":   imgs[1] if len(imgs) >= 2 else "",
        "blv":         "",
        "blv_sources": [{"blv": "", "detail_url": detail_url}],
    }

# ═══════════════════════════════════════════════════════
#  Parse datetime + score
# ═══════════════════════════════════════════════════════

def parse_datetime(raw):
    if not raw: return "", "", ""
    raw = str(raw)
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})', raw)
    if m:
        _, mon, day, hh, mm = m.groups()
        return f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}"
    m = re.search(r'(\d{1,2}):(\d{2})\s*[|\s]?\s*(\d{1,2})[./](\d{1,2})', raw)
    if m:
        hh, mm, day, mon = m.group(1).zfill(2), m.group(2), m.group(3).zfill(2), m.group(4).zfill(2)
        if int(hh) <= 23 and int(mm) <= 59:
            return f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}"
    m2 = re.search(r'(\d{1,2}):(\d{2})', raw)
    if m2:
        hh, mm = m2.group(1).zfill(2), m2.group(2)
        if int(hh) <= 23 and int(mm) <= 59:
            today = datetime.now(VN_TZ)
            return f"{hh}:{mm}", today.strftime("%d/%m"), f"{today.strftime('%m-%d')} {hh}:{mm}"
    return "", "", ""

# ═══════════════════════════════════════════════════════
#  Thumbnail
# ═══════════════════════════════════════════════════════

def _font(size, bold=True):
    if not _PIL: return None
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold
            else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold
            else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for p in paths:
        try: return ImageFont.truetype(p, size)
        except: pass
    return ImageFont.load_default()

def fetch_logo_img(url, max_px=200):
    if not url or not _PIL: return None
    try:
        r = requests.get(url.strip(), timeout=8,
                         headers={"User-Agent":"Mozilla/5.0"}, stream=True)
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
    W, H = 800, 450
    canvas = Image.new("RGB", (W, H))
    draw   = ImageDraw.Draw(canvas)

    # Gradient nền xanh đậm
    for y in range(H):
        t = y/H
        draw.line([(0,y),(W,y)], fill=(int(8+12*t), int(16+20*t), int(40+35*t)))

    # Accent top
    draw.rectangle([(0,0),(W,4)], fill=(255,140,0))

    # Bar giải đấu
    draw.rectangle([(0,4),(W,52)], fill=(0,0,0))
    if league:
        draw.text((W//2, 28), league[:44], fill=(255,200,50),
                  font=_font(20), anchor="mm")
    draw.line([(0,52),(W,52)], fill=(255,140,0,80), width=1)

    CONTENT_TOP = 60
    CONTENT_BOT = H - 62
    NAME_H      = 28
    LOGO_AREA_H = CONTENT_BOT - CONTENT_TOP - NAME_H
    LOGO_MAX    = min(LOGO_AREA_H - 4, 155)
    CX = W//2; LX = 145; RX = W-145
    LY = CONTENT_TOP + LOGO_AREA_H//2 + 4
    NY = CONTENT_BOT - 4

    def draw_logo(cx, cy, url, name):
        logo = fetch_logo_img(url, LOGO_MAX*3) if url else None
        if logo:
            if logo.mode != "RGBA": logo = logo.convert("RGBA")
            lw, lh = logo.size
            scale = min((LOGO_MAX-4)/lw, (LOGO_MAX-4)/lh, 1.0)
            nw, nh = max(1,int(lw*scale)), max(1,int(lh*scale))
            logo = logo.resize((nw,nh), Image.LANCZOS)
            ox, oy = cx-nw//2, cy-nh//2
            canvas.paste(logo.convert("RGB"), (ox,oy), logo.split()[3])
        else:
            sz = LOGO_MAX*3//4
            draw.rectangle([(cx-sz//2,cy-sz//2),(cx+sz//2,cy+sz//2)],
                           fill=(20,40,80), outline=(80,120,200), width=2)
            words = (name or "?").split()
            init  = "".join(w[0].upper() for w in words[:2]) or "?"
            draw.text((cx,cy), init, fill=(160,200,255), font=_font(44), anchor="mm")

        short = (name or "?")
        if len(short) > 20: short = short[:19]+"…"
        draw.text((cx+1,NY+1), short, fill=(0,0,0,180), font=_font(17), anchor="mm")
        draw.text((cx,NY), short, fill=(240,240,240), font=_font(17), anchor="mm")

    draw_logo(LX, LY, home_logo_url, home_team)
    draw_logo(RX, LY, away_logo_url, away_team)

    # Giữa: giờ / VS / LIVE
    if status == "live":
        line1, c1 = "● LIVE", (255,70,70)
        line2, c2 = "",       (255,255,255)
        f1 = 38
    else:
        line1, c1 = time_str or "VS", (255,255,255)
        line2, c2 = date_str or "",   (160,160,160)
        f1 = 46

    draw.line([(LX+LOGO_MAX//2+8,LY),(CX-36,LY)], fill=(255,255,255,40), width=1)
    draw.line([(CX+36,LY),(RX-LOGO_MAX//2-8,LY)], fill=(255,255,255,40), width=1)
    draw.text((CX+1,LY+1), line1, fill=(0,0,0,160), font=_font(f1), anchor="mm")
    draw.text((CX, LY),    line1, fill=c1,           font=_font(f1), anchor="mm")
    if line2:
        draw.text((CX,LY+36), line2, fill=c2, font=_font(16,False), anchor="mm")

    # Footer
    draw.rectangle([(0,H-50),(W,H)], fill=(0,0,0))
    draw.line([(0,H-50),(W,H-50)], fill=(255,140,0,100), width=1)

    buf = io.BytesIO()
    canvas.save(buf, format="JPEG", quality=87, optimize=True)
    return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()

# ═══════════════════════════════════════════════════════
#  Crawl detail page để lấy stream + logo chính xác
# ═══════════════════════════════════════════════════════

def crawl_detail(detail_url, blv, scraper):
    html = fetch(detail_url, scraper, retries=2)
    if not html: return [], {}

    bs  = BeautifulSoup(html, "lxml")
    nd  = extract_nextdata(html)
    info = {}

    if nd:
        props = nd.get("props",{}).get("pageProps",{})
        if props:
            home  = _find_deep(props,"home_team","hometeam","team_a","home_name","home")
            away  = _find_deep(props,"away_team","awayteam","team_b","away_name","away")
            league = _find_deep(props,"league","competition","tournament","league_name")
            time_raw = _find_deep(props,"match_time","start_time","kickoff","matchTime")
            t_str, d_str, _ = parse_datetime(time_raw)

            imgs = _find_img_urls(props)
            HOME_K = {"home_logo","home_image","logo_a","logoa","home_badge"}
            AWAY_K = {"away_logo","away_image","logo_b","logob","away_badge"}
            home_logo = away_logo = ""
            for k,v in imgs:
                kl = k.lower()
                if kl in HOME_K or any(x in kl for x in ("home","team_a","logo_a")):
                    if not home_logo: home_logo = v
                elif kl in AWAY_K or any(x in kl for x in ("away","team_b","logo_b")):
                    if not away_logo: away_logo = v
            if not home_logo and len(imgs)>=1: home_logo = imgs[0][1]
            if not away_logo and len(imgs)>=2: away_logo = imgs[1][1]

            info = {
                "home_team": home, "away_team": away,
                "home_logo": home_logo, "away_logo": away_logo,
                "league": league, "time_str": t_str, "date_str": d_str,
            }

    # Streams
    seen, streams = set(), []
    def add(name, url, kind):
        url = url.strip()
        if url and url not in seen and len(url) > 12:
            seen.add(url)
            streams.append({"name":name,"url":url,"type":kind,
                            "referer":detail_url,"blv":blv})

    for fr in bs.find_all("iframe", src=True):
        if re.search(r"live|stream|embed|player|sport|watch", fr["src"], re.I):
            add("embed", fr["src"], "iframe")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("HLS", m.group(1), "hls")
    for sc in bs.find_all("script"):
        c = sc.string or ""
        for m in re.finditer(
            r'"(?:file|src|source|stream|url|hls)"\s*:\s*"(https?://[^"]+)"', c):
            u = m.group(1)
            if re.search(r"m3u8|stream|live|video", u, re.I): add("config", u, "hls")

    if not streams:
        streams.append({"name":"Trực tiếp","url":detail_url,"type":"iframe",
                        "referer":detail_url,"blv":blv})
    return streams, info

# ═══════════════════════════════════════════════════════
#  Merge + dedup
# ═══════════════════════════════════════════════════════

def _norm(t): return re.sub(r"[^\w\s]","",t.lower().strip())

def merge_matches(raw):
    merged = {}
    for m in raw:
        key = _norm(m["base_title"])
        if key not in merged:
            merged[key] = {**m, "blv_sources": []}
        e = merged[key]
        for f in ("league","home_logo","away_logo","time_str","date_str"):
            if not e.get(f) and m.get(f): e[f] = m[f]
        if not e.get("home_team") and m.get("home_team"): e["home_team"] = m["home_team"]
        if not e.get("away_team") and m.get("away_team"): e["away_team"] = m["away_team"]
        if e.get("status","upcoming") == "upcoming" and m.get("status") in ("live","finished"):
            e["status"] = m["status"]
        existing = {s["detail_url"] for s in e["blv_sources"]}
        if m["detail_url"] not in existing:
            e["blv_sources"].append({"blv":m.get("blv",""),"detail_url":m["detail_url"]})
    result = list(merged.values())
    pri = {"live":0,"upcoming":1,"finished":2}
    result.sort(key=lambda x: (pri.get(x.get("status","upcoming"),9), x.get("sort_key","")))
    return result

# ═══════════════════════════════════════════════════════
#  Build channel + JSON output
# ═══════════════════════════════════════════════════════

def make_id(*parts):
    return hashlib.md5("-".join(str(p) for p in parts).encode()).hexdigest()[:16]

def build_display_name(m):
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
    name   = build_display_name(m)
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

    # Streams
    blv_groups = {}
    for s in all_streams:
        blv_groups.setdefault(s.get("blv") or "__no_blv__", []).append(s)

    stream_objs = []
    for idx, (bkey, raw_s) in enumerate(blv_groups.items()):
        if not raw_s: continue
        slabel = f"🎙 {bkey}" if bkey != "__no_blv__" else f"Nguồn {idx+1}"
        slinks = []
        for li, s in enumerate(raw_s):
            ref = s.get("referer", BASE_URL+"/")
            slinks.append({
                "id": make_id(ch_id,f"b{idx}",f"l{li}"),
                "name": s.get("name","Auto"), "type": s["type"],
                "default": li==0, "url": s["url"],
                "request_headers":[
                    {"key":"Referer","value":ref},
                    {"key":"User-Agent","value":CHROME_UA},
                ],
            })
        stream_objs.append({"id":make_id(ch_id,f"st{idx}"),
                             "name":slabel,"stream_links":slinks})

    if not stream_objs:
        fb = m.get("blv_sources",[{}])[0].get("detail_url", BASE_URL+"/") if m.get("blv_sources") else BASE_URL+"/"
        stream_objs.append({"id":"fallback","name":"Trực tiếp","stream_links":[{
            "id":"lnk0","name":"Link 1","type":"iframe","default":True,"url":fb,
            "request_headers":[{"key":"Referer","value":fb},
                               {"key":"User-Agent","value":CHROME_UA}],
        }]})

    la = m.get("home_logo",""); lb = m.get("away_logo","")
    if _PIL and (la or lb or m.get("home_team")):
        uri = make_thumbnail(
            m.get("home_team",""), m.get("away_team",""),
            la, lb,
            m.get("time_str",""), m.get("date_str",""),
            status, league,
        )
        img_obj = ({"padding":0,"background_color":"#0f3460","display":"cover",
                    "url":uri,"width":800,"height":450}
                   if uri else PLACEHOLDER_IMG)
    else:
        img_obj = PLACEHOLDER_IMG

    content_name = name
    if league and len(league) < 50: content_name += f" · {league.strip()}"

    has_multi = len(stream_objs) > 1
    return {
        "id":            ch_id,
        "name":          name,
        "type":          "multi" if has_multi else "single",
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
        "description": f"Cập nhật lúc {now_str}",
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
    ap.add_argument("--no-stream", action="store_true")
    ap.add_argument("--debug",     action="store_true")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    args = ap.parse_args()

    log("\n" + "═"*62)
    log("  🔥  CRAWLER cauthutv.shop  v5")
    log("  📡  API JSON → __NEXT_DATA__ → HTML scraping")
    log("═"*62 + "\n")

    now_vn  = datetime.now(VN_TZ)
    now_str = now_vn.strftime("%d/%m/%Y %H:%M") + " ICT (UTC+7)"
    scraper = make_scraper()

    raw_matches = []

    # ── Bước 1: Thử API endpoints ─────────────────────
    log("📡 Bước 1: Thử API endpoints...")
    api_data, api_url = try_api_endpoints(scraper)
    if api_data:
        items = api_data if isinstance(api_data, list) else []
        if not items and isinstance(api_data, dict):
            for k,v in api_data.items():
                if isinstance(v, list) and v:
                    items = v; break
        for obj in items:
            m = parse_match_from_json(obj)
            if m: raw_matches.append(m)
        log(f"  → API: {len(raw_matches)} trận")

    # ── Bước 2: Tải HTML và parse __NEXT_DATA__ ───────
    log("\n📥 Bước 2: Tải trang chủ + __NEXT_DATA__...")
    html = fetch(BASE_URL, scraper)
    if not html:
        log("❌ Không tải được trang chủ."); sys.exit(1)
    if "Just a moment" in html or "cf-browser-verification" in html:
        log("⚠ Cloudflare challenge."); sys.exit(1)

    if args.debug:
        with open(DEBUG_HTML,"w",encoding="utf-8") as f: f.write(html)
        log(f"  💾 {DEBUG_HTML}")

    nd = extract_nextdata(html)
    if nd:
        log("  → Tìm thấy __NEXT_DATA__")
        hot_items = find_hot_matches_in_nextdata(nd)
        nd_matches = 0
        for obj in hot_items:
            m = parse_match_from_json(obj)
            if m:
                raw_matches.append(m)
                nd_matches += 1
        log(f"  → __NEXT_DATA__: {nd_matches} trận")
    else:
        log("  ⚠ Không có __NEXT_DATA__ (CSR/client-side render)")

    # ── Bước 3: Inline JSON trong script tags ─────────
    if not raw_matches:
        log("\n🔍 Bước 3: Tìm inline JSON...")
        inline = extract_inline_json_matches(html)
        for obj in inline:
            m = parse_match_from_json(obj)
            if m: raw_matches.append(m)
        log(f"  → Inline JSON: {len(raw_matches)} trận")

    # ── Bước 4: HTML scraping fallback ────────────────
    if not raw_matches:
        log("\n🔍 Bước 4: HTML scraping fallback...")
        bs = BeautifulSoup(html, "lxml")
        raw_matches = scrape_html_fallback(html, bs, args.debug)

    if not raw_matches:
        log("❌ Không tìm được trận nào.")
        log("  💡 Chạy --debug để lưu HTML kiểm tra.")
        sys.exit(1)

    matches = merge_matches(raw_matches)
    log(f"\n  ✅ {len(raw_matches)} raw → {len(matches)} trận\n")

    # ── Crawl stream + detail ─────────────────────────
    log("🖼  Crawl detail + tạo thumbnail...")
    channels = []
    for i, m in enumerate(matches, 1):
        all_streams = []

        if not args.no_stream:
            for src in m.get("blv_sources",[]):
                streams, info = crawl_detail(src["detail_url"], src.get("blv",""), scraper)
                if info:
                    if info.get("home_team") and not m.get("home_team"): m["home_team"] = info["home_team"]
                    if info.get("away_team") and not m.get("away_team"): m["away_team"] = info["away_team"]
                    if info.get("home_logo"): m["home_logo"] = info["home_logo"]
                    if info.get("away_logo"): m["away_logo"] = info["away_logo"]
                    if info.get("league") and not m.get("league"): m["league"] = info["league"]
                    if info.get("time_str") and not m.get("time_str"):
                        m["time_str"] = info["time_str"]; m["date_str"] = info.get("date_str","")
                seen_u = {s["url"] for s in all_streams}
                all_streams.extend(s for s in streams if s["url"] not in seen_u)
            time.sleep(0.3)

        if m.get("home_team") and m.get("away_team"):
            m["base_title"] = f"{m['home_team']} vs {m['away_team']}"

        log(f"  [{i:03d}] {m.get('base_title','?')[:40]:40s} | "
            f"{'🔴' if m.get('status')=='live' else '🕐'} | "
            f"logo={'✓' if m.get('home_logo') else '✗'}{'✓' if m.get('away_logo') else '✗'} | "
            f"streams={len(all_streams)}")

        channels.append(build_channel(m, all_streams, i))

    result = build_json(channels, now_str)
    with open(args.output,"w",encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log(f"\n{'═'*62}")
    log(f"  ✅ {args.output}  —  {len(channels)} trận")
    log(f"  🕐 {now_str}")
    log("═"*62+"\n")

if __name__ == "__main__":
    main()
