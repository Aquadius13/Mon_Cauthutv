#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler Trực Tiếp — cauthutv.shop  v4                     ║
║   CHỈ crawl mục "Các Trận HOT"                              ║
║   Logo + thông tin: đọc từ __NEXT_DATA__ trang detail       ║
║   Thumbnail: ghép 2 logo team (rapid-api.icu / fallback)    ║
║   Fix: score vs giờ, tên trận, tên đội                      ║
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
VN_TZ = timezone(timedelta(hours=7))

SITE_ICON = f"{BASE_URL}/assets/image/favicon64.png"
PLACEHOLDER_IMG = {
    "padding": 0, "background_color": "#0f3460",
    "display": "cover", "url": SITE_ICON,
    "width": 512, "height": 512,
}

def log(*a, **kw): print(*a, **kw, flush=True)

# ═══════════════════════════════════════════════════════
#  HTTP
# ═══════════════════════════════════════════════════════

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

def fetch_html(url, scraper, retries=3):
    for i in range(retries):
        try:
            r = scraper.get(url, timeout=30, allow_redirects=True)
            r.raise_for_status()
            log(f"  ✓ [{r.status_code}] {url[:80]}")
            return r.text
        except Exception as e:
            wait = 2 ** i
            log(f"  ⚠ {i+1}/{retries}: {e} → {wait}s")
            if i < retries - 1: time.sleep(wait)
    return None

# ═══════════════════════════════════════════════════════
#  __NEXT_DATA__ parser — nguồn thông tin chính xác nhất
# ═══════════════════════════════════════════════════════

def get_next_data(html):
    """Trích xuất __NEXT_DATA__ JSON từ trang Next.js."""
    m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html, re.S)
    if not m: return {}
    try:
        return json.loads(m.group(1))
    except Exception:
        return {}

def _dig(obj, *keys, default=""):
    """Tìm value theo nhiều key alias trong dict lồng nhau."""
    if not isinstance(obj, dict): return default
    for k in keys:
        for dk in obj:
            if dk.lower() == k.lower():
                v = obj[dk]
                if v is not None and v != "": return v
    return default

def _find_all_values(obj, key_patterns, depth=0, found=None):
    """Tìm tất cả value có key khớp pattern, trả về list."""
    if found is None: found = []
    if depth > 12: return found
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = k.lower()
            if any(p in kl for p in key_patterns) and isinstance(v, str) and v:
                found.append((k, v))
            _find_all_values(v, key_patterns, depth+1, found)
    elif isinstance(obj, list):
        for item in obj[:50]:
            _find_all_values(item, key_patterns, depth+1, found)
    return found

def extract_match_info_from_nextdata(nd):
    """
    Từ __NEXT_DATA__, trích xuất:
      home_team, away_team, home_logo, away_logo,
      score, status, league, time_str, date_str
    """
    props = nd.get("props", {}).get("pageProps", {})

    # ── Tên đội ──────────────────────────────────────
    home = away = ""
    # Thử key trực tiếp trong pageProps
    for k in props:
        kl = k.lower()
        v  = props[k]
        if not isinstance(v, dict): continue
        h = _dig(v, "home_team","hometeam","team_a","home","team1","home_name","teamHome")
        a = _dig(v, "away_team","awayteam","team_b","away","team2","away_name","teamAway")
        if h and a:
            home, away = str(h), str(a)
            break

    # Tìm trong match/event object lồng sâu hơn
    if not home:
        for _, v in _find_all_values(props, ["match","event","fixture","game"], depth=0):
            if isinstance(v, dict):
                h = _dig(v,"home_team","hometeam","team_a","home")
                a = _dig(v,"away_team","awayteam","team_b","away")
                if h and a:
                    home, away = str(h), str(a); break

    # Fallback: tìm pattern "home" / "away" bất kỳ
    if not home:
        all_home = _find_all_values(props, ["home_team","hometeam","team_a","home_name"])
        all_away = _find_all_values(props, ["away_team","awayteam","team_b","away_name"])
        if all_home: home = str(all_home[0][1])
        if all_away: away = str(all_away[0][1])

    # ── Logo ─────────────────────────────────────────
    home_logo = away_logo = ""
    logo_pairs = _find_all_values(props,
        ["logo","badge","crest","emblem","image_url","flag",
         "home_logo","away_logo","logoa","logob","team_logo",
         "home_image","away_image","team_image"])
    img_urls = [(k, v) for k, v in logo_pairs
                if isinstance(v, str) and v.startswith("http")
                and (
                    re.search(r'\.(png|jpg|jpeg|svg|webp)(\?|$)', v, re.I)   # có extension
                    or re.search(r'/(image|logo|badge|crest|small|medium|large|thumb)', v, re.I)  # path logo
                    or any(d in v for d in ("rapid-api","api-sports","thesportsdb",
                                            "sofascore","flashscore","upload.wikimedia"))  # CDN logo
                )]

    # Phân loại theo key — thứ tự ưu tiên: exact key trước, rồi pattern
    HOME_KEYS = ("home_logo","logoa","logo_a","team_a_logo","home_badge",
                 "home_image","hometeam_logo","homeLogo","home")
    AWAY_KEYS = ("away_logo","logob","logo_b","team_b_logo","away_badge",
                 "away_image","awayteam_logo","awayLogo","away")
    for k, v in img_urls:
        kl = k.lower()
        if any(kl == x or kl.endswith("_"+x) or kl.startswith(x+"_") or x in kl
               for x in HOME_KEYS):
            if not home_logo: home_logo = v
        elif any(kl == x or kl.endswith("_"+x) or kl.startswith(x+"_") or x in kl
                 for x in AWAY_KEYS):
            if not away_logo: away_logo = v
    # Nếu vẫn không phân biệt được, lấy 2 URL đầu tiên theo thứ tự xuất hiện
    if not home_logo and len(img_urls) >= 1: home_logo = img_urls[0][1]
    if not away_logo and len(img_urls) >= 2: away_logo = img_urls[1][1]

    # ── Score ─────────────────────────────────────────
    score = ""
    for k, v in _find_all_values(props, ["score","result","ft_score","fulltime"]):
        if isinstance(v, str) and re.search(r'^\d{1,2}\s*[-:]\s*\d{1,2}$', v.strip()):
            score = v.strip().replace(":", "-"); break
        if isinstance(v, (int, float)):
            pass  # ignore raw numbers
    if not score:
        # score dạng {home: N, away: N}
        for k, v in _find_all_values(props, ["score","goals"]):
            if isinstance(v, dict):
                hg = _dig(v, "home","home_score","fulltime_home","ht_home")
                ag = _dig(v, "away","away_score","fulltime_away","ht_away")
                if hg != "" and ag != "":
                    try:
                        score = f"{int(hg)}-{int(ag)}"; break
                    except: pass

    # ── Status ────────────────────────────────────────
    status_raw = ""
    for _, v in _find_all_values(props, ["status","state","match_status","matchstatus",
                                          "match_state","eventstatus"]):
        if isinstance(v, str) and v: status_raw = v.lower(); break
    if not status_raw:
        status_raw = str(_dig(props, "status","state") or "").lower()

    if any(x in status_raw for x in ("live","playing","inprogress","1h","2h","ht","et","progress")):
        status = "live"
    elif any(x in status_raw for x in ("ft","finish","ended","finished","full","complete")):
        status = "finished"
    else:
        status = "upcoming"

    # ── Giải đấu ──────────────────────────────────────
    league = ""
    for _, v in _find_all_values(props, ["league","competition","tournament",
                                          "league_name","competition_name","leaguename",
                                          "leaguetitle","league_title"]):
        if isinstance(v, str) and v and len(v) < 80:
            league = v; break
    if not league:
        league = str(_dig(props, "league","competition","tournament") or "")

    # ── Thời gian ─────────────────────────────────────
    time_str = date_str = ""
    time_raw = ""
    for _, v in _find_all_values(props, ["match_time","start_time","kickoff","matchtime",
                                          "starttime","match_date","matchdate","date_time",
                                          "datetime","timestamp"]):
        if isinstance(v, str) and v and re.search(r'\d{2}:\d{2}|\d{4}-\d{2}-\d{2}', v):
            time_raw = v; break
    if not time_raw:
        time_raw = str(_dig(props, "match_time","start_time","time","kickoff","date") or "")
    if time_raw:
        time_str, date_str, _ = parse_datetime(time_raw)

    return {
        "home_team":  home,
        "away_team":  away,
        "home_logo":  home_logo,
        "away_logo":  away_logo,
        "score":      score,
        "status":     status,
        "league":     league,
        "time_str":   time_str,
        "date_str":   date_str,
    }

# ═══════════════════════════════════════════════════════
#  Parse datetime
# ═══════════════════════════════════════════════════════

def parse_datetime(raw):
    """Trả về (time_str, date_str, sort_key)."""
    if not raw: return "", "", ""
    raw = str(raw)
    # ISO: 2026-04-11T14:30:00
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})', raw)
    if m:
        _, mon, day, hh, mm = m.groups()
        return f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}"
    # Giờ + ngày: "14:30 | 11/04" hoặc "14:30 11/04"
    m = re.search(r'(\d{1,2}):(\d{2})\s*[|\s]?\s*(\d{1,2})[./](\d{1,2})', raw)
    if m:
        hh, mm, day, mon = m.group(1).zfill(2), m.group(2), m.group(3).zfill(2), m.group(4).zfill(2)
        if int(hh) <= 23 and int(mm) <= 59:
            return f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}"
    # Chỉ giờ: "14:30"
    m2 = re.search(r'(\d{1,2}):(\d{2})', raw)
    if m2:
        hh, mm = m2.group(1).zfill(2), m2.group(2)
        if int(hh) <= 23 and int(mm) <= 59:
            today = datetime.now(VN_TZ)
            return f"{hh}:{mm}", today.strftime("%d/%m"), f"{today.strftime('%m-%d')} {hh}:{mm}"
    return "", "", ""

def parse_score(raw_text):
    """
    Tìm score thực sự (d-d hoặc d:d) trong text.
    Không nhầm với giờ thi đấu (hh:mm).
    Score thường có số ≤ 20, giờ thì hh ≤ 23 và mm là 00/15/30/45/00.
    """
    # Ưu tiên pattern "d - d" (có khoảng trắng) hoặc nằm giữa tên đội
    for pat in [
        r'(?<!\d)(\d{1,2})\s*-\s*(\d{1,2})(?!\d)',   # d-d (gạch ngang)
    ]:
        for m in re.finditer(pat, raw_text):
            a, b = int(m.group(1)), int(m.group(2))
            if a <= 20 and b <= 20:
                # Kiểm tra không phải giờ (giờ thường a<=23, b%15==0)
                return f"{a}-{b}"
    return ""

# ═══════════════════════════════════════════════════════
#  Thumbnail — ghép 2 logo
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
    """Tải logo từ URL → PIL Image RGBA, hoặc None."""
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
                   time_str="", date_str="", status="upcoming",
                   score="", league=""):
    """
    Tạo ảnh JPEG 800×450 ghép 2 logo đội.
    Trả về data:image/jpeg;base64,...
    """
    if not _PIL: return ""
    W, H = 800, 450

    canvas = Image.new("RGBA", (W, H))
    draw   = ImageDraw.Draw(canvas)

    # Gradient nền
    for y in range(H):
        t = y / H
        r_ = int(15 + 25*t); g_ = int(25 + 30*t); b_ = int(50 + 40*t)
        draw.line([(0,y),(W,y)], fill=(r_,g_,b_,255))

    # Bar giải đấu
    draw.rectangle([(0,0),(W,54)], fill=(5,10,22,255))
    if league:
        draw.text((W//2, 28), league[:42], fill=(235,235,235,255),
                  font=_font(21), anchor="mm")
    draw.line([(0,54),(W,54)], fill=(60,100,200,100), width=2)

    # Kích thước logo
    LR   = 100          # bán kính vòng tròn logo
    LY   = 54 + (H-54-80)//2 + 10 + LR   # tâm Y logo
    NY   = LY + LR + 24     # Y tên đội
    LX   = 175          # tâm X trái
    RX   = W - 175      # tâm X phải
    CX   = W // 2

    def draw_logo_slot(cx, cy, url, name):
        """Vẽ 1 logo (hoặc chữ tắt nếu không có ảnh) tại (cx, cy)."""
        # Vòng tròn nền mờ
        r_out = LR + 8
        draw.ellipse([(cx-r_out, cy-r_out),(cx+r_out, cy+r_out)],
                     fill=(255,255,255,15), outline=(150,180,255,50), width=2)

        logo = fetch_logo_img(url, LR*4) if url else None

        if logo:
            lw, lh = logo.size
            scale  = min((LR*2-8)/lw, (LR*2-8)/lh, 1.0)
            nw, nh = max(1,int(lw*scale)), max(1,int(lh*scale))
            logo   = logo.resize((nw,nh), Image.LANCZOS)

            # Clip tròn
            mask  = Image.new("L",(nw,nh),0)
            mdraw = ImageDraw.Draw(mask)
            mdraw.ellipse([(0,0),(nw-1,nh-1)], fill=255)
            # Nhân alpha logo với mask tròn
            alpha    = logo.split()[3]
            combined = ImageChops.multiply(alpha, mask)
            logo.putalpha(combined)

            ox = cx - nw//2
            oy = cy - nh//2
            canvas.paste(logo, (ox,oy), logo.split()[3])
        else:
            # Fallback: vòng tròn màu + chữ tắt
            draw.ellipse([(cx-LR,cy-LR),(cx+LR,cy+LR)],
                         fill=(28,50,100,220), outline=(100,150,230,200), width=3)
            words = (name or "?").split()
            init  = "".join(w[0].upper() for w in words[:2]) or "?"
            draw.text((cx,cy), init, fill=(170,210,255,255),
                      font=_font(48), anchor="mm")

        # Tên đội
        short = (name or "?")
        if len(short) > 18: short = short[:17] + "…"
        draw.text((cx, NY), short, fill=(255,255,255,215),
                  font=_font(17), anchor="mm")

    # Vẽ 2 logo
    draw_logo_slot(LX, LY, home_logo_url, home_team)
    draw_logo_slot(RX, LY, away_logo_url, away_team)

    # Vùng giữa: score / giờ / "VS"
    if status == "live" and score:
        ctr, cc = score, (255,55,55,255)
        sub, sc2 = "● LIVE", (255,100,100,255)
    elif status == "finished" and score:
        ctr, cc = score, (255,255,255,255)
        sub, sc2 = "Kết thúc", (160,160,160,255)
    else:
        ctr, cc = time_str or "VS", (255,255,255,255)
        sub, sc2 = date_str or "", (170,170,170,255)

    # Gạch ngang
    draw.line([(CX-70,LY-10),(CX-24,LY-10)], fill=(255,255,255,60), width=2)
    draw.line([(CX+24,LY-10),(CX+70,LY-10)], fill=(255,255,255,60), width=2)
    draw.text((CX, LY-8), ctr, fill=cc, font=_font(48), anchor="mm")
    if sub:
        draw.text((CX, LY+36), sub, fill=sc2, font=_font(16,False), anchor="mm")

    # Fade bottom
    for y in range(H-50, H):
        a = int(255*(y-(H-50))/50)
        draw.line([(0,y),(W,y)], fill=(5,14,25,a))

    buf = io.BytesIO()
    canvas.convert("RGB").save(buf, format="JPEG", quality=85, optimize=True)
    return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()

# ═══════════════════════════════════════════════════════
#  Tìm section "Các Trận HOT"
# ═══════════════════════════════════════════════════════

_HOT_TEXTS = [
    "Các Trận HOT", "Các trận HOT", "Các Trận Hot",
    "các trận hot", "Trận HOT", "Trận Hot",
]
_HOT_RE = re.compile(
    r"các\s+trận\s+hot|trận\s*hot|hot\s+match|nổi\s*bật|tâm\s*điểm",
    re.I | re.UNICODE,
)
_CARD_RE = re.compile(r"\bvs\b|\blive\b|:\d{2}|trực tiếp|live", re.I)

def _card_links(container, min_n=1):
    seen, out = set(), []
    for a in container.find_all("a", href=True):
        href = a.get("href","")
        if href in seen: continue
        t = a.get_text(" ", strip=True)
        if _CARD_RE.search(t) and len(t) > 4:
            out.append(a); seen.add(href)
    return out if len(out) >= min_n else []

def _climb(tag, min_n=1):
    p = tag.parent
    for _ in range(8):
        if p is None or p.name in ("body","html"): break
        if _card_links(p, min_n): return p
        p = p.parent
    return None

def find_hot_section(bs, debug=False):
    # 1. Exact text
    for exact in _HOT_TEXTS:
        for node in bs.find_all(string=lambda t, e=exact: t and e in t):
            pt = node.parent
            log(f"  → Exact '{exact}' trong <{pt.name}>")
            sec = _climb(pt, 1)
            if sec:
                log(f"  ✅ HOT (exact): {len(_card_links(sec,1))} card — "
                    f"<{sec.name} class='{' '.join(sec.get('class',[]))[:50]}'>")
                return sec

    # 2. Regex
    for h in bs.find_all(["h1","h2","h3","h4","h5","p","span","strong"]):
        t = h.get_text(strip=True)
        if _HOT_RE.search(t) and len(t) < 60:
            log(f"  → Regex '{t[:40]}'")
            sec = _climb(h, 1)
            if sec:
                log(f"  ✅ HOT (regex): {len(_card_links(sec,1))} card")
                return sec

    # 3. id/class
    for tag in bs.find_all(["section","div","ul"]):
        tid  = tag.get("id","") or ""
        tcls = " ".join(tag.get("class",[]))
        if re.search(r"hot|featured|highlight|trending", tid+tcls, re.I):
            if _card_links(tag, 1):
                log(f"  ✅ HOT (class): <{tag.name} id='{tid}' cls='{tcls[:40]}'>")
                return tag

    # 4. Fallback: đầu tiên có card
    if debug:
        log("  ⚠ Không tìm được mục HOT. Heading trên trang:")
        for h in bs.find_all(["h1","h2","h3","h4","h5"]):
            log(f"    '{h.get_text(strip=True)[:60]}'")
        log("  Sections có card:")
        for tag in bs.find_all(["section","div"], limit=40):
            c = _card_links(tag, 1)
            if c:
                log(f"    <{tag.name} id='{tag.get('id','')}' "
                    f"class='{' '.join(tag.get('class',[]))[:40]}'> → {len(c)} card")

    for tag in bs.find_all(["section","div","ul"]):
        if _card_links(tag, 2):
            log(f"  ✅ HOT (fallback): {len(_card_links(tag))} card")
            return tag
    return None

# ═══════════════════════════════════════════════════════
#  Parse card từ trang chủ (thông tin sơ bộ)
# ═══════════════════════════════════════════════════════

def parse_card(a):
    href = a.get("href","")
    if not href: return None
    detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
    raw = a.get_text(" ", strip=True)

    # Tên đội từ class
    home = away = ""
    for tag in ["div","span","p"]:
        for cls in ["team-name","team_name","club-name","team","flex-1","flex-col",
                    "name","title"]:
            cands = a.find_all(tag, class_=re.compile(cls, re.I))
            texts = [c.get_text(" ",strip=True) for c in cands
                     if c.get_text(strip=True) and len(c.get_text(strip=True)) >= 2
                     and not re.fullmatch(r"[\d\s:|\-./]+", c.get_text(strip=True))]
            if len(texts) >= 2:
                home, away = texts[0], texts[1]; break
        if home: break

    # VS regex fallback
    if not home:
        m = re.search(
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34}?)"
            r"\s+(?:VS|vs)\s+"
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34})",
            raw, re.UNICODE)
        if m: home, away = m.group(1).strip(), m.group(2).strip()

    # Logo từ card
    imgs = []
    for img in a.find_all("img"):
        src = (img.get("src") or img.get("data-src") or "").strip()
        if not src: continue
        if not src.startswith("http"): src = urljoin(BASE_URL, src)
        _bad = ("banner","background","bg-","bg_","opengraph","og-","favicon","logo-site")
        if any(b in src.lower() for b in _bad): continue
        imgs.append(src)

    # Giờ thi đấu (KHÔNG phải score)
    mt_raw = ""
    m_time = re.search(r'(\d{1,2}:\d{2})\s*[|]?\s*(\d{1,2})[./](\d{1,2})', raw)
    if m_time: mt_raw = m_time.group(0)
    else:
        m_time2 = re.search(r'(\d{1,2}):(\d{2})', raw)
        if m_time2:
            hh, mm = int(m_time2.group(1)), int(m_time2.group(2))
            if hh <= 23 and mm <= 59: mt_raw = m_time2.group(0)
    t_str, d_str, sort_k = parse_datetime(mt_raw)

    # Score: chỉ lấy nếu có dấu -
    score = parse_score(raw)

    # Status
    if re.search(r"\blive\b|trực tiếp|đang phát|playing", raw, re.I):
        status = "live"
    elif re.search(r"kết thúc|finished|\bft\b|ended", raw, re.I):
        status = "finished"
    else:
        status = "upcoming"

    # League
    league = ""
    for d in a.find_all(["div","span","p"],
                         class_=re.compile(r"league|tournament|competition|giải", re.I)):
        t = d.get_text(strip=True)
        if t and 3 < len(t) < 60 and not re.fullmatch(r"[\d:\s|./\-]+", t):
            league = t; break

    # BLV
    blv = ""
    for sp in a.find_all("span", class_=re.compile(r"blv|commentator", re.I)):
        blv = sp.get_text(strip=True)
        if blv: break

    base_title = (f"{home} vs {away}" if home and away
                  else re.sub(r"\s{2,}", " ", raw)[:60])
    if not base_title or not detail_url: return None

    return {
        "base_title":  base_title,
        "home_team":   home,
        "away_team":   away,
        "score":       score,
        "status":      status,
        "league":      league,
        "time_str":    t_str,
        "date_str":    d_str,
        "sort_key":    sort_k,
        "detail_url":  detail_url,
        "home_logo":   imgs[0] if len(imgs) >= 1 else "",
        "away_logo":   imgs[1] if len(imgs) >= 2 else "",
        "blv":         blv,
    }

# ═══════════════════════════════════════════════════════
#  Merge trận trùng
# ═══════════════════════════════════════════════════════

def _norm(t): return re.sub(r"[^\w\s]","",t.lower().strip())

def merge_matches(raw):
    merged = {}
    for m in raw:
        key = _norm(m["base_title"])
        if key not in merged:
            merged[key] = {**m, "blv_sources": []}
        e = merged[key]
        for f in ("score","league","home_logo","away_logo"):
            if not e[f] and m[f]: e[f] = m[f]
        if not e["home_team"] and m["home_team"]: e["home_team"] = m["home_team"]
        if not e["away_team"] and m["away_team"]: e["away_team"] = m["away_team"]
        if e["status"] == "upcoming" and m["status"] in ("live","finished"):
            e["status"] = m["status"]
        existing = {s["detail_url"] for s in e["blv_sources"]}
        if m["detail_url"] not in existing:
            e["blv_sources"].append({"blv": m.get("blv",""), "detail_url": m["detail_url"]})
    result = list(merged.values())
    pri = {"live":0,"upcoming":1,"finished":2}
    result.sort(key=lambda x: (pri.get(x["status"],9), x.get("sort_key","")))
    return result

# ═══════════════════════════════════════════════════════
#  Crawl trang detail: lấy stream + thông tin đầy đủ
# ═══════════════════════════════════════════════════════

def extract_streams(detail_url, html, bs):
    seen, raw = set(), []
    def add(name, url, kind):
        url = url.strip()
        if url and url not in seen and len(url) > 12:
            seen.add(url)
            raw.append({"name":name,"url":url,"type":kind,"referer":detail_url})

    for fr in bs.find_all("iframe", src=True):
        if re.search(r"live|stream|embed|player|sport|watch|truc.?tiep", fr["src"], re.I):
            add("embed", fr["src"], "iframe")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("HLS", m.group(1), "hls")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.mpd(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("DASH", m.group(1), "dash")
    for sc in bs.find_all("script"):
        c = sc.string or ""
        for m in re.finditer(
                r'"(?:file|src|source|stream|url|hls|playlist|videoUrl|streamUrl)"\s*:\s*"(https?://[^"]+)"', c):
            u = m.group(1)
            if re.search(r"m3u8|stream|live|video|play", u, re.I): add("config", u, "hls")
        for m in re.finditer(r'(?:streamUrl|videoUrl|hlsUrl)\s*=\s*["\']([^"\']+)["\']', c):
            u = m.group(1)
            if u.startswith("http"): add("js", u, "hls")

    if not raw:
        raw.append({"name":"Trực tiếp","url":detail_url,"type":"iframe","referer":detail_url})
        return raw

    hls = [s for s in raw if s["type"]=="hls"]
    if not hls: return raw

    # Nhóm base, lấy nhóm lớn nhất
    _QUAL_RE = re.compile(r"[_-](?:fhd|1080p?|720p?|480p?|360p?|hd|sd)$", re.I)
    _QUAL_MAP = {"hd":"HD","sd":"SD","fhd":"Full HD","1080":"Full HD","1080p":"Full HD",
                 "720":"HD","720p":"HD","480":"SD","480p":"SD","360":"360p"}
    _QUAL_ORD = {"Auto":0,"Full HD":1,"HD":2,"SD":3,"360p":4}
    def base(u): return _QUAL_RE.sub("",re.sub(r"\.\w+$","",u.rstrip("/").split("/")[-1])).lower()
    def qlabel(u):
        fname = re.sub(r"\.\w+$","",u.rstrip("/").split("/")[-1]).lower()
        m = _QUAL_RE.search(fname)
        return _QUAL_MAP.get(m.group(0).lstrip("-_").lower(), m.group(0).upper()) if m else "Auto"

    from collections import Counter
    cnt = Counter(base(s["url"]) for s in hls)
    top = cnt.most_common(1)[0][0]
    grp = [{**s,"name":qlabel(s["url"])} for s in hls if base(s["url"])==top]
    grp.sort(key=lambda x: _QUAL_ORD.get(x["name"],99))
    return grp

def crawl_detail(detail_url, blv, scraper):
    """
    Crawl trang chi tiết:
      - Đọc __NEXT_DATA__ → thông tin đầy đủ (tên đội, logo, score, giờ)
      - Extract streams
    """
    html = fetch_html(detail_url, scraper, retries=2)
    if not html: return [], {}

    bs  = BeautifulSoup(html, "lxml")
    nd  = get_next_data(html)
    info = extract_match_info_from_nextdata(nd) if nd else {}

    streams = extract_streams(detail_url, html, bs)
    for s in streams: s["blv"] = blv
    return streams, info

# ═══════════════════════════════════════════════════════
#  Build channel object
# ═══════════════════════════════════════════════════════

def make_id(*parts):
    return hashlib.md5("-".join(str(p) for p in parts).encode()).hexdigest()[:16]

def build_display_name(m):
    home, away = m["home_team"], m["away_team"]
    base  = f"{home} vs {away}" if home and away else m["base_title"]
    score = m.get("score","")
    t, d  = m.get("time_str",""), m.get("date_str","")
    st    = m["status"]

    if st == "live":
        return (f"{home} {score} {away}  🔴" if score
                else f"{base}  🔴 LIVE")
    if st == "finished":
        return (f"{home} {score} {away}  ✅" if score
                else f"{base}  ✅ KT")
    if t and d: return f"{base}  🕐 {t} | {d}"
    if t:       return f"{base}  🕐 {t}"
    if d:       return f"{base}  📅 {d}"
    return base

def build_channel(m, all_streams, index):
    ch_id = make_id("ctt", index, re.sub(r"[^a-z0-9]","-",m["base_title"].lower())[:24])
    name  = build_display_name(m)
    league = m.get("league","")
    score  = m.get("score","")
    status = m["status"]

    # Labels
    sc_map = {
        "live":     {"text":"● Live",          "color":"#E73131","text_color":"#fff"},
        "upcoming": {"text":"🕐 Sắp diễn ra", "color":"#d54f1a","text_color":"#fff"},
        "finished": {"text":"✅ Kết thúc",     "color":"#444444","text_color":"#fff"},
    }
    labels = [{**sc_map.get(status, sc_map["live"]), "position":"top-left"}]

    blv_names = [s["blv"] for s in m.get("blv_sources",[]) if s["blv"]]
    if len(blv_names) > 1:
        labels.append({"text":f"🎙 {len(blv_names)} BLV","position":"top-right",
                       "color":"#00601f","text_color":"#fff"})
    elif blv_names:
        labels.append({"text":f"🎙 {blv_names[0]}","position":"top-right",
                       "color":"#00601f","text_color":"#fff"})

    if score:
        col = "#E73131" if status=="live" else "#444444"
        pfx = "⚽" if status=="live" else "KT"
        labels.append({"text":f"{pfx} {score}","position":"bottom-right",
                       "color":col,"text_color":"#fff"})

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
            ref = s.get("referer", m["blv_sources"][0]["detail_url"] if m["blv_sources"] else BASE_URL+"/")
            slinks.append({
                "id":      make_id(ch_id,f"b{idx}",f"l{li}"),
                "name":    s.get("name","Auto"),
                "type":    s["type"],
                "default": li==0,
                "url":     s["url"],
                "request_headers":[
                    {"key":"Referer","value":ref},
                    {"key":"User-Agent","value":CHROME_UA},
                ],
            })
        stream_objs.append({"id":make_id(ch_id,f"st{idx}"),
                             "name":slabel,"stream_links":slinks})

    if not stream_objs:
        fb = m["blv_sources"][0]["detail_url"] if m["blv_sources"] else BASE_URL+"/"
        stream_objs.append({"id":"fallback","name":"Trực tiếp","stream_links":[{
            "id":"lnk0","name":"Link 1","type":"iframe","default":True,"url":fb,
            "request_headers":[{"key":"Referer","value":fb},
                               {"key":"User-Agent","value":CHROME_UA}],
        }]})

    # ── Thumbnail ─────────────────────────────────────
    la = m.get("home_logo","")
    lb = m.get("away_logo","")

    if _PIL and (la or lb or m.get("home_team")):
        # Luôn tạo thumbnail ghép 2 logo
        uri = make_thumbnail(
            home_team     = m.get("home_team",""),
            away_team     = m.get("away_team",""),
            home_logo_url = la,
            away_logo_url = lb,
            time_str      = m.get("time_str",""),
            date_str      = m.get("date_str",""),
            status        = status,
            score         = score,
            league        = league,
        )
        img_obj = ({"padding":0,"background_color":"#0f3460","display":"cover",
                    "url":uri,"width":800,"height":450}
                   if uri else PLACEHOLDER_IMG)
    else:
        img_obj = PLACEHOLDER_IMG

    content_name = name
    if league and len(league) < 50:
        content_name += f" · {league.strip()}"

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

# ═══════════════════════════════════════════════════════
#  Build JSON
# ═══════════════════════════════════════════════════════

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
            "name":     "🔥 Các Trận HOT",
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
    log("  🔥  CRAWLER cauthutv.shop  v4  — Các Trận HOT")
    log("  🖼  Logo từ __NEXT_DATA__ detail page + Pillow ghép 2 logo")
    log("═"*62 + "\n")

    now_vn  = datetime.now(VN_TZ)
    now_str = now_vn.strftime("%d/%m/%Y %H:%M") + " ICT (UTC+7)"
    scraper = make_scraper()

    log(f"📥 Tải trang chủ {BASE_URL}...")
    html = fetch_html(BASE_URL, scraper)
    if not html: log("❌ Không tải được."); sys.exit(1)
    if "Just a moment" in html or "cf-browser-verification" in html:
        log("⚠ Cloudflare challenge."); sys.exit(1)

    if args.debug:
        with open(DEBUG_HTML,"w",encoding="utf-8") as f: f.write(html)
        log(f"  💾 {DEBUG_HTML}")

    bs = BeautifulSoup(html, "lxml")

    log("\n🔍 Tìm mục 'Các Trận HOT'...")
    section = find_hot_section(bs, debug=args.debug)
    if not section:
        log("❌ Không tìm thấy. Chạy --debug để xem cấu trúc HTML.")
        sys.exit(1)

    # Parse cards
    raw, seen_urls = [], set()
    for a in section.find_all("a", href=True):
        if not _CARD_RE.search(a.get_text(" ",strip=True)): continue
        m = parse_card(a)
        if m and m["detail_url"] not in seen_urls:
            seen_urls.add(m["detail_url"])
            raw.append(m)

    matches = merge_matches(raw)
    log(f"\n  ✅ {len(raw)} card → {len(matches)} trận HOT\n")
    if not matches: log("❌ Không có trận."); sys.exit(1)

    # Crawl detail + build channels
    log("🖼  Crawl detail + tạo thumbnail...")
    channels = []
    for i, m in enumerate(matches, 1):
        all_streams = []

        if not args.no_stream:
            for src in m.get("blv_sources",[]):
                streams, info = crawl_detail(src["detail_url"], src["blv"], scraper)

                # Cập nhật thông tin từ detail page (ưu tiên hơn card)
                if info:
                    if info.get("home_team") and not m["home_team"]:
                        m["home_team"] = info["home_team"]
                    if info.get("away_team") and not m["away_team"]:
                        m["away_team"] = info["away_team"]
                    if not m["home_team"] and info.get("home_team"):
                        m["home_team"] = info["home_team"]
                    if not m["away_team"] and info.get("away_team"):
                        m["away_team"] = info["away_team"]
                    # Logo: ưu tiên từ detail page
                    if info.get("home_logo"):
                        m["home_logo"] = info["home_logo"]
                    if info.get("away_logo"):
                        m["away_logo"] = info["away_logo"]
                    # Score từ detail (chính xác hơn)
                    if info.get("score") and not m["score"]:
                        m["score"] = info["score"]
                    if info.get("status") and m["status"] == "upcoming":
                        m["status"] = info["status"]
                    if info.get("league") and not m["league"]:
                        m["league"] = info["league"]
                    if info.get("time_str") and not m["time_str"]:
                        m["time_str"] = info["time_str"]
                        m["date_str"] = info.get("date_str","")

                seen_u = {s["url"] for s in all_streams}
                all_streams.extend(s for s in streams if s["url"] not in seen_u)
            time.sleep(0.4)

        # Cập nhật base_title nếu giờ đã có tên đội từ detail
        if m["home_team"] and m["away_team"]:
            m["base_title"] = f"{m['home_team']} vs {m['away_team']}"

        log(f"  [{i:03d}] {m['base_title'][:42]:42s} | "
            f"st={m['status']:8s} | "
            f"logo={'✓' if m['home_logo'] else '✗'}{'✓' if m['away_logo'] else '✗'} | "
            f"streams={len(all_streams)}")

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
