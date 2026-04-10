#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler Trực Tiếp — cauthutv.shop  v1                     ║
║   Dựa theo crawler_quechoa9_v10, adapt cho cauthutv.shop     ║
║   + Phát hiện card đa chiến lược (Tailwind / Bootstrap /     ║
║     HTML thông thường)                                       ║
║   + Thumbnail tạo bằng Pillow (logo 2 đội)                   ║
║   + Crawl stream: m3u8 / DASH / iframe                       ║
║   + Phân loại môn thể thao tự động                          ║
║   + Debug mode: lưu HTML để phân tích cấu trúc              ║
╚══════════════════════════════════════════════════════════════╝
Cài đặt:
    pip install cloudscraper beautifulsoup4 lxml requests pillow

Chạy:
    python crawler_cauthutv.py                  # mặc định
    python crawler_cauthutv.py --all            # tất cả trận
    python crawler_cauthutv.py --no-stream      # không crawl stream
    python crawler_cauthutv.py --debug          # lưu HTML để kiểm tra
    python crawler_cauthutv.py --output out.json
"""

import argparse, base64, hashlib, io, json, os, re, sys, time, unicodedata
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse

try:
    import cloudscraper
    from bs4 import BeautifulSoup, NavigableString, Tag
    import requests
except ImportError:
    print("Cài đặt: pip install cloudscraper beautifulsoup4 lxml requests")
    sys.exit(1)

# ── Constants ─────────────────────────────────────────────────
BASE_URL    = "https://cauthutv.shop"
OUTPUT_FILE = "cauthutv_iptv.json"
DEBUG_HTML  = "debug_cauthutv.html"
CHROME_UA   = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
VN_TZ = timezone(timedelta(hours=7))

PLACEHOLDER_IMG = {
    "padding": 0, "background_color": "#000000", "display": "cover",
    "url": f"{BASE_URL}/favicon.ico", "width": 512, "height": 512,
}

# ── Logging ───────────────────────────────────────────────────
def log(*args, **kwargs):
    print(*args, **kwargs, flush=True)

# ── Pillow (thumbnail) ────────────────────────────────────────
try:
    from PIL import Image, ImageDraw, ImageFont
    _PILLOW_OK = True
except ImportError:
    _PILLOW_OK = False

def _font(size, bold=True):
    if not _PILLOW_OK:
        return None
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold
            else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold
            else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    for p in paths:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass
    return ImageFont.load_default()

_LOGO_GOOD_DOMAINS = [
    "media.api-sports.io", "thesportsdb.com", "r2.dev",
    "sofascore.com", "flashscore.com", "upload.wikimedia.org/wikipedia",
    "logos-world.net", "worldvectorlogo.com", "footballdatabase.eu",
]
_LOGO_BLOCKED = ["wikipedia.org/api/", "/static/images/", "opengraph", "og-image"]

def _is_logo_url(url: str) -> bool:
    if not url: return False
    ul = url.lower()
    for b in _LOGO_BLOCKED:
        if b in ul: return False
    for d in _LOGO_GOOD_DOMAINS:
        if d in ul: return True
    if re.search(r'\.(png|svg|webp)(\?|$)', ul):
        if any(k in ul for k in ['team', 'club', 'badge', 'logo', 'crest', 'emblem']):
            return True
    return False

def _fetch_logo(url: str, size: int) -> "Image.Image | None":
    if not url or not _PILLOW_OK or not _is_logo_url(url): return None
    try:
        resp = requests.get(url, timeout=7, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        ct = resp.headers.get("content-type", "")
        if "html" in ct or "json" in ct: return None
        logo = Image.open(io.BytesIO(resp.content)).convert("RGBA")
        if logo.width > 2000 or logo.height > 2000: return None
        logo.thumbnail((size, size), Image.LANCZOS)
        return logo
    except Exception:
        return None

def find_team_logo(name: str, provided_url: str = "", size: int = 200) -> "Image.Image | None":
    if provided_url and _is_logo_url(provided_url):
        logo = _fetch_logo(provided_url, size)
        if logo: return logo
    if not name: return None
    name_q = requests.utils.quote(name.strip())
    try:
        r = requests.get(
            f"https://www.thesportsdb.com/api/v1/json/3/searchteams.php?t={name_q}",
            timeout=7, headers={"User-Agent": "Mozilla/5.0"})
        teams = r.json().get("teams") or []
        if teams:
            for key in ("strTeamBadge", "strTeamLogo"):
                url = teams[0].get(key, "")
                if url:
                    logo = _fetch_logo(url + "/preview", size) or _fetch_logo(url, size)
                    if logo: return logo
    except Exception:
        pass
    return None

def make_match_thumbnail_b64(
    home_team: str, away_team: str,
    logo_a_url: str = "", logo_b_url: str = "",
    time_str: str = "", date_str: str = "",
    status: str = "upcoming", score: str = "",
    league: str = "",
) -> str:
    """Tạo thumbnail JPEG 800×450 → data:image/jpeg;base64,..."""
    if not _PILLOW_OK:
        return ""
    W, H = 800, 450
    img  = Image.new("RGB", (W, H), (30, 42, 65))
    draw = ImageDraw.Draw(img)

    for y in range(H):
        t = y / H
        draw.line([(0, y), (W, y)], fill=(
            int(25 + 20*t), int(38 + 24*t), int(60 + 30*t),
        ))

    # League bar
    draw.rectangle([(0, 0), (W, 56)], fill=(8, 14, 28))
    if league:
        draw.text((W//2, 30), league[:35], fill=(245, 245, 245),
                  font=_font(22), anchor="mm")
    draw.line([(0, 56), (W, 56)], fill=(80, 110, 180, 80), width=2)

    CONTENT_MID = (64 + 420) // 2
    LOGO_SIZE   = 130
    LOGO_Y      = CONTENT_MID - 30
    NAME_Y      = LOGO_Y + LOGO_SIZE // 2 + 26
    LX = 155; RX = W - 155

    def _paste_logo(cx, cy, logo_img, name):
        if logo_img:
            lw, lh  = logo_img.size
            scale   = min(LOGO_SIZE / lw, LOGO_SIZE / lh, 1.0)
            nw, nh  = max(1, int(lw * scale)), max(1, int(lh * scale))
            resized = logo_img.resize((nw, nh), Image.LANCZOS)
            ox, oy  = cx - nw // 2, cy - nh // 2
            if resized.mode == "RGBA":
                bg     = Image.new("RGBA", (nw, nh), (255, 255, 255, 30))
                merged = Image.alpha_composite(bg, resized)
                img.paste(merged.convert("RGB"), (ox, oy), merged.split()[3])
            else:
                img.paste(resized.convert("RGB"), (ox, oy))
        else:
            init = "".join(w[0].upper() for w in (name or "?").split()[:2]) or "?"
            draw.text((cx, cy), init, fill=(190, 210, 255),
                      font=_font(48), anchor="mm")

    logo_a = find_team_logo(home_team, logo_a_url, LOGO_SIZE * 3)
    logo_b = find_team_logo(away_team, logo_b_url, LOGO_SIZE * 3)
    _paste_logo(LX, LOGO_Y, logo_a, home_team)
    _paste_logo(RX, LOGO_Y, logo_b, away_team)

    draw.text((LX, NAME_Y), home_team[:16], fill=(255, 255, 255, 230),
              font=_font(20), anchor="mm")
    draw.text((RX, NAME_Y), away_team[:16], fill=(255, 255, 255, 230),
              font=_font(20), anchor="mm")

    cx, cy = W // 2, LOGO_Y
    if status == "live" and score and score not in ("", "VS"):
        ctr, ctr_col = score, (255, 70, 70, 255)
        sub, sub_col = "● LIVE", (255, 120, 120, 255)
    elif status == "finished" and score and score not in ("", "VS"):
        ctr, ctr_col = score, (255, 255, 255, 255)
        sub, sub_col = "Kết thúc", (170, 170, 170, 255)
    else:
        ctr, ctr_col = time_str or "VS", (255, 255, 255, 255)
        sub, sub_col = date_str or "", (180, 180, 180, 255)

    draw.line([(cx-72, cy-10), (cx-26, cy-10)], fill=(255, 255, 255, 80), width=2)
    draw.line([(cx+26, cy-10), (cx+72, cy-10)], fill=(255, 255, 255, 80), width=2)
    draw.text((cx, cy-8), ctr, fill=ctr_col, font=_font(52), anchor="mm")
    if sub:
        draw.text((cx, cy+40), sub, fill=sub_col, font=_font(18, False), anchor="mm")

    for y in range(H - 60, H):
        alpha = int(255 * (y - (H - 60)) / 60)
        draw.line([(0, y), (W, y)], fill=(8, 20, 32, alpha))

    out = io.BytesIO()
    img.convert("RGB").save(out, format="JPEG", quality=82, optimize=True)
    b64 = base64.b64encode(out.getvalue()).decode()
    return f"data:image/jpeg;base64,{b64}"

# ── Utils ─────────────────────────────────────────────────────
def make_id(*parts: str) -> str:
    raw = "-".join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:16]

def normalize_name(name: str) -> str:
    if not name: return ""
    n = unicodedata.normalize("NFD", name.lower())
    n = "".join(c for c in n if not unicodedata.combining(c))
    n = re.sub(r"[^a-z0-9\s]", " ", n)
    return re.sub(r"\s+", " ", n).strip()

def tokenize(name: str) -> list:
    stopwords = {"fc", "cf", "sc", "ac", "rc", "us", "cd", "sk", "bk", "if",
                 "the", "of", "de", "di", "del", "la", "le", "los", "las",
                 "a", "b", "c", "1", "2", "ii", "iii"}
    return [t for t in normalize_name(name).split() if t and t not in stopwords]

def team_match_score(a: str, b: str) -> float:
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb: return 0.0
    if na == nb: return 1.0
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    if len(shorter) >= 4 and shorter in longer: return 0.85
    ta, tb = set(tokenize(a)), set(tokenize(b))
    if ta and tb:
        inter  = len(ta & tb)
        union  = len(ta | tb)
        jaccard = inter / union
        if jaccard >= 0.5: return jaccard
        st = ta if len(ta) <= len(tb) else tb
        lt = ta if len(ta) > len(tb) else tb
        if st and st.issubset(lt): return 0.7
    return 0.0

# ── HTTP ──────────────────────────────────────────────────────
def make_scraper():
    sc = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    sc.headers.update({
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
        "Referer":         BASE_URL + "/",
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return sc

def fetch_html(url: str, scraper, retries: int = 3) -> str | None:
    for i in range(retries):
        try:
            r = scraper.get(url, timeout=30, allow_redirects=True)
            r.raise_for_status()
            log(f"  ✓ [{r.status_code}] {url[:80]}")
            return r.text
        except Exception as e:
            wait = 2 ** i
            log(f"  ⚠ Lần {i+1}/{retries}: {e} → chờ {wait}s")
            if i < retries - 1:
                time.sleep(wait)
    return None

# ── Parse ngày giờ ────────────────────────────────────────────
def parse_match_datetime(match_time: str):
    if not match_time:
        return ("", "", "")
    m = re.search(r"(\d{1,2}):(\d{2})\s*[|\s]?\s*(\d{1,2})[./](\d{1,2})", match_time)
    if m:
        hh, mm    = m.group(1).zfill(2), m.group(2)
        day, mon  = m.group(3).zfill(2), m.group(4).zfill(2)
        if int(hh) <= 23 and int(mm) <= 59 and 1 <= int(day) <= 31 and 1 <= int(mon) <= 12:
            return (f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}")
    m2 = re.search(r"(\d{1,2}):(\d{2})", match_time)
    if m2:
        hh, mm = m2.group(1).zfill(2), m2.group(2)
        if int(hh) <= 23 and int(mm) <= 59:
            today = datetime.now(VN_TZ)
            return (f"{hh}:{mm}", today.strftime("%d/%m"), f"{today.strftime('%m-%d')} {hh}:{mm}")
    return ("", "", "")

# ── Card detection — đa chiến lược ───────────────────────────
def _has_class(tag, *classes) -> bool:
    cls = " ".join(tag.get("class", []))
    return all(c in cls for c in classes)

# === Chiến lược 1: quechoa/Tailwind style ===
_TAILWIND_CARD_CLASSES = [
    ("hover:border-[#83ff65]", "rounded-xl"),
    ("rounded-xl", "block"),
    ("border", "rounded"),
]

def _find_tailwind_cards(bs) -> list:
    for cls1, cls2 in _TAILWIND_CARD_CLASSES:
        cards = [t for t in bs.find_all("a") if _has_class(t, cls1, cls2)]
        # Lọc: chỉ lấy card có chứa text VS hoặc tên đội
        valid = [c for c in cards
                 if re.search(r"\bvs\b|:\d{2}|\bLive\b|trực tiếp", c.get_text(), re.I)]
        if valid:
            log(f"  → Tailwind strategy: {len(valid)} cards (cls='{cls1}+{cls2}')")
            return valid
    return []

# === Chiến lược 2: Bootstrap / generic card style ===
_GENERIC_CARD_SELECTORS = [
    ("div", ["match-card", "card-match", "fixture", "game-card", "event-card"]),
    ("article", ["match", "fixture", "event"]),
    ("li", ["match", "fixture"]),
    ("div", ["item-match", "match-item", "sport-card"]),
]

def _find_generic_cards(bs) -> list:
    for tag_name, cls_list in _GENERIC_CARD_SELECTORS:
        for cls in cls_list:
            cards = bs.find_all(tag_name, class_=re.compile(cls, re.I))
            if cards:
                # Lấy href từ thẻ a bên trong nếu chính nó không phải <a>
                result = []
                for card in cards:
                    a = card if card.name == "a" else card.find("a", href=True)
                    if a and re.search(r"\bvs\b|\bLive\b|:\d{2}", card.get_text(), re.I):
                        result.append(a if card.name != "a" else card)
                if result:
                    log(f"  → Generic strategy: {len(result)} cards (tag={tag_name}, cls={cls})")
                    return result
    return []

# === Chiến lược 3: Tìm theo pattern VS trong anchor ===
def _find_vs_cards(bs) -> list:
    """Tìm thẻ <a> chứa 'VS' giữa 2 tên đội."""
    result = []
    seen   = set()
    vs_re  = re.compile(
        r"[\w\u00C0-\u024F\u1E00-\u1EFF .'-]{2,35}"
        r"\s+(?:VS|vs)\s+"
        r"[\w\u00C0-\u024F\u1E00-\u1EFF .'-]{2,35}",
        re.UNICODE | re.I
    )
    for a in bs.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if href in seen: continue
        if vs_re.search(text) and len(text) > 8:
            result.append(a)
            seen.add(href)
    if result:
        log(f"  → VS-pattern strategy: {len(result)} cards")
    return result

# === Chiến lược 4: __NEXT_DATA__ JSON từ Next.js ===
def _find_nextdata_matches(html: str) -> list[dict]:
    """
    Nhiều site Next.js nhúng toàn bộ data vào <script id='__NEXT_DATA__'>.
    Cố parse để lấy danh sách trận.
    """
    m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
                  html, re.S)
    if not m: return []
    try:
        nd     = json.loads(m.group(1))
        props  = nd.get("props", {}).get("pageProps", {})
        # Tìm đệ quy danh sách có vẻ là trận đấu
        matches = []
        _dig_matches(props, matches, depth=0)
        if matches:
            log(f"  → __NEXT_DATA__ strategy: {len(matches)} matches")
        return matches
    except Exception:
        return []

def _dig_matches(obj, out: list, depth: int):
    """Đệ quy tìm object có dạng {'home_team':..., 'away_team':...} hoặc tương tự."""
    if depth > 8: return
    if isinstance(obj, dict):
        keys = {k.lower() for k in obj}
        home_keys = {"home_team","hometeam","team_a","team1","home","local"}
        away_keys = {"away_team","awayteam","team_b","team2","away","visitor"}
        if home_keys & keys and away_keys & keys:
            out.append(obj)
            return
        for v in obj.values():
            _dig_matches(v, out, depth + 1)
    elif isinstance(obj, list):
        for item in obj[:100]:
            _dig_matches(item, out, depth + 1)

def find_match_cards(bs, html: str = "", only_featured: bool = True) -> list:
    """
    Thử các chiến lược theo thứ tự ưu tiên.
    Trả về list các <a> tag hoặc list dict (từ __NEXT_DATA__).
    """
    # 1. Tailwind (quechoa clone)
    cards = _find_tailwind_cards(bs)
    if cards: return cards

    # 2. Generic class
    cards = _find_generic_cards(bs)
    if cards: return cards

    # 3. VS pattern
    cards = _find_vs_cards(bs)
    if cards: return cards

    log("  ⚠ Không tìm thấy card nào qua chiến lược HTML")
    return []

# ── Parse card → match dict ───────────────────────────────────
def parse_card(card) -> dict | None:
    """Parse 1 card <a> thành dict trận đấu."""
    href       = card.get("href", "")
    if not href: return None
    detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
    raw_text   = card.get_text(" ", strip=True)

    # Trạng thái
    if re.search(r"\bLive\b|trực tiếp|đang phát", raw_text, re.I):
        status = "live"
    elif re.search(r"Kết thúc|Finished|\bFT\b|đã kết", raw_text, re.I):
        status = "finished"
    else:
        status = "upcoming"

    # Giờ thi đấu
    match_time_raw = ""
    _mt = re.search(r"(\d{1,2}:\d{2})\s*[|]\s*(\d{1,2}[./]\d{1,2})", raw_text)
    if _mt:
        match_time_raw = _mt.group(0)
    else:
        _mt2 = re.search(r"(\d{1,2}:\d{2})", raw_text)
        if _mt2:
            hh, mm = int(_mt2.group(0).split(":")[0]), int(_mt2.group(0).split(":")[1])
            if hh <= 23 and mm <= 59:
                match_time_raw = _mt2.group(0)

    time_str, date_str, sort_key = parse_match_datetime(match_time_raw)

    # Tên đội từ thẻ có class phù hợp
    home_team = away_team = ""
    for tag in ["div", "span", "p"]:
        for cls_hint in ["team-name", "team_name", "club-name", "team", "flex-1", "flex-col"]:
            candidates = card.find_all(tag, class_=re.compile(cls_hint, re.I))
            texts = [c.get_text(" ", strip=True) for c in candidates
                     if c.get_text(strip=True) and len(c.get_text(strip=True)) >= 2
                     and not re.fullmatch(r"[\d\s:]+", c.get_text(strip=True))]
            if len(texts) >= 2:
                home_team, away_team = texts[0], texts[1]
                break
        if home_team: break

    # Fallback: regex VS trong raw_text
    if not home_team:
        vm = re.search(
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34}?)"
            r"\s+(?:VS|vs)\s+"
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34})",
            raw_text, re.UNICODE)
        if vm:
            home_team, away_team = vm.group(1).strip(), vm.group(2).strip()

    # Tên giải
    league = ""
    for d in card.find_all(["div", "span", "p"],
                            class_=re.compile(r"league|tournament|competition|giải", re.I)):
        t = d.get_text(strip=True)
        if t and 3 < len(t) < 60 and not re.fullmatch(r"[\d:\s|./]+", t):
            league = t; break
    if not league:
        # Fallback: đoán từ raw_text (phần trước tên đội)
        if home_team:
            idx = raw_text.lower().find(home_team.lower())
            if idx > 5:
                candidate = raw_text[:idx].strip()
                candidate = re.sub(r"\d{1,2}:\d{2}.*", "", candidate).strip()
                if 3 < len(candidate) < 55:
                    league = candidate

    # Tỉ số
    score = ""
    score_m = re.search(r"\b(\d{1,2})\s*[-:]\s*(\d{1,2})\b", raw_text)
    if score_m:
        score = f"{score_m.group(1)}-{score_m.group(2)}"

    # BLV
    blv = ""
    for span in card.find_all("span", class_=re.compile(r"blv|commentator|reporter", re.I)):
        blv = span.get_text(strip=True)
        if blv: break

    # Thumbnail
    thumbnail = ""
    img = card.find("img")
    if img:
        src = img.get("src") or img.get("data-src") or ""
        thumbnail = src if src.startswith("http") else urljoin(BASE_URL, src)

    base_title = (f"{home_team} vs {away_team}"
                  if home_team and away_team
                  else home_team or re.sub(r"\s{2,}", " ", raw_text)[:60])
    if not base_title or not detail_url:
        return None

    return {
        "base_title":  base_title,
        "home_team":   home_team,
        "away_team":   away_team,
        "score":       score,
        "status":      status,
        "league":      league,
        "match_time":  match_time_raw,
        "time_str":    time_str,
        "date_str":    date_str,
        "sort_key":    sort_key,
        "detail_url":  detail_url,
        "thumbnail":   thumbnail,
        "blv":         blv,
        "_logo_a":     "",
        "_logo_b":     "",
    }

def parse_nextdata_match(obj: dict) -> dict | None:
    """Parse match từ __NEXT_DATA__ JSON."""
    def _get(d, *keys):
        for k in keys:
            for dk in d.keys():
                if dk.lower() == k.lower():
                    return d[dk]
        return ""

    home  = _get(obj, "home_team", "hometeam", "team_a", "team1", "home")
    away  = _get(obj, "away_team", "awayteam", "team_b", "team2", "away")
    url   = _get(obj, "url", "link", "detail_url", "slug", "href")
    if isinstance(url, str) and url and not url.startswith("http"):
        url = urljoin(BASE_URL, url)

    if not home or not away: return None
    base_title = f"{home} vs {away}"
    return {
        "base_title": base_title,
        "home_team":  str(home), "away_team": str(away),
        "score":      str(_get(obj, "score", "result", "")),
        "status":     str(_get(obj, "status", "state", "upcoming")).lower(),
        "league":     str(_get(obj, "league", "competition", "tournament", "")),
        "match_time": str(_get(obj, "time", "start_time", "match_time", "")),
        "time_str":   "", "date_str": "", "sort_key": "",
        "detail_url": url or BASE_URL + "/",
        "thumbnail":  str(_get(obj, "thumbnail", "thumb", "image", "")),
        "blv":        "", "_logo_a": "", "_logo_b": "",
    }

# ── Merge trận trùng ─────────────────────────────────────────
def _normalize_title(title: str) -> str:
    t = title.lower().strip()
    return re.sub(r"[^\w\s]", "", re.sub(r"\s+", " ", t))

def merge_matches(raw_matches: list) -> list:
    merged: dict[str, dict] = {}
    for m in raw_matches:
        key = _normalize_title(m["base_title"])
        if key not in merged:
            merged[key] = {**m, "blv_sources": []}
        entry = merged[key]
        if not entry["score"] and m["score"]:         entry["score"]     = m["score"]
        if not entry["thumbnail"] and m["thumbnail"]: entry["thumbnail"] = m["thumbnail"]
        if not entry["league"] and m["league"]:       entry["league"]    = m["league"]
        if entry["status"] == "upcoming" and m["status"] in ("live", "finished"):
            entry["status"] = m["status"]
        existing_urls = {s["detail_url"] for s in entry["blv_sources"]}
        if m["detail_url"] not in existing_urls:
            entry["blv_sources"].append({"blv": m.get("blv", "") or "", "detail_url": m["detail_url"]})

    result = list(merged.values())
    priority = {"live": 0, "upcoming": 1, "finished": 2}
    result.sort(key=lambda x: (priority.get(x["status"], 9), x.get("sort_key", "")))
    return result

def extract_matches(html: str, bs, only_featured: bool = True) -> list:
    raw, seen_urls = [], set()
    cards = find_match_cards(bs, html, only_featured)

    for card in cards:
        if isinstance(card, dict):
            m = parse_nextdata_match(card)
        else:
            m = parse_card(card)
        if m and m["detail_url"] not in seen_urls:
            seen_urls.add(m["detail_url"])
            raw.append(m)

    merged = merge_matches(raw)
    log(f"  → {len(raw)} card → gộp còn {len(merged)} trận")
    return merged

# ── Phân loại môn thể thao ────────────────────────────────────
_SPORT_RULES: list[tuple[str, re.Pattern]] = [
    ("⚽ Bóng đá", re.compile(
        r"\bliga\b|\bleague\b|\bcup\b|serie a|bundesliga|ligue 1|premier|\bchampions\b|europa|"
        r"world cup|euro|aff|v.?league|ngoại hạng|bóng đá|football|soccer|"
        r"arsenal|chelsea|manchester|liverpool|barcelona|real madrid|juventus|"
        r"inter|milan|dortmund|paris|napoli|atletico|"
        r"mu\b|mcfc|manu\b|mci\b", re.I)),
    ("🏀 Bóng rổ", re.compile(
        r"\bnba\b|\bncaa\b|\bwnba\b|bóng rổ|basketball", re.I)),
    ("🏐 Bóng chuyền", re.compile(
        r"bóng chuyền|volleyball|\bvcl\b", re.I)),
    ("🎾 Tennis", re.compile(
        r"tennis|atp|wta|wimbledon|roland garros|us open|australian open", re.I)),
    ("🏉 Rugby/Bóng bầu dục", re.compile(
        r"rugby|bầu dục|\bnfl\b|\bnrl\b|\bsuper rugby\b", re.I)),
    ("🏏 Cricket", re.compile(r"cricket|\bipl\b|\bpsl\b|\bbbl\b", re.I)),
    ("🏒 Hockey", re.compile(r"hockey|\bnhl\b", re.I)),
    ("🥊 Boxing/MMA", re.compile(r"boxing|mma|ufc|fight|đấm bốc", re.I)),
    ("🏆 Thể thao khác", re.compile(r".*", re.I)),
]

def classify_sport(match: dict) -> str:
    text = " ".join([
        match.get("league", ""),
        match.get("home_team", ""),
        match.get("away_team", ""),
        match.get("base_title", ""),
    ])
    for sport_name, pattern in _SPORT_RULES:
        if pattern.search(text):
            return sport_name
    return "🏆 Thể thao khác"

def extract_matches_by_section(html: str, bs) -> list[tuple[str, list]]:
    all_matches = extract_matches(html, bs, only_featured=False)
    if not all_matches:
        return []

    # Tách live/upcoming/finished trong cùng section
    sport_buckets: dict[str, list] = {}
    for m in all_matches:
        sport = classify_sport(m)
        sport_buckets.setdefault(sport, []).append(m)

    result = []
    order  = ["⚽ Bóng đá", "🏀 Bóng rổ", "🏐 Bóng chuyền", "🎾 Tennis",
              "🏉 Rugby/Bóng bầu dục", "🏏 Cricket", "🏒 Hockey",
              "🥊 Boxing/MMA", "🏆 Thể thao khác"]
    for sport in order:
        matches = sport_buckets.get(sport, [])
        if matches:
            result.append((sport, matches))
            log(f"  ✅ {sport}: {len(matches)} trận")
    return result

# ── Stream extraction ─────────────────────────────────────────
CDN_THUMB_RE  = re.compile(
    r'https?://pub-[a-f0-9]+\.r2\.dev/[^\s\'"<>\\]+\.webp(?:\?[^\s\'"<>\\]*)?', re.I)
IMG_URL_RE    = re.compile(
    r'https?://[^\s\'"<>\\]+\.(?:webp|jpg|jpeg|png)(?:\?[^\s\'"<>\\]*)?', re.I)
_THUMB_EXCL   = re.compile(
    r'(?:favicon|logo-site|avatar|icon-\d|sprite|\d{1,2}x\d{1,2}|/ads?/)', re.I)

def _is_valid_thumb(url: str) -> bool:
    return bool(url) and not _THUMB_EXCL.search(url) and len(url) > 20

def extract_thumb_from_detail(html: str, bs) -> str:
    next_tag = bs.find("script", id="__NEXT_DATA__")
    if next_tag and next_tag.string:
        m = CDN_THUMB_RE.search(next_tag.string)
        if m and _is_valid_thumb(m.group(0)):
            return m.group(0)
    for attr in [{"property": "og:image"}, {"name": "og:image"}, {"name": "twitter:image"}]:
        tag = bs.find("meta", attrs=attr)
        if tag:
            url = tag.get("content", "")
            if url and _is_valid_thumb(url) and IMG_URL_RE.match(url):
                return url
    m = CDN_THUMB_RE.search(html)
    if m and _is_valid_thumb(m.group(0)):
        return m.group(0)
    best_url, best_w = "", 0
    for img in bs.find_all("img", src=True):
        src = img.get("src", "") or img.get("data-src", "")
        if not src or not src.startswith("http") or not _is_valid_thumb(src): continue
        try:    w = int(img.get("width", 0))
        except: w = 0
        if w > best_w: best_w, best_url = w, src
    return best_url if best_w >= 200 else ""

_QUALITY_RE  = re.compile(r"[_-](?:full[_-]?hd|fhd|1080p?|720p?|480p?|360p?|hd|sd)$", re.I)
_QUALITY_MAP = {"hd":"HD","sd":"SD","full-hd":"Full HD","full_hd":"Full HD","fhd":"Full HD",
                "1080":"Full HD","1080p":"Full HD","720":"HD","720p":"HD",
                "480":"SD","480p":"SD","360":"360p","360p":"360p"}
_QUALITY_ORDER = {"Auto": 0, "Full HD": 1, "HD": 2, "SD": 3}

def _stream_base(url):
    return _QUALITY_RE.sub("", re.sub(r"\.\w+$", "", url.rstrip("/").split("/")[-1])).lower()

def _quality_label(url):
    fname = re.sub(r"\.\w+$", "", url.rstrip("/").split("/")[-1]).lower()
    m = _QUALITY_RE.search(fname)
    return _QUALITY_MAP.get(m.group(0).lstrip("-_").lower(), m.group(0).upper()) if m else "Auto"

def filter_streams(streams: list) -> list:
    hls   = [s for s in streams if s["type"] == "hls"]
    other = [s for s in streams if s["type"] != "hls"]
    if hls:
        base  = _stream_base(hls[0]["url"])
        group = [{**s, "name": _quality_label(s["url"])} for s in hls
                 if _stream_base(s["url"]) == base]
        group.sort(key=lambda x: _QUALITY_ORDER.get(x["name"], 99))
        return group + other
    return other

def extract_streams_from_url(detail_url: str, html: str, bs) -> list:
    seen, raw = set(), []

    def add(name, url, kind):
        url = url.strip()
        if url and url not in seen and len(url) > 12:
            seen.add(url)
            raw.append({"name": name, "url": url, "type": kind, "referer": detail_url})

    for iframe in bs.find_all("iframe", src=True):
        if re.search(r"live|stream|embed|player|sport|watch|truc.?tiep", iframe["src"], re.I):
            add("embed", iframe["src"], "iframe")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("HLS", m.group(1), "hls")
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.mpd(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("DASH", m.group(1), "dash")
    for script in bs.find_all("script"):
        c = script.string or ""
        for m in re.finditer(
                r'"(?:file|src|source|stream|url|hls|playlist|videoUrl|streamUrl)"\s*:\s*"(https?://[^"]+)"', c):
            u = m.group(1)
            if re.search(r"m3u8|stream|live|video|play", u, re.I):
                add("Stream config", u, "hls")
        for m in re.finditer(r'(?:streamUrl|videoUrl|hlsUrl|playerUrl)\s*=\s*["\']([^"\']+)["\']', c):
            u = m.group(1)
            if u.startswith("http"): add("JS stream", u, "hls")
    for a in bs.find_all("a", href=True):
        href, txt = a["href"], a.get_text(strip=True)
        if re.search(r"xem|live|watch|stream|truc.?tiep|play|server", txt + href, re.I):
            if href.startswith("http") and href != detail_url:
                add(txt or "Link", href, "hls")

    if not raw:
        raw.append({"name": "Trang trực tiếp", "url": detail_url,
                    "type": "iframe", "referer": detail_url})
        return raw
    hls = [s for s in raw if s["type"] == "hls"]
    return filter_streams(hls) if hls else raw

def extract_logos_from_detail(html: str, bs) -> tuple[str, str]:
    logo_a = logo_b = ""
    next_tag = bs.find("script", id="__NEXT_DATA__")
    if next_tag and next_tag.string:
        try:
            nd = json.loads(next_tag.string)
            def _find_logos(obj, depth=0):
                nonlocal logo_a, logo_b
                if depth > 10 or (logo_a and logo_b): return
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        kl = k.lower()
                        if isinstance(v, str) and v.startswith("http"):
                            if any(x in kl for x in ("logo_a","logoa","team_a_logo","home_logo")):
                                if not logo_a and _is_logo_url(v): logo_a = v
                            elif any(x in kl for x in ("logo_b","logob","team_b_logo","away_logo")):
                                if not logo_b and _is_logo_url(v): logo_b = v
                            elif any(x in kl for x in ("logo","badge","crest","emblem")):
                                if not logo_a: logo_a = v
                                elif not logo_b: logo_b = v
                        else:
                            _find_logos(v, depth + 1)
                elif isinstance(obj, list):
                    for item in obj[:20]: _find_logos(item, depth + 1)
            _find_logos(nd)
        except Exception:
            pass
    if not logo_a or not logo_b:
        api_logos = re.findall(
            r'https://media\.api-sports\.io/(?:football|basketball|baseball|hockey|rugby|volleyball)'
            r'/teams/\d+\.png', html)
        seen = list(dict.fromkeys(api_logos))
        if len(seen) >= 1 and not logo_a: logo_a = seen[0]
        if len(seen) >= 2 and not logo_b: logo_b = seen[1]
    return logo_a, logo_b

def crawl_blv_source(detail_url: str, blv_name: str, scraper) -> tuple[list, str, str, str]:
    html = fetch_html(detail_url, scraper, retries=2)
    if not html: return [], "", "", ""
    bs     = BeautifulSoup(html, "lxml")
    thumb  = extract_thumb_from_detail(html, bs)
    la, lb = extract_logos_from_detail(html, bs)
    streams = extract_streams_from_url(detail_url, html, bs)
    for s in streams:
        s["blv"] = blv_name
    return streams, thumb, la, lb

# ── Build display title ───────────────────────────────────────
def build_display_title(m: dict) -> str:
    base, score, t, d = m["base_title"], m["score"], m["time_str"], m["date_str"]
    if m["status"] == "live":
        if score and score != "VS":
            return f"{m['home_team']} {score} {m['away_team']}  🔴"
        return f"{base}  🔴 LIVE"
    elif m["status"] == "finished":
        if score and score != "VS":
            return f"{m['home_team']} {score} {m['away_team']}  ✅"
        return f"{base}  ✅ KT"
    else:
        if t and d:  return f"{base}  🕐 {t} | {d}"
        elif t:      return f"{base}  🕐 {t}"
        elif d:      return f"{base}  📅 {d}"
        return base

# ── Build channel ─────────────────────────────────────────────
def build_channel(m: dict, all_streams: list, thumb: str,
                  index: int, league: str = "") -> dict:

    ch_id        = make_id("ctt", str(index), re.sub(r"[^a-z0-9]", "-", m["base_title"].lower())[:24])
    display_name = build_display_title(m)
    blv_sources  = m.get("blv_sources", [])
    score        = m.get("score", "")
    eff_league   = league or m.get("league", "")

    # Labels
    labels = []
    status_cfg = {
        "live":     {"text": "● Live",          "color": "#E73131", "text_color": "#ffffff"},
        "upcoming": {"text": "🕐 Sắp diễn ra", "color": "#d54f1a", "text_color": "#ffffff"},
        "finished": {"text": "✅ Kết thúc",     "color": "#444444", "text_color": "#ffffff"},
    }.get(m["status"], {"text": "● Live", "color": "#E73131", "text_color": "#ffffff"})
    labels.append({**status_cfg, "position": "top-left"})

    blv_names = [s["blv"] for s in blv_sources if s["blv"]]
    if len(blv_names) > 1:
        labels.append({"text": f"🎙 {len(blv_names)} BLV", "position": "top-right",
                       "color": "#00601f", "text_color": "#ffffff"})
    elif blv_names:
        labels.append({"text": f"🎙 {blv_names[0]}", "position": "top-right",
                       "color": "#00601f", "text_color": "#ffffff"})

    if score and score not in ("", "VS"):
        color = "#E73131" if m["status"] == "live" else "#444444"
        prefix = "⚽" if m["status"] == "live" else "KT"
        labels.append({"text": f"{prefix} {score}", "position": "bottom-right",
                       "color": color, "text_color": "#ffffff"})

    # Streams
    stream_objs = []
    blv_groups: dict[str, list] = {}
    for s in all_streams:
        key = s.get("blv") or "__no_blv__"
        blv_groups.setdefault(key, []).append(s)

    for blv_idx, (blv_key, raw_s) in enumerate(blv_groups.items()):
        filtered = filter_streams(raw_s) if raw_s else []
        if not filtered: continue
        stream_label = f"🎙 {blv_key}" if blv_key != "__no_blv__" else f"Nguồn {blv_idx + 1}"
        slinks = []
        for lnk_idx, s in enumerate(filtered):
            quality   = s.get("name", "Auto")
            link_name = quality if quality != "Auto" else f"Link {lnk_idx + 1}"
            referer   = s.get("referer", blv_sources[0]["detail_url"] if blv_sources else BASE_URL + "/")
            slinks.append({
                "id":      make_id(ch_id, f"b{blv_idx}", f"l{lnk_idx}"),
                "name":    link_name,
                "type":    s["type"],
                "default": lnk_idx == 0,
                "url":     s["url"],
                "request_headers": [
                    {"key": "Referer",    "value": referer},
                    {"key": "User-Agent", "value": CHROME_UA},
                ],
            })
        stream_objs.append({
            "id":           make_id(ch_id, f"st{blv_idx}"),
            "name":         stream_label,
            "stream_links": slinks,
        })

    if not stream_objs:
        fallback_url = blv_sources[0]["detail_url"] if blv_sources else BASE_URL + "/"
        stream_objs.append({
            "id": make_id(ch_id, "st0"), "name": "Trực tiếp",
            "stream_links": [{
                "id": "lnk0", "name": "Link 1", "type": "iframe", "default": True,
                "url": fallback_url,
                "request_headers": [
                    {"key": "Referer",    "value": fallback_url},
                    {"key": "User-Agent", "value": CHROME_UA},
                ],
            }],
        })

    # Thumbnail
    logo_a_url = m.get("_logo_a", "")
    logo_b_url = m.get("_logo_b", "")
    _BAD_THUMB = ("opengraph", "favicon", "og-image", "og_image", "site-logo",
                  "/favicon.", "opengraph-image")
    thumb_ok = bool(thumb and thumb.startswith("http")
                    and not any(b in thumb.lower() for b in _BAD_THUMB))

    if thumb_ok:
        img_obj = {"padding": 0, "background_color": "#000000", "display": "cover",
                   "url": thumb, "width": 1600, "height": 1200}
    elif _PILLOW_OK:
        jpeg_uri = make_match_thumbnail_b64(
            home_team=m["home_team"], away_team=m["away_team"],
            logo_a_url=logo_a_url, logo_b_url=logo_b_url,
            time_str=m.get("time_str", ""), date_str=m.get("date_str", ""),
            status=m["status"], score=m.get("score", ""), league=eff_league,
        )
        img_obj = ({"padding": 0, "background_color": "#000000", "display": "cover",
                    "url": jpeg_uri, "width": 800, "height": 450}
                   if jpeg_uri else PLACEHOLDER_IMG)
    else:
        img_obj = PLACEHOLDER_IMG

    content_name = display_name
    if eff_league and len(eff_league.strip()) < 50:
        content_name += f" · {eff_league.strip()}"

    has_multi = len(stream_objs) > 1
    return {
        "id":            ch_id,
        "name":          display_name,
        "type":          "multi" if has_multi else "single",
        "display":       "thumbnail-only",
        "enable_detail": has_multi,
        "image":         img_obj,
        "labels":        labels,
        "sources": [{
            "id":   make_id(ch_id, "src"),
            "name": "CauThuTV Live",
            "contents": [{
                "id":      make_id(ch_id, "ct"),
                "name":    content_name,
                "streams": stream_objs,
            }],
        }],
    }

# ── Root JSON ─────────────────────────────────────────────────
def build_iptv_json(groups_data: list, now_str: str) -> dict:
    groups_out = []
    for label, channels in groups_data:
        gid = re.sub(r"[^a-z0-9]", "-", label.lower())[:24].strip("-")
        groups_out.append({"id": gid, "name": label, "image": None, "channels": channels})
    return {
        "id":          "cauthutv-live",
        "name":        "CauThu TV - Trực tiếp thể thao",
        "url":         BASE_URL + "/",
        "description": f"Cập nhật lúc {now_str}",
        "disable_ads": True,
        "color":       "#0f3460",
        "grid_number": 3,
        "image":       {"type": "cover", "url": f"{BASE_URL}/favicon.ico"},
        "groups":      groups_out,
    }

# ── Main ──────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Crawler cauthutv.shop")
    ap.add_argument("--all",       action="store_true", help="Tất cả trận (không phân loại)")
    ap.add_argument("--no-stream", action="store_true", help="Không crawl stream (nhanh hơn)")
    ap.add_argument("--debug",     action="store_true", help="Lưu HTML để phân tích cấu trúc")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    args = ap.parse_args()

    log("\n" + "═" * 62)
    log("  🏟  CRAWLER — cauthutv.shop  v1")
    log("  📸  Thumbnail Pillow + 🎙 Tách stream theo BLV")
    log("═" * 62 + "\n")

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
        with open(DEBUG_HTML, "w", encoding="utf-8") as f:
            f.write(html)
        log(f"  💾 Đã lưu HTML → {DEBUG_HTML}")

    bs = BeautifulSoup(html, "lxml")

    log(f"\n🔍 Phân tích trang...")
    if args.all:
        matches = extract_matches(html, bs, only_featured=False)
        sections = [("🏟 Tất cả trận đấu", matches)] if matches else []
    else:
        sections = extract_matches_by_section(html, bs)
        if not sections:
            log("  ⚠ Không có section nào, thử toàn trang...")
            matches = extract_matches(html, bs, only_featured=False)
            sections = [("🏟 Trận đấu", matches)] if matches else []

    if not sections:
        log("  ❌ Không tìm thấy trận nào.")
        if not args.debug:
            log("  💡 Thử chạy với --debug để lưu HTML và kiểm tra cấu trúc.")
        sys.exit(1)

    total_matches = sum(len(m) for _, m in sections)
    log(f"\n  ✅ {len(sections)} section, {total_matches} trận tổng cộng\n")

    # Crawl streams + build channels
    log(f"🖼  Crawl streams + tạo thumbnail...")
    groups_data = []
    global_idx  = 0

    for label, matches in sections:
        log(f"\n  ── {label} ({len(matches)} trận) ──")
        channels = []
        for m in matches:
            global_idx += 1
            i = global_idx
            m["_logo_a"] = m.get("_logo_a", "")
            m["_logo_b"] = m.get("_logo_b", "")

            all_streams = []
            thumb       = m.get("thumbnail", "")

            if not args.no_stream:
                for src in m.get("blv_sources", []):
                    blv_name   = src["blv"] or ""
                    detail_url = src["detail_url"]
                    streams, page_thumb, pg_la, pg_lb = crawl_blv_source(
                        detail_url, blv_name, scraper)
                    if not thumb and page_thumb: thumb = page_thumb
                    if not m["_logo_a"] and pg_la: m["_logo_a"] = pg_la
                    if not m["_logo_b"] and pg_lb: m["_logo_b"] = pg_lb
                    seen_u = {s["url"] for s in all_streams}
                    all_streams.extend(s for s in streams if s["url"] not in seen_u)
                time.sleep(0.4)

            log(f"  [{i:03d}] {m['base_title'][:45]}"
                f"  streams={len(all_streams)}"
                f"  thumb={'✓' if thumb else '✗'}")
            channels.append(build_channel(m, all_streams, thumb, i, m.get("league", "")))
        groups_data.append((label, channels))

    result = build_iptv_json(groups_data, now_str)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log(f"\n{'═' * 62}")
    log(f"  ✅ Xong!  📁 {args.output}  {total_matches} trận  {len(sections)} group")
    log(f"  🕐 {now_str}")
    log("═" * 62 + "\n")

if __name__ == "__main__":
    main()
