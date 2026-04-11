#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler Trực Tiếp — cauthutv.shop  v3                     ║
║   CHỈ crawl mục "Trận HOT" trên trang chủ                  ║
║   Logo thumbnail: ghép 2 ảnh đội từ card HTML               ║
║     (không dùng thesportsdb / vẽ chữ tắt)                  ║
║   + Crawl stream: m3u8 / DASH / iframe                       ║
║   + Debug mode: lưu HTML để kiểm tra                        ║
╚══════════════════════════════════════════════════════════════╝
Cài đặt:
    pip install cloudscraper beautifulsoup4 lxml requests pillow

Chạy:
    python crawler_cauthutv.py                  # mặc định
    python crawler_cauthutv.py --no-stream      # không crawl stream
    python crawler_cauthutv.py --debug          # lưu HTML để kiểm tra
    python crawler_cauthutv.py --output out.json
"""

import argparse, base64, hashlib, io, json, os, re, sys, time, unicodedata
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse

try:
    import cloudscraper
    from bs4 import BeautifulSoup
    import requests
except ImportError:
    print("Cài đặt: pip install cloudscraper beautifulsoup4 lxml requests pillow")
    sys.exit(1)

try:
    from PIL import Image, ImageDraw, ImageFont
    _PILLOW_OK = True
except ImportError:
    _PILLOW_OK = False

# ── Constants ──────────────────────────────────────────────────
BASE_URL    = "https://cauthutv.shop"
OUTPUT_FILE = "cauthutv_iptv.json"
DEBUG_HTML  = "debug_cauthutv.html"
CHROME_UA   = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
VN_TZ = timezone(timedelta(hours=7))

# Icon favicon thật của trang (phát hiện tự động, đây là fallback)
SITE_ICON_CANDIDATES = [
    f"{BASE_URL}/assets/image/favicon64.png",
    f"{BASE_URL}/assets/image/logo.png",
    f"{BASE_URL}/favicon.ico",
]

PLACEHOLDER_IMG = {
    "padding": 0, "background_color": "#0f3460", "display": "cover",
    "url": SITE_ICON_CANDIDATES[0], "width": 512, "height": 512,
}

def log(*a, **kw): print(*a, **kw, flush=True)

# ══════════════════════════════════════════════════════════════
#  THUMBNAIL — ghép 2 logo đội từ URL ảnh lấy trên card
# ══════════════════════════════════════════════════════════════

def _font(size, bold=True):
    if not _PILLOW_OK: return None
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold
            else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold
            else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
    ]
    for p in paths:
        try: return ImageFont.truetype(p, size)
        except: pass
    return ImageFont.load_default()


def fetch_img(url: str, max_px: int = 300) -> "Image.Image | None":
    """Tải ảnh từ URL, resize về max_px × max_px, trả về RGBA."""
    if not url or not _PILLOW_OK: return None
    try:
        r = requests.get(url.strip(), timeout=8,
                         headers={"User-Agent": "Mozilla/5.0"}, stream=True)
        r.raise_for_status()
        ct = r.headers.get("content-type", "")
        if "html" in ct or "json" in ct: return None
        data = b""
        for chunk in r.iter_content(65536):
            data += chunk
            if len(data) > 2_000_000: return None   # bỏ qua ảnh > 2 MB
        img = Image.open(io.BytesIO(data)).convert("RGBA")
        if img.width > 3000 or img.height > 3000: return None
        img.thumbnail((max_px, max_px), Image.LANCZOS)
        return img
    except Exception:
        return None


def make_thumbnail_b64(
    home_team: str, away_team: str,
    logo_a_url: str = "", logo_b_url: str = "",
    time_str: str = "", date_str: str = "",
    status: str = "upcoming", score: str = "",
    league: str = "",
) -> str:
    """
    Tạo thumbnail JPEG 800×450.
    Logo A (đội nhà) lấy từ logo_a_url  → paste bên TRÁI.
    Logo B (đội khách) lấy từ logo_b_url → paste bên PHẢI.
    Nếu URL không tải được → vẽ vòng tròn + chữ viết tắt.
    Trả về data:image/jpeg;base64,...
    """
    if not _PILLOW_OK: return ""

    W, H = 800, 450
    img  = Image.new("RGBA", (W, H), (20, 30, 55, 255))
    draw = ImageDraw.Draw(img)

    # ── gradient nền ──
    for y in range(H):
        t = y / H
        r_ = int(18 + 22*t); g_ = int(28 + 30*t); b_ = int(52 + 35*t)
        draw.line([(0, y), (W, y)], fill=(r_, g_, b_, 255))

    # ── thanh giải đấu ──
    draw.rectangle([(0, 0), (W, 52)], fill=(8, 12, 26, 255))
    if league:
        f22 = _font(22)
        draw.text((W//2, 28), league[:40], fill=(240, 240, 240, 255),
                  font=f22, anchor="mm")
    draw.line([(0, 52), (W, 52)], fill=(60, 100, 200, 120), width=2)

    LOGO_D  = 160          # diameter vòng tròn nền logo
    LOGO_R  = LOGO_D // 2
    LOGO_Y  = 55 + (H - 55 - 80) // 2 + 10   # tâm logo theo chiều dọc
    NAME_Y  = LOGO_Y + LOGO_R + 22
    LX      = 160          # tâm logo trái
    RX      = W - 160      # tâm logo phải
    CX      = W // 2       # tâm giữa (score / giờ)

    # ── hàm vẽ 1 logo ──
    def _draw_logo(cx, cy, url, name):
        # vòng tròn nền
        draw.ellipse(
            [(cx - LOGO_R - 6, cy - LOGO_R - 6),
             (cx + LOGO_R + 6, cy + LOGO_R + 6)],
            fill=(255, 255, 255, 18), outline=(180, 200, 255, 60), width=2
        )

        logo = fetch_img(url, LOGO_D * 2) if url else None

        if logo:
            # scale giữ tỉ lệ
            lw, lh = logo.size
            scale  = min((LOGO_D - 10) / lw, (LOGO_D - 10) / lh, 1.0)
            nw     = max(1, int(lw * scale))
            nh     = max(1, int(lh * scale))
            logo   = logo.resize((nw, nh), Image.LANCZOS)

            # tạo mask tròn để clip logo
            mask   = Image.new("L", (nw, nh), 0)
            mdraw  = ImageDraw.Draw(mask)
            mdraw.ellipse([(0, 0), (nw - 1, nh - 1)], fill=255)

            ox = cx - nw // 2
            oy = cy - nh // 2
            # Clip logo với mask tròn (không cần numpy)
            # Nhân alpha của logo với mask tròn bằng ImageChops
            from PIL import ImageChops
            alpha_ch  = logo.split()[3]
            mask_rs   = mask.resize((nw, nh), Image.LANCZOS)
            combined  = ImageChops.multiply(alpha_ch, mask_rs)
            # Tạo ảnh RGBA với alpha đã clip
            logo_clip = logo.copy()
            logo_clip.putalpha(combined)
            # Paste lên canvas
            img.paste(logo_clip, (ox, oy), logo_clip.split()[3])
        else:
            # fallback: vòng tròn màu + chữ tắt
            draw.ellipse(
                [(cx - LOGO_R, cy - LOGO_R), (cx + LOGO_R, cy + LOGO_R)],
                fill=(30, 55, 110, 220), outline=(100, 150, 230, 200), width=3
            )
            words = (name or "?").split()
            init  = "".join(w[0].upper() for w in words[:2]) or "?"
            draw.text((cx, cy), init, fill=(180, 215, 255, 255),
                      font=_font(52), anchor="mm")

        # tên đội
        short = (name or "?")[:16]
        draw.text((cx, NAME_Y), short, fill=(255, 255, 255, 220),
                  font=_font(18), anchor="mm")

    # ── vẽ 2 logo ──
    _draw_logo(LX, LOGO_Y, logo_a_url, home_team)
    _draw_logo(RX, LOGO_Y, logo_b_url, away_team)

    # ── vùng giữa: tỉ số / giờ ──
    if status == "live" and score and score not in ("", "VS"):
        ctr, ctr_col = score, (255, 60, 60, 255)
        sub, sub_col = "● LIVE", (255, 110, 110, 255)
    elif status == "finished" and score and score not in ("", "VS"):
        ctr, ctr_col = score, (255, 255, 255, 255)
        sub, sub_col = "Kết thúc", (170, 170, 170, 255)
    else:
        ctr, ctr_col = time_str or "VS", (255, 255, 255, 255)
        sub, sub_col = date_str or "", (180, 180, 180, 255)

    # gạch 2 bên chữ giữa
    draw.line([(CX - 68, LOGO_Y - 8), (CX - 22, LOGO_Y - 8)],
              fill=(255, 255, 255, 70), width=2)
    draw.line([(CX + 22, LOGO_Y - 8), (CX + 68, LOGO_Y - 8)],
              fill=(255, 255, 255, 70), width=2)
    draw.text((CX, LOGO_Y - 6), ctr, fill=ctr_col, font=_font(50), anchor="mm")
    if sub:
        draw.text((CX, LOGO_Y + 36), sub, fill=sub_col,
                  font=_font(17, False), anchor="mm")

    # ── fade bottom ──
    for y in range(H - 55, H):
        a = int(255 * (y - (H - 55)) / 55)
        draw.line([(0, y), (W, y)], fill=(6, 16, 28, a))

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=84, optimize=True)
    return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()


# ══════════════════════════════════════════════════════════════
#  HTTP helpers
# ══════════════════════════════════════════════════════════════

def make_scraper():
    sc = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    sc.headers.update({
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
        "Referer":         BASE_URL + "/",
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
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


# ══════════════════════════════════════════════════════════════
#  Tìm icon trang
# ══════════════════════════════════════════════════════════════

def detect_site_icon(html: str, bs) -> str:
    for rel in ("apple-touch-icon", "icon", "shortcut icon"):
        tag = bs.find("link", rel=lambda r, _r=rel: r and _r in " ".join(r))
        if tag:
            href = tag.get("href", "")
            if href:
                return href if href.startswith("http") else urljoin(BASE_URL, href)
    for img_tag in bs.find_all("img", src=True):
        src = img_tag.get("src", "")
        if "logo" in src.lower() and src.startswith("http"):
            return src
    return SITE_ICON_CANDIDATES[0]


# ══════════════════════════════════════════════════════════════
#  Parse ngày giờ
# ══════════════════════════════════════════════════════════════

def parse_match_datetime(raw: str):
    if not raw: return "", "", ""
    m = re.search(r"(\d{1,2}):(\d{2})\s*[|]?\s*(\d{1,2})[./](\d{1,2})", raw)
    if m:
        hh, mm   = m.group(1).zfill(2), m.group(2)
        day, mon = m.group(3).zfill(2), m.group(4).zfill(2)
        if int(hh) <= 23 and int(mm) <= 59:
            return f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}"
    m2 = re.search(r"(\d{1,2}):(\d{2})", raw)
    if m2:
        hh, mm = m2.group(1).zfill(2), m2.group(2)
        if int(hh) <= 23 and int(mm) <= 59:
            today = datetime.now(VN_TZ)
            return f"{hh}:{mm}", today.strftime("%d/%m"), \
                   f"{today.strftime('%m-%d')} {hh}:{mm}"
    return "", "", ""


# ══════════════════════════════════════════════════════════════
#  Tìm mục TRẬN HOT trên trang chủ
# ══════════════════════════════════════════════════════════════

# Regex nhận diện section HOT
_HOT_ID_RE = re.compile(
    r"tran[-_]?hot|hot[-_]?match|featured|highlight|tam[-_]?diem|"
    r"trandau[-_]?hot|tran[-_]?tam[-_]?diem|match[-_]?hot|"
    r"top[-_]?match|pick|trending|spotlight|hot",
    re.I
)
_HOT_TEXT_RE = re.compile(
    r"trận\s*hot|hot\s*match|nổi\s*bật|tâm\s*điểm|đỉnh\s*cao|"
    r"trận\s*đỉnh|được\s*xem\s*nhiều|trending|featured|trận hot",
    re.I | re.UNICODE
)


def _cards_in(container, min_cards: int = 1) -> list:
    """Lấy thẻ <a> trông như card trận đấu trong container."""
    VS_RE = re.compile(r"\bvs\b|\blive\b|:\d{2}|trực tiếp", re.I)
    result = []
    seen   = set()
    for a in container.find_all("a", href=True):
        href = a.get("href", "")
        if href in seen: continue
        text = a.get_text(" ", strip=True)
        if VS_RE.search(text) and len(text) > 5:
            result.append(a)
            seen.add(href)
    return result if len(result) >= min_cards else []


def find_hot_section(bs) -> "Tag | None":
    """
    Trả về phần tử HTML chứa các card trận HOT.
    Thử theo thứ tự: id/class HOT → heading HOT → section đầu tiên có nhiều card.
    """
    # 1. id / class khớp từ khóa hot
    for tag in bs.find_all(["section", "div", "ul", "article"]):
        tid  = " ".join(tag.get("id",    []) if isinstance(tag.get("id"), list)    else [tag.get("id","") or ""])
        tcls = " ".join(tag.get("class", []))
        if _HOT_ID_RE.search(tid) or _HOT_ID_RE.search(tcls):
            if _cards_in(tag):
                log(f"  → HOT section via id/class: id='{tag.get('id','')}' cls='{tcls[:50]}'")
                return tag

    # 2. heading có chữ hot
    for h in bs.find_all(["h1","h2","h3","h4","span","p","strong"]):
        if _HOT_TEXT_RE.search(h.get_text()):
            parent = h.parent
            for _ in range(5):
                if parent is None: break
                cards = _cards_in(parent)
                if len(cards) >= 2:
                    log(f"  → HOT section via heading: '{h.get_text(strip=True)[:40]}'")
                    return parent
                parent = parent.parent
            break

    # 3. Fallback: section / div đầu tiên chứa nhiều card (trang chủ thường để HOT lên đầu)
    for tag in bs.find_all(["section", "div", "ul"], recursive=False):
        cards = _cards_in(tag, min_cards=3)
        if cards:
            log(f"  → HOT section via fallback (first big section)")
            return tag
    # Thử toàn body
    for tag in bs.find_all(["section", "div"], limit=20):
        cards = _cards_in(tag, min_cards=3)
        if len(cards) >= 3:
            log(f"  → HOT section via body scan")
            return tag

    return None


# ══════════════════════════════════════════════════════════════
#  Parse 1 card → dict trận
# ══════════════════════════════════════════════════════════════

def parse_card(a) -> dict | None:
    href = a.get("href", "")
    if not href: return None
    detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
    raw_text   = a.get_text(" ", strip=True)

    # Trạng thái
    if re.search(r"\bLive\b|trực tiếp|đang phát", raw_text, re.I):
        status = "live"
    elif re.search(r"Kết thúc|Finished|\bFT\b|đã kết", raw_text, re.I):
        status = "finished"
    else:
        status = "upcoming"

    # Giờ
    mt_raw = ""
    _mt = re.search(r"(\d{1,2}:\d{2})\s*[|]\s*(\d{1,2}[./]\d{1,2})", raw_text)
    if _mt:
        mt_raw = _mt.group(0)
    else:
        _mt2 = re.search(r"(\d{1,2}:\d{2})", raw_text)
        if _mt2:
            h_, m_ = _mt2.group(0).split(":")
            if int(h_) <= 23 and int(m_) <= 59:
                mt_raw = _mt2.group(0)
    time_str, date_str, sort_key = parse_match_datetime(mt_raw)

    # Tên đội — ưu tiên class
    home_team = away_team = ""
    for tag in ["div", "span", "p"]:
        for cls_hint in ["team-name","team_name","club-name","team","flex-1","flex-col"]:
            cands = a.find_all(tag, class_=re.compile(cls_hint, re.I))
            texts = [c.get_text(" ", strip=True) for c in cands
                     if c.get_text(strip=True) and len(c.get_text(strip=True)) >= 2
                     and not re.fullmatch(r"[\d\s:]+", c.get_text(strip=True))]
            if len(texts) >= 2:
                home_team, away_team = texts[0], texts[1]
                break
        if home_team: break

    # Fallback VS regex
    if not home_team:
        vm = re.search(
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34}?)"
            r"\s+(?:VS|vs)\s+"
            r"([\w\u00C0-\u024F\u1E00-\u1EFF][\w\u00C0-\u024F\u1E00-\u1EFF .'-]{1,34})",
            raw_text, re.UNICODE)
        if vm:
            home_team, away_team = vm.group(1).strip(), vm.group(2).strip()

    # Giải đấu
    league = ""
    for d in a.find_all(["div","span","p"],
                         class_=re.compile(r"league|tournament|competition|giải", re.I)):
        t = d.get_text(strip=True)
        if t and 3 < len(t) < 60 and not re.fullmatch(r"[\d:\s|./]+", t):
            league = t; break

    # Tỉ số
    score = ""
    sm = re.search(r"\b(\d{1,2})\s*[-:]\s*(\d{1,2})\b", raw_text)
    if sm: score = f"{sm.group(1)}-{sm.group(2)}"

    # BLV
    blv = ""
    for sp in a.find_all("span", class_=re.compile(r"blv|commentator", re.I)):
        blv = sp.get_text(strip=True)
        if blv: break

    # ── Logo URLs: lấy TẤT CẢ ảnh trong card theo thứ tự xuất hiện ──
    logo_urls = []
    for img in a.find_all("img"):
        src = (img.get("src") or img.get("data-src") or "").strip()
        if not src: continue
        if not src.startswith("http"): src = urljoin(BASE_URL, src)
        # loại ảnh nền/banner, chỉ lấy ảnh logo nhỏ
        _BAD = ("banner","background","bg-","bg_","cover","thumbnail",
                "splash","ad","ads","opengraph","og-")
        if any(b in src.lower() for b in _BAD): continue
        logo_urls.append(src)

    logo_a_url = logo_urls[0] if len(logo_urls) >= 1 else ""
    logo_b_url = logo_urls[1] if len(logo_urls) >= 2 else ""

    # Thumbnail chính (ảnh lớn đầu tiên trong card, nếu có)
    thumbnail = ""
    for img in a.find_all("img"):
        src = (img.get("src") or img.get("data-src") or "").strip()
        if not src: continue
        if not src.startswith("http"): src = urljoin(BASE_URL, src)
        w = 0
        try: w = int(img.get("width", 0))
        except: pass
        if w >= 300:
            thumbnail = src; break

    base_title = (f"{home_team} vs {away_team}"
                  if home_team and away_team
                  else re.sub(r"\s{2,}", " ", raw_text)[:60])
    if not base_title or not detail_url: return None

    return {
        "base_title":  base_title,
        "home_team":   home_team,
        "away_team":   away_team,
        "score":       score,
        "status":      status,
        "league":      league,
        "time_str":    time_str,
        "date_str":    date_str,
        "sort_key":    sort_key,
        "detail_url":  detail_url,
        "thumbnail":   thumbnail,
        "logo_a_url":  logo_a_url,
        "logo_b_url":  logo_b_url,
        "blv":         blv,
    }


# ══════════════════════════════════════════════════════════════
#  Merge trận trùng URL
# ══════════════════════════════════════════════════════════════

def _norm(t): return re.sub(r"[^\w\s]", "", t.lower().strip())


def merge_matches(raw: list) -> list:
    merged: dict[str, dict] = {}
    for m in raw:
        key = _norm(m["base_title"])
        if key not in merged:
            merged[key] = {**m, "blv_sources": []}
        e = merged[key]
        if not e["score"] and m["score"]: e["score"] = m["score"]
        if not e["thumbnail"] and m["thumbnail"]: e["thumbnail"] = m["thumbnail"]
        if not e["league"] and m["league"]: e["league"] = m["league"]
        if not e["logo_a_url"] and m["logo_a_url"]: e["logo_a_url"] = m["logo_a_url"]
        if not e["logo_b_url"] and m["logo_b_url"]: e["logo_b_url"] = m["logo_b_url"]
        if e["status"] == "upcoming" and m["status"] in ("live","finished"):
            e["status"] = m["status"]
        existing = {s["detail_url"] for s in e["blv_sources"]}
        if m["detail_url"] not in existing:
            e["blv_sources"].append({"blv": m.get("blv","") or "", "detail_url": m["detail_url"]})
    result = list(merged.values())
    pri = {"live": 0, "upcoming": 1, "finished": 2}
    result.sort(key=lambda x: (pri.get(x["status"], 9), x.get("sort_key", "")))
    return result


# ══════════════════════════════════════════════════════════════
#  Crawl chi tiết: stream + logo từ detail page
# ══════════════════════════════════════════════════════════════

_QUAL_RE  = re.compile(r"[_-](?:full[_-]?hd|fhd|1080p?|720p?|480p?|360p?|hd|sd)$", re.I)
_QUAL_MAP = {"hd":"HD","sd":"SD","full-hd":"Full HD","fhd":"Full HD",
             "1080":"Full HD","1080p":"Full HD","720":"HD","720p":"HD",
             "480":"SD","480p":"SD","360":"360p","360p":"360p"}
_QUAL_ORD = {"Auto":0,"Full HD":1,"HD":2,"SD":3}


def _quality_label(url):
    fname = re.sub(r"\.\w+$", "", url.rstrip("/").split("/")[-1]).lower()
    m = _QUAL_RE.search(fname)
    return _QUAL_MAP.get(m.group(0).lstrip("-_").lower(), m.group(0).upper()) if m else "Auto"


def extract_streams(detail_url: str, html: str, bs) -> list:
    seen, raw = set(), []

    def add(name, url, kind):
        url = url.strip()
        if url and url not in seen and len(url) > 12:
            seen.add(url); raw.append({"name":name,"url":url,"type":kind,"referer":detail_url})

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

    hls = [s for s in raw if s["type"] == "hls"]
    if hls:
        # nhóm theo base, lấy nhóm lớn nhất
        from collections import Counter
        def base(u): return _QUAL_RE.sub("", re.sub(r"\.\w+$","",u.rstrip("/").split("/")[-1])).lower()
        cnt = Counter(base(s["url"]) for s in hls)
        top_base = cnt.most_common(1)[0][0]
        group = [{**s,"name":_quality_label(s["url"])} for s in hls if base(s["url"])==top_base]
        group.sort(key=lambda x: _QUAL_ORD.get(x["name"], 99))
        return group
    return raw


def extract_logos_from_detail(html: str, bs) -> tuple:
    """Tìm URL logo 2 đội từ trang chi tiết (Next.js data / img tags)."""
    logo_a = logo_b = ""

    # __NEXT_DATA__
    nd_tag = bs.find("script", id="__NEXT_DATA__")
    if nd_tag and nd_tag.string:
        try:
            nd = json.loads(nd_tag.string)
            def _dig(obj, depth=0):
                nonlocal logo_a, logo_b
                if depth > 10 or (logo_a and logo_b): return
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        kl = k.lower()
                        if isinstance(v, str) and v.startswith("http") and \
                           re.search(r"\.(png|svg|webp|jpg)(\?|$)", v, re.I):
                            if any(x in kl for x in ("logo_a","logoa","home_logo","team_a_logo")):
                                if not logo_a: logo_a = v
                            elif any(x in kl for x in ("logo_b","logob","away_logo","team_b_logo")):
                                if not logo_b: logo_b = v
                            elif any(x in kl for x in ("logo","badge","crest","emblem")):
                                if not logo_a: logo_a = v
                                elif not logo_b: logo_b = v
                        else: _dig(v, depth+1)
                elif isinstance(obj, list):
                    for item in obj[:30]: _dig(item, depth+1)
            _dig(nd)
        except: pass

    # img tags: lấy 2 ảnh nhỏ (logo) đầu tiên
    if not logo_a or not logo_b:
        imgs = []
        for img in bs.find_all("img", src=True):
            src = (img.get("src") or "").strip()
            if not src or not src.startswith("http"): continue
            _BAD = ("banner","background","cover","thumbnail","splash","opengraph","og-")
            if any(b in src.lower() for b in _BAD): continue
            w = 0
            try: w = int(img.get("width",0))
            except: pass
            if w == 0 or w <= 200:   # logo thường nhỏ
                imgs.append(src)
        if len(imgs) >= 1 and not logo_a: logo_a = imgs[0]
        if len(imgs) >= 2 and not logo_b: logo_b = imgs[1]

    return logo_a, logo_b


def crawl_detail(detail_url: str, blv: str, scraper):
    html = fetch_html(detail_url, scraper, retries=2)
    if not html: return [], "", ""
    bs      = BeautifulSoup(html, "lxml")
    streams = extract_streams(detail_url, html, bs)
    for s in streams: s["blv"] = blv
    la, lb  = extract_logos_from_detail(html, bs)
    return streams, la, lb


# ══════════════════════════════════════════════════════════════
#  Build channel object
# ══════════════════════════════════════════════════════════════

def make_id(*parts): return hashlib.md5("-".join(str(p) for p in parts).encode()).hexdigest()[:16]


def build_display_name(m: dict) -> str:
    base, score, t, d = m["base_title"], m["score"], m["time_str"], m["date_str"]
    if m["status"] == "live":
        return f"{base}  🔴 LIVE" if not score or score=="VS" \
               else f"{m['home_team']} {score} {m['away_team']}  🔴"
    if m["status"] == "finished":
        return f"{base}  ✅ KT" if not score or score=="VS" \
               else f"{m['home_team']} {score} {m['away_team']}  ✅"
    if t and d:  return f"{base}  🕐 {t} | {d}"
    if t:        return f"{base}  🕐 {t}"
    if d:        return f"{base}  📅 {d}"
    return base


def build_channel(m: dict, all_streams: list, index: int) -> dict:
    ch_id   = make_id("ctt", index, re.sub(r"[^a-z0-9]", "-", m["base_title"].lower())[:24])
    name    = build_display_name(m)
    league  = m.get("league", "")

    # Labels
    labels = []
    sc_map = {
        "live":     {"text": "● Live",          "color": "#E73131", "text_color": "#fff"},
        "upcoming": {"text": "🕐 Sắp diễn ra", "color": "#d54f1a", "text_color": "#fff"},
        "finished": {"text": "✅ Kết thúc",     "color": "#444444", "text_color": "#fff"},
    }
    labels.append({**sc_map.get(m["status"], sc_map["live"]), "position": "top-left"})

    blv_names = [s["blv"] for s in m.get("blv_sources", []) if s["blv"]]
    if len(blv_names) > 1:
        labels.append({"text": f"🎙 {len(blv_names)} BLV", "position": "top-right",
                       "color": "#00601f", "text_color": "#fff"})
    elif blv_names:
        labels.append({"text": f"🎙 {blv_names[0]}", "position": "top-right",
                       "color": "#00601f", "text_color": "#fff"})

    score = m.get("score", "")
    if score and score not in ("","VS"):
        col = "#E73131" if m["status"]=="live" else "#444444"
        pfx = "⚽" if m["status"]=="live" else "KT"
        labels.append({"text": f"{pfx} {score}", "position": "bottom-right",
                       "color": col, "text_color": "#fff"})

    # Streams → objects
    blv_groups: dict[str, list] = {}
    for s in all_streams:
        key = s.get("blv") or "__no_blv__"
        blv_groups.setdefault(key, []).append(s)

    stream_objs = []
    for idx, (bkey, raw_s) in enumerate(blv_groups.items()):
        if not raw_s: continue
        slabel = f"🎙 {bkey}" if bkey != "__no_blv__" else f"Nguồn {idx+1}"
        slinks = []
        for li, s in enumerate(raw_s):
            ref = s.get("referer", m["blv_sources"][0]["detail_url"] if m["blv_sources"] else BASE_URL+"/")
            slinks.append({
                "id":      make_id(ch_id, f"b{idx}", f"l{li}"),
                "name":    s.get("name","Auto"),
                "type":    s["type"],
                "default": li == 0,
                "url":     s["url"],
                "request_headers": [
                    {"key":"Referer",    "value": ref},
                    {"key":"User-Agent", "value": CHROME_UA},
                ],
            })
        stream_objs.append({
            "id":           make_id(ch_id, f"st{idx}"),
            "name":         slabel,
            "stream_links": slinks,
        })

    if not stream_objs:
        fallback = m["blv_sources"][0]["detail_url"] if m["blv_sources"] else BASE_URL+"/"
        stream_objs.append({
            "id":"fallback","name":"Trực tiếp",
            "stream_links":[{
                "id":"lnk0","name":"Link 1","type":"iframe","default":True,
                "url": fallback,
                "request_headers":[
                    {"key":"Referer","value":fallback},
                    {"key":"User-Agent","value":CHROME_UA},
                ],
            }],
        })

    # ── Thumbnail ──
    # Thứ tự ưu tiên:
    # 1. Dùng 2 logo URL từ card/detail → tạo thumbnail ghép bằng Pillow
    # 2. Thumbnail ảnh lớn trực tiếp từ trang (nếu có)
    # 3. Placeholder icon site
    la = m.get("logo_a_url", "")
    lb = m.get("logo_b_url", "")

    _BAD_THUMB = ("opengraph","favicon","og-image","og_image","site-logo","/favicon.")
    thumb_ok   = bool(m.get("thumbnail") and m["thumbnail"].startswith("http")
                      and not any(b in m["thumbnail"].lower() for b in _BAD_THUMB))

    if _PILLOW_OK and (la or lb):
        # Luôn tạo thumbnail ghép 2 logo nếu Pillow khả dụng
        jpeg_uri = make_thumbnail_b64(
            home_team  = m["home_team"],
            away_team  = m["away_team"],
            logo_a_url = la,
            logo_b_url = lb,
            time_str   = m.get("time_str",""),
            date_str   = m.get("date_str",""),
            status     = m["status"],
            score      = score,
            league     = league,
        )
        img_obj = {"padding":0,"background_color":"#0f3460","display":"cover",
                   "url": jpeg_uri, "width":800, "height":450} if jpeg_uri else PLACEHOLDER_IMG
    elif thumb_ok:
        img_obj = {"padding":0,"background_color":"#000000","display":"cover",
                   "url": m["thumbnail"], "width":1600, "height":1200}
    else:
        img_obj = PLACEHOLDER_IMG

    content_name = name
    if league and len(league.strip()) < 50:
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


# ══════════════════════════════════════════════════════════════
#  Build JSON output
# ══════════════════════════════════════════════════════════════

def build_json(channels: list, now_str: str, site_icon: str) -> dict:
    return {
        "id":          "cauthutv-live",
        "name":        "CauThu TV - Trực tiếp thể thao",
        "url":         BASE_URL + "/",
        "description": f"Cập nhật lúc {now_str}",
        "disable_ads": True,
        "color":       "#0f3460",
        "grid_number": 3,
        "image": {
            "type":          "cover",
            "url":           site_icon,
            "fallback_urls": [u for u in SITE_ICON_CANDIDATES if u != site_icon],
        },
        "groups": [{
            "id":       "tran-hot",
            "name":     "🔥 Trận HOT",
            "image":    None,
            "channels": channels,
        }],
    }


# ══════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser(description="Crawler cauthutv.shop — chỉ Trận HOT")
    ap.add_argument("--no-stream", action="store_true", help="Không crawl stream")
    ap.add_argument("--debug",     action="store_true", help="Lưu HTML để phân tích")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    args = ap.parse_args()

    log("\n" + "═"*62)
    log("  🔥  CRAWLER — cauthutv.shop  v3  (CHỈ TRẬN HOT)")
    log("  🖼  Thumbnail: ghép 2 logo đội từ card HTML")
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
        with open(DEBUG_HTML, "w", encoding="utf-8") as f: f.write(html)
        log(f"  💾 Đã lưu HTML → {DEBUG_HTML}")

    bs = BeautifulSoup(html, "lxml")

    # Icon trang
    site_icon = detect_site_icon(html, bs)
    log(f"  🖼  Icon trang: {site_icon}")

    log("\n🔍 Tìm mục Trận HOT...")
    hot_section = find_hot_section(bs)
    if not hot_section:
        log("❌ Không tìm thấy mục Trận HOT.")
        if not args.debug:
            log("  💡 Thử --debug để lưu HTML và kiểm tra cấu trúc.")
        sys.exit(1)

    # Parse cards
    raw, seen_urls = [], set()
    VS_RE = re.compile(r"\bvs\b|\blive\b|:\d{2}|trực tiếp", re.I)
    for a in hot_section.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        if not VS_RE.search(text): continue
        m = parse_card(a)
        if m and m["detail_url"] not in seen_urls:
            seen_urls.add(m["detail_url"])
            raw.append(m)

    matches = merge_matches(raw)
    log(f"\n  ✅ {len(raw)} card → gộp còn {len(matches)} trận HOT\n")

    if not matches:
        log("❌ Không tìm thấy trận nào trong mục HOT.")
        sys.exit(1)

    # Crawl stream + logo chi tiết + tạo thumbnail
    log("🖼  Crawl streams + tạo thumbnail 2 logo...")
    channels = []
    for i, m in enumerate(matches, 1):
        all_streams = []

        if not args.no_stream:
            for src in m.get("blv_sources", []):
                streams, la, lb = crawl_detail(src["detail_url"], src["blv"], scraper)
                if not m["logo_a_url"] and la: m["logo_a_url"] = la
                if not m["logo_b_url"] and lb: m["logo_b_url"] = lb
                seen_u = {s["url"] for s in all_streams}
                all_streams.extend(s for s in streams if s["url"] not in seen_u)
            time.sleep(0.4)

        log(f"  [{i:03d}] {m['base_title'][:45]}"
            f"  streams={len(all_streams)}"
            f"  logo_a={'✓' if m['logo_a_url'] else '✗'}"
            f"  logo_b={'✓' if m['logo_b_url'] else '✗'}")

        channels.append(build_channel(m, all_streams, i))

    result = build_json(channels, now_str, site_icon)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    log(f"\n{'═'*62}")
    log(f"  ✅ Xong!  📁 {args.output}  {len(channels)} trận HOT")
    log(f"  🕐 {now_str}")
    log("═"*62 + "\n")

if __name__ == "__main__":
    main()
