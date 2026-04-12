#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   Crawler cauthutv.shop  v6  — PRODUCTION                   ║
║   Cấu trúc thực tế (từ HTML debug):                         ║
║     Section HOT: <div id="live-score-game-hot">             ║
║     Card:        <div class="card-single">                   ║
║     Link:        <a aria-label="TeamA vs TeamB" href="/slug">║
║     Logo:        <img class="img-lazy" data-src="...">       ║
║     League:      <span class="...tracking-wider...">         ║
║     Time:        <span class="...tracking-widest...">        ║
║     BLV:         text "BLV Tên" trong card                   ║
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
#  Parse datetime
# ═══════════════════════════════════════════════════════

def parse_datetime(time_str, date_str):
    """Ghép time + date → (time_str, date_str, sort_key)."""
    if not time_str: return "", "", ""
    # Chuẩn hóa time
    tm = re.match(r'(\d{1,2}):(\d{2})', time_str.strip())
    if not tm: return "", "", ""
    hh, mm = tm.group(1).zfill(2), tm.group(2)
    if not (int(hh) <= 23 and int(mm) <= 59): return "", "", ""

    # Chuẩn hóa date
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
    Dựa trên cấu trúc HTML thực tế của cauthutv.shop.
    """
    # ── Link + tên đội từ aria-label ──────────────────
    a = card_div.find("a", href=True)
    if not a: return None

    href = a.get("href","")
    if not href: return None
    detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)

    # aria-label chứa "TeamA vs TeamB" — nguồn chính xác nhất
    aria = a.get("aria-label","").strip()
    home = away = ""
    if " vs " in aria:
        parts = aria.split(" vs ", 1)
        home = parts[0].strip()
        away = parts[1].strip()

    if not home or not away:
        return None  # bỏ qua card không có tên đội

    # ── Giải đấu ──────────────────────────────────────
    # <span class="...tracking-wider...">
    league_el = card_div.find("span", class_=lambda c: c and "tracking-wider" in c)
    league = league_el.get_text(strip=True) if league_el else ""

    # ── Giờ thi đấu ───────────────────────────────────
    # <span class="...tracking-widest...text-white...">12:30</span>
    time_el = card_div.find("span", class_=lambda c: c and "tracking-widest" in c)
    time_raw = time_el.get_text(strip=True) if time_el else ""

    # ── Ngày ─────────────────────────────────────────
    # <span class="...text-gray-400...">12/04</span>
    date_raw = ""
    for span in card_div.find_all("span", class_=lambda c: c and "text-gray-400" in c):
        t = span.get_text(strip=True)
        if re.match(r'\d{1,2}/\d{2}', t):
            date_raw = t
            break

    t_str, d_str, sort_k = parse_datetime(time_raw, date_raw)

    # ── Trạng thái ────────────────────────────────────
    raw_text = card_div.get_text(" ", strip=True)
    if re.search(r'\bLIVE\b|\bĐang Live\b|\bHiệp\s+\d|\bPT\s+\d', raw_text, re.I):
        status = "live"
    elif re.search(r'Kết thúc|Finished|\bFT\b', raw_text, re.I):
        status = "finished"
    else:
        status = "upcoming"

    # ── Logo ─────────────────────────────────────────
    # <img class="...img-lazy..." data-src="URL_hoặc_base64">
    logos = []
    for img in card_div.find_all("img", class_=lambda c: c and "img-lazy" in c):
        src = img.get("data-src") or img.get("src","")
        if src:
            # Chỉ lấy HTTP URL (bỏ base64 — quá nặng, không ổn định)
            if src.startswith("http"):
                logos.append(src)
            # base64 → bỏ qua, sẽ dùng fallback chữ tắt

    home_logo = logos[0] if len(logos) >= 1 else ""
    away_logo = logos[1] if len(logos) >= 2 else ""

    # ── BLV ───────────────────────────────────────────
    blv = ""
    blv_m = re.search(r'BLV\s+(\S+)', raw_text)
    if blv_m:
        blv = blv_m.group(1)

    # ── data-type (sport) ─────────────────────────────
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
#  Tìm và parse section HOT
# ═══════════════════════════════════════════════════════

def extract_hot_matches(html, bs, debug=False):
    """
    Tìm <div id="live-score-game-hot"> và parse tất cả card-single bên trong.
    """
    # Cách 1: id chính xác
    hot_section = bs.find(id="live-score-game-hot")

    if not hot_section:
        # Cách 2: tìm theo text "Các Trận Hot"
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

    # Parse tất cả card-single
    cards = hot_section.find_all("div", class_="card-single")
    log(f"  ✅ Tìm thấy {len(cards)} card-single trong HOT section")

    if debug:
        # In class của section
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
    return matches

# ═══════════════════════════════════════════════════════
#  Crawl detail page → stream + logo
# ═══════════════════════════════════════════════════════

def crawl_detail(detail_url, blv, scraper):
    html = fetch_html(detail_url, scraper, retries=2)
    if not html: return [], {}
    bs   = BeautifulSoup(html, "lxml")

    # Cập nhật logo từ detail page (nếu có HTTP URL tốt hơn)
    info = {}
    logos = []
    for img in bs.find_all("img", class_=lambda c: c and "img-lazy" in c):
        src = img.get("data-src") or img.get("src","")
        if src and src.startswith("http"):
            logos.append(src)
    if len(logos) >= 1: info["home_logo"] = logos[0]
    if len(logos) >= 2: info["away_logo"] = logos[1]

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
    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.mpd(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add("DASH", m.group(1), "dash")
    for sc in bs.find_all("script"):
        c = sc.string or ""
        for m in re.finditer(
                r'"(?:file|src|source|stream|url|hls|videoUrl|streamUrl)"\s*:\s*"(https?://[^"]+)"', c):
            u = m.group(1)
            if re.search(r"m3u8|stream|live|video|play", u, re.I):
                add("config", u, "hls")

    if not streams:
        streams.append({"name":"Trực tiếp","url":detail_url,"type":"iframe",
                        "referer":detail_url,"blv":blv})
    return streams, info

# ═══════════════════════════════════════════════════════
#  Thumbnail
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
    if not url or not _PIL: return None
    try:
        r = requests.get(url.strip(), timeout=8,
                        headers={"User-Agent":"Mozilla/5.0"}, stream=True)
        r.raise_for_status()
        if "html" in r.headers.get("content-type",""): return None
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

    # Gradient nền
    for y in range(H):
        t = y/H
        draw.line([(0,y),(W,y)], fill=(int(8+12*t), int(16+20*t), int(40+35*t)))

    # Accent line trên
    draw.rectangle([(0,0),(W,4)], fill=(255,140,0))

    # Bar giải đấu
    draw.rectangle([(0,4),(W,52)], fill=(0,0,0))
    if league:
        draw.text((W//2, 28), league[:44], fill=(255,200,50),
                  font=_font(20), anchor="mm")
    draw.line([(0,52),(W,52)], fill=(255,140,0,80), width=1)

    CTOP = 60; CBOT = H-62
    LMAX = min((CBOT-CTOP-28-4), 155)
    CX=W//2; LX=145; RX=W-145
    LY = CTOP + (CBOT-CTOP-28)//2 + 4
    NY = CBOT - 4

    def draw_logo(cx, cy, url, name):
        logo = fetch_logo(url, LMAX*3) if url else None
        if logo:
            if logo.mode != "RGBA": logo = logo.convert("RGBA")
            lw,lh = logo.size
            scale = min((LMAX-4)/lw, (LMAX-4)/lh, 1.0)
            nw,nh = max(1,int(lw*scale)), max(1,int(lh*scale))
            logo  = logo.resize((nw,nh), Image.LANCZOS)
            ox,oy = cx-nw//2, cy-nh//2
            canvas.paste(logo.convert("RGB"), (ox,oy), logo.split()[3])
        else:
            sz = LMAX*3//4
            draw.rectangle([(cx-sz//2,cy-sz//2),(cx+sz//2,cy+sz//2)],
                           fill=(20,40,80), outline=(80,120,200), width=2)
            init = "".join(w[0].upper() for w in (name or "?").split()[:2]) or "?"
            draw.text((cx,cy), init, fill=(160,200,255), font=_font(44), anchor="mm")

        short = (name or "?")
        if len(short) > 20: short = short[:19]+"…"
        draw.text((cx+1,NY+1), short, fill=(0,0,0), font=_font(17), anchor="mm")
        draw.text((cx,NY), short, fill=(240,240,240), font=_font(17), anchor="mm")

    draw_logo(LX, LY, home_logo_url, home_team)
    draw_logo(RX, LY, away_logo_url, away_team)

    # Giữa
    if status == "live":
        l1,c1,l2,c2,f1 = "● LIVE",(255,70,70),"","",38
    else:
        l1,c1 = time_str or "VS",(255,255,255)
        l2,c2 = date_str or "",(160,160,160)
        f1 = 46

    draw.line([(LX+LMAX//2+8,LY),(CX-36,LY)], fill=(255,255,255,40), width=1)
    draw.line([(CX+36,LY),(RX-LMAX//2-8,LY)], fill=(255,255,255,40), width=1)
    draw.text((CX+1,LY+1), l1, fill=(0,0,0,160), font=_font(f1), anchor="mm")
    draw.text((CX,LY),     l1, fill=c1,           font=_font(f1), anchor="mm")
    if l2:
        draw.text((CX,LY+38), l2, fill=c2, font=_font(16,False), anchor="mm")

    # Footer
    draw.rectangle([(0,H-50),(W,H)], fill=(0,0,0))
    draw.line([(0,H-50),(W,H-50)], fill=(255,140,0,100), width=1)

    buf = io.BytesIO()
    canvas.save(buf, format="JPEG", quality=87, optimize=True)
    return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()

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

    # Streams
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
    if _PIL and (la or lb or m.get("home_team")):
        uri = make_thumbnail(
            m.get("home_team",""), m.get("away_team",""),
            la, lb, m.get("time_str",""), m.get("date_str",""),
            status, league,
        )
        img_obj = ({"padding":0,"background_color":"#0f3460","display":"cover",
                    "url":uri,"width":800,"height":450} if uri else PLACEHOLDER)
    else:
        img_obj = PLACEHOLDER

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
    ap.add_argument("--no-stream", action="store_true", help="Bỏ qua crawl stream")
    ap.add_argument("--debug",     action="store_true", help="Lưu HTML debug")
    ap.add_argument("--output",    default=OUTPUT_FILE)
    args = ap.parse_args()

    log("\n" + "═"*62)
    log("  🔥  CRAWLER cauthutv.shop  v6  — PRODUCTION")
    log("  📌  id='live-score-game-hot' → card-single → aria-label")
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
    log(f"\n  ✅ {len(matches)} trận HOT\n")

    # Crawl detail
    log("🖼  Crawl detail + tạo thumbnail...")
    channels = []
    for i, m in enumerate(matches, 1):
        all_streams = []

        if not args.no_stream:
            for src in m.get("blv_sources",[]):
                streams, info = crawl_detail(src["detail_url"], src.get("blv",""), scraper)
                # Cập nhật logo từ detail nếu tốt hơn
                if info.get("home_logo") and not m["home_logo"]:
                    m["home_logo"] = info["home_logo"]
                if info.get("away_logo") and not m["away_logo"]:
                    m["away_logo"] = info["away_logo"]
                seen_u = {s["url"] for s in all_streams}
                all_streams.extend(s for s in streams if s["url"] not in seen_u)
            time.sleep(0.3)

        log(f"  [{i:03d}] {m.get('base_title','?')[:42]:42s} | "
            f"{'🔴' if m.get('status')=='live' else '🕐'} | "
            f"logo={'✓' if m.get('home_logo') else '✗'}{'✓' if m.get('away_logo') else '✗'} | "
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
