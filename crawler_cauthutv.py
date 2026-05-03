#!/usr/bin/env python3
"""Crawler cauthutv.shop — gọn nhẹ.
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
    sys.exit("pip install cloudscraper beautifulsoup4 lxml requests pillow")

try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL = True
except ImportError:
    _PIL = False

BASE_URL    = "https://cauthutv.shop"
OUTPUT_FILE = "cauthutv_iptv.json"
THUMB_DIR   = "thumbnails"
CHROME_UA   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
VN_TZ       = timezone(timedelta(hours=7))
SITE_ICON   = f"{BASE_URL}/assets/image/favicon64.png"
PLACEHOLDER = {"padding":0,"background_color":"#0f3460","display":"cover",
               "url":SITE_ICON,"width":512,"height":512}
DEBUG_HTML  = "debug_cauthutv.html"

def log(*a): print(*a, flush=True)

def _cdn_base():
    if o := os.environ.get("THUMB_CDN_BASE","").rstrip("/"): return o
    repo = os.environ.get("GITHUB_REPOSITORY","")
    branch = os.environ.get("GITHUB_REF_NAME","main")
    return f"https://raw.githubusercontent.com/{repo}/{branch}/{THUMB_DIR}" if repo else ""

def make_scraper():
    sc = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","mobile":False})
    sc.headers.update({"Accept-Language":"vi-VN,vi;q=0.9","Referer":BASE_URL+"/"})
    return sc

def fetch_html(url, scraper, retries=3):
    for i in range(retries):
        try:
            r = scraper.get(url, timeout=25, allow_redirects=True)
            r.raise_for_status()
            log(f"  + {url[:80]}")
            return r.text
        except Exception:
            if i < retries-1: time.sleep(2**i)
    return None

def to_webp_url(url):
    if not url: return url
    if re.search(r'\.webp(\?|$)|pub-[a-f0-9]+\.r2\.dev', url): return url
    return url + ("&" if "?" in url else "?") + "format=webp"

def parse_datetime(t_str, d_str):
    if not t_str: return "","",""
    tm = re.match(r'(\d{1,2}):(\d{2})', t_str.strip())
    if not tm: return "","",""
    hh, mm = tm.group(1).zfill(2), tm.group(2)
    dm = re.match(r'(\d{1,2})/(\d{2})', (d_str or "").strip())
    if dm:
        day, mon = dm.group(1).zfill(2), dm.group(2)
        return f"{hh}:{mm}", f"{day}/{mon}", f"{mon}-{day} {hh}:{mm}"
    today = datetime.now(VN_TZ)
    return f"{hh}:{mm}", today.strftime("%d/%m"), f"{today.strftime('%m-%d')} {hh}:{mm}"

def parse_card(card):
    a = card.find("a", href=True)
    if not a: return None
    href = a.get("href","")
    if not href: return None
    detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)
    aria = a.get("aria-label","")
    if " vs " not in aria: return None
    home, away = (p.strip() for p in aria.split(" vs ", 1))
    if not home or not away: return None

    league = ""
    if el := card.find("span", class_=lambda c: c and "tracking-wider" in c):
        league = el.get_text(strip=True)
    time_raw = ""
    if el := card.find("span", class_=lambda c: c and "tracking-widest" in c):
        time_raw = el.get_text(strip=True)
    date_raw = ""
    for sp in card.find_all("span", class_=lambda c: c and "text-gray-400" in c):
        t = sp.get_text(strip=True)
        if re.match(r'\d{1,2}/\d{2}', t): date_raw = t; break

    t_str, d_str, sort_k = parse_datetime(time_raw, date_raw)
    raw = card.get_text(" ", strip=True)
    status = ("live" if re.search(r'\bLIVE\b|\bHiep\s*\d|\bPT\s*\d', raw, re.I)
              else "finished" if re.search(r'Ket thuc|Finished|\bFT\b', raw, re.I)
              else "upcoming")

    _SKIP = ("/bgs/","/bg_","/bg-","bg-soccer","bg-volleyball","/icon-sports/","/background")
    logos = []
    for img in card.find_all("img", class_=lambda c: c and "img-lazy" in c):
        src = img.get("data-src") or img.get("src","")
        if src and src.startswith("http") and not any(s in src for s in _SKIP):
            if src.endswith("/image/s"): src += "mall"
            logos.append(to_webp_url(src))

    blv = ""
    if bc := card.find("div", class_=lambda c: c and "flex" in c and "gap-2" in c):
        if m := re.match(r'BLV\s+(.+)', bc.get_text(" ", strip=True)):
            blv = m.group(1).strip()
    if not blv:
        if m := re.search(r'BLV\s+([\w\s\(\)\.]+?)(?:\s*FB88|\s*DEBET|\s*XEM)', raw):
            blv = m.group(1).strip()

    return {"base_title":f"{home} vs {away}", "home_team":home, "away_team":away,
            "status":status, "league":league, "sport":card.get("data-type",""),
            "time_str":t_str, "date_str":d_str, "sort_key":sort_k, "detail_url":detail_url,
            "home_logo":logos[0] if logos else "",
            "away_logo":logos[1] if len(logos)>1 else "",
            "blv":blv, "blv_sources":[{"blv":blv,"detail_url":detail_url}]}

def merge_matches(matches):
    seen, result = {}, []
    for m in matches:
        key = m["base_title"].strip().lower()
        if key in seen:
            ex = result[seen[key]]
            ex_urls = {s["detail_url"] for s in ex["blv_sources"]}
            for src in m["blv_sources"]:
                if src["detail_url"] not in ex_urls:
                    ex["blv_sources"].append(src); ex_urls.add(src["detail_url"])
            if not ex.get("home_logo") and m.get("home_logo"): ex["home_logo"] = m["home_logo"]
            if not ex.get("away_logo") and m.get("away_logo"): ex["away_logo"] = m["away_logo"]
            if m.get("status") == "live": ex["status"] = "live"
        else:
            seen[key] = len(result); result.append(m)
    merged = len(matches) - len(result)
    if merged: log(f"  Merge {merged} tran trung -> {len(result)} tran")
    return result

def extract_hot_matches(html, bs, debug=False):
    sec = bs.find(id="live-score-game-hot")
    if not sec:
        for node in bs.find_all(string=lambda t: t and "Cac Tran Hot" in t):
            p = node.parent
            for _ in range(6):
                if not p: break
                if p.find_all("div", class_="card-single"): sec = p; break
                p = p.parent
            if sec: break
    if not sec: return []
    cards = sec.find_all("div", class_="card-single")
    matches = [m for c in cards if (m := parse_card(c))]
    log(f"  {len(matches)}/{len(cards)} tran hop le")
    return merge_matches(matches)

def _label_m3u8(url):
    return "HD" if re.search(r'index\.m3u8', url, re.I) else "Nha Dai"

def _extract_m3u8(html, bs, page_url, blv, seen):
    out = []
    def add(url):
        url = url.strip()
        if not url or url in seen or ".m3u8" not in url.lower(): return
        lbl = _label_m3u8(url)
        if lbl == "Nha Dai" and re.search(r'livehd', url, re.I): return
        seen.add(url)
        out.append({"name": "HD" if lbl=="HD" else "Nha Dai SD",
                    "url":url,"type":"hls","referer":page_url,"blv":blv})

    for m in re.finditer(r'(https?://[^\s\'"<>\]\\]+\.m3u8(?:[?#][^\s\'"<>\]\\]*)?)', html):
        add(m.group(1))
    for sc in bs.find_all("script"):
        c = sc.string or ""
        for m in re.finditer(r'"(?:file|src|url|hls|streamUrl|hlsUrl)"\s*:\s*"(https?://[^"]*\.m3u8[^"]*)"', c):
            add(m.group(1))
        for m in re.finditer(r'(?:streamUrl|videoUrl|hlsUrl)\s*[=:]\s*["\']([^"\']+\.m3u8[^"\']*)["\']', c):
            add(m.group(1))
        for m in re.finditer(r'(?:playerConfig|PLAYER_CONFIG)\s*=\s*(\{[^;]+\})', c, re.S):
            try:
                cfg = json.loads(m.group(1))
                srcs = cfg.get("sources", cfg.get("source",[]))
                if isinstance(srcs, str): srcs=[{"src":srcs}]
                for s in (srcs if isinstance(srcs,list) else []):
                    u = s.get("src","") or s.get("file","") or s.get("url","")
                    if u and ".m3u8" in u.lower(): add(u)
            except: pass
        for m in re.finditer(r'sources\s*:\s*\[([^\]]+)\]', c, re.S):
            for sm in re.finditer(r'(https?://[^\s\'"]+\.m3u8[^\s\'"]*)', m.group(1)): add(sm.group(1))
    return out

def crawl_detail(detail_url, blv, scraper):
    base = re.sub(r'\?.*$', '', detail_url.strip())
    info, seen, streams = {}, set(), []
    html = fetch_html(base, scraper, retries=2)
    if html:
        bs = BeautifulSoup(html, "lxml")
        if r2 := re.findall(r'https://pub-[a-f0-9]+\.r2\.dev/[^\s\'"<>]+\.(?:webp|jpg|png)', html, re.I):
            info["thumb_url"] = r2[0]
        logos = []
        for img in bs.find_all("img", class_=lambda c: c and "img-lazy" in c):
            src = img.get("data-src") or img.get("src","")
            if src and src.startswith("http") and not any(s in src for s in ("/bgs/","/icon-sports/","/bg_","/background")):
                logos.append(to_webp_url(src))
        if logos: info["home_logo"] = logos[0]
        if len(logos)>1: info["away_logo"] = logos[1]
        streams.extend(_extract_m3u8(html, bs, base, blv, seen))
    # ndsd mode only
    url_m = f"{base}?mode=ndsd"
    if html_m := fetch_html(url_m, scraper, retries=2):
        bs_m = BeautifulSoup(html_m, "lxml")
        new = _extract_m3u8(html_m, bs_m, url_m, blv, seen)
        streams.extend(new)
        if new: log(f"    +{len(new)} (ndsd)")
    time.sleep(0.2)
    streams.sort(key=lambda s: (0 if s["name"]=="HD" else 1))
    return streams, info

# Theme: (bg, accent)
_THEMES = {
    "soccer":    ((232,245,232),(27,122,27)),
    "basketball":((255,244,224),(200,85,0)),
    "tennis":    ((245,240,210),(60,130,60)),
    "volleyball":((224,238,255),(20,90,180)),
    "esports":   ((18,18,32),(130,60,220)),
    "boxing":    ((255,235,235),(180,20,20)),
    "badminton": ((240,250,255),(0,140,180)),
    "golf":      ((240,248,224),(30,100,30)),
    "racing":    ((240,240,240),(180,0,0)),
    "default":   ((240,244,252),(30,70,160)),
}

def _sport_key(sport, league):
    raw = (sport+" "+league).lower()
    if re.search(r'soccer|football|bong.?da|futsal|v\.?league|laliga|premier|bundesliga|serie|ligue|champion|cup|euro|afc', raw): return "soccer"
    if re.search(r'basketball|bong.?ro|nba', raw): return "basketball"
    if re.search(r'tennis|atp|wta', raw): return "tennis"
    if re.search(r'volleyball|bong.?chuyen|vnl', raw): return "volleyball"
    if re.search(r'esport|lol|dota|csgo|valorant|pubg|gaming|lck|lpl|lcs|vcs|\bkt\b|rolster|t1|gen\.g|faker', raw): return "esports"
    if re.search(r'boxing|mma|ufc|muay', raw): return "boxing"
    if re.search(r'badminton|cau.?long', raw): return "badminton"
    if re.search(r'golf|pga', raw): return "golf"
    if re.search(r'formula|f1|motogp|racing', raw): return "racing"
    return "default"

def _team_palette(home, away, bg, accent):
    seed = int(hashlib.md5(f"{home}|{away}".lower().encode()).hexdigest(), 16)
    def sh(v,d): return max(0,min(255,v+d))
    dr=(seed>>0&0xFF)%41-20; dg=(seed>>8&0xFF)%41-20; db=(seed>>16&0xFF)%41-20
    bg2=(sh(bg[0],dr),sh(bg[1],dg),sh(bg[2],db))
    da=(seed>>24&0xFF)%21-10
    ac2=(sh(accent[0],da),sh(accent[1],da//2),sh(accent[2],-da//2))
    return bg2, ac2

def _font(size, bold=True):
    if not _PIL: return None
    for p in [f"/usr/share/fonts/truetype/dejavu/DejaVuSans{'-Bold' if bold else ''}.ttf",
              f"/usr/share/fonts/truetype/liberation/LiberationSans-{'Bold' if bold else 'Regular'}.ttf"]:
        try: return ImageFont.truetype(p, size)
        except: pass
    return ImageFont.load_default()

def _load_image_from_url(url, max_px=400):
    """Download and return PIL RGBA image from url, or None on failure."""
    if not url or not _PIL: return None
    try:
        r = requests.get(url.strip(), timeout=8,
                         headers={"User-Agent":"Mozilla/5.0","Accept":"image/webp,image/*,*/*"}, stream=True)
        r.raise_for_status()
        ct = r.headers.get("content-type","")
        if "html" in ct and "svg" not in ct: return None
        data = b""
        for chunk in r.iter_content(65536):
            data += chunk
            if len(data) > 3_000_000: break
        img = Image.open(io.BytesIO(data)).convert("RGBA")
        img.thumbnail((max_px, max_px), Image.LANCZOS)
        return img
    except: return None

def _trim_whitespace(img):
    """Cắt bỏ vùng trắng/trong suốt xung quanh logo để 2 logo có kích thước thật đồng đều.
    Xử lý được PNG alpha và ảnh JPG/WebP nền trắng."""
    if not _PIL or img is None: return img
    rgba = img.convert("RGBA")
    r_ch, g_ch, b_ch, a_ch = rgba.split()
    try:
        import numpy as np
        arr_a = np.array(a_ch)
        arr_r = np.array(r_ch); arr_g = np.array(g_ch); arr_b = np.array(b_ch)
        # Pixel "trống" = trong suốt (alpha<10) HOẶC gần trắng (r,g,b > 240)
        empty = (arr_a < 10) | ((arr_r > 240) & (arr_g > 240) & (arr_b > 240))
        non_empty = ~empty
        rows = np.any(non_empty, axis=1)
        cols = np.any(non_empty, axis=0)
        if not rows.any() or not cols.any(): return img
        rmin, rmax = int(np.where(rows)[0][0]),  int(np.where(rows)[0][-1])
        cmin, cmax = int(np.where(cols)[0][0]),  int(np.where(cols)[0][-1])
    except ImportError:
        # Fallback: dùng PIL getbbox trên alpha channel
        bbox = a_ch.getbbox()
        if not bbox: return img
        cmin, rmin, cmax, rmax = bbox[0], bbox[1], bbox[2]-1, bbox[3]-1
    # Padding 4px mỗi phía để logo không bị cắt sát
    pad = 4
    rmin = max(0, rmin - pad); rmax = min(img.height - 1, rmax + pad)
    cmin = max(0, cmin - pad); cmax = min(img.width  - 1, cmax + pad)
    if cmax <= cmin or rmax <= rmin: return img
    return img.crop((cmin, rmin, cmax + 1, rmax + 1))

# ── Country-code map: tên quốc gia → ISO 3166-1 alpha-2 (lower) ──
_COUNTRY_CODES = {
    "vietnam":"vn","viet nam":"vn","việt nam":"vn",
    "china":"cn","trung quoc":"cn","trung quốc":"cn",
    "france":"fr","pháp":"fr","phap":"fr",
    "japan":"jp","nhat ban":"jp","nhật bản":"jp",
    "south korea":"kr","korea":"kr","han quoc":"kr","hàn quốc":"kr",
    "north korea":"kp",
    "thailand":"th","thai lan":"th","thái lan":"th",
    "indonesia":"id",
    "malaysia":"my",
    "philippines":"ph",
    "singapore":"sg",
    "myanmar":"mm",
    "cambodia":"kh",
    "laos":"la",
    "australia":"au","uc":"au","úc":"au",
    "india":"in","an do":"in",
    "usa":"us","united states":"us","my":"us",
    "brazil":"br","brazil":"br",
    "argentina":"ar",
    "germany":"de","duc":"de","đức":"de",
    "spain":"es","tay ban nha":"es","tây ban nha":"es",
    "italy":"it","y":"it",
    "england":"gb-eng","great britain":"gb",
    "netherlands":"nl","ha lan":"nl","hà lan":"nl",
    "portugal":"pt","bo dao nha":"pt","bồ đào nha":"pt",
    "russia":"ru","nga":"ru",
    "ukraine":"ua",
    "poland":"pl","ba lan":"pl",
    "czech":"cz",
    "croatia":"hr",
    "sweden":"se","thuy dien":"se",
    "norway":"no","na uy":"no",
    "denmark":"dk","dan mach":"dk",
    "belgium":"be","bi":"be","bỉ":"be",
    "switzerland":"ch","thuy si":"ch","thụy sĩ":"ch",
    "austria":"at","ao":"at",
    "turkey":"tr","tho nhi ky":"tr","thổ nhĩ kỳ":"tr",
    "iran":"ir",
    "saudi arabia":"sa","a rap xeut":"sa","ả rập xê út":"sa",
    "qatar":"qa",
    "uae":"ae","united arab emirates":"ae",
    "iraq":"iq",
    "mexico":"mx","mexico":"mx",
    "colombia":"co",
    "chile":"cl",
    "uruguay":"uy",
    "nigeria":"ng",
    "ghana":"gh",
    "egypt":"eg","ai cap":"eg","ai cập":"eg",
    "morocco":"ma","maroc":"ma",
    "senegal":"sn",
    "cameroon":"cm",
    "canada":"ca",
    "new zealand":"nz",
    "scotland":"gb-sct",
    "wales":"gb-wls",
    "ireland":"ie",
}

# ── Esports / NBA / football team logo map ──
_TEAM_LOGO_URLS = {
    # eSports
    "kt":          "https://lol.fandom.com/wiki/Special:FilePath/KT_Rolster_2023.png",
    "kt rolster":  "https://lol.fandom.com/wiki/Special:FilePath/KT_Rolster_2023.png",
    "kt esport":   "https://lol.fandom.com/wiki/Special:FilePath/KT_Rolster_2023.png",
    "gen.g":       "https://lol.fandom.com/wiki/Special:FilePath/Gen.G_2024.png",
    "gen g":       "https://lol.fandom.com/wiki/Special:FilePath/Gen.G_2024.png",
    "geng":        "https://lol.fandom.com/wiki/Special:FilePath/Gen.G_2024.png",
    "gen.g esports":"https://lol.fandom.com/wiki/Special:FilePath/Gen.G_2024.png",
    "t1":          "https://lol.fandom.com/wiki/Special:FilePath/T1_2022.png",
    "bro":         "https://lol.fandom.com/wiki/Special:FilePath/BRION_2024.png",
    "brion":       "https://lol.fandom.com/wiki/Special:FilePath/BRION_2024.png",
    "dns":         "https://lol.fandom.com/wiki/Special:FilePath/DRX_2023.png",
    "drx":         "https://lol.fandom.com/wiki/Special:FilePath/DRX_2023.png",
    "bfx":         "https://lol.fandom.com/wiki/Special:FilePath/Bilibili_Gaming_2024.png",
    "cloud9":      "https://lol.fandom.com/wiki/Special:FilePath/Cloud9_2021.png",
    "c9":          "https://lol.fandom.com/wiki/Special:FilePath/Cloud9_2021.png",
    "fnatic":      "https://lol.fandom.com/wiki/Special:FilePath/Fnatic_2021.png",
    "g2":          "https://lol.fandom.com/wiki/Special:FilePath/G2_Esports_2022.png",
    "navi":        "https://lol.fandom.com/wiki/Special:FilePath/Natus_Vincere_2021.png",
    "evil geniuses":"https://lol.fandom.com/wiki/Special:FilePath/Evil_Geniuses_2023.png",
    "eg":          "https://lol.fandom.com/wiki/Special:FilePath/Evil_Geniuses_2023.png",
    # NBA teams — ESPN logo CDN (các URL đã xác nhận hoạt động)
    "lakers":           "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/lal.png",
    "los angeles lakers":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/lal.png",
    "celtics":          "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/bos.png",
    "boston celtics":   "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/bos.png",
    "warriors":         "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/gs.png",
    "golden state warriors":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/gs.png",
    "bulls":            "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/chi.png",
    "chicago bulls":    "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/chi.png",
    "heat":             "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mia.png",
    "miami heat":       "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mia.png",
    "nets":             "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/bkn.png",
    "brooklyn nets":    "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/bkn.png",
    "knicks":           "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/ny.png",
    "new york knicks":  "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/ny.png",
    "clippers":         "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/lac.png",
    "los angeles clippers":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/lac.png",
    "bucks":            "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mil.png",
    "milwaukee bucks":  "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mil.png",
    "76ers":            "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/phi.png",
    "philadelphia 76ers":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/phi.png",
    "suns":             "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/phx.png",
    "phoenix suns":     "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/phx.png",
    "mavericks":        "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/dal.png",
    "dallas mavericks": "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/dal.png",
    "nuggets":          "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/den.png",
    "denver nuggets":   "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/den.png",
    "thunder":          "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/okc.png",
    "oklahoma city thunder":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/okc.png",
    "raptors":          "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/tor.png",
    "toronto raptors":  "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/tor.png",
    "cavaliers":        "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/cle.png",
    "cleveland cavaliers":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/cle.png",
    "pistons":          "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/det.png",
    "detroit pistons":  "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/det.png",
    "magic":            "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/orl.png",
    "orlando magic":    "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/orl.png",
    "pacers":           "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/ind.png",
    "indiana pacers":   "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/ind.png",
    "hawks":            "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/atl.png",
    "atlanta hawks":    "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/atl.png",
    "wizards":          "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/wsh.png",
    "washington wizards":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/wsh.png",
    "hornets":          "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/cha.png",
    "charlotte hornets":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/cha.png",
    "timberwolves":     "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/min.png",
    "minnesota timberwolves":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/min.png",
    "jazz":             "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/utah.png",
    "utah jazz":        "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/utah.png",
    "pelicans":         "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/no.png",
    "new orleans pelicans":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/no.png",
    "grizzlies":        "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mem.png",
    "memphis grizzlies":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/mem.png",
    "rockets":          "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/hou.png",
    "houston rockets":  "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/hou.png",
    "spurs":            "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/sa.png",
    "san antonio spurs":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/sa.png",
    "kings":            "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/sac.png",
    "sacramento kings": "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/sac.png",
    "trail blazers":    "https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/por.png",
    "portland trail blazers":"https://a.espncdn.com/combiner/i?img=/i/teamlogos/nba/500/por.png",
}

def _get_country_flag_img(team_name):
    """Try to get flag image for country teams via flagcdn.com."""
    if not _PIL: return None
    key = team_name.lower().strip()
    # Remove prefixes like "U17 Women", "U20", etc.
    clean = re.sub(r'\s*(u\d+|women|men|national|team)\s*', ' ', key, flags=re.I).strip()
    code = _COUNTRY_CODES.get(clean) or _COUNTRY_CODES.get(key)
    if not code: return None
    # flagcdn.com provides SVG/PNG flags
    flag_url = f"https://flagcdn.com/w160/{code.replace('-','_')}.png"
    img = _load_image_from_url(flag_url, 300)
    if img:
        log(f"    [logo] flag {team_name} -> {flag_url}")
    return img

def _get_team_logo_img(team_name):
    """Look up team logo from the static map."""
    if not _PIL: return None
    key = team_name.lower().strip()
    url = _TEAM_LOGO_URLS.get(key)
    if not url:
        # Try partial match (e.g. "KT Rolster" → "kt rolster")
        for k, v in _TEAM_LOGO_URLS.items():
            if k in key or key in k:
                url = v; break
    if not url: return None
    img = _load_image_from_url(url, 400)
    if img:
        log(f"    [logo] static map {team_name} -> {url}")
    return img

def fetch_logo(url, max_px=390, team_name=""):
    """Fetch logo from URL; if missing/failed, try fallback sources."""
    if not _PIL: return None
    img = None
    if url:
        for try_url in ([to_webp_url(url), url] if to_webp_url(url)!=url else [url]):
            img = _load_image_from_url(try_url, max_px)
            if img: break
    if img is None and team_name:
        img = _get_team_logo_img(team_name)
    if img is None and team_name:
        img = _get_country_flag_img(team_name)
    return img

def make_thumbnail(home_team, away_team, home_logo_url, away_logo_url,
                   time_str="", date_str="", status="upcoming",
                   league="", sport="", blv_text=""):
    if not _PIL: return b""
    W, H = 820, 660   # Tăng chiều cao cho logo +40% (361px)
    key = _sport_key(sport, league)
    _, ac_base = _THEMES.get(key, _THEMES["default"])
    _, A = _team_palette(home_team, away_team, (245,245,248), ac_base)

    canvas = Image.new("RGB", (W, H), (255, 255, 255))   # nền trắng
    draw   = ImageDraw.Draw(canvas)

    # ── Layout: header giải đấu (80px) + body + footer nhỏ (30px) ──
    LEAGUE_H = 80    # đủ cao để chữ to + padding
    FOOTER_H = 30
    BODY_TOP = LEAGUE_H
    BODY_BOT = H - FOOTER_H
    BODY_H   = BODY_BOT - BODY_TOP
    CX = W // 2

    # ── Header: tên giải đấu căn giữa, font 32, màu accent ──
    draw.rectangle([(0,0),(W,LEAGUE_H)], fill=(255,255,255))
    if league:
        draw.text((CX, LEAGUE_H//2 + 4), league[:36],
                  fill=A, font=_font(32), anchor="mm")
    # Đường kẻ accent dưới header
    draw.rectangle([(0, LEAGUE_H-3),(W, LEAGUE_H)], fill=A)

    # ── Body: 2 logo + tên đội + hộp VS/LIVE ──
    # Logo tăng thêm 40% (so với lần trước 258 → 361); tên đội tăng 20%: 31 → 37
    NAME_H = 52; GAP = 16
    BW = 148

    LX = W//4; RX = 3*W//4
    MAX_HALF = (CX - BW//2 - 20) - LX
    # Tăng 40% so với 258 → 361, giới hạn bởi không gian thực tế
    LMAX = min(BODY_H - NAME_H - GAP - 16, MAX_HALF * 2, 361)
    LMAX = max(LMAX, 90)
    # LOGO_BOX: bounding box vuông dùng chung cho cả 2 logo → kích thước đồng đều
    LOGO_BOX = LMAX

    CONTENT_H = LOGO_BOX + GAP + NAME_H
    TOP_PAD   = (BODY_H - CONTENT_H) // 2
    LY   = BODY_TOP + TOP_PAD + LOGO_BOX // 2
    NY_Y = LY + LOGO_BOX // 2 + GAP + NAME_H // 2

    def draw_logo(cx, cy, url, name):
        # Thử lấy logo, fallback theo tên đội
        logo = fetch_logo(url, LOGO_BOX * 4, team_name=name)
        if logo:
            if logo.mode != "RGBA": logo = logo.convert("RGBA")
            # ── Trim vùng trắng/trong suốt trước khi scale ──
            # Giải quyết trường hợp logo VN nhỏ hơn TQ do padding trắng khác nhau
            logo = _trim_whitespace(logo)
            lw, lh = logo.size
            # Scale đồng đều vào bounding box LOGO_BOX × LOGO_BOX
            sc = min(LOGO_BOX / lw, LOGO_BOX / lh, 1.0)
            nw, nh = max(1, int(lw * sc)), max(1, int(lh * sc))
            logo = logo.resize((nw, nh), Image.LANCZOS)
            canvas.paste(logo.convert("RGB"), (cx - nw//2, cy - nh//2), logo.split()[3])
        else:
            R2 = LOGO_BOX // 2
            draw.ellipse([(cx-R2, cy-R2), (cx+R2, cy+R2)],
                         fill=(235,235,240), outline=A, width=2)
            draw.text((cx, cy), "".join(w[0].upper() for w in (name or "?").split()[:2]) or "?",
                      fill=A, font=_font(66), anchor="mm")
        # Tên đội — tăng 20%: 31 → 37
        draw.text((cx, NY_Y), (name or "?")[:22],
                  fill=(20,20,20), font=_font(37, bold=False), anchor="mm")

    draw_logo(LX, LY, home_logo_url, home_team)
    draw_logo(RX, LY, away_logo_url, away_team)

    # ── Hộp VS / LIVE giữa — font tăng 20% ──
    if status == "live":
        bbg, l1, l2, f1, f2 = (34,160,60), "LIVE", "", 29, 24
    else:
        bbg = (255,255,255)
        l1, l2 = time_str or "VS", date_str or ""
        f1, f2 = 34, 26   # 28×1.2≈34, 22×1.2≈26
    BH = 84 if l2 else 62   # 70×1.2=84, 52×1.2≈62
    BW2 = 176              # 148×1.2≈176
    bx0,by0 = CX-BW2//2, LY-BH//2
    bx1,by1 = CX+BW2//2, LY+BH//2
    draw.rounded_rectangle([(bx0,by0),(bx1,by1)], radius=16, fill=bbg, outline=A, width=3)
    if status == "live":
        draw.ellipse([(bx0+14,LY-8),(bx0+30,LY+8)], fill=(255,50,50))
        draw.text((CX+10, LY), "LIVE", fill=(255,255,255), font=_font(f1), anchor="mm")
    elif l2:
        draw.text((CX, by0+BH//2-f1//2-2), l1, fill=A, font=_font(f1), anchor="mm")
        draw.text((CX, by0+BH//2+f2//2+2), l2, fill=(100,100,100),
                  font=_font(f2,bold=False), anchor="mm")
    else:
        draw.text((CX, LY), l1, fill=A, font=_font(f1), anchor="mm")

    # ── Footer: đường kẻ accent ──
    draw.rectangle([(0,BODY_BOT),(W,H)], fill=(252,252,255))
    draw.rectangle([(0,H-3),(W,H)], fill=A)

    buf = io.BytesIO()
    canvas.save(buf, format="WEBP", quality=85, method=4)
    return buf.getvalue()

def save_thumbnail(raw, ch_id):
    if not raw: return ""
    cdn=_cdn_base()
    if cdn:
        p=Path(THUMB_DIR); p.mkdir(exist_ok=True)
        (p/f"{ch_id}.webp").write_bytes(raw)
        return f"{cdn}/{ch_id}.webp"
    return "data:image/webp;base64,"+base64.b64encode(raw).decode()

def make_id(*parts):
    return hashlib.md5("-".join(str(p) for p in parts).encode()).hexdigest()[:16]

def build_name(m):
    base=f"{m.get('home_team','')} vs {m.get('away_team','')}".strip() or m.get("base_title","")
    t,d,st=m.get("time_str",""),m.get("date_str",""),m.get("status","upcoming")
    if st=="live":     return f"{base}  LIVE"
    if st=="finished": return f"{base}  Ket thuc"
    if t and d: return f"{base}  {t} | {d}"
    return f"{base}  {t}" if t else base

def build_channel(m, all_streams, index):
    ch_id=make_id("ctt",index,re.sub(r"[^a-z0-9]","-",m.get("base_title","").lower())[:24])
    name=build_name(m); league=m.get("league",""); status=m.get("status","upcoming")
    sc_map={"live":{"text":"● Live","color":"#E73131","text_color":"#fff"},
            "finished":{"text":"Ket thuc","color":"#444","text_color":"#fff"}}
    labels=[]
    if status in sc_map:
        labels=[{**sc_map[status],"position":"top-left"}]
    blv_names=[s["blv"] for s in m.get("blv_sources",[]) if s.get("blv")]
    if len(blv_names)>1:
        labels.append({"text":f"🎙 {len(blv_names)} BLV","position":"bottom-left","color":"#1a8a2e","text_color":"#fff"})
    elif blv_names:
        labels.append({"text":f"🎙 {blv_names[0]}","position":"bottom-left","color":"#1a8a2e","text_color":"#fff"})
    blv_text=blv_names[0] if len(blv_names)==1 else (f"{len(blv_names)} BLV" if blv_names else "")
    blv_groups={}
    for s in all_streams:
        bk=s.get("blv") or "__"
        grp=blv_groups.setdefault(bk,[])
        if s["url"] not in {x["url"] for x in grp}: grp.append(s)
    stream_objs=[]
    for idx,(bk,raw_s) in enumerate(blv_groups.items()):
        if not raw_s: continue
        slabel=f"🎙 {bk}" if bk!="__" else f"Nguon {idx+1}"
        slinks=[{"id":make_id(ch_id,f"b{idx}",f"l{li}"),"name":s.get("name","Auto"),
                 "type":s["type"],"default":li==0,"url":s["url"],
                 "request_headers":[{"key":"Referer","value":s.get("referer",BASE_URL+"/")},
                                    {"key":"User-Agent","value":CHROME_UA}]}
                for li,s in enumerate(raw_s)]
        stream_objs.append({"id":make_id(ch_id,f"st{idx}"),"name":slabel,"stream_links":slinks})
    if not stream_objs:
        fb=(m.get("blv_sources",[{}])[0].get("detail_url",BASE_URL+"/") if m.get("blv_sources") else BASE_URL+"/")
        stream_objs.append({"id":"fb","name":"Truc tiep","stream_links":[{
            "id":"lnk0","name":"Link 1","type":"iframe","default":True,"url":fb,
            "request_headers":[{"key":"Referer","value":fb},{"key":"User-Agent","value":CHROME_UA}]}]})
    la,lb=m.get("home_logo",""),m.get("away_logo","")
    if thumb:=m.get("thumb_url",""):
        img_obj={"padding":0,"background_color":"#fff","display":"cover","url":thumb,"width":820,"height":660}
    elif _PIL:
        raw=make_thumbnail(m.get("home_team",""),m.get("away_team",""),la,lb,
                           m.get("time_str",""),m.get("date_str",""),
                           status,league,m.get("sport",""),blv_text)
        cdn_url=save_thumbnail(raw,ch_id)
        img_obj=({"padding":0,"background_color":"#fff","display":"cover","url":cdn_url,"width":820,"height":660}
                 if cdn_url else PLACEHOLDER)
    else:
        img_obj=PLACEHOLDER
    content_name=name+(f" . {league.strip()}" if league and len(league)<50 else "")
    has_multi=len(stream_objs)>1
    return {"id":ch_id,"name":name,"type":"single","display":"thumbnail-only",
            "enable_detail":has_multi,"image":img_obj,"labels":labels,
            "sources":[{"id":make_id(ch_id,"src"),"name":"CauThuTV Live",
                        "contents":[{"id":make_id(ch_id,"ct"),"name":content_name,"streams":stream_objs}]}]}

def build_json(channels,now_str):
    return {"id":"cauthutv-live","name":"CauThu TV - Trực tiếp thể thao",
            "url":BASE_URL+"/",
            "description":"Nền tảng xem thể thao trực tuyến hàng đầu Việt Nam. Trực tiếp bóng đá, bóng rổ, tennis, esports với bình luận tiếng Việt chất lượng cao.",
            "disable_ads":True,"color":"#0f3460","grid_number":2,
            "image":{"type":"cover","url":SITE_ICON},
            "groups":[{"id":"tran-hot","name":"Cac Tran Hot","display":"vertical",
                       "grid_number":2,"enable_detail":False,"image":None,"channels":channels}]}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--no-stream",action="store_true")
    ap.add_argument("--debug",action="store_true")
    ap.add_argument("--output",default=OUTPUT_FILE)
    args=ap.parse_args()
    log(f"\n{'='*50}\n  CRAWLER cauthutv.shop\n  CDN: {_cdn_base() or 'local'}\n{'='*50}\n")
    now_vn=datetime.now(VN_TZ)
    now_str=now_vn.strftime("%d/%m/%Y %H:%M ICT")
    scraper=make_scraper()
    log(f"Tai {BASE_URL}...")
    html=fetch_html(BASE_URL,scraper)
    if not html: sys.exit("Khong tai duoc trang chu")
    if "Just a moment" in html: sys.exit("Cloudflare challenge")
    if args.debug: Path(DEBUG_HTML).write_text(html,encoding="utf-8")
    bs=BeautifulSoup(html,"lxml")
    matches=extract_hot_matches(html,bs,debug=args.debug)
    if not matches: sys.exit("Khong tim thay tran nao")
    pri={"live":0,"upcoming":1,"finished":2}
    matches.sort(key=lambda x:(pri.get(x.get("status","upcoming"),9),x.get("sort_key","")))
    log(f"\n  {len(matches)} tran HOT\n")
    channels=[]
    for i,m in enumerate(matches,1):
        all_streams=[]
        if not args.no_stream:
            for src in m.get("blv_sources",[]):
                streams,info=crawl_detail(src["detail_url"],src.get("blv",""),scraper)
                if info.get("thumb_url") and not m.get("thumb_url"): m["thumb_url"]=info["thumb_url"]
                if info.get("home_logo") and not m.get("home_logo"): m["home_logo"]=info["home_logo"]
                if info.get("away_logo") and not m.get("away_logo"): m["away_logo"]=info["away_logo"]
                blv_key=src.get("blv") or "__"
                seen_per={s["url"] for s in all_streams if (s.get("blv") or "__")==blv_key}
                all_streams.extend(s for s in streams if s["url"] not in seen_per)
            time.sleep(0.3)
        log(f"  [{i:03d}] {m.get('base_title','?')[:38]:38s} streams={len(all_streams)}")
        channels.append(build_channel(m,all_streams,i))
    result=build_json(channels,now_str)
    with open(args.output,"w",encoding="utf-8") as f: json.dump(result,f,ensure_ascii=False,indent=2)
    if cdn:=_cdn_base():
        td=Path(THUMB_DIR)
        if td.exists():
            active={ch["id"] for ch in channels}
            for f in td.glob("*.webp"):
                if f.stem not in active: f.unlink(missing_ok=True)
    log(f"\n{'='*50}\n  {args.output} -- {len(channels)} tran | {now_str}\n{'='*50}\n")

if __name__=="__main__":
    main()
