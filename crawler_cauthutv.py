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
    if re.search(r'esport|lol|dota|csgo|valorant|pubg|gaming', raw): return "esports"
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

def fetch_logo(url, max_px=260):
    if not url or not _PIL: return None
    for try_url in ([to_webp_url(url), url] if to_webp_url(url)!=url else [url]):
        try:
            r = requests.get(try_url.strip(), timeout=7,
                             headers={"User-Agent":"Mozilla/5.0","Accept":"image/webp,image/*"}, stream=True)
            r.raise_for_status()
            if "html" in r.headers.get("content-type",""): continue
            data = b""
            for chunk in r.iter_content(65536):
                data += chunk
                if len(data)>2_000_000: break
            img = Image.open(io.BytesIO(data)).convert("RGBA")
            img.thumbnail((max_px, max_px), Image.LANCZOS)
            return img
        except: continue
    return None

def make_thumbnail(home_team, away_team, home_logo_url, away_logo_url,
                   time_str="", date_str="", status="upcoming",
                   league="", sport="", blv_text=""):
    if not _PIL: return b""
    W, H = 820, 540
    key = _sport_key(sport, league)
    _, ac_base = _THEMES.get(key, _THEMES["default"])
    # Nền trắng cố định — màu accent độc nhất mỗi trận
    _, A = _team_palette(home_team, away_team, (245,245,248), ac_base)

    canvas = Image.new("RGB", (W, H), (252, 252, 254))   # nền trắng sáng
    draw   = ImageDraw.Draw(canvas)

    LEAGUE_H = 56    # thanh giải đấu
    FOOTER_H = 48    # footer (không còn BLV pill nhưng giữ khoảng thở)
    BODY_TOP = LEAGUE_H
    BODY_BOT = H - FOOTER_H
    BODY_H   = BODY_BOT - BODY_TOP
    CX = W // 2

    # ── Viền trên accent ──
    draw.rectangle([(0,0),(W,5)], fill=A)

    # ── Thanh giải đấu: nền trắng, viền accent, chữ màu accent ──
    draw.rectangle([(0,5),(W,LEAGUE_H)], fill=(255,255,255))
    draw.rectangle([(0,LEAGUE_H),(W,LEAGUE_H+2)], fill=A)   # gạch dưới
    if league:
        # Chữ màu accent đậm, không cần nền đen
        draw.text((CX, 5+(LEAGUE_H-5)//2), league[:36],
                  fill=A, font=_font(30), anchor="mm")

    # ── Body: 2 logo cùng kích thước + hộp VS/LIVE giữa ──
    NAME_H = 38; GAP = 10
    BW = 148   # chiều rộng hộp VS/LIVE (dùng để tính vùng an toàn)
    LOGO_ZONE = BODY_H - NAME_H - GAP - 10

    LX = W//4; RX = 3*W//4     # tâm logo trái / phải

    # Logo KHÔNG được vượt qua biên hộp VS: giới hạn = (CX - BW//2 - 20) - LX
    MAX_HALF = (CX - BW//2 - 20) - LX   # bán kính tối đa theo chiều ngang
    # Giới hạn theo chiều dọc và cap cứng 160px
    LMAX = min(LOGO_ZONE - 14, MAX_HALF * 2, 160)
    LMAX = max(LMAX, 60)   # tối thiểu 60px

    LY = BODY_TOP + 12 + LMAX//2
    NY_Y = LY + LMAX//2 + GAP + NAME_H//2

    def draw_logo(cx, cy, url, name):
        """Logo fit trong hộp LMAX×LMAX — cả 2 dùng cùng hộp → kích thước tương đương."""
        logo = fetch_logo(url, LMAX*4) if url else None
        if logo:
            if logo.mode != "RGBA": logo = logo.convert("RGBA")
            lw, lh = logo.size
            # Fit vào hộp vuông LMAX×LMAX giữ tỉ lệ
            sc = min(LMAX/lw, LMAX/lh, 1.0)
            nw, nh = max(1,int(lw*sc)), max(1,int(lh*sc))
            logo = logo.resize((nw,nh), Image.LANCZOS)
            ox, oy = cx-nw//2, cy-nh//2
            canvas.paste(logo.convert("RGB"), (ox,oy), logo.split()[3])
        else:
            R2 = LMAX//2
            draw.ellipse([(cx-R2,cy-R2),(cx+R2,cy+R2)],
                         fill=(235,235,240), outline=A, width=2)
            init="".join(w[0].upper() for w in (name or "?").split()[:2]) or "?"
            draw.text((cx,cy), init, fill=A, font=_font(42), anchor="mm")
        # Tên đội dưới logo
        draw.text((cx, NY_Y), (name or "?")[:18],
                  fill=(25,25,25), font=_font(20), anchor="mm")

    draw_logo(LX, LY, home_logo_url, home_team)
    draw_logo(RX, LY, away_logo_url, away_team)

    # ── Hộp VS / LIVE giữa ──
    if status=="live":
        bbg,bfg,l1,l2,f1,f2=(34,160,60),(255,255,255),"LIVE","",22,18
    else:
        bbg,bfg=(255,255,255),A
        l1,l2=time_str or "VS",date_str or ""
        f1,f2=26,20
    BH=68 if l2 else 50
    bx0,by0=CX-BW//2,LY-BH//2; bx1,by1=CX+BW//2,LY+BH//2
    draw.rounded_rectangle([(bx0,by0),(bx1,by1)], radius=12, fill=bbg, outline=A, width=3)
    if status=="live":
        draw.ellipse([(bx0+14,LY-6),(bx0+26,LY+6)], fill=(255,60,60))
        draw.text((CX+8,LY),"LIVE",fill=(255,255,255),font=_font(24),anchor="mm")
    elif l2:
        draw.text((CX,by0+BH//2-f1//2-2),l1,fill=bfg,font=_font(f1),anchor="mm")
        draw.text((CX,by0+BH//2+f2//2+2),l2,fill=(110,110,110),font=_font(f2,bold=False),anchor="mm")
    else:
        draw.text((CX,LY),l1,fill=bfg,font=_font(f1),anchor="mm")

    # ── Footer: nền trắng nhạt, không có BLV pill, viền dưới accent ──
    draw.line([(0,BODY_BOT),(W,BODY_BOT)], fill=(220,220,225), width=1)
    draw.rectangle([(0,BODY_BOT),(W,H)], fill=(250,250,252))
    draw.rectangle([(0,H-4),(W,H)], fill=A)

    buf=io.BytesIO()
    canvas.save(buf,format="WEBP",quality=85,method=4)
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
    sc_map={"live":{"text":"Live","color":"#E73131","text_color":"#fff"},
            "upcoming":{"text":"Sap dien ra","color":"#d54f1a","text_color":"#fff"},
            "finished":{"text":"Ket thuc","color":"#444","text_color":"#fff"}}
    labels=[{**sc_map.get(status,sc_map["live"]),"position":"top-left"}]
    blv_names=[s["blv"] for s in m.get("blv_sources",[]) if s.get("blv")]
    if len(blv_names)>1:
        labels.append({"text":f"BLV {len(blv_names)}","position":"bottom-left","color":"#1a8a2e","text_color":"#fff"})
    elif blv_names:
        labels.append({"text":f"BLV {blv_names[0]}","position":"bottom-left","color":"#1a8a2e","text_color":"#fff"})
    blv_text=blv_names[0] if len(blv_names)==1 else (f"{len(blv_names)} BLV" if blv_names else "")
    blv_groups={}
    for s in all_streams:
        bk=s.get("blv") or "__"
        grp=blv_groups.setdefault(bk,[])
        if s["url"] not in {x["url"] for x in grp}: grp.append(s)
    stream_objs=[]
    for idx,(bk,raw_s) in enumerate(blv_groups.items()):
        if not raw_s: continue
        slabel=f"BLV {bk}" if bk!="__" else f"Nguon {idx+1}"
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
        img_obj={"padding":0,"background_color":"#fff","display":"cover","url":thumb,"width":820,"height":540}
    elif _PIL:
        raw=make_thumbnail(m.get("home_team",""),m.get("away_team",""),la,lb,
                           m.get("time_str",""),m.get("date_str",""),
                           status,league,m.get("sport",""),blv_text)
        cdn_url=save_thumbnail(raw,ch_id)
        img_obj=({"padding":0,"background_color":"#fff","display":"cover","url":cdn_url,"width":820,"height":540}
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
    return {"id":"cauthutv-live","name":"CauThu TV - Truc tiep the thao",
            "url":BASE_URL+"/","disable_ads":True,"color":"#0f3460","grid_number":2,
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
