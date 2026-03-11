import os
import json
import requests
import feedparser
import gspread
import calendar
import re

from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound

from requests_oauthlib import OAuth1
from bs4 import BeautifulSoup
import yake

# ---------------- Config ----------------
CONFIG_FILE = os.environ.get("CONFIG_FILE", "config.json")


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------- Debug helpers ----------------
def url_path_only(u: str) -> str:
    """scheme://host/path (drops query+fragment) to compare slugs regardless of UTM."""
    try:
        p = urlparse(u)
        return f"{p.scheme}://{p.netloc}{p.path}"
    except Exception:
        return (u or "").strip()


def shorten(s: str, n: int = 110) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def col_to_a1(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ---------------- Utils ----------------
def safe_int(x, default=0):
    try:
        return int(str(x).strip())
    except Exception:
        return default


def add_utm(url: str, utm: dict) -> str:
    """Merge UTM params into URL without breaking existing query params."""
    if not url:
        return url
    parts = urlparse(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    for k, v in (utm or {}).items():
        q.setdefault(k, v)
    new_query = urlencode(q, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def entry_ts(entry) -> int:
    """Best-effort timestamp for sorting feed entries by publish time."""
    try:
        if getattr(entry, "published_parsed", None):
            return calendar.timegm(entry.published_parsed)
        if getattr(entry, "updated_parsed", None):
            return calendar.timegm(entry.updated_parsed)
    except Exception:
        pass
    return 0


def parse_sheet_date_to_utc(date_str: str):
    """Parse RSS/ISO date string from sheet to UTC datetime; returns None if can't parse."""
    if not date_str:
        return None
    s = str(date_str).strip()
    if not s or s.upper() == "N/A":
        return None

    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def prune_rows_older_than(sheet, all_rows, col_date: int, retention_days: int, header_rows: int = 1) -> int:
    """
    Deletes rows older than now-retention_days based on date column.
    Deletes in contiguous batches from bottom to top.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    to_delete = []
    for idx, row in enumerate(all_rows):
        if idx < header_rows:
            continue
        if len(row) <= col_date:
            continue
        dt = parse_sheet_date_to_utc(row[col_date])
        if dt and dt < cutoff:
            to_delete.append(idx + 1)  # gspread rows are 1-based

    if not to_delete:
        return 0

    to_delete.sort()

    ranges = []
    start = prev = to_delete[0]
    for r in to_delete[1:]:
        if r == prev + 1:
            prev = r
        else:
            ranges.append((start, prev))
            start = prev = r
    ranges.append((start, prev))

    deleted = 0
    for (s, e) in reversed(ranges):
        sheet.delete_rows(s, e)
        deleted += (e - s + 1)

    return deleted


# ---------------- Google Auth/Sheets ----------------
def load_credentials(google_cfg: dict):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    env_name = google_cfg.get("service_account_json_env", "GOOGLE_SERVICE_ACCOUNT_JSON")
    sa_json = os.environ.get(env_name)

    if sa_json:
        info = json.loads(sa_json)
        print(f"🔑 Service account (env:{env_name}): {info.get('client_email')}")
        return Credentials.from_service_account_info(info, scopes=scopes)

    sa_file = google_cfg.get("service_account_file", "service_account.json")
    print(f"🔑 Service account (file:{sa_file}) exists={os.path.exists(sa_file)}")
    return Credentials.from_service_account_file(sa_file, scopes=scopes)


def get_worksheet(google_cfg: dict, spreadsheet_id: str, worksheet_index: int):
    creds = load_credentials(google_cfg)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    ws = sh.get_worksheet(int(worksheet_index))
    print(f"📄 Spreadsheet: {sh.title} | Worksheet: {ws.title} | index: {worksheet_index}")
    return ws


# ---------------- Feed ----------------
def fetch_feed(rss_url: str):
    """Fetch RSS with a cache-buster query param to reduce CDN caching issues."""
    cb = int(datetime.now(timezone.utc).timestamp())
    sep = "&" if "?" in rss_url else "?"
    url = f"{rss_url}{sep}cb={cb}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    print(f"🌐 Fetching feed: {url}")

    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code == 200:
            return feedparser.parse(response.content).entries
        print(f"❌ Feed returned status: {response.status_code}")
        return []
    except Exception as e:
        print(f"❌ Feed Network Error: {e}")
        return []


# ---------------- X (Twitter) helpers ----------------
def load_x_oauth1(config: dict) -> OAuth1:
    xcfg = config.get("x", {})
    k_env = xcfg.get("api_key_env", "X_API_KEY")
    s_env = xcfg.get("api_secret_env", "X_API_SECRET")
    t_env = xcfg.get("access_token_env", "X_ACCESS_TOKEN")
    ts_env = xcfg.get("access_token_secret_env", "X_ACCESS_TOKEN_SECRET")

    api_key = os.environ.get(k_env, "").strip()
    api_secret = os.environ.get(s_env, "").strip()
    access_token = os.environ.get(t_env, "").strip()
    access_token_secret = os.environ.get(ts_env, "").strip()

    missing = [name for name, val in [
        (k_env, api_key), (s_env, api_secret), (t_env, access_token), (ts_env, access_token_secret)
    ] if not val]
    if missing:
        raise RuntimeError(f"Missing X env vars: {', '.join(missing)}")

    return OAuth1(api_key, api_secret, access_token, access_token_secret)


def post_tweet(oauth: OAuth1, text: str) -> str:
    url = "https://api.twitter.com/2/tweets"
    r = requests.post(url, auth=oauth, json={"text": text}, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"X API error {r.status_code}: {r.text}")
    data = r.json()
    tweet_id = (data.get("data") or {}).get("id") or ""
    return tweet_id


def build_tweet_text(title: str, url: str, hashtags: str) -> str:
    """
    Required format:
      Title
      hashtags
      URL (with UTM)

    Important: X treats URLs as a fixed t.co length (roughly 23 chars),
    so we budget using TCO_LEN instead of the raw URL string length.
    """
    TCO_LEN = 23

    title = (title or "").strip()
    url = (url or "").strip()
    hashtags = (hashtags or "").strip()

    parts = [title, hashtags, url]
    text = "\n\n".join([p for p in parts if p])

    effective_len = len(title) + len(hashtags) + (TCO_LEN if url else 0)
    if title and hashtags:
        effective_len += 2
    if (title or hashtags) and url:
        effective_len += 2

    if effective_len <= 280:
        return text

    fixed = (len(hashtags) + (TCO_LEN if url else 0))
    fixed += (2 if hashtags and title else 0) + (2 if url and (title or hashtags) else 0)

    max_title = max(30, 280 - fixed)
    if len(title) > max_title:
        title = shorten(title, max_title)

    parts = [title, hashtags, url]
    text = "\n\n".join([p for p in parts if p])

    effective_len = len(title) + len(hashtags) + (TCO_LEN if url else 0)
    if title and hashtags:
        effective_len += 2
    if (title or hashtags) and url:
        effective_len += 2

    if effective_len <= 280:
        return text

    tags = hashtags.split()
    if len(tags) > 1:
        keep_last = tags[-1]
        tags = tags[:-1]

        while tags:
            candidate = " ".join(tags + [keep_last])
            parts = [title, candidate, url]
            candidate_text = "\n\n".join([p for p in parts if p])

            eff = len(title) + len(candidate) + (TCO_LEN if url else 0)
            if title and candidate:
                eff += 2
            if (title or candidate) and url:
                eff += 2

            if eff <= 280:
                return candidate_text

            tags.pop(0)

        hashtags = keep_last
    else:
        hashtags = hashtags

    parts = [title, hashtags, url]
    text = "\n\n".join([p for p in parts if p])
    return text


# ---------------- Facebook helpers ----------------
def load_facebook_shopscan_credentials(config: dict) -> tuple[str, str]:
    fcfg = config.get("facebook", {})
    page_id_env = fcfg.get("shopscan_page_id_env", "FB_PAGE_ID_SHOPSCAN")
    page_token_env = fcfg.get("shopscan_page_token_env", "FB_PAGE_TOKEN_SHOPSCAN")

    page_id = os.environ.get(page_id_env, "").strip()
    page_token = os.environ.get(page_token_env, "").strip()

    missing = [name for name, val in [
        (page_id_env, page_id),
        (page_token_env, page_token),
    ] if not val]

    if missing:
        raise RuntimeError(f"Missing Facebook env vars: {', '.join(missing)}")

    return page_id, page_token


def build_facebook_message(title: str, hashtags: str, link: str = "", mode: str = "title_hashtags_link") -> str:
    title = (title or "").strip()
    hashtags = (hashtags or "").strip()
    link = (link or "").strip()
    mode = (mode or "title_hashtags_link").strip().lower()

    if mode == "title_hashtags":
        parts = [title, hashtags]
    elif mode == "title_only":
        parts = [title]
    else:
        parts = [title, hashtags, link]

    return "\n\n".join([p for p in parts if p]).strip()


def post_to_facebook_page(page_id: str, page_token: str, message: str, link: str = "") -> str:
    endpoint = f"https://graph.facebook.com/v25.0/{page_id}/feed"

    payload = {
        "message": (message or "").strip(),
        "access_token": page_token,
    }
    if (link or "").strip():
        payload["link"] = link.strip()

    r = requests.post(endpoint, data=payload, timeout=30)

    try:
        data = r.json()
    except Exception:
        data = {"raw_text": r.text}

    if r.status_code not in (200, 201):
        raise RuntimeError(f"Facebook API error {r.status_code}: {data}")

    post_id = data.get("id") or ""
    if not post_id:
        raise RuntimeError(f"Facebook API success but no post id returned: {data}")

    return post_id


# ---------------- Deep hashtag generation (Option A - non-AI) ----------------
def fetch_article_html_and_text(url: str) -> tuple[str, str]:
    """
    Fetch article HTML and return (html, cleaned_text).
    Cleans Taxscan promo/footer blocks so keyword extraction stays relevant.
    """
    base_url = url_path_only(url)

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Accept": "text/html,application/xhtml+xml",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    r = requests.get(base_url, headers=headers, timeout=25)
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
        tag.decompose()

    node = (
        soup.select_one("article")
        or soup.select_one("div.entry-content")
        or soup.select_one("div.td-post-content")
        or soup.select_one("div.post-content")
        or soup.select_one("main")
        or soup
    )

    text = node.get_text("\n", strip=True)
    text = re.sub(r"\n{2,}", "\n", text).strip()

    cut_markers = [
        "Support our journalism",
        "Next Story",
        "Related Stories",
        "Quick Links",
        "Know More",
        "Get news delivered",
        "©",
        "Powered by",
        "Read the full article.",
        "* * *",
    ]

    if "Read the full article." in text and "* * *" in text:
        parts = text.split("* * *", 1)
        if len(parts) == 2:
            text = parts[1].strip()

    for m in cut_markers:
        idx = text.find(m)
        if idx != -1 and idx > 200:
            text = text[:idx].strip()
            break

    bad_line_starts = (
        "Read More:",
        "Also read:",
        "Also Read:",
        "Follow us on",
        "Subscribe",
        "Telegram",
        "WhatsApp",
    )
    lines = []
    for line in text.split("\n"):
        s = line.strip()
        if not s:
            continue
        if s.startswith(bad_line_starts):
            continue
        lines.append(s)

    text = " ".join(lines)
    text = re.sub(r"\s+", " ", text).strip()

    text = remove_author_lines(text)
    return html, text[:8000]


def extract_taxscan_tags(html: str) -> list[str]:
    """
    Try to extract Taxscan's own topic tags from HTML while avoiding nav/footer/author links.
    Returns a small list of tag-like strings (not normalized to hashtags yet).
    """
    soup = BeautifulSoup(html, "lxml")

    containers = []
    for sel in [
        ".tags", ".tagcloud", ".td-post-small-box", ".td-post-source-tags",
        ".post-tags", ".entry-tags", ".td-tags", ".jp-relatedposts",
        "[class*='tag']", "[id*='tag']"
    ]:
        containers.extend(soup.select(sel))
    search_scope = containers if containers else [soup]

    candidates: list[str] = []

    for scope in search_scope:
        for a in scope.find_all("a"):
            txt = (a.get_text(" ", strip=True) or "").strip()
            if not txt:
                continue

            if re.match(r"(?i)^\s*by\b", txt):
                continue

            if not (2 <= len(txt) <= 35 and len(txt.split()) <= 5):
                continue

            if not re.search(r"[A-Za-z]", txt):
                continue

            candidates.append(txt)

    blacklist = {
        "Home", "Top Stories", "News Updates", "Columns", "Login", "Subscribe",
        "Next Story", "Related Stories", "Privacy Policy", "Terms and Conditions",
        "Contact Us", "About Us", "Careers", "Advertise", "Telegram", "Taxscan premium",
        "Facebook", "Instagram", "YouTube", "WhatsApp", "LinkedIn", "X", "Twitter",
        "Read More", "Read More:", "Read Order", "Read Full Article", "Read the full article",
        "Support our journalism", "Donate", "Join", "Follow", "Share"
    }

    junk_phrases = {
        "read more", "read order", "read full article", "support our journalism",
        "terms", "privacy", "contact", "about", "careers", "advertise"
    }

    out: list[str] = []
    seen: set[str] = set()

    for t in candidates:
        t = re.sub(r"\s+", " ", t).strip()
        if not t:
            continue

        if t in blacklist:
            continue

        low = t.lower()
        if low in seen:
            continue

        if low in junk_phrases:
            continue

        if re.match(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}$", t):
            continue

        if low in {"taxscan", "order", "court", "case", "tribunal"}:
            continue

        seen.add(low)
        out.append(t)

    return out[-10:]


def normalize_tag(s: str) -> str:
    """Convert keyword phrase to a hashtag token."""
    s = re.sub(r"[^A-Za-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""

    acronyms = {"GST", "ITAT", "CESTAT", "NCLT", "NCLAT", "HC", "SC", "CBDT", "CBIC", "VAT", "TDS", "TCS", "FEMA"}
    parts = s.split()
    out = []
    for p in parts:
        up = p.upper()
        if up in acronyms:
            out.append(up)
        else:
            out.append(p.capitalize())
    tag = "".join(out)

    return tag[:40]


def extract_hashtags_from_text(text: str, max_tags: int = 6) -> list[str]:
    """Keyword extraction using YAKE (non-AI) with stronger noise filtering (ads/bylines/names)."""
    if not text:
        return []

    kw_extractor = yake.KeywordExtractor(
        lan="en",
        n=3,
        top=40,
        dedupLim=0.9,
        windowsSize=2
    )
    keywords = kw_extractor.extract_keywords(text)

    tags: list[str] = []
    seen: set[str] = set()

    stop = {
        "order", "case", "court", "tribunal", "tax", "act", "section", "rule", "rules",
        "judgment", "appeal", "petition", "authority", "officer", "department",
        "read more", "also read", "read full article", "support our journalism",
        "subscribe", "follow", "join", "share", "click", "download",
        "telegram", "whatsapp", "youtube", "facebook", "instagram",
        "advertisement", "sponsored", "promo", "offer",
        "by"
    }

    byline_prefix = re.compile(r"(?i)^\s*by\s*[-:–—]?\s+")
    person_name_like = re.compile(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}$")

    for kw, _score in keywords:
        raw_kw = (kw or "").strip()
        if not raw_kw:
            continue

        if byline_prefix.match(raw_kw):
            continue

        if person_name_like.match(raw_kw):
            continue

        low_raw = raw_kw.lower()
        if any(x in low_raw for x in ["support our journalism", "read more", "also read", "subscribe", "follow us"]):
            continue

        tag = normalize_tag(raw_kw)
        if not tag:
            continue

        low = tag.lower()

        if low in stop:
            continue

        if len(tag) < 3:
            continue

        if low in seen:
            continue

        seen.add(low)
        tags.append(tag)

        if len(tags) >= max_tags:
            break

    return tags


def remove_author_lines(text: str) -> str:
    """
    Removes author bylines like:
      'By - Kavi Priya'
      'By: Kavi Priya'
      'By Kavi Priya'
    """
    if not text:
        return text

    t = text.replace("\r", "\n")

    t = re.sub(r"(?im)^\s*by\s*[-:–—]?\s*[A-Za-z][A-Za-z .'-]{1,80}\s*$", "", t)

    t = re.sub(r"(?i)\bby\s*[-:–—]?\s*[A-Za-z][A-Za-z .'-]{1,80}", "", t)

    t = re.sub(r"\n{2,}", "\n", t)
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t


def build_hashtags_fallback(base: list[str], title: str, max_total: int) -> str:
    """Simple fallback: base tags + title boosts + always #taxscan at end."""
    base = [h.strip().lstrip("#") for h in (base or []) if str(h).strip()]

    t = (title or "").lower()
    boosts = []
    if "itat" in t:
        boosts.append("ITAT")
    if "cestat" in t:
        boosts.append("CESTAT")
    if "nclat" in t:
        boosts.append("NCLAT")
    if "nclt" in t:
        boosts.append("NCLT")
    if "gst" in t:
        boosts.append("GST")
    if "income tax" in t:
        boosts.append("IncomeTax")
    if "service tax" in t:
        boosts.append("ServiceTax")
    if "high court" in t or " hc " in f" {t} ":
        boosts.append("HighCourt")
    if "supreme court" in t or " sc " in f" {t} ":
        boosts.append("SupremeCourt")

    tags = []
    seen = set()
    for x in base + boosts:
        x = x.strip().lstrip("#")
        if not x:
            continue
        lx = x.lower()
        if lx not in seen:
            tags.append(x)
            seen.add(lx)
        if len(tags) >= max_total:
            break

    if "taxscan" not in seen:
        tags.append("taxscan")

    return " ".join([f"#{t}" for t in tags if t])


def build_deep_hashtags(title: str, url: str, max_tags: int = 6, always_last: str = "taxscan") -> str:
    html, text = fetch_article_html_and_text(url)

    tags = extract_taxscan_tags(html)

    if not tags or len(" ".join(tags)) < 12:
        tags = extract_hashtags_from_text(text, max_tags=max_tags)

    t = (title or "").lower()
    boosts = []
    if "itat" in t:
        boosts.append("ITAT")
    if "cestat" in t:
        boosts.append("CESTAT")
    if "nclat" in t:
        boosts.append("NCLAT")
    if "nclt" in t:
        boosts.append("NCLT")
    if "gst" in t:
        boosts.append("GST")
    if "high court" in t:
        boosts.append("HighCourt")
    if "supreme court" in t:
        boosts.append("SupremeCourt")

    merged = []
    seen = set()
    for x in boosts + tags:
        x = x.strip().lstrip("#")
        if not x:
            continue
        x = normalize_tag(x)
        if not x:
            continue
        lx = x.lower()
        if lx not in seen:
            merged.append(x)
            seen.add(lx)
        if len(merged) >= max_tags:
            break

    if always_last:
        al = always_last.strip().lstrip("#")
        merged = [m for m in merged if m.lower() != al.lower()]
        merged.append(al)

    print("🧠 Clean text sample:", text[:250])
    print("🏷️ Tags picked:", tags[:10])

    return " ".join([f"#{t}" for t in merged if t])


# ---------------- Jobs ----------------
def job_taxscan_feed_to_sheet(cfg: dict, google_cfg: dict):
    """
    RSS -> Sheet
    Inserts new rows at top, prunes older rows, fills LinkedIn, X, and optional Facebook prep columns.
    """
    rss_url = cfg["rss_url"]
    spreadsheet_id = cfg["spreadsheet_id"]
    worksheet_index = cfg.get("worksheet_index", 0)
    insert_at_row = int(cfg.get("insert_at_row", 2))
    utm = cfg.get("utm", {})

    retention_days = int(cfg.get("retention_days", 0))

    cols = cfg.get("columns", {})
    col_serial = int(cols.get("serial", 0))
    col_title = int(cols.get("title", 1))
    col_link = int(cols.get("link", 2))
    col_date = int(cols.get("date", 3))

    li_cols = cfg.get("linkedin_columns", {})
    col_li_post = int(li_cols.get("post_to_linkedin", 4))
    col_li_posted_at = int(li_cols.get("posted_at", 5))
    col_li_post_id = int(li_cols.get("post_id", 6))
    col_li_error = int(li_cols.get("error", 7))
    post_to_linkedin_default = str(cfg.get("post_to_linkedin_default", "YES")).strip() or "YES"

    x_cols = cfg.get("x_columns", {})
    col_x_post = int(x_cols.get("post_to_x", 8)) if x_cols else None
    col_x_posted_at = int(x_cols.get("posted_at", 9)) if x_cols else None
    col_x_tweet_id = int(x_cols.get("tweet_id", 10)) if x_cols else None
    col_x_error = int(x_cols.get("error", 11)) if x_cols else None
    post_to_x_default = str(cfg.get("post_to_x_default", "YES")).strip() or "YES"

    fb_shopscan_cols = cfg.get("facebook_shopscan_columns", {})
    col_fb_post = int(fb_shopscan_cols.get("post_to_facebook", 20)) if fb_shopscan_cols else None
    col_fb_posted_at = int(fb_shopscan_cols.get("posted_at", 21)) if fb_shopscan_cols else None
    col_fb_post_id = int(fb_shopscan_cols.get("post_id", 22)) if fb_shopscan_cols else None
    col_fb_error = int(fb_shopscan_cols.get("error", 23)) if fb_shopscan_cols else None
    post_to_fb_default = str(cfg.get("post_to_facebook_shopscan_default", "NO")).strip() or "NO"

    print(f"\n🧩 Job: {cfg.get('name', 'taxscan_feed_to_sheet')}")
    print(f"   RSS: {rss_url}")
    print(f"   Sheet: {spreadsheet_id} (tab index {worksheet_index})")
    if retention_days > 0:
        print(f"   Retention: keep last {retention_days} days")

    try:
        sheet = get_worksheet(google_cfg, spreadsheet_id, worksheet_index)
        all_rows = sheet.get_all_values()
        print(f"📏 Sheet rows fetched (incl header): {len(all_rows)}")
    except SpreadsheetNotFound:
        print("❌ Spreadsheet not found. Check spreadsheet_id and sharing permissions.")
        return
    except APIError as e:
        print("❌ Google Sheets API Error (connect):")
        print(getattr(e.response, "text", str(e)))
        return

    if retention_days > 0:
        try:
            deleted = prune_rows_older_than(sheet, all_rows, col_date=col_date, retention_days=retention_days, header_rows=1)
            if deleted:
                print(f"🧹 Pruned {deleted} rows older than {retention_days} days.")
                all_rows = sheet.get_all_values()
                print(f"📏 Sheet rows after prune refresh (incl header): {len(all_rows)}")
        except APIError as e:
            print("❌ Google Sheets API Error (prune):")
            print(getattr(e.response, "text", str(e)))
            return

    existing_records = set()
    max_serial = 0
    for i, row in enumerate(all_rows):
        if i == 0:
            continue
        if len(row) > col_serial and str(row[col_serial]).strip():
            max_serial = max(max_serial, safe_int(row[col_serial], 0))
        if len(row) > max(col_link, col_date):
            sheet_link = str(row[col_link]).strip()
            sheet_date = str(row[col_date]).strip()
            existing_records.add((sheet_link, sheet_date))
            existing_records.add((add_utm(sheet_link, utm), sheet_date))

    print(f"📊 Connected. Existing keys: {len(existing_records)} | Max Serial: {max_serial}")

    entries = fetch_feed(rss_url)
    if not entries:
        print("⚠️ No entries found in the feed.")
        return

    new_items = []
    for entry in entries:
        title = (entry.get("title") or "").strip()
        raw_link = (entry.get("link") or "").strip()
        date = (entry.get("published") or entry.get("updated") or "N/A").strip()
        if not raw_link:
            continue
        utm_link = add_utm(raw_link, utm)
        ts = entry_ts(entry)

        if (raw_link, date) in existing_records or (utm_link, date) in existing_records:
            continue

        new_items.append((ts, title, raw_link, utm_link, date))
        existing_records.add((raw_link, date))
        existing_records.add((utm_link, date))

    if not new_items:
        print("✨ No new entries to add.")
        return

    new_items_sorted = sorted(new_items, key=lambda x: x[0], reverse=True)
    n = len(new_items_sorted)
    serial = max_serial + n

    max_col_needed = max(col_serial, col_title, col_link, col_date, col_li_post, col_li_posted_at, col_li_post_id, col_li_error)
    if col_x_post is not None:
        max_col_needed = max(max_col_needed, col_x_post, col_x_posted_at, col_x_tweet_id, col_x_error)
    if col_fb_post is not None:
        max_col_needed = max(max_col_needed, col_fb_post, col_fb_posted_at, col_fb_post_id, col_fb_error)

    rows_to_insert = []
    for (_ts, title, _raw_link, utm_link, date) in new_items_sorted:
        row = [""] * (max_col_needed + 1)
        row[col_serial] = serial
        row[col_title] = title
        row[col_link] = utm_link
        row[col_date] = date

        row[col_li_post] = post_to_linkedin_default
        row[col_li_posted_at] = ""
        row[col_li_post_id] = ""
        row[col_li_error] = ""

        if col_x_post is not None:
            row[col_x_post] = post_to_x_default
            row[col_x_posted_at] = ""
            row[col_x_tweet_id] = ""
            row[col_x_error] = ""

        if col_fb_post is not None:
            row[col_fb_post] = post_to_fb_default
            row[col_fb_posted_at] = ""
            row[col_fb_post_id] = ""
            row[col_fb_error] = ""

        rows_to_insert.append(row)
        serial -= 1

    try:
        sheet.insert_rows(rows_to_insert, row=insert_at_row, value_input_option="RAW")
        print(f"✅ Inserted {n} rows at row {insert_at_row}.")
        print(f"🔢 Serial assigned: {max_serial + 1} .. {max_serial + n} (highest at top).")
        print(f"🧾 Top inserted title: {rows_to_insert[0][col_title][:80]}...")
    except APIError as e:
        print("❌ Google Sheets API Error (insert_rows):")
        print(getattr(e.response, "text", str(e)))
    except Exception as e:
        print(f"❌ Insert rows error: {repr(e)}")


def job_sheet_to_x(cfg: dict, config: dict, google_cfg: dict):
    """
    Sheet -> X
    Tweets unposted rows, generates per-article hashtags by fetching article content.
    """
    spreadsheet_id = cfg["spreadsheet_id"]
    worksheet_index = cfg.get("worksheet_index", 0)
    scan_top_rows = int(cfg.get("scan_top_rows", 200))
    post_limit = int(cfg.get("post_limit_per_run", 3))

    x_utm = cfg.get("x_utm", {"utm_source": "taxscan", "utm_medium": "x", "utm_campaign": "news"})
    base_hashtags = cfg.get("hashtags", ["TaxNews", "taxscan"])
    max_hashtags = int(cfg.get("max_hashtags", 6))

    feed_job = None
    for j in config.get("jobs", []):
        if j.get("name") == "taxscan_feed_to_sheet":
            feed_job = j
            break
    if not feed_job:
        print("❌ sheet_to_x: cannot find taxscan_feed_to_sheet job for column mapping.")
        return

    cols = feed_job.get("columns", {})
    col_title = int(cols.get("title", 1))
    col_link = int(cols.get("link", 2))
    col_date = int(cols.get("date", 3))

    x_cols = feed_job.get("x_columns", {})
    if not x_cols:
        print("❌ sheet_to_x: x_columns not found in taxscan_feed_to_sheet config.")
        return

    col_x_post = int(x_cols.get("post_to_x", 8))
    col_x_posted_at = int(x_cols.get("posted_at", 9))
    col_x_tweet_id = int(x_cols.get("tweet_id", 10))
    col_x_error = int(x_cols.get("error", 11))
    post_to_x_default = str(feed_job.get("post_to_x_default", "YES")).strip() or "YES"

    print(f"\n🧩 Job: {cfg.get('name', 'sheet_to_x')}")
    print(f"   Sheet: {spreadsheet_id} (tab index {worksheet_index})")
    print(f"   Scan top rows: {scan_top_rows} | Post limit: {post_limit}")

    try:
        oauth = load_x_oauth1(config)
    except Exception as e:
        print(f"❌ X auth error: {e}")
        return

    try:
        sheet = get_worksheet(google_cfg, spreadsheet_id, worksheet_index)
    except Exception as e:
        print(f"❌ sheet_to_x: sheet open error: {e}")
        return

    last_col_index = max(col_title, col_link, col_date, col_x_post, col_x_posted_at, col_x_tweet_id, col_x_error) + 1
    last_col_letter = col_to_a1(last_col_index)
    end_row = 1 + scan_top_rows
    rng = f"A1:{last_col_letter}{end_row}"

    try:
        rows = sheet.get(rng)
    except Exception as e:
        print(f"❌ sheet_to_x: read range error: {e}")
        return

    if not rows or len(rows) < 2:
        print("⚠️ sheet_to_x: no data rows.")
        return

    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S IST")

    posted = 0
    for i in range(1, len(rows)):
        if posted >= post_limit:
            break

        row = rows[i]
        sheet_row_number = i + 1

        def cell(col_idx: int) -> str:
            return str(row[col_idx]).strip() if len(row) > col_idx and row[col_idx] is not None else ""

        title = cell(col_title)
        link = cell(col_link)
        post_flag = cell(col_x_post).upper() if cell(col_x_post) else post_to_x_default.upper()
        already_posted_at = cell(col_x_posted_at)

        if post_flag == "NO":
            continue
        if already_posted_at:
            continue
        if not title or not link:
            continue

        x_link = add_utm(url_path_only(link), x_utm)

        try:
            hashtags = build_deep_hashtags(title, link, max_tags=max_hashtags, always_last="taxscan")
        except Exception as e:
            hashtags = build_hashtags_fallback(base_hashtags, title, max_hashtags)
            print(f"⚠️ Hashtag deep-gen failed row {sheet_row_number}: {shorten(str(e), 120)}")

        tweet_text = build_tweet_text(title, x_link, hashtags)

        try:
            print("✍️ Tweet preview:\n" + tweet_text + "\n" + "-" * 50)
            print(f"DEBUG row={sheet_row_number} title_len={len(title)} hashtags_len={len(hashtags)} url_len={len(x_link)}")
            tweet_id = post_tweet(oauth, tweet_text)
            sheet.update_cell(sheet_row_number, col_x_posted_at + 1, now_ist)
            sheet.update_cell(sheet_row_number, col_x_tweet_id + 1, tweet_id)
            sheet.update_cell(sheet_row_number, col_x_error + 1, "")
            print(f"✅ Tweeted row {sheet_row_number}: id={tweet_id} | {shorten(title, 70)}")
            posted += 1
        except Exception as e:
            err = str(e)
            try:
                sheet.update_cell(sheet_row_number, col_x_error + 1, shorten(err, 240))
            except Exception:
                pass
            print(f"❌ Tweet failed row {sheet_row_number}: {shorten(title, 70)} | {shorten(err, 160)}")

    print(f"✨ sheet_to_x complete. Posted {posted} tweet(s).")


def job_sheet_to_facebook_shopscan(cfg: dict, config: dict, google_cfg: dict):
    """
    Sheet -> Facebook (Shopscan Page)
    Posts unposted rows to Shopscan Facebook page.
    """
    spreadsheet_id = cfg["spreadsheet_id"]
    worksheet_index = cfg.get("worksheet_index", 0)
    scan_top_rows = int(cfg.get("scan_top_rows", 200))
    post_limit = int(cfg.get("post_limit_per_run", 3))

    fb_utm = cfg.get("facebook_utm", {"utm_source": "taxscan", "utm_medium": "facebook", "utm_campaign": "news"})
    base_hashtags = cfg.get("hashtags", ["TaxNews", "taxscan"])
    max_hashtags = int(cfg.get("max_hashtags", 6))
    message_mode = cfg.get("facebook_message_mode", "title_hashtags_link")

    feed_job = None
    for j in config.get("jobs", []):
        if j.get("name") == "taxscan_feed_to_sheet":
            feed_job = j
            break
    if not feed_job:
        print("❌ sheet_to_facebook_shopscan: cannot find taxscan_feed_to_sheet job for column mapping.")
        return

    cols = feed_job.get("columns", {})
    col_title = int(cols.get("title", 1))
    col_link = int(cols.get("link", 2))
    col_date = int(cols.get("date", 3))

    fb_cols = feed_job.get("facebook_shopscan_columns", {})
    if not fb_cols:
        print("❌ sheet_to_facebook_shopscan: facebook_shopscan_columns not found in taxscan_feed_to_sheet config.")
        return

    col_fb_post = int(fb_cols.get("post_to_facebook", 20))
    col_fb_posted_at = int(fb_cols.get("posted_at", 21))
    col_fb_post_id = int(fb_cols.get("post_id", 22))
    col_fb_error = int(fb_cols.get("error", 23))
    post_to_fb_default = str(feed_job.get("post_to_facebook_shopscan_default", "NO")).strip() or "NO"

    print(f"\n🧩 Job: {cfg.get('name', 'sheet_to_facebook_shopscan')}")
    print(f"   Sheet: {spreadsheet_id} (tab index {worksheet_index})")
    print(f"   Scan top rows: {scan_top_rows} | Post limit: {post_limit}")

    try:
        page_id, page_token = load_facebook_shopscan_credentials(config)
    except Exception as e:
        print(f"❌ Facebook auth error: {e}")
        return

    try:
        sheet = get_worksheet(google_cfg, spreadsheet_id, worksheet_index)
    except Exception as e:
        print(f"❌ sheet_to_facebook_shopscan: sheet open error: {e}")
        return

    last_col_index = max(col_title, col_link, col_date, col_fb_post, col_fb_posted_at, col_fb_post_id, col_fb_error) + 1
    last_col_letter = col_to_a1(last_col_index)
    end_row = 1 + scan_top_rows
    rng = f"A1:{last_col_letter}{end_row}"

    try:
        rows = sheet.get(rng)
    except Exception as e:
        print(f"❌ sheet_to_facebook_shopscan: read range error: {e}")
        return

    if not rows or len(rows) < 2:
        print("⚠️ sheet_to_facebook_shopscan: no data rows.")
        return

    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S IST")

    posted = 0
    for i in range(1, len(rows)):
        if posted >= post_limit:
            break

        row = rows[i]
        sheet_row_number = i + 1

        def cell(col_idx: int) -> str:
            return str(row[col_idx]).strip() if len(row) > col_idx and row[col_idx] is not None else ""

        title = cell(col_title)
        link = cell(col_link)
        post_flag = cell(col_fb_post).upper() if cell(col_fb_post) else post_to_fb_default.upper()
        already_posted_at = cell(col_fb_posted_at)

        if post_flag == "NO":
            continue
        if already_posted_at:
            continue
        if not title or not link:
            continue

        fb_link = add_utm(url_path_only(link), fb_utm)

        try:
            hashtags = build_deep_hashtags(title, link, max_tags=max_hashtags, always_last="taxscan")
        except Exception as e:
            hashtags = build_hashtags_fallback(base_hashtags, title, max_hashtags)
            print(f"⚠️ Facebook hashtag deep-gen failed row {sheet_row_number}: {shorten(str(e), 120)}")

        fb_message = build_facebook_message(title, hashtags, fb_link, mode=message_mode)

        try:
            print("📘 Facebook post preview:\n" + fb_message + "\n" + "-" * 50)
            print(f"DEBUG FB row={sheet_row_number} title_len={len(title)} hashtags_len={len(hashtags)} url_len={len(fb_link)}")
            fb_post_id = post_to_facebook_page(page_id, page_token, fb_message, fb_link)
            sheet.update_cell(sheet_row_number, col_fb_posted_at + 1, now_ist)
            sheet.update_cell(sheet_row_number, col_fb_post_id + 1, fb_post_id)
            sheet.update_cell(sheet_row_number, col_fb_error + 1, "")
            print(f"✅ Facebook posted row {sheet_row_number}: id={fb_post_id} | {shorten(title, 70)}")
            posted += 1
        except Exception as e:
            err = str(e)
            try:
                sheet.update_cell(sheet_row_number, col_fb_error + 1, shorten(err, 240))
            except Exception:
                pass
            print(f"❌ Facebook post failed row {sheet_row_number}: {shorten(title, 70)} | {shorten(err, 160)}")

    print(f"✨ sheet_to_facebook_shopscan complete. Posted {posted} post(s).")


def run_jobs(config: dict):
    google_cfg = config.get("google", {})
    jobs = config.get("jobs", [])

    if not jobs:
        print("⚠️ No jobs found in config.json")
        return

    for job in jobs:
        if not job.get("enabled", True):
            print(f"⏭️ Skipping disabled job: {job.get('name', 'unnamed')}")
            continue

        name = job.get("name")

        try:
            if name == "taxscan_feed_to_sheet":
                job_taxscan_feed_to_sheet(job, google_cfg)
            elif name == "sheet_to_x":
                job_sheet_to_x(job, config, google_cfg)
            elif name == "sheet_to_facebook_shopscan":
                job_sheet_to_facebook_shopscan(job, config, google_cfg)
            else:
                print(f"⚠️ Unknown job name '{name}'. (Add handler in main.py)")
        except Exception as e:
            print(f"❌ Job failed: {name} | {repr(e)}")
            
def main():
    print("🚀 Starting Taxscan Automation Runner...")
    print(f"🧾 Using CONFIG_FILE: {CONFIG_FILE} | exists={os.path.exists(CONFIG_FILE)}")

    try:
        config = load_config()
    except Exception as e:
        print(f"❌ Failed to read {CONFIG_FILE}: {repr(e)}")
        return

    run_jobs(config)
    print("\n✨ All enabled jobs finished.")


if __name__ == "__main__":
    main()