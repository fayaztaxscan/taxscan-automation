import os
import json
import requests
import feedparser
import gspread
import calendar

from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound

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
    """
    Parse date in sheet (usually RSS published string) to UTC datetime.
    Returns None if can't parse.
    """
    if not date_str:
        return None
    s = str(date_str).strip()
    if not s or s.upper() == "N/A":
        return None

    # RSS dates are typically RFC822; parsedate_to_datetime handles that
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # Fallback: ISO-like
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def prune_rows_older_than(sheet, all_rows, col_date: int, retention_days: int, header_rows: int = 1) -> int:
    """
    Deletes rows with parsed date older than now - retention_days.
    Deletes in contiguous batches from bottom to top to keep indices valid.
    Returns number of rows deleted.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    to_delete = []
    for idx, row in enumerate(all_rows):
        if idx < header_rows:
            continue  # keep header
        if len(row) <= col_date:
            continue
        dt = parse_sheet_date_to_utc(row[col_date])
        if dt and dt < cutoff:
            to_delete.append(idx + 1)  # gspread rows are 1-based

    if not to_delete:
        return 0

    to_delete.sort()

    # group contiguous row numbers into ranges
    ranges = []
    start = prev = to_delete[0]
    for r in to_delete[1:]:
        if r == prev + 1:
            prev = r
        else:
            ranges.append((start, prev))
            start = prev = r
    ranges.append((start, prev))

    # delete from bottom to top
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
        # Safe to print: reveals only service account email
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
    """
    Fetch RSS with a cache-buster query param to reduce CDN caching issues.
    """
    # Cache buster: unique per run (seconds since epoch)
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


# ---------------- Jobs ----------------
def job_taxscan_feed_to_sheet(cfg: dict, google_cfg: dict):
    """
    Job: Fetch RSS, write to Google Sheet:
    - Inserts new rows at top (row 2 by default)
    - Newest item appears at the very top (row 2)
    - Serial number is highest at top (descending)
    - URL includes UTM params
    - Duplicate protection (link + date), tolerant to old rows without UTM
    - Optional pruning of rows older than retention_days (based on Published Date column)
    - Fills LinkedIn prep columns (E-H): Post to LinkedIn = YES, others blank

    Debug logs:
    - Config file used
    - Service account email (no secrets)
    - Spreadsheet title + worksheet title
    - Sheet row counts (before/after prune)
    - Feed top items (title/link/date)
    - Skip reasons for duplicates (raw vs utm)
    - Path-match warnings (same slug but date mismatch)
    """
    rss_url = cfg["rss_url"]
    spreadsheet_id = cfg["spreadsheet_id"]
    worksheet_index = cfg.get("worksheet_index", 0)
    insert_at_row = int(cfg.get("insert_at_row", 2))
    utm = cfg.get("utm", {})

    retention_days = int(cfg.get("retention_days", 0))  # 0 = no pruning

    cols = cfg.get("columns", {})
    col_serial = int(cols.get("serial", 0))
    col_title = int(cols.get("title", 1))
    col_link = int(cols.get("link", 2))
    col_date = int(cols.get("date", 3))

    # LinkedIn columns (0-based): E=4, F=5, G=6, H=7
    li_cols = cfg.get("linkedin_columns", {})
    col_li_post = int(li_cols.get("post_to_linkedin", 4))
    col_li_posted_at = int(li_cols.get("posted_at", 5))
    col_li_post_id = int(li_cols.get("post_id", 6))
    col_li_error = int(li_cols.get("error", 7))

    post_to_linkedin_default = str(cfg.get("post_to_linkedin_default", "YES")).strip() or "YES"

    print(f"\n🧩 Job: {cfg.get('name', 'taxscan_feed_to_sheet')}")
    print(f"   RSS: {rss_url}")
    print(f"   Sheet: {spreadsheet_id} (tab index {worksheet_index})")
    if retention_days > 0:
        print(f"   Retention: keep last {retention_days} days")

    # 1) Connect to sheet
    try:
        sheet = get_worksheet(google_cfg, spreadsheet_id, worksheet_index)
        all_rows = sheet.get_all_values()  # includes header
        print(f"📏 Sheet rows fetched (incl header): {len(all_rows)}")
        if len(all_rows) >= 2:
            top_serial = all_rows[1][col_serial] if len(all_rows[1]) > col_serial else ""
            top_link = all_rows[1][col_link] if len(all_rows[1]) > col_link else ""
            top_date = all_rows[1][col_date] if len(all_rows[1]) > col_date else ""
            print(f"🔎 Current top row: serial={top_serial} | date={top_date} | link={shorten(top_link, 140)}")
    except SpreadsheetNotFound:
        print("❌ Spreadsheet not found. Check spreadsheet_id and sharing permissions.")
        return
    except APIError as e:
        print("❌ Google Sheets API Error (connect):")
        print(getattr(e.response, "text", str(e)))
        return

    # 1b) Prune old rows (optional)
    if retention_days > 0:
        try:
            deleted = prune_rows_older_than(
                sheet,
                all_rows,
                col_date=col_date,
                retention_days=retention_days,
                header_rows=1
            )
            if deleted:
                print(f"🧹 Pruned {deleted} rows older than {retention_days} days.")
                all_rows = sheet.get_all_values()  # refresh after deletes
                print(f"📏 Sheet rows after prune refresh (incl header): {len(all_rows)}")
        except APIError as e:
            print("❌ Google Sheets API Error (prune):")
            print(getattr(e.response, "text", str(e)))
            return

    # 2) Build existing record set + max serial (+ path-only index for debugging)
    existing_records = set()  # (link, date)
    existing_paths = set()    # path-only (drops utm/query)
    max_serial = 0

    for i, row in enumerate(all_rows):
        if i == 0:
            continue

        # max serial
        if len(row) > col_serial and str(row[col_serial]).strip():
            max_serial = max(max_serial, safe_int(row[col_serial], 0))

        # existing (link,date)
        if len(row) > max(col_link, col_date):
            sheet_link = str(row[col_link]).strip()
            sheet_date = str(row[col_date]).strip()

            if sheet_link:
                existing_paths.add(url_path_only(sheet_link))

            existing_records.add((sheet_link, sheet_date))
            existing_records.add((add_utm(sheet_link, utm), sheet_date))

    print(f"📊 Connected. Existing keys: {len(existing_records)} | Existing paths: {len(existing_paths)} | Max Serial: {max_serial}")

    # 3) Fetch feed (with cache buster)
    entries = fetch_feed(rss_url)
    if not entries:
        print("⚠️ No entries found in the feed.")
        return

    # Debug: show top 5 feed entries
    print(f"📰 Feed entries fetched: {len(entries)}")
    for i, e in enumerate(entries[:5]):
        t = (e.get("title") or "").strip()
        l = (e.get("link") or "").strip()
        dt = (e.get("published") or e.get("updated") or "").strip()
        print(f"🆕 Feed[{i}] date={dt} | path={url_path_only(l)} | title={shorten(t, 70)}")
        print(f"    link={shorten(l, 160)}")

    # 4) Collect new items with timestamp so we can sort newest-first
    new_items = []  # (ts, title, raw_link, utm_link, date)

    # Keep skip logs short (only first few skips)
    skip_logged = 0
    path_mismatch_logged = 0

    for entry in entries:
        title = (entry.get("title") or "").strip()
        raw_link = (entry.get("link") or "").strip()
        date = (entry.get("published") or entry.get("updated") or "N/A").strip()

        if not raw_link:
            continue

        utm_link = add_utm(raw_link, utm)
        ts = entry_ts(entry)

        raw_key = (raw_link, date)
        utm_key = (utm_link, date)

        raw_exists = raw_key in existing_records
        utm_exists = utm_key in existing_records
        path_exists = url_path_only(raw_link) in existing_paths or url_path_only(utm_link) in existing_paths

        # If the article path exists but (link,date) does not, likely date formatting mismatch
        if path_exists and not (raw_exists or utm_exists):
            if path_mismatch_logged < 3:
                print(f"⚠️ PATH MATCH but (link,date) not found: {shorten(title, 70)}")
                print(f"    path={url_path_only(raw_link)}")
                print(f"    feed_date={date}")
            path_mismatch_logged += 1

        # Duplicate check against both raw and utm form
        if raw_exists or utm_exists:
            if skip_logged < 5:
                reasons = []
                if raw_exists:
                    reasons.append("raw(link,date) exists")
                if utm_exists:
                    reasons.append("utm(link,date) exists")
                print(f"⏭️ SKIP: {shorten(title, 70)} | feed_date={date} | reasons={', '.join(reasons)}")
                print(f"    raw={shorten(raw_link, 170)}")
                print(f"    utm={shorten(utm_link, 170)}")
            skip_logged += 1
            continue

        new_items.append((ts, title, raw_link, utm_link, date))
        existing_records.add(raw_key)
        existing_records.add(utm_key)

    if not new_items:
        print("✨ No new entries to add.")
        return

    # 5) Sort newest -> oldest so newest appears at top (row 2)
    new_items_sorted = sorted(new_items, key=lambda x: x[0], reverse=True)
    n = len(new_items_sorted)

    # Debug: show what we plan to insert (top few)
    print(f"🧩 New items to insert: {n}")
    for i, (_ts, title, raw_link, utm_link, date) in enumerate(new_items_sorted[:5]):
        print(f"➕ New[{i}] date={date} | path={url_path_only(raw_link)} | title={shorten(title, 70)}")
        print(f"    raw={shorten(raw_link, 160)}")
        print(f"    utm={shorten(utm_link, 160)}")

    # Serial highest at top
    serial = max_serial + n

    # Ensure inserted row covers up to LinkedIn columns too
    max_col_needed = max(
        col_serial, col_title, col_link, col_date,
        col_li_post, col_li_posted_at, col_li_post_id, col_li_error
    )

    rows_to_insert = []
    for (_ts, title, _raw_link, utm_link, date) in new_items_sorted:
        row = [""] * (max_col_needed + 1)
        row[col_serial] = serial
        row[col_title] = title
        row[col_link] = utm_link
        row[col_date] = date

        # LinkedIn prep columns
        row[col_li_post] = post_to_linkedin_default
        row[col_li_posted_at] = ""
        row[col_li_post_id] = ""
        row[col_li_error] = ""

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
        if name == "taxscan_feed_to_sheet":
            job_taxscan_feed_to_sheet(job, google_cfg)
        else:
            print(f"⚠️ Unknown job name '{name}'. (Add handler in main.py)")


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