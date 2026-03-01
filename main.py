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
        return Credentials.from_service_account_info(info, scopes=scopes)

    sa_file = google_cfg.get("service_account_file", "service_account.json")
    return Credentials.from_service_account_file(sa_file, scopes=scopes)


def get_worksheet(google_cfg: dict, spreadsheet_id: str, worksheet_index: int):
    creds = load_credentials(google_cfg)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)
    return sh.get_worksheet(int(worksheet_index))


# ---------------- Feed ----------------
def fetch_feed(rss_url: str):
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
    try:
        response = requests.get(rss_url, headers=headers, timeout=20)
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
        except APIError as e:
            print("❌ Google Sheets API Error (prune):")
            print(getattr(e.response, "text", str(e)))
            return

    # 2) Build existing record set + max serial
    existing_records = set()
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

            existing_records.add((sheet_link, sheet_date))
            existing_records.add((add_utm(sheet_link, utm), sheet_date))

    print(f"📊 Connected. Existing keys: {len(existing_records)} | Max Serial: {max_serial}")

    # 3) Fetch feed
    entries = fetch_feed(rss_url)
    if not entries:
        print("⚠️ No entries found in the feed.")
        return

    # 4) Collect new items with timestamp so we can sort newest-first
    new_items = []  # (ts, title, raw_link, utm_link, date)
    for entry in entries:
        title = (entry.get("title") or "").strip()
        raw_link = (entry.get("link") or "").strip()
        date = (entry.get("published") or entry.get("updated") or "N/A").strip()

        if not raw_link:
            continue

        utm_link = add_utm(raw_link, utm)
        ts = entry_ts(entry)

        # Duplicate check against both raw and utm form
        if (raw_link, date) not in existing_records and (utm_link, date) not in existing_records:
            new_items.append((ts, title, raw_link, utm_link, date))
            existing_records.add((raw_link, date))
            existing_records.add((utm_link, date))

    if not new_items:
        print("✨ No new entries to add.")
        return

    # 5) Sort newest -> oldest so newest appears at top (row 2)
    new_items_sorted = sorted(new_items, key=lambda x: x[0], reverse=True)
    n = len(new_items_sorted)

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
    try:
        config = load_config()
    except Exception as e:
        print(f"❌ Failed to read {CONFIG_FILE}: {repr(e)}")
        return

    run_jobs(config)
    print("\n✨ All enabled jobs finished.")


if __name__ == "__main__":
    main()