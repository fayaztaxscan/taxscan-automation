import os
import json
import time
import requests
import feedparser
import gspread

from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, SpreadsheetNotFound

# --- CONFIGURATION ---
SERVICE_ACCOUNT_FILE = "service_account.json"   # local dev fallback
SERVICE_ACCOUNT_JSON_ENV = "GOOGLE_SERVICE_ACCOUNT_JSON"  # GitHub Actions secret env name

SPREADSHEET_ID = "197zi-fymx0YaFYXcnzS0HQmanWccOcOH7oMRPy3OK60"
WORKSHEET_INDEX = 0

RSS_URL = "https://www.taxscan.in/feeds.xml"

HEADER_ROW = 1
INSERT_AT_ROW = 2  # row 1 = header; insert new items starting at row 2

# UTM to be appended
UTM_PARAMS = {
    "utm_source": "taxscan",
    "utm_medium": "whatsapp",
    "utm_campaign": "news",
}


# ---------------- Helpers ----------------
def add_utm(url: str, utm: dict) -> str:
    """Merge UTM params into URL without breaking existing query params."""
    if not url:
        return url
    parts = urlparse(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    # Keep existing UTM values if present; otherwise set defaults
    for k, v in utm.items():
        q.setdefault(k, v)
    new_query = urlencode(q, doseq=True)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def safe_int(x, default=0):
    try:
        return int(str(x).strip())
    except Exception:
        return default


# ---------------- Google Sheet ----------------
def load_credentials():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # GitHub Actions: JSON stored in secret GOOGLE_SERVICE_ACCOUNT_JSON
    sa_json = os.environ.get(SERVICE_ACCOUNT_JSON_ENV)
    if sa_json:
        info = json.loads(sa_json)
        return Credentials.from_service_account_info(info, scopes=scopes)

    # Local dev: read from file
    return Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)


def get_google_sheet():
    creds = load_credentials()
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    return sh.get_worksheet(WORKSHEET_INDEX)


# ---------------- Feed ----------------
def fetch_feed():
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
    try:
        response = requests.get(RSS_URL, headers=headers, timeout=20)
        if response.status_code == 200:
            return feedparser.parse(response.content).entries
        print(f"❌ Feed returned status: {response.status_code}")
        return []
    except Exception as e:
        print(f"❌ Feed Network Error: {e}")
        return []


def main():
    print("🚀 Starting Taxscan Sync (Top Insert + Desc Serial + UTM Enabled)...")

    # 1) Connect to sheet + read existing rows
    try:
        sheet = get_google_sheet()
        all_rows = sheet.get_all_values()  # includes header

        existing_records = set()

        # Expected columns:
        # A: Serial No, B: Title, C: Link, D: Date
        # Build duplicates set from (Link, Date)
        for i, row in enumerate(all_rows):
            if i == 0:  # header row
                continue
            if len(row) >= 4:
                sheet_link = row[2].strip()
                sheet_date = row[3].strip()

                # Store both as-is and UTM-normalized so older rows without UTM won't duplicate
                existing_records.add((sheet_link, sheet_date))
                existing_records.add((add_utm(sheet_link, UTM_PARAMS), sheet_date))

        # Find max serial number from column A (skip header)
        max_serial = 0
        for i, row in enumerate(all_rows):
            if i == 0:
                continue
            if len(row) >= 1 and row[0].strip():
                max_serial = max(max_serial, safe_int(row[0], 0))

        print(f"📊 Connected. Found {len(existing_records)} (link,date) keys. Max Serial = {max_serial}")

    except SpreadsheetNotFound:
        print("❌ Spreadsheet not found. Double-check SPREADSHEET_ID.")
        print("   Also ensure the Sheet is shared with the service-account email as Editor.")
        return
    except APIError as e:
        print("❌ Google Sheets API Error:")
        print(getattr(e.response, "text", str(e)))
        return
    except Exception as e:
        print(f"❌ Google Sheets Connection Error: {repr(e)}")
        return

    # 2) Fetch RSS feed
    entries = fetch_feed()
    if not entries:
        print("⚠️ No entries found in the feed.")
        return

    # 3) Collect new items (do NOT write row-by-row)
    # Feed is often newest->oldest; we keep as-is then decide insertion order later.
    new_items = []  # tuples: (title, raw_link, utm_link, date)
    for entry in entries:
        title = (entry.get("title") or "").strip()
        raw_link = (entry.get("link") or "").strip()
        date = (entry.get("published") or "N/A").strip()

        if not raw_link:
            continue

        utm_link = add_utm(raw_link, UTM_PARAMS)

        # Duplicate check: try both raw and UTM form against existing_records
        if (raw_link, date) not in existing_records and (utm_link, date) not in existing_records:
            new_items.append((title, raw_link, utm_link, date))
            # Add both forms to prevent duplicates within the same run
            existing_records.add((raw_link, date))
            existing_records.add((utm_link, date))

    if not new_items:
        print("✨ No new entries to add.")
        return

    # 4) Insert at top + keep newest on top + Serial highest at top
    # We insert rows at row 2. If we insert in oldest->newest order,
    # the newest ends up at the very top after insertion.
    new_items_to_insert = list(reversed(new_items))  # oldest -> newest
    n = len(new_items_to_insert)

    # Serial highest at top (descending down the sheet)
    serial = max_serial + n  # highest serial goes to first inserted row (row 2)
    rows_to_insert = []
    for (title, _raw_link, utm_link, date) in new_items_to_insert:
        rows_to_insert.append([serial, title, utm_link, date])
        serial -= 1

    try:
        # Single API call to insert all rows
        sheet.insert_rows(rows_to_insert, row=INSERT_AT_ROW, value_input_option="RAW")
        print(f"✅ Inserted {n} new rows at top (row {INSERT_AT_ROW}).")
        print(f"🔢 Serial range: {max_serial + 1} .. {max_serial + n} (highest at top).")
    except APIError as e:
        print("❌ Insert rows API Error:")
        print(getattr(e.response, "text", str(e)))
        return
    except Exception as e:
        print(f"❌ Insert rows Error: {repr(e)}")
        return

    print("\n✨ Sync Complete!")


if __name__ == "__main__":
    main()