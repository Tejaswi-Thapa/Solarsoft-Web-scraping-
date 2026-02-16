from __future__ import annotations

from bs4 import BeautifulSoup
from urllib.parse import urljoin

import os
import sys
import pandas as pd
import requests


# =========================
# CONSTANTS
# =========================
BASE_URL = "https://lmsal.com/solarsoft/latest_events_archive.html"
MIN_YEAR = 2020

EVENTS_CSV_PATH = "lmsal_events.csv"
SUMMARY_CSV_PATH = "lmsal_latest_events_archive.csv"  # optional


# =========================
# HELPERS
# =========================
def get_soup(url: str) -> BeautifulSoup | None:
    """Download a URL and return BeautifulSoup, or None if request fails/404s."""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.exceptions.RequestException:
        return None


def load_existing(csv_path: str) -> pd.DataFrame:
    """Load existing CSV if it exists; otherwise return empty DataFrame."""
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return pd.DataFrame()
    return pd.read_csv(csv_path, dtype=str)


# =========================
# OPTIONAL: SUMMARY SCRAPE (base table)
# =========================
def fetch_archive_summary_table(url: str) -> pd.DataFrame:
    soup = get_soup(url)
    if soup is None:
        return pd.DataFrame()

    rows = []
    for tr in soup.find_all("tr"):
        cols = tr.find_all("td")
        if len(cols) < 9:
            continue
        row_data = [col.get_text(strip=True) for col in cols[:9]]
        rows.append(row_data)

    columns = [
        "Report Date",
        "Start Time",
        "End Time",
        "Total Events",
        "Largest Flare",
        "C-class",
        "M-class",
        "X-class",
        "Proton Events",
    ]

    df = pd.DataFrame(rows, columns=columns)
    df["Report Date"] = pd.to_datetime(
        df["Report Date"], format="%d-%b-%Y %H:%M", errors="coerce")
    df = df[df["Report Date"].dt.year >= MIN_YEAR]
    df = df.sort_values("Report Date").reset_index(drop=True)
    return df


def update_summary_csv() -> None:
    print("SUMMARY SCRAPER STARTED ...")
    new_df = fetch_archive_summary_table(BASE_URL)
    if new_df.empty:
        print("No summary rows scraped.")
        return

    print("Summary rows scraped this run:", len(new_df))

    old_df = load_existing(SUMMARY_CSV_PATH)
    combined = pd.concat(
        [old_df, new_df], ignore_index=True) if not old_df.empty else new_df

    combined["Report Date"] = pd.to_datetime(
        combined["Report Date"], errors="coerce")
    combined = combined.drop_duplicates(subset=["Report Date"], keep="last")
    combined = combined.sort_values("Report Date", ascending=False)

    combined.to_csv(SUMMARY_CSV_PATH, index=False)
    added = max(0, len(combined) - len(old_df))
    print(
        f"[OK] Wrote {len(combined)} rows to {SUMMARY_CSV_PATH}. Added {added} new rows.")


# =========================
# REQUIRED: EVENT-LEVEL SCRAPE (nested table)
# =========================
def fetch_snapshot_links(base_url: str) -> list[dict]:
    """Collect Snapshot Time links from base page (stops once older than MIN_YEAR)."""
    soup = get_soup(base_url)
    if soup is None:
        return []

    snapshots: list[dict] = []

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 1:
            continue

        a = tds[0].find("a", href=True)
        if not a:
            continue

        snapshot_time_text = a.get_text(strip=True)
        snapshot_url = urljoin(base_url, a["href"])

        snap_dt = pd.to_datetime(
            snapshot_time_text, format="%d-%b-%Y %H:%M", errors="coerce")
        if pd.isna(snap_dt):
            continue

        if snap_dt.year < MIN_YEAR:
            break

        snapshots.append({"snapshot_time": snapshot_time_text,
                         "snapshot_url": snapshot_url})

    return snapshots


def find_event_table(soup: BeautifulSoup):
    """Find the event table by checking for headers that include EName/GOES/Derived Position."""
    for table in soup.find_all("table"):
        header_text = table.get_text(" ", strip=True)
        if "EName" in header_text and "GOES Class" in header_text and "Derived Position" in header_text:
            return table
    return None


def fetch_events_from_snapshot(snapshot_time: str, snapshot_url: str) -> pd.DataFrame:
    """Scrape events from a single snapshot page."""
    soup = get_soup(snapshot_url)
    if soup is None:
        return pd.DataFrame()

    table = find_event_table(soup)
    if table is None:
        return pd.DataFrame()

    rows = []
    for tr in table.find_all("tr"):
        cols = tr.find_all("td")
        if len(cols) < 7:
            continue

        event_num = cols[0].get_text(strip=True)
        ename = cols[1].get_text(strip=True)
        start = cols[2].get_text(strip=True)
        stop = cols[3].get_text(strip=True)
        peak = cols[4].get_text(strip=True)
        goes_class = cols[5].get_text(strip=True)
        derived_pos = cols[6].get_text(strip=True)

        if not ename:
            continue

        rows.append([snapshot_time, snapshot_url, event_num, ename,
                    start, stop, peak, goes_class, derived_pos])

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=[
        "Snapshot Time",
        "Snapshot URL",
        "Event#",
        "EName",
        "Start",
        "Stop",
        "Peak",
        "GOES Class",
        "Derived Position",
    ])


def update_events_csv() -> None:
    print("EVENT SCRAPER STARTED ...")

    snapshots = fetch_snapshot_links(BASE_URL)
    if not snapshots:
        print("No snapshot links found.")
        return

    print("Snapshot links found:", len(snapshots))

    all_events: list[pd.DataFrame] = []
    skipped_pages = 0
    broken_urls: list[str] = []

    for i, s in enumerate(snapshots, start=1):
        df = fetch_events_from_snapshot(s["snapshot_time"], s["snapshot_url"])
        if df.empty:
            skipped_pages += 1
            # could be 404 or no data; log the URL so you can show your professor you handled it
            broken_urls.append(s["snapshot_url"])
            continue

        all_events.append(df)

        if i % 20 == 0:
            print(f"Progress: {i}/{len(snapshots)} snapshots visited...")

    print("Snapshots skipped (404/no data/etc):", skipped_pages)

    if broken_urls:
        # Keep it short in console, but useful
        print("Example skipped URL:", broken_urls[0])

    if not all_events:
        print("No event rows scraped. Nothing to write.")
        return

    new_df = pd.concat(all_events, ignore_index=True)
    print("Event rows scraped this run:", len(new_df))

    old_df = load_existing(EVENTS_CSV_PATH)
    combined = pd.concat(
        [old_df, new_df], ignore_index=True) if not old_df.empty else new_df

    combined["Snapshot Time Parsed"] = pd.to_datetime(
        combined["Snapshot Time"], format="%d-%b-%Y %H:%M", errors="coerce"
    )
    for col in ["Start", "Stop", "Peak"]:
        combined[col] = pd.to_datetime(combined[col], errors="coerce")

    combined = combined.sort_values(
        ["Snapshot Time Parsed", "Peak"], ascending=[False, False])

    # Deduplicate overlapping events across snapshots
    combined = combined.drop_duplicates(subset=["EName"], keep="first")

    combined = combined.drop(columns=["Snapshot Time Parsed"])

    combined.to_csv(EVENTS_CSV_PATH, index=False)

    old_len = len(old_df) if not old_df.empty else 0
    added = max(0, len(combined) - old_len)
    print(
        f"[OK] Wrote {len(combined)} unique events to {EVENTS_CSV_PATH}. Added {added} new unique events.")


# =========================
# QUERY
# =========================
def query_events_csv() -> None:
    if not os.path.exists(EVENTS_CSV_PATH):
        print(f"No events CSV yet. Run: python {os.path.basename(__file__)}")
        return

    df = pd.read_csv(EVENTS_CSV_PATH, dtype=str)
    print("Total unique events in CSV:", len(df))

    if "GOES Class" in df.columns:
        x = df[df["GOES Class"].str.startswith("X", na=False)]
        print("\nX-class events (up to 20):")
        if x.empty:
            print("None found.")
        else:
            print(x[["EName", "Peak", "GOES Class", "Derived Position"]].head(
                20).to_string(index=False))


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # python lmsal_archive_scrape.py           -> update events CSV (assignment)
    # python lmsal_archive_scrape.py query     -> query events CSV
    # python lmsal_archive_scrape.py summary   -> update summary CSV (optional)

    if len(sys.argv) > 1 and sys.argv[1].lower() == "query":
        query_events_csv()
    elif len(sys.argv) > 1 and sys.argv[1].lower() == "summary":
        update_summary_csv()
    else:
        update_events_csv()
